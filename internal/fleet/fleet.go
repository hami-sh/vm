package fleet

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/hami-sh/vm/internal/actor"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/chaos"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/invariant"
	"github.com/hami-sh/vm/internal/scheduler"
	"github.com/hami-sh/vm/internal/store"
)

// Scheduler is the assembly surface; a factory override supports isolated
// fault tests. The default uses the real SQLite-backed scheduler.
type Scheduler interface {
	api.ControlPlane
	Snapshot(context.Context) (api.Snapshot, error)
	EventsSince(context.Context, int64) ([]api.Event, bool, error)
	SetDown(context.Context, bool) error
	SetHostAdmin(context.Context, string, api.AdminState) error
	SetStoreDelay(context.Context, time.Duration) error
}
type SchedulerFactory func(context.Context, config.Config, clock.Clock, api.Transport, string) (Scheduler, func(), error)
type Options struct {
	Config              config.Config
	Racks, HostsPerRack int
	Seed                int64
	Speed               float64
	Clock               clock.Controllable
	StorePath           string
	SchedulerFactory    SchedulerFactory
}
type Fleet struct {
	Control api.ControlPlane
	Chaos   *chaos.Engine
	Clock   clock.Controllable
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	queries chan publicationQuery
	runtime *runtime
	once    sync.Once
	temp    string
}
type publicationQuery struct {
	reply  chan<- api.Snapshot
	sub    *subscription
	remove *subscription
	ack    chan struct{}
}
type subscription struct {
	snapshots chan api.Snapshot
	events    chan api.Event
}

func newScheduler(ctx context.Context, cfg config.Config, c clock.Clock, transport api.Transport, path string) (Scheduler, func(), error) {
	db, err := store.Open(path)
	if err != nil {
		return nil, nil, err
	}
	s, err := scheduler.New(ctx, cfg, c, transport, db)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return s, func() { <-s.Done(); db.Close() }, nil
}

func New(ctx context.Context, o Options) (*Fleet, error) {
	cfg := o.Config
	if cfg.SnapshotEvery == 0 {
		cfg = config.Default()
	}
	if o.Racks > 0 {
		cfg.Racks = o.Racks
	}
	if o.HostsPerRack > 0 {
		cfg.HostsPerRack = o.HostsPerRack
	}
	speed := o.Speed
	if speed == 0 {
		speed = cfg.DefaultSpeed
	}
	c := o.Clock
	if c == nil {
		c = clock.NewScaled(time.Unix(0, 0).UTC(), speed)
	}
	ctx, cancel := context.WithCancel(ctx)
	f := &Fleet{ctx: ctx, cancel: cancel, done: make(chan struct{}), queries: make(chan publicationQuery), Clock: c}
	if o.StorePath == "" {
		dir, err := os.MkdirTemp("", "vm-fleet-")
		if err != nil {
			cancel()
			return nil, err
		}
		f.temp = dir
		o.StorePath = dir + "/fleet.db"
	}
	events := make(chan api.Event, cfg.EventRetention)
	emit := func(e api.Event) {
		select {
		case events <- e:
		case <-ctx.Done():
		}
	}
	r, err := newRuntime(ctx, cfg, c, o, emit)
	if err != nil {
		cancel()
		os.RemoveAll(f.temp)
		return nil, err
	}
	f.runtime = r
	f.Control = r
	f.Chaos = chaos.New(ctx, cfg, c, r, o.Seed, emit)
	go f.publish(cfg, events)
	return f, nil
}
func (f *Fleet) ControlPlane() api.ControlPlane { return f.Control }
func (f *Fleet) ChaosEngine() api.ChaosEngine   { return f.Chaos }
func (f *Fleet) Close() {
	f.once.Do(func() {
		f.cancel()
		<-f.done
		f.Chaos.Close()
		<-f.runtime.done
		if f.temp != "" {
			os.RemoveAll(f.temp)
		}
	})
}
func clone(s api.Snapshot) api.Snapshot {
	b, _ := json.Marshal(s)
	var out api.Snapshot
	_ = json.Unmarshal(b, &out)
	return out
}
func (f *Fleet) Latest() api.Snapshot {
	r, err := actor.Ask(f.ctx, f.queries, func(ch chan<- api.Snapshot) publicationQuery { return publicationQuery{reply: ch} })
	if err != nil {
		return api.Snapshot{}
	}
	return r
}
func (f *Fleet) Subscribe() (<-chan api.Snapshot, <-chan api.Event, func()) {
	// Recorders can be busy dispatching an entire synchronous flood. Keep the
	// bounded event backlog proportional to fleet size, without blocking actors.
	s := &subscription{snapshots: make(chan api.Snapshot, 1), events: make(chan api.Event, f.runtime.cfg.MaxVMs*16)}
	ack := make(chan struct{})
	select {
	case f.queries <- publicationQuery{sub: s, ack: ack}:
		<-ack
	case <-f.ctx.Done():
		close(s.snapshots)
		close(s.events)
	}
	var once sync.Once
	return s.snapshots, s.events, func() {
		once.Do(func() {
			ack := make(chan struct{})
			select {
			case f.queries <- publicationQuery{remove: s, ack: ack}:
				<-ack
			case <-f.ctx.Done():
			}
		})
	}
}
func (f *Fleet) publish(cfg config.Config, events <-chan api.Event) {
	defer close(f.done)
	check := invariant.New(cfg)
	tick := f.Clock.NewTicker(cfg.InvariantEvery)
	defer tick.Stop()
	wall := (clock.Real{}).NewTicker(cfg.SSEEvery)
	defer wall.Stop()
	subs := map[*subscription]bool{}
	defer func() {
		for s := range subs {
			close(s.snapshots)
			close(s.events)
		}
	}()
	var latest api.Snapshot
	var recent []api.Event
	var eventSeq, snapshotSeq, schedulerSeq int64
	var logIncarnation int64
	var next time.Time
	var nextCheck time.Time
	appendEvent := func(e api.Event) {
		if e.Kind == api.EventChaos && e.Detail == "reset " {
			check = invariant.New(cfg)
		}
		eventSeq++
		e.Seq = eventSeq
		recent = append(recent, e)
		for s := range subs {
			select {
			case s.events <- e:
			default:
				// Never renumber after dropping: the next delivered event retains
				// this gap in the publisher's sequence for recording validation.
			}
		}
	}
	refresh := func(force bool) {
		s := clone(latest)
		now := f.Clock.Now()
		if nextCheck.IsZero() || !now.Before(nextCheck) {
			o, err := f.runtime.observe(f.ctx)
			if err != nil {
				return
			}
			s = o.snapshot
			raw, epoch := o.raw, o.epoch
			if epoch != logIncarnation {
				schedulerSeq = 0
				logIncarnation = epoch
			}
			batch, truncated, err := f.runtime.events(f.ctx, schedulerSeq)
			if err != nil {
				return
			}
			if truncated {
				appendEvent(check.EventsLost(s.At))
			}
			for _, e := range batch {
				if e.Seq > schedulerSeq {
					schedulerSeq = e.Seq
					appendEvent(e)
				}
			}
			for {
				select {
				case e := <-events:
					appendEvent(e)
				default:
					goto drained
				}
			}
		drained:
			s.Recent = recent
			s.Observed = make([]api.HostObservation, 0, len(raw))
			for _, observation := range raw {
				s.Observed = append(s.Observed, api.HostObservation{HostID: observation.Host.ID, Connected: observation.Connected, Actual: slices.Clone(observation.Actual)})
			}
			for _, e := range check.Check(s, raw...) {
				appendEvent(e)
			}
			for _, e := range check.CheckKeys(s, o.keys, raw...) {
				appendEvent(e)
			}
			nextCheck = now.Add(cfg.InvariantEvery)
		}
		s.At = f.Clock.Now().UnixMilli()
		if sim, err := f.Chaos.Status(f.ctx); err == nil {
			s.Sim = sim
		}
		s.Invariants = check.Status()
		if force || next.IsZero() || !f.Clock.Now().Before(next) {
			snapshotSeq++
			s.Seq = snapshotSeq
			s.Recent = recent
			if len(s.Recent) > cfg.RecentEventLimit {
				s.Recent = s.Recent[len(s.Recent)-cfg.RecentEventLimit:]
			}
			latest = clone(s)
			recent = nil
			next = f.Clock.Now().Add(cfg.SnapshotEvery)
			for sub := range subs {
				select {
				case sub.snapshots <- clone(latest):
				default:
				}
			}
		}
	}
	refresh(true)
	for {
		select {
		case <-f.ctx.Done():
			return
		case <-tick.C():
			refresh(false)
		case <-f.Chaos.Changes():
			refresh(true)
		case <-wall.C():
			if f.Clock.Paused() {
				refresh(true)
			}
		case e := <-events:
			appendEvent(e)
		case q := <-f.queries:
			if q.reply != nil {
				q.reply <- clone(latest)
			}
			if q.sub != nil {
				subs[q.sub] = true
				q.sub.snapshots <- clone(latest)
			}
			if q.remove != nil {
				if subs[q.remove] {
					delete(subs, q.remove)
					close(q.remove.snapshots)
					close(q.remove.events)
				}
			}
			if q.ack != nil {
				close(q.ack)
			}
		}
	}
}

var _ api.SnapshotSource = (*Fleet)(nil)
