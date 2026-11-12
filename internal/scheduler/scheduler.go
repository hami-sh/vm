// Package scheduler owns placement intent and the control-plane actor.
package scheduler

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"time"

	"github.com/hami-sh/vm/internal/actor"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/store"
)

var Address = api.Addr{Kind: api.KindScheduler, ID: "scheduler"}

type request struct {
	op, id, key string
	arg         any
	reply       chan<- result
}
type result struct {
	value any
	err   error
}
type tick struct{}
type delivery struct {
	to  api.Addr
	msg any
}
type host struct {
	record     store.HostRecord
	lease      api.LeaseState
	last       int64
	seq        uint64
	disk       bool
	drainUntil int64
	inventory  []api.InventoryItem
}
type reservation struct {
	host       string
	gen, until int64
	admitted   bool
}
type state struct {
	data         store.Data
	hosts        map[string]host
	reservations map[string]reservation
	exclude      map[string]string
	recovering   map[string]int64
	replacement  map[string]bool
	claims       map[string]string
	fresh        bool
	recoverUntil int64
	down         bool
	storeDelay   time.Duration
	epoch        int64
	counters     api.Counters
	metrics      metrics
	events       []api.Event
	seq          int64
}

// Effect is a proposed transaction followed by network deliveries and an API
// reply. handle never publishes tentative state or does I/O.
type Effect struct {
	next     *state
	events   []api.Event
	sends    []delivery
	reply    chan<- result
	response result
}

type Scheduler struct {
	cfg      config.Config
	clock    clock.Clock
	store    *store.Store
	endpoint api.Endpoint
	inbox    chan request
	done     chan struct{}
	state    *state // accessed only by run, or directly by single-threaded tests
}

var _ api.ControlPlane = (*Scheduler)(nil)

func New(ctx context.Context, cfg config.Config, c clock.Clock, transport api.Transport, db *store.Store) (*Scheduler, error) {
	d, epoch, err := db.Start(ctx)
	if err != nil {
		return nil, err
	}
	initialized := d
	if d.RecoverUntil == -1 {
		initialized.RecoverUntil = c.Now().Add(cfg.HostLeaseTTL).UnixMilli()
	} else if d.RecoverUntil <= c.Now().UnixMilli() {
		initialized.RecoverUntil = 0
	}
	if initialized.RecoverUntil != d.RecoverUntil {
		if err := db.Commit(ctx, d, initialized, nil, cfg.EventRetention); err != nil {
			return nil, err
		}
		d = initialized
	}
	ep, err := transport.Attach(Address)
	if err != nil {
		return nil, err
	}
	s := &Scheduler{cfg: cfg, clock: c, store: db, endpoint: ep, inbox: make(chan request, cfg.MaxVMs), done: make(chan struct{})}
	s.state = newState(d, epoch, c.Now(), cfg)
	s.state.recoverUntil = d.RecoverUntil
	s.state.fresh = d.RecoverUntil > 0
	events, err := db.Events(ctx)
	if err != nil {
		ep.Close()
		return nil, err
	}
	if len(events) > 0 {
		s.state.seq = events[len(events)-1].Seq
		if len(events) > cfg.EventRetention {
			events = events[len(events)-cfg.EventRetention:]
		}
		s.state.events = events
	}
	go s.run(ctx)
	return s, nil
}

func newState(d store.Data, epoch int64, now time.Time, cfg config.Config) *state {
	s := &state{data: d, epoch: epoch, hosts: map[string]host{}, reservations: map[string]reservation{}, exclude: map[string]string{}, recovering: map[string]int64{}, replacement: map[string]bool{}, claims: map[string]string{}, metrics: newMetrics()}
	s.counters.Recoveries = map[string]int{}
	for id, h := range d.Hosts {
		// These leases are already Suspect: only the fresh lost-grace period
		// remains, not another healthy TTL followed by that grace period.
		s.hosts[id] = host{record: h, lease: api.LeaseSuspect, last: now.Add(-cfg.HostLeaseTTL).UnixMilli()}
		if h.Admin == api.AdminDraining {
			host := s.hosts[id]
			host.drainUntil = now.Add(cfg.DrainGrace).UnixMilli()
			s.hosts[id] = host
		}
	}
	return s
}

func (st *state) clone() *state {
	n := *st
	n.data = store.Data{Epoch: st.data.Epoch, RecoverUntil: st.data.RecoverUntil, VMs: maps.Clone(st.data.VMs), Hosts: maps.Clone(st.data.Hosts), Queue: maps.Clone(st.data.Queue)}
	n.hosts = maps.Clone(st.hosts)
	for id, h := range n.hosts {
		h.inventory = append([]api.InventoryItem(nil), h.inventory...)
		n.hosts[id] = h
	}
	n.reservations = maps.Clone(st.reservations)
	n.exclude = maps.Clone(st.exclude)
	n.recovering = maps.Clone(st.recovering)
	n.replacement = maps.Clone(st.replacement)
	n.claims = maps.Clone(st.claims)
	n.counters.Recoveries = maps.Clone(st.counters.Recoveries)
	n.metrics = st.metrics.clone()
	// Events are append-only; existing elements are never rewritten. Sharing
	// that prefix avoids copying the retained audit log on every query/tick.
	return &n
}

func (s *Scheduler) run(ctx context.Context) {
	defer close(s.done)
	defer s.endpoint.Close()
	timer := s.clock.NewTicker(s.cfg.SnapshotEvery)
	defer timer.Stop()
	for {
		var msg any
		select {
		case <-ctx.Done():
			return
		case r := <-s.inbox:
			msg = r
		case e, ok := <-s.endpoint.Inbox():
			if !ok {
				return
			}
			msg = e
		case <-timer.C():
			msg = tick{}
		}
		for _, e := range s.handle(s.clock.Now(), msg) {
			// The actor waits for this commit's completion before planning the
			// next mutation. SQLite and slow-store faults run outside handle.
			if !reflect.DeepEqual(s.state.data, e.next.data) || len(e.events) > 0 {
				completed := make(chan error, 1)
				before, after, events := s.state.data, e.next.data, e.events
				delay := s.state.storeDelay
				actor.Work(ctx, completed, func(ctx context.Context) error {
					if delay > 0 {
						timer := s.clock.NewTimer(delay)
						defer timer.Stop()
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-timer.C():
						}
					}
					return s.store.Commit(ctx, before, after, events, s.cfg.EventRetention)
				})
				select {
				case <-ctx.Done():
					return
				case err := <-completed:
					if err != nil {
						s.state.down = true
						if e.reply != nil {
							e.reply <- result{err: &api.Error{Status: 503, Msg: err.Error(), RetryAfterS: s.retryAfter()}}
						}
						continue
					}
				}
			}
			s.state = e.next
			for _, d := range e.sends {
				_ = s.endpoint.Send(d.to, api.PortControl, d.msg)
			}
			if e.reply != nil {
				e.reply <- e.response
			}
		}
	}
}

func (s *Scheduler) call(ctx context.Context, r request) (any, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// done also bounds calls whose caller supplied no deadline.
	go func() {
		select {
		case <-s.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	rsp, err := actor.Ask(ctx, s.inbox, func(ch chan<- result) request { r.reply = ch; return r })
	if err != nil {
		if errors.Is(err, context.Canceled) {
			select {
			case <-s.done:
				return nil, &api.Error{Status: 503, Msg: "scheduler stopped", RetryAfterS: s.retryAfter()}
			default:
			}
		}
		return nil, err
	}
	return rsp.value, rsp.err
}

func (s *Scheduler) retryAfter() int {
	n := int((s.cfg.BackoffBase + time.Second - 1) / time.Second)
	if n < 1 {
		n = 1
	}
	return n
}

func (s *Scheduler) CreateVM(ctx context.Context, key string, r api.CreateVMRequest) (api.CreateVMResponse, error) {
	v, e := s.call(ctx, request{op: "create", key: key, arg: r})
	if e != nil {
		return api.CreateVMResponse{}, e
	}
	return v.(api.CreateVMResponse), nil
}
func (s *Scheduler) GetVM(ctx context.Context, id string) (api.VM, error) {
	v, e := s.call(ctx, request{op: "get", id: id})
	if e != nil {
		return api.VM{}, e
	}
	return v.(api.VM), nil
}
func (s *Scheduler) DeleteVM(ctx context.Context, id string) (api.VM, error) {
	v, e := s.call(ctx, request{op: "delete", id: id})
	if e != nil {
		return api.VM{}, e
	}
	return v.(api.VM), nil
}
func (s *Scheduler) ListVMs(ctx context.Context) ([]api.VM, error) {
	v, e := s.call(ctx, request{op: "vms"})
	if e != nil {
		return nil, e
	}
	return v.([]api.VM), nil
}
func (s *Scheduler) ListHosts(ctx context.Context) ([]api.Host, error) {
	v, e := s.call(ctx, request{op: "hosts"})
	if e != nil {
		return nil, e
	}
	return v.([]api.Host), nil
}
func (s *Scheduler) Queue(ctx context.Context) ([]api.QueueEntry, error) {
	v, e := s.call(ctx, request{op: "queue"})
	if e != nil {
		return nil, e
	}
	return v.([]api.QueueEntry), nil
}
func (s *Scheduler) SetPriority(ctx context.Context, id string, p api.Priority) error {
	_, e := s.call(ctx, request{op: "priority", id: id, arg: p})
	return e
}
func (s *Scheduler) Snapshot(ctx context.Context) (api.Snapshot, error) {
	v, e := s.call(ctx, request{op: "snapshot"})
	if e != nil {
		return api.Snapshot{}, e
	}
	return v.(api.Snapshot), nil
}
func (s *Scheduler) Metrics(ctx context.Context) (string, error) {
	v, e := s.call(ctx, request{op: "metrics"})
	if e != nil {
		return "", e
	}
	return v.(string), nil
}
func (s *Scheduler) SetDown(ctx context.Context, down bool) error {
	_, e := s.call(ctx, request{op: "down", arg: down})
	return e
}

func (s *Scheduler) SetStoreDelay(ctx context.Context, d time.Duration) error {
	_, err := s.call(ctx, request{op: "store-delay", arg: d})
	return err
}
func (s *Scheduler) SetHostAdmin(ctx context.Context, id string, a api.AdminState) error {
	_, e := s.call(ctx, request{op: "admin", id: id, arg: a})
	return e
}
func (s *Scheduler) Done() <-chan struct{} { return s.done }

type eventResult struct {
	events    []api.Event
	truncated bool
}

// EventsSince provides the retained audit log, independently of the UI tail.
func (s *Scheduler) EventsSince(ctx context.Context, afterSeq int64) ([]api.Event, bool, error) {
	v, e := s.call(ctx, request{op: "events", arg: afterSeq})
	if e != nil {
		return nil, false, e
	}
	r := v.(eventResult)
	return r.events, r.truncated, nil
}
