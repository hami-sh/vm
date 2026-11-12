package chaos

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
)

type controlPlane = api.ControlPlane
type backend struct {
	controlPlane
	faults  []string
	creates []api.CreateVMRequest
	attacks []string
	reset   chan struct{}
}

func (b *backend) Apply(_ context.Context, a string, q api.ChaosRequest) error {
	b.faults = append(b.faults, a+":"+q.Target)
	return nil
}
func (b *backend) Attack(_ context.Context, id, a string) error {
	b.attacks = append(b.attacks, a)
	return nil
}
func (b *backend) Topology(_ context.Context, a string, q api.TopologyRequest) error {
	b.faults = append(b.faults, a)
	return nil
}
func (b *backend) Reset(context.Context) error {
	b.faults = nil
	b.creates = nil
	b.attacks = nil
	if b.reset != nil {
		select {
		case b.reset <- struct{}{}:
		default:
		}
	}
	return nil
}
func (b *backend) Snapshot(context.Context) (api.Snapshot, error) {
	return api.Snapshot{Racks: []api.Rack{{ID: "r1"}, {ID: "r2"}, {ID: "r3"}}, Hosts: []api.Host{{ID: "h1", Rack: "r1"}, {ID: "h2", Rack: "r2"}, {ID: "h3", Rack: "r3"}}, VMs: []api.VM{{ID: "v1", Rack: "r1"}, {ID: "v2", Rack: "r2"}, {ID: "v3", Rack: "r3"}}}, nil
}
func (b *backend) ControlPlane() api.ControlPlane { return b }
func (b *backend) CreateVM(_ context.Context, _ string, q api.CreateVMRequest) (api.CreateVMResponse, error) {
	b.creates = append(b.creates, q)
	return api.CreateVMResponse{ID: "new"}, nil
}
func (b *backend) DeleteVM(context.Context, string) (api.VM, error) {
	return api.VM{State: api.VMTerminated}, nil
}

func TestEntireCatalogDispatches(t *testing.T) {
	ctx := context.Background()
	if len(catalogue) != 26 || len(presets) != 6 {
		t.Fatal("catalogue is incomplete")
	}
	for _, a := range append(append(append([]api.ChaosAction{}, catalogue...), recoveryActions...), presets...) {
		t.Run(a.Name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			c := clock.NewScaled(time.Unix(0, 0), 1)
			c.Pause()
			b := &backend{}
			e := New(ctx, config.Default(), c, b, 42, nil)
			defer func() { cancel(); e.Close() }()
			if _, err := e.Do(ctx, a.Name, api.ChaosRequest{Target: "v1"}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestSeedFiltersAndLoad(t *testing.T) {
	run := func() ([]string, []api.CreateVMRequest) {
		ctx := context.Background()
		cfg := config.Default()
		c := clock.NewScaled(time.Unix(0, 0), 1)
		c.Pause()
		b := &backend{}
		s := &state{cfg: cfg, clock: c, wall: clock.NewFake(time.Unix(0, 0)), backend: b, mix: cfg.LoadPriorityMix, previous: c.Now()}
		one := 1.0
		rate := 2.5
		if err := s.sim("seed", api.SimRequest{Seed: 17}); err != nil {
			t.Fatal(err)
		}
		if err := s.sim("chaos", api.SimRequest{Intensity: &one, Filters: []string{"kill-host", "wipe-store"}, Racks: []string{"r2"}, LoadRate: &rate}); err != nil {
			t.Fatal(err)
		}
		if err := s.do(ctx, c.Now(), "compromise", api.ChaosRequest{Target: "v1"}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 1000; i++ {
			c.Step(cfg.AmbientEvery)
			s.tick(ctx, c.Now())
		}
		return b.faults, b.creates
	}
	faults, creates := run()
	f2, c2 := run()
	if !reflect.DeepEqual(faults, f2) || !reflect.DeepEqual(creates, c2) {
		t.Fatal("seed did not reproduce decisions")
	}
	if len(creates) != 250 {
		t.Fatalf("2.5/s for 100s made %d", len(creates))
	}
	if len(faults) < 2 {
		t.Fatal("ambient never fired")
	}
	for _, f := range faults {
		if f != "kill-host:h2" && f != "restart-host:h2" && !strings.HasPrefix(f, "compromise:") {
			t.Fatal("filter escaped", f)
		}
	}
}

func TestExplicitZeroSeedAndSnapshotOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := clock.NewScaled(time.Unix(0, 0), 1)
	c.Pause()
	e := New(ctx, config.Default(), c, &backend{}, 42, nil)
	defer func() { cancel(); e.Close() }()
	s, err := e.Sim(ctx, "seed", api.SimRequest{Seed: 0})
	if err != nil || s.Seed != 0 {
		t.Fatal(s, err)
	}
	s, err = e.Sim(ctx, "chaos", api.SimRequest{Seed: 99, Racks: []string{"r2"}, Filters: []string{"kill-host"}})
	if err != nil || s.Seed != 0 {
		t.Fatal("settings implicitly reseeded", s, err)
	}
	s.Racks[0] = "r1"
	s.Filters[0] = "wipe-store"
	s, err = e.Status(ctx)
	if err != nil || s.Racks[0] != "r2" || s.Filters[0] != "kill-host" {
		t.Fatal("status aliases actor state", s, err)
	}
}

func TestIdleResetUsesWallTimeWhilePaused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c := clock.NewScaled(time.Unix(0, 0), 1)
	c.Pause()
	cfg := config.Default()
	cfg.IdleResetAfter = 20 * time.Millisecond
	b := &backend{reset: make(chan struct{}, 1)}
	e := New(ctx, cfg, c, b, 42, nil)
	defer func() { cancel(); e.Close() }()
	select {
	case <-b.reset:
	case <-ctx.Done():
		t.Fatal("paused simulation disabled idle reset")
	}
	status, err := e.Status(ctx)
	if err != nil || status.Seed != 42 || !status.Paused {
		t.Fatal(status, err)
	}
}

type ambientCall struct {
	at     time.Time
	action string
	req    api.ChaosRequest
}

// The engine's simulation controls are orthogonal to these manually driven
// ticks; keep Fake as the actual source of all deadlines.
type ambientClock struct{ *clock.Fake }

func (*ambientClock) Speed() float64   { return 1 }
func (*ambientClock) Paused() bool     { return true }
func (*ambientClock) SetSpeed(float64) { panic("unused simulation control") }
func (*ambientClock) Pause()           { panic("unused simulation control") }
func (*ambientClock) Play()            { panic("unused simulation control") }

// An independent small fleet: faults change physical availability, and rack
// removal completes only after its drain grace, not when requested.
type ambientBackend struct {
	backend
	c       clock.Clock
	cfg     config.Config
	faults  map[string]bool
	calls   []ambientCall
	drainAt time.Time
	removed bool
}

func (b *ambientBackend) Apply(_ context.Context, action string, q api.ChaosRequest) error {
	b.calls = append(b.calls, ambientCall{b.c.Now(), action, q})
	clear := func(a string) { delete(b.faults, a+":"+q.Target) }
	switch action {
	case "restart-scheduler":
		clear("kill-scheduler")
	case "restart-host":
		for _, a := range []string{"kill-host", "one-way-partition", "pause-heartbeats", "fill-disk", "slow-boot"} {
			clear(a)
		}
	case "heal-rack":
		clear("partition-rack")
	case "resume-heartbeats":
		clear("pause-heartbeats")
	case "uncordon":
		clear("cordon")
	case "power-off-rack":
		for _, host := range []string{"h1", "h2"} {
			b.faults["kill-host:"+host] = true
		}
	default:
		if len(q.Params) > 0 {
			clear(action)
		} else {
			b.faults[action+":"+q.Target] = true
		}
	}
	return nil
}
func (b *ambientBackend) Topology(_ context.Context, action string, q api.TopologyRequest) error {
	b.calls = append(b.calls, ambientCall{b.c.Now(), action, api.ChaosRequest{Target: q.Rack, Params: map[string]float64{"hosts": float64(q.Hosts)}}})
	if action == "remove-rack" {
		b.drainAt = b.c.Now().Add(b.cfg.DrainGrace)
		b.faults["remove-rack:"+q.Rack] = true
	} else if action == "add-rack" {
		if !b.removed {
			return invalid("rack still draining")
		}
		if q.Rack != "r1" || q.Hosts != 2 {
			return invalid("rack identity or capacity changed")
		}
		b.removed, b.drainAt = false, time.Time{}
		delete(b.faults, "remove-rack:"+q.Rack)
	}
	return nil
}
func (b *ambientBackend) Snapshot(context.Context) (api.Snapshot, error) {
	if !b.drainAt.IsZero() && !b.c.Now().Before(b.drainAt) {
		b.removed = true
	}
	s := api.Snapshot{Scheduler: api.SchedulerUp}
	if b.faults["kill-scheduler:"] {
		s.Scheduler = api.SchedulerDown
	}
	if !b.removed {
		s.Racks = []api.Rack{{ID: "r1", Hosts: []string{"h1", "h2"}, Partitioned: b.faults["partition-rack:r1"]}}
		for _, id := range []string{"h1", "h2"} {
			h := api.Host{ID: id, Rack: "r1", Lease: api.LeaseHealthy, AdminState: api.AdminActive}
			if b.faults["kill-host:"+id] || b.faults["one-way-partition:"+id] || b.faults["pause-heartbeats:"+id] || s.Racks[0].Partitioned {
				h.Lease = api.LeaseLost
			}
			if b.faults["cordon:"+id] {
				h.AdminState = api.AdminCordoned
			}
			s.Hosts = append(s.Hosts, h)
		}
	}
	return s, nil
}

func TestAmbientFaultsHealWithoutStacking(t *testing.T) {
	for _, test := range []struct {
		fault, applied, heal string
		params               map[string]float64
	}{
		{"kill-scheduler", "kill-scheduler", "restart-scheduler", nil},
		{"kill-host", "kill-host", "restart-host", nil},
		{"partition-rack", "partition-rack", "heal-rack", nil},
		{"pause-heartbeats", "pause-heartbeats", "resume-heartbeats", nil},
		{"one-way-partition", "one-way-partition", "restart-host", nil},
		{"fill-disk", "fill-disk", "fill-disk", map[string]float64{"on": 0}},
		{"cordon", "cordon", "uncordon", nil},
		{"slow-store", "slow-store", "slow-store", map[string]float64{"ms": 0}},
		{"latency-spike", "latency-spike", "latency-spike", map[string]float64{"ms": 0}},
		{"packet-loss", "packet-loss", "packet-loss", map[string]float64{"percent": 0}},
		{"duplicate-messages", "duplicate-messages", "duplicate-messages", map[string]float64{"percent": 0}},
		{"reorder-messages", "reorder-messages", "reorder-messages", map[string]float64{"on": 0}},
		{"slow-boot", "slow-boot", "slow-boot", map[string]float64{"factor": 1}},
		{"clock-skew", "clock-skew", "clock-skew", map[string]float64{"ms": 0}},
		{"remove-rack", "remove-rack", "add-rack", map[string]float64{"hosts": 2}},
		{"burst-after-rack-loss", "power-off-rack", "restart-host", nil},
	} {
		t.Run(test.fault, func(t *testing.T) {
			ctx := t.Context()
			cfg := config.Default()
			cfg.ChaosBaseRates = map[string]float64{test.fault: 1e9}
			c := &ambientClock{clock.NewFake(time.Unix(0, 0))}
			b := &ambientBackend{c: c, cfg: cfg, faults: map[string]bool{}}
			var events []api.Event
			s := &state{cfg: cfg, clock: c, wall: c, backend: b, emit: func(e api.Event) { events = append(events, e) }, previous: c.Now(), mix: cfg.LoadPriorityMix}
			if err := s.sim("seed", api.SimRequest{Seed: 17}); err != nil {
				t.Fatal(err)
			}
			one := 1.0
			if err := s.sim("chaos", api.SimRequest{Intensity: &one, Filters: []string{test.fault}}); err != nil {
				t.Fatal(err)
			}
			advance := func(d time.Duration) {
				for elapsed := time.Duration(0); elapsed < d; elapsed += cfg.AmbientEvery {
					c.Advance(cfg.AmbientEvery)
					s.tick(ctx, c.Now())
				}
			}
			advance(120 * time.Second)
			zero := 0.0
			if err := s.sim("chaos", api.SimRequest{Intensity: &zero}); err != nil {
				t.Fatal(err)
			}
			advance(cfg.DrainGrace + cfg.AmbientHealAfter)
			faults := 0
			last := map[string]time.Time{}
			for i, call := range b.calls {
				if call.action != test.applied || (test.applied == test.heal && len(call.req.Params) > 0) {
					continue
				}
				faults++
				bound := cfg.AmbientHealAfter
				if test.fault == "remove-rack" {
					bound = max(bound, cfg.DrainGrace)
				}
				if prev, ok := last[call.req.Target]; ok && call.at.Sub(prev) < bound {
					t.Fatal("stacked ambient fault", call)
				}
				last[call.req.Target] = call.at
				matched := map[string]bool{}
				for _, heal := range b.calls[i+1:] {
					if heal.at.Sub(call.at) > bound {
						break
					}
					if heal.action == test.heal && reflect.DeepEqual(heal.req.Params, test.params) && (heal.req.Target == call.req.Target || test.fault == "burst-after-rack-loss") {
						matched[heal.req.Target] = true
					}
				}
				want := 1
				if test.fault == "burst-after-rack-loss" {
					want = 2
				}
				if len(matched) != want {
					t.Fatalf("unhealed %s at %s: got %v", test.fault, call.at, matched)
				}
			}
			if faults < 2 {
				t.Fatal("regression did not exercise recurring ambient churn", b.calls)
			}
			if len(b.faults) != 0 {
				t.Fatal("ambient left permanent faults", b.faults)
			}
			snap, _ := b.Snapshot(ctx)
			if snap.Scheduler != api.SchedulerUp || len(snap.Hosts) != 2 || len(snap.Racks) != 1 || snap.Racks[0].Partitioned || slices.ContainsFunc(snap.Hosts, func(h api.Host) bool { return h.Lease != api.LeaseHealthy || h.AdminState != api.AdminActive }) {
				t.Fatal("fleet did not recover", snap)
			}
			scheduled, healed := 0, 0
			for _, e := range events {
				if e.Kind == api.EventChaos && strings.HasPrefix(e.Detail, "ambient heal scheduled ") {
					scheduled++
				}
				if e.Kind == api.EventChaos && strings.HasPrefix(e.Detail, "ambient healed ") {
					healed++
				}
			}
			expected := faults
			if test.fault == "burst-after-rack-loss" {
				expected *= 2
			}
			if scheduled != expected || healed != expected {
				t.Fatalf("recovery events: scheduled=%d healed=%d expected=%d", scheduled, healed, expected)
			}
		})
	}
}

func TestManualAndScriptedFaultsDoNotAutoHeal(t *testing.T) {
	for _, scripted := range []bool{false, true} {
		cfg := config.Default()
		c := &ambientClock{clock.NewFake(time.Unix(0, 0))}
		b := &ambientBackend{c: c, cfg: cfg, faults: map[string]bool{}}
		s := &state{cfg: cfg, clock: c, wall: c, backend: b, previous: c.Now()}
		if scripted {
			s.tasks = []scheduled{{at: c.Now(), action: "kill-scheduler"}}
		} else if err := s.do(t.Context(), c.Now(), "kill-scheduler", api.ChaosRequest{}); err != nil {
			t.Fatal(err)
		}
		s.tick(t.Context(), c.Now())
		c.Advance(2 * cfg.AmbientHealAfter)
		s.tick(t.Context(), c.Now())
		if !b.faults["kill-scheduler:"] || len(b.calls) != 1 {
			t.Fatal("manual/scripted action acquired an automatic heal", b.calls)
		}
	}
}
