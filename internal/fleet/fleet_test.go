package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/driver"
)

func TestRealHostAssemblyFaultsTopologyAndSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := clock.NewScaled(time.Unix(0, 0), 1)
	c.Pause()
	f, err := New(ctx, Options{Racks: 2, HostsPerRack: 2, Seed: 73, Clock: c, SchedulerFactory: newStandIn})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := f.Latest()
	if len(s.Racks) != 2 || len(s.Hosts) != 4 || s.Sim.Seed != 73 {
		t.Fatalf("initial state: %+v", s)
	}
	s.Racks[0].ID = "mutated"
	s.Hosts[0].Slots = nil
	if got := f.Latest(); got.Racks[0].ID == "mutated" || len(got.Hosts[0].Slots) == 0 {
		t.Fatal("published state aliased")
	}
	snaps, _, unsubscribe := f.Subscribe()
	defer unsubscribe()
	<-snaps
	for _, action := range []string{"kill-host", "restart-host", "pause-heartbeats", "resume-heartbeats", "clock-skew", "fill-disk", "slow-boot", "one-way-partition", "cordon", "uncordon"} {
		if _, err := f.Chaos.Do(ctx, action, api.ChaosRequest{Target: s.Hosts[0].ID}); err != nil {
			t.Fatal(action, err)
		}
	}
	for _, action := range []string{"partition-rack", "heal-rack"} {
		if _, err := f.Chaos.Do(ctx, action, api.ChaosRequest{Target: "r2"}); err != nil {
			t.Fatal(action, err)
		}
	}
	for _, action := range []string{"cordon", "uncordon", "power-off-rack"} {
		if _, err := f.Chaos.Do(ctx, action, api.ChaosRequest{Target: "r2"}); err != nil {
			t.Fatal(action, err)
		}
	}
	if _, err := f.Chaos.Do(ctx, "drain", api.ChaosRequest{Target: s.Hosts[0].ID}); err != nil {
		t.Fatal("drain host through catalogue", err)
	}
	if _, err := f.Chaos.Topology(ctx, "add-rack", api.TopologyRequest{Hosts: 1}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		c.Step(time.Second)
	}
	// SnapshotSource subscribers are non-blocking and eventually see new topology.
	for {
		select {
		case s := <-snaps:
			if len(s.Hosts) == 5 {
				for _, h := range s.Hosts {
					if h.ID == "r1-h1" && h.AdminState != api.AdminDraining {
						t.Fatalf("host drain lost: %+v", h)
					}
					if h.Rack == "r2" && len(h.Faults) == 0 {
						t.Fatalf("rack power-off not reflected in host: %+v", h)
					}
				}
				return
			}
			c.Step(time.Second)
		case <-ctx.Done():
			t.Fatal("no topology publication")
		}
	}
}

func TestSchedulerRestartAndResetDoNotDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f, err := New(ctx, Options{Racks: 1, HostsPerRack: 1, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, action := range []string{"kill-scheduler", "restart-scheduler", "wipe-store", "reset"} {
		if _, err := f.Chaos.Do(ctx, action, api.ChaosRequest{}); err != nil {
			t.Fatal(action, err)
		}
	}
}

type drainScheduler struct {
	standIn
	mu    sync.Mutex
	vms   []api.VM
	admin map[string]api.AdminState
}

func (s *drainScheduler) Snapshot(context.Context) (api.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := api.Snapshot{Scheduler: api.SchedulerUp, VMs: slices.Clone(s.vms)}
	for id, admin := range s.admin {
		out.Hosts = append(out.Hosts, api.Host{ID: id, AdminState: admin})
	}
	return out, nil
}
func (s *drainScheduler) SetHostAdmin(_ context.Context, id string, admin api.AdminState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.admin[id] = admin
	return nil
}
func (s *drainScheduler) setState(index int, state api.VMState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vms[index].State = state
}

func TestDrainCompletionWaitsForTerminationAndEmptyDriver(t *testing.T) {
	for _, mode := range []string{"host", "rack", "live-driver"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c := clock.NewScaled(time.Unix(0, 0), 1)
			c.Pause()
			cfg := config.Default()
			cfg.BootMin, cfg.BootMax = 0, 0
			scheduler := &drainScheduler{admin: map[string]api.AdminState{}, vms: []api.VM{
				{ID: "a", HostID: "r1-h1", State: api.VMReady},
				{ID: "b", HostID: "r1-h2", State: api.VMReady},
				{ID: "neighbour", HostID: "r2-h1", State: api.VMReady},
			}}
			factory := func(ctx context.Context, cfg config.Config, c clock.Clock, n api.Transport, path string) (Scheduler, func(), error) {
				_, close, err := newStandIn(ctx, cfg, c, n, path)
				return scheduler, close, err
			}
			f, err := New(ctx, Options{Config: cfg, Racks: 2, HostsPerRack: 2, Clock: c, SchedulerFactory: factory})
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			var live *driver.Fake
			if mode == "live-driver" {
				// Deliberately disagree with the scheduler: the hypervisor still
				// runs a VM, despite an empty agent slot inventory.
				v, err := f.runtime.call(ctx, func(context.Context) (any, error) { return f.runtime.hosts["r1-h1"].driver, nil })
				if err != nil {
					t.Fatal(err)
				}
				live = v.(*driver.Fake)
				if err := live.Start(ctx, api.VMSpec{DiskMB: 10}, api.BootParams{VMID: "a", Generation: 1, Host: "r1-h1", Rack: "r1"}); err != nil {
					t.Fatal(err)
				}
			}
			action, target := "drain", "r1-h1"
			if mode == "rack" {
				action, target = "remove-rack", "r1"
			}
			if _, err := f.Chaos.Do(ctx, action, api.ChaosRequest{Target: target}); err != nil {
				t.Fatal(err)
			}
			assertTopology := func(hosts, racks int) api.Snapshot {
				t.Helper()
				s, err := f.runtime.Snapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if len(s.Hosts) != hosts || len(s.Racks) != racks {
					t.Fatalf("topology: hosts=%+v racks=%+v; want %d hosts, %d racks", s.Hosts, s.Racks, hosts, racks)
				}
				for _, rack := range s.Racks {
					for _, id := range rack.Hosts {
						if !slices.ContainsFunc(s.Hosts, func(h api.Host) bool { return h.ID == id }) {
							t.Fatalf("rack retains removed host %s", id)
						}
					}
				}
				if !slices.ContainsFunc(s.Hosts, func(h api.Host) bool { return h.ID == "r2-h1" && h.AdminState == api.AdminActive }) || s.VMs[2].State != api.VMReady {
					t.Fatal("drain affected neighbouring rack")
				}
				return s
			}
			assertTopology(4, 2) // Ready must not be treated as drained.
			scheduler.setState(0, api.VMTerminating)
			assertTopology(4, 2) // Terminating is not Terminated.
			scheduler.setState(0, api.VMTerminated)
			if live != nil {
				assertTopology(4, 2) // Scheduler belief alone cannot remove a running VM.
				if err := live.Stop(ctx, "a", 1); err != nil {
					t.Fatal(err)
				}
			}
			s := assertTopology(3, 2)
			if slices.ContainsFunc(s.Hosts, func(h api.Host) bool { return h.ID == "r1-h1" }) {
				t.Fatal("terminated host was not removed")
			}
			if mode == "rack" {
				scheduler.setState(1, api.VMTerminated)
				s = assertTopology(2, 1)
				if s.Racks[0].ID != "r2" {
					t.Fatal("removed the wrong rack")
				}
			}
			// Completing the drain must release network endpoints too.
			if mode == "rack" {
				_, err = f.Chaos.Topology(ctx, "add-rack", api.TopologyRequest{Rack: "r1", Hosts: 2})
			} else {
				_, err = f.Chaos.Topology(ctx, "add-host", api.TopologyRequest{Rack: "r1", Host: "r1-h1"})
			}
			if err != nil {
				t.Fatal("cannot reattach drained capacity:", err)
			}
			assertTopology(4, 2)
			admission, err := scheduler.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(admission.Hosts, func(h api.Host) bool { return h.ID == "r1-h1" && h.AdminState == api.AdminActive }) {
				t.Fatalf("reattached host still refuses scheduler placements: %+v", admission.Hosts)
			}
		})
	}
}

type eventScheduler struct {
	standIn
	truncated bool
}

func TestRecordingSubscriptionBuffersSlowFloodAndPreservesGaps(t *testing.T) {
	ctx := t.Context()
	c := clock.NewScaled(time.Unix(0, 0), 1)
	c.Pause()
	f, err := New(ctx, Options{Clock: c, Racks: 1, HostsPerRack: 1, SchedulerFactory: newStandIn})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	snapshots, events, unsubscribe := f.Subscribe()
	defer unsubscribe()
	<-snapshots
	barrier := func() {
		t.Helper()
		c.Step(config.Default().InvariantEvery)
		at := c.Now().UnixMilli()
		timer := (clock.Real{}).NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case s := <-snapshots:
				if s.At >= at {
					return
				}
			case <-timer.C():
				t.Fatal("publisher did not drain events at sim tick")
			}
		}
	}
	// Deliberately consume no events until all synchronous calls finish. This
	// exceeds the old 64-event buffer, like a recorder busy issuing a flood.
	for range 200 {
		if _, err := f.Chaos.Do(ctx, "fill-disk", api.ChaosRequest{Target: "r1-h1"}); err != nil {
			t.Fatal(err)
		}
	}
	// A checked sim tick drains all enqueued events before publishing.
	if _, err := f.Chaos.Do(ctx, "clock-skew", api.ChaosRequest{Target: "r1-h1"}); err != nil {
		t.Fatal(err)
	}
	barrier()
	timer := (clock.Real{}).NewTimer(5 * time.Second)
	defer timer.Stop()
	var last int64
	count := 0
	for count < 201 {
		select {
		case e := <-events:
			if last != 0 && e.Seq != last+1 {
				t.Fatalf("recording dropped events: %d -> %d", last, e.Seq)
			}
			last = e.Seq
			count++
		case <-timer.C():
			t.Fatalf("slow subscriber received only %d of 201 events", count)
		}
	}
	// Deliberately exceed even the larger bounded capacity. Subsequent
	// delivered events must keep the publisher's sequence gap, not hide loss.
	for range f.runtime.cfg.MaxVMs*16 + 10 {
		if _, err := f.Chaos.Do(ctx, "fill-disk", api.ChaosRequest{Target: "r1-h1"}); err != nil {
			t.Fatal(err)
		}
	}
	barrier()
	for len(events) > 0 {
		last = (<-events).Seq
	}
	if _, err := f.Chaos.Do(ctx, "clock-skew", api.ChaosRequest{Target: "r1-h1"}); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-events:
		if e.Seq <= last+1 {
			t.Fatalf("overflow hidden: %d -> %d", last, e.Seq)
		}
	case <-timer.C():
		t.Fatal("fleet blocked on slow subscriber")
	}
}

func TestPublishedDriverEvidenceSurvivesJSONAndCallerMutation(t *testing.T) {
	h := newScenarioFleet(t)
	h.create(1)
	s := h.ready(1)
	v := s.VMs[0]
	i := slices.IndexFunc(s.Observed, func(o api.HostObservation) bool { return o.HostID == v.HostID })
	if i < 0 || !s.Observed[i].Connected || !slices.ContainsFunc(s.Observed[i].Actual, func(actual api.DriverVM) bool {
		return actual.VMID == v.ID && actual.Generation == v.Generation && actual.Running
	}) {
		t.Fatalf("missing running raw driver evidence: %+v", s.Observed)
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var decoded api.Snapshot
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Observed) != len(s.Observed) || decoded.Observed[i].Actual[0] != s.Observed[i].Actual[0] {
		t.Fatalf("observation lost in recording: %s", encoded)
	}
	s.Observed[i].HostID = "mutated"
	s.Observed[i].Actual[0].VMID = "mutated"
	if h.f.Latest().Observed[i].HostID != v.HostID || h.f.Latest().Observed[i].Actual[0].VMID != v.ID {
		t.Fatal("observed evidence aliases published snapshot")
	}
	var legacy api.Snapshot
	if err := json.Unmarshal([]byte(`{"seq":1,"at":0}`), &legacy); err != nil || legacy.Observed != nil {
		t.Fatalf("legacy recording no longer decodes without evidence: %+v, %v", legacy.Observed, err)
	}
}

type keyScheduler struct {
	drainScheduler
	next int
}

func (s *keyScheduler) Snapshot(ctx context.Context) (api.Snapshot, error) {
	out, err := s.drainScheduler.Snapshot(ctx)
	for i := range out.Hosts {
		out.Hosts[i].Lease = api.LeaseHealthy
	}
	return out, err
}

func TestAdoptedLaunchKeyCannotHideSecondLogicalVM(t *testing.T) {
	ctx := t.Context()
	c := clock.NewScaled(time.Unix(0, 0), 1)
	c.Pause()
	cfg := config.Default()
	cfg.BootMin, cfg.BootMax = 0, 0
	scheduler := &keyScheduler{drainScheduler: drainScheduler{admin: map[string]api.AdminState{}}}
	factory := func(ctx context.Context, cfg config.Config, c clock.Clock, n api.Transport, path string) (Scheduler, func(), error) {
		_, close, err := newStandIn(ctx, cfg, c, n, path)
		return scheduler, close, err
	}
	f, err := New(ctx, Options{Config: cfg, Clock: c, Racks: 1, HostsPerRack: 1, SchedulerFactory: factory})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// This VM was launched before this runtime's create ledger existed. Only
	// the driver's trusted launch identity can associate it with K.
	_, err = f.runtime.call(ctx, func(ctx context.Context) (any, error) {
		h := f.runtime.hosts["r1-h1"]
		return nil, h.driver.StartCommand(ctx, api.StartVM{VMID: "adopted", Generation: 7, IdempotencyKey: "K"}, api.BootParams{VMID: "adopted", Generation: 7, Host: h.addr.ID, Rack: h.addr.Rack})
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Control.CreateVM(ctx, "K", api.CreateVMRequest{}); err != nil {
		t.Fatal(err)
	}
	snapshots, _, unsubscribe := f.Subscribe()
	defer unsubscribe()
	<-snapshots
	// First check establishes connected running time; the second crosses its
	// heartbeat bound. Neither scheduler key ownership nor adoption can hide it.
	for _, step := range []time.Duration{cfg.InvariantEvery, cfg.HostHeartbeat + cfg.InvariantEvery} {
		c.Step(step)
		at := c.Now().UnixMilli()
		timer := (clock.Real{}).NewTimer(5 * time.Second)
		for {
			select {
			case s := <-snapshots:
				if s.At < at {
					continue
				}
				if step > cfg.HostHeartbeat && !slices.ContainsFunc(s.Invariants, func(inv api.InvariantStatus) bool { return inv.Name == api.InvIdempotencyUnique && inv.Violations > 0 }) {
					t.Fatal("driver LaunchInfo key was not checked", s.Invariants)
				}
			case <-timer.C():
				t.Fatal("missing checked snapshot")
			}
			break
		}
		timer.Stop()
	}
}

func (s *keyScheduler) CreateVM(_ context.Context, _ string, _ api.CreateVMRequest) (api.CreateVMResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	id := fmt.Sprintf("logical-%d", s.next)
	s.vms = append(s.vms, api.VM{ID: id, State: api.VMPending, Desired: api.DesiredRunning})
	return api.CreateVMResponse{ID: id}, nil
}

func TestAcceptedKeyLedgerSurvivesWipeAndAdoptedKeyMovement(t *testing.T) {
	ctx := t.Context()
	c := clock.NewScaled(time.Unix(0, 0), 1)
	c.Pause()
	scheduler := &keyScheduler{drainScheduler: drainScheduler{admin: map[string]api.AdminState{}}}
	factory := func(ctx context.Context, cfg config.Config, c clock.Clock, n api.Transport, path string) (Scheduler, func(), error) {
		_, close, err := newStandIn(ctx, cfg, c, n, path)
		return scheduler, close, err
	}
	f, err := New(ctx, Options{Clock: c, Racks: 1, HostsPerRack: 1, SchedulerFactory: factory})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	old, err := f.Control.CreateVM(ctx, "K", api.CreateVMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	scheduler.mu.Lock()
	scheduler.vms = nil
	scheduler.mu.Unlock()
	if _, err := f.Chaos.Do(ctx, "wipe-store", api.ChaosRequest{}); err != nil {
		t.Fatal(err)
	}
	retry, err := f.Control.CreateVM(ctx, "K", api.CreateVMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// The broken scheduler adopts the former key owner alongside its retry.
	// The runtime ledger must remember both, regardless of the current claim.
	scheduler.mu.Lock()
	scheduler.vms = append(scheduler.vms, api.VM{ID: old.ID, State: api.VMReady, Desired: api.DesiredRunning})
	scheduler.mu.Unlock()
	snaps, _, unsubscribe := f.Subscribe()
	defer unsubscribe()
	<-snaps
	c.Step(config.Default().InvariantEvery)
	timer := (clock.Real{}).NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case s := <-snaps:
			for _, inv := range s.Invariants {
				if inv.Name == api.InvIdempotencyUnique && inv.Violations > 0 {
					if old.ID == retry.ID {
						t.Fatal("stand-in failed to reproduce duplicate logical IDs")
					}
					return
				}
			}
		case <-timer.C():
			t.Fatal("adoption/key movement hid duplicate idempotency key")
		}
	}
}

func TestSimulationChangesPublishWhilePausedWithoutExtraChecks(t *testing.T) {
	ctx := t.Context()
	cfg := config.Default()
	c := clock.NewScaled(time.Unix(0, 0), 1)
	c.Pause()
	factory := func(ctx context.Context, cfg config.Config, c clock.Clock, n api.Transport, path string) (Scheduler, func(), error) {
		_, close, err := newStandIn(ctx, cfg, c, n, path)
		// Every checker read would report a retention gap. Repeated wall-clock
		// publications must not keep incrementing that violation while paused.
		return eventScheduler{truncated: true}, close, err
	}
	f, err := New(ctx, Options{Config: cfg, Racks: 1, HostsPerRack: 1, Clock: c, SchedulerFactory: factory})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	snapshots, _, unsubscribe := f.Subscribe()
	defer unsubscribe()
	<-snapshots
	await := func(matches func(api.Snapshot) bool) api.Snapshot {
		t.Helper()
		timer := (clock.Real{}).NewTimer(cfg.SSEEvery)
		defer timer.Stop()
		for {
			select {
			case s := <-snapshots:
				if matches(s) {
					return s
				}
			case <-timer.C():
				t.Fatal("simulation change not published within SSEEvery")
			}
		}
	}
	change := func(action string, request api.SimRequest) {
		t.Helper()
		if _, err := f.Chaos.Sim(ctx, action, request); err != nil {
			t.Fatal(err)
		}
	}
	change("play", api.SimRequest{})
	await(func(s api.Snapshot) bool { return !s.Sim.Paused })
	change("pause", api.SimRequest{})
	paused := await(func(s api.Snapshot) bool { return s.Sim.Paused })
	change("speed", api.SimRequest{Speed: 3})
	await(func(s api.Snapshot) bool { return s.Sim.Speed == 3 && s.Sim.Paused })
	intensity := .25
	change("chaos", api.SimRequest{Intensity: &intensity})
	await(func(s api.Snapshot) bool { return s.Sim.Intensity == intensity })
	change("step", api.SimRequest{StepMs: 1})
	stepped := await(func(s api.Snapshot) bool { return s.At == paused.At+1 && s.Sim.Paused })
	baseline := slices.Clone(stepped.Invariants)
	wallBudget := (clock.Real{}).NewTimer(3 * cfg.SSEEvery)
	defer wallBudget.Stop()
	for range 2 {
		// Two subsequent updates with no Sim requests prove the paused wall
		// ticker publishes, rather than only the explicit change notification.
		select {
		case stepped = <-snapshots:
		case <-wallBudget.C():
			t.Fatal("paused wall-clock publications stopped")
		}
		if stepped.At != paused.At+1 || !slices.Equal(stepped.Invariants, baseline) {
			t.Fatalf("wall publication advanced simulation or repeated checker: %+v", stepped)
		}
	}
	change("play", api.SimRequest{})
	await(func(s api.Snapshot) bool { return !s.Sim.Paused })
}

func (s eventScheduler) Snapshot(context.Context) (api.Snapshot, error) {
	return api.Snapshot{Scheduler: api.SchedulerUp}, nil
}
func (s eventScheduler) EventsSince(_ context.Context, after int64) ([]api.Event, bool, error) {
	var events []api.Event
	for seq := after + 1; seq <= 60; seq++ {
		e := api.Event{Seq: seq, Kind: api.EventTransition, To: api.VMPending}
		if seq == 1 {
			e.From = api.VMReady
		}
		events = append(events, e)
	}
	return events, s.truncated, nil
}
func TestCheckerReadsCompleteEventLogAndRejectsTruncation(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		t.Run(map[bool]string{false: "illegal event outside recent window", true: "retention gap"}[truncated], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			factory := func(ctx context.Context, cfg config.Config, c clock.Clock, n api.Transport, path string) (Scheduler, func(), error) {
				_, close, err := newStandIn(ctx, cfg, c, n, path)
				return eventScheduler{truncated: truncated}, close, err
			}
			c := clock.NewScaled(time.Unix(0, 0), 1)
			c.Pause()
			f, err := New(ctx, Options{Racks: 1, HostsPerRack: 1, Clock: c, SchedulerFactory: factory})
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			s := f.Latest()
			violations := 0
			for _, v := range s.Invariants {
				if v.Name == api.InvLegalTransitions {
					violations = v.Violations
				}
			}
			want := 1
			if truncated {
				want = 2
			}
			if violations != want {
				t.Fatalf("got %d violations, want %d", violations, want)
			}
			if len(s.Recent) != config.Default().RecentEventLimit {
				t.Fatalf("UI recent cap changed: %d", len(s.Recent))
			}
		})
	}
}
