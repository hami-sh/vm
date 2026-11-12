package fleet

import (
	"context"
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
		})
	}
}

type eventScheduler struct {
	standIn
	truncated bool
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
