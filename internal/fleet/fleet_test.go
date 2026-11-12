package fleet

import (
	"context"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
)

func TestRealHostAssemblyFaultsTopologyAndSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := clock.NewScaled(time.Unix(0, 0), 1)
	c.Pause()
	f, err := New(ctx, Options{Racks: 2, HostsPerRack: 2, Seed: 73, Clock: c})
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
