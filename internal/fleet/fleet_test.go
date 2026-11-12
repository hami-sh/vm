package fleet

import (
	"context"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
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
