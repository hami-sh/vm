package scenario

import (
	"context"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/fleet"
)

// RealFleet isolates each run's clock, SQLite store, actors and fault seed.
func RealFleet(ctx context.Context, s Scenario, seed int64, speed float64) (System, func(), error) {
	cfg := config.Default()
	cfg.Racks, cfg.HostsPerRack, cfg.SlotsPerHost = s.Topology.Racks, s.Topology.Hosts, s.Topology.Slots
	c := clock.NewScaled(time.Unix(1000, 0), speed)
	started := c.Now()
	f, err := fleet.New(ctx, fleet.Options{Config: cfg, Clock: c, Seed: seed, Speed: speed})
	if err != nil {
		return nil, nil, err
	}
	// Fresh SQLite stores reject creates for HostLeaseTTL while inventory
	// recovers. Initialization is outside the scenario's relative timeline.
	tick := c.NewTicker(cfg.InvariantEvery)
	defer tick.Stop()
	for {
		snap := f.Latest()
		ready := snap.Scheduler == api.SchedulerUp && len(snap.Hosts) == cfg.Racks*cfg.HostsPerRack
		for _, h := range snap.Hosts {
			ready = ready && h.Lease == api.LeaseHealthy
		}
		if ready && c.Now().Sub(started) >= cfg.HostLeaseTTL {
			return FromFleet(f.Control, f.Chaos, f), f.Close, nil
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, nil, ctx.Err()
		case <-tick.C():
		}
	}
}
