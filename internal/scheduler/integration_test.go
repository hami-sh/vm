package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/fleet"
	"github.com/hami-sh/vm/internal/scheduler"
	"github.com/hami-sh/vm/internal/store"
)

func TestRealAgentFleetCapacityAndRackSpreading(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := config.Default()
	c := clock.NewScaled(time.Unix(1000, 0), 1)
	f, err := fleet.New(ctx, fleet.Options{Config: cfg, Clock: c, Seed: 73, StorePath: filepath.Join(t.TempDir(), "fleet.db"), SchedulerFactory: func(ctx context.Context, cfg config.Config, c clock.Clock, transport api.Transport, path string) (fleet.Scheduler, func(), error) {
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
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// Poll using the injected clock, including the fresh-store recovery window.
	ticker := c.NewTicker(cfg.InvariantEvery)
	defer ticker.Stop()
	for n := 0; n < 30; {
		_, err := f.Control.CreateVM(ctx, fmt.Sprint(n), api.CreateVMRequest{Pool: config.Pools[n%2].Name})
		if err == nil {
			n++
			continue
		}
		var unavailable *api.Error
		if !errors.As(err, &unavailable) || unavailable.Status != 503 {
			t.Fatal(err)
		}
		select {
		case <-ticker.C():
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	for {
		s := f.Latest()
		if s.Counters.ByState[api.VMReady] == 24 {
			if len(s.Queue) != 6 || len(s.VMs) != 30 {
				t.Fatalf("capacity result: %d VMs, %d queued", len(s.VMs), len(s.Queue))
			}
			counts := map[string]int{}
			for _, v := range s.VMs {
				if v.State == api.VMReady {
					counts[v.Pool+v.Rack]++
				}
			}
			for _, p := range config.Pools {
				for r := 1; r <= 3; r++ {
					if counts[p.Name+fmt.Sprint("r", r)] != 4 {
						t.Fatalf("per-pool rack imbalance: %v", counts)
					}
				}
			}
			t.Log("30 creates -> 24 Ready, 6 queued; each pool has 4 Ready per rack through real agents and simnet")
			return
		}
		select {
		case <-ticker.C():
		case <-ctx.Done():
			t.Fatalf("capacity timed out: states=%v queue=%d", s.Counters.ByState, len(s.Queue))
		}
	}
}
