package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/agent"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/driver"
	"github.com/hami-sh/vm/internal/fleet"
	"github.com/hami-sh/vm/internal/scheduler"
	"github.com/hami-sh/vm/internal/simnet"
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

func TestRealAgentWipedStoreEpochAndIdempotencyRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := config.Default()
	c := clock.NewFake(time.Unix(1000, 0))
	net := simnet.New(c, cfg, 73)
	defer net.Close()
	d := driver.NewFake(c, cfg, net, 73)
	defer d.Close()
	ep, err := net.Attach(api.Addr{Kind: api.KindAgent, ID: "a", Host: "a", Rack: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "state.db")
	var db *store.Store
	var s *scheduler.Scheduler
	var kill context.CancelFunc
	start := func() {
		var err error
		db, err = store.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		sctx, stop := context.WithCancel(ctx)
		kill = stop
		s, err = scheduler.New(sctx, cfg, c, net, db)
		if err != nil {
			stop()
			db.Close()
			t.Fatal(err)
		}
	}
	stop := func() {
		kill()
		<-s.Done()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	start()
	defer func() { kill(); <-s.Done(); db.Close() }()
	a := agent.New(cfg, c, ep, scheduler.Address, d)
	actx, stopAgent := context.WithCancel(ctx)
	go a.Run(actx)
	defer func() { stopAgent(); <-a.Done() }()
	// Give real actors CPU between fake-clock steps, without jumping leases
	// past goroutines that have not yet processed their last heartbeat.
	wait := func(advance bool, condition func() bool) {
		t.Helper()
		for !condition() {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			default:
			}
			if advance {
				c.Advance(cfg.InvariantEvery)
			}
			(clock.Real{}).Sleep(time.Millisecond)
		}
	}
	checkStatus := func(err error, want int) {
		t.Helper()
		var e *api.Error
		if !errors.As(err, &e) || e.Status != want || want == 503 && e.RetryAfterS < 1 {
			t.Fatalf("error %v, want status %d", err, want)
		}
	}
	wait(false, func() bool { hs, err := s.ListHosts(ctx); return err == nil && len(hs) == 1 })
	wait(true, func() bool { return !c.Now().Before(time.Unix(1000, 0).Add(cfg.HostLeaseTTL)) })
	body := api.CreateVMRequest{Pool: config.Pools[0].Name}
	created, err := s.CreateVM(ctx, "K", body)
	if err != nil {
		t.Fatal(err)
	}
	wait(true, func() bool { v, err := s.GetVM(ctx, created.ID); return err == nil && v.State == api.VMReady })
	original, err := s.GetVM(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	paused := c.Now()
	for n := 0; n < 3; n++ {
		before, _ := s.Snapshot(ctx)
		stop()
		start()
		after, err := s.Snapshot(ctx)
		if err != nil || after.Epoch != before.Epoch+1 || !c.Now().Equal(paused) {
			t.Fatalf("paused restart: before=%+v after=%+v err=%v", before, after, err)
		}
		v, err := s.GetVM(ctx, created.ID)
		if err != nil || v.Generation != original.Generation {
			t.Fatalf("generation regressed on restart: %+v %v", v, err)
		}
	}
	beforeWipe, _ := s.Snapshot(ctx)
	// Let the actual agent accept the last persisted epoch before the wipe.
	wait(true, func() bool { snap, err := a.Snapshot(ctx); return err == nil && snap.Epoch == beforeWipe.Epoch })
	stop()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(path + suffix); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	start()
	freshAt := c.Now()
	_, err = s.CreateVM(ctx, "K", body)
	checkStatus(err, 503)
	wait(true, func() bool { v, err := s.GetVM(ctx, created.ID); return err == nil && v.State == api.VMReady })
	afterWipe, _ := s.Snapshot(ctx)
	if afterWipe.Epoch <= beforeWipe.Epoch {
		t.Fatalf("agent's retained epoch not recovered: %d -> %d", beforeWipe.Epoch, afterWipe.Epoch)
	}
	boundary := freshAt.Add(cfg.HostLeaseTTL - time.Millisecond)
	for c.Now().Before(boundary) {
		c.Advance(min(cfg.InvariantEvery, boundary.Sub(c.Now())))
		(clock.Real{}).Sleep(time.Millisecond)
	}
	_, err = s.CreateVM(ctx, "K", body)
	checkStatus(err, 503)
	c.Advance(time.Millisecond)
	recovered, err := s.CreateVM(ctx, "K", body)
	if err != nil || recovered.ID != created.ID || recovered.Generation != original.Generation {
		t.Fatalf("identity lost on wipe: %+v %v", recovered, err)
	}
	_, err = s.CreateVM(ctx, "K", api.CreateVMRequest{Pool: body.Pool, Priority: "batch"})
	checkStatus(err, 422)
	second, err := s.CreateVM(ctx, "after-wipe", body)
	if err != nil || second.ID == created.ID {
		t.Fatalf("new create after wipe: %+v %v", second, err)
	}
	wait(true, func() bool { v, err := s.GetVM(ctx, second.ID); return err == nil && v.State == api.VMReady })
	actual, err := d.List(ctx)
	if err != nil || len(actual) != 2 {
		t.Fatalf("real driver inventory after recovery: %+v %v", actual, err)
	}
	for _, v := range actual {
		if !v.Running {
			t.Fatalf("agent did not accept post-wipe StartVM: %+v", v)
		}
	}
	t.Log("real agent preserved its epoch fence across paused scheduler restarts and wipe; K returned 503 before TTL, same ID at TTL, changed body 422, new StartVM reached Ready")
}
