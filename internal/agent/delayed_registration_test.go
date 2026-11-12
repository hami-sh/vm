package agent_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/agent"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/driver"
	"github.com/hami-sh/vm/internal/scheduler"
	"github.com/hami-sh/vm/internal/simnet"
	"github.com/hami-sh/vm/internal/store"
)

func TestDelayedRegistrationDoesNotInvalidateInFlightStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cfg := config.Default()
	c := clock.NewFake(time.Unix(1000, 0))
	n := simnet.New(c, cfg, 37)
	defer n.Close()
	d := driver.NewFake(c, cfg, n, 42)
	defer d.Close()
	db, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := scheduler.New(ctx, cfg, c, n, db)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); <-s.Done() }()
	ep, err := n.Attach(api.Addr{Kind: api.KindAgent, ID: "h1", Host: "h1", Rack: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	a := agent.New(cfg, c, ep, scheduler.Address, d)
	go a.Run(ctx)
	defer func() { cancel(); <-a.Done() }()
	for {
		snapshot, err := s.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Hosts) == 1 {
			break
		}
		(clock.Real{}).Sleep(time.Millisecond)
	}
	// Let the fresh store's key-recovery window finish with live heartbeats.
	for elapsed := time.Duration(0); elapsed < cfg.HostLeaseTTL; elapsed += cfg.InvariantEvery {
		c.Advance(cfg.InvariantEvery)
		(clock.Real{}).Sleep(time.Millisecond)
	}
	// A duplicate of the original empty registration is already in flight
	// when placement commits. It arrives before the newer StartVM can arrive.
	n.SetLatency(500 * time.Millisecond)
	if err := ep.Send(scheduler.Address, api.PortControl, api.RegisterHost{HostID: "h1", Rack: "r1", Slots: 2}); err != nil {
		t.Fatal(err)
	}
	n.SetLatency(time.Second)
	created, err := s.CreateVM(ctx, "delayed-registration", api.CreateVMRequest{Pool: config.Pools[0].Name})
	if err != nil {
		t.Fatal(err)
	}
	c.Advance(500 * time.Millisecond)
	for {
		snapshot, err := s.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Hosts[0].LeaseAgeMs == 0 {
			v, err := s.GetVM(ctx, created.ID)
			if err != nil || v.State != api.VMBooting || v.Generation != 1 {
				t.Fatalf("pre-start registration invalidated in-flight placement: %+v %v", v, err)
			}
			break
		}
		(clock.Real{}).Sleep(time.Millisecond)
	}
	if actual, err := d.List(ctx); err != nil || len(actual) != 0 {
		t.Fatalf("StartVM arrived before the delayed registration: %+v %v", actual, err)
	}
	deadline := c.Now().Add(cfg.BootDeadline)
	for {
		v, err := s.GetVM(ctx, created.ID)
		if err != nil || v.Generation != 1 || (v.State != api.VMBooting && v.State != api.VMReady) {
			t.Fatalf("in-flight placement did not survive registration: %+v %v", v, err)
		}
		if v.State == api.VMReady {
			actual, err := d.List(ctx)
			if err != nil || len(actual) != 1 || actual[0].VMID != created.ID || actual[0].Generation != 1 || !actual[0].Running {
				t.Fatalf("Ready did not match the driver: %+v %v", actual, err)
			}
			break
		}
		if !c.Now().Before(deadline) {
			t.Fatalf("original generation did not reach Ready: %+v", v)
		}
		c.Advance(cfg.InvariantEvery)
		(clock.Real{}).Sleep(time.Millisecond)
	}
}
