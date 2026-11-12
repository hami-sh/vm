package agent_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
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

type lostLeaseReports struct {
	api.Endpoint
	dropped, unhealthy atomic.Int32
}

func (e *lostLeaseReports) Send(to api.Addr, port api.Port, msg any) error {
	switch m := msg.(type) {
	case api.UnhealthyReport:
		if m.Reason == api.ReasonGuestLease {
			e.dropped.Add(1)
			return nil
		}
	case api.HostHeartbeat:
		for _, item := range m.Inventory {
			if item.State == api.VMUnhealthy {
				e.unhealthy.Add(1)
			}
		}
	}
	return e.Endpoint.Send(to, port, msg)
}

func TestLostLeaseReportRecyclesWithoutCapacityGap(t *testing.T) {
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
	loss := &lostLeaseReports{Endpoint: ep}
	a := agent.New(cfg, c, loss, scheduler.Address, d)
	go a.Run(ctx)
	defer func() { cancel(); <-a.Done() }()
	wait := func(within time.Duration, check func() bool) {
		t.Helper()
		deadline := c.Now().Add(within)
		for !check() {
			if !c.Now().Before(deadline) || ctx.Err() != nil {
				vms, _ := s.ListVMs(ctx)
				t.Fatalf("recovery did not converge: %+v", vms)
			}
			c.Advance(cfg.InvariantEvery)
			(clock.Real{}).Sleep(time.Millisecond)
		}
	}
	create := func(key string) api.CreateVMResponse {
		t.Helper()
		var response api.CreateVMResponse
		wait(2*cfg.HostLeaseTTL, func() bool {
			response, err = s.CreateVM(ctx, key, api.CreateVMRequest{Pool: config.Pools[0].Name})
			var unavailable *api.Error
			if err != nil && (!errors.As(err, &unavailable) || unavailable.Status != 503) {
				t.Fatal(err)
			}
			return err == nil
		})
		return response
	}
	victim, neighbour := create("victim"), create("neighbour")
	wait(cfg.BootDeadline, func() bool {
		v, _ := s.GetVM(ctx, victim.ID)
		b, _ := s.GetVM(ctx, neighbour.ID)
		return v.State == api.VMReady && b.State == api.VMReady
	})
	demand := create("new-demand")
	if demand.State != api.VMPending {
		t.Fatal("test did not saturate the host")
	}
	d.KillGuest(victim.ID)
	wait(cfg.GuestLeaseTTL+cfg.BootDeadline, func() bool {
		v, _ := s.GetVM(ctx, victim.ID)
		other, _ := s.GetVM(ctx, demand.ID)
		if other.State != api.VMPending {
			t.Fatalf("lost lease report freed recovery's slot for demand: victim=%+v demand=%+v", v, other)
		}
		return v.State == api.VMReady && v.Generation == 2 && v.HostID == "h1"
	})
	if loss.dropped.Load() == 0 || loss.unhealthy.Load() == 0 {
		t.Fatalf("recovery did not use inventory after report loss: dropped=%d unhealthy heartbeats=%d", loss.dropped.Load(), loss.unhealthy.Load())
	}
	actual, err := d.List(ctx)
	if err != nil || len(actual) != 2 {
		t.Fatalf("driver capacity after recovery: %+v %v", actual, err)
	}
	for _, v := range actual {
		if v.VMID == victim.ID && (v.Generation != 2 || !v.Running) {
			t.Fatalf("old generation still running after recovery: %+v", actual)
		}
	}
}
