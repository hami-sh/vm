package scheduler_test

import (
	"context"
	"path/filepath"
	"sync"
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

type stopDeliveryLoss struct {
	api.Transport
	mu         sync.Mutex
	armed      bool
	stops      []api.StopVM
	heartbeats []api.HostHeartbeat
}

type stopLossEndpoint struct {
	api.Endpoint
	loss *stopDeliveryLoss
}

func (n *stopDeliveryLoss) Attach(addr api.Addr) (api.Endpoint, error) {
	ep, err := n.Transport.Attach(addr)
	return stopLossEndpoint{ep, n}, err
}

func (e stopLossEndpoint) Send(to api.Addr, port api.Port, msg any) error {
	e.loss.mu.Lock()
	drop := false
	if e.loss.armed {
		switch m := msg.(type) {
		case api.StopVM:
			e.loss.stops = append(e.loss.stops, m)
			drop = len(e.loss.stops) == 1
		case api.HostHeartbeat:
			// Retain the next inventory until after the two-second assertion.
			e.loss.heartbeats = append(e.loss.heartbeats, m)
			drop = true
		}
	}
	e.loss.mu.Unlock()
	if drop {
		return nil
	}
	return e.Endpoint.Send(to, port, msg)
}

func TestRealAgentDeleteRetriesStopWithoutHeartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cfg := config.Default()
	c := clock.NewFake(time.Unix(1000, 0))
	n := simnet.New(c, cfg, 17)
	defer n.Close()
	loss := &stopDeliveryLoss{Transport: n}
	d := driver.NewFake(c, cfg, n, 19)
	defer d.Close()
	db, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := scheduler.New(ctx, cfg, c, loss, db)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); <-s.Done() }()
	ep, err := loss.Attach(api.Addr{Kind: api.KindAgent, ID: "a", Host: "a", Rack: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	a := agent.New(cfg, c, ep, scheduler.Address, d)
	go a.Run(ctx)
	defer func() { cancel(); <-a.Done() }()
	advance := func() {
		c.Advance(cfg.SnapshotEvery)
		(clock.Real{}).Sleep(time.Millisecond)
	}
	for elapsed := time.Duration(0); elapsed < 2*cfg.HostLeaseTTL; elapsed += cfg.SnapshotEvery {
		advance()
	}
	created, err := s.CreateVM(ctx, "delete", api.CreateVMRequest{Pool: config.Pools[0].Name})
	if err != nil {
		t.Fatal(err)
	}
	for deadline := c.Now().Add(cfg.BootDeadline); ; advance() {
		v, err := s.GetVM(ctx, created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if v.State == api.VMReady {
			break
		}
		if !c.Now().Before(deadline) {
			t.Fatalf("initial boot: %+v", v)
		}
	}
	// Pause while arming the fault. Subsequent heartbeat inventories are
	// retained by the transport rather than reaching reconciliation.
	if err := a.PauseHeartbeats(ctx, true); err != nil {
		t.Fatal(err)
	}
	_, err = a.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n.SetLatency(time.Second)
	loss.mu.Lock()
	loss.armed = true
	loss.mu.Unlock()
	deletedAt := c.Now()
	deleted, err := s.DeleteVM(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.PauseHeartbeats(ctx, false); err != nil {
		t.Fatal(err)
	}
	for c.Now().Before(deletedAt.Add(cfg.HostHeartbeat)) {
		advance()
	}
	// The driver and host ticker run on different goroutines. Observe the
	// withheld heartbeat explicitly instead of assuming a wall-clock yield
	// has already let the agent consume its due ticker under race-suite load.
	for {
		loss.mu.Lock()
		held := len(loss.heartbeats)
		loss.mu.Unlock()
		if held > 0 {
			break
		}
		if _, err := a.Snapshot(ctx); err != nil {
			t.Fatal(err)
		}
		(clock.Real{}).Sleep(time.Millisecond)
	}
	// Let due delivery, release and driver shutdown finish at this exact
	// simulated deadline; do not grant additional simulated time.
	for {
		actual, err := d.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(actual) == 0 {
			break
		}
		loss.mu.Lock()
		stops := append([]api.StopVM(nil), loss.stops...)
		loss.mu.Unlock()
		if len(stops) < 2 || ctx.Err() != nil {
			t.Fatalf("copy still running two seconds after delete; stops=%+v actual=%+v", stops, actual)
		}
		(clock.Real{}).Sleep(time.Millisecond)
	}
	loss.mu.Lock()
	stops := append([]api.StopVM(nil), loss.stops...)
	held := append([]api.HostHeartbeat(nil), loss.heartbeats...)
	loss.armed = false
	loss.mu.Unlock()
	if len(stops) < 2 || stops[0] != stops[1] || stops[0].VMID != created.ID || stops[0].Generation != deleted.Generation {
		t.Fatalf("stop retry changed identity: %+v", stops)
	}
	if len(held) == 0 {
		t.Fatal("test did not delay a heartbeat")
	}
	for _, hb := range held {
		if err := ep.Send(scheduler.Address, api.PortControl, hb); err != nil {
			t.Fatal(err)
		}
	}
}
