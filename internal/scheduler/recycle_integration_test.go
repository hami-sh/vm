package scheduler_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/fleet"
	"github.com/hami-sh/vm/internal/scheduler"
	"github.com/hami-sh/vm/internal/store"
)

type recycleStartLoss struct {
	api.Transport
	mu       sync.Mutex
	commands []api.StartVM
}

type recycleLossEndpoint struct {
	api.Endpoint
	loss *recycleStartLoss
}

func (n *recycleStartLoss) Attach(addr api.Addr) (api.Endpoint, error) {
	ep, err := n.Transport.Attach(addr)
	return recycleLossEndpoint{ep, n}, err
}

func (e recycleLossEndpoint) Send(to api.Addr, port api.Port, msg any) error {
	if start, ok := msg.(api.StartVM); ok && start.Generation == 2 {
		e.loss.mu.Lock()
		e.loss.commands = append(e.loss.commands, start)
		drop := len(e.loss.commands) == 1
		e.loss.mu.Unlock()
		if drop {
			return nil
		}
	}
	return e.Endpoint.Send(to, port, msg)
}

func TestRealFleetRecycleResendsIdenticalStartWithoutLosingSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cfg := config.Default()
	c := clock.NewScaled(time.Unix(1000, 0), 1)
	c.Pause()
	var control *scheduler.Scheduler
	loss := &recycleStartLoss{}
	f, err := fleet.New(ctx, fleet.Options{Config: cfg, Clock: c, Seed: 3, Racks: 1, HostsPerRack: 1, StorePath: filepath.Join(t.TempDir(), "fleet.db"), SchedulerFactory: func(ctx context.Context, cfg config.Config, c clock.Clock, transport api.Transport, path string) (fleet.Scheduler, func(), error) {
		loss.Transport = transport
		db, err := store.Open(path)
		if err != nil {
			return nil, nil, err
		}
		s, err := scheduler.New(ctx, cfg, c, loss, db)
		if err != nil {
			db.Close()
			return nil, nil, err
		}
		control = s
		return s, func() { <-s.Done(); db.Close() }, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	wait := func(within time.Duration, detail string, done func() bool) {
		t.Helper()
		until := c.Now().Add(within)
		for !done() {
			if !c.Now().Before(until) || ctx.Err() != nil {
				vms, _ := control.ListVMs(ctx)
				events, _, _ := control.EventsSince(ctx, 0)
				t.Fatalf("%s: VMs=%+v events=%+v", detail, vms, events)
			}
			c.Step(cfg.InvariantEvery)
			(clock.Real{}).Sleep(time.Millisecond)
		}
	}
	create := func(key, priority string) api.CreateVMResponse {
		t.Helper()
		var v api.CreateVMResponse
		wait(2*cfg.HostLeaseTTL, "create "+key, func() bool {
			v, err = f.Control.CreateVM(ctx, key, api.CreateVMRequest{Pool: config.Pools[0].Name, Priority: priority})
			var unavailable *api.Error
			if err != nil && (!errors.As(err, &unavailable) || unavailable.Status != 503) {
				t.Fatal(err)
			}
			return err == nil
		})
		return v
	}
	victim, neighbour := create("victim", "batch"), create("neighbour", "standard")
	wait(cfg.BootDeadline, "initial readiness", func() bool {
		v, _ := f.Control.GetVM(ctx, victim.ID)
		n, _ := f.Control.GetVM(ctx, neighbour.ID)
		return v.State == api.VMReady && n.State == api.VMReady
	})
	original, _ := f.Control.GetVM(ctx, victim.ID)
	demand := create("new-demand", "interactive")
	if demand.State != api.VMPending {
		t.Fatal("test did not saturate both real slots")
	}
	if _, err := f.Chaos.Do(ctx, "kill-guestd", api.ChaosRequest{Target: victim.ID}); err != nil {
		t.Fatal(err)
	}
	wait(cfg.GuestLeaseTTL+cfg.ReservationTTL+cfg.BootDeadline, "lost recycle command recovery", func() bool {
		v, _ := f.Control.GetVM(ctx, victim.ID)
		n, _ := f.Control.GetVM(ctx, demand.ID)
		if n.State != api.VMPending {
			t.Fatalf("new request stole previously Ready VM's slot: recovery=%+v demand=%+v", v, n)
		}
		queue, err := f.Control.Queue(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queue {
			if q.VMID == victim.ID && (q.Priority != api.PriorityReplacement || q.Reason != api.QueueReplace) {
				t.Fatalf("recycle demoted to ordinary demand: %+v", q)
			}
		}
		return v.State == api.VMReady && v.Generation == original.Generation+1
	})
	got, _ := f.Control.GetVM(ctx, victim.ID)
	if got.HostID != original.HostID || got.Generation != 2 {
		t.Fatalf("recycle lost its host or skipped generation: %+v", got)
	}
	loss.mu.Lock()
	commands := append([]api.StartVM(nil), loss.commands...)
	loss.mu.Unlock()
	if len(commands) < 2 {
		t.Fatal("lost StartVM was never resent")
	}
	for _, command := range commands[1:] {
		if command != commands[0] {
			t.Fatal("retry changed generation, token, epoch or request identity")
		}
	}
	events, truncated, err := control.EventsSince(ctx, 0)
	if err != nil || truncated {
		t.Fatalf("incomplete events: %v truncated=%v", err, truncated)
	}
	for _, event := range events {
		if event.VMID == victim.ID && event.Kind == api.EventFencing && event.Generation != original.Generation {
			t.Fatalf("fencing logged intended rather than fenced generation: %+v", event)
		}
	}
	t.Logf("dropped initial gen2 StartVM; %d identical sends; victim Ready gen2 on %s, new demand Pending", len(commands), got.HostID)
}
