package scheduler_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/fleet"
	"github.com/hami-sh/vm/internal/scheduler"
	"github.com/hami-sh/vm/internal/store"
)

// Fault only the scheduler's receive path; agents, drivers, attacker, SQLite
// and simnet remain real. The second case models a pre-quarantine lease report
// delayed until quarantine begins, before the first tombstone can arrive.
type quarantineReportLoss struct {
	api.Transport
	ctx                          context.Context
	delayedLease                 bool
	security, stopped, tombstone atomic.Int64
	refused                      atomic.Int64
}

type quarantineLossEndpoint struct {
	api.Endpoint
	inbox <-chan api.Envelope
}

func (e quarantineLossEndpoint) Inbox() <-chan api.Envelope { return e.inbox }

func (n *quarantineReportLoss) Attach(addr api.Addr) (api.Endpoint, error) {
	ep, err := n.Transport.Attach(addr)
	if err != nil {
		return nil, err
	}
	inbox := make(chan api.Envelope)
	go func() {
		defer close(inbox)
		for {
			var e api.Envelope
			select {
			case <-n.ctx.Done():
				return
			case next, ok := <-ep.Inbox():
				if !ok {
					return
				}
				e = next
			}
			switch m := e.Msg.(type) {
			case api.SecurityReport:
				if !m.Quarantined {
					continue
				}
				n.security.Add(1)
				if !n.delayedLease {
					continue
				}
				// Inject the delayed report at the actual quarantine boundary,
				// retaining the real authenticated host's envelope identity.
				e.Msg = api.UnhealthyReport{HostID: m.HostID, VMID: m.VMID, Generation: m.Generation, Reason: api.ReasonGuestLease}
			case api.StoppedReport:
				if m.Reason == api.ReasonQuarantined {
					n.stopped.Add(1)
					continue
				}
			case api.StartVMResult:
				if !m.Admitted && m.Reason == api.ReasonQuarantined {
					n.refused.Add(1)
				}
			case api.HostHeartbeat:
				for _, i := range m.Inventory {
					if i.State == api.VMFailed {
						n.tombstone.Add(1)
					}
				}
			}
			select {
			case inbox <- e:
			case <-n.ctx.Done():
				return
			}
		}
	}()
	return quarantineLossEndpoint{ep, inbox}, nil
}

func TestRealFleetQuarantineWithBothReportsLost(t *testing.T) {
	for _, delayedLease := range []bool{false, true} {
		name := "reports_lost"
		if delayedLease {
			name = "reports_lost_delayed_guest_lease"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cfg := config.Default()
			c := clock.NewScaled(time.Unix(1000, 0), 1)
			c.Pause()
			var control *scheduler.Scheduler
			loss := &quarantineReportLoss{delayedLease: delayedLease}
			f, err := fleet.New(ctx, fleet.Options{Config: cfg, Clock: c, Seed: 1, StorePath: filepath.Join(t.TempDir(), "fleet.db"), SchedulerFactory: func(ctx context.Context, cfg config.Config, c clock.Clock, transport api.Transport, path string) (fleet.Scheduler, func(), error) {
				loss.Transport, loss.ctx = transport, ctx
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
			create := func(key string) api.CreateVMResponse {
				t.Helper()
				var v api.CreateVMResponse
				wait(2*cfg.HostLeaseTTL, "create "+key, func() bool {
					v, err = f.Control.CreateVM(ctx, key, api.CreateVMRequest{Pool: config.Pools[0].Name})
					var unavailable *api.Error
					if err != nil && (!errors.As(err, &unavailable) || unavailable.Status != 503) {
						t.Fatal(err)
					}
					return err == nil
				})
				return v
			}
			victim, neighbour := create("victim"), create("neighbour")
			wait(cfg.BootDeadline, "initial readiness", func() bool {
				v, _ := f.Control.GetVM(ctx, victim.ID)
				n, _ := f.Control.GetVM(ctx, neighbour.ID)
				return v.State == api.VMReady && n.State == api.VMReady
			})
			if _, err := f.Chaos.Do(ctx, "compromise", api.ChaosRequest{Target: victim.ID}); err != nil {
				t.Fatal(err)
			}
			wait(cfg.QuarantineWindow, "quarantine after both reports lost", func() bool {
				v, _ := f.Control.GetVM(ctx, victim.ID)
				return v.State == api.VMFailed
			})
			// Keep running past a whole boot deadline: an already queued
			// recycle must not quietly bring the victim back to Ready later.
			until := c.Now().Add(cfg.BootDeadline)
			wait(cfg.BootDeadline+cfg.InvariantEvery, "post-quarantine observation", func() bool { return !c.Now().Before(until) })
			v, err := f.Control.GetVM(ctx, victim.ID)
			n, neighbourErr := f.Control.GetVM(ctx, neighbour.ID)
			if err != nil || neighbourErr != nil || v.State != api.VMFailed || v.LastReason != api.ReasonQuarantined || n.State != api.VMReady || n.Generation != neighbour.Generation {
				t.Fatalf("quarantine escaped or neighbour changed: victim=%+v neighbour=%+v errors=%v/%v", v, n, err, neighbourErr)
			}
			if loss.security.Load() == 0 || loss.stopped.Load() == 0 || loss.tombstone.Load() == 0 {
				t.Fatalf("did not exercise real quarantine loss: security=%d stopped=%d tombstones=%d", loss.security.Load(), loss.stopped.Load(), loss.tombstone.Load())
			}
			if delayedLease && loss.refused.Load() == 0 {
				t.Fatal("did not exercise real agent refusal of quarantined gen2 recycle")
			}
			events, truncated, err := control.EventsSince(ctx, 0)
			if err != nil || truncated {
				t.Fatalf("incomplete transition evidence: %v truncated=%v", err, truncated)
			}
			for _, event := range events {
				if event.VMID == victim.ID && event.To == api.VMReady && event.Generation > victim.Generation {
					t.Fatalf("quarantined identity reached Ready again: %+v", event)
				}
			}
			t.Logf("victim Failed/quarantined gen=%d, neighbour Ready gen=%d; dropped security=%d stopped=%d; observed tombstones=%d refused=%d", v.Generation, n.Generation, loss.security.Load(), loss.stopped.Load(), loss.tombstone.Load(), loss.refused.Load())
		})
	}
}
