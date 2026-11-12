package scheduler

import (
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
)

func TestRecycleReservationRetryKeepsRecoveryPriorityAndBudget(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	v := h.create("previously-ready", "batch")
	h.ready(v)
	demand := h.create("new-demand", "interactive")
	h.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: api.ReasonGuestLease})
	recycle := h.s.state.data.VMs[v.ID].VM
	// All gen2 StartVM sends are lost, including retries. The old copy is
	// fenced by its next inventory report and its disk is then clean.
	e := h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 1, Inventory: []api.InventoryItem{{VMID: v.ID, Generation: v.Generation, State: api.VMUnhealthy}}})
	fenced := false
	for _, event := range e.events {
		if event.Kind == api.EventFencing && event.VMID == v.ID {
			fenced = true
			if event.Generation != v.Generation {
				t.Errorf("fencing event reports intended rather than fenced generation: %+v", event)
			}
		}
	}
	if !fenced {
		t.Fatal("missing old-generation fencing event")
	}
	h.agent("a", "r1", api.StoppedReport{HostID: "a", VMID: v.ID, Generation: v.Generation})
	h.c.Advance(h.s.cfg.ReservationTTL)
	h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 2})
	h.step(tick{})
	q := h.s.state.data.Queue[v.ID]
	got := h.s.state.data.VMs[v.ID].VM
	if q.Reason != api.QueueReplace || q.Priority != api.PriorityReplacement || got.Attempts != recycle.Attempts {
		t.Fatalf("recovery demoted or retry budget reset: queue=%+v VM=%+v", q, got)
	}
	// When the expired reservation's cleanup frees the slot, recovery must
	// take it before the queued interactive request, without another wait.
	h.agent("a", "r1", api.StoppedReport{HostID: "a", VMID: v.ID, Generation: recycle.Generation})
	got = h.s.state.data.VMs[v.ID].VM
	if got.State != api.VMBooting || got.Generation != recycle.Generation+1 || got.Attempts != recycle.Attempts+1 || h.s.state.data.VMs[demand.ID].VM.State != api.VMPending {
		t.Fatalf("new demand stole recycle slot or attempts reset: recovery=%+v demand=%+v", got, h.s.state.data.VMs[demand.ID].VM)
	}
}

func TestRecoveryBootRetryBudgetRemainsBounded(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	v := h.create("K", "standard")
	h.ready(v)
	h.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: api.ReasonGuestLease})
	for attempt := 1; attempt <= h.s.cfg.MaxAttempts; attempt++ {
		v = h.s.state.data.VMs[v.ID].VM
		h.agent("a", "r1", api.StartVMResult{HostID: "a", VMID: v.ID, Generation: v.Generation, Admitted: true})
		h.c.Advance(h.s.cfg.BootDeadline)
		h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: uint64(2*attempt - 1), Inventory: []api.InventoryItem{{VMID: v.ID, Generation: v.Generation, State: api.VMBooting}}})
		h.step(tick{})
		got := h.s.state.data.VMs[v.ID].VM
		if got.Attempts != attempt {
			t.Fatalf("genuine boot budget reset: %+v", got)
		}
		if attempt == h.s.cfg.MaxAttempts {
			if got.State != api.VMFailed || got.LastReason != api.ReasonRetryBudget {
				t.Fatalf("recovery boot loop was unbounded: %+v", got)
			}
		} else {
			if q := h.s.state.data.Queue[v.ID]; q.Priority != api.PriorityReplacement || q.Reason != api.QueueReplace {
				t.Fatalf("failed recovery lost class 0: %+v", q)
			}
			h.agent("a", "r1", api.StoppedReport{HostID: "a", VMID: v.ID, Generation: v.Generation})
			h.c.Advance(h.s.cfg.BackoffMax)
			h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: uint64(2 * attempt)})
			h.step(tick{})
		}
	}
}

func TestRecoveryBackoffBlocksLowerClassPlacement(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	v := h.create("recovery", "batch")
	h.ready(v)
	demand := h.create("already-queued", "interactive")
	h.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: api.ReasonGuestLease})
	v = h.s.state.data.VMs[v.ID].VM
	h.agent("a", "r1", api.StartVMResult{HostID: "a", VMID: v.ID, Generation: v.Generation, Admitted: true})
	h.c.Advance(h.s.cfg.BootDeadline)
	h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 1, Inventory: []api.InventoryItem{{VMID: v.ID, Generation: v.Generation, State: api.VMBooting}}})
	h.step(tick{})
	// Set a valid persisted backoff deterministically rather than depending
	// on the full-jitter draw. No slot is free until cleanup completes below.
	q := h.s.state.data.Queue[v.ID]
	q.NotBefore = h.c.Now().Add(h.s.cfg.BackoffMax).UnixMilli()
	h.s.state.data.Queue[v.ID] = q
	h.agent("a", "r1", api.StoppedReport{HostID: "a", VMID: v.ID, Generation: v.Generation})
	if got := h.s.state.data.VMs[demand.ID].VM; got.State != api.VMPending {
		t.Fatalf("queued demand bypassed backing-off recovery: %+v", got)
	}
	newDemand := h.create("new-arrival", "interactive")
	if newDemand.State != api.VMPending {
		t.Fatalf("direct create bypassed backing-off recovery: %+v", newDemand)
	}
	h.c.Advance(h.s.cfg.BackoffMax - time.Millisecond)
	h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 2})
	h.step(tick{})
	if got := h.s.state.data.VMs[v.ID].VM; got.State != api.VMPending {
		t.Fatalf("recovery ignored its backoff: %+v", got)
	}
	h.c.Advance(time.Millisecond)
	h.step(tick{})
	got := h.s.state.data.VMs[v.ID].VM
	if got.State != api.VMBooting || got.Generation != v.Generation+1 {
		t.Fatalf("eligible recovery did not receive capacity first: %+v", got)
	}
	for _, id := range []string{demand.ID, newDemand.ID} {
		if got := h.s.state.data.VMs[id].VM; got.State != api.VMPending {
			t.Fatalf("ordinary work took recovery capacity: %+v", got)
		}
	}
}
