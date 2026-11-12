package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
)

func TestQuarantinedStopCannotBecomeReplacement(t *testing.T) {
	for _, state := range []api.VMState{api.VMReady, api.VMUnhealthy} {
		t.Run(string(state), func(t *testing.T) {
			h := newHarness(t)
			h.register("a", "r1", 2)
			h.register("b", "r2", 2)
			v := h.create("K", "interactive")
			h.ready(v)
			if state == api.VMUnhealthy {
				r := h.s.state.data.VMs[v.ID]
				r.VM.State = state
				h.s.state.data.VMs[v.ID] = r
			}
			// SecurityReport is lost, but the release completion still arrives.
			h.agent(v.HostID, v.Rack, api.StoppedReport{HostID: v.HostID, VMID: v.ID, Generation: v.Generation, Reason: api.ReasonQuarantined})
			h.agent(v.HostID, v.Rack, api.HostHeartbeat{HostID: v.HostID, Seq: 1})
			got := h.s.state.data.VMs[v.ID].VM
			if got.State != api.VMFailed || got.Generation != v.Generation || got.LastReason != api.ReasonQuarantined {
				t.Fatalf("quarantine replaced: %+v", got)
			}
		})
	}
}

func TestGuestLeaseWithRecentBlockedAttackQuarantines(t *testing.T) {
	for _, source := range []string{"heartbeat", "report"} {
		for _, control := range []string{"recent", "expired", "older-generation", "other-reason"} {
			t.Run(source+"/"+control, func(t *testing.T) {
				h := newHarness(t)
				h.register("a", "r1", 1)
				v := h.create("K", "standard")
				h.ready(v)
				generation := v.Generation
				if control == "older-generation" {
					generation--
				}
				h.agent("a", "r1", api.SecurityReport{HostID: "a", VMID: v.ID, Generation: generation, Attack: api.AttackReplay})
				age := h.s.cfg.QuarantineWindow - time.Millisecond
				if control == "expired" {
					age = h.s.cfg.QuarantineWindow
				}
				h.c.Advance(age)
				reason := api.ReasonGuestLease
				if control == "other-reason" {
					reason = "driver_exit"
				}
				var e Effect
				if source == "heartbeat" && control != "other-reason" {
					e = h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 1, Inventory: []api.InventoryItem{{VMID: v.ID, Generation: v.Generation, State: api.VMUnhealthy}}})
				} else {
					e = h.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: reason})
				}
				got := h.s.state.data.VMs[v.ID].VM
				if control == "recent" {
					if got.State != api.VMFailed || got.LastReason != api.ReasonQuarantined || got.Generation != v.Generation || !hasStop(e, v.ID, v.Generation) {
						t.Fatalf("recent blocked attacker recycled: %+v", got)
					}
				} else if got.State != api.VMBooting || got.Generation != v.Generation+1 {
					t.Fatalf("unrelated or expired evidence poisoned recycle: %+v", got)
				}
			})
		}
	}
}

func TestQuarantineTombstoneSurvivesLostReports(t *testing.T) {
	for _, initial := range []api.VMState{api.VMReady, api.VMBooting, api.VMUnhealthy, api.VMTerminating} {
		t.Run(string(initial), func(t *testing.T) {
			h := newHarness(t)
			h.register("a", "r1", 1)
			h.register("b", "r2", 1)
			v := h.create("K", "interactive")
			if initial != api.VMBooting {
				h.ready(v)
			}
			if initial == api.VMUnhealthy {
				r := h.s.state.data.VMs[v.ID]
				r.VM.State = initial
				h.s.state.data.VMs[v.ID] = r
			}
			if initial == api.VMTerminating {
				h.step(request{op: "delete", id: v.ID})
			}
			item := api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMFailed}
			// Neither SecurityReport nor StoppedReport is delivered. Also lose
			// the first ack: the tombstone must be safe to repeat.
			for seq := uint64(1); seq <= 2; seq++ {
				e := h.agent(v.HostID, v.Rack, api.HostHeartbeat{HostID: v.HostID, Seq: seq, Inventory: []api.InventoryItem{item}})
				acked := false
				for _, d := range e.sends {
					if ack, ok := d.msg.(api.HeartbeatAck); ok {
						for _, stale := range ack.Stale {
							acked = acked || stale.VMID == v.ID && stale.Generation == v.Generation
						}
					}
				}
				if !acked {
					t.Fatal("quarantine tombstone not acknowledged as stale")
				}
			}
			// A tombstone is already-clean capacity, even before its ack
			// reaches the agent. It must not block unrelated placement.
			h.step(request{op: "admin", id: "b", arg: api.AdminCordoned})
			other := h.create("other", "standard")
			if other.HostID != v.HostID {
				t.Fatalf("tombstone retained occupied capacity: %+v", other)
			}
			// Once the ack arrives the agent forgets only the tombstone.
			h.agent(v.HostID, v.Rack, api.HostHeartbeat{HostID: v.HostID, Seq: 3, Inventory: []api.InventoryItem{{VMID: other.ID, Generation: other.Generation, State: api.VMBooting}}})
			got := h.s.state.data.VMs[v.ID].VM
			wantState, wantReason := api.VMFailed, api.ReasonQuarantined
			if initial == api.VMTerminating {
				wantState, wantReason = api.VMTerminated, api.ReasonDeleted
			}
			if got.State != wantState || got.LastReason != wantReason || got.Generation != v.Generation {
				t.Fatalf("quarantined VM replaced or delete resurrected: %+v", got)
			}
			loaded, _, err := h.db.Start(context.Background())
			if err != nil || loaded.VMs[v.ID].VM.State != wantState {
				t.Fatalf("quarantine result not durable: %+v %v", loaded.VMs[v.ID], err)
			}
		})
	}
}

func TestOlderQuarantineTombstoneFailsNewGeneration(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 2)
	v := h.create("K", "standard")
	h.ready(v)
	h.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: api.ReasonGuestLease})
	newVM := h.s.state.data.VMs[v.ID].VM
	h.ready(newVM)
	old := api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMFailed}
	e := h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 1, Inventory: []api.InventoryItem{old, {VMID: v.ID, Generation: newVM.Generation, State: api.VMReady}}})
	if got := h.s.state.data.VMs[v.ID].VM; got.State != api.VMFailed || got.LastReason != api.ReasonQuarantined || got.Generation != newVM.Generation {
		t.Fatalf("old quarantine did not fail logical VM: %+v", got)
	}
	acked := false
	for _, d := range e.sends {
		if ack, ok := d.msg.(api.HeartbeatAck); ok {
			for _, i := range ack.Stale {
				acked = acked || i == old
			}
		}
	}
	if !acked || !hasStop(e, v.ID, newVM.Generation) {
		t.Fatal("old tombstone must be acknowledged and stop the current copy")
	}
}

func TestQuarantinedRecycleRefusalIsTerminal(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 2)
	v := h.create("K", "standard")
	h.ready(v)
	// A lease-expiry report emitted before quarantine arrives before the
	// tombstone and requests an in-place recycle at the next generation.
	h.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: api.ReasonGuestLease})
	next := h.s.state.data.VMs[v.ID].VM
	if next.State != api.VMBooting || next.Generation != v.Generation+1 || next.HostID != v.HostID {
		t.Fatalf("did not exercise in-place recycle: %+v", next)
	}
	h.agent("a", "r1", api.StartVMResult{HostID: "a", VMID: v.ID, Generation: next.Generation, Reason: api.ReasonQuarantined})
	got := h.s.state.data.VMs[v.ID].VM
	if got.State != api.VMFailed || got.LastReason != api.ReasonQuarantined || got.Attempts != next.Attempts || len(h.s.state.data.Queue) != 0 {
		t.Fatalf("quarantine refusal retried or changed budget: %+v", got)
	}
	h.agent("a", "r1", api.ReadyReport{HostID: "a", VMID: v.ID, Generation: next.Generation})
	if h.s.state.data.VMs[v.ID].VM.State != api.VMFailed {
		t.Fatal("late Ready resurrected quarantined identity")
	}
}

func TestOldQuarantineEvidenceStopsReplacementHost(t *testing.T) {
	for _, evidence := range []string{"tombstone", "security", "stop"} {
		t.Run(evidence, func(t *testing.T) {
			h := newHarness(t)
			h.register("a", "r1", 1)
			h.register("b", "r2", 1)
			v := h.create("K", "standard")
			h.ready(v)
			h.c.Advance(h.s.cfg.HostLeaseTTL)
			h.step(tick{})
			h.register("b", "r2", 1)
			h.c.Advance(h.s.cfg.HostLostGrace)
			h.agent("b", "r2", api.HostHeartbeat{HostID: "b", Seq: 1})
			h.step(tick{})
			next := h.s.state.data.VMs[v.ID].VM
			if next.HostID != "b" || next.Generation != v.Generation+1 {
				t.Fatalf("did not replace onto another host: %+v", next)
			}
			h.ready(next)
			var e Effect
			switch evidence {
			case "tombstone":
				e = h.register("a", "r1", 1, api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMFailed})
			case "security":
				e = h.agent("a", "r1", api.SecurityReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Quarantined: true, Attack: api.AttackReplay})
			case "stop":
				e = h.agent("a", "r1", api.StoppedReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: api.ReasonQuarantined})
			}
			got := h.s.state.data.VMs[v.ID].VM
			if got.State != api.VMFailed || got.LastReason != api.ReasonQuarantined || got.Generation != next.Generation || len(h.s.state.data.Queue) != 0 {
				t.Fatalf("old quarantine did not fail replacement: %+v", got)
			}
			stoppedCurrent := false
			for _, d := range e.sends {
				if stop, ok := d.msg.(api.StopVM); ok && d.to.ID == "b" && stop.VMID == v.ID && stop.Generation == next.Generation && stop.Reason == api.ReasonQuarantined {
					stoppedCurrent = true
				}
			}
			if !stoppedCurrent {
				t.Fatal("did not stop current generation on its actual host")
			}
		})
	}
}

func TestLateWipeCopyCannotStealRetriedKey(t *testing.T) {
	h := newHarness(t)
	h.s.state.recoverUntil = h.c.Now().Add(h.s.cfg.HostLeaseTTL).UnixMilli()
	h.c.Advance(h.s.cfg.HostLeaseTTL)
	h.register("new", "r1", 2)
	v := h.create("K", "standard")
	h.ready(v)
	r := h.s.state.data.VMs[v.ID]
	old := api.InventoryItem{VMID: "prewipe", Generation: v.Generation + 10, State: api.VMReady, Pool: v.Pool, IdempotencyKey: "K", RequestHash: r.Hash}
	e := h.register("healed", "r2", 2, old)
	if !hasStop(e, old.VMID, old.Generation) {
		t.Fatal("late unknown copy was not stopped")
	}
	if got := h.create("K", "standard"); got.ID != v.ID {
		t.Fatalf("retry after heal changed identity: %s -> %s", v.ID, got.ID)
	}
	for id, r := range h.s.state.data.VMs {
		if id != v.ID && r.Key == "K" && r.VM.Desired == api.DesiredRunning {
			t.Fatalf("duplicate running key: %+v", r)
		}
	}
}

func TestDelayedFirstHeartbeatCannotReplaceReadyVM(t *testing.T) {
	for _, seq := range []uint64{0, 1, 5} {
		t.Run(string(rune('0'+seq)), func(t *testing.T) {
			h := newHarness(t)
			h.register("a", "r1", 2)
			v := h.create("K", "standard")
			h.ready(v)
			h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 7, Inventory: []api.InventoryItem{{VMID: v.ID, Generation: v.Generation, State: api.VMReady}}})
			h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: seq})
			if got := h.s.state.data.VMs[v.ID].VM; got.State != api.VMReady || got.Generation != v.Generation {
				t.Fatalf("stale inventory replaced VM: %+v", got)
			}
		})
	}
}

func TestCapacityRefusalsDoNotConsumeBootBudget(t *testing.T) {
	for _, reason := range []string{"full", "cordoned", "disk_full"} {
		t.Run(reason, func(t *testing.T) {
			h := newHarness(t)
			v := h.create("K", "standard")
			for n := 0; n < h.s.cfg.MaxAttempts+1; n++ {
				id := string(rune('a' + n))
				h.register(id, "r1", 2)
				h.c.Advance(h.s.cfg.HostHeartbeat)
				h.step(tick{})
				v = h.s.state.data.VMs[v.ID].VM
				if v.State != api.VMBooting {
					t.Fatalf("refusal consumed budget: %+v", v)
				}
				// Isolate the refunded state from a valid immediate retry. Each
				// iteration opens a new host; refused hosts remain unavailable.
				h.step(request{op: "admin", id: v.HostID, arg: api.AdminCordoned})
				h.agent(v.HostID, v.Rack, api.StartVMResult{HostID: v.HostID, VMID: v.ID, Generation: v.Generation, Reason: reason})
				if got := h.s.state.data.VMs[v.ID].VM; got.State != api.VMPending || got.Attempts != 0 {
					t.Fatalf("capacity refusal counted as boot: %+v", got)
				}
			}
			h.register("success", "r2", 2)
			h.c.Advance(h.s.cfg.HostHeartbeat)
			h.step(tick{})
			v = h.s.state.data.VMs[v.ID].VM
			h.ready(v)
			if got := h.s.state.data.VMs[v.ID].VM; got.State != api.VMReady || got.Attempts != 1 {
				t.Fatalf("could not boot after capacity healed: %+v", got)
			}
		})
	}
}

func TestAwaitingSchedulerAckRetriesSameHostWithoutAttempt(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 2)
	v := h.create("K", "standard")
	for n := 0; n <= h.s.cfg.MaxAttempts; n++ {
		h.agent("a", "r1", api.StartVMResult{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: "awaiting_scheduler_ack"})
		h.c.Advance(h.s.cfg.BackoffBase)
		// The normal heartbeat is acked; the host is usable again without
		// another RegisterHost or removing any host exclusion by hand.
		h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: uint64(n + 1)})
		v = h.s.state.data.VMs[v.ID].VM
		if v.State != api.VMBooting || v.HostID != "a" || v.Attempts != 1 {
			t.Fatalf("ack barrier consumed attempts or excluded host: %+v", v)
		}
	}
	h.ready(v)
	if got := h.s.state.data.VMs[v.ID].VM; got.State != api.VMReady {
		t.Fatalf("acked host did not boot: %+v", got)
	}
}

func TestFirstReadyLatencyNotRepeatedAfterRecovery(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 2)
	v := h.create("K", "interactive")
	h.c.Advance(time.Second)
	h.ready(v)
	h.c.Advance(time.Second)
	h.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: api.ReasonGuestLease})
	v = h.s.state.data.VMs[v.ID].VM
	h.ready(v)
	e := h.step(request{op: "metrics"})
	text := e.response.value.(string)
	if !strings.Contains(text, "vm_time_to_ready_seconds_count{priority=\"interactive\"} 1\n") || !strings.Contains(text, "time_to_recover_seconds_count{priority=\"interactive\"} 1\n") {
		t.Fatalf("first Ready or recovery sample wrong: %s", text)
	}
	// Persisted first-Ready history must survive a restart while recycling,
	// when the current state alone no longer establishes prior readiness.
	h.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: api.ReasonGuestLease})
	d, epoch, err := h.db.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	h.s.state = newState(d, epoch, h.c.Now(), h.s.cfg)
	v = d.VMs[v.ID].VM
	h.register("a", "r1", 2, api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMReady})
	text = h.step(request{op: "metrics"}).response.value.(string)
	if !strings.Contains(text, "vm_time_to_ready_seconds_count{priority=\"interactive\"} 0\n") {
		t.Fatalf("re-ready after restart counted as a new create: %s", text)
	}
}
