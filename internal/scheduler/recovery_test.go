package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
)

func TestDuplicateRegistrationCannotReopenHeartbeatWindow(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	v := h.create("registration-order", "standard")
	h.ready(v)
	old := api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMReady}
	h.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: api.ReasonGuestLease})
	v = h.s.state.data.VMs[v.ID].VM
	h.ready(v)
	current := api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMReady}
	epoch := h.s.state.epoch
	h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 20, Epoch: epoch, Inventory: []api.InventoryItem{current}})
	h.c.Advance(time.Second)
	// Even a registration carrying correct inventory must not reopen the
	// sequence window for the queued, older heartbeat that follows it.
	h.agent("a", "r1", api.RegisterHost{HostID: "a", Rack: "r1", Slots: 1, Epoch: epoch, Inventory: []api.InventoryItem{current}})
	h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 19, Epoch: epoch, Inventory: []api.InventoryItem{old}})
	got := h.step(request{op: "snapshot"}).response.value.(api.Snapshot)
	if got.VMs[0].State != api.VMReady || got.VMs[0].Generation != v.Generation || got.Hosts[0].LeaseAgeMs != time.Second.Milliseconds() {
		t.Fatalf("duplicate registration accepted obsolete inventory or renewed lease: %+v", got)
	}
}

func TestLateHeartbeatDoesNotBlockClearedDiskInventory(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	// Establish a nonzero identity, as after a genuine restart handshake.
	h.register("a", "r1", 1)
	h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 20, DiskFull: true})
	v := h.create("disk-healed", "standard")
	if h.s.state.data.VMs[v.ID].VM.State != api.VMPending {
		t.Fatal("placed on a full disk")
	}
	h.c.Advance(time.Second)
	e := h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 19, DiskFull: true})
	for _, send := range e.sends {
		if ack, ok := send.msg.(api.HeartbeatAck); ok && ack.Reregister {
			t.Fatal("late heartbeat started a registration cycle for a healthy incarnation")
		}
	}
	h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 21})
	if got := h.s.state.data.VMs[v.ID].VM; got.State != api.VMBooting || got.HostID != "a" {
		t.Fatalf("newer cleared-disk inventory did not enable placement: %+v", got)
	}
}

func TestRegistrationResetRequiresExactOutstandingChallenge(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	v := h.create("challenge", "standard")
	h.ready(v)
	item := api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMReady}
	h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 20, Inventory: []api.InventoryItem{item}})
	epoch := h.s.state.epoch
	h.c.Advance(time.Second)
	e := h.agent("a", "r1", api.RegisterHost{HostID: "a", Rack: "r1", Slots: 1})
	challenge := e.sends[0].msg.(api.HeartbeatAck)
	if !challenge.Reregister || challenge.Incarnation == (api.HostIncarnation{}) || challenge.Epoch != epoch {
		t.Fatalf("missing per-host challenge or global epoch changed: %+v", challenge)
	}
	// Neither a higher pre-challenge sequence nor a heartbeat that echoes
	// the challenge substitutes for its authoritative registration.
	h.agent("a", "r1", api.RegisterHost{HostID: "a", Rack: "r1", Slots: 1, Seq: 99})
	h.step(api.Envelope{From: api.Addr{Kind: api.KindAgent, ID: "a", Host: "a", Rack: "r1"}, Port: api.PortControl, Msg: api.HostHeartbeat{HostID: "a", Seq: 100, Incarnation: challenge.Incarnation}})
	before := h.step(request{op: "snapshot"}).response.value.(api.Snapshot)
	if before.VMs[0].State != api.VMReady || before.Hosts[0].LeaseAgeMs != time.Second.Milliseconds() {
		t.Fatalf("pre-challenge report became authoritative: %+v", before)
	}
	registration := api.RegisterHost{HostID: "a", Rack: "r1", Slots: 1, Seq: 1, Incarnation: challenge.Incarnation, Inventory: []api.InventoryItem{item}}
	h.agent("a", "r1", registration)
	h.c.Advance(time.Second)
	registration.Inventory = nil
	h.agent("a", "r1", registration)
	h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 1})
	after := h.step(request{op: "snapshot"}).response.value.(api.Snapshot)
	if after.VMs[0].State != api.VMReady || after.Hosts[0].LeaseAgeMs != time.Second.Milliseconds() {
		t.Fatalf("duplicate established incarnation refreshed or reconciled: %+v", after)
	}
	// A genuinely restarted process has lost its identity and can obtain
	// another challenge, but an old proof cannot satisfy that new request.
	e = h.agent("a", "r1", api.RegisterHost{HostID: "a", Rack: "r1", Slots: 1})
	next := e.sends[0].msg.(api.HeartbeatAck)
	if next.Incarnation.Epoch != challenge.Incarnation.Epoch || next.Incarnation.Challenge <= challenge.Incarnation.Challenge {
		t.Fatalf("challenge reused: old=%+v new=%+v", challenge, next)
	}
	registration.Seq = 1000
	h.agent("a", "r1", registration)
	if h.s.state.data.VMs[v.ID].VM.State != api.VMReady {
		t.Fatal("old proof satisfied a later challenge")
	}
	registration.Incarnation = next.Incarnation
	registration.Seq = 1
	e = h.agent("a", "r1", registration)
	lost := false
	for _, event := range e.events {
		lost = lost || event.VMID == v.ID && event.To == api.VMLost
	}
	if !lost || h.s.state.epoch != epoch {
		t.Fatal("fresh restart proof did not reconcile absence, or advanced global epoch")
	}
}

func TestNewerRetainedIncarnationObtainsFreshProof(t *testing.T) {
	h := newHarness(t)
	previous := api.HostIncarnation{Epoch: 7, Challenge: 2}
	h.agent("a", "r1", api.RegisterHost{HostID: "a", Rack: "r1", Slots: 1, Epoch: 7, Seq: 20, Incarnation: previous})
	v := h.create("retained-incarnation", "standard")
	h.ready(v)
	// On scheduler restart, an old registration can arrive before the
	// agent's newer retained identity. That identity is not retired just
	// because the scheduler bootstrapped from the earlier packet.
	newer := api.HostIncarnation{Epoch: 7, Challenge: 3}
	e := h.agent("a", "r1", api.RegisterHost{HostID: "a", Rack: "r1", Slots: 1, Epoch: 7, Seq: 30, Incarnation: newer})
	if len(e.sends) != 1 {
		t.Fatalf("newer retained identity stranded without a challenge: %+v", e.sends)
	}
	challenge := e.sends[0].msg.(api.HeartbeatAck)
	if !challenge.Reregister || !challenge.Incarnation.After(newer) {
		t.Fatalf("challenge reused a previously observed identity: %+v", challenge)
	}
	item := api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMReady}
	h.agent("a", "r1", api.RegisterHost{HostID: "a", Rack: "r1", Slots: 1, Epoch: challenge.Epoch, Seq: 1, Incarnation: challenge.Incarnation, Inventory: []api.InventoryItem{item}})
	got := h.step(request{op: "snapshot"}).response.value.(api.Snapshot)
	if got.VMs[0].State != api.VMReady || got.VMs[0].Generation != v.Generation || got.Hosts[0].Lease != api.LeaseHealthy {
		t.Fatalf("fresh proof did not recover retained identity: %+v", got)
	}
}

func TestRegistrationAbsencePreservesUnadmittedBoot(t *testing.T) {
	for _, state := range []string{"unadmitted", "admitted", "ready", "unhealthy"} {
		t.Run(state, func(t *testing.T) {
			h := newHarness(t)
			h.register("a", "r1", 1)
			v := h.create("K", "standard")
			original := h.s.state.reservations[v.ID]
			if state != "unadmitted" {
				h.agent("a", "r1", api.StartVMResult{VMID: v.ID, Generation: v.Generation, HostID: "a", Admitted: true})
			}
			if state == "ready" || state == "unhealthy" {
				h.ready(v)
			}
			if state == "unhealthy" {
				r := h.s.state.data.VMs[v.ID]
				r.VM.State = api.VMUnhealthy
				h.s.state.data.VMs[v.ID] = r
			}
			// Registration was captured before StartVM arrived, but reaches
			// the scheduler after the placement was committed and sent.
			h.c.Advance(400 * time.Millisecond)
			registration := h.register("a", "r1", 1)
			got := h.s.state.data.VMs[v.ID].VM
			if state != "unadmitted" {
				lost := false
				for _, event := range registration.events {
					lost = lost || event.VMID == v.ID && event.To == api.VMLost && event.Detail == "missing from inventory"
				}
				if !lost {
					t.Fatalf("authoritative absence ignored: %+v", got)
				}
				return
			}
			if got.State != api.VMBooting || got.Generation != v.Generation || got.Attempts != v.Attempts || h.s.state.reservations[v.ID] != original {
				t.Fatalf("in-flight boot abandoned or deadline reset: %+v", got)
			}
			e := h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 1})
			resent := false
			for _, d := range e.sends {
				if command, ok := d.msg.(api.StartVM); ok && command == original.command {
					resent = true
				}
			}
			if !resent {
				t.Fatal("missing inventory did not resend original StartVM")
			}
			h.ready(v)
			if got := h.s.state.data.VMs[v.ID].VM; got.State != api.VMReady || got.Generation != v.Generation {
				t.Fatalf("delayed boot did not complete: %+v", got)
			}
		})
	}
}

func TestRepeatedInFlightRecoveryPlacementsSurviveRegistration(t *testing.T) {
	h := newHarness(t)
	h.s.cfg.BackoffBase = 0
	h.register("a", "r1", 1)
	v := h.create("K", "standard")
	h.ready(v)
	h.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: api.ReasonGuestLease})
	for attempt := 1; attempt <= 3; attempt++ {
		boot := h.s.state.data.VMs[v.ID].VM
		if boot.State != api.VMBooting || boot.Generation != int64(attempt+1) || boot.Attempts != attempt {
			t.Fatalf("unexpected recovery placement: %+v", boot)
		}
		h.c.Advance(400 * time.Millisecond)
		h.register("a", "r1", 1)
		if got := h.s.state.data.VMs[v.ID].VM; got.State != api.VMBooting || got.Generation != boot.Generation || got.Attempts != attempt {
			t.Fatalf("registration consumed recovery budget: %+v", got)
		}
		if attempt < 3 {
			h.agent("a", "r1", api.StartVMResult{HostID: "a", VMID: v.ID, Generation: boot.Generation, Admitted: true})
			h.agent("a", "r1", api.StoppedReport{HostID: "a", VMID: v.ID, Generation: boot.Generation})
		} else {
			h.ready(boot)
		}
	}
	if got := h.s.state.data.VMs[v.ID].VM; got.State != api.VMReady || got.Generation != 4 {
		t.Fatalf("recovery did not finish: %+v", got)
	}
}

func TestHealedHostRestoresReplacementBeforeDemand(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	h.register("b", "r2", 1)
	v := h.create("recovery", "batch")
	h.ready(v)
	other := h.create("occupied", "standard")
	h.ready(other)
	demand := h.create("queued", "interactive")
	h.c.Advance(h.s.cfg.HostLeaseTTL + h.s.cfg.HostLostGrace)
	h.agent("b", "r2", api.HostHeartbeat{HostID: "b", Seq: 1, Inventory: []api.InventoryItem{{VMID: other.ID, Generation: other.Generation, State: api.VMReady}}})
	h.step(tick{})
	if got := h.s.state.data.Queue[v.ID]; got.Reason != api.QueueReplace {
		t.Fatalf("host loss did not queue replacement: %+v", got)
	}
	h.register("a", "r1", 1)
	got := h.s.state.data.VMs[v.ID].VM
	if got.State != api.VMBooting || got.HostID != "a" || got.Generation != v.Generation+1 || h.s.state.data.VMs[demand.ID].VM.State != api.VMPending {
		t.Fatalf("healed slot stolen: recovery=%+v demand=%+v", got, h.s.state.data.VMs[demand.ID].VM)
	}
}

func TestHealedHostNoLongerAvoidedAfterLoss(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	v := h.create("recovery", "batch")
	h.ready(v)
	h.c.Advance(h.s.cfg.HostLeaseTTL + h.s.cfg.HostLostGrace)
	h.step(tick{})
	// Keep the replacement queued across recovery, then expose two equally
	// suitable hosts. The healed host must participate in normal spreading
	// again, rather than remaining only a last-resort fallback.
	q := h.s.state.data.Queue[v.ID]
	q.NotBefore = h.c.Now().Add(time.Second).UnixMilli()
	h.s.state.data.Queue[v.ID] = q
	h.register("a", "r1", 1)
	h.register("b", "r1", 1)
	demand := h.create("queued", "interactive")
	h.c.Advance(time.Second)
	h.step(tick{})
	got := h.s.state.data.VMs[v.ID].VM
	if got.HostID != "a" || got.Generation != v.Generation+1 || got.State != api.VMBooting {
		t.Fatalf("healed host still avoided: %+v", got)
	}
	if got := h.s.state.data.VMs[demand.ID].VM; got.State != api.VMBooting || got.HostID != "b" {
		t.Fatalf("demand did not follow replacement: %+v", got)
	}
}

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
