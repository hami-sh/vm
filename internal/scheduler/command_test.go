package scheduler

import (
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
)

func TestTimerRetriesUnadmittedStartUntilAdmissionOrDelete(t *testing.T) {
	for _, finish := range []string{"admitted", "deleted", "suspect"} {
		t.Run(finish, func(t *testing.T) {
			h := newHarness(t)
			h.register("a", "r1", 1)
			v := h.create("K", "standard")
			original := h.s.state.reservations[v.ID].command
			h.c.Advance(h.s.cfg.CommandRetryEvery - time.Millisecond)
			for _, d := range h.step(tick{}).sends {
				if _, ok := d.msg.(api.StartVM); ok {
					t.Fatal("start retried before interval")
				}
			}
			// An unrelated host advances the epoch without acknowledging this
			// boot. Retries preserve its token/generation but use the new epoch.
			h.agent("b", "r2", api.RegisterHost{HostID: "b", Rack: "r2", Slots: 1, Epoch: original.Epoch + 4})
			h.c.Advance(time.Millisecond)
			want := original
			want.Epoch = original.Epoch + 5
			found := false
			for _, d := range h.step(tick{}).sends {
				if command, ok := d.msg.(api.StartVM); ok {
					if command != want {
						t.Fatalf("retry changed boot identity: %+v want %+v", command, want)
					}
					found = true
				}
			}
			if !found {
				t.Fatal("no timer-driven StartVM")
			}
			switch finish {
			case "admitted":
				h.agent("a", "r1", api.StartVMResult{HostID: "a", VMID: v.ID, Generation: v.Generation, Admitted: true})
			case "deleted":
				h.step(request{op: "delete", id: v.ID})
			case "suspect":
				h.c.Advance(h.s.cfg.HostLeaseTTL)
			}
			h.c.Advance(h.s.cfg.CommandRetryEvery)
			for _, d := range h.step(tick{}).sends {
				if _, ok := d.msg.(api.StartVM); ok {
					t.Fatalf("start revived after %s", finish)
				}
			}
		})
	}
}

func TestTimerRetriesExactStoppedCopyUntilConfirmation(t *testing.T) {
	for _, kind := range []string{"deleted", "older-generation"} {
		for _, confirmation := range []string{"report", "inventory"} {
			t.Run(kind+"/"+confirmation, func(t *testing.T) {
				h := newHarness(t)
				h.register("a", "r1", 1)
				v := h.create("K", "standard")
				h.ready(v)
				var initial Effect
				var inventory []api.InventoryItem
				if kind == "deleted" {
					initial = h.step(request{op: "delete", id: v.ID})
				} else {
					h.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: v.ID, Generation: v.Generation, Reason: api.ReasonGuestLease})
					current := h.s.state.data.VMs[v.ID].VM
					h.ready(current)
					inventory = []api.InventoryItem{{VMID: v.ID, Generation: current.Generation, State: api.VMReady}}
					initial = h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 1, Inventory: append(inventory, api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMReady})})
				}
				var original api.StopVM
				for _, d := range initial.sends {
					if stop, ok := d.msg.(api.StopVM); ok {
						original = stop
					}
				}
				h.agent("b", "r2", api.RegisterHost{HostID: "b", Rack: "r2", Slots: 1, Epoch: original.Epoch + 4})
				h.c.Advance(h.s.cfg.CommandRetryEvery)
				want := original
				want.Epoch = original.Epoch + 5
				found := false
				for _, d := range h.step(tick{}).sends {
					if stop, ok := d.msg.(api.StopVM); ok {
						if stop != want {
							t.Fatalf("wrong stopped copy: %+v want %+v", stop, want)
						}
						found = true
					}
				}
				if !found {
					t.Fatal("no timer-driven StopVM")
				}
				// Neither the wrong host nor another generation acknowledges
				// this exact stopped copy.
				h.agent("b", "r2", api.StoppedReport{HostID: "b", VMID: v.ID, Generation: v.Generation})
				h.agent("a", "r1", api.StoppedReport{HostID: "a", VMID: v.ID, Generation: 0})
				h.c.Advance(h.s.cfg.CommandRetryEvery)
				if !hasStop(h.step(tick{}), v.ID, v.Generation) {
					t.Fatal("wrong identity cleared retry")
				}
				h.c.Advance(h.s.cfg.HostLeaseTTL)
				if hasStop(h.step(tick{}), v.ID, v.Generation) {
					t.Fatal("retried stop on suspect host")
				}
				h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 2, Inventory: append(inventory, api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMReady})})
				if confirmation == "report" {
					h.agent("a", "r1", api.StoppedReport{HostID: "a", VMID: v.ID, Generation: v.Generation})
				} else {
					h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 3, Inventory: inventory})
				}
				h.c.Advance(h.s.cfg.CommandRetryEvery)
				if hasStop(h.step(tick{}), v.ID, v.Generation) {
					t.Fatal("confirmed stop revived")
				}
			})
		}
	}
}

func TestTimerDoesNotStopAnAdoptedCurrentCopy(t *testing.T) {
	h := newHarness(t)
	v := h.create("K", "standard")
	// A report ahead of host registration is fenced, but subsequent inventory
	// can establish that higher generation as the authoritative current boot.
	h.agent("a", "r1", api.ReadyReport{HostID: "a", VMID: v.ID, Generation: 1})
	h.register("a", "r1", 1, api.InventoryItem{VMID: v.ID, Generation: 1, State: api.VMBooting})
	h.c.Advance(h.s.cfg.CommandRetryEvery)
	if hasStop(h.step(tick{}), v.ID, 1) {
		t.Fatal("obsolete fence retried against adopted current boot")
	}
	got := h.s.state.data.VMs[v.ID].VM
	if got.State != api.VMBooting || got.Generation != 1 || got.HostID != "a" {
		t.Fatalf("test did not adopt the fenced copy: %+v", got)
	}
}
