package invariant

import (
	"testing"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

func TestEachInvariantDetectsBrokenState(t *testing.T) {
	for _, name := range api.AllInvariants {
		t.Run(string(name), func(t *testing.T) {
			cfg := config.Default()
			c := New(cfg)
			s := api.Snapshot{At: 1000, Scheduler: api.SchedulerUp, VMs: []api.VM{{ID: "v", State: api.VMReady, Generation: 2}}, Hosts: []api.Host{{ID: "h", SlotCount: 1, Lease: api.LeaseHealthy, AdminState: api.AdminActive, Slots: []api.Slot{{State: api.SlotOccupied, VMID: "v", Generation: 2}}}}}
			raw := []Observation{{Host: s.Hosts[0], Connected: true, Actual: []api.DriverVM{{VMID: "v", Generation: 2, Running: true}}}}
			if out := c.Check(s, raw...); len(out) != 0 {
				t.Fatalf("healthy state: %v", out)
			}
			switch name {
			case api.InvOneAuthoritative:
				raw = append(raw, raw[0])
				c.Check(s, raw...)
				s.At += cfg.HostHeartbeat.Milliseconds() + 1
			case api.InvStaleStopped:
				raw[0].Actual[0].Generation = 1
				raw[0].Host.Slots[0].Generation = 1
				c.Check(s, raw...)
				s.At += cfg.HostHeartbeat.Milliseconds() + 1
			case api.InvSlotCapacity:
				raw[0].Host.Slots = append(raw[0].Host.Slots, api.Slot{State: api.SlotReleasing, VMID: "other"})
			case api.InvGenMonotonic:
				s.VMs[0].Generation = 1
			case api.InvLegalTransitions:
				s.Recent = []api.Event{{Seq: 1, Kind: api.EventTransition, From: api.VMReady, To: api.VMPending}}
			case api.InvQueueBounded:
				s.Queue = make([]api.QueueEntry, cfg.QueueCap+1)
			case api.InvNoAttackSuccess:
				raw[0].Security.Succeeded = 1
			case api.InvPendingPlaced:
				s.VMs[0].State = api.VMPending
				raw[0].Host.Slots[0] = api.Slot{State: api.SlotFree}
				raw[0].Actual = nil
				s.Queue = []api.QueueEntry{{VMID: "v"}}
				c.Check(s, raw...)
				s.At += (cfg.PendingPlaceWithin + cfg.HostHeartbeat).Milliseconds() + 1
			}
			out := c.Check(s, raw...)
			found := false
			for _, v := range c.Status() {
				if v.Name == name && v.Violations > 0 {
					found = true
				}
			}
			if len(out) == 0 || !found {
				t.Fatalf("did not detect %s: %v", name, out)
			}
		})
	}
}

func TestCrossComponentSlackAndEventDeduplication(t *testing.T) {
	cfg := config.Default()
	c := New(cfg)
	s := api.Snapshot{At: 1000, VMs: []api.VM{{ID: "v", Generation: 2}}}
	raw := []Observation{{Host: api.Host{ID: "h", SlotCount: 1}, Connected: true, Actual: []api.DriverVM{{VMID: "v", Generation: 1, Running: true}}}}
	c.Check(s, raw...)
	s.At += cfg.HostHeartbeat.Milliseconds()
	if e := c.Check(s, raw...); len(e) != 0 {
		t.Fatal("no heartbeat slack", e)
	}
	s.Hosts = nil
	s.Recent = []api.Event{{Seq: 1, Kind: api.EventTransition, From: api.VMReady, To: api.VMPending}}
	if e := c.Check(s); len(e) != 1 {
		t.Fatal(e)
	}
	if e := c.Check(s); len(e) != 0 {
		t.Fatal("duplicate event checked twice", e)
	}
}

func TestPendingPlacementRequiresJointlyAvailableCapacity(t *testing.T) {
	for _, mode := range []string{"killed host before lease expiry", "scheduler occupied", "agent occupied", "different free slots", "disconnected", "disk full", "scheduler suspect", "scheduler cordoned", "available"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config.Default()
			c := New(cfg)
			s := api.Snapshot{At: 1000, Scheduler: api.SchedulerUp,
				VMs: []api.VM{{ID: "pending", State: api.VMPending}},
				Hosts: []api.Host{{ID: "h", SlotCount: 2, Lease: api.LeaseHealthy, AdminState: api.AdminActive,
					Slots: []api.Slot{{Index: 0, State: api.SlotFree}, {Index: 1, State: api.SlotOccupied}}}},
			}
			raw := Observation{Host: s.Hosts[0], Connected: true}
			raw.Host.Slots = []api.Slot{{Index: 0, State: api.SlotFree}, {Index: 1, State: api.SlotOccupied}}
			switch mode {
			case "killed host before lease expiry":
				s.Hosts[0].Slots[0].State = api.SlotOccupied
				raw.Host.Slots[1].State = api.SlotFree
				raw.Host.Faults = []string{"kill-host"}
			case "scheduler occupied":
				s.Hosts[0].Slots[0].State = api.SlotOccupied
			case "agent occupied":
				raw.Host.Slots[0].State = api.SlotOccupied
			case "different free slots":
				raw.Host.Slots[0].State = api.SlotOccupied
				raw.Host.Slots[1].State = api.SlotFree
			case "disconnected":
				raw.Connected = false
			case "disk full":
				raw.Host.Faults = []string{"fill-disk"}
			case "scheduler suspect":
				s.Hosts[0].Lease = api.LeaseSuspect
			case "scheduler cordoned":
				s.Hosts[0].AdminState = api.AdminCordoned
			}
			c.Check(s, raw)
			// Still inside the healthy host lease, but beyond the unchanged
			// placement deadline and one heartbeat of cross-component slack.
			s.At += cfg.HostLeaseTTL.Milliseconds() - 1
			out := c.Check(s, raw)
			want := 0
			if mode == "available" {
				want = 1
			}
			if len(out) != want || (want == 1 && out[0].Detail != string(api.InvPendingPlaced)+": pending") {
				t.Fatalf("violations=%+v, want %d placement violations", out, want)
			}
		})
	}
}
