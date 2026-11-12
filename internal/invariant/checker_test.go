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
			if out := c.Check(s); len(out) != 0 {
				t.Fatalf("healthy state: %v", out)
			}
			switch name {
			case api.InvOneAuthoritative:
				s.Hosts = append(s.Hosts, s.Hosts[0])
			case api.InvStaleStopped:
				s.Hosts[0].Stale = []api.InventoryItem{{VMID: "v", Generation: 1}}
				c.Check(s)
				s.At += cfg.HostHeartbeat.Milliseconds() + 1
			case api.InvSlotCapacity:
				s.Hosts[0].Slots = append(s.Hosts[0].Slots, api.Slot{State: api.SlotReleasing, VMID: "other"})
			case api.InvGenMonotonic:
				s.VMs[0].Generation = 1
			case api.InvLegalTransitions:
				s.Recent = []api.Event{{Seq: 1, Kind: api.EventTransition, From: api.VMReady, To: api.VMPending}}
			case api.InvQueueBounded:
				s.Queue = make([]api.QueueEntry, cfg.QueueCap+1)
			case api.InvNoAttackSuccess:
				s.Security.Succeeded = 1
			case api.InvPendingPlaced:
				s.VMs[0].State = api.VMPending
				s.Hosts[0].Slots[0] = api.Slot{State: api.SlotFree}
				s.Queue = []api.QueueEntry{{VMID: "v"}}
				c.Check(s)
				s.At += (cfg.PendingPlaceWithin + cfg.HostHeartbeat).Milliseconds() + 1
			}
			out := c.Check(s)
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
	s := api.Snapshot{At: 1000, Hosts: []api.Host{{ID: "h", Lease: api.LeaseHealthy, Stale: []api.InventoryItem{{VMID: "v", Generation: 1}}}}}
	c.Check(s)
	s.At += cfg.HostHeartbeat.Milliseconds()
	if e := c.Check(s); len(e) != 0 {
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
