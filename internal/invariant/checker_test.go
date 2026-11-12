package invariant

import (
	"strings"
	"testing"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

func TestEachInvariantDetectsBrokenState(t *testing.T) {
	for _, name := range api.AllInvariants {
		t.Run(string(name), func(t *testing.T) {
			cfg := config.Default()
			cfg.SlotsPerHost = 1
			c := New(cfg)
			s := api.Snapshot{At: 1000, Scheduler: api.SchedulerUp, VMs: []api.VM{{ID: "v", State: api.VMReady, Generation: 2, HostID: "h", Desired: api.DesiredRunning}}, Hosts: []api.Host{{ID: "h", SlotCount: 1, Lease: api.LeaseHealthy, AdminState: api.AdminActive, Slots: []api.Slot{{State: api.SlotOccupied, VMID: "v", Generation: 2}}}}}
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
				raw[0].Actual = append(raw[0].Actual, api.DriverVM{VMID: "other", Generation: 1, Running: true})
			case api.InvGenMonotonic:
				s.VMs[0].Generation = 1
			case api.InvLegalTransitions:
				s.Recent = []api.Event{{Seq: 1, Kind: api.EventTransition, From: api.VMReady, To: api.VMPending}}
			case api.InvQueueBounded:
				s.Queue = make([]api.QueueEntry, cfg.QueueCap+1)
			case api.InvNoAttackSuccess:
				s.Security.Succeeded = 1
			case api.InvIdempotencyUnique:
				s.VMs = append(s.VMs, api.VM{ID: "second", State: api.VMPending})
			case api.InvPendingPlaced:
				s.VMs[0].State = api.VMPending
				raw[0].Host.Slots[0] = api.Slot{State: api.SlotFree}
				raw[0].Actual = nil
				s.Queue = []api.QueueEntry{{VMID: "v"}}
				c.Check(s, raw...)
				s.At += (cfg.PendingPlaceWithin + cfg.HostHeartbeat).Milliseconds() + 1
			}
			out := c.Check(s, raw...)
			out = append(out, c.CheckKeys(s, map[string]string{"v": "K", "second": "K"}, raw...)...)
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

func TestDriverAheadDuringSchedulerOutage(t *testing.T) {
	c := New(config.Default())
	s := api.Snapshot{At: 1036834, Scheduler: api.SchedulerUp, VMs: []api.VM{{ID: "vm33", State: api.VMReady, Desired: api.DesiredRunning, Generation: 1, HostID: "h"}}}
	raw := Observation{Host: api.Host{ID: "h"}, Connected: true, Actual: []api.DriverVM{{VMID: "vm33", Generation: 2, Running: true}}}
	// Seed 15's durable placement reached the driver before publication;
	// the scheduler died 117ms later and published gen2 only after restart.
	for _, at := range []int64{1036834, 1036951, 1044244, 1046699, 1046700, 1049392} {
		s.At = at
		if at >= 1036951 {
			s.Scheduler = api.SchedulerDown
		}
		if at >= 1046700 {
			s.Scheduler = api.SchedulerUp
			s.VMs[0].Generation = 2
			s.VMs[0].State = api.VMBooting
		}
		if at >= 1049392 {
			s.VMs[0].State = api.VMReady
		}
		if out := c.Check(s, raw); len(out) != 0 {
			t.Fatalf("at %d, durable copy is not stale: %v", at, out)
		}
		if out := c.CheckKeys(s, map[string]string{"vm33": "K"}, raw); len(out) != 0 {
			t.Fatalf("at %d, one logical VM counted twice: %v", at, out)
		}
	}
}

func TestDriverAheadRequiresContinuousUpPublication(t *testing.T) {
	cfg := config.Default()
	hb := cfg.HostHeartbeat.Milliseconds()
	for _, reset := range []string{"none", "higher-driver-generation", "down", "reconciling", "disconnected", "stopped", "caught-up"} {
		t.Run(reset, func(t *testing.T) {
			c := New(cfg)
			s := api.Snapshot{At: 1000, Scheduler: api.SchedulerUp, VMs: []api.VM{{ID: "v", State: api.VMReady, Desired: api.DesiredRunning, Generation: 1, HostID: "h"}}}
			raw := Observation{Host: api.Host{ID: "h"}, Connected: true, Actual: []api.DriverVM{{VMID: "v", Generation: 2, Running: true}}}
			check := func(want bool) {
				t.Helper()
				out := c.Check(s, raw)
				if !want && len(out) != 0 {
					t.Fatalf("unexpected violation at %d: %v", s.At, out)
				}
				if want && (len(out) != 1 || !strings.HasPrefix(out[0].Detail, string(api.InvGenMonotonic)+":")) {
					t.Fatalf("missing persist-before-act violation at %d: %v", s.At, out)
				}
			}
			check(false)
			s.At += hb
			check(false) // Exactly one heartbeat is allowed.
			switch reset {
			case "higher-driver-generation":
				raw.Actual[0].Generation = 3
			case "down":
				s.Scheduler = api.SchedulerDown
			case "reconciling":
				s.Scheduler = api.SchedulerReconciling
			case "disconnected":
				raw.Connected = false
			case "stopped":
				raw.Actual[0].Running = false
			case "caught-up":
				s.VMs[0].Generation = 2
			}
			s.At++
			if reset == "higher-driver-generation" {
				out := c.Check(s, raw)
				if len(out) != 1 || !strings.HasPrefix(out[0].Detail, string(api.InvGenMonotonic)+":") {
					t.Fatalf("increasing driver generation reset the deadline: %v", out)
				}
				return
			}
			check(reset == "none")
			if reset == "none" {
				return
			}
			s.At += 4 * hb // Neither outage nor disconnection ages the deadline.
			check(false)
			s.Scheduler, raw.Connected, raw.Actual[0].Running = api.SchedulerUp, true, true
			raw.Actual[0].Generation = s.VMs[0].Generation + 1
			s.At++
			check(false)
			s.At += hb
			check(false)
			s.At++
			out := c.Check(s, raw)
			if len(out) != 1 || !strings.HasPrefix(out[0].Detail, string(api.InvGenMonotonic)+":") {
				t.Fatalf("continuous up publication did not fail: %v", out)
			}
		})
	}
}

func TestDriverAheadKeyOwnershipAfterPublication(t *testing.T) {
	cfg := config.Default()
	c := New(cfg)
	s := api.Snapshot{At: 1000, Scheduler: api.SchedulerDown, VMs: []api.VM{
		{ID: "old", State: api.VMTerminated, Generation: 1},
		{ID: "retry", State: api.VMReady, Generation: 1},
	}}
	raw := Observation{Host: api.Host{ID: "h"}, Connected: true, Actual: []api.DriverVM{{VMID: "old", Generation: 2, Running: true}}}
	keys := map[string]string{"old": "K", "retry": "K"}
	for range 3 {
		if out := c.CheckKeys(s, keys, raw); len(out) != 0 {
			t.Fatalf("ahead-of-publication copy became a second owner: %v", out)
		}
		s.At += cfg.HostHeartbeat.Milliseconds() + 1
	}
	// Once publication catches up, this is a genuine retired copy sharing
	// the retry's key, and must stop within the ordinary heartbeat bound.
	s.Scheduler = api.SchedulerUp
	s.VMs[0].Generation = 2
	if out := c.CheckKeys(s, keys, raw); len(out) != 0 {
		t.Fatal("missing heartbeat slack after publication", out)
	}
	s.At += cfg.HostHeartbeat.Milliseconds()
	if out := c.CheckKeys(s, keys, raw); len(out) != 0 {
		t.Fatal("exact heartbeat boundary", out)
	}
	s.At++
	if out := c.CheckKeys(s, keys, raw); len(out) != 1 {
		t.Fatal("published second logical VM not detected", out)
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

func TestDriverEvidenceDetectsFencedAndUnknownCopies(t *testing.T) {
	for _, mode := range []string{"older", "equal on wrong host", "unknown after wipe", "deleted", "fenced equal", "current Ready", "current Booting", "stopped", "disconnected"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config.Default()
			c := New(cfg)
			s := api.Snapshot{At: 1000, VMs: []api.VM{{ID: "v", Generation: 3, HostID: "h", State: api.VMReady, Desired: api.DesiredRunning}}}
			o := Observation{Host: api.Host{ID: "h"}, Connected: true, Actual: []api.DriverVM{{VMID: "v", Generation: 3, Running: true}}}
			want := true
			switch mode {
			case "older":
				o.Actual[0].Generation = 2
			case "equal on wrong host":
				s.VMs[0].HostID = "other"
			case "unknown after wipe":
				s.VMs = nil
			case "deleted":
				s.VMs[0].Desired = api.DesiredDeleted
			case "fenced equal":
				s.VMs[0].State = api.VMPending
			case "current Ready":
				want = false
			case "current Booting":
				s.VMs[0].State = api.VMBooting
				want = false
			case "stopped":
				o.Actual[0].Running = false
				want = false
			case "disconnected":
				s.VMs = nil
				o.Connected = false
				want = false
			}
			if events := c.Check(s, o); len(events) != 0 {
				t.Fatal("no cross-component slack", events)
			}
			s.At += cfg.HostHeartbeat.Milliseconds()
			if events := c.Check(s, o); len(events) != 0 {
				t.Fatal("heartbeat boundary", events)
			}
			s.At++
			c.Check(s, o)
			for _, status := range c.Status() {
				if status.Name == api.InvStaleStopped && (status.Violations > 0) != want {
					t.Fatalf("%s: %+v", mode, status)
				}
			}
		})
	}
}

func TestCapacityCountsOnlyIndependentRunningDrivers(t *testing.T) {
	for _, mode := range []string{"driver exceeds cap", "agent hides overflow", "asynchronous slot union", "nonrunning driver", "configured capacity", "disconnected"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config.Default()
			cfg.SlotsPerHost = 2
			o := Observation{Host: api.Host{ID: "h", SlotCount: 2, Slots: []api.Slot{{VMID: "a", Generation: 1, State: api.SlotReleasing}, {VMID: "b", Generation: 1, State: api.SlotOccupied}}}, Connected: true,
				Actual: []api.DriverVM{{VMID: "b", Generation: 1, Running: true}, {VMID: "c", Generation: 1, Running: true}}}
			want := false
			switch mode {
			case "driver exceeds cap", "agent hides overflow":
				o.Actual = append(o.Actual, api.DriverVM{VMID: "d", Generation: 1, Running: true})
				want = true
				if mode == "agent hides overflow" {
					o.Host.Slots = nil
					o.Host.SlotCount = 99
				}
			case "nonrunning driver":
				o.Actual = append(o.Actual, api.DriverVM{VMID: "d", Generation: 1})
			case "configured capacity":
				cfg.SlotsPerHost = 3
				o.Actual = append(o.Actual, api.DriverVM{VMID: "d", Generation: 1, Running: true})
			case "disconnected":
				o.Actual = append(o.Actual, api.DriverVM{VMID: "d", Generation: 1, Running: true})
				o.Connected = false
			}
			c := New(cfg)
			s := api.Snapshot{At: 1000}
			c.Check(s, o)
			for _, status := range c.Status() {
				if status.Name == api.InvSlotCapacity && (status.Violations > 0) != want {
					t.Fatalf("%s: %+v", mode, status)
				}
			}
		})
	}
}

func TestIdempotencyPartitionWipeRetryHealAndAdoption(t *testing.T) {
	for _, mode := range []string{"late driver", "late adopted", "key moves to adopted copy", "same logical ID", "different keys", "stopped old copy", "disconnected"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config.Default()
			c := New(cfg)
			keys := map[string]string{"old": "K", "retry": "K"}
			s := api.Snapshot{At: 1000, VMs: []api.VM{{ID: "old", State: api.VMReady}}}
			o := Observation{Host: api.Host{ID: "late-host"}, Connected: false, Actual: []api.DriverVM{{VMID: "old", Generation: 7, Running: true}}}
			if out := c.CheckKeys(s, keys, o); len(out) != 0 {
				t.Fatal(out)
			}
			// Partition, wipe, then retry K: the old copy is still unobserved.
			s.VMs = []api.VM{{ID: "retry", State: api.VMReady}}
			s.At += cfg.HostLeaseTTL.Milliseconds()
			if out := c.CheckKeys(s, keys, o); len(out) != 0 {
				t.Fatal("disconnected copy observed", out)
			}
			o.Connected = true // Heal; observe the late copy independently.
			want := true
			immediate := false
			switch mode {
			case "late adopted":
				s.VMs = append(s.VMs, api.VM{ID: "old", State: api.VMReady})
				immediate = true
			case "key moves to adopted copy":
				// Changing the store's current key owner cannot change the retained
				// accepted-create ledger: both logical IDs still claim K.
				s.VMs = append(s.VMs, api.VM{ID: "old", State: api.VMReady})
				immediate = true
			case "same logical ID":
				o.Actual[0].VMID = "retry"
				want = false
			case "different keys":
				keys["old"] = "other"
				want = false
			case "stopped old copy":
				o.Actual[0].Running = false
				want = false
			case "disconnected":
				o.Connected = false
				want = false
			}
			if out := c.CheckKeys(s, keys, o); (len(out) > 0) != immediate {
				t.Fatalf("initial heal: %+v", out)
			}
			s.At += cfg.HostHeartbeat.Milliseconds()
			if out := c.CheckKeys(s, keys, o); (len(out) > 0) != immediate {
				t.Fatalf("heartbeat boundary: %+v", out)
			}
			s.At++
			if out := c.CheckKeys(s, keys, o); (len(out) > 0) != want {
				t.Fatalf("after heartbeat: %+v", out)
			}
		})
	}
}
