package api

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"
)

// The dashboard (W4) builds against fixtures/snapshot.json until live SSE is
// wired. This test keeps the fixture in lockstep with the Snapshot type:
// run `go test ./internal/api -update` after changing the contract.
var update = flag.Bool("update", false, "rewrite fixtures/snapshot.json")

const fixturePath = "../../fixtures/snapshot.json"

func fixtureSnapshot() Snapshot {
	const t0 = int64(1_767_225_600_000) // 2026-01-01T00:00:00Z on the sim clock
	s := Snapshot{
		Seq: 1234, At: t0 + 95_000, Scheduler: SchedulerUp, Epoch: 3,
		Sim: SimStatus{Speed: 5, Seed: 42, Intensity: 0.3,
			Filters: []string{"kill-guestd", "kill-host", "latency-spike", "compromise"}, LoadRate: 1.5},
		Pools: []Pool{
			{Name: "ios-xcode16-m", Image: "macos-15-xcode16", CPUs: 4, MemoryMB: 8192},
			{Name: "ios-xcode15-s", Image: "macos-14-xcode15", CPUs: 2, MemoryMB: 4096},
		},
		Latency: map[string]LatencyStats{
			"interactive": {P50Ms: 4800, P99Ms: 9100, TargetMs: 12000, Samples: 40},
			"standard":    {P50Ms: 5200, P99Ms: 14200, TargetMs: 12000, Samples: 77},
			"batch":       {P50Ms: 21000, P99Ms: 58000, TargetMs: 0, Samples: 31},
		},
		Security: SecurityStatus{Blocked: 7, Succeeded: 0,
			ByAttack: map[string]int{AttackImpersonate: 2, AttackEastWest: 3, AttackEgress: 2}},
	}
	// One VM per state, spread so the grid shows every chip style.
	type placed struct {
		state VMState
		host  string
		slot  int
		gen   int64
		why   string
	}
	plan := []placed{
		{VMReady, "r1-h1", 0, 1, ""}, {VMReady, "r1-h1", 1, 2, ""},
		{VMBooting, "r1-h2", 0, 1, ""}, {VMReady, "r1-h3", 0, 1, ""},
		{VMUnhealthy, "r1-h4", 0, 3, ReasonGuestLease},
		{VMReady, "r2-h1", 0, 1, ""}, {VMReady, "r2-h1", 1, 1, ""},
		{VMReady, "r2-h2", 0, 4, ""}, // re-placed here from r3-h4 (gen 3 is stale there)
		{VMTerminating, "r2-h3", 1, 1, ReasonDeleted},
		{VMLost, "r2-h4", 0, 2, ReasonHostLost},
		{VMReady, "r3-h1", 0, 1, ""}, {VMReady, "r3-h2", 0, 1, ""},
		{VMBooting, "r3-h3", 0, 2, ""},
		{VMPending, "", 0, 0, ""}, {VMPending, "", 0, 0, ""}, {VMPending, "", 0, 2, ReasonHostLost},
		{VMFailed, "", 0, 3, ReasonBootDeadline}, {VMFailed, "", 0, 0, ReasonShed},
		{VMTerminated, "", 0, 1, ReasonDeleted},
	}
	hostIdx := map[string]int{}
	for r := 1; r <= 3; r++ {
		rack := Rack{ID: fmt.Sprintf("r%d", r)}
		for h := 1; h <= 4; h++ {
			id := fmt.Sprintf("r%d-h%d", r, h)
			rack.Hosts = append(rack.Hosts, id)
			hostIdx[id] = len(s.Hosts)
			s.Hosts = append(s.Hosts, Host{ID: id, Rack: rack.ID, SlotCount: 2, AdminState: AdminActive,
				Lease: LeaseHealthy, LeaseAgeMs: 800,
				Slots: []Slot{{Index: 0, State: SlotFree}, {Index: 1, State: SlotFree}}})
		}
		s.Racks = append(s.Racks, rack)
	}
	// Host-level states worth rendering.
	s.Hosts[hostIdx["r2-h4"]].Lease, s.Hosts[hostIdx["r2-h4"]].LeaseAgeMs = LeaseLost, 14_000
	s.Hosts[hostIdx["r2-h4"]].Faults = []string{"killed"}
	s.Hosts[hostIdx["r3-h4"]].Lease, s.Hosts[hostIdx["r3-h4"]].LeaseAgeMs = LeaseSuspect, 7_100
	s.Hosts[hostIdx["r3-h4"]].Faults = []string{"paused-heartbeats"}
	s.Hosts[hostIdx["r3-h4"]].Stale = []InventoryItem{{VMID: "vm-0008", Generation: 3, State: VMReady}}
	s.Hosts[hostIdx["r1-h3"]].AdminState = AdminCordoned
	s.Hosts[hostIdx["r3-h2"]].AdminState = AdminDraining

	byState := map[VMState]int{}
	for i, p := range plan {
		v := VM{ID: fmt.Sprintf("vm-%04d", i+1), Pool: s.Pools[i%2].Name, State: p.state,
			Desired: DesiredRunning, Generation: p.gen, HostID: p.host, Priority: Priority(1 + i%3),
			Attempts: int(p.gen), LastReason: p.why, CreatedAt: t0 + int64(i)*1000, UpdatedAt: t0 + 90_000}
		if v.Attempts == 0 && p.state != VMPending {
			v.Attempts = 1
		}
		if p.state == VMTerminating || p.state == VMTerminated {
			v.Desired = DesiredDeleted
		}
		if p.state == VMReady {
			v.ReadyAt = v.CreatedAt + 5000
		}
		if p.host != "" {
			h := &s.Hosts[hostIdx[p.host]]
			v.Rack = h.Rack
			st := SlotOccupied
			switch p.state {
			case VMBooting:
				st = SlotReserved
			case VMTerminating:
				st = SlotReleasing
			}
			h.Slots[p.slot] = Slot{Index: p.slot, State: st, VMID: v.ID, Generation: v.Generation}
		}
		if v.ID == "vm-0007" {
			v.Compromised = true
		}
		byState[v.State]++
		s.VMs = append(s.VMs, v)
	}
	s.Queue = []QueueEntry{
		{VMID: "vm-0016", Priority: PriorityReplacement, EffectivePriority: PriorityReplacement, Reason: QueueReplace, EnqueuedAt: t0 + 88_000, NotBefore: t0 + 88_000},
		{VMID: "vm-0014", Priority: PriorityInteractive, EffectivePriority: PriorityInteractive, Reason: QueueNew, EnqueuedAt: t0 + 13_000, NotBefore: t0 + 13_000},
		{VMID: "vm-0015", Priority: PriorityBatch, EffectivePriority: PriorityStandard, Reason: QueueRetry, EnqueuedAt: t0 + 14_000, NotBefore: t0 + 97_000},
	}
	s.Counters = Counters{ByState: byState, QueueDepth: len(s.Queue), QueueCap: 32,
		Placed: 61, Queued: 19, Rejected429: 4, Shed: 1,
		Recoveries: map[string]int{"recycle": 3, "replace": 2, "retry": 5}, Headroom: 2}
	for _, n := range AllInvariants {
		s.Invariants = append(s.Invariants, InvariantStatus{Name: n})
	}
	s.Recent = []Event{
		{Seq: 900, At: t0 + 80_000, Kind: EventChaos, HostID: "r2-h4", Rack: "r2", Detail: "kill-host"},
		{Seq: 901, At: t0 + 88_000, Kind: EventTransition, VMID: "vm-0010", HostID: "r2-h4", Generation: 2, From: VMReady, To: VMLost, Detail: ReasonHostLost},
		{Seq: 902, At: t0 + 88_000, Kind: EventAdmission, VMID: "vm-0016", Detail: "queued class 0 (replace)"},
		{Seq: 903, At: t0 + 89_500, Kind: EventFencing, VMID: "vm-0008", HostID: "r3-h4", Generation: 3, Detail: "stale gen 3 < 4: heartbeat rejected, StopVM queued"},
		{Seq: 904, At: t0 + 91_000, Kind: EventSecurity, VMID: "vm-0007", HostID: "r2-h1", Generation: 1, Detail: "blocked east_west to vm-0006"},
		{Seq: 905, At: t0 + 93_000, Kind: EventReconcile, HostID: "r1-h2", Detail: "adopted 1, stopped 0, re-placed 0"},
		{Seq: 906, At: t0 + 94_000, Kind: EventTopology, HostID: "r3-h2", Rack: "r3", Detail: "drain started"},
	}
	return s
}

func TestFixtureMatchesContract(t *testing.T) {
	want, err := json.MarshalIndent(fixtureSnapshot(), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want = append(want, '\n')
	if *update {
		if err := os.WriteFile(fixturePath, want, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("%v (run: go test ./internal/api -update)", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("fixtures/snapshot.json is out of date with the Snapshot type: run go test ./internal/api -update")
	}
	// Strict decode: no field in the fixture the type doesn't know.
	dec := json.NewDecoder(bytes.NewReader(got))
	dec.DisallowUnknownFields()
	var s Snapshot
	if err := dec.Decode(&s); err != nil {
		t.Fatal(err)
	}
	slots := 0
	for _, h := range s.Hosts {
		slots += len(h.Slots)
	}
	if len(s.Racks) != 3 || len(s.Hosts) != 12 || slots != 24 {
		t.Fatalf("topology: %d racks, %d hosts, %d slots", len(s.Racks), len(s.Hosts), slots)
	}
	if len(s.Invariants) != 9 {
		t.Fatalf("want 9 invariants, got %d", len(s.Invariants))
	}
	for _, st := range []VMState{VMPending, VMBooting, VMReady, VMUnhealthy, VMLost, VMTerminating, VMTerminated, VMFailed} {
		if s.Counters.ByState[st] == 0 {
			t.Errorf("fixture has no %s VM; the UI needs every state to style", st)
		}
	}
}
