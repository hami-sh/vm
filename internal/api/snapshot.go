package api

// EventKind matches the events.kind CHECK list in the schema. alert and
// reconcile were added in Phase 0: without them SQLite would reject the row
// and roll back the state change it is written with.
type EventKind string

const (
	EventTransition EventKind = "transition"
	EventFencing    EventKind = "fencing"
	EventAdmission  EventKind = "admission"
	EventChaos      EventKind = "chaos"
	EventTopology   EventKind = "topology"
	EventSecurity   EventKind = "security"
	EventInvariant  EventKind = "invariant"
	EventAlert      EventKind = "alert"
	EventReconcile  EventKind = "reconcile"
)

// Event is one row of the append-only event log, and one SSE "event" message.
type Event struct {
	Seq        int64     `json:"seq"`
	At         int64     `json:"at"` // sim-clock unix ms
	Kind       EventKind `json:"kind"`
	VMID       string    `json:"vm_id,omitempty"`
	HostID     string    `json:"host_id,omitempty"`
	Rack       string    `json:"rack,omitempty"`
	Generation int64     `json:"gen,omitempty"`
	From       VMState   `json:"from,omitempty"`
	To         VMState   `json:"to,omitempty"`
	Detail     string    `json:"detail"`
}

// SchedulerStatus is the control plane's own health.
type SchedulerStatus string

const (
	SchedulerUp          SchedulerStatus = "up"
	SchedulerDown        SchedulerStatus = "down"
	SchedulerReconciling SchedulerStatus = "reconciling"
)

// InvariantName identifies one of the 8 continuously checked invariants.
type InvariantName string

const (
	InvOneAuthoritative  InvariantName = "one_authoritative_copy"  // 1
	InvStaleStopped      InvariantName = "stale_copy_stopped"      // 2
	InvSlotCapacity      InvariantName = "slot_capacity"           // 3
	InvGenMonotonic      InvariantName = "generation_monotonic"    // 4
	InvLegalTransitions  InvariantName = "legal_transitions"       // 5
	InvQueueBounded      InvariantName = "queue_bounded"           // 6
	InvNoAttackSuccess   InvariantName = "no_successful_attack"    // 7
	InvPendingPlaced     InvariantName = "pending_placed_promptly" // 8
	InvIdempotencyUnique InvariantName = "idempotency_unique"      // 9
)

// AllInvariants lists invariants in display order.
var AllInvariants = []InvariantName{
	InvOneAuthoritative, InvStaleStopped, InvSlotCapacity, InvGenMonotonic,
	InvLegalTransitions, InvQueueBounded, InvNoAttackSuccess, InvPendingPlaced,
	InvIdempotencyUnique,
}

// HostObservation is the independent driver evidence evaluated by the checker
// for a published snapshot. Disconnected hosts remain unobserved by invariants;
// consumers must honor Connected rather than treating Actual as authoritative.
type HostObservation struct {
	HostID    string     `json:"host_id"`
	Connected bool       `json:"connected"`
	Actual    []DriverVM `json:"actual"`
}

// InvariantStatus is one light on the evidence panel.
type InvariantStatus struct {
	Name       InvariantName `json:"name"`
	Violations int           `json:"violations"`
	LastAt     int64         `json:"last_at,omitempty"`
	LastDetail string        `json:"last_detail,omitempty"`
}

// SecurityStatus is the "attacks blocked N · succeeded 0" counter.
type SecurityStatus struct {
	Blocked   int            `json:"blocked"`
	Succeeded int            `json:"succeeded"`
	ByAttack  map[string]int `json:"by_attack"`
}

// Counters are totals for the status bar.
type Counters struct {
	ByState     map[VMState]int `json:"by_state"`
	QueueDepth  int             `json:"queue_depth"`
	QueueCap    int             `json:"queue_cap"`
	Placed      int             `json:"placed"`
	Queued      int             `json:"queued"`
	Rejected429 int             `json:"rejected_429"`
	Shed        int             `json:"shed"`
	Recoveries  map[string]int  `json:"recoveries"` // recycle | replace | retry
	// Headroom is free slots minus the busiest rack's VMs. Negative means
	// one rack failure can't be absorbed.
	Headroom int `json:"headroom"`
}

// LatencyStats summarise time to Ready for the live chart, in sim ms.
type LatencyStats struct {
	P50Ms    int64 `json:"p50_ms"`
	P99Ms    int64 `json:"p99_ms"`
	TargetMs int64 `json:"target_ms"`
	Samples  int   `json:"samples"`
}

// SimStatus is the clock and chaos engine state.
type SimStatus struct {
	Speed     float64  `json:"speed"`
	Paused    bool     `json:"paused"`
	Seed      int64    `json:"seed"`
	Intensity float64  `json:"intensity"`       // 0..1
	Filters   []string `json:"filters"`         // enabled ambient fault types
	Racks     []string `json:"racks,omitempty"` // ambient chaos scope; empty means all
	LoadRate  float64  `json:"load_rate"`       // creates per sim second
	// Scenario is set while a scenario started from the dashboard runs.
	Scenario string `json:"scenario,omitempty"`
}

// Snapshot is the full state the dashboard renders. It is published every
// 100 ms of sim time (streamed over SSE every 500 ms wall time) and recorded
// to runs/<id>/snapshots.jsonl. The UI re-renders entirely from each one.
type Snapshot struct {
	Seq       int64           `json:"seq"`
	At        int64           `json:"at"` // sim-clock unix ms
	Scheduler SchedulerStatus `json:"scheduler"`
	Epoch     int64           `json:"epoch"`
	Sim       SimStatus       `json:"sim"`
	Pools     []Pool          `json:"pools"`
	Racks     []Rack          `json:"racks"`
	Hosts     []Host          `json:"hosts"`
	// Observed preserves the exact independent evidence checked for this
	// snapshot. It is absent in older recordings, not an empty observation.
	Observed   []HostObservation       `json:"observed,omitempty"`
	VMs        []VM                    `json:"vms"`
	Queue      []QueueEntry            `json:"queue"`
	Counters   Counters                `json:"counters"`
	Latency    map[string]LatencyStats `json:"latency"` // by class: interactive, standard, batch
	Invariants []InvariantStatus       `json:"invariants"`
	Security   SecurityStatus          `json:"security"`
	// Recent holds the last events since the previous snapshot (max 50).
	Recent []Event `json:"recent"`
}
