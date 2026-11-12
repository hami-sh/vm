// Package api holds the frozen contracts shared by every workstream: VM and
// host records, the internal protocol messages, the event log, and the JSON
// snapshot the dashboard renders. Only the Phase 0 owner edits this package;
// request changes in COORDINATION.md.
package api

// VMState is the scheduler's view of one VM. Every change goes through
// vm.Transition, which rejects moves not in the legal-move table.
type VMState string

const (
	VMPending     VMState = "Pending"
	VMBooting     VMState = "Booting"
	VMReady       VMState = "Ready"
	VMUnhealthy   VMState = "Unhealthy"
	VMLost        VMState = "Lost"
	VMTerminating VMState = "Terminating"
	VMTerminated  VMState = "Terminated"
	VMFailed      VMState = "Failed"
)

// Desired is the operator's intent for a VM, kept separate from its state.
type Desired string

const (
	DesiredRunning Desired = "running"
	DesiredDeleted Desired = "deleted"
)

// Priority is a queue class. 0 is reserved for replacements of VMs that were
// Ready and were lost through no fault of their own.
type Priority int

const (
	PriorityReplacement Priority = 0
	PriorityInteractive Priority = 1
	PriorityStandard    Priority = 2
	PriorityBatch       Priority = 3
)

// QueueReason says why a VM is in the placement queue.
type QueueReason string

const (
	QueueNew     QueueReason = "new"
	QueueRetry   QueueReason = "retry"
	QueueReplace QueueReason = "replace"
)

// LeaseState is the scheduler's view of a host lease. Leases live in memory
// only: after a scheduler restart every host starts Suspect.
type LeaseState string

const (
	LeaseHealthy LeaseState = "healthy"
	LeaseSuspect LeaseState = "suspect"
	LeaseLost    LeaseState = "lost"
)

// AdminState is operator intent about a host.
type AdminState string

const (
	AdminActive   AdminState = "active"
	AdminCordoned AdminState = "cordoned"
	AdminDraining AdminState = "draining"
	AdminRemoved  AdminState = "removed"
)

// SlotState is the lifecycle of one VM slot on a host:
// free -> reserved -> occupied -> releasing -> free.
type SlotState string

const (
	SlotFree      SlotState = "free"
	SlotReserved  SlotState = "reserved"
	SlotOccupied  SlotState = "occupied"
	SlotReleasing SlotState = "releasing"
)

// Well-known last_reason values. Free-form reasons are allowed too.
const (
	ReasonShed           = "shed"
	ReasonQuarantined    = "quarantined"
	ReasonBootDeadline   = "boot_deadline"
	ReasonRetryBudget    = "retry_budget_exhausted"
	ReasonHostLost       = "host_lost"
	ReasonGuestLease     = "guest_lease_expired"
	ReasonRefused        = "refused"
	ReasonReservationTTL = "reservation_expired"
	ReasonDeleted        = "deleted"
	ReasonDrained        = "drained"
	ReasonStale          = "stale_generation"
)

// Pool is a named group of interchangeable VMs with the same image and size.
type Pool struct {
	Name     string `json:"name"`
	Image    string `json:"image"`
	CPUs     int    `json:"cpus"`
	MemoryMB int    `json:"memory_mb"`
}

// VMSpec is what a host needs to boot a VM.
type VMSpec struct {
	Pool     string `json:"pool"`
	Image    string `json:"image"`
	CPUs     int    `json:"cpus"`
	MemoryMB int    `json:"memory_mb"`
	DiskMB   int    `json:"disk_mb"`
}

// VM is the scheduler's record of one VM, as served by GET /v1/vms and in
// snapshots. Times are sim-clock unix milliseconds.
type VM struct {
	ID         string   `json:"id"`
	Pool       string   `json:"pool"`
	State      VMState  `json:"state"`
	Desired    Desired  `json:"desired"`
	Generation int64    `json:"gen"`
	HostID     string   `json:"host,omitempty"`
	Rack       string   `json:"rack,omitempty"`
	Priority   Priority `json:"priority"`
	Attempts   int      `json:"attempts"`
	LastReason string   `json:"last_reason,omitempty"`
	// Compromised marks a VM whose guest runs the attack script.
	Compromised bool  `json:"compromised,omitempty"`
	CreatedAt   int64 `json:"created_at"`
	UpdatedAt   int64 `json:"updated_at"`
	ReadyAt     int64 `json:"ready_at,omitempty"`
}

// Slot is one VM slot on a host, as the scheduler sees it.
type Slot struct {
	Index      int       `json:"index"`
	State      SlotState `json:"state"`
	VMID       string    `json:"vm_id,omitempty"`
	Generation int64     `json:"gen,omitempty"`
}

// Host is the scheduler's record of one host.
type Host struct {
	ID         string     `json:"id"`
	Rack       string     `json:"rack"`
	SlotCount  int        `json:"slot_count"`
	AdminState AdminState `json:"admin_state"`
	Lease      LeaseState `json:"lease"`
	// LeaseAgeMs is how long since the last accepted heartbeat, on the
	// scheduler's clock.
	LeaseAgeMs int64  `json:"lease_age_ms"`
	Slots      []Slot `json:"slots"`
	// Stale lists copies the host reports at an older generation than the
	// scheduler's record; they are fenced and will be stopped.
	Stale []InventoryItem `json:"stale,omitempty"`
	// Faults lists active chaos faults on this host, e.g. "paused-heartbeats".
	Faults []string `json:"faults,omitempty"`
}

// Rack is a failure-domain label, not a scheduler.
type Rack struct {
	ID          string   `json:"id"`
	Partitioned bool     `json:"partitioned"`
	Hosts       []string `json:"hosts"`
}

// QueueEntry is one row of the placement queue, in dequeue order.
type QueueEntry struct {
	VMID              string      `json:"vm_id"`
	Priority          Priority    `json:"priority"`
	EffectivePriority Priority    `json:"effective_priority"`
	Reason            QueueReason `json:"reason"`
	EnqueuedAt        int64       `json:"enqueued_at"`
	NotBefore         int64       `json:"not_before"`
}
