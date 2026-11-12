package api

// Internal protocol. In the sim these travel over simnet as Go values; between
// real machines they would become gRPC or HTTP. (vm_id, generation) is the
// idempotency key for every command, so receivers need no request-ID cache.

// InventoryItem is one VM a host reports running (or stopping).
type InventoryItem struct {
	VMID       string  `json:"vm_id"`
	Generation int64   `json:"gen"`
	State      VMState `json:"state"` // agent's local view: Booting, Ready, Unhealthy, Terminating
	// Pool is copied from StartVM.Spec.Pool. It lets a scheduler whose store
	// was wiped adopt a VM it has no record of; the spec follows from the pool.
	Pool string `json:"pool,omitempty"`
}

// --- Agent -> Scheduler ---

// RegisterHost is sent when an agent starts or reconnects. It carries the full
// inventory so the scheduler can reconcile (adopt / stop / re-place).
type RegisterHost struct {
	HostID    string          `json:"host_id"`
	Rack      string          `json:"rack"`
	Slots     int             `json:"slots"`
	DiskFull  bool            `json:"disk_full,omitempty"`
	Inventory []InventoryItem `json:"inventory"`
}

// HostHeartbeat renews the host lease and carries the inventory. One per host,
// not one per VM: fan-in at the scheduler is O(hosts).
type HostHeartbeat struct {
	HostID    string          `json:"host_id"`
	Seq       uint64          `json:"seq"`
	DiskFull  bool            `json:"disk_full,omitempty"`
	Inventory []InventoryItem `json:"inventory"`
}

// StartVMResult answers a StartVM.
type StartVMResult struct {
	VMID       string `json:"vm_id"`
	Generation int64  `json:"gen"`
	HostID     string `json:"host_id"`
	Admitted   bool   `json:"admitted"`
	Reason     string `json:"reason,omitempty"` // why refused: full, disk_full, stale_generation, cordoned
}

// ReadyReport tells the scheduler a guest registered with a valid (vm, gen, token).
// Only this message moves a VM to Ready.
type ReadyReport struct {
	HostID     string `json:"host_id"`
	VMID       string `json:"vm_id"`
	Generation int64  `json:"gen"`
}

// UnhealthyReport reports a guest lease expiry or a driver-reported exit.
type UnhealthyReport struct {
	HostID     string `json:"host_id"`
	VMID       string `json:"vm_id"`
	Generation int64  `json:"gen"`
	Reason     string `json:"reason"`
}

// StoppedReport reports that a slot is clean again (Releasing -> Free).
type StoppedReport struct {
	HostID     string `json:"host_id"`
	VMID       string `json:"vm_id"`
	Generation int64  `json:"gen"`
	Reason     string `json:"reason,omitempty"`
}

// SecurityReport reports a blocked attack by a guest. The agent counts and
// quarantines; the scheduler records it.
type SecurityReport struct {
	HostID      string `json:"host_id"`
	VMID        string `json:"vm_id"`
	Generation  int64  `json:"gen"`
	Attack      string `json:"attack"`
	Quarantined bool   `json:"quarantined,omitempty"`
}

// --- Scheduler -> Agent ---

// StartVM asks a host to boot vm at gen. Idempotent: same gen running -> ack;
// lower gen running -> stop it then start; higher gen running -> refuse.
type StartVM struct {
	VMID       string `json:"vm_id"`
	Generation int64  `json:"gen"`
	Token      string `json:"token"` // fresh per boot; the agent checks it when guestd registers
	Spec       VMSpec `json:"spec"`
	Epoch      int64  `json:"epoch"` // scheduler_epoch = max(stored+1, sim unix ms) at start; agents ignore older epochs
}

// StopVM asks a host to stop vm at gen. Not running -> ack; running a higher
// gen -> ignore, so a delayed stop never kills a newer copy.
type StopVM struct {
	VMID       string `json:"vm_id"`
	Generation int64  `json:"gen"`
	Reason     string `json:"reason,omitempty"`
	Epoch      int64  `json:"epoch"`
}

// HeartbeatAck answers a HostHeartbeat or RegisterHost. Stale lists copies the
// host must stop (fencing).
type HeartbeatAck struct {
	HostID string          `json:"host_id"`
	Epoch  int64           `json:"epoch"`
	Stale  []InventoryItem `json:"stale,omitempty"`
}

// --- guestd -> Agent (vsock in real life) ---

// GuestRegister is sent once per boot. The agent ignores the claimed VMID for
// identity: it uses the channel the message arrived on (Envelope.From) and
// treats the token as a second factor.
type GuestRegister struct {
	VMID       string   `json:"vm_id"`
	Generation int64    `json:"gen"`
	Token      string   `json:"token"`
	Checks     []string `json:"checks"` // e.g. "simctl"
}

// GuestHeartbeat renews the guest lease.
type GuestHeartbeat struct {
	VMID       string `json:"vm_id"`
	Generation int64  `json:"gen"`
}

// GuestRegisterResult answers a GuestRegister.
type GuestRegisterResult struct {
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

// Attack names used in SecurityReport and the security metrics.
const (
	AttackImpersonate = "impersonate"  // claim another VM's vm_id
	AttackReplay      = "replay_token" // stale token or newer generation
	AttackFlood       = "flood"        // heartbeat flood / oversized message
	AttackEastWest    = "east_west"    // reach a neighbouring guest
	AttackControl     = "control_plane"
	AttackEgress      = "egress"
	AttackDiskFill    = "disk_fill"
)
