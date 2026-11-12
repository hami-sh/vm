package api

import "context"

// ---------------------------------------------------------------- Transport

// AddrKind is the kind of endpoint on the network. simnet's policy table is
// keyed on (source kind, destination kind, port) and denies guests by default.
type AddrKind string

const (
	KindScheduler AddrKind = "scheduler"
	KindAgent     AddrKind = "agent"
	KindGuest     AddrKind = "guest"
	KindInternet  AddrKind = "internet"
)

// Port names a channel. Guests may only use PortVsock to their own agent.
type Port string

const (
	PortControl Port = "control" // scheduler <-> agent
	PortVsock   Port = "vsock"   // guest -> its own agent
	PortGuest   Port = "guest"   // anything -> a guest (denied guest -> guest)
	PortHTTPS   Port = "https"   // egress
)

// Addr identifies an endpoint. For guests, ID is the VM ID and Host is the
// host the guest runs on; Rack is set for agents and guests so simnet can
// partition a whole rack.
type Addr struct {
	Kind AddrKind `json:"kind"`
	ID   string   `json:"id"`
	Host string   `json:"host,omitempty"`
	Rack string   `json:"rack,omitempty"`
}

// Envelope is a delivered message. From is stamped by the transport from the
// sending endpoint, never by the sender: identity comes from where a message
// arrives, not from what it claims.
type Envelope struct {
	From Addr
	To   Addr
	Port Port
	Msg  any
}

// Endpoint is one attached actor's view of the network.
type Endpoint interface {
	Addr() Addr
	// Send delivers msg asynchronously. It returns ErrDenied if the policy
	// table forbids the connection (counted as a blocked attempt), and nil
	// otherwise even if the message is later dropped by a fault: senders
	// must not assume delivery.
	Send(to Addr, port Port, msg any) error
	// Inbox yields delivered messages. It is closed by Close.
	Inbox() <-chan Envelope
	Close()
}

// Transport connects actors. simnet is the in-memory implementation with
// latency, drops, duplicates, reordering and partitions.
type Transport interface {
	Attach(addr Addr) (Endpoint, error)
}

// ErrDenied is returned by Endpoint.Send when network policy blocks it.
var ErrDenied = denied{}

type denied struct{}

func (denied) Error() string { return "simnet: denied by policy" }

// ------------------------------------------------------------------- Driver

// DriverVM is one VM the hypervisor knows about on a host.
type DriverVM struct {
	VMID       string `json:"vm_id"`
	Generation int64  `json:"gen"`
	Running    bool   `json:"running"`
}

// DriverExit reports a VM that exited without being asked (crash).
type DriverExit struct {
	VMID       string
	Generation int64
	Reason     string
}

// BootParams is what the driver passes to the guest it launches. In real
// life it goes in via a shared directory; in the sim, the FakeDriver hands it
// to the guest goroutine it starts.
type BootParams struct {
	VMID       string
	Generation int64
	Token      string
	Host       string
	Rack       string
}

// Driver wraps one host's hypervisor. Start and Stop are slow (a real boot
// is 30-60 s) and must only be called from worker goroutines, never inside
// an actor's handle. Implementations: driver.Fake (sim), driver.Tart (real).
type Driver interface {
	// Start clones a fresh disk and boots the VM, launching guestd with p.
	// It returns once the hypervisor reports the VM running, not when the
	// guest is Ready; readiness is the guest's GuestRegister.
	Start(ctx context.Context, spec VMSpec, p BootParams) error
	// Stop stops the VM and deletes its disk clone. Stopping a VM that is
	// not running returns nil.
	Stop(ctx context.Context, vmID string, gen int64) error
	// List reports what the hypervisor is running.
	List(ctx context.Context) ([]DriverVM, error)
	// Exits reports VMs that exit on their own.
	Exits() <-chan DriverExit
}
