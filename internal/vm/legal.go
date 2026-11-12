// Package vm holds the VM state machine. The legal-move table is part of the
// Phase 0 contract (invariant 5 checks every logged transition against it);
// W1 owns Transition and everything else in this package.
package vm

import "github.com/hami-sh/vm/internal/api"

// None is the "from" state of a VM row being created.
const None api.VMState = ""

// legal lists every allowed move. Comments name the cause.
var legal = map[api.VMState][]api.VMState{
	None: {
		api.VMPending, // POST /v1/vms
		api.VMReady,   // reconcile after a wiped store: adopt a VM the store lost
	},
	api.VMPending: {
		api.VMBooting,    // placed + admitted (gen+1, persisted before StartVM)
		api.VMFailed,     // shed from a full queue, or retry budget exhausted
		api.VMTerminated, // deleted while queued: queue row removed in the same tx
	},
	api.VMBooting: {
		api.VMReady,       // guestd registered with current token + gen
		api.VMPending,     // deadline missed, host refused, reservation expired: retry with backoff
		api.VMLost,        // host lease expired
		api.VMFailed,      // third attempt failed, or quarantined
		api.VMTerminating, // deleted while booting
	},
	api.VMReady: {
		api.VMUnhealthy,   // guest lease expired or VM exited
		api.VMLost,        // host lease expired, rack partitioned, drained
		api.VMFailed,      // quarantined after repeated attacks
		api.VMTerminating, // deleted
	},
	api.VMUnhealthy: {
		api.VMBooting,     // recycled in place on the same host (gen+1)
		api.VMLost,        // host lease expired before the recycle
		api.VMFailed,      // quarantined, or recycle budget exhausted
		api.VMTerminating, // deleted
	},
	api.VMLost: {
		api.VMPending,    // re-placed (replacement class if it was Ready)
		api.VMTerminated, // desired=deleted: nothing to stop on a lost host
	},
	api.VMTerminating: {
		api.VMTerminated, // host reported the slot clean (or host lost)
	},
	api.VMFailed: {
		api.VMTerminated, // DELETE of a failed VM
	},
	api.VMTerminated: {},
}

// Legal reports whether from -> to is in the legal-move table.
func Legal(from, to api.VMState) bool {
	for _, s := range legal[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Terminal reports whether a VM in s will never run again.
func Terminal(s api.VMState) bool { return s == api.VMTerminated || s == api.VMFailed }
