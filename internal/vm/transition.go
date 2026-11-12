package vm

import (
	"fmt"
	"time"

	"github.com/hami-sh/vm/internal/api"
)

// Transition validates a move and returns its audit event. The caller commits
// the changed record and event together before publishing either.
func Transition(v *api.VM, to api.VMState, reason string, now time.Time) (api.Event, error) {
	if !Legal(v.State, to) {
		return api.Event{}, fmt.Errorf("illegal VM transition %s -> %s", v.State, to)
	}
	e := api.Event{At: now.UnixMilli(), Kind: api.EventTransition, VMID: v.ID,
		HostID: v.HostID, Rack: v.Rack, Generation: v.Generation, From: v.State, To: to, Detail: reason}
	v.State, v.LastReason, v.UpdatedAt = to, reason, now.UnixMilli()
	if to == api.VMReady {
		v.ReadyAt = now.UnixMilli()
	}
	return e, nil
}
