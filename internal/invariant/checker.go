// Package invariant checks published component observations. A Checker belongs
// to the fleet actor; it has no independently shared mutable state.
package invariant

import (
	"fmt"
	"slices"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/vm"
)

type Checker struct {
	cfg            config.Config
	gen            map[string]int64
	stale, pending map[string]int64
	duplicates     map[string]int64
	keyRunning     map[string]int64
	ahead          map[string]int64
	replacements   map[string]bool
	status         map[api.InvariantName]api.InvariantStatus
	seq            int64
}

// Observation comes from the agent actor and its independent driver.List, not
// from the scheduler's cached inventory. Connected is the scheduler's lease
// observation; a disconnected host is allowed to retain fenced stale copies.
type Observation struct {
	Host      api.Host
	Actual    []api.DriverVM
	Connected bool
}

func New(c config.Config) *Checker {
	return &Checker{cfg: c, gen: map[string]int64{}, stale: map[string]int64{}, pending: map[string]int64{}, duplicates: map[string]int64{}, keyRunning: map[string]int64{}, ahead: map[string]int64{}, replacements: map[string]bool{}, status: map[api.InvariantName]api.InvariantStatus{}}
}
func (c *Checker) Status() []api.InvariantStatus {
	out := make([]api.InvariantStatus, 0, len(api.AllInvariants))
	for _, n := range api.AllInvariants {
		s := c.status[n]
		s.Name = n
		out = append(out, s)
	}
	return out
}
func (c *Checker) violation(n api.InvariantName, at int64, detail string) api.Event {
	v := c.status[n]
	v.Name = n
	v.Violations++
	v.LastAt = at
	v.LastDetail = detail
	c.status[n] = v
	return api.Event{At: at, Kind: api.EventInvariant, Detail: string(n) + ": " + detail}
}

// EventsLost makes retention gaps visible instead of silently declaring an
// incomplete transition log legal.
func (c *Checker) EventsLost(at int64) api.Event {
	return c.violation(api.InvLegalTransitions, at, "events lost")
}

// CheckKeys uses independently retained create/launch identity, not the store's
// current key owner: a wiped store can assign the same key to a second VM.
func (c *Checker) CheckKeys(s api.Snapshot, keys map[string]string, observations ...Observation) []api.Event {
	var out []api.Event
	active := map[string]bool{}
	published := map[string]api.VM{}
	for _, v := range s.VMs {
		published[v.ID] = v
		if !vm.Terminal(v.State) {
			active[v.ID] = true
		}
	}
	running := map[string]int64{}
	for _, o := range observations {
		if !o.Connected {
			continue
		}
		for _, actual := range o.Actual {
			if !actual.Running {
				continue
			}
			if v, ok := published[actual.VMID]; ok && actual.Generation > v.Generation {
				continue // Publication lag is checked by generation_monotonic.
			}
			copyID := fmt.Sprintf("%s/%s/%d", o.Host.ID, actual.VMID, actual.Generation)
			at, ok := c.keyRunning[copyID]
			if !ok {
				at = s.At
			}
			running[copyID] = at
			if s.At-at > c.cfg.HostHeartbeat.Milliseconds() {
				active[actual.VMID] = true
			}
		}
	}
	c.keyRunning = running
	owners := map[string]string{}
	ids := make([]string, 0, len(active))
	for id := range active {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		key := keys[id]
		if key == "" {
			continue
		}
		if owner, ok := owners[key]; ok {
			out = append(out, c.violation(api.InvIdempotencyUnique, s.At, "logical VMs "+owner+" and "+id+" share a create key"))
		}
		owners[key] = id
	}
	return out
}
func (c *Checker) Check(s api.Snapshot, observations ...Observation) []api.Event {
	var out []api.Event
	bad := func(n api.InvariantName, detail string) {
		out = append(out, c.violation(n, s.At, detail))
	}
	byID := map[string]api.VM{}
	for _, v := range s.VMs {
		byID[v.ID] = v
		if v.Generation < c.gen[v.ID] {
			bad(api.InvGenMonotonic, v.ID)
		}
		c.gen[v.ID] = max(c.gen[v.ID], v.Generation)
		if (v.Priority == api.PriorityReplacement || c.replacements[v.ID]) && v.LastReason == api.ReasonShed {
			bad(api.InvQueueBounded, "replacement shed: "+v.ID)
		}
	}
	auth := map[string]int{}
	staleNow := map[string]int64{}
	aheadNow := map[string]int64{}
	free := false
	hosts := map[string]api.Host{}
	for _, h := range s.Hosts {
		hosts[h.ID] = h
	}
	for _, o := range observations {
		h := o.Host
		sh, known := hosts[h.ID]
		placeable := known && o.Connected && sh.Lease == api.LeaseHealthy && sh.AdminState == api.AdminActive &&
			h.Lease == api.LeaseHealthy && h.AdminState == api.AdminActive &&
			!slices.Contains(h.Faults, "kill-host") && !slices.Contains(h.Faults, "one-way-partition") &&
			!slices.Contains(h.Faults, "fill-disk") && !slices.Contains(sh.Faults, "fill-disk")
		used := 0
		for _, slot := range h.Slots {
			if slot.State == api.SlotFree && placeable && slices.ContainsFunc(sh.Slots, func(s api.Slot) bool { return s.Index == slot.Index && s.State == api.SlotFree }) {
				free = true
			}
		}
		if !o.Connected {
			continue // Deliberately no global view of disconnected hosts.
		}
		for _, actual := range o.Actual {
			if !actual.Running {
				continue
			}
			used++
			v, ok := byID[actual.VMID]
			if ok && actual.Generation > v.Generation {
				// A durable placement can reach the driver before publication,
				// especially if the scheduler dies before publishing it.
				if s.Scheduler == api.SchedulerUp {
					key := h.ID + "/" + actual.VMID
					at, seen := c.ahead[key]
					if !seen {
						at = s.At
					}
					aheadNow[key] = at
					if s.At-at > c.cfg.HostHeartbeat.Milliseconds() {
						bad(api.InvGenMonotonic, fmt.Sprintf("%s driver generation %d ahead of published %d", key, actual.Generation, v.Generation))
					}
				}
				continue
			}
			if ok && v.State == api.VMReady && actual.Generation == v.Generation {
				auth[v.ID]++
			}
			stale := !ok || actual.Generation != v.Generation || v.HostID != h.ID ||
				v.Desired != api.DesiredRunning || (v.State != api.VMBooting && v.State != api.VMReady && v.State != api.VMUnhealthy)
			if stale {
				key := fmt.Sprintf("%s/%s/%d", h.ID, actual.VMID, actual.Generation)
				at, ok := c.stale[key]
				if !ok {
					at = s.At
				}
				staleNow[key] = at
				if s.At-at > c.cfg.HostHeartbeat.Milliseconds() {
					bad(api.InvStaleStopped, key)
				}
			}
		}
		// This is a single independent hypervisor observation, not a union of
		// asynchronously sampled agent slots and scheduler inventory.
		if used > c.cfg.SlotsPerHost {
			bad(api.InvSlotCapacity, h.ID)
		}
	}
	c.stale = staleNow
	c.ahead = aheadNow
	duplicateNow := map[string]int64{}
	for id, n := range auth {
		if n > 1 {
			at, ok := c.duplicates[id]
			if !ok {
				at = s.At
			}
			duplicateNow[id] = at
			if s.At-at > c.cfg.HostHeartbeat.Milliseconds() {
				bad(api.InvOneAuthoritative, id)
			}
		}
	}
	c.duplicates = duplicateNow
	for _, q := range s.Queue {
		if q.Reason == api.QueueReplace {
			c.replacements[q.VMID] = true
		}
	}
	if len(s.Queue) > c.cfg.QueueCap {
		bad(api.InvQueueBounded, "queue capacity exceeded")
	}
	for _, e := range s.Recent {
		if e.Seq <= c.seq {
			continue
		}
		if e.Kind == api.EventTransition && !vm.Legal(e.From, e.To) {
			bad(api.InvLegalTransitions, e.VMID)
		}
		if e.Kind == api.EventAdmission && e.Detail == api.ReasonShed {
			for _, q := range s.Queue {
				if q.VMID == e.VMID && q.Reason == api.QueueReplace {
					bad(api.InvQueueBounded, "replacement shed")
				}
			}
		}
	}
	for _, e := range s.Recent {
		c.seq = max(c.seq, e.Seq)
	}
	if s.Security.Succeeded > 0 {
		bad(api.InvNoAttackSuccess, "successful guest attack")
	}
	pendingNow := map[string]int64{}
	queue := map[string]api.QueueEntry{}
	for _, q := range s.Queue {
		queue[q.VMID] = q
	}
	for _, v := range s.VMs {
		if v.State != api.VMPending || queue[v.ID].NotBefore > s.At || !free || s.Scheduler != api.SchedulerUp {
			continue
		}
		at, ok := c.pending[v.ID]
		if !ok {
			at = s.At
		}
		pendingNow[v.ID] = at
		if time.Duration(s.At-at)*time.Millisecond > c.cfg.PendingPlaceWithin+c.cfg.HostHeartbeat {
			bad(api.InvPendingPlaced, v.ID)
		}
	}
	c.pending = pendingNow
	return out
}
