package scheduler

import (
	"fmt"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/store"
	"github.com/hami-sh/vm/internal/vm"
)

func (p *planner) receive(from api.Addr, msg any) {
	id := from.ID
	switch m := msg.(type) {
	case api.RegisterHost:
		if m.HostID != id || m.Rack != from.Rack || m.Slots <= 0 {
			return
		}
		p.observeEpoch(m.Epoch)
		h, exists := p.hosts[id]
		if !exists {
			h.record = store.HostRecord{ID: id, Rack: m.Rack, Slots: m.Slots, Admin: api.AdminActive, RegisteredAt: p.now.UnixMilli()}
		} else {
			h.record.Rack = m.Rack
			h.record.Slots = m.Slots
		}
		h.seq = 0
		h.lease = api.LeaseHealthy
		h.last = p.now.UnixMilli()
		h.disk = m.DiskFull
		p.hosts[id] = h
		p.data.Hosts[id] = h.record
		p.reconcile(id, m.Inventory, true)
	case api.HostHeartbeat:
		h, ok := p.hosts[id]
		if m.HostID != id {
			return
		}
		if !ok {
			p.observeEpoch(m.Epoch)
			// A re-registration request must be strictly ahead even if a
			// fresh store happens to have the agent's exact epoch already.
			if p.epoch == m.Epoch {
				p.epoch++
				p.data.Epoch = p.epoch
				p.log(api.EventReconcile, "", "", "advanced scheduler epoch for re-registration")
			}
			p.effect.sends = append(p.effect.sends, delivery{from, api.HeartbeatAck{HostID: id, Epoch: p.epoch, Reregister: true}})
			return
		}
		if from.Rack != h.record.Rack {
			return
		}
		p.observeEpoch(m.Epoch)
		// Only RegisterHost resets the sequence window. Seq 1 may be a
		// delayed packet, not evidence of a new agent incarnation.
		if m.Seq <= h.seq {
			// Repeat the request on each stale heartbeat so packet loss cannot
			// strand an agent whose initial registration was dropped.
			p.send(id, api.HeartbeatAck{HostID: id, Epoch: p.epoch, Reregister: true})
			return
		}
		rejoin := h.lease != api.LeaseHealthy
		h.lease = api.LeaseHealthy
		h.last = p.now.UnixMilli()
		h.seq = m.Seq
		h.disk = m.DiskFull
		p.hosts[id] = h
		p.reconcile(id, m.Inventory, rejoin)
		// Missing/older inventory cannot acknowledge a command that was
		// dropped. Retry its exact incarnation until admission is observed.
		for _, res := range p.reservations {
			if res.host == id && !res.admitted {
				p.send(id, res.command)
			}
		}
	case api.StartVMResult:
		if m.HostID != id || !p.current(id, m.VMID, m.Generation) {
			return
		}
		if !m.Admitted && m.Reason == api.ReasonQuarantined {
			p.quarantine(m.VMID)
			return
		}
		r := p.reservations[m.VMID]
		if r.gen != m.Generation {
			return
		}
		if !m.Admitted {
			switch m.Reason {
			case "full", "cordoned", "disk_full", "awaiting_scheduler_ack":
				// Admission never started a boot, so refund just this attempt.
				record := p.data.VMs[m.VMID]
				record.VM.Attempts--
				p.data.VMs[m.VMID] = record
				delete(p.reservations, m.VMID)
				if m.Reason != "awaiting_scheduler_ack" {
					p.exclude[m.VMID] = id
					p.stop(id, m.VMID, m.Generation, api.ReasonRefused)
				} else {
					delete(p.exclude, m.VMID)
				}
				p.enqueue(m.VMID, false, m.Reason)
			default:
				p.retry(m.VMID, api.ReasonRefused)
			}
		} else {
			r.admitted = true
			p.reservations[m.VMID] = r
		}
	case api.ReadyReport:
		if m.HostID != id {
			return
		}
		if !p.current(id, m.VMID, m.Generation) {
			p.stop(id, m.VMID, m.Generation, api.ReasonStale)
			return
		}
		v := p.data.VMs[m.VMID].VM
		if v.Desired == api.DesiredDeleted || v.State == api.VMTerminating {
			p.stop(id, m.VMID, m.Generation, api.ReasonDeleted)
			return
		}
		if v.State == api.VMBooting {
			p.ready(m.VMID)
		}
	case api.UnhealthyReport:
		if m.HostID != id || !p.current(id, m.VMID, m.Generation) {
			return
		}
		if blocked, ok := p.blocked[m.VMID]; ok && m.Reason == api.ReasonGuestLease && blocked.generation == m.Generation && blocked.at.After(p.now.Add(-p.cfg.QuarantineWindow)) {
			p.quarantine(m.VMID)
			return
		}
		v := p.data.VMs[m.VMID].VM
		if v.State == api.VMReady {
			p.move(v.ID, api.VMUnhealthy, m.Reason)
			p.recovering[v.ID] = p.now.UnixMilli()
			p.counters.Recoveries["recycle"]++
			// A healthy guest's previous attempts are not a crash-loop budget.
			r := p.data.VMs[v.ID]
			r.VM.Attempts = 0
			p.data.VMs[v.ID] = r
			p.place(v.ID, id)
		} else if v.State == api.VMBooting {
			p.retry(v.ID, m.Reason)
		}
	case api.StoppedReport:
		if m.HostID != id {
			return
		}
		p.removeCopy(id, m.VMID, m.Generation)
		if m.Reason == api.ReasonQuarantined {
			p.quarantine(m.VMID)
			if p.current(id, m.VMID, m.Generation) && p.data.VMs[m.VMID].VM.State == api.VMTerminating {
				p.move(m.VMID, api.VMTerminated, api.ReasonDeleted)
			}
			return
		}
		if !p.current(id, m.VMID, m.Generation) {
			return
		}
		v := p.data.VMs[m.VMID].VM
		if v.State == api.VMTerminating {
			p.move(v.ID, api.VMTerminated, api.ReasonDeleted)
		} else if v.State == api.VMBooting {
			p.retry(v.ID, "stopped during boot")
			p.removeCopy(id, m.VMID, m.Generation)
		}
	case api.SecurityReport:
		if m.HostID != id {
			return
		}
		if m.Quarantined {
			p.quarantine(m.VMID)
		}
		if !p.current(id, m.VMID, m.Generation) {
			return
		}
		p.blocked[m.VMID] = blockedAttack{generation: m.Generation, at: p.now}
		p.metrics.blocked[m.Attack]++
		p.log(api.EventSecurity, m.VMID, id, m.Attack)
	}
}

// Quarantine belongs to the logical identity, not the reporting incarnation.
// Evidence from an old host must also fence a replacement on another host.
func (p *planner) quarantine(id string) {
	r, ok := p.data.VMs[id]
	if !ok || vm.Terminal(r.VM.State) {
		return
	}
	v := r.VM
	if v.State == api.VMLost {
		p.move(id, api.VMPending, api.ReasonQuarantined)
	}
	// Preserve an in-progress delete until the current copy is clean. It is
	// already desired-deleted and can never be placed again.
	if v.State != api.VMTerminating {
		p.move(id, api.VMFailed, api.ReasonQuarantined)
	}
	delete(p.data.Queue, id)
	delete(p.reservations, id)
	if v.HostID != "" {
		p.stop(v.HostID, id, v.Generation, api.ReasonQuarantined)
	}
}

func (p *planner) current(hostID, id string, gen int64) bool {
	r, ok := p.data.VMs[id]
	return ok && r.VM.HostID == hostID && r.VM.Generation == gen
}

// Epoch changes are part of the same durable transaction as reconciliation.
// The runtime cannot send any of its effects until this counter is committed.
func (p *planner) observeEpoch(reported int64) {
	if reported > p.epoch {
		p.epoch = reported + 1
		p.data.Epoch = p.epoch
		p.log(api.EventReconcile, "", "", "advanced scheduler epoch past host report")
	}
}
func (p *planner) removeCopy(hostID, id string, gen int64) {
	h := p.hosts[hostID]
	out := h.inventory[:0]
	for _, i := range h.inventory {
		if i.VMID != id || i.Generation != gen {
			out = append(out, i)
		}
	}
	h.inventory = out
	p.hosts[hostID] = h
}
func (p *planner) ready(id string) {
	v := p.data.VMs[id].VM
	p.move(id, api.VMReady, "guest ready")
	delete(p.reservations, id)
	h := p.hosts[v.HostID]
	found := false
	for j, i := range h.inventory {
		if i.VMID == id && i.Generation == v.Generation {
			h.inventory[j].State = api.VMReady
			found = true
		}
	}
	if !found {
		h.inventory = append(h.inventory, api.InventoryItem{VMID: id, Generation: v.Generation, State: api.VMReady, Pool: v.Pool})
	}
	p.hosts[v.HostID] = h
	r := p.data.VMs[id]
	if !r.ReachedReady {
		p.metrics.observe("vm_time_to_ready_seconds", v.Priority, float64(p.now.UnixMilli()-v.CreatedAt)/1000)
		r.ReachedReady = true
		p.data.VMs[id] = r
	}
	if at, ok := p.recovering[id]; ok {
		p.metrics.observe("time_to_recover_seconds", v.Priority, float64(p.now.UnixMilli()-at)/1000)
		delete(p.recovering, id)
	}
}

func (p *planner) reconcile(hostID string, inventory []api.InventoryItem, strict bool) {
	h := p.hosts[hostID]
	h.inventory = append([]api.InventoryItem(nil), inventory...)
	p.hosts[hostID] = h
	seen := map[string]bool{}
	stale := []api.InventoryItem{}
	for _, i := range inventory {
		r, known := p.data.VMs[i.VMID]
		if !known {
			_, validPool := pool(i.Pool)
			if p.fresh {
				detail := "store lost unknown VM; idempotency key recovered"
				if i.IdempotencyKey == "" || i.RequestHash == "" {
					detail = "store lost unknown VM; idempotency key lost"
				}
				p.log(api.EventAlert, i.VMID, hostID, detail)
			}
			if p.fresh && i.State == api.VMReady && validPool {
				v := api.VM{ID: i.VMID, Pool: i.Pool, Generation: i.Generation, HostID: hostID, Rack: h.record.Rack, Desired: api.DesiredRunning, Priority: api.PriorityStandard, CreatedAt: p.now.UnixMilli()}
				key := i.IdempotencyKey
				if key != "" {
					for id, existing := range p.data.VMs {
						if existing.Key != key {
							continue
						}
						p.log(api.EventAlert, i.VMID, hostID, "two adopted VMs claim one idempotency key; highest generation retained")
						if i.Generation > existing.VM.Generation {
							existing.Key = ""
							p.data.VMs[id] = existing
							p.request(request{op: "delete", id: id})
						} else {
							key = ""
						}
						break
					}
				}
				if i.IdempotencyKey != "" && key == "" {
					stale = append(stale, i)
					p.stop(hostID, i.VMID, i.Generation, "duplicate recovered key")
					continue
				}
				p.data.VMs[i.VMID] = store.Record{VM: v, Key: key, Hash: i.RequestHash, ReachedReady: true}
				p.move(i.VMID, api.VMReady, "adopted after store loss")
				p.claims[i.VMID] = hostID
				seen[i.VMID] = true
				continue
			}
			stale = append(stale, i)
			if i.State == api.VMFailed {
				p.removeCopy(hostID, i.VMID, i.Generation)
			} else {
				p.stop(hostID, i.VMID, i.Generation, "unknown VM")
			}
			continue
		}
		v := r.VM
		if v.HostID == hostID && v.Generation == i.Generation {
			seen[v.ID] = true
		}
		if i.Generation > v.Generation {
			p.log(api.EventAlert, v.ID, hostID, fmt.Sprintf("store generation %d below reported %d", v.Generation, i.Generation))
			if v.HostID != "" && v.HostID != hostID {
				p.stop(v.HostID, v.ID, v.Generation, api.ReasonStale)
			}
			r.VM.Generation = i.Generation
			p.data.VMs[v.ID] = r
			v = r.VM
			if v.Desired == api.DesiredRunning && !vm.Terminal(v.State) && v.State != api.VMTerminating {
				r.VM.HostID = hostID
				r.VM.Rack = h.record.Rack
				p.data.VMs[v.ID] = r
				p.claims[v.ID] = hostID
				delete(p.data.Queue, v.ID)
				delete(p.reservations, v.ID)
				r = p.data.VMs[v.ID]
				r.Deadline = p.now.Add(p.cfg.BootDeadline).UnixMilli()
				p.data.VMs[v.ID] = r
				if v.State == api.VMReady && i.State != api.VMReady {
					p.move(v.ID, api.VMUnhealthy, "adopt higher generation")
					p.move(v.ID, api.VMBooting, "adopt higher generation")
				}
				if v.State == api.VMLost {
					p.move(v.ID, api.VMPending, "adopt higher generation")
				}
				if p.data.VMs[v.ID].VM.State == api.VMPending || v.State == api.VMUnhealthy {
					p.move(v.ID, api.VMBooting, "adopt higher generation")
				}
			}
			r = p.data.VMs[v.ID]
			if r.VM.State != api.VMBooting {
				r.Deadline = 0
				p.data.VMs[v.ID] = r
			}
			v = p.data.VMs[v.ID].VM
		}
		claim := p.claims[v.ID]
		wrongHost := claim != "" && claim != hostID
		if claim == "" && i.Generation == v.Generation && v.HostID != "" && v.HostID != hostID && p.hosts[v.HostID].lease != api.LeaseLost {
			wrongHost = true
		}
		if i.Generation == v.Generation && wrongHost {
			p.log(api.EventAlert, v.ID, hostID, "equal-generation copies on two hosts; existing owner retained")
		}
		if i.State == api.VMFailed {
			// A quarantine tombstone means the driver has already cleaned this
			// generation. Commit its outcome before acknowledging the exact copy.
			stale = append(stale, i)
			p.removeCopy(hostID, i.VMID, i.Generation)
			p.quarantine(i.VMID)
			if p.current(hostID, i.VMID, i.Generation) {
				seen[v.ID] = true
				if p.data.VMs[v.ID].VM.State == api.VMTerminating {
					p.move(v.ID, api.VMTerminated, api.ReasonDeleted)
				}
			}
			continue
		}
		if i.Generation != v.Generation || wrongHost || v.Desired == api.DesiredDeleted || vm.Terminal(v.State) || v.State == api.VMPending || v.State == api.VMLost || v.State == api.VMTerminating {
			stale = append(stale, i)
			p.stop(hostID, v.ID, i.Generation, api.ReasonStale)
			p.event(api.Event{At: p.now.UnixMilli(), Kind: api.EventFencing, VMID: v.ID, HostID: hostID, Generation: i.Generation, Detail: api.ReasonStale})
			continue
		}
		if v.HostID != hostID {
			p.stop(v.HostID, v.ID, v.Generation, api.ReasonStale)
			r = p.data.VMs[v.ID]
			r.VM.HostID = hostID
			r.VM.Rack = h.record.Rack
			p.data.VMs[v.ID] = r
		}
		p.claims[v.ID] = hostID
		seen[v.ID] = true
		if res, ok := p.reservations[v.ID]; ok && res.gen == i.Generation {
			res.admitted = true
			p.reservations[v.ID] = res
		}
		if i.State == api.VMReady && p.data.VMs[v.ID].VM.State == api.VMBooting {
			p.ready(v.ID)
		}
		if i.State == api.VMUnhealthy && v.State == api.VMReady {
			p.receive(api.Addr{Kind: api.KindAgent, ID: hostID}, api.UnhealthyReport{HostID: hostID, VMID: v.ID, Generation: v.Generation, Reason: api.ReasonGuestLease})
		}
	}
	for id, r := range p.data.VMs {
		v := r.VM
		if v.HostID != hostID || seen[id] || vm.Terminal(v.State) || v.State == api.VMPending || v.State == api.VMLost {
			continue
		}
		if v.State == api.VMTerminating {
			p.move(id, api.VMTerminated, "clean inventory")
			continue
		}
		// Even a registration can have been sent before StartVM arrived.
		// Admission or the existing reservation deadline, not absence in an
		// in-flight inventory, decides whether this boot has been lost.
		if v.State == api.VMBooting {
			res, reserved := p.reservations[id]
			if !strict || (reserved && !res.admitted) {
				continue
			}
		}
		wasReady := v.State == api.VMReady
		p.replacement[id] = wasReady
		p.move(id, api.VMLost, "missing from inventory")
		p.exclude[id] = hostID
		delete(p.reservations, id)
		p.enqueue(id, wasReady, "missing from inventory")
	}
	// A lost/missing host's clean return ends that exclusion. Refusals and
	// genuine boot failures still prefer a different host on their next try.
	for id, excluded := range p.exclude {
		v := p.data.VMs[id].VM
		if excluded == hostID && (v.LastReason == api.ReasonHostLost || v.LastReason == "missing from inventory") {
			delete(p.exclude, id)
		}
	}
	p.send(hostID, api.HeartbeatAck{HostID: hostID, Epoch: p.epoch, Stale: stale})
}
