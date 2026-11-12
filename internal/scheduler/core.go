package scheduler

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/store"
	"github.com/hami-sh/vm/internal/vm"
)

type planner struct {
	*state
	cfg    config.Config
	now    time.Time
	effect *Effect
}

func (s *Scheduler) handle(now time.Time, msg any) []Effect {
	e := Effect{next: s.state.clone()}
	p := planner{state: e.next, cfg: s.cfg, now: now, effect: &e}
	switch m := msg.(type) {
	case request:
		e.reply = m.reply
		if p.down && m.op != "down" && m.op != "snapshot" && m.op != "metrics" && m.op != "events" {
			e.response.err = &api.Error{Status: 503, Msg: "scheduler down", RetryAfterS: s.retryAfter()}
			break
		}
		e.response = p.request(m)
	case api.Envelope:
		if !p.down && m.From.Kind == api.KindAgent && m.Port == api.PortControl {
			p.receive(m.From, m.Msg)
		}
	case tick:
		if !p.down {
			p.tick()
		}
	}
	if !p.down {
		p.drain()
	}
	// Reply with the committed, possibly immediately placed, VM.
	if _, ok := msg.(request); ok && e.response.err == nil {
		if r, ok := e.response.value.(api.CreateVMResponse); ok {
			v := p.data.VMs[r.ID].VM
			e.response.value = api.CreateVMResponse{ID: v.ID, State: v.State, Generation: v.Generation}
		}
	}
	return []Effect{e}
}

func (p *planner) move(id string, to api.VMState, reason string) {
	r := p.data.VMs[id]
	if r.VM.State == to {
		return
	}
	e, err := vm.Transition(&r.VM, to, reason, p.now)
	if err != nil {
		panic(err)
	}
	if to != api.VMBooting {
		r.Deadline = 0
	}
	p.data.VMs[id] = r
	p.event(e)
}
func (p *planner) event(e api.Event) {
	p.seq++
	e.Seq = p.seq
	p.effect.events = append(p.effect.events, e)
	p.events = append(p.events, e)
	if len(p.events) > p.cfg.EventRetention {
		p.events = p.events[len(p.events)-p.cfg.EventRetention:]
	}
}
func (p *planner) log(kind api.EventKind, id, host, detail string) {
	v := p.data.VMs[id].VM
	p.event(api.Event{At: p.now.UnixMilli(), Kind: kind, VMID: id, HostID: host, Generation: v.Generation, Detail: detail})
}
func (p *planner) send(hostID string, msg any) {
	h := p.hosts[hostID]
	p.effect.sends = append(p.effect.sends, delivery{api.Addr{Kind: api.KindAgent, ID: hostID, Host: hostID, Rack: h.record.Rack}, msg})
}
func (p *planner) stop(hostID, id string, gen int64, reason string) {
	p.send(hostID, api.StopVM{VMID: id, Generation: gen, Epoch: p.epoch, Reason: reason})
}

func priority(s string) (api.Priority, bool) {
	switch s {
	case "interactive":
		return api.PriorityInteractive, true
	case "", "standard":
		return api.PriorityStandard, true
	case "batch":
		return api.PriorityBatch, true
	}
	return 0, false
}
func pool(name string) (api.Pool, bool) {
	for _, p := range config.Pools {
		if p.Name == name {
			return api.Pool{Name: p.Name, Image: p.Image, CPUs: p.CPUs, MemoryMB: p.MemoryMB}, true
		}
	}
	return api.Pool{}, false
}
func failure(status int, msg string) result { return result{err: &api.Error{Status: status, Msg: msg}} }

func (p *planner) request(r request) result {
	switch r.op {
	case "create":
		return p.create(r.key, r.arg.(api.CreateVMRequest))
	case "get":
		v, ok := p.data.VMs[r.id]
		if !ok {
			return failure(404, "VM not found")
		}
		return result{value: v.VM}
	case "vms":
		return result{value: p.snapshot().VMs}
	case "hosts":
		return result{value: p.snapshot().Hosts}
	case "queue":
		return result{value: p.ordered(false)}
	case "snapshot":
		return result{value: p.snapshot()}
	case "metrics":
		return result{value: p.prometheus()}
	case "events":
		after := r.arg.(int64)
		start := sort.Search(len(p.events), func(i int) bool { return p.events[i].Seq > after })
		truncated := len(p.events) > 0 && after < p.events[0].Seq-1
		return result{value: eventResult{events: append([]api.Event{}, p.events[start:]...), truncated: truncated}}
	case "delete":
		r, ok := p.data.VMs[r.id]
		if !ok {
			return failure(404, "VM not found")
		}
		if r.VM.State == api.VMTerminated {
			return result{value: r.VM}
		}
		r.VM.Desired = api.DesiredDeleted
		p.data.VMs[r.VM.ID] = r
		delete(p.data.Queue, r.VM.ID)
		switch r.VM.State {
		case api.VMPending, api.VMLost, api.VMFailed:
			p.move(r.VM.ID, api.VMTerminated, api.ReasonDeleted)
		case api.VMTerminating:
		default:
			p.move(r.VM.ID, api.VMTerminating, api.ReasonDeleted)
			p.stop(r.VM.HostID, r.VM.ID, r.VM.Generation, api.ReasonDeleted)
		}
		delete(p.reservations, r.VM.ID)
		return result{value: p.data.VMs[r.VM.ID].VM}
	case "priority":
		q, ok := p.data.Queue[r.id]
		if !ok {
			return failure(404, "queued VM not found")
		}
		v := r.arg.(api.Priority)
		if v < 1 || v > 3 || q.Reason == api.QueueReplace {
			return failure(422, "replacement priority is reserved")
		}
		q.Priority = v
		p.data.Queue[r.id] = q
	case "down":
		was := p.down
		p.down = r.arg.(bool)
		if was && !p.down {
			for id, h := range p.hosts {
				h.lease = api.LeaseSuspect
				h.last = p.now.UnixMilli()
				p.hosts[id] = h
			}
			p.log(api.EventReconcile, "", "", "scheduler resumed; hosts suspect")
		}
	case "admin":
		h, ok := p.hosts[r.id]
		if !ok {
			return failure(404, "host not found")
		}
		a := r.arg.(api.AdminState)
		if a != api.AdminActive && a != api.AdminCordoned && a != api.AdminDraining && a != api.AdminRemoved {
			return failure(422, "invalid admin state")
		}
		h.record.Admin = a
		p.hosts[r.id] = h
		p.data.Hosts[r.id] = h.record
		if a == api.AdminDraining || a == api.AdminRemoved {
			p.lose(r.id, api.ReasonDrained)
		}
	}
	return result{}
}

func (p *planner) create(key string, req api.CreateVMRequest) result {
	if remaining := p.recoverUntil - p.now.UnixMilli(); remaining > 0 {
		return result{err: &api.Error{Status: 503, Msg: "recovering idempotency keys from hosts", RetryAfterS: int((remaining + 999) / 1000)}}
	}
	// Hash the actual body, before defaults: different bodies with one key
	// are rejected rather than silently treated as equivalent.
	b, _ := json.Marshal(req)
	hash := fmt.Sprintf("%x", sha256.Sum256(b))
	if key != "" {
		for _, r := range p.data.VMs {
			if r.Key == key {
				if r.Hash != hash {
					return failure(422, "idempotency key reused with a different body")
				}
				v := r.VM
				return result{value: api.CreateVMResponse{ID: v.ID, State: v.State, Generation: v.Generation}}
			}
		}
	}
	pr, ok := priority(req.Priority)
	if !ok {
		return failure(422, "invalid priority")
	}
	pl, ok := pool(req.Pool)
	if !ok {
		return failure(422, "unknown pool")
	}
	if req.Image != "" && req.Image != pl.Image {
		return failure(422, "image must match pool")
	}
	active := 0
	for _, r := range p.data.VMs {
		if !vm.Terminal(r.VM.State) {
			active++
		}
	}
	if active >= p.cfg.MaxVMs {
		return p.reject()
	}
	// Drain older work before deciding whether this request needs a queue slot.
	p.drain()
	if len(p.data.Queue) >= p.cfg.QueueCap && p.chooseHost(api.VM{Pool: req.Pool}) == "" {
		if !p.makeRoom(pr, false) {
			return p.reject()
		}
	}
	// The sortable prefix preserves FIFO when the sim clock is paused (many
	// arrivals share a millisecond), including across a scheduler restart.
	// The random suffix prevents collisions with inventory adopted after loss.
	id := fmt.Sprintf("vm-%020d-%020d-%s", p.epoch, p.seq, uuid.NewString())
	now := p.now.UnixMilli()
	v := api.VM{ID: id, Pool: req.Pool, Priority: pr, Desired: api.DesiredRunning, CreatedAt: now, UpdatedAt: now}
	p.data.VMs[id] = store.Record{VM: v, Key: key, Hash: hash}
	p.move(id, api.VMPending, "created")
	if h := p.chooseHost(v); h != "" {
		p.place(id, h)
	} else {
		p.data.Queue[id] = api.QueueEntry{VMID: id, Priority: pr, Reason: api.QueueNew, EnqueuedAt: now, NotBefore: now}
		p.counters.Queued++
		p.log(api.EventAdmission, id, "", "queued")
	}
	return result{value: api.CreateVMResponse{ID: id}}
}
func (p *planner) reject() result {
	p.counters.Rejected429++
	return result{err: &api.Error{Status: 429, Msg: "capacity and queue full", RetryAfterS: int((p.cfg.BackoffBase + time.Second - 1) / time.Second)}}
}

func (p *planner) effective(q api.QueueEntry) api.Priority {
	if q.Reason == api.QueueReplace {
		return 0
	}
	v := q.Priority - api.Priority((p.now.UnixMilli()-q.EnqueuedAt)/p.cfg.AgingStep.Milliseconds())
	if v < 1 {
		return 1
	}
	return v
}
func (p *planner) ordered(eligible bool) []api.QueueEntry {
	out := []api.QueueEntry{}
	for _, q := range p.data.Queue {
		if eligible && q.NotBefore > p.now.UnixMilli() {
			continue
		}
		q.EffectivePriority = p.effective(q)
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.EffectivePriority != b.EffectivePriority {
			return a.EffectivePriority < b.EffectivePriority
		}
		if a.EnqueuedAt != b.EnqueuedAt {
			return a.EnqueuedAt < b.EnqueuedAt
		}
		return a.VMID < b.VMID
	})
	return out
}
func (p *planner) makeRoom(pr api.Priority, internal bool) bool {
	if len(p.data.Queue) < p.cfg.QueueCap {
		return true
	}
	q := p.ordered(false)
	for i := len(q) - 1; i >= 0; i-- {
		v := q[i]
		if v.Reason == api.QueueReplace {
			continue
		}
		if !internal && pr >= v.EffectivePriority {
			return false
		}
		delete(p.data.Queue, v.VMID)
		p.move(v.VMID, api.VMFailed, api.ReasonShed)
		p.counters.Shed++
		p.log(api.EventAdmission, v.VMID, "", "shed")
		return true
	}
	return false
}

// occupied is the union of host observations and optimistic reservations.
// Releasing and stale copies count until a clean inventory or StoppedReport.
func (p *planner) occupied(hostID string) map[string]api.InventoryItem {
	out := map[string]api.InventoryItem{}
	h := p.hosts[hostID]
	for _, i := range h.inventory {
		if previous, ok := out[i.VMID]; !ok || i.Generation > previous.Generation {
			out[i.VMID] = i
		}
	}
	for _, r := range p.data.VMs {
		v := r.VM
		if v.HostID == hostID && (v.State == api.VMBooting || v.State == api.VMTerminating) {
			out[v.ID] = api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: v.State}
		}
	}
	return out
}
func (p *planner) chooseHost(v api.VM) string {
	rackCount := map[string]int{}
	for _, r := range p.data.VMs {
		w := r.VM
		if w.Pool == v.Pool && (w.State == api.VMBooting || w.State == api.VMReady || w.State == api.VMUnhealthy) {
			rackCount[w.Rack]++
		}
	}
	best := ""
	fallback := ""
	bestRack, bestLoad := int(^uint(0)>>1), int(^uint(0)>>1)
	for id, h := range p.hosts {
		load := len(p.occupied(id))
		if h.lease != api.LeaseHealthy || h.record.Admin != api.AdminActive || h.disk || load >= h.record.Slots {
			continue
		}
		if id == p.exclude[v.ID] {
			fallback = id
			continue
		}
		rc := rackCount[h.record.Rack]
		if rc < bestRack || (rc == bestRack && (load < bestLoad || (load == bestLoad && (best == "" || id < best)))) {
			best, bestRack, bestLoad = id, rc, load
		}
	}
	if best == "" {
		return fallback
	}
	return best
}
func (p *planner) place(id, hostID string) {
	r := p.data.VMs[id]
	v := &r.VM
	h := p.hosts[hostID]
	v.Generation++
	v.Attempts++
	v.HostID = hostID
	v.Rack = h.record.Rack
	r.Deadline = p.now.Add(p.cfg.BootDeadline).UnixMilli()
	p.data.VMs[id] = r
	p.move(id, api.VMBooting, "placed")
	p.claims[id] = hostID
	if q, ok := p.data.Queue[id]; ok {
		p.metrics.observe("queue_wait_seconds", q.Priority, float64(p.now.UnixMilli()-q.EnqueuedAt)/1000)
		delete(p.data.Queue, id)
	}
	p.reservations[id] = reservation{host: hostID, gen: v.Generation, until: p.now.Add(p.cfg.ReservationTTL).UnixMilli()}
	pl, _ := pool(v.Pool)
	p.send(hostID, api.StartVM{VMID: id, Generation: v.Generation, Epoch: p.epoch, Token: uuid.NewString(), Spec: api.VMSpec{Pool: pl.Name, Image: pl.Image, CPUs: pl.CPUs, MemoryMB: pl.MemoryMB}, IdempotencyKey: r.Key, RequestHash: r.Hash})
	p.counters.Placed++
	p.log(api.EventAdmission, id, hostID, "placed")
}
func (p *planner) drain() {
	for {
		q := p.ordered(true)
		if len(q) > p.cfg.QueueScan {
			q = q[:p.cfg.QueueScan]
		}
		placed := false
		for _, q := range q {
			v := p.data.VMs[q.VMID].VM
			if h := p.chooseHost(v); h != "" {
				p.place(v.ID, h)
				placed = true
				break
			}
		}
		if !placed {
			return
		}
	}
}

func (p *planner) enqueue(id string, replacement bool, reason string) {
	r := p.data.VMs[id]
	v := r.VM
	if v.Desired == api.DesiredDeleted {
		if v.State == api.VMLost {
			p.move(id, api.VMTerminated, api.ReasonDeleted)
		}
		return
	}
	if !replacement && v.Attempts >= p.cfg.MaxAttempts {
		if v.State == api.VMLost {
			p.move(id, api.VMPending, reason)
		}
		p.move(id, api.VMFailed, api.ReasonRetryBudget)
		return
	}
	pr := v.Priority
	qr := api.QueueRetry
	nb := p.now.UnixMilli()
	enqueued := v.CreatedAt
	if replacement {
		pr = 0
		qr = api.QueueReplace
		enqueued = nb
		p.replacement[id] = true
	} else {
		backoff := p.cfg.BackoffBase
		for i := 1; i < v.Attempts && backoff < p.cfg.BackoffMax; i++ {
			backoff *= 2
		}
		if backoff > p.cfg.BackoffMax {
			backoff = p.cfg.BackoffMax
		}
		if backoff > 0 {
			nb = p.now.Add(time.Duration(rand.Int64N(int64(backoff) + 1))).UnixMilli()
		}
	}
	if !p.makeRoom(pr, true) {
		if v.State == api.VMBooting {
			p.move(id, api.VMPending, reason)
		}
		return
	}
	if v.State != api.VMPending {
		p.move(id, api.VMPending, reason)
	}
	r = p.data.VMs[id]
	r.VM.HostID = ""
	r.VM.Rack = ""
	r.Deadline = 0
	if replacement {
		r.VM.Attempts = 0
	}
	p.data.VMs[id] = r
	p.data.Queue[id] = api.QueueEntry{VMID: id, Priority: pr, Reason: qr, EnqueuedAt: enqueued, NotBefore: nb}
	kind := "retry"
	if replacement {
		kind = "replace"
	}
	p.counters.Recoveries[kind]++
}

func (p *planner) retry(id, reason string) {
	v := p.data.VMs[id].VM
	p.stop(v.HostID, id, v.Generation, reason)
	p.exclude[id] = v.HostID
	// Keep the releasing copy in host capacity until its clean report.
	h := p.hosts[v.HostID]
	h.inventory = append(h.inventory, api.InventoryItem{VMID: id, Generation: v.Generation, State: api.VMTerminating})
	p.hosts[v.HostID] = h
	delete(p.reservations, id)
	p.enqueue(id, false, reason)
}
func (p *planner) lose(hostID, reason string) {
	for id, r := range p.data.VMs {
		v := r.VM
		if v.HostID != hostID || vm.Terminal(v.State) || v.State == api.VMPending || v.State == api.VMLost {
			continue
		}
		delete(p.reservations, id)
		if v.Desired == api.DesiredDeleted || v.State == api.VMTerminating {
			p.move(id, api.VMTerminated, reason)
			continue
		}
		replacement := v.State == api.VMReady
		p.replacement[id] = replacement
		p.recovering[id] = p.now.UnixMilli()
		p.move(id, api.VMLost, reason)
		p.exclude[id] = hostID
		p.enqueue(id, replacement, reason)
	}
}
func (p *planner) tick() {
	now := p.now.UnixMilli()
	for id, h := range p.hosts {
		age := now - h.last
		switch {
		case age >= (p.cfg.HostLeaseTTL + p.cfg.HostLostGrace).Milliseconds():
			if h.lease != api.LeaseLost {
				h.lease = api.LeaseLost
				p.hosts[id] = h
				p.lose(id, api.ReasonHostLost)
			}
		case age >= p.cfg.HostLeaseTTL.Milliseconds():
			h.lease = api.LeaseSuspect
			p.hosts[id] = h
		}
	}
	for id, r := range p.data.VMs {
		v := r.VM
		if v.State == api.VMLost {
			p.enqueue(id, p.replacement[id], api.ReasonHostLost)
			continue
		}
		if v.State == api.VMPending {
			if _, ok := p.data.Queue[id]; !ok {
				p.enqueue(id, false, "retry")
			}
			continue
		}
		if v.State != api.VMBooting {
			continue
		}
		h := p.hosts[v.HostID]
		if h.lease != api.LeaseHealthy {
			continue
		}
		if r.Deadline > 0 && now >= r.Deadline {
			p.retry(id, api.ReasonBootDeadline)
			continue
		}
		if res, ok := p.reservations[id]; ok && !res.admitted && now >= res.until {
			p.retry(id, api.ReasonReservationTTL)
		}
	}
}
