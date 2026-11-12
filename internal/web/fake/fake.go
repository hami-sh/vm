// Package fake is a stand-in fleet for building and testing the dashboard
// without W1 and W3: it serves fixtures/snapshot.json, advances its sim time,
// and applies a canned chaos catalogue to the snapshot so clicks have
// visible effects. It is a toy, not a model of the real system.
package fake

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
)

// LoadFixture reads a snapshot such as fixtures/snapshot.json.
func LoadFixture(path string) (api.Snapshot, error) {
	var s api.Snapshot
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	return s, json.Unmarshal(b, &s)
}

// Catalog mirrors the real chaos catalogue's names, targets and params.
func Catalog() api.ChaosCatalog {
	a := func(name, label, layer string, t api.TargetKind, ambient bool, params ...string) api.ChaosAction {
		return api.ChaosAction{Name: name, Label: label, Layer: layer, Target: t, Ambient: ambient, Params: params}
	}
	return api.ChaosCatalog{
		Actions: []api.ChaosAction{
			a("kill-guestd", "Kill guestd", "guest", api.TargetVM, true),
			a("crash-vm", "Crash VM", "guest", api.TargetVM, true),
			a("hang-next-boot", "Hang next boot", "guest", api.TargetVM, true),
			a("slow-boot", "Slow boot", "guest", api.TargetHost, true, "factor"),
			a("compromise", "Compromise guest", "guest", api.TargetVM, true),
			a("kill-host", "Kill host", "host", api.TargetHost, true),
			a("pause-heartbeats", "Pause heartbeats", "host", api.TargetHost, true),
			a("restart-host", "Restart host", "host", api.TargetHost, true),
			a("clock-skew", "Clock skew", "host", api.TargetHost, true, "ms"),
			a("fill-disk", "Fill disk", "host", api.TargetHost, true),
			a("cordon", "Cordon", "host", api.TargetHost, true),
			a("partition-rack", "Partition rack", "network", api.TargetRack, true),
			a("one-way-partition", "One-way partition", "network", api.TargetHost, true),
			a("latency-spike", "Latency spike", "network", api.TargetGlobal, true, "ms"),
			a("packet-loss", "Packet loss", "network", api.TargetGlobal, true, "percent"),
			a("duplicate-messages", "Duplicate messages", "network", api.TargetGlobal, true),
			a("reorder-messages", "Reorder messages", "network", api.TargetGlobal, true),
			a("kill-scheduler", "Kill scheduler", "control", api.TargetScheduler, true),
			a("wipe-store", "Wipe store", "control", api.TargetScheduler, true),
			a("slow-store", "Slow store", "control", api.TargetScheduler, true, "ms"),
			a("flood", "Flood N", "load", api.TargetGlobal, true, "count"),
			a("sustained-load", "Sustained load", "load", api.TargetGlobal, true, "rate", "interactive", "standard", "batch"),
			a("burst-after-rack-loss", "Burst after rack loss", "load", api.TargetRack, true, "count"),
			a("add-rack", "Add rack", "topology", api.TargetGlobal, true, "hosts"),
			a("remove-rack", "Drain and remove rack", "topology", api.TargetRack, true),
			a("heal-rack", "Heal rack", "network", api.TargetRack, false),
			a("power-off-rack", "Power off rack", "host", api.TargetRack, false),
			a("resume-heartbeats", "Resume heartbeats", "host", api.TargetHost, false),
			a("restart-scheduler", "Restart scheduler", "control", api.TargetScheduler, false),
			a("uncordon", "Uncordon", "host", api.TargetHost, false),
			a("drain", "Drain", "topology", api.TargetHost, false),
			a("add-host", "Add host", "topology", api.TargetRack, false),
			a("impostor-register", "Impostor register", "guest", api.TargetVM, false),
			a("delete", "Delete VM", "guest", api.TargetVM, false),
			a("reset", "Reset fleet", "control", api.TargetGlobal, false),
			a("noise", "Ambient chaos", "ambient", api.TargetGlobal, false, "intensity"),
		},
		Presets: []api.ChaosAction{
			a("split-brain", "Split brain", "", api.TargetGlobal, false),
			a("gray-failure", "Gray failure", "", api.TargetGlobal, false),
			a("thundering-herd", "Thundering herd", "", api.TargetGlobal, false),
			a("rolling-maintenance", "Rolling maintenance", "", api.TargetGlobal, false),
			a("hostile-tenant", "Hostile tenant", "", api.TargetGlobal, false),
			a("bad-day", "Bad day", "", api.TargetGlobal, false),
		},
	}
}

// Fleet implements api.ControlPlane, api.ChaosEngine and api.SnapshotSource
// over one in-memory snapshot. Step advances it; Run calls Step on a ticker.
type Fleet struct {
	mu      sync.Mutex
	base    api.Snapshot
	s       api.Snapshot
	rng     *rand.Rand
	seq     int64 // event seq
	pending []api.Event
	keys    map[string]string
	nextVM  int
	subs    map[*sub]struct{}
}

type sub struct {
	snaps  chan api.Snapshot
	events chan api.Event
}

// New starts from base (usually the fixture).
func New(base api.Snapshot) *Fleet {
	f := &Fleet{base: clone(base), subs: map[*sub]struct{}{}}
	f.resetLocked()
	return f
}

func clone(s api.Snapshot) api.Snapshot {
	b, _ := json.Marshal(s)
	var out api.Snapshot
	_ = json.Unmarshal(b, &out)
	return out
}

func (f *Fleet) resetLocked() {
	f.s = clone(f.base)
	f.rng = rand.New(rand.NewSource(f.s.Sim.Seed))
	f.keys = map[string]string{}
	f.nextVM = len(f.s.VMs) + 1
	for _, e := range f.s.Recent {
		f.seq = max(f.seq, e.Seq)
	}
}

// Run advances sim time every 100 ms of wall time on c until ctx ends.
func (f *Fleet) Run(ctx context.Context, c clock.Clock) {
	t := c.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
			f.Step(100 * time.Millisecond)
		}
	}
}

// Step advances sim time by wall*speed (unless paused) and publishes a snapshot.
func (f *Fleet) Step(wall time.Duration) {
	f.mu.Lock()
	if !f.s.Sim.Paused {
		f.advanceLocked(time.Duration(float64(wall) * f.s.Sim.Speed))
	}
	f.s.Seq++
	f.s.Recent = f.pending
	f.pending = nil
	snap := clone(f.s)
	subs := slices.Collect(maps.Keys(f.subs))
	f.mu.Unlock()
	for _, s := range subs {
		select {
		case s.snaps <- clone(snap):
		default:
		}
	}
}

func (f *Fleet) advanceLocked(d time.Duration) {
	s := &f.s
	s.At += d.Milliseconds()
	// Latency wanders and rises with chaos intensity and queue depth.
	k := 1 + 1.5*s.Sim.Intensity + float64(len(s.Queue))/40
	if s.Latency == nil {
		s.Latency = map[string]api.LatencyStats{}
	}
	for class, base := range map[string][2]float64{"interactive": {4800, 9100}, "standard": {5200, 14200}, "batch": {21000, 58000}} {
		l := s.Latency[class]
		t := float64(s.At) / 9000
		w := 1 + 0.12*math.Sin(t) + 0.06*math.Sin(t*2.7+1) + 0.05*(f.rng.Float64()-0.5)
		l.P50Ms = int64(base[0] * k * w)
		l.P99Ms = int64(base[1] * k * (w + 0.08*math.Sin(t*1.3)))
		l.Samples++
		s.Latency[class] = l
	}
	for i := range s.Hosts {
		h := &s.Hosts[i]
		if h.Lease == api.LeaseHealthy {
			h.LeaseAgeMs = (h.LeaseAgeMs + d.Milliseconds()) % 2000
		} else {
			h.LeaseAgeMs += d.Milliseconds()
		}
	}
	// Sustained load: occasionally admit a new VM to the queue.
	if s.Sim.LoadRate > 0 && f.rng.Float64() < s.Sim.LoadRate*d.Seconds() {
		f.enqueueLocked(api.Priority(1+f.rng.Intn(3)), "load")
	}
	// Ambient chaos: an occasional chaos event from the enabled filters.
	if s.Sim.Intensity > 0 && len(s.Sim.Filters) > 0 && f.rng.Float64() < s.Sim.Intensity*0.2*d.Seconds() {
		a := s.Sim.Filters[f.rng.Intn(len(s.Sim.Filters))]
		f.emitLocked(api.Event{Kind: api.EventChaos, Detail: "ambient " + a})
	}
}

func (f *Fleet) emitLocked(e api.Event) {
	f.seq++
	e.Seq = f.seq
	if e.At == 0 {
		e.At = f.s.At
	}
	f.pending = append(f.pending, e)
	for s := range f.subs {
		select {
		case s.events <- e:
		default:
		}
	}
}

// ------------------------------------------------------------ SnapshotSource

func (f *Fleet) Latest() api.Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return clone(f.s)
}

func (f *Fleet) Subscribe() (<-chan api.Snapshot, <-chan api.Event, func()) {
	s := &sub{snaps: make(chan api.Snapshot, 1), events: make(chan api.Event, 64)}
	f.mu.Lock()
	f.subs[s] = struct{}{}
	s.snaps <- clone(f.s)
	f.mu.Unlock()
	var once sync.Once
	return s.snaps, s.events, func() {
		once.Do(func() {
			f.mu.Lock()
			delete(f.subs, s)
			f.mu.Unlock()
		})
	}
}

// -------------------------------------------------------------- ControlPlane

func notFound(what string) error {
	return &api.Error{Status: http.StatusNotFound, Msg: what + " not found"}
}

func (f *Fleet) down() error {
	if f.s.Scheduler != api.SchedulerUp {
		return &api.Error{Status: http.StatusServiceUnavailable, Msg: "scheduler " + string(f.s.Scheduler), RetryAfterS: 2}
	}
	return nil
}

func (f *Fleet) CreateVM(_ context.Context, key string, req api.CreateVMRequest) (api.CreateVMResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.down(); err != nil {
		return api.CreateVMResponse{}, err
	}
	body := req.Pool + "|" + req.Image + "|" + req.Priority
	if id, ok := f.keys[key]; ok {
		if !strings.HasSuffix(id, "|"+body) {
			return api.CreateVMResponse{}, &api.Error{Status: http.StatusUnprocessableEntity, Msg: "idempotency key reused with a different body"}
		}
		vm, _ := f.vmLocked(strings.SplitN(id, "|", 2)[0])
		return api.CreateVMResponse{ID: vm.ID, State: vm.State, Generation: vm.Generation}, nil
	}
	p := map[string]api.Priority{"interactive": 1, "": 2, "standard": 2, "batch": 3}[req.Priority]
	vm, err := f.enqueueLocked(p, "api")
	if err != nil {
		return api.CreateVMResponse{}, err
	}
	f.keys[key] = vm.ID + "|" + body
	return api.CreateVMResponse{ID: vm.ID, State: vm.State, Generation: vm.Generation}, nil
}

func (f *Fleet) enqueueLocked(p api.Priority, why string) (api.VM, error) {
	s := &f.s
	if len(s.Queue) >= s.Counters.QueueCap {
		s.Counters.Rejected429++
		f.emitLocked(api.Event{Kind: api.EventAdmission, Detail: fmt.Sprintf("rejected 429: queue full (%d)", len(s.Queue))})
		return api.VM{}, &api.Error{Status: http.StatusTooManyRequests, Msg: "queue full", RetryAfterS: 2}
	}
	vm := api.VM{ID: fmt.Sprintf("vm-%04d", f.nextVM), Pool: s.Pools[f.nextVM%len(s.Pools)].Name,
		State: api.VMPending, Desired: api.DesiredRunning, Priority: p, CreatedAt: s.At, UpdatedAt: s.At}
	f.nextVM++
	s.VMs = append(s.VMs, vm)
	s.Queue = append(s.Queue, api.QueueEntry{VMID: vm.ID, Priority: p, EffectivePriority: p,
		Reason: api.QueueNew, EnqueuedAt: s.At, NotBefore: s.At})
	slices.SortStableFunc(s.Queue, func(a, b api.QueueEntry) int { return int(a.EffectivePriority) - int(b.EffectivePriority) })
	s.Counters.Queued++
	f.recountLocked()
	f.emitLocked(api.Event{Kind: api.EventTransition, VMID: vm.ID, To: api.VMPending, Detail: "created (" + why + ")"})
	return vm, nil
}

func (f *Fleet) vmLocked(id string) (*api.VM, bool) {
	for i := range f.s.VMs {
		if f.s.VMs[i].ID == id {
			return &f.s.VMs[i], true
		}
	}
	return nil, false
}

func (f *Fleet) GetVM(_ context.Context, id string) (api.VM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.vmLocked(id); ok {
		return *v, nil
	}
	return api.VM{}, notFound("vm " + id)
}

func (f *Fleet) ListVMs(context.Context) ([]api.VM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.s.VMs), nil
}

func (f *Fleet) DeleteVM(_ context.Context, id string) (api.VM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleteLocked(id)
}

func (f *Fleet) deleteLocked(id string) (api.VM, error) {
	if err := f.down(); err != nil {
		return api.VM{}, err
	}
	v, ok := f.vmLocked(id)
	if !ok {
		return api.VM{}, notFound("vm " + id)
	}
	if v.Desired == api.DesiredDeleted || v.State == api.VMTerminated {
		return *v, nil
	}
	v.Desired = api.DesiredDeleted
	if v.State == api.VMPending || v.State == api.VMFailed || v.State == api.VMLost {
		f.moveLocked(v, api.VMTerminated, api.ReasonDeleted)
		f.s.Queue = slices.DeleteFunc(f.s.Queue, func(q api.QueueEntry) bool { return q.VMID == id })
	} else {
		f.moveLocked(v, api.VMTerminating, api.ReasonDeleted)
		f.setSlotLocked(v.HostID, v.ID, api.SlotReleasing)
	}
	f.recountLocked()
	return *v, nil
}

func (f *Fleet) ListHosts(context.Context) ([]api.Host, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return clone(f.s).Hosts, nil
}

func (f *Fleet) Queue(context.Context) ([]api.QueueEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.s.Queue), nil
}

func (f *Fleet) SetPriority(_ context.Context, id string, p api.Priority) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.s.Queue {
		q := &f.s.Queue[i]
		if q.VMID != id {
			continue
		}
		if q.Priority == api.PriorityReplacement {
			return &api.Error{Status: http.StatusConflict, Msg: "replacements stay in class 0"}
		}
		q.Priority, q.EffectivePriority = p, p
		if v, ok := f.vmLocked(id); ok {
			v.Priority = p
		}
		slices.SortStableFunc(f.s.Queue, func(a, b api.QueueEntry) int { return int(a.EffectivePriority) - int(b.EffectivePriority) })
		f.emitLocked(api.Event{Kind: api.EventAdmission, VMID: id, Detail: fmt.Sprintf("priority set to class %d", p)})
		return nil
	}
	return notFound("queued vm " + id)
}

// --------------------------------------------------------------- ChaosEngine

func (f *Fleet) Catalog() api.ChaosCatalog { return Catalog() }

func invalid(msg string) error { return &api.Error{Status: http.StatusUnprocessableEntity, Msg: msg} }

func (f *Fleet) hostLocked(id string) *api.Host {
	for i := range f.s.Hosts {
		if f.s.Hosts[i].ID == id {
			return &f.s.Hosts[i]
		}
	}
	return nil
}

func (f *Fleet) rackLocked(id string) *api.Rack {
	for i := range f.s.Racks {
		if f.s.Racks[i].ID == id {
			return &f.s.Racks[i]
		}
	}
	return nil
}

func (f *Fleet) moveLocked(v *api.VM, to api.VMState, why string) {
	from := v.State
	v.State, v.LastReason, v.UpdatedAt = to, why, f.s.At
	if to == api.VMReady {
		v.ReadyAt = f.s.At
	}
	f.emitLocked(api.Event{Kind: api.EventTransition, VMID: v.ID, HostID: v.HostID, Rack: v.Rack,
		Generation: v.Generation, From: from, To: to, Detail: why})
}

func (f *Fleet) setSlotLocked(host, vm string, st api.SlotState) {
	if h := f.hostLocked(host); h != nil {
		for i := range h.Slots {
			if h.Slots[i].VMID == vm {
				h.Slots[i].State = st
			}
		}
	}
}

func (f *Fleet) recountLocked() {
	c := &f.s.Counters
	c.ByState = map[api.VMState]int{}
	for _, v := range f.s.VMs {
		c.ByState[v.State]++
	}
	c.QueueDepth = len(f.s.Queue)
	free, busiest := 0, 0
	for _, r := range f.s.Racks {
		n := 0
		for _, id := range r.Hosts {
			h := f.hostLocked(id)
			if h == nil {
				continue
			}
			for _, sl := range h.Slots {
				if sl.State == api.SlotFree && h.Lease == api.LeaseHealthy && h.AdminState == api.AdminActive {
					free++
				} else if sl.State != api.SlotFree {
					n++
				}
			}
		}
		busiest = max(busiest, n)
	}
	c.Headroom = free - busiest
}

func addFault(h *api.Host, fault string) {
	if !slices.Contains(h.Faults, fault) {
		h.Faults = append(h.Faults, fault)
	}
}

func dropFault(h *api.Host, fault string) {
	h.Faults = slices.DeleteFunc(h.Faults, func(s string) bool { return s == fault })
}

func (f *Fleet) loseHostLocked(h *api.Host, fault string) {
	addFault(h, fault)
	h.Lease, h.LeaseAgeMs = api.LeaseLost, 12000
	for _, sl := range h.Slots {
		if v, ok := f.vmLocked(sl.VMID); ok && v.HostID == h.ID && v.State != api.VMLost {
			f.moveLocked(v, api.VMLost, api.ReasonHostLost)
		}
	}
}

func (f *Fleet) Do(_ context.Context, action string, req api.ChaosRequest) (api.ChaosResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cat := Catalog()
	all := append(cat.Actions, cat.Presets...)
	i := slices.IndexFunc(all, func(a api.ChaosAction) bool { return a.Name == action })
	if i < 0 {
		return api.ChaosResult{}, invalid("unknown chaos action: " + action)
	}
	target := all[i].Target
	var (
		v *api.VM
		h *api.Host
		r *api.Rack
	)
	switch target {
	case api.TargetVM:
		if v, _ = f.vmLocked(req.Target); v == nil {
			return api.ChaosResult{}, notFound("vm " + req.Target)
		}
	case api.TargetHost:
		if h = f.hostLocked(req.Target); h == nil {
			return api.ChaosResult{}, notFound("host " + req.Target)
		}
	case api.TargetRack:
		if r = f.rackLocked(req.Target); r == nil {
			return api.ChaosResult{}, notFound("rack " + req.Target)
		}
	}
	if action == "reset" {
		f.resetLocked()
		f.emitLocked(api.Event{Kind: api.EventChaos, Detail: "reset"})
		return api.ChaosResult{Action: action, Detail: "fleet reset"}, nil
	}
	ev := api.Event{Kind: api.EventChaos, Detail: action, VMID: req.Target}
	switch target {
	case api.TargetHost:
		ev = api.Event{Kind: api.EventChaos, Detail: action, HostID: h.ID, Rack: h.Rack}
	case api.TargetRack:
		ev = api.Event{Kind: api.EventChaos, Detail: action, Rack: r.ID}
	case api.TargetVM:
		ev = api.Event{Kind: api.EventChaos, Detail: action, VMID: v.ID, HostID: v.HostID, Rack: v.Rack}
	case api.TargetScheduler, api.TargetGlobal:
		ev = api.Event{Kind: api.EventChaos, Detail: action}
	}
	f.emitLocked(ev)
	switch action {
	case "kill-guestd", "crash-vm":
		if v.State == api.VMReady {
			f.moveLocked(v, api.VMUnhealthy, api.ReasonGuestLease)
		}
	case "compromise":
		v.Compromised = true
	case "impostor-register":
		f.s.Security.Blocked++
		f.s.Security.ByAttack[api.AttackImpersonate]++
		f.emitLocked(api.Event{Kind: api.EventSecurity, VMID: v.ID, HostID: v.HostID, Detail: "blocked impersonate: token mismatch"})
	case "delete":
		if _, err := f.deleteLocked(v.ID); err != nil {
			return api.ChaosResult{}, err
		}
	case "kill-host", "restart-host":
		f.loseHostLocked(h, "killed")
	case "pause-heartbeats":
		addFault(h, "paused-heartbeats")
		h.Lease, h.LeaseAgeMs = api.LeaseSuspect, 6500
	case "resume-heartbeats":
		dropFault(h, "paused-heartbeats")
		h.Lease, h.LeaseAgeMs = api.LeaseHealthy, 0
	case "fill-disk", "slow-boot", "clock-skew", "one-way-partition":
		addFault(h, action)
	case "cordon":
		h.AdminState = api.AdminCordoned
	case "uncordon":
		h.AdminState = api.AdminActive
	case "drain":
		h.AdminState = api.AdminDraining
	case "partition-rack":
		r.Partitioned = true
		for _, id := range r.Hosts {
			f.hostLocked(id).Lease = api.LeaseSuspect
		}
	case "heal-rack":
		r.Partitioned = false
		for _, id := range r.Hosts {
			if hh := f.hostLocked(id); !slices.Contains(hh.Faults, "killed") {
				hh.Lease, hh.LeaseAgeMs = api.LeaseHealthy, 0
			}
		}
	case "power-off-rack", "burst-after-rack-loss":
		for _, id := range r.Hosts {
			f.loseHostLocked(f.hostLocked(id), "killed")
		}
	case "remove-rack":
		for _, id := range r.Hosts {
			f.hostLocked(id).AdminState = api.AdminDraining
		}
	case "add-host":
		f.addHostLocked(r)
	case "add-rack":
		f.addRackLocked(int(req.Params["hosts"]))
	case "kill-scheduler", "wipe-store":
		f.s.Scheduler = api.SchedulerDown
	case "restart-scheduler":
		f.s.Scheduler = api.SchedulerUp
		f.s.Epoch++
		f.emitLocked(api.Event{Kind: api.EventReconcile, Detail: "reconciled: adopted all, stopped 0, re-placed 0"})
	case "flood":
		n := int(req.Params["count"])
		if n <= 0 {
			n = 60
		}
		for range n {
			if _, err := f.enqueueLocked(api.Priority(1+f.rng.Intn(3)), "flood"); err != nil {
				continue
			}
		}
	case "sustained-load":
		f.s.Sim.LoadRate = req.Params["rate"]
	case "noise":
		f.s.Sim.Intensity = req.Params["intensity"]
	}
	f.recountLocked()
	return api.ChaosResult{Action: action, Target: req.Target, Detail: "applied"}, nil
}

func (f *Fleet) addHostLocked(r *api.Rack) {
	id := fmt.Sprintf("%s-h%d", r.ID, len(r.Hosts)+1)
	r.Hosts = append(r.Hosts, id)
	f.s.Hosts = append(f.s.Hosts, api.Host{ID: id, Rack: r.ID, SlotCount: 2, AdminState: api.AdminActive,
		Lease: api.LeaseHealthy, Slots: []api.Slot{{Index: 0, State: api.SlotFree}, {Index: 1, State: api.SlotFree}}})
	f.emitLocked(api.Event{Kind: api.EventTopology, HostID: id, Rack: r.ID, Detail: "host registered"})
}

func (f *Fleet) addRackLocked(hosts int) {
	if hosts <= 0 {
		hosts = 4
	}
	f.s.Racks = append(f.s.Racks, api.Rack{ID: fmt.Sprintf("r%d", len(f.s.Racks)+1)})
	r := &f.s.Racks[len(f.s.Racks)-1]
	f.emitLocked(api.Event{Kind: api.EventTopology, Rack: r.ID, Detail: "rack added"})
	for range hosts {
		f.addHostLocked(r)
	}
}

func (f *Fleet) Sim(_ context.Context, action string, req api.SimRequest) (api.SimStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := &f.s.Sim
	switch action {
	case "status":
	case "pause":
		st.Paused = true
	case "play":
		st.Paused = false
	case "speed":
		st.Speed = req.Speed
	case "step":
		if !st.Paused || req.StepMs <= 0 {
			return *st, invalid("step requires a paused clock and step_ms > 0")
		}
		f.advanceLocked(time.Duration(req.StepMs) * time.Millisecond)
	case "chaos":
		if req.Intensity != nil {
			st.Intensity = *req.Intensity
		}
		if req.Filters != nil {
			st.Filters = slices.Clone(req.Filters)
		}
		if req.Racks != nil {
			st.Racks = slices.Clone(req.Racks)
		}
		if req.LoadRate != nil {
			st.LoadRate = *req.LoadRate
		}
		if req.Seed != 0 {
			st.Seed = req.Seed
		}
	default:
		return *st, invalid("unknown sim action: " + action)
	}
	return *st, nil
}

func (f *Fleet) Topology(_ context.Context, action string, req api.TopologyRequest) (api.ChaosResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch action {
	case "add-rack":
		f.addRackLocked(req.Hosts)
	case "add-host":
		r := f.rackLocked(req.Rack)
		if r == nil {
			return api.ChaosResult{}, notFound("rack " + req.Rack)
		}
		f.addHostLocked(r)
	case "drain":
		n := 0
		for i := range f.s.Hosts {
			h := &f.s.Hosts[i]
			if h.ID == req.Host || (req.Host == "" && h.Rack == req.Rack) {
				h.AdminState = api.AdminDraining
				n++
			}
		}
		if n == 0 {
			return api.ChaosResult{}, notFound("host or rack")
		}
		f.emitLocked(api.Event{Kind: api.EventTopology, Rack: req.Rack, HostID: req.Host, Detail: "drain started"})
	default:
		return api.ChaosResult{}, invalid("unknown topology action: " + action)
	}
	f.recountLocked()
	return api.ChaosResult{Action: action, Target: req.Rack + req.Host, Detail: "applied"}, nil
}

var (
	_ api.ControlPlane   = (*Fleet)(nil)
	_ api.ChaosEngine    = (*Fleet)(nil)
	_ api.SnapshotSource = (*Fleet)(nil)
)
