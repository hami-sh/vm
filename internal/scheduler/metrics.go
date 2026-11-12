package scheduler

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

// Fixed buckets keep observations bounded even during an indefinite demo.
var buckets = []float64{.1, .5, 1, 2, 5, 10, 20, 30, 60, 120}

type histogram struct {
	counts [10]uint64
	count  uint64
	sum    float64
}
type metrics struct {
	hist    map[string]map[api.Priority]histogram
	blocked map[string]int
}

func newMetrics() metrics {
	return metrics{hist: map[string]map[api.Priority]histogram{}, blocked: map[string]int{}}
}
func (m metrics) clone() metrics {
	n := newMetrics()
	n.blocked = maps.Clone(m.blocked)
	for name, h := range m.hist {
		n.hist[name] = maps.Clone(h)
	}
	return n
}
func (m *metrics) observe(name string, class api.Priority, value float64) {
	if m.hist[name] == nil {
		m.hist[name] = map[api.Priority]histogram{}
	}
	h := m.hist[name][class]
	h.count++
	h.sum += value
	for i, b := range buckets {
		if value <= b {
			h.counts[i]++
		}
	}
	m.hist[name][class] = h
}
func class(p api.Priority) string {
	switch p {
	case 0:
		return "replacement"
	case 1:
		return "interactive"
	case 3:
		return "batch"
	}
	return "standard"
}
func (p *planner) prometheus() string {
	var b strings.Builder
	fmt.Fprintf(&b, "queue_depth %d\n", len(p.data.Queue))
	fmt.Fprintf(&b, "admissions_total{result=\"placed\"} %d\nadmissions_total{result=\"queued\"} %d\nadmissions_total{result=\"rejected_429\"} %d\nadmissions_total{result=\"shed\"} %d\n", p.counters.Placed, p.counters.Queued, p.counters.Rejected429, p.counters.Shed)
	for _, kind := range []string{"recycle", "replace", "retry"} {
		fmt.Fprintf(&b, "recoveries_total{kind=%q} %d\n", kind, p.counters.Recoveries[kind])
	}
	for _, name := range []string{"vm_time_to_ready_seconds", "queue_wait_seconds", "time_to_recover_seconds"} {
		fmt.Fprintf(&b, "# TYPE %s histogram\n", name)
		for c := api.PriorityReplacement; c <= api.PriorityBatch; c++ {
			h := p.metrics.hist[name][c]
			for i, v := range buckets {
				fmt.Fprintf(&b, "%s_bucket{priority=%q,le=%q} %d\n", name, class(c), strconv.FormatFloat(v, 'f', -1, 64), h.counts[i])
			}
			fmt.Fprintf(&b, "%s_bucket{priority=%q,le=\"+Inf\"} %d\n%s_count{priority=%q} %d\n%s_sum{priority=%q} %g\n", name, class(c), h.count, name, class(c), h.count, name, class(c), h.sum)
		}
	}
	attacks := []string{api.AttackImpersonate, api.AttackReplay, api.AttackFlood, api.AttackEastWest, api.AttackControl, api.AttackEgress, api.AttackDiskFill}
	for _, a := range attacks {
		fmt.Fprintf(&b, "security_blocked_total{attack=%q} %d\n", a, p.metrics.blocked[a])
	}
	return b.String()
}

func (p *planner) snapshot() api.Snapshot {
	recent := p.events
	if len(recent) > p.cfg.RecentEventLimit {
		recent = recent[len(recent)-p.cfg.RecentEventLimit:]
	}
	s := api.Snapshot{Seq: p.seq, At: p.now.UnixMilli(), Epoch: p.epoch, Scheduler: api.SchedulerUp, VMs: []api.VM{}, Hosts: []api.Host{}, Racks: []api.Rack{}, Pools: []api.Pool{}, Queue: p.ordered(false), Recent: append([]api.Event(nil), recent...), Counters: p.counters, Latency: map[string]api.LatencyStats{}}
	if p.down {
		s.Scheduler = api.SchedulerDown
	} else if p.now.UnixMilli() < p.recoverUntil {
		s.Scheduler = api.SchedulerReconciling
	}
	s.Counters.ByState = map[api.VMState]int{}
	s.Counters.Recoveries = maps.Clone(p.counters.Recoveries)
	s.Counters.QueueDepth = len(s.Queue)
	s.Counters.QueueCap = p.cfg.QueueCap
	for _, pl := range config.Pools {
		v, _ := pool(pl.Name)
		s.Pools = append(s.Pools, v)
	}
	for _, r := range p.data.VMs {
		s.VMs = append(s.VMs, r.VM)
		s.Counters.ByState[r.VM.State]++
	}
	sort.Slice(s.VMs, func(i, j int) bool { return s.VMs[i].ID < s.VMs[j].ID })
	racks := map[string][]string{}
	rackLoad := map[string]int{}
	free := 0
	for id, h := range p.hosts {
		x := api.Host{ID: id, Rack: h.record.Rack, SlotCount: h.record.Slots, AdminState: h.record.Admin, Lease: h.lease, LeaseAgeMs: p.now.UnixMilli() - h.last, Slots: []api.Slot{}}
		items := []api.InventoryItem{}
		for _, i := range p.occupied(id) {
			items = append(items, i)
		}
		sort.Slice(items, func(i, j int) bool {
			if items[i].VMID != items[j].VMID {
				return items[i].VMID < items[j].VMID
			}
			return items[i].Generation < items[j].Generation
		})
		for j, i := range items {
			st := api.SlotOccupied
			if i.State == api.VMTerminating {
				st = api.SlotReleasing
			}
			if r, ok := p.reservations[i.VMID]; ok && r.host == id && r.gen == i.Generation && !r.admitted {
				st = api.SlotReserved
			}
			x.Slots = append(x.Slots, api.Slot{Index: j, State: st, VMID: i.VMID, Generation: i.Generation})
			v, ok := p.data.VMs[i.VMID]
			if !ok || v.VM.Generation != i.Generation || v.VM.HostID != id {
				x.Stale = append(x.Stale, i)
			}
		}
		for len(x.Slots) < x.SlotCount {
			x.Slots = append(x.Slots, api.Slot{Index: len(x.Slots), State: api.SlotFree})
		}
		if h.lease == api.LeaseHealthy && h.record.Admin == api.AdminActive {
			free += h.record.Slots - len(items)
			rackLoad[h.record.Rack] += len(items)
		}
		s.Hosts = append(s.Hosts, x)
		racks[x.Rack] = append(racks[x.Rack], id)
	}
	sort.Slice(s.Hosts, func(i, j int) bool { return s.Hosts[i].ID < s.Hosts[j].ID })
	maxRack := 0
	for rack, hosts := range racks {
		sort.Strings(hosts)
		s.Racks = append(s.Racks, api.Rack{ID: rack, Hosts: hosts})
		if rackLoad[rack] > maxRack {
			maxRack = rackLoad[rack]
		}
	}
	sort.Slice(s.Racks, func(i, j int) bool { return s.Racks[i].ID < s.Racks[j].ID })
	s.Counters.Headroom = free - maxRack
	s.Security = api.SecurityStatus{ByAttack: maps.Clone(p.metrics.blocked)}
	for _, n := range s.Security.ByAttack {
		s.Security.Blocked += n
	}
	for c := api.PriorityInteractive; c <= api.PriorityBatch; c++ {
		h := p.metrics.hist["vm_time_to_ready_seconds"][c]
		l := api.LatencyStats{Samples: int(h.count)}
		if c != api.PriorityBatch {
			l.TargetMs = p.cfg.TimeToReadyTarget.Milliseconds()
		}
		if h.count > 0 {
			l.P50Ms = quantile(h, .5)
			l.P99Ms = quantile(h, .99)
		}
		s.Latency[class(c)] = l
	}
	return s
}
func quantile(h histogram, q float64) int64 {
	for i, n := range h.counts {
		if float64(n) >= float64(h.count)*q {
			return int64(buckets[i] * 1000)
		}
	}
	return int64(buckets[len(buckets)-1] * 1000)
}
