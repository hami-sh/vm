package scenario

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

// Catalog includes all eight failure scenarios and the six dashboard presets.
func Catalog() []Scenario {
	sec := time.Second
	cfg := config.Default()
	base := func(name string, steps ...Step) Scenario {
		return Scenario{Name: name, Topology: Topology{cfg.Racks, cfg.HostsPerRack, cfg.SlotsPerHost}, Load: Load{12, "standard"}, Duration: 90 * sec, Timeline: steps,
			Expect: []Check{Always("invariants", NoInvariantViolations), Eventually(90*sec, "desired VMs Ready", AllDesiredVMsReady)}}
	}
	s := []Scenario{
		base("guestd-dies", Step{10 * sec, Chaos("kill-guestd", "@ready", nil)}),
		base("boot-hangs", Step{0, Chaos("hang-next-boot", "r1-h1", nil)}),
		base("host-dies", Step{10 * sec, queueBatch}, Step{11 * sec, Chaos("kill-host", "r1-h1", nil)}, Step{30 * sec, addRack("r4")}),
		base("overload", Step{10 * sec, Chaos("flood", "", map[string]float64{"count": 60})}),
		base("rack-partition", Step{10 * sec, Chaos("partition-rack", "r2", nil)}, Step{40 * sec, Chaos("heal-rack", "r2", nil)}),
		base("scheduler-crash", Step{10 * sec, Chaos("kill-scheduler", "", nil)}, Step{15 * sec, unavailableCreate}, Step{20 * sec, Chaos("restart-scheduler", "", nil)}),
		base("duplicate-create", Step{5 * sec, duplicateCreate}),
		base("impostor-guest", Step{10 * sec, Chaos("compromise", "@ready", nil)}),
		base("split-brain", Step{10 * sec, Chaos("partition-rack", "r2", nil)}, Step{40 * sec, Chaos("heal-rack", "r2", nil)}),
		base("gray-failure", Step{10 * sec, Chaos("pause-heartbeats", "r1-h1", nil)}, Step{10 * sec, Chaos("pause-heartbeats", "r2-h1", nil)}, Step{10 * sec, Chaos("pause-heartbeats", "r3-h1", nil)}, Step{40 * sec, Chaos("restart-host", "r1-h1", nil)}, Step{40 * sec, Chaos("restart-host", "r2-h1", nil)}, Step{40 * sec, Chaos("restart-host", "r3-h1", nil)}),
		base("thundering-herd", Step{10 * sec, Chaos("partition-rack", "r2", nil)}, Step{11 * sec, Chaos("flood", "", map[string]float64{"count": 60})}),
		base("rolling-maintenance", Step{10 * sec, drain("r1")}, Step{20 * sec, addRack("r4")}, Step{30 * sec, drain("r2")}, Step{40 * sec, addRack("r5")}, Step{50 * sec, drain("r3")}, Step{60 * sec, addRack("r6")}),
		base("hostile-tenant", Step{10 * sec, Chaos("flood", "", map[string]float64{"count": 60})}, Step{11 * sec, compromiseThree}),
		base("bad-day", Step{10 * sec, ambient}, Step{11 * sec, Chaos("flood", "", map[string]float64{"count": 60})}, Step{12 * sec, Chaos("latency-spike", "", map[string]float64{"ms": 200})}, Step{30 * sec, Chaos("kill-scheduler", "", nil)}, Step{40 * sec, Chaos("restart-scheduler", "", nil)}),
	}
	// Overload is intentionally not an all-ready assertion: excess demand must
	// queue or be rejected; it cannot all fit on 24 slots.
	for _, i := range []int{3, 10, 11, 12, 13} {
		s[i].Expect = s[i].Expect[:1]
	}
	s[3].Expect = append(s[3].Expect, Eventually(30*sec, "backpressure observed", func(s api.Snapshot) bool { return s.Counters.Rejected429+s.Counters.Shed > 0 }))
	for _, i := range []int{4, 8, 9} {
		s[i].Expect = append(s[i].Expect, Eventually(70*sec, "stale copies stopped", NoStaleCopies))
	}
	for _, i := range []int{7, 12} {
		s[i].Expect = s[i].Expect[:1]
		s[i].Expect = append(s[i].Expect, Eventually(60*sec, "attacks blocked", func(s api.Snapshot) bool { return s.Security.Blocked > 0 && s.Security.Succeeded == 0 }))
	}
	s[10].Load.Count = 20
	// Power-off destroys the old copies. Saturate capacity, queue batch work,
	// then add capacity only after replacement priority has been observable.
	s[2].Load.Count = cfg.Racks * cfg.HostsPerRack * cfg.SlotsPerHost
	s[2].Expect = []Check{Always("invariants", NoInvariantViolations), {
		Name: "powered-off VMs replaced at gen+1 ahead of batch", Until: 90 * sec,
		NewTest: func() Predicate { return hostReplacement("r1-h1") },
	}}
	for _, i := range []int{0, 4, 8, 9} {
		s[i].Expect = append(s[i].Expect, Eventually(90*sec, "new generation Ready", func(s api.Snapshot) bool {
			for _, v := range s.VMs {
				if v.Generation > 1 && v.State == api.VMReady {
					return true
				}
			}
			return false
		}))
	}
	s[1].Expect = []Check{Always("invariants", NoInvariantViolations), Always("bounded attempts", func(s api.Snapshot) bool {
		for _, v := range s.VMs {
			if v.Attempts > cfg.MaxAttempts {
				return false
			}
		}
		return true
	}), Eventually(90*sec, "boot resolved", func(s api.Snapshot) bool {
		for _, v := range s.VMs {
			if v.State != api.VMReady && v.State != api.VMFailed {
				return false
			}
		}
		return len(s.VMs) > 0
	})}
	s[11].Expect = append(s[11].Expect, Eventually(90*sec, "maintenance recovered", AllDesiredVMsReady))
	s[8].Expect = append(s[8].Expect, Metric("p99_recover_ms", 20000))
	return s
}

// TargetScenario is an opt-in measurement, not a quieter replacement for the
// bad-day preset. Fixed offered load isolates the 70%-load readiness target.
func TargetScenario() Scenario {
	cfg := config.Default()
	return Scenario{
		Name: "target-50pct-chaos", Topology: Topology{cfg.Racks, cfg.HostsPerRack, cfg.SlotsPerHost},
		Load: Load{17, "interactive"}, Duration: 90 * time.Second,
		Timeline: []Step{{0, ambientTarget}},
		Expect:   []Check{Always("invariants", NoInvariantViolations)},
	}
}

// Observe the exact victims before power-off; unrelated recoveries, skipped
// generations, normal-priority retries and a reversed queue must not pass.
func hostReplacement(host string) Predicate {
	victims := map[string]int64{}
	priorityObserved := false
	return func(s api.Snapshot) bool {
		for _, v := range s.VMs {
			if v.HostID == host && v.State == api.VMReady {
				if _, ok := victims[v.ID]; !ok {
					victims[v.ID] = v.Generation
				}
			}
		}
		if len(victims) == 0 {
			return false
		}
		queued := 0
		normalAhead := false
		for _, q := range s.Queue {
			if _, ok := victims[q.VMID]; ok {
				if normalAhead || q.Reason != api.QueueReplace || q.Priority != api.PriorityReplacement || q.EffectivePriority != api.PriorityReplacement {
					return false
				}
				queued++
			} else if q.Reason != api.QueueReplace {
				normalAhead = true
			}
		}
		if queued == len(victims) {
			priorityObserved = true
		}
		if !priorityObserved {
			return false
		}
		replaced := 0
		for _, v := range s.VMs {
			if gen, ok := victims[v.ID]; ok && v.Generation == gen+1 && v.State == api.VMReady && v.HostID != "" && v.HostID != host {
				replaced++
			}
		}
		return replaced == len(victims)
	}
}

func queueBatch(ctx context.Context, f System) error {
	for i := 0; i < 8; i++ {
		if _, err := f.CreateVM(ctx, fmt.Sprintf("queued-batch-%d", i), api.CreateVMRequest{Pool: "ios-xcode16-m", Priority: "batch"}); err != nil {
			return err
		}
	}
	return nil
}

func duplicateCreate(ctx context.Context, f System) error {
	var wg sync.WaitGroup
	var responses [2]api.CreateVMResponse
	var errors [2]error
	for i := range responses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			responses[i], errors[i] = f.CreateVM(ctx, "duplicate-key", api.CreateVMRequest{Pool: "ios-xcode16-m", Priority: "interactive"})
		}(i)
	}
	wg.Wait()
	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	if responses[0].ID == "" || responses[0].ID != responses[1].ID {
		return fmt.Errorf("duplicate create returned different IDs: %v", responses)
	}
	return nil
}
func drain(rack string) Action {
	return func(ctx context.Context, f System) error {
		_, err := f.Topology(ctx, "drain", api.TopologyRequest{Rack: rack})
		return err
	}
}
func addRack(rack string) Action {
	return func(ctx context.Context, f System) error {
		_, err := f.Topology(ctx, "add-rack", api.TopologyRequest{Rack: rack, Hosts: 4})
		return err
	}
}
func unavailableCreate(ctx context.Context, f System) error {
	_, err := f.CreateVM(ctx, "during-outage", api.CreateVMRequest{Pool: "ios-xcode16-m"})
	if e, ok := err.(*api.Error); ok && e.Status == 503 {
		return nil
	}
	return fmt.Errorf("create during scheduler outage: want 503, got %v", err)
}
func compromiseThree(ctx context.Context, f System) error {
	count := 0
	for _, v := range f.Latest().VMs {
		if v.State == api.VMReady {
			if _, err := f.Do(ctx, "compromise", api.ChaosRequest{Target: v.ID}); err != nil {
				return err
			}
			count++
			if count == 3 {
				return nil
			}
		}
	}
	return fmt.Errorf("hostile tenant needs 3 Ready VMs, found %d", count)
}
func ambient(ctx context.Context, f System) error {
	intensity := 0.5
	_, err := f.Sim(ctx, "chaos", api.SimRequest{Intensity: &intensity})
	return err
}

func ambientTarget(ctx context.Context, f System) error {
	intensity := 0.5
	filters := []string{}
	for _, action := range f.Catalog().Actions {
		if action.Ambient && action.Layer != "load" && action.Layer != "topology" {
			filters = append(filters, action.Name)
		}
	}
	_, err := f.Sim(ctx, "chaos", api.SimRequest{Intensity: &intensity, Filters: filters})
	return err
}
