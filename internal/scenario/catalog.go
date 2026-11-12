package scenario

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

// Catalog includes the failure scenarios, six dashboard presets and review regressions.
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
		base("gray-failure", Step{10 * sec, Chaos("pause-heartbeats", "r1-h1", nil)}, Step{10 * sec, Chaos("pause-heartbeats", "r2-h1", nil)}, Step{10 * sec, Chaos("pause-heartbeats", "r3-h1", nil)}, Step{40 * sec, Chaos("resume-heartbeats", "r1-h1", nil)}, Step{40 * sec, Chaos("resume-heartbeats", "r2-h1", nil)}, Step{40 * sec, Chaos("resume-heartbeats", "r3-h1", nil)}),
		base("thundering-herd", Step{10 * sec, Chaos("power-off-rack", "r2", nil)}, Step{11 * sec, Chaos("flood", "", map[string]float64{"count": 60, "interactive": 1})}),
		base("rolling-maintenance", Step{10 * sec, drain("r1")}, Step{20 * sec, addRack("r4")}, Step{30 * sec, drain("r2")}, Step{40 * sec, addRack("r5")}, Step{50 * sec, drain("r3")}, Step{60 * sec, addRack("r6")}),
		base("hostile-tenant"),
		base("bad-day", Step{10 * sec, ambient}, Step{11 * sec, Chaos("sustained-load", "", map[string]float64{"rate": 6})}, Step{11 * sec, Chaos("flood", "", map[string]float64{"count": 60})}, Step{12 * sec, Chaos("latency-spike", "", map[string]float64{"ms": 1000})}, Step{30 * sec, Chaos("kill-scheduler", "", nil)}, Step{40 * sec, Chaos("restart-scheduler", "", nil)}, Step{60 * sec, func(ctx context.Context, f System) error {
			intensity := 0.0
			if _, err := f.Sim(ctx, "chaos", api.SimRequest{Intensity: &intensity}); err != nil {
				return err
			}
			return Chaos("sustained-load", "", map[string]float64{"rate": 0})(ctx, f)
		}}),
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
	s[11].Duration = max(s[11].Duration, 50*sec+cfg.DrainGrace+cfg.BootDeadline+cfg.HostLeaseTTL)
	s[8].Expect = append(s[8].Expect, Metric("p99_recover_ms", 20000))
	for _, i := range []int{3, 10, 11, 12, 13} {
		s[i].Expect = append(s[i].Expect,
			Always("replacement queue precedes new requests", ReplacementQueueOrdered),
			Eventually(s[i].Duration, "scheduler Up", func(s api.Snapshot) bool { return s.Scheduler == api.SchedulerUp }))
		if i != 10 { // Herd checks each victim and placement order separately.
			s[i].Expect = append(s[i].Expect, Check{Name: "previously Ready VMs recovered", Until: s[i].Duration, NewTest: readyCohort}, Metric("recovery_failures", 1))
		}
	}
	s[10].Expect = append(s[10].Expect, Eventually(s[10].Duration, "rack loss exposes negative headroom and backpressure", func(s api.Snapshot) bool {
		return s.Counters.Headroom < 0 && s.Counters.Rejected429+s.Counters.Shed > 0
	}))
	s[3] = overload(s[3])
	for _, i := range []int{0, 4, 8, 9, 10, 11} {
		index := i
		s[i] = victimRecovery(s[i], func(v api.VM) bool {
			switch index {
			case 0, 11:
				return true
			case 9:
				return v.HostID == "r1-h1" || v.HostID == "r2-h1" || v.HostID == "r3-h1"
			default:
				return v.Rack == "r2"
			}
		}, i == 0, i == 11)
	}
	s[5] = restartProgress(s[5], true)
	s[13] = restartProgress(s[13], false)
	s[1].Expect = append(s[1].Expect, Check{Name: "hang caused a boot deadline", Until: s[1].Duration, NewTest: func() Predicate {
		hung := false
		return func(s api.Snapshot) bool {
			for _, e := range s.Recent {
				hung = hung || e.Detail == api.ReasonBootDeadline
			}
			for _, v := range s.VMs {
				hung = hung || v.LastReason == api.ReasonBootDeadline
			}
			return hung
		}
	}})
	s[12] = hostileTenant(s[12])
	s = append(s, wipeLateHost(base("wipe-late-host")))
	return s
}

func ReplacementQueueOrdered(s api.Snapshot) bool {
	newAhead := false
	for _, q := range s.Queue {
		if q.Reason == api.QueueReplace {
			if newAhead || q.Priority != api.PriorityReplacement || q.EffectivePriority != api.PriorityReplacement {
				return false
			}
		} else {
			newAhead = true
		}
	}
	return true
}

func readyCohort() Predicate {
	return readyCohortExcept(nil)
}

func readyCohortExcept(deleted map[string]bool) Predicate {
	ids := map[string]bool{}
	return func(s api.Snapshot) bool {
		current := map[string]api.VM{}
		for _, v := range s.VMs {
			current[v.ID] = v
			if v.State == api.VMReady {
				ids[v.ID] = true
			}
		}
		for id := range ids {
			if deleted[id] {
				continue
			}
			v := current[id]
			if v.State != api.VMReady && !(v.State == api.VMFailed && v.LastReason == api.ReasonQuarantined) {
				return false
			}
		}
		return len(ids) > 0
	}
}

// Capture the complete victim cohort before the fault, not a single lucky
// higher-generation survivor. Maintenance also forbids terminal losses.
func victimRecovery(base Scenario, pick func(api.VM) bool, first, maintenance bool) Scenario {
	base.NewRun = func() Scenario {
		run := base
		run.NewRun = nil
		run.Timeline = append([]Step(nil), base.Timeline...)
		victims := map[string]api.VM{}
		action := run.Timeline[0].Do
		run.Timeline[0].Do = func(ctx context.Context, f System) error {
			return selectTargets(ctx, f, func(s api.Snapshot) bool {
				clear(victims)
				for _, v := range s.VMs {
					if v.State == api.VMReady && pick(v) {
						victims[v.ID] = v
						if first {
							break
						}
					}
				}
				return len(victims) > 0
			}, func(ctx context.Context, _ api.Snapshot) error { return action(ctx, f) })
		}
		run.Expect = append(append([]Check(nil), base.Expect...), Eventually(run.Duration, "every affected VM recovered at a higher generation", func(s api.Snapshot) bool {
			recovered := 0
			for _, v := range s.VMs {
				if old, ok := victims[v.ID]; ok && v.State == api.VMReady && v.Generation > old.Generation && (!first || v.HostID == old.HostID) {
					recovered++
				}
			}
			return len(victims) > 0 && recovered == len(victims)
		}))
		if run.Name == "thundering-herd" {
			run.Timeline = append(run.Timeline, Step{40 * time.Second, func(ctx context.Context, f System) error {
				for i := 1; i <= config.Default().HostsPerRack; i++ {
					if _, err := f.Do(ctx, "restart-host", api.ChaosRequest{Target: fmt.Sprintf("r2-h%d", i)}); err != nil {
						return err
					}
				}
				return nil
			}})
			waiting, replacements := map[string]bool{}, map[string]bool{}
			priority := Always("lost victims wait at class 0 and are never shed", func(s api.Snapshot) bool {
				queued := map[string]bool{}
				for _, q := range s.Queue {
					queued[q.VMID] = q.Reason == api.QueueReplace && q.Priority == api.PriorityReplacement && q.EffectivePriority == api.PriorityReplacement
				}
				for _, v := range s.VMs {
					if _, victim := victims[v.ID]; victim && replacements[v.ID] &&
						(v.State == api.VMFailed || v.State == api.VMTerminated || (v.State == api.VMPending && !queued[v.ID])) {
						return false
					}
				}
				return true
			})
			priority.EventTest = func(e api.Event) bool {
				if e.Kind != api.EventTransition {
					return true
				}
				if e.To == api.VMLost {
					replacements[e.VMID], waiting[e.VMID] = true, true
				}
				if replacements[e.VMID] && e.To == api.VMPending {
					waiting[e.VMID] = true
				}
				if _, victim := victims[e.VMID]; victim && replacements[e.VMID] && (e.To == api.VMFailed || e.To == api.VMTerminated || e.Detail == api.ReasonShed) {
					return false
				}
				if e.To == api.VMFailed || e.To == api.VMTerminated {
					delete(waiting, e.VMID) // Terminal non-victims are no longer queued.
				}
				if e.To == api.VMBooting {
					// No preemption: placements before loss detection are valid.
					// After loss, every newly available slot serves replacements first.
					if !replacements[e.VMID] && len(waiting) > 0 {
						return false
					}
					delete(waiting, e.VMID)
				}
				return true
			}
			run.Expect = append(run.Expect, priority)
		}
		if maintenance {
			check := Always("no VM lost during maintenance", func(s api.Snapshot) bool {
				for _, v := range s.VMs {
					if _, ok := victims[v.ID]; ok && (v.State == api.VMFailed || v.State == api.VMTerminated) {
						return false
					}
				}
				return true
			})
			check.EventTest = func(e api.Event) bool {
				_, victim := victims[e.VMID]
				return !(victim && e.Kind == api.EventTransition && (e.To == api.VMFailed || e.To == api.VMTerminated))
			}
			run.Expect = append(run.Expect, check)
		}
		return run
	}
	return base
}

func restartProgress(base Scenario, create bool) Scenario {
	base.NewRun = func() Scenario {
		run := base
		run.NewRun = nil
		run.Timeline = append([]Step(nil), base.Timeline...)
		probe := ""
		placed, restartedAt, ambientEvents := false, int64(0), 0
		// The restart is the final scheduler restart in these timelines.
		at := 20 * time.Second
		if !create {
			at = 40 * time.Second
		}
		for i, step := range run.Timeline {
			if step.At == at {
				run.Timeline[i].Do = func(ctx context.Context, f System) error {
					err := step.Do(ctx, f)
					restartedAt = f.Latest().At
					return err
				}
			}
		}
		if create {
			run.Timeline = append(run.Timeline, Step{25 * time.Second, func(ctx context.Context, f System) error {
				v, err := f.CreateVM(ctx, "post-restart", api.CreateVMRequest{Pool: "ios-xcode16-m", Priority: "interactive"})
				probe = v.ID
				return err
			}})
		}
		check := Eventually(run.Duration, "restarted scheduler places useful work", func(s api.Snapshot) bool {
			if s.Scheduler != api.SchedulerUp || !placed {
				return false
			}
			if !create {
				return ambientEvents > 0
			}
			for _, v := range s.VMs {
				if v.ID == probe && probe != "" && v.State == api.VMReady {
					return true
				}
			}
			return false
		})
		// Use an Always event observer; Eventually is decided only at the end.
		observe := Always("observe restart and ambient events", func(api.Snapshot) bool { return true })
		observe.EventTest = func(e api.Event) bool {
			if restartedAt != 0 && e.At > restartedAt && e.Kind == api.EventTransition && e.To == api.VMBooting {
				placed = true
			}
			if !create && e.Kind == api.EventChaos {
				// Exclude all explicit injections; these other successful chaos
				// actions can only have come from the ambient engine.
				parts := strings.Fields(e.Detail)
				if len(parts) > 0 {
					switch parts[0] {
					case "scenario", "sustained-load", "flood", "latency-spike", "kill-scheduler", "restart-scheduler", "ambient", "failed", "load:":
					default:
						ambientEvents++
					}
				}
			}
			return true
		}
		run.Expect = append(append([]Check(nil), base.Expect...), observe, check)
		if !create {
			run = badDayProgress(run)
		}
		return run
	}
	return base
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
	// This is an explicit negative probe, not a client trying to create work.
	if client, ok := f.(*scriptedClient); ok {
		f = client.System
	}
	_, err := f.CreateVM(ctx, "during-outage", api.CreateVMRequest{Pool: "ios-xcode16-m"})
	if e, ok := err.(*api.Error); ok && e.Status == 503 {
		return nil
	}
	return fmt.Errorf("create during scheduler outage: want 503, got %v", err)
}

// Capture victims and check outcomes in per-run state, not shared catalog
// closures: Suite runs the same scenario with several seeds concurrently.
func hostileTenant(base Scenario) Scenario {
	base.NewRun = func() Scenario {
		run := base
		run.NewRun = nil
		victims := map[string]int64{}
		run.Timeline = []Step{
			{10 * time.Second, Chaos("flood", "", map[string]float64{"count": 60})},
			{11 * time.Second, Chaos("packet-loss", "", map[string]float64{"percent": 25})},
			{11 * time.Second, func(ctx context.Context, f System) error {
				return selectTargets(ctx, f, func(s api.Snapshot) bool {
					count := 0
					for _, v := range s.VMs {
						if v.State == api.VMReady {
							count++
						}
					}
					return count >= 3
				}, func(ctx context.Context, s api.Snapshot) error {
					for _, v := range s.VMs {
						if v.State != api.VMReady {
							continue
						}
						victims[v.ID] = v.Generation
						if _, err := f.Do(ctx, "compromise", api.ChaosRequest{Target: v.ID}); err != nil {
							return err
						}
						if len(victims) == 3 {
							return nil
						}
					}
					return fmt.Errorf("hostile tenant needs 3 Ready VMs, found %d", len(victims))
				})
			}},
			{40 * time.Second, Chaos("packet-loss", "", map[string]float64{"percent": 0})},
		}
		// Only accepted SecurityReports carry a generation. Runtime attack
		// verdicts such as "unknown impersonate" cannot attribute a blocked
		// attack to an incarnation, and a clean recycle is not compromised.
		blocked := map[string]int64{}
		resurrected := map[string]bool{}
		noResurrection := Always("victims with attributed blocked attacks never return Ready", func(s api.Snapshot) bool {
			for _, v := range s.VMs {
				gen, victim := victims[v.ID]
				if victim && v.Generation > gen && v.State == api.VMReady {
					resurrected[v.ID] = true
				}
				if victim && blocked[v.ID] >= gen && resurrected[v.ID] {
					return false
				}
			}
			return true
		})
		noResurrection.EventTest = func(e api.Event) bool {
			if e.Kind == api.EventSecurity && e.Generation > blocked[e.VMID] {
				blocked[e.VMID] = e.Generation
			}
			if e.Kind == api.EventTransition && e.To == api.VMReady && blocked[e.VMID] > 0 && e.Generation >= blocked[e.VMID] {
				resurrected[e.VMID] = true
			}
			gen, victim := victims[e.VMID]
			return !(victim && blocked[e.VMID] >= gen && resurrected[e.VMID])
		}
		run.Expect = append(append([]Check(nil), base.Expect...), noResurrection,
			Eventually(60*time.Second, "attributed victims end quarantined Failed; quarantine exercised", func(s api.Snapshot) bool {
				failed := 0
				seen := map[string]bool{}
				for _, v := range s.VMs {
					if gen, ok := victims[v.ID]; ok {
						seen[v.ID] = true
						quarantined := v.State == api.VMFailed && v.LastReason == api.ReasonQuarantined
						if blocked[v.ID] >= gen && !quarantined {
							return false
						}
						if quarantined {
							failed++
						}
					}
				}
				return len(victims) == 3 && len(seen) == 3 && failed > 0
			}), Eventually(60*time.Second, "every compromised generation stopped in driver observations", func(s api.Snapshot) bool {
				observed := map[string]bool{}
				for _, h := range s.Observed {
					if !h.Connected {
						return false
					}
					observed[h.HostID] = true
					for _, actual := range h.Actual {
						if gen, ok := victims[actual.VMID]; ok && actual.Generation == gen && actual.Running {
							return false
						}
					}
				}
				for _, h := range s.Hosts {
					if !observed[h.ID] {
						return false
					}
				}
				return len(victims) == 3 && len(observed) > 0
			}))
		return run
	}
	return base
}

func wipeLateHost(base Scenario) Scenario {
	base.NewRun = func() Scenario {
		run := base
		run.NewRun = nil
		run.Load.Count = 0 // The time-zero action retains the IDs returned for all 12 keys.
		type request struct {
			key, original, retry string
			late                 bool
		}
		requests := make([]request, 12)
		keyByID := map[string]string{}
		wipeAt, wipeEpoch := int64(0), int64(0)
		wiped := false
		retry := func(ctx context.Context, f System) error {
			for i := range requests {
				r := &requests[i]
				v, err := f.CreateVM(ctx, r.key, api.CreateVMRequest{Pool: "ios-xcode16-m", Priority: "standard"})
				if err != nil {
					return err
				}
				if (!r.late && v.ID != r.original) || (r.retry != "" && v.ID != r.retry) {
					return fmt.Errorf("key %s moved: original=%s previous retry=%s returned=%s", r.key, r.original, r.retry, v.ID)
				}
				r.retry = v.ID
				keyByID[v.ID] = r.key
			}
			return nil
		}
		run.Timeline = []Step{
			{0, func(ctx context.Context, f System) error {
				for i := range requests {
					key := fmt.Sprintf("wipe-key-%d", i)
					v, err := f.CreateVM(ctx, key, api.CreateVMRequest{Pool: "ios-xcode16-m", Priority: "standard"})
					if err != nil {
						return err
					}
					requests[i] = request{key: key, original: v.ID}
					keyByID[v.ID] = key
				}
				return nil
			}},
			{10 * time.Second, func(ctx context.Context, f System) error {
				late := 0
				for i := range requests {
					v, err := f.GetVM(ctx, requests[i].original)
					if err != nil {
						return err
					}
					if v.State != api.VMReady {
						return fmt.Errorf("wipe baseline %s not Ready: %s", v.ID, v.State)
					}
					requests[i].late = v.Rack == "r2"
					if requests[i].late {
						late++
					}
				}
				if late == 0 || late == len(requests) {
					return fmt.Errorf("wipe requires both reachable and late keys, got %d late", late)
				}
				_, err := f.Do(ctx, "partition-rack", api.ChaosRequest{Target: "r2"})
				return err
			}},
			{12 * time.Second, func(ctx context.Context, f System) error {
				before := f.Latest()
				wipeAt, wipeEpoch = before.At, before.Epoch
				_, err := f.Do(ctx, "wipe-store", api.ChaosRequest{})
				wiped = err == nil
				return err
			}},
			{30 * time.Second, retry},
			{40 * time.Second, Chaos("heal-rack", "r2", nil)},
			{60 * time.Second, retry},
			{80 * time.Second, retry},
		}
		unique := Always("at most one desired-running VM per key", func(s api.Snapshot) bool {
			counts := map[string]int{}
			for _, v := range s.VMs {
				if v.Desired != api.DesiredRunning {
					continue
				}
				key, known := keyByID[v.ID]
				if !known {
					return false
				}
				counts[key]++
				if counts[key] > 1 {
					return false
				}
			}
			return true
		})
		lateNeverReady := Always("late copies never adopted Ready", func(s api.Snapshot) bool {
			if !wiped || s.Epoch <= wipeEpoch {
				return true // Ignore pre-wipe snapshots buffered during actions.
			}
			for _, v := range s.VMs {
				for _, r := range requests {
					if r.late && v.ID == r.original && v.State == api.VMReady {
						return false
					}
				}
			}
			return true
		})
		lateNeverReady.EventTest = func(e api.Event) bool {
			if wiped && e.At >= wipeAt && e.Kind == api.EventTransition && e.To == api.VMReady {
				for _, r := range requests {
					if r.late && e.VMID == r.original {
						return false
					}
				}
			}
			return true
		}
		run.Expect = []Check{Always("invariants", NoInvariantViolations), unique, lateNeverReady,
			Eventually(90*time.Second, "one Ready VM per retried key", func(s api.Snapshot) bool {
				ready := 0
				for _, r := range requests {
					if r.retry == "" {
						return false
					}
					for _, v := range s.VMs {
						if v.ID == r.retry && v.State == api.VMReady && v.Desired == api.DesiredRunning {
							ready++
						}
					}
				}
				return ready == len(requests)
			}), Eventually(90*time.Second, "late driver copies stopped", NoStaleCopies)}
		return run
	}
	return base
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
