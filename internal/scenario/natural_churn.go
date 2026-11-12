package scenario

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

// Natural churn is opt-in; no lifetime setting is applied to other scenarios.
func naturalChurn() Scenario {
	cfg := config.Default()
	s := Scenario{Name: "natural-churn", Topology: Topology{cfg.Racks, cfg.HostsPerRack, cfg.SlotsPerHost}, Duration: 90 * time.Second}
	s.NewRun = func() Scenario {
		run := s
		run.NewRun = nil
		policy := cfg.DemoLifetime
		policy.MinMs = cfg.LifetimeMin.Milliseconds()
		policy.MaxMs, policy.IdleMs, policy.ReleaseP = 2*policy.MinMs, policy.MinMs, .5
		// 0.5/s × (7.5s busy + 0.5 × 5s idle) = 5 Ready VMs on
		// average, leaving room for boots and host-failure replacements.
		rate, zero, intensity := .5, 0.0, .15
		run.Timeline = []Step{
			{0, func(ctx context.Context, f System) error {
				_, err := f.Sim(ctx, "chaos", api.SimRequest{Lifetime: &policy, LoadRate: &rate})
				return err
			}},
			{10 * time.Second, func(ctx context.Context, f System) error {
				_, err := f.Sim(ctx, "chaos", api.SimRequest{Intensity: &intensity, Filters: []string{"kill-guestd", "kill-host", "pause-heartbeats"}})
				return err
			}},
			{55 * time.Second, func(ctx context.Context, f System) error {
				// Stop new arrivals/faults, not lifetimes or outstanding heals.
				_, err := f.Sim(ctx, "chaos", api.SimRequest{Intensity: &zero, LoadRate: &zero})
				return err
			}},
		}
		ended := map[string]string{}
		previous := map[string]api.Event{}
		deleted, validDelete, failed := map[string]bool{}, map[string]bool{}, map[string]bool{}
		races, unrecovered := map[string]bool{}, map[string]bool{}
		seenReady, placedAfterEnd := map[string]bool{}, map[string]bool{}
		completed, idle, reused := 0, 0, 0
		readyOnly := Always("workload deletes target Ready VMs or the measured one-tick guest race, never Lost", func(api.Snapshot) bool { return true })
		readyOnly.EventTest = func(e api.Event) bool {
			if e.Kind == api.EventTransition {
				if e.To == api.VMLost || e.To == api.VMUnhealthy {
					unrecovered[e.VMID] = true
				}
				if e.To == api.VMReady {
					delete(unrecovered, e.VMID)
				}
				if e.To == api.VMTerminating {
					deleted[e.VMID] = true
					prior := previous[e.VMID]
					delta := e.At - prior.At
					races[e.VMID] = e.From == api.VMUnhealthy && prior.From == api.VMReady && prior.To == api.VMUnhealthy && delta >= 0 && delta <= cfg.AmbientEvery.Milliseconds()
					validDelete[e.VMID] = e.From == api.VMReady || races[e.VMID]
					if ended[e.VMID] != "" && !validDelete[e.VMID] {
						return false
					}
				}
				previous[e.VMID] = e
				if e.To == api.VMFailed {
					failed[e.VMID] = true
					if ended[e.VMID] != "" {
						return false
					}
				}
				if e.To == api.VMBooting && len(ended) > 0 {
					placedAfterEnd[e.VMID] = true
				}
				if e.To == api.VMReady {
					if !seenReady[e.VMID] && placedAfterEnd[e.VMID] {
						reused++
					}
					seenReady[e.VMID] = true
				}
			}
			if e.Kind != api.EventWorkload {
				return true
			}
			if e.VMID == "" || ended[e.VMID] != "" || failed[e.VMID] || (deleted[e.VMID] && !validDelete[e.VMID]) {
				return false
			}
			// Publishers can race: match successful-delete markers by VM ID,
			// then require the matching authoritative transition at the end.
			switch e.Detail {
			case "completed":
				completed++
			case "idle":
				idle++
			default:
				return false
			}
			ended[e.VMID] = e.Detail
			return true
		}
		run.Expect = []Check{
			Always("invariants", NoInvariantViolations), readyOnly,
			Always("natural load never sheds or returns 429", func(s api.Snapshot) bool { return s.Counters.Shed == 0 && s.Counters.Rejected429 == 0 }),
			Eventually(run.Duration, "at least three completed and three idle endings", func(api.Snapshot) bool { return completed >= 3 && idle >= 3 }),
			Eventually(run.Duration, "every workload ending is Terminated, never Failed", func(s api.Snapshot) bool {
				terminated := map[string]bool{}
				for _, v := range s.VMs {
					terminated[v.ID] = v.State == api.VMTerminated
				}
				for id := range deleted {
					if ended[id] == "" {
						return false
					}
				}
				for id := range ended {
					if !terminated[id] || failed[id] || !validDelete[id] {
						return false
					}
				}
				return len(ended) > 0
			}),
			Eventually(run.Duration, "new admissions reach Ready after workload endings", func(api.Snapshot) bool { return reused >= 3 }),
			Eventually(run.Duration, "remaining demand is Ready after recovery", AllDesiredVMsReady),
		}
		run.Report = func(report *api.RunReport) {
			var raceIDs, lostIDs []string
			for id := range ended {
				if races[id] {
					raceIDs = append(raceIDs, id)
				}
			}
			for id := range unrecovered {
				lostIDs = append(lostIDs, id)
			}
			slices.Sort(raceIDs)
			slices.Sort(lostIDs)
			report.Metrics["workload_completed"] = float64(completed)
			report.Metrics["workload_idle"] = float64(idle)
			report.Metrics["workload_reused"] = float64(reused)
			report.Metrics["workload_delete_race"] = float64(len(raceIDs))
			// Preserve the runner's real loss count and samples. An approved
			// race is a deletion, not a fabricated recovery to Ready.
			recovery, measured := report.Metrics["recovery_failures"]
			report.Checks = append(report.Checks, api.CheckResult{
				Name:   "unrecovered IDs exactly match tolerated workload-delete races",
				Passed: measured && recovery == float64(len(raceIDs)) && slices.Equal(lostIDs, raceIDs),
				Detail: fmt.Sprintf("recovery_failures=%g unrecovered=%v workload_delete_race=%v", recovery, lostIDs, raceIDs),
			})
		}
		return run
	}
	return s
}
