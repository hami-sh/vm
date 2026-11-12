package scenario

import (
	"context"
	"fmt"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

// Release serving capacity deliberately: priority admission promises the next
// free slot, not preemption of the first 24 successful creates.
func overload(base Scenario) Scenario {
	base.NewRun = func() Scenario {
		run := base
		run.NewRun = nil
		run.Timeline = append([]Step(nil), base.Timeline...)
		run.Expect = append([]Check(nil), base.Expect...)
		cfg := config.Default()
		deleted := map[string]bool{}
		run.Timeline = append(run.Timeline, Step{20 * time.Second, func(ctx context.Context, f System) error {
			return selectTargets(ctx, f, func(s api.Snapshot) bool {
				interactive := false
				ready := 0
				for _, v := range s.VMs {
					if v.State == api.VMReady {
						ready++
					}
				}
				for _, q := range s.Queue {
					interactive = interactive || q.Priority == api.PriorityInteractive
				}
				return len(s.Queue) == cfg.QueueCap && interactive && ready >= 4
			}, func(ctx context.Context, s api.Snapshot) error {
				for _, v := range s.VMs {
					if v.State == api.VMReady && len(deleted) < 4 {
						if _, err := f.DeleteVM(ctx, v.ID); err != nil {
							return err
						}
						deleted[v.ID] = true
					}
				}
				if len(deleted) != 4 {
					return fmt.Errorf("overload needs four Ready VMs to release")
				}
				return nil
			})
		}}, Step{35 * time.Second, func(ctx context.Context, f System) error {
			// Refill the four queue positions consumed by queued placements.
			for i := range 4 {
				if _, err := f.CreateVM(ctx, fmt.Sprintf("overload-refill-%d", i), api.CreateVMRequest{Pool: "ios-xcode16-m", Priority: "batch"}); err != nil {
					return err
				}
			}
			return nil
		}})
		for i := range run.Expect {
			if run.Expect[i].Name == "previously Ready VMs recovered" {
				run.Expect[i].NewTest = func() Predicate { return readyCohortExcept(deleted) }
			}
		}
		run.Expect = append(run.Expect, Always("queue ordered by effective class", func(s api.Snapshot) bool {
			for i := 1; i < len(s.Queue); i++ {
				if s.Queue[i-1].EffectivePriority > s.Queue[i].EffectivePriority {
					return false
				}
			}
			return true
		}), Eventually(run.Duration, "all 24 slots Ready under overload", func(s api.Snapshot) bool {
			ready, interactive, terminated := 0, 0, 0
			for _, v := range s.VMs {
				if v.State == api.VMReady {
					ready++
					if v.Priority == api.PriorityInteractive {
						interactive++
					}
				}
				if deleted[v.ID] && v.State == api.VMTerminated && v.Desired == api.DesiredDeleted {
					terminated++
				}
			}
			return len(deleted) == 4 && terminated == 4 && ready == cfg.Racks*cfg.HostsPerRack*cfg.SlotsPerHost &&
				interactive > 0 && len(s.Queue) == cfg.QueueCap && s.Counters.Shed > 0 && s.Counters.Rejected429 > 0
		}))
		// Join the complete source log to immutable requested classes only after
		// the final frame. Snapshot delivery and event delivery are independent.
		var events []api.Event
		audit := Eventually(run.Duration, "placements serve queue head and shedding serves lowest class", func(s api.Snapshot) bool {
			vms := map[string]api.VM{}
			for _, v := range s.VMs {
				vms[v.ID] = v
			}
			queued := map[string]bool{}
			effective := func(id string, at int64) api.Priority {
				v := vms[id]
				return max(api.PriorityInteractive, v.Priority-api.Priority((at-v.CreatedAt)/cfg.AgingStep.Milliseconds()))
			}
			placements, sheds := 0, 0
			for _, e := range events {
				if e.Kind == api.EventAdmission && e.Detail == "queued" {
					if _, ok := vms[e.VMID]; !ok {
						return false
					}
					queued[e.VMID] = true
				}
				if e.Kind != api.EventTransition {
					continue
				}
				if e.To == api.VMBooting && e.Detail == "placed" {
					if _, ok := vms[e.VMID]; !ok {
						return false
					}
					for id := range queued {
						p, candidate := effective(id, e.At), effective(e.VMID, e.At)
						if p < candidate || (p == candidate && (vms[id].CreatedAt < vms[e.VMID].CreatedAt ||
							(vms[id].CreatedAt == vms[e.VMID].CreatedAt && id < e.VMID))) {
							return false
						}
					}
					if queued[e.VMID] {
						placements++
					}
					delete(queued, e.VMID)
				}
				if e.To == api.VMFailed && e.Detail == api.ReasonShed {
					if !queued[e.VMID] {
						return false
					}
					for id := range queued {
						if effective(id, e.At) > effective(e.VMID, e.At) ||
							(vms[e.VMID].Priority == api.PriorityInteractive && vms[id].Priority > api.PriorityInteractive) {
							return false
						}
					}
					sheds++
				}
				if e.To == api.VMFailed || e.To == api.VMTerminating || e.To == api.VMTerminated {
					delete(queued, e.VMID)
				}
			}
			if len(queued) != len(s.Queue) {
				return false
			}
			for _, q := range s.Queue {
				if !queued[q.VMID] {
					return false
				}
			}
			return placements >= 4 && sheds > 0
		})
		audit.EventTest = func(e api.Event) bool { events = append(events, e); return true }
		run.Expect = append(run.Expect, audit)
		return run
	}
	return base
}
