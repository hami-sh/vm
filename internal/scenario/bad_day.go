package scenario

import (
	"context"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

// Make post-restart admission observable without preempting serving work.
func badDayProgress(run Scenario) Scenario {
	cfg := config.Default()
	deleted := map[string]bool{}
	run.Timeline = append(run.Timeline, Step{45 * time.Second, func(ctx context.Context, f System) error {
		var candidates []api.VM
		return selectTargets(ctx, f, func(s api.Snapshot) bool {
			candidates = nil
			usable := map[string]bool{}
			for _, h := range s.Hosts {
				usable[h.ID] = h.Lease == api.LeaseHealthy && h.AdminState == api.AdminActive
			}
			for _, v := range s.VMs {
				if v.State == api.VMReady && usable[v.HostID] {
					candidates = append(candidates, v)
				}
			}
			return len(s.Queue) >= 2 && len(candidates) >= 2
		}, func(ctx context.Context, _ api.Snapshot) error {
			for _, v := range candidates[:2] {
				if _, err := f.DeleteVM(ctx, v.ID); err != nil {
					return err
				}
				deleted[v.ID] = true
			}
			return nil
		})
	}})
	for i := range run.Expect {
		if run.Expect[i].Name == "previously Ready VMs recovered" {
			run.Expect[i].NewTest = func() Predicate { return readyCohortExcept(deleted) }
		}
	}
	var events []api.Event
	placements := map[string]bool{}
	admission := Eventually(run.Duration, "post-restart freed capacity serves queue heads within boot deadline", func(s api.Snapshot) bool {
		vms := map[string]api.VM{}
		for _, v := range s.VMs {
			vms[v.ID] = v
		}
		queued := map[string]api.QueueEntry{}
		served := map[string]bool{}
		ready := map[string]bool{}
		// Each deletion releases one slot on its host, starting at the durable
		// delete transition rather than a potentially older published clock.
		deadlines := map[string][]int64{}
		terminations := 0
		effective := func(q api.QueueEntry, at int64) api.Priority {
			if q.Priority == api.PriorityReplacement {
				return q.Priority
			}
			return max(api.PriorityInteractive, q.Priority-api.Priority((at-q.EnqueuedAt)/cfg.AgingStep.Milliseconds()))
		}
		for _, e := range events {
			if e.Kind == api.EventAdmission && e.Detail == "queued" {
				v, ok := vms[e.VMID]
				if !ok {
					return false
				}
				queued[e.VMID] = api.QueueEntry{VMID: v.ID, Priority: v.Priority, EnqueuedAt: v.CreatedAt}
			}
			if e.Kind != api.EventTransition {
				continue
			}
			if e.To == api.VMReady {
				ready[e.VMID] = true
			}
			if e.To == api.VMPending && e.Detail != "created" {
				v, ok := vms[e.VMID]
				if !ok {
					return false
				}
				q := api.QueueEntry{VMID: v.ID, Priority: v.Priority, EnqueuedAt: v.CreatedAt}
				if ready[e.VMID] || e.Detail == api.ReasonDrained {
					q.Priority, q.EnqueuedAt = api.PriorityReplacement, e.At
				}
				queued[e.VMID] = q
			}
			if deleted[e.VMID] && e.To == api.VMTerminating {
				deadlines[e.HostID] = append(deadlines[e.HostID], e.At+cfg.BootDeadline.Milliseconds())
			}
			if deleted[e.VMID] && e.To == api.VMTerminated {
				terminations++
			}
			if e.To == api.VMBooting {
				if due := deadlines[e.HostID]; len(due) > 0 && e.From == api.VMPending {
					q, ok := queued[e.VMID]
					if !ok || e.At > due[0] {
						return false
					}
					for _, other := range queued {
						if effective(other, e.At) < effective(q, e.At) {
							return false
						}
					}
					served[e.VMID] = true
					deadlines[e.HostID] = due[1:]
				}
				delete(queued, e.VMID)
			}
			if e.To == api.VMFailed || e.To == api.VMTerminating || e.To == api.VMTerminated {
				delete(queued, e.VMID)
			}
		}
		placements = served
		for _, due := range deadlines {
			if len(due) != 0 {
				return false
			}
		}
		return len(deleted) == 2 && terminations == 2 && len(served) == 2
	})
	admission.EventTest = func(e api.Event) bool { events = append(events, e); return true }
	run.Expect = append(run.Expect, admission, Eventually(run.Duration, "post-restart queued placement ends Ready", func(s api.Snapshot) bool {
		for _, v := range s.VMs {
			if placements[v.ID] && v.State == api.VMReady {
				return true
			}
		}
		return false
	}))
	return run
}
