package chaos

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/hami-sh/vm/internal/api"
)

type workload struct {
	api.Workload
	policy  api.Lifetime
	ending  string
	retryAt time.Time
}

func (s *state) workloadStatus() []api.Workload {
	var out []api.Workload
	for _, w := range s.workloads {
		if w.Phase != "" {
			out = append(out, w.Workload)
		}
	}
	slices.SortFunc(out, func(a, b api.Workload) int { return strings.Compare(a.VMID, b.VMID) })
	return out
}

func (s *state) workloadTick(ctx context.Context, now time.Time) {
	if len(s.workloads) == 0 {
		return // In particular, off never changes snapshot reads or RNG draws.
	}
	snap, err := s.backend.Snapshot(ctx)
	if err != nil {
		return
	}
	byID := make(map[string]api.VM, len(snap.VMs))
	for _, v := range snap.VMs {
		byID[v.ID] = v
	}
	ids := make([]string, 0, len(s.workloads))
	for id := range s.workloads {
		ids = append(ids, id)
	}
	slices.Sort(ids) // Stable draw order, independent of Go map iteration.
	for _, id := range ids {
		w := s.workloads[id]
		v, found := byID[id]
		if !found {
			continue // A store wipe must not erase the tenant's timer.
		}
		if v.State == api.VMTerminated || v.State == api.VMFailed {
			delete(s.workloads, id)
			continue
		}
		if w.Phase == "" {
			if v.State != api.VMReady {
				continue
			}
			w.Phase = api.WorkloadBusy
			w.EndsAt = now.UnixMilli() + w.policy.MinMs + s.lifetimeRNG.Int63n(w.policy.MaxMs-w.policy.MinMs+1)
		}
		if now.UnixMilli() < w.EndsAt {
			continue
		}
		if w.Phase == api.WorkloadBusy && w.ending == "" {
			if s.lifetimeRNG.Float64() < w.policy.ReleaseP {
				w.ending = "completed"
			} else {
				w.Phase = api.WorkloadIdle
				w.EndsAt += w.policy.IdleMs
				w.ending = "idle"
			}
		}
		if now.UnixMilli() < w.EndsAt || v.State != api.VMReady || now.Before(w.retryAt) {
			continue
		}
		// Other work in this tick may have changed the fleet. Re-read just
		// before deletion; only the remaining snapshot/delete race is tolerated.
		latest, err := s.backend.Snapshot(ctx)
		if err != nil || !slices.ContainsFunc(latest.VMs, func(v api.VM) bool { return v.ID == id && v.State == api.VMReady }) {
			continue
		}
		if _, err := s.backend.ControlPlane().DeleteVM(ctx, id); err != nil {
			var e *api.Error
			if errors.As(err, &e) && (e.Status == 503 || e.Status == 429) {
				w.retryAt = s.clock.Now().Add(time.Duration(e.RetryAfterS) * time.Second)
			}
			continue
		}
		delete(s.workloads, id) // Success is never replayed, even on a stale snapshot.
		if s.emit != nil {
			s.emit(api.Event{At: s.clock.Now().UnixMilli(), Kind: api.EventWorkload, VMID: id, Detail: w.ending})
		}
	}
}
