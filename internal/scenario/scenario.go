package scenario

import (
	"context"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
)

// System keeps the runner independent of fleet assembly and HTTP.
type System interface {
	api.ControlPlane
	api.ChaosEngine
	api.SnapshotSource
}

// FromFleet combines the separate surfaces exported by fleet.Fleet as
// f.Control, f.Chaos and f. It deliberately depends only on frozen contracts,
// so the runner can compile before the assembled fleet lands on main.
func FromFleet(control api.ControlPlane, chaos api.ChaosEngine, source api.SnapshotSource) System {
	return struct {
		api.ControlPlane
		api.ChaosEngine
		api.SnapshotSource
	}{control, chaos, source}
}

type Topology struct{ Racks, Hosts, Slots int }
type Load struct {
	Count    int
	Priority string
}
type Action func(context.Context, System) error
type Step struct {
	At time.Duration
	Do Action
}
type Predicate func(api.Snapshot) bool
type Check struct {
	Name  string
	Until time.Duration // zero means Always; nonzero checks the final frame, not an earlier success
	Test  Predicate
	// EventTest checks transitions that may occur between snapshot frames.
	// Like Always, a failure is permanent for the run.
	EventTest func(api.Event) bool
	// NewTest allocates observation state separately for each parallel run.
	NewTest func() Predicate
	Metric  string
	Limit   float64
}
type Scenario struct {
	Name     string
	Topology Topology
	Load     Load
	Duration time.Duration
	Timeline []Step
	Expect   []Check
	// NewRun isolates state shared by a scenario's actions and assertions.
	NewRun func() Scenario
}

func Always(name string, p Predicate) Check { return Check{Name: name, Test: p} }
func Eventually(by time.Duration, name string, p Predicate) Check {
	return Check{Name: name, Until: by, Test: p}
}

// Metric checks a measured value, failing if no sample was observed.
func Metric(name string, lessThan float64) Check {
	return Check{Name: name, Metric: name, Limit: lessThan}
}

func NoInvariantViolations(s api.Snapshot) bool {
	if len(s.Invariants) != len(api.AllInvariants) {
		return false
	}
	seen := make(map[api.InvariantName]bool, len(s.Invariants))
	for _, i := range s.Invariants {
		if i.Violations != 0 || seen[i.Name] {
			return false
		}
		seen[i.Name] = true
	}
	for _, name := range api.AllInvariants {
		if !seen[name] {
			return false
		}
	}
	return s.Security.Succeeded == 0 && s.Counters.QueueDepth <= s.Counters.QueueCap
}
func AllDesiredVMsReady(s api.Snapshot) bool {
	if len(s.VMs) == 0 {
		return false
	}
	for _, v := range s.VMs {
		if v.Desired == api.DesiredRunning && v.State != api.VMReady {
			return false
		}
	}
	return true
}
func NoStaleCopies(s api.Snapshot) bool {
	observed := make(map[string]bool, len(s.Observed))
	current := make(map[string]api.VM, len(s.VMs))
	for _, v := range s.VMs {
		current[v.ID] = v
	}
	for _, h := range s.Observed {
		// A disconnected driver cannot establish that an old copy stopped.
		if !h.Connected {
			return false
		}
		observed[h.HostID] = true
		for _, actual := range h.Actual {
			if !actual.Running {
				continue
			}
			v, ok := current[actual.VMID]
			if !ok || v.Generation != actual.Generation || v.HostID != h.HostID ||
				v.Desired != api.DesiredRunning || (v.State != api.VMBooting && v.State != api.VMReady && v.State != api.VMUnhealthy) {
				return false
			}
		}
	}
	for _, h := range s.Hosts {
		if h.Lease == api.LeaseHealthy && !observed[h.ID] {
			return false
		}
	}
	return true
}

func Chaos(action, target string, params map[string]float64) Action {
	return func(ctx context.Context, f System) error {
		// VM IDs are assigned by the implementation; select an actual Ready VM.
		if target == "@ready" {
			targetID := ""
			return selectTargets(ctx, f, func(s api.Snapshot) bool {
				for _, v := range s.VMs {
					if v.State == api.VMReady {
						targetID = v.ID
						return true
					}
				}
				return false
			}, func(ctx context.Context, _ api.Snapshot) error {
				_, err := f.Do(ctx, action, api.ChaosRequest{Target: targetID, Params: params})
				return err
			})
		}
		_, err := f.Do(ctx, action, api.ChaosRequest{Target: target, Params: params})
		return err
	}
}

// WallClock is used only for supervision and reporting, never sim deadlines.
var WallClock clock.Clock = clock.Real{}
