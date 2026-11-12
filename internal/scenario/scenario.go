package scenario

import (
	"context"
	"fmt"
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
	Until time.Duration // zero means Always, otherwise satisfied by the deadline
	Test  Predicate
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
	for _, h := range s.Hosts {
		if len(h.Stale) > 0 {
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
			for _, v := range f.Latest().VMs {
				if v.State == api.VMReady {
					targetID = v.ID
					break
				}
			}
			if targetID == "" {
				return fmt.Errorf("%s: no Ready VM", action)
			}
			_, err := f.Do(ctx, action, api.ChaosRequest{Target: targetID, Params: params})
			return err
		}
		_, err := f.Do(ctx, action, api.ChaosRequest{Target: target, Params: params})
		return err
	}
}

// WallClock is used only for supervision and reporting, never sim deadlines.
var WallClock clock.Clock = clock.Real{}
