package web

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/scenario"
	"github.com/hami-sh/vm/internal/web/recording"
)

// Subscribe before applying the preset so its first action is in the recording.
// Only one dashboard preset runs at a time; reset interrupts it explicitly.
func (s *Server) startPreset(ctx context.Context, action string) (api.ChaosResult, error) {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if s.closing || s.runCancel != nil {
		return api.ChaosResult{}, &api.Error{Status: 409, Msg: "a preset is already recording or the server is closing"}
	}
	if s.o.Snapshots == nil || s.o.ControlPlane == nil {
		return api.ChaosResult{}, s.unavailable("scenario system")
	}
	definitions := scenario.Catalog()
	i := slices.IndexFunc(definitions, func(d scenario.Scenario) bool { return d.Name == action })
	if i < 0 {
		return api.ChaosResult{}, fmt.Errorf("no scenario expectations for %s", action)
	}
	definition := definitions[i]
	if definition.NewRun != nil {
		definition = definition.NewRun()
	}
	initial := s.o.Snapshots.Latest()
	// Snapshot publication can lag a just-applied seed or speed change.
	status, err := s.o.Chaos.Sim(ctx, "status", api.SimRequest{})
	if err != nil {
		return api.ChaosResult{}, err
	}
	initial.Sim = status
	id := "dashboard-" + action + "-" + uuid.NewString()
	w, err := recording.Create(filepath.Join(s.o.RunsDir, id))
	if err != nil {
		return api.ChaosResult{}, err
	}
	snaps, events, unsubscribe := s.o.Snapshots.Subscribe()
	err = w.Snapshot(initial)
	if err != nil {
		unsubscribe()
		w.Close()
		os.RemoveAll(w.Dir()) // this request's disposable, unlisted recording
		return api.ChaosResult{}, err
	}
	runCtx, cancel := context.WithCancel(s.ctx)
	s.runCancel = cancel
	s.runWG.Add(1)
	go func() {
		defer s.runWG.Done()
		defer func() {
			cancel()
			unsubscribe()
			s.runMu.Lock()
			s.runCancel = nil
			s.runMu.Unlock()
		}()
		if err := s.recordPreset(runCtx, w, id, definition, initial, snaps, events); err != nil {
			s.o.Logf("dashboard recording %s: %v", id, err)
		}
	}()
	return api.ChaosResult{Action: action, Detail: "Recording " + id + "; available in Runs when finished"}, nil
}

func (s *Server) recordPreset(ctx context.Context, w *recording.Writer, id string, definition scenario.Scenario, initial api.Snapshot, snaps <-chan api.Snapshot, events <-chan api.Event) (err error) {
	defer func() { err = errors.Join(err, w.Close()) }()
	report := api.RunReport{RunSummary: api.RunSummary{
		ID: id, Scenario: definition.Name, Seed: initial.Sim.Seed, Speed: initial.Sim.Speed,
		StartedAt: s.o.Clock.Now().UTC().Format(time.RFC3339),
	}, Metrics: map[string]float64{}}
	results := make([]api.CheckResult, len(definition.Expect))
	tests := make([]scenario.Predicate, len(definition.Expect))
	for i, check := range definition.Expect {
		results[i] = api.CheckResult{Name: check.Name, Passed: check.Until == 0 && check.Metric == ""}
		tests[i] = check.Test
		if check.NewTest != nil {
			tests[i] = check.NewTest()
		}
	}
	var initialEventSeq int64
	for _, e := range initial.Recent {
		initialEventSeq = max(initialEventSeq, e.Seq)
	}
	baseline := map[api.InvariantName]int{}
	counts := map[api.InvariantName]int{}
	for _, inv := range initial.Invariants {
		baseline[inv.Name] = inv.Violations
	}
	conditions := api.CheckResult{Name: "conditions unchanged", Passed: true}
	observe := func(snap api.Snapshot) {
		// Ignore the starting publication, but still detect changes while paused.
		if (snap.At > initial.At || snap.Seq > initial.Seq) && conditions.Passed {
			var changes []string
			if snap.Sim.Seed != report.Seed {
				changes = append(changes, fmt.Sprintf("seed changed %d→%d", report.Seed, snap.Sim.Seed))
			}
			if snap.Sim.Speed != report.Speed {
				changes = append(changes, fmt.Sprintf("speed changed %g×→%g×", report.Speed, snap.Sim.Speed))
			}
			if len(changes) > 0 {
				conditions.Passed = false
				conditions.FirstBadSeq = snap.Seq
				conditions.Detail = strings.Join(changes, "; ") + " at " + time.UnixMilli(snap.At).UTC().Format("15:04:05")
			}
		}
		// A dashboard run shares the fleet's lifetime counters. Evaluate only
		// this run's invariant deltas, while retaining the full security state.
		snap.Invariants = slices.Clone(snap.Invariants)
		for i := range snap.Invariants {
			inv := &snap.Invariants[i]
			inv.Violations = max(0, inv.Violations-baseline[inv.Name])
		}
		elapsed := time.Duration(snap.At-initial.At) * time.Millisecond
		for i, check := range definition.Expect {
			if check.Metric != "" {
				continue
			}
			good := tests[i](snap)
			if check.Until == 0 && !good && results[i].Passed {
				results[i].Passed = false
				results[i].FirstBadSeq = snap.Seq
				results[i].Detail = fmt.Sprintf("failed at sim +%s", elapsed)
			}
		}
	}
	observe(initial)
	contiguous := api.CheckResult{Name: "recording events contiguous", Passed: true}
	// The subscription starts after Latest; anchor at its first source event,
	// not at an older publication's recent-event window.
	var lastEventSeq int64
	addEvent := func(e api.Event) error {
		if e.Seq <= initialEventSeq && e.Seq > 0 {
			return nil
		}
		if (e.Seq <= 0 || (lastEventSeq != 0 && e.Seq != lastEventSeq+1)) && contiguous.Passed {
			contiguous.Passed = false
			contiguous.Detail = fmt.Sprintf("event sequence gap: after %d received %d", lastEventSeq, e.Seq)
		}
		lastEventSeq = e.Seq
		for i, check := range definition.Expect {
			if check.EventTest != nil && !check.EventTest(e) && results[i].Passed {
				results[i].Passed = false
				results[i].FirstBadSeq = e.Seq
				results[i].Detail = fmt.Sprintf("failed on event %d: %s", e.Seq, e.Detail)
			}
		}
		if e.Kind == api.EventChaos || e.Kind == api.EventInvariant {
			report.Markers = append(report.Markers, e)
		}
		return w.Event(e)
	}
	latest := initial
	completed := false
	system := scenario.FromFleet(s.o.ControlPlane, s.o.Chaos, s.o.Snapshots)
	steps := slices.Clone(definition.Timeline)
	slices.SortStableFunc(steps, func(a, b scenario.Step) int { return cmp.Compare(a.At, b.At) })
	next := 0
	var actionErr error
	advance := func(elapsed time.Duration) {
		for next < len(steps) && elapsed >= steps[next].At && actionErr == nil {
			actionErr = steps[next].Do(ctx, system)
			if actionErr != nil {
				actionErr = fmt.Errorf("at %s: %w", steps[next].At, actionErr)
				return
			}
			report.Markers = append(report.Markers, api.Event{At: latest.At, Kind: api.EventChaos, Detail: fmt.Sprintf("scenario %s step %d", definition.Name, next)})
			next++
		}
	}
	// Materialize once: timeline actions populate the same closure state that
	// assertions inspect. Time-zero injections precede the initial load.
	advance(0)
	for i := 0; i < definition.Load.Count && actionErr == nil; i++ {
		_, actionErr = system.CreateVM(ctx, fmt.Sprintf("%s-seed-%d-%d", id, report.Seed, i), api.CreateVMRequest{Pool: "ios-xcode16-m", Priority: definition.Load.Priority})
	}
loop:
	for actionErr == nil {
		select {
		case <-ctx.Done():
			break loop
		case e, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			if err = addEvent(e); err != nil {
				return err
			}
		case snap, ok := <-snaps:
			if !ok {
				break loop
			}
			if snap.At < initial.At {
				break loop
			}
			latest = snap
			advance(time.Duration(snap.At-initial.At) * time.Millisecond)
			observe(snap)
			for _, inv := range snap.Invariants {
				counts[inv.Name] = max(counts[inv.Name], inv.Violations-baseline[inv.Name])
			}
			if err = w.Snapshot(snap); err != nil {
				return err
			}
			if snap.At-initial.At >= definition.Duration.Milliseconds() && actionErr == nil {
				completed = true
				break loop
			}
		}
	}
	// A final frame can race with its source events. Evaluate every buffered
	// event before final-frame and metric checks, as the CLI runner does.
drain:
	for {
		select {
		case e, ok := <-events:
			if !ok {
				break drain
			}
			if err = addEvent(e); err != nil {
				return err
			}
		default:
			break drain
		}
	}
	report.DurationMs = max(0, latest.At-initial.At)
	report.Checks = append(report.Checks, api.CheckResult{Name: "recording completed", Passed: completed, Detail: "Reset or server shutdown interrupts the observation window"})
	actions := api.CheckResult{Name: "scenario actions completed", Passed: actionErr == nil && next == len(steps)}
	if actionErr != nil {
		actions.Detail = actionErr.Error()
	}
	report.Checks = append(report.Checks, actions, contiguous)
	for _, name := range api.AllInvariants {
		n := counts[name]
		report.Violations += n
		check := api.CheckResult{Name: string(name), Passed: n == 0}
		for _, inv := range latest.Invariants {
			if inv.Name == name && n > 0 {
				check.Detail = inv.LastDetail
			}
		}
		report.Checks = append(report.Checks, check)
	}
	// Snapshot counters survive dropped subscriber events. Conversely, retain
	// an observed violation even if reset cleared the final snapshot counters.
	violations := 0
	for _, e := range report.Markers {
		if e.Kind == api.EventInvariant {
			violations++
		}
	}
	report.Violations = max(report.Violations, violations)
	report.Checks = append(report.Checks, api.CheckResult{Name: "zero invariant violations during recording", Passed: report.Violations == 0})
	if err = w.Flush(); err != nil {
		return err
	}
	if err = recording.DeriveMetrics(w.Dir(), &report); err != nil {
		return err
	}
	for i, check := range definition.Expect {
		if check.Until > 0 {
			results[i].Passed = tests[i](latest)
			if !results[i].Passed {
				results[i].FirstBadSeq = latest.Seq
				results[i].Detail = "condition false at end of run"
			}
		}
		if check.Metric != "" {
			value, ok := report.Metrics[check.Metric]
			if check.Metric == "p99_recover_ms" {
				ok = ok && report.Metrics["recovery_samples"] > 0
			}
			results[i].Passed = ok && value < check.Limit
			results[i].Detail = fmt.Sprintf("value=%g limit<%g sampled=%t", value, check.Limit, ok)
		}
	}
	report.Checks = append(report.Checks, conditions)
	report.Checks = append(report.Checks, results...)
	report.Passed = true
	for _, check := range report.Checks {
		report.Passed = report.Passed && check.Passed
	}
	return w.WriteReport(report)
}
