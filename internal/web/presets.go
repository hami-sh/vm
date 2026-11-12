package web

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/web/recording"
)

// Subscribe before applying the preset so its first action is in the recording.
// Only one dashboard preset runs at a time; reset interrupts it explicitly.
func (s *Server) startPreset(ctx context.Context, action string, req api.ChaosRequest) (api.ChaosResult, error) {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if s.closing || s.runCancel != nil {
		return api.ChaosResult{}, &api.Error{Status: 409, Msg: "a preset is already recording or the server is closing"}
	}
	if s.o.Snapshots == nil {
		return api.ChaosResult{}, s.unavailable("snapshot source")
	}
	initial := s.o.Snapshots.Latest()
	id := "dashboard-" + action + "-" + uuid.NewString()
	w, err := recording.Create(filepath.Join(s.o.RunsDir, id))
	if err != nil {
		return api.ChaosResult{}, err
	}
	snaps, events, unsubscribe := s.o.Snapshots.Subscribe()
	if err = w.Snapshot(initial); err == nil {
		_, err = s.o.Chaos.Do(ctx, action, req)
	}
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
		if err := s.recordPreset(runCtx, w, id, action, initial, snaps, events); err != nil {
			s.o.Logf("dashboard recording %s: %v", id, err)
		}
	}()
	return api.ChaosResult{Action: action, Detail: "Recording " + id + "; available in Runs when finished"}, nil
}

func (s *Server) recordPreset(ctx context.Context, w *recording.Writer, id, action string, initial api.Snapshot, snaps <-chan api.Snapshot, events <-chan api.Event) (err error) {
	defer func() { err = errors.Join(err, w.Close()) }()
	cfg := s.o.Config
	// Cover the last scheduled fault/heal or rack drain, then allow recovery.
	// All durations derive from config, never from wall speed or UI timers.
	duration := max(cfg.PresetHealAfter, 2*cfg.PresetStepEvery)
	if action == "rolling-maintenance" {
		duration = time.Duration(max(0, len(initial.Racks)-1))*cfg.PresetHealAfter + cfg.DrainGrace
	}
	duration += cfg.BootDeadline + cfg.HostLeaseTTL
	report := api.RunReport{RunSummary: api.RunSummary{
		ID: id, Scenario: action, Seed: initial.Sim.Seed, Speed: initial.Sim.Speed,
		StartedAt: s.o.Clock.Now().UTC().Format(time.RFC3339),
	}, Metrics: map[string]float64{}}
	seen := map[int64]bool{}
	for _, e := range initial.Recent {
		seen[e.Seq] = true
	}
	baseline := map[api.InvariantName]int{}
	counts := map[api.InvariantName]int{}
	for _, inv := range initial.Invariants {
		baseline[inv.Name] = inv.Violations
	}
	addEvent := func(e api.Event) error {
		if e.At < initial.At || seen[e.Seq] {
			return nil
		}
		seen[e.Seq] = true
		if e.Kind == api.EventChaos || e.Kind == api.EventInvariant {
			report.Markers = append(report.Markers, e)
		}
		return w.Event(e)
	}
	latest := initial
	completed := false
loop:
	for {
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
			for _, e := range snap.Recent {
				if err = addEvent(e); err != nil {
					return err
				}
			}
			for _, inv := range snap.Invariants {
				counts[inv.Name] = max(counts[inv.Name], inv.Violations-baseline[inv.Name])
			}
			if err = w.Snapshot(snap); err != nil {
				return err
			}
			if snap.At-initial.At >= duration.Milliseconds() {
				completed = true
				break loop
			}
		}
	}
	report.DurationMs = max(0, latest.At-initial.At)
	report.Checks = append(report.Checks, api.CheckResult{Name: "recording completed", Passed: completed, Detail: "Reset or server shutdown interrupts the observation window"})
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
	report.Passed = completed && report.Violations == 0
	if err = w.Flush(); err != nil {
		return err
	}
	return w.WriteReport(report)
}
