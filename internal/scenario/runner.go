package scenario

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

type Factory func(context.Context, Scenario, int64, float64) (System, func(), error)
type Options struct {
	Repeat, Parallel int
	Seed             int64
	Speed            float64
	Output           string
}

// Suite uses an isolated system per run. Reports remain available on failure.
func Suite(ctx context.Context, scenarios []Scenario, factory Factory, opt Options, out io.Writer) ([]api.RunReport, error) {
	if opt.Repeat < 1 || opt.Parallel < 1 || opt.Speed <= 0 {
		return nil, fmt.Errorf("repeat, parallel and speed must be positive")
	}
	type job struct {
		s     Scenario
		seed  int64
		index int
	}
	jobs := make(chan job)
	reports := make([]api.RunReport, len(scenarios)*opt.Repeat)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for range opt.Parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				id := fmt.Sprintf("%s-%d-%gx-%d", j.s.Name, j.seed, opt.Speed, WallClock.Now().UnixNano())
				r, err := Run(ctx, j.s, factory, j.seed, opt.Speed, filepath.Join(opt.Output, id))
				mu.Lock()
				reports[j.index] = r
				if err != nil && firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	for i, s := range scenarios {
		for n := range opt.Repeat {
			jobs <- job{s, opt.Seed + int64(n), i*opt.Repeat + n}
		}
	}
	close(jobs)
	wg.Wait()
	failed := false
	for i, s := range scenarios {
		passed, violations, unrecovered := 0, 0, 0
		var recovery []int64
		for _, r := range reports[i*opt.Repeat : (i+1)*opt.Repeat] {
			if r.Passed {
				passed++
			} else {
				failed = true
			}
			violations += r.Violations
			unrecovered += int(r.Metrics["recovery_failures"])
			if _, sampled := r.Metrics["p99_recover_ms"]; sampled {
				recovery = append(recovery, r.P99RecoverMs)
			}
		}
		p99 := "not sampled"
		if len(recovery) > 0 {
			p99 = fmt.Sprintf("%dms", percentile(recovery))
		}
		fmt.Fprintf(out, "%s: %d/%d passed, p99 recovery %s, unrecovered %d, violations %d\n", s.Name, passed, opt.Repeat, p99, unrecovered, violations)
	}
	if firstErr != nil {
		return reports, firstErr
	}
	if failed {
		return reports, fmt.Errorf("scenario verification failed")
	}
	return reports, nil
}

func Run(ctx context.Context, s Scenario, factory Factory, seed int64, speed float64, dir string) (report api.RunReport, err error) {
	if s.NewRun != nil {
		s = s.NewRun()
	}
	if speed <= 0 || s.Duration <= 0 {
		return report, fmt.Errorf("speed and duration must be positive")
	}
	ctx, cancelRun := context.WithTimeout(ctx, 30*time.Second+time.Duration(float64(s.Duration)/speed)*3)
	defer cancelRun()
	started := WallClock.Now()
	report = api.RunReport{RunSummary: api.RunSummary{ID: filepath.Base(dir), Scenario: s.Name, Seed: seed, Speed: speed, StartedAt: started.Format(time.RFC3339Nano)}, Metrics: map[string]float64{}}
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	events, e := os.Create(filepath.Join(dir, "events.jsonl"))
	if e != nil {
		return report, e
	}
	defer events.Close()
	snaps, e := os.Create(filepath.Join(dir, "snapshots.jsonl"))
	if e != nil {
		return report, e
	}
	defer snaps.Close()
	defer func() {
		report.DurationMs = WallClock.Now().Sub(started).Milliseconds()
		if err != nil {
			report.Checks = append(report.Checks, api.CheckResult{Name: "runner", Detail: err.Error()})
		}
		report.Passed = len(report.Checks) > 0
		for _, c := range report.Checks {
			if !c.Passed {
				report.Passed = false
			}
		}
		for name, value := range map[string]any{"metrics.json": report.Metrics, "report.json": report} {
			b, e := json.MarshalIndent(value, "", "  ")
			if e == nil {
				e = os.WriteFile(filepath.Join(dir, name), append(b, '\n'), 0o644)
			}
			if e != nil && err == nil {
				err = e
			}
		}
	}()
	f, closeFleet, e := factory(ctx, s, seed, speed)
	if e != nil {
		return report, e
	}
	defer closeFleet()
	rawSnapshots, rawEvents, cancel := f.Subscribe()
	defer cancel()
	// Drain the fleet while a synchronous action (e.g. a 60-create flood)
	// runs. Fleet subscriptions deliberately drop slow readers; recordings
	// must not lose those intermediate transitions or published frames.
	cfg := config.Default()
	snapshots := make(chan api.Snapshot, int(s.Duration/cfg.SnapshotEvery)+cfg.QueueCap)
	eventStream := make(chan api.Event, cfg.EventRetention)
	go func() {
		defer close(snapshots)
		defer close(eventStream)
		pendingEvents := rawEvents
		forward := func(ev api.Event) bool {
			select {
			case eventStream <- ev:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for {
			select {
			case ev, ok := <-pendingEvents:
				if !ok {
					pendingEvents = nil
					continue
				}
				if !forward(ev) {
					return
				}
			case snap, ok := <-rawSnapshots:
				if !ok {
					return
				}
			drain:
				for {
					select {
					case ev, ok := <-pendingEvents:
						if !ok {
							pendingEvents = nil
							break drain
						}
						if !forward(ev) {
							return
						}
					default:
						break drain
					}
				}
				select {
				case snapshots <- snap:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	var eventsIn <-chan api.Event = eventStream
	initial := f.Latest()
	origin := initial.At
	if len(initial.Racks) != s.Topology.Racks || len(initial.Hosts) != s.Topology.Racks*s.Topology.Hosts {
		return report, fmt.Errorf("topology mismatch: want %+v", s.Topology)
	}
	for _, h := range initial.Hosts {
		if h.SlotCount != s.Topology.Slots {
			return report, fmt.Errorf("slot count mismatch on %s", h.ID)
		}
	}
	steps := append([]Step(nil), s.Timeline...)
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].At < steps[j].At })
	next := 0
	// Time-zero injections (e.g. hang-next-boot) must precede initial load.
	for next < len(steps) && steps[next].At == 0 {
		if e := steps[next].Do(ctx, f); e != nil {
			return report, e
		}
		next++
	}
	cohort := map[string]bool{}
	for i := 0; i < s.Load.Count; i++ {
		created, e := f.CreateVM(ctx, fmt.Sprintf("seed-%d-%d", seed, i), api.CreateVMRequest{Pool: "ios-xcode16-m", Priority: s.Load.Priority})
		if e != nil {
			return report, e
		}
		cohort[created.ID] = true
	}
	results := make([]api.CheckResult, len(s.Expect))
	tests := make([]Predicate, len(s.Expect))
	for i, c := range s.Expect {
		results[i] = api.CheckResult{Name: c.Name, Passed: c.Until == 0 && c.Metric == ""}
		tests[i] = c.Test
		if c.NewTest != nil {
			tests[i] = c.NewTest()
		}
	}
	report.Checks = results
	lost := map[string]int64{}
	createdAt := map[string]int64{}
	firstReady := map[string]int64{}
	var recoveries []int64
	var lastEventSeq int64
	recordEvent := func(ev api.Event) error {
		if e := json.NewEncoder(events).Encode(ev); e != nil {
			return e
		}
		if lastEventSeq != 0 && ev.Seq != lastEventSeq+1 {
			return fmt.Errorf("event sequence gap: after %d received %d", lastEventSeq, ev.Seq)
		}
		if ev.Seq <= 0 {
			return fmt.Errorf("invalid event sequence %d", ev.Seq)
		}
		lastEventSeq = ev.Seq
		for i, check := range s.Expect {
			if check.EventTest != nil && !check.EventTest(ev) && results[i].Passed {
				results[i].Passed = false
				results[i].FirstBadSeq = ev.Seq
				results[i].Detail = fmt.Sprintf("failed on event %d: %s", ev.Seq, ev.Detail)
			}
		}
		if ev.Kind == api.EventChaos || ev.Kind == api.EventInvariant {
			report.Markers = append(report.Markers, ev)
		}
		if ev.Kind == api.EventTransition {
			if ev.To == api.VMReady && cohort[ev.VMID] {
				if _, ok := firstReady[ev.VMID]; !ok {
					firstReady[ev.VMID] = ev.At
				}
			}
			if ev.To == api.VMLost || ev.To == api.VMUnhealthy {
				if _, ok := lost[ev.VMID]; !ok {
					lost[ev.VMID] = ev.At
				}
			}
			if ev.To == api.VMReady {
				if at, ok := lost[ev.VMID]; ok && ev.At >= at {
					recoveries = append(recoveries, ev.At-at)
					delete(lost, ev.VMID)
				}
			}
		}
		return nil
	}
	seen := false
	watchdog := WallClock.NewTimer(30*time.Second + time.Duration(float64(s.Duration)/speed)*3)
	defer watchdog.Stop()
	for {
		select {
		case <-ctx.Done():
			return report, ctx.Err()
		case <-watchdog.C():
			return report, fmt.Errorf("snapshot stream stalled before scenario deadline")
		case ev, ok := <-eventsIn:
			if !ok {
				eventsIn = nil
				continue
			}
			if e := recordEvent(ev); e != nil {
				return report, e
			}
		case snap, ok := <-snapshots:
			if !ok {
				return report, fmt.Errorf("snapshot stream closed")
			}
			seen = true
			if e := json.NewEncoder(snaps).Encode(snap); e != nil {
				return report, e
			}
			for _, v := range snap.VMs {
				if cohort[v.ID] {
					if _, ok := createdAt[v.ID]; !ok {
						createdAt[v.ID] = v.CreatedAt
					}
				}
			}
			if snap.Sim.Intensity > report.Metrics["chaos_intensity"] {
				report.Metrics["chaos_intensity"] = snap.Sim.Intensity
			}
			elapsed := time.Duration(snap.At-origin) * time.Millisecond
			for next < len(steps) && elapsed >= steps[next].At {
				step := steps[next]
				if e := step.Do(ctx, f); e != nil {
					return report, fmt.Errorf("at %s: %w", step.At, e)
				}
				marker := api.Event{Seq: snap.Seq, At: snap.At, Kind: api.EventChaos, Detail: fmt.Sprintf("scenario %s step %d", s.Name, next)}
				report.Markers = append(report.Markers, marker)
				if e := json.NewEncoder(events).Encode(marker); e != nil {
					return report, e
				}
				next++
			}
			for i, c := range s.Expect {
				if c.Metric != "" {
					continue
				}
				good := tests[i](snap)
				if c.Until == 0 && !good && results[i].Passed {
					results[i].Passed = false
					results[i].FirstBadSeq = snap.Seq
					results[i].Detail = fmt.Sprintf("failed at sim +%s", elapsed)
				}
			}
			violations := 0
			safety := 0
			for _, v := range snap.Invariants {
				violations += v.Violations
				if v.Name != api.InvPendingPlaced && v.Name != api.InvStaleStopped {
					safety += v.Violations
				}
			}
			if float64(safety) > report.Metrics["safety_violations"] {
				report.Metrics["safety_violations"] = float64(safety)
			}
			if violations > report.Violations {
				report.Violations = violations
			}
			if elapsed >= s.Duration {
				if !seen || next != len(steps) {
					return report, fmt.Errorf("timeline did not complete")
				}
				// Events and snapshots are separate channels. Consume buffered
				// transitions before deciding whether a metric had samples.
			drainEvents:
				for {
					select {
					case ev, ok := <-eventStream:
						if !ok {
							break drainEvents
						}
						if e := recordEvent(ev); e != nil {
							return report, e
						}
					default:
						break drainEvents
					}
				}
				if len(recoveries) > 0 {
					report.P99RecoverMs = percentile(recoveries)
					report.Metrics["p99_recover_ms"] = float64(report.P99RecoverMs)
				}
				// Failed quarantine is intentional, not a promised recovery.
				for _, v := range snap.VMs {
					if v.State == api.VMFailed && v.LastReason == api.ReasonQuarantined {
						delete(lost, v.ID)
					}
				}
				report.Metrics["recovery_failures"] = float64(len(lost))
				report.Metrics["recovery_samples"] = float64(len(recoveries))
				// Target attainment is evidence, never a pass/fail check. Count
				// only first Ready for the initial interactive cohort; lost or
				// failed boots remain visible as missing samples, not successes.
				if s.Load.Priority == "interactive" && len(cohort) > 0 {
					var ready []int64
					for id, at := range firstReady {
						if created, ok := createdAt[id]; ok && at >= created {
							ready = append(ready, at-created)
						}
					}
					report.Metrics["time_to_ready_requested"] = float64(len(cohort))
					report.Metrics["time_to_ready_samples"] = float64(len(ready))
					report.Metrics["time_to_ready_target_ms"] = float64(cfg.TimeToReadyTarget.Milliseconds())
					report.Metrics["time_to_ready_target_met"] = 0
					report.Metrics["offered_load_fraction"] = float64(s.Load.Count) / float64(s.Topology.Racks*s.Topology.Hosts*s.Topology.Slots)
					if len(ready) > 0 {
						p99 := percentile(ready)
						report.Metrics["p99_time_to_ready_ms"] = float64(p99)
						if len(ready) == len(cohort) && p99 <= cfg.TimeToReadyTarget.Milliseconds() {
							report.Metrics["time_to_ready_target_met"] = 1
						}
					}
				}
				for i, c := range s.Expect {
					if c.Until > 0 {
						results[i].Passed = tests[i](snap)
						if !results[i].Passed {
							results[i].FirstBadSeq = snap.Seq
							results[i].Detail = "condition false at end of run"
						}
					}
					if c.Metric != "" {
						value, ok := report.Metrics[c.Metric]
						results[i].Passed = ok && value < c.Limit
						results[i].Detail = fmt.Sprintf("value=%g limit<%g sampled=%t", value, c.Limit, ok)
					}
				}
				report.Checks = results
				return report, nil
			}
		}
	}
}

func percentile(v []int64) int64 {
	if len(v) == 0 {
		return 0
	}
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	return v[(99*len(v)+99)/100-1]
}

// Calibrate compares each scenario's pass rate at 5× and 50× using identical
// seeds. A discrepancy selects 5×; it never turns a failed run into a pass.
func Calibrate(ctx context.Context, scenarios []Scenario, f Factory, opt Options, out io.Writer) (float64, error) {
	opt.Speed = 5
	slow, e1 := Suite(ctx, scenarios, f, opt, out)
	opt.Speed = 50
	fast, e2 := Suite(ctx, scenarios, f, opt, out)
	for _, runs := range [][]api.RunReport{slow, fast} {
		for _, r := range runs {
			if r.Metrics["safety_violations"] > 0 {
				return 5, fmt.Errorf("safety invariant failed at %gx in %s: calibration cannot cap safety failures", r.Speed, r.ID)
			}
		}
	}
	selected := float64(50)
	for i := range scenarios {
		a, b := 0, 0
		for n := range opt.Repeat {
			if slow[i*opt.Repeat+n].Passed {
				a++
			}
			if fast[i*opt.Repeat+n].Passed {
				b++
			}
		}
		if a != b {
			selected = 5
		}
	}
	fmt.Fprintf(out, "calibration: selected %gx (5x and 50x compared with identical seeds)\n", selected)
	if e1 != nil {
		return selected, fmt.Errorf("5x calibration baseline failed: %v (50x: %v)", e1, e2)
	}
	return selected, nil
}
