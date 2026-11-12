// Command webdev serves the dashboard against the fixture-backed fake fleet,
// for working on web/static without the real control plane:
//
//	go run ./internal/web/cmd/webdev
//
// Static files are read from disk on every request, so an edit shows on
// reload. Unless -runs is given it writes two sample recordings to a
// temporary directory so the Runs page has something to play.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/web"
	"github.com/hami-sh/vm/internal/web/fake"
	"github.com/hami-sh/vm/internal/web/recording"
)

func main() {
	addr := flag.String("addr", "", "listen address (default :$PORT or :8080)")
	fixture := flag.String("fixture", "fixtures/snapshot.json", "snapshot the fake fleet starts from")
	staticDir := flag.String("static", "web/static", "dashboard files, served from disk")
	runsDir := flag.String("runs", "", "recordings directory (default: a temporary one with sample runs)")
	flag.Parse()
	if *addr == "" {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		*addr = ":" + port
	}

	base, err := fake.LoadFixture(*fixture)
	if err != nil {
		log.Fatalf("fixture: %v", err)
	}
	if *runsDir == "" {
		dir, err := os.MkdirTemp("", "webdev-runs-")
		if err != nil {
			log.Fatal(err)
		}
		if err := seedRuns(dir, base, clock.Real{}.Now()); err != nil {
			log.Fatalf("sample runs: %v", err)
		}
		*runsDir = dir
	}

	fleet := fake.New(base)
	go fleet.Run(context.Background(), clock.Real{})
	srv := web.New(web.Options{
		ControlPlane: fleet,
		Chaos:        fleet,
		Snapshots:    fleet,
		Config:       config.Default(),
		RunsDir:      *runsDir,
		Fixture:      true,
		Static:       os.DirFS(*staticDir),
	})
	log.Printf("webdev: fake fleet from %s, static from %s, runs in %s, listening on %s", *fixture, *staticDir, *runsDir, *addr)
	log.Fatal(http.ListenAndServe(*addr, srv))
}

// step is one scripted chaos action in a sample run.
type step struct {
	at     int // snapshot index
	action string
	target string
	params map[string]float64
}

// sample describes one fabricated recording.
type sample struct {
	id, scenario string
	seed         int64
	script       []step
	// violateAt, when positive, is the snapshot index at which the run
	// reports an invariant violation, so the Runs page has a red marker
	// and a failing check to show.
	violateAt int
}

// seedRuns records the fake fleet under a few scripted faults.
func seedRuns(root string, base api.Snapshot, started time.Time) error {
	samples := []sample{
		{id: "split-brain-42", scenario: "split-brain", seed: 42, script: []step{
			{at: 40, action: "partition-rack", target: "r2"},
			{at: 70, action: "flood", params: map[string]float64{"count": 12}},
			{at: 150, action: "heal-rack", target: "r2"},
			{at: 210, action: "kill-scheduler"},
			{at: 240, action: "restart-scheduler"},
		}},
		{id: "bad-day-7", scenario: "bad-day", seed: 7, violateAt: 190, script: []step{
			{at: 30, action: "kill-host", target: "r1-h2"},
			{at: 60, action: "noise", params: map[string]float64{"intensity": 0.8}},
			{at: 90, action: "pause-heartbeats", target: "r3-h1"},
			{at: 120, action: "power-off-rack", target: "r2"},
			{at: 160, action: "flood", params: map[string]float64{"count": 40}},
			{at: 230, action: "noise", params: map[string]float64{"intensity": 0.1}},
			{at: 250, action: "resume-heartbeats", target: "r3-h1"},
		}},
	}
	for i, s := range samples {
		if err := seedRun(filepath.Join(root, s.id), s, base, started.Add(-time.Duration(len(samples)-i)*7*time.Minute)); err != nil {
			return fmt.Errorf("%s: %w", s.id, err)
		}
	}
	return nil
}

func seedRun(dir string, s sample, base api.Snapshot, started time.Time) error {
	const (
		snapshots = 300
		wallStep  = 200 * time.Millisecond
	)
	base.Sim.Seed = s.seed
	base.Sim.Scenario = s.scenario
	fleet := fake.New(base)
	w, err := recording.Create(dir)
	if err != nil {
		return err
	}
	defer w.Close()

	ctx := context.Background()
	var (
		markers   []api.Event
		violation *api.Event
		first     int64
	)
	for i := range snapshots {
		for _, st := range s.script {
			if st.at == i {
				if _, err := fleet.Do(ctx, st.action, api.ChaosRequest{Target: st.target, Params: st.params}); err != nil {
					return fmt.Errorf("%s %s: %w", st.action, st.target, err)
				}
			}
		}
		fleet.Step(wallStep)
		snap := fleet.Latest()
		if i == 0 {
			first = snap.At
		}
		if s.violateAt > 0 && i == s.violateAt {
			violation = &api.Event{Seq: 1_000_000, At: snap.At, Kind: api.EventInvariant, HostID: "r2-h3",
				VMID: "vm-0008", Detail: string(api.InvStaleStopped) + ": stale gen 1 still running 4.2 s after its lease ended"}
			snap.Recent = append(snap.Recent, *violation)
		}
		if violation != nil {
			for j := range snap.Invariants {
				if snap.Invariants[j].Name == api.InvStaleStopped {
					snap.Invariants[j].Violations = 1
					snap.Invariants[j].LastAt = violation.At
					snap.Invariants[j].LastDetail = violation.Detail
				}
			}
		}
		for _, e := range snap.Recent {
			if e.Kind == api.EventChaos || e.Kind == api.EventInvariant {
				markers = append(markers, e)
			}
			if err := w.Event(e); err != nil {
				return err
			}
		}
		if err := w.Snapshot(snap); err != nil {
			return err
		}
	}
	last := fleet.Latest()

	rep := api.RunReport{
		RunSummary: api.RunSummary{
			ID: s.id, Scenario: s.scenario, Seed: s.seed, Speed: last.Sim.Speed,
			Passed: violation == nil, P99RecoverMs: 9400 + s.seed%10*130,
			StartedAt:  started.UTC().Format(time.RFC3339),
			DurationMs: last.At - first,
		},
		Checks: []api.CheckResult{
			{Name: "zero invariant violations", Passed: violation == nil},
			{Name: "every replacement Ready within the recovery target", Passed: true, Detail: "p99 recovery under 12 s"},
			{Name: "no successful attack", Passed: true, Detail: fmt.Sprintf("%d blocked", last.Security.Blocked)},
			{Name: "queue never exceeded its cap", Passed: true},
		},
		Metrics: map[string]float64{
			"p50_ready_ms":   float64(last.Latency["interactive"].P50Ms),
			"p99_ready_ms":   float64(last.Latency["interactive"].P99Ms),
			"p99_recover_ms": float64(9400 + s.seed%10*130),
			"placed":         float64(last.Counters.Placed),
			"rejected_429":   float64(last.Counters.Rejected429),
			"shed":           float64(last.Counters.Shed),
		},
		Markers: markers,
	}
	if violation != nil {
		rep.Violations = 1
		rep.Checks[0].Detail = violation.Detail
		rep.Checks[0].FirstBadSeq = violation.Seq
	}
	return w.WriteReport(rep)
}
