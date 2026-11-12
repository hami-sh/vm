package scenario

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
)

func TestReadinessTargetIsReportedNotGated(t *testing.T) {
	target := config.Default().TimeToReadyTarget.Milliseconds()
	for _, test := range []struct {
		latency int64
		met     float64
	}{{target, 1}, {target + 1, 0}, {0, 0}} {
		frame := snapshot(target+10000, api.VMReady)
		frame.VMs[0].CreatedAt = 1000
		frame.Sim.Intensity = .5
		f := &fakeFleet{initial: snapshot(0, api.VMReady), frames: []api.Snapshot{frame}}
		if test.latency > 0 {
			f.events = []api.Event{
				{Kind: api.EventTransition, VMID: "unrelated", At: 1001, To: api.VMReady},
				{Kind: api.EventTransition, VMID: "vm-1", At: 1000 + test.latency, To: api.VMReady},
				{Kind: api.EventTransition, VMID: "vm-1", At: frame.At - 1, To: api.VMReady},
			}
		}
		s := Scenario{Name: "target", Topology: Topology{1, 1, 2}, Load: Load{1, "interactive"}, Duration: time.Duration(frame.At) * time.Millisecond, Expect: []Check{Always("invariants", NoInvariantViolations)}}
		r, err := Run(context.Background(), s, func(context.Context, Scenario, int64, float64) (System, func(), error) { return f, func() {}, nil }, 1, 5, t.TempDir())
		if err != nil || !r.Passed || r.Metrics["time_to_ready_target_met"] != test.met || r.Metrics["time_to_ready_requested"] != 1 || r.Metrics["offered_load_fraction"] != .5 {
			t.Fatalf("readiness target became a gate or lost load context: %+v %v", r, err)
		}
		if test.latency > 0 && (r.Metrics["p99_time_to_ready_ms"] != float64(test.latency) || r.Metrics["time_to_ready_samples"] != 1) {
			t.Fatalf("must measure first Ready for the requested cohort only: %+v", r.Metrics)
		}
		if test.latency == 0 {
			if _, ok := r.Metrics["p99_time_to_ready_ms"]; ok || r.Metrics["time_to_ready_samples"] != 0 {
				t.Fatalf("missing Ready event became a zero-latency success: %+v", r.Metrics)
			}
		}
	}
}

func TestBundleSelectsAndThinsWithoutLosingMarkers(t *testing.T) {
	input, output := filepath.Join(t.TempDir(), "batch"), filepath.Join(t.TempDir(), "latest")
	write := func(name, scenario string, seed int64) {
		dir := filepath.Join(input, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		r := api.RunReport{RunSummary: api.RunSummary{ID: name, Scenario: scenario, Seed: seed, Passed: true}, Markers: []api.Event{{Kind: api.EventChaos, At: 3500}, {Kind: api.EventInvariant, At: 8100}}}
		b, _ := json.Marshal(r)
		for file, data := range map[string][]byte{"report.json": b, "metrics.json": []byte(`{}`), "events.jsonl": []byte("{}\n")} {
			if err := os.WriteFile(filepath.Join(dir, file), data, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		var frames bytes.Buffer
		for at := int64(0); at <= 10300; at += 100 {
			if err := json.NewEncoder(&frames).Encode(snapshot(at, api.VMReady)); err != nil {
				t.Fatal(err)
			}
		}
		if err := json.NewEncoder(&frames).Encode(snapshot(10345, api.VMReady)); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "snapshots.jsonl"), frames.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("a-seed2", "alpha", 2)
	write("z-seed1", "alpha", 1)
	write("beta-seed4", "beta", 4)
	original, _ := os.ReadFile(filepath.Join(input, "z-seed1", "snapshots.jsonl"))
	count, size, err := Bundle(input, output)
	if err != nil || count != 2 || size <= 0 || size >= 100_000_000 {
		t.Fatalf("bundle: count=%d bytes=%d err=%v", count, size, err)
	}
	if _, err := os.Stat(filepath.Join(output, "a-seed2")); !os.IsNotExist(err) {
		t.Fatal("bundled more than one run per scenario")
	}
	b, err := os.ReadFile(filepath.Join(output, "z-seed1", "snapshots.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	kept := map[int64]bool{}
	d := json.NewDecoder(bytes.NewReader(b))
	for {
		var frame api.Snapshot
		if err := d.Decode(&frame); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		kept[frame.At] = true
	}
	for _, at := range []int64{0, 1500, 3500, 5500, 6100, 8100, 10100, 10345} {
		if !kept[at] {
			t.Fatalf("lost endpoint/marker-window frame at %d", at)
		}
	}
	for _, at := range []int64{1000, 1400, 5600, 5900, 10300} {
		if kept[at] {
			t.Fatalf("did not thin quiet frame at %d", at)
		}
	}
	after, _ := os.ReadFile(filepath.Join(input, "z-seed1", "snapshots.jsonl"))
	if !bytes.Equal(original, after) {
		t.Fatal("modified full evidence batch")
	}
	if _, _, err := Bundle(input, input); err == nil {
		t.Fatal("allowed replacing the full batch")
	}
	if err := os.WriteFile(filepath.Join(input, "a-seed2", "report.json"), []byte(`{"passed":false}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Bundle(input, output); err == nil {
		t.Fatal("published a failed batch")
	}
	after, _ = os.ReadFile(filepath.Join(output, "z-seed1", "snapshots.jsonl"))
	if !bytes.Equal(b, after) {
		t.Fatal("failed bundle replaced last green evidence")
	}
}

// Embedding the contracts makes accidental use of an unimplemented endpoint
// fail immediately; the runner only needs create and the snapshot stream here.
type fakeFleet struct {
	System
	initial api.Snapshot
	frames  []api.Snapshot
	events  []api.Event
}

func (f *fakeFleet) Latest() api.Snapshot { return f.initial }
func (f *fakeFleet) CreateVM(context.Context, string, api.CreateVMRequest) (api.CreateVMResponse, error) {
	return api.CreateVMResponse{ID: "vm-1"}, nil
}
func (f *fakeFleet) Subscribe() (<-chan api.Snapshot, <-chan api.Event, func()) {
	s := make(chan api.Snapshot, len(f.frames))
	for _, v := range f.frames {
		s <- v
	}
	close(s)
	events := make(chan api.Event, len(f.events))
	for _, ev := range f.events {
		events <- ev
	}
	close(events)
	return s, events, func() {}
}
func snapshot(at int64, state api.VMState) api.Snapshot {
	s := api.Snapshot{Seq: at/1000 + 1, At: at, Racks: []api.Rack{{ID: "r1"}}, Hosts: []api.Host{{ID: "r1-h1", SlotCount: 2}}, VMs: []api.VM{{ID: "vm-1", Desired: api.DesiredRunning, State: state}}, Counters: api.Counters{QueueCap: 32}}
	for _, n := range api.AllInvariants {
		s.Invariants = append(s.Invariants, api.InvariantStatus{Name: n})
	}
	return s
}

func TestRunnerRecordsFirstViolationAndPostFaultRecovery(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(map[bool]string{false: "recover", true: "regression"}[broken], func(t *testing.T) {
			frames := []api.Snapshot{snapshot(1000, api.VMReady), snapshot(2000, api.VMLost), snapshot(3000, api.VMLost), snapshot(4000, api.VMReady)}
			if broken {
				frames[2].Invariants[0].Violations = 1
				frames[3].VMs[0].State = api.VMLost
			}
			s := Scenario{Name: "fault", Topology: Topology{1, 1, 2}, Duration: 4 * time.Second, Timeline: []Step{{2 * time.Second, func(context.Context, System) error { return nil }}}, Expect: []Check{Always("safe", NoInvariantViolations), Eventually(4*time.Second, "recovered", AllDesiredVMsReady), Metric("p99_recover_ms", 2500)}}
			factory := func(context.Context, Scenario, int64, float64) (System, func(), error) {
				f := &fakeFleet{initial: snapshot(0, api.VMReady), frames: frames}
				f.events = []api.Event{{Kind: api.EventTransition, VMID: "vm-1", At: 2000, To: api.VMLost}}
				if !broken {
					f.events = append(f.events, api.Event{Kind: api.EventTransition, VMID: "vm-1", At: 4000, To: api.VMReady})
				}
				return FromFleet(f, f, f), func() {}, nil
			}
			dir := t.TempDir()
			r, err := Run(context.Background(), s, factory, 17, 50, dir)
			if err != nil {
				t.Fatal(err)
			}
			if r.Passed == broken {
				t.Fatalf("passed=%t checks=%+v", r.Passed, r.Checks)
			}
			if broken && (r.Checks[0].FirstBadSeq != 4 || r.Checks[1].Passed || r.Checks[2].Passed) {
				t.Fatalf("missed regression: %+v", r.Checks)
			}
			if !broken && r.P99RecoverMs != 2000 {
				t.Fatalf("recovery=%d", r.P99RecoverMs)
			}
			for _, file := range []string{"events.jsonl", "snapshots.jsonl", "metrics.json", "report.json"} {
				b, e := os.ReadFile(filepath.Join(dir, file))
				if e != nil {
					t.Fatal(e)
				}
				if file == "report.json" {
					var saved api.RunReport
					if e := json.Unmarshal(b, &saved); e != nil || saved.Passed != r.Passed {
						t.Fatalf("saved report: %s %v", b, e)
					}
				}
			}
		})
	}
}

func TestSuiteSeedsAndCalibration(t *testing.T) {
	var mu sync.Mutex
	seeds := map[int64]int{}
	s := Scenario{Name: "calibration", Topology: Topology{1, 1, 2}, Duration: time.Second, Expect: []Check{Always("invariants", NoInvariantViolations)}}
	f := func(_ context.Context, _ Scenario, seed int64, speed float64) (System, func(), error) {
		mu.Lock()
		seeds[seed]++
		mu.Unlock()
		snap := snapshot(1000, api.VMReady)
		if speed == 50 && seed == 12 {
			for i := range snap.Invariants {
				if snap.Invariants[i].Name == api.InvPendingPlaced {
					snap.Invariants[i].Violations = 1
				}
			}
		}
		return &fakeFleet{initial: snapshot(0, api.VMReady), frames: []api.Snapshot{snap}}, func() {}, nil
	}
	speed, err := Calibrate(context.Background(), []Scenario{s}, f, Options{Repeat: 3, Parallel: 2, Seed: 11, Output: t.TempDir()}, io.Discard)
	if err != nil || speed != 5 {
		t.Fatalf("speed %g err %v", speed, err)
	}
	for _, seed := range []int64{11, 12, 13} {
		if seeds[seed] != 2 {
			t.Fatalf("seed counts: %v", seeds)
		}
	}
	_, err = Suite(context.Background(), []Scenario{s}, f, Options{Repeat: 3, Parallel: 2, Seed: 11, Speed: 50, Output: t.TempDir()}, io.Discard)
	if err == nil {
		t.Fatal("partial failure must fail suite")
	}
	safetyFault := func(_ context.Context, _ Scenario, _ int64, speed float64) (System, func(), error) {
		snap := snapshot(1000, api.VMReady)
		if speed == 50 {
			snap.Invariants[0].Violations = 1
		}
		return &fakeFleet{initial: snapshot(0, api.VMReady), frames: []api.Snapshot{snap}}, func() {}, nil
	}
	_, err = Calibrate(context.Background(), []Scenario{s}, safetyFault, Options{Repeat: 1, Parallel: 1, Output: t.TempDir()}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "cannot cap safety") {
		t.Fatalf("suppressed high-speed fencing failure: %v", err)
	}
	boundedFault := func(_ context.Context, _ Scenario, _ int64, speed float64) (System, func(), error) {
		snap := snapshot(1000, api.VMReady)
		if speed == 50 {
			snap.Invariants[1].Violations = 1
		}
		return &fakeFleet{initial: snapshot(0, api.VMReady), frames: []api.Snapshot{snap}}, func() {}, nil
	}
	speed, err = Calibrate(context.Background(), []Scenario{s}, boundedFault, Options{Repeat: 1, Parallel: 1, Output: t.TempDir()}, io.Discard)
	if err != nil || speed != 5 {
		t.Fatalf("time-bounded invariant 2 should cap speed: %g %v", speed, err)
	}
}

func TestSSEContract(t *testing.T) {
	b, err := os.ReadFile("../../fixtures/snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	var snap api.Snapshot
	if err := DecodeSnapshot(b, &snap); err != nil {
		t.Fatal(err)
	}
	if err := DecodeSnapshot([]byte(`{"seq":1,"at":2}`), &snap); err == nil {
		t.Fatal("accepted Event as Snapshot")
	}
	stream := ": keepalive\r\nevent: snapshot\r\ndata: {\r\ndata: \"seq\": 7}\r\n\r\n"
	calls := 0
	err = ReadSSE(strings.NewReader(stream), func(kind string, b []byte) error {
		calls++
		if kind != "snapshot" || string(b) != "{\n\"seq\": 7}" {
			t.Fatalf("%s %s", kind, b)
		}
		return nil
	})
	if err != nil || calls != 1 {
		t.Fatalf("calls %d err %v", calls, err)
	}
}

func TestPoweredOffHostReplacementContract(t *testing.T) {
	for _, fault := range []string{"", "wrong class", "batch ahead", "one victim missing", "skipped generation", "same host", "unrelated recovery"} {
		t.Run(fault, func(t *testing.T) {
			s := Catalog()[2]
			s.Topology = Topology{1, 1, 2}
			s.Load.Count = 0
			// Exercise the catalog's checks through the runner, with the fault
			// already modeled in snapshots rather than invoking a fake driver.
			s.Timeline = []Step{{30 * time.Second, func(context.Context, System) error { return nil }}}
			f := func(_ context.Context, _ Scenario, seed int64, _ float64) (System, func(), error) {
				before := snapshot(1000, api.VMReady)
				before.VMs = []api.VM{
					{ID: "a", HostID: "r1-h1", Generation: seed + 7, State: api.VMReady},
					{ID: "b", HostID: "r1-h1", Generation: seed + 3, State: api.VMReady},
					{ID: "other", HostID: "r2-h1", Generation: 1, State: api.VMReady},
				}
				queued := snapshot(24000, api.VMLost)
				queued.Queue = []api.QueueEntry{
					{VMID: "a", Priority: api.PriorityReplacement, EffectivePriority: api.PriorityReplacement, Reason: api.QueueReplace},
					{VMID: "b", Priority: api.PriorityReplacement, EffectivePriority: api.PriorityReplacement, Reason: api.QueueReplace},
					{VMID: "batch", Priority: api.PriorityBatch, EffectivePriority: api.PriorityBatch, Reason: api.QueueNew},
				}
				after := snapshot(90000, api.VMReady)
				after.VMs = []api.VM{
					{ID: "a", HostID: "r4-h1", Generation: seed + 8, State: api.VMReady},
					{ID: "b", HostID: "r4-h2", Generation: seed + 4, State: api.VMReady},
					{ID: "other", HostID: "r2-h1", Generation: 2, State: api.VMReady},
				}
				switch fault {
				case "wrong class":
					queued.Queue[0].Priority = api.PriorityStandard
				case "batch ahead":
					queued.Queue[0], queued.Queue[2] = queued.Queue[2], queued.Queue[0]
				case "one victim missing":
					after.VMs = append(after.VMs[:1], after.VMs[2:]...)
				case "skipped generation":
					after.VMs[0].Generation++
				case "same host":
					after.VMs[0].HostID = "r1-h1"
				case "unrelated recovery":
					after.VMs = after.VMs[2:]
				}
				return &fakeFleet{initial: snapshot(0, api.VMReady), frames: []api.Snapshot{before, queued, after}}, func() {}, nil
			}
			reports, err := Suite(context.Background(), []Scenario{s}, f, Options{Repeat: 3, Parallel: 3, Seed: 1, Speed: 50, Output: t.TempDir()}, io.Discard)
			if (err == nil) != (fault == "") {
				t.Fatalf("fault %q: err=%v reports=%+v", fault, err, reports)
			}
		})
	}
}
