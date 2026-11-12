package scenario

import (
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
)

// Embedding the contracts makes accidental use of an unimplemented endpoint
// fail immediately; the runner only needs create and the snapshot stream here.
type fakeFleet struct {
	System
	initial api.Snapshot
	frames  []api.Snapshot
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
	return s, make(chan api.Event), func() {}
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
				return &fakeFleet{initial: snapshot(0, api.VMReady), frames: frames}, func() {}, nil
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
			snap.Invariants[0].Violations = 1
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
