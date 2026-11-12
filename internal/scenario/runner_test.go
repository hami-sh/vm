package scenario

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

func TestEventuallyUsesFinalState(t *testing.T) {
	for _, test := range []struct {
		name   string
		middle api.VMState
		final  api.VMState
		passed bool
	}{
		{"regresses after success", api.VMReady, api.VMLost, false},
		{"recovers only in final frame", api.VMLost, api.VMReady, true},
		{"recovers by deadline and ends ready", api.VMReady, api.VMReady, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeFleet{initial: snapshot(0, api.VMReady), frames: []api.Snapshot{
				snapshot(2000, api.VMLost), snapshot(3000, test.middle), snapshot(4000, test.final),
			}}
			s := Scenario{Name: "eventually", Topology: Topology{1, 1, 2}, Duration: 4 * time.Second,
				Timeline: []Step{{time.Second, func(context.Context, System) error { return nil }}},
				Expect:   []Check{Eventually(3*time.Second, "recovered", AllDesiredVMsReady)}}
			r, err := Run(context.Background(), s, func(context.Context, Scenario, int64, float64) (System, func(), error) {
				return f, func() {}, nil
			}, 1, 5, t.TempDir())
			if err != nil || r.Passed != test.passed {
				t.Fatalf("passed=%t want=%t checks=%+v err=%v", r.Passed, test.passed, r.Checks, err)
			}
		})
	}
}

func TestNoStaleCopiesRequiresRawEvidence(t *testing.T) {
	for _, test := range []struct {
		name, observed string
		passed         bool
	}{
		{"missing evidence", `[]`, false},
		{"wrong observed host", `[{"host_id":"other","connected":true,"actual":[]}]`, false},
		{"raw stopped despite stale cache", `[{"host_id":"h","connected":true,"actual":[{"vm_id":"v","gen":1,"running":false}]}]`, true},
		{"current copy", `[{"host_id":"h","connected":true,"actual":[{"vm_id":"v","gen":3,"running":true}]}]`, true},
		{"stale while connected", `[{"host_id":"h","connected":true,"actual":[{"vm_id":"v","gen":1,"running":true}]}]`, false},
		{"stale while disconnected", `[{"host_id":"h","connected":false,"actual":[{"vm_id":"v","gen":1,"running":true}]}]`, false},
		{"unknown copy", `[{"host_id":"h","connected":true,"actual":[{"vm_id":"unknown","gen":3,"running":true}]}]`, false},
		{"same generation on wrong host", `[{"host_id":"h","connected":true,"actual":[]},{"host_id":"other","connected":true,"actual":[{"vm_id":"v","gen":3,"running":true}]}]`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The cache deliberately contradicts the raw stopped observation.
			cache := "null"
			if test.name == "raw stopped despite stale cache" {
				cache = `[{"vm_id":"v","gen":1}]`
			}
			data := fmt.Sprintf(`{"hosts":[{"id":"h","lease":"healthy","stale":%s}],"vms":[{"id":"v","gen":3,"host":"h","desired":"running","state":"Ready"}],"observed":%s}`, cache, test.observed)
			var snap api.Snapshot
			if err := json.Unmarshal([]byte(data), &snap); err != nil {
				t.Fatal(err)
			}
			if got := NoStaleCopies(snap); got != test.passed {
				t.Fatalf("NoStaleCopies=%t want=%t", got, test.passed)
			}
		})
	}
}

func TestRunnerRejectsMissingEventsAndTransientViolations(t *testing.T) {
	for _, fault := range []string{"", "gap", "between frames"} {
		t.Run(fault, func(t *testing.T) {
			f := &fakeFleet{initial: snapshot(0, api.VMReady), frames: []api.Snapshot{snapshot(4000, api.VMReady)},
				events: []api.Event{{Seq: 11, Kind: api.EventTransition, VMID: "victim", To: api.VMLost}, {Seq: 12, Kind: api.EventTransition, VMID: "victim", To: api.VMReady}}}
			if fault == "gap" {
				f.events[1].Seq = 13
			}
			check := Always("no resurrection", func(api.Snapshot) bool { return true })
			check.EventTest = func(e api.Event) bool { return fault != "between frames" || e.To != api.VMReady }
			s := Scenario{Name: "events", Topology: Topology{1, 1, 2}, Duration: 4 * time.Second, Expect: []Check{check}}
			r, err := Run(context.Background(), s, func(context.Context, Scenario, int64, float64) (System, func(), error) {
				return f, func() {}, nil
			}, 1, 5, t.TempDir())
			if r.Passed != (fault == "") || (fault == "gap" && (err == nil || !strings.Contains(err.Error(), "after 11 received 13"))) {
				t.Fatalf("fault=%s report=%+v err=%v", fault, r, err)
			}
		})
	}
}

func TestHostileTenantRequiresEveryVictimToStayQuarantined(t *testing.T) {
	for _, fault := range []string{"", "resurrection", "missing victim", "wrong terminal reason"} {
		t.Run(fault, func(t *testing.T) {
			s := Catalog()[12]
			newRun := s.NewRun
			s.NewRun = func() Scenario {
				run := newRun()
				run.Topology, run.Load.Count = Topology{1, 1, 2}, 0
				return run
			}
			factory := func(_ context.Context, _ Scenario, seed int64, _ float64) (System, func(), error) {
				before, middle, after := snapshot(11000, api.VMReady), snapshot(30000, api.VMFailed), snapshot(90000, api.VMFailed)
				before.VMs, middle.VMs, after.VMs = nil, nil, nil
				for i := range 3 {
					v := api.VM{ID: fmt.Sprintf("%d-victim-%d", seed, i), Generation: seed + int64(i), State: api.VMReady, Desired: api.DesiredRunning}
					before.VMs = append(before.VMs, v)
					v.State, v.LastReason = api.VMFailed, api.ReasonQuarantined
					middle.VMs, after.VMs = append(middle.VMs, v), append(after.VMs, v)
				}
				middle.Security.Blocked, after.Security.Blocked = 15, 15
				switch fault {
				case "resurrection":
					middle.VMs[1].State = api.VMReady
					middle.VMs[1].Generation++
				case "missing victim":
					after.VMs = after.VMs[:2]
				case "wrong terminal reason":
					after.VMs[2].LastReason = api.ReasonRetryBudget
				}
				initial := before
				initial.At = 0
				return &fakeFleet{initial: initial, frames: []api.Snapshot{before, middle, after}}, func() {}, nil
			}
			// Different victim IDs and generations across concurrent runs expose
			// accidental sharing between the catalog's action/check closures.
			reports, err := Suite(context.Background(), []Scenario{s}, factory, Options{Repeat: 3, Parallel: 3, Seed: 7, Speed: 5, Output: t.TempDir()}, io.Discard)
			if (err == nil) != (fault == "") {
				t.Fatalf("fault=%s reports=%+v err=%v", fault, reports, err)
			}
		})
	}
}

func TestWipeLateHostRetryIdentityContract(t *testing.T) {
	for _, fault := range []string{"", "late adopted", "key moves after heal", "reachable identity lost", "missing Ready"} {
		t.Run(fault, func(t *testing.T) {
			s := Catalog()[len(Catalog())-1]
			newRun := s.NewRun
			s.NewRun = func() Scenario {
				run := newRun()
				run.Topology = Topology{1, 1, 2}
				return run
			}
			initial := snapshot(0, api.VMReady)
			initial.Epoch, initial.VMs = 1, nil
			var retried []api.VM
			for i := range 12 {
				v := api.VM{ID: fmt.Sprintf("original-%d", i), State: api.VMReady, Desired: api.DesiredRunning, Generation: 1, Rack: "r1"}
				if i%3 == 1 {
					v.Rack = "r2"
				}
				initial.VMs = append(initial.VMs, v)
				if v.Rack == "r2" {
					v.ID = fmt.Sprintf("retry-%d", i)
				}
				retried = append(retried, v)
			}
			f := &fakeFleet{initial: initial}
			for _, at := range []int64{10000, 12000, 30000, 41000, 60000, 80000, 90000} {
				frame := initial
				frame.At, frame.Seq = at, at/100+1
				if at >= 30000 {
					frame.Epoch, frame.VMs = 2, append([]api.VM(nil), retried...)
				}
				if fault == "late adopted" && at == 41000 {
					frame.VMs = append(frame.VMs, initial.VMs[1])
				}
				if fault == "missing Ready" && at == 90000 {
					frame.VMs = frame.VMs[:11]
				}
				f.frames = append(f.frames, frame)
			}
			calls := map[string]int{}
			f.create = func(key string) (api.CreateVMResponse, error) {
				var i int
				if _, err := fmt.Sscanf(key, "wipe-key-%d", &i); err != nil {
					return api.CreateVMResponse{}, err
				}
				calls[key]++
				id := initial.VMs[i].ID
				if calls[key] > 1 {
					id = retried[i].ID
					if fault == "reachable identity lost" && i == 0 {
						id = "wrong-reachable-ID"
					}
					if fault == "key moves after heal" && i == 1 && calls[key] > 2 {
						id = initial.VMs[i].ID
					}
				}
				return api.CreateVMResponse{ID: id}, nil
			}
			f.get = func(id string) (api.VM, error) {
				for _, v := range initial.VMs {
					if v.ID == id {
						return v, nil
					}
				}
				return api.VM{}, fmt.Errorf("unknown %s", id)
			}
			r, err := Run(context.Background(), s, func(context.Context, Scenario, int64, float64) (System, func(), error) { return f, func() {}, nil }, 1, 5, t.TempDir())
			if r.Passed != (fault == "") {
				t.Fatalf("fault=%s report=%+v err=%v", fault, r, err)
			}
		})
	}
}

func TestEveryVictimMustRecoverAndMaintenanceCannotDropVMs(t *testing.T) {
	for _, index := range []int{4, 11} {
		for _, fault := range []string{"", "only one recovers", "victim disappears", "terminal then revived"} {
			t.Run(fmt.Sprintf("%d/%s", index, fault), func(t *testing.T) {
				s := Catalog()[index]
				newRun := s.NewRun
				s.NewRun = func() Scenario {
					run := newRun()
					run.Topology, run.Load.Count = Topology{1, 1, 2}, 0
					return run
				}
				before := snapshot(0, api.VMReady)
				before.VMs = []api.VM{
					{ID: "a", Desired: api.DesiredRunning, Rack: "r2", Generation: 7, State: api.VMReady},
					{ID: "b", Desired: api.DesiredRunning, Rack: "r2", Generation: 3, State: api.VMReady},
				}
				start, middle, final := before, before, before
				start.At, middle.At, final.At = 10000, 74000, 92000
				middle.VMs, final.VMs = append([]api.VM(nil), before.VMs...), append([]api.VM(nil), before.VMs...)
				for i := range final.VMs {
					middle.VMs[i].Generation++
					final.VMs[i].Generation++
				}
				switch fault {
				case "only one recovers":
					final.VMs[1].Generation = 3
				case "victim disappears":
					final.VMs = final.VMs[:1]
				case "terminal then revived":
					middle.VMs[1].State = api.VMFailed
				}
				f := &fakeFleet{initial: before, frames: []api.Snapshot{start, middle, final}}
				r, err := Run(context.Background(), s, func(context.Context, Scenario, int64, float64) (System, func(), error) { return f, func() {}, nil }, 1, 5, t.TempDir())
				want := fault == "" || (fault == "terminal then revived" && index == 4)
				if err != nil || r.Passed != want {
					t.Fatalf("passed=%t want=%t checks=%+v err=%v", r.Passed, want, r.Checks, err)
				}
			})
		}
	}
}

func TestHerdDoesNotPreemptButPrioritizesEveryFreedSlot(t *testing.T) {
	for _, fault := range []string{"", "new placement while waiting", "victim shed", "wrong queued class", "missing final victim"} {
		t.Run(fault, func(t *testing.T) {
			run := Catalog()[10].NewRun()
			run.Topology, run.Load.Count = Topology{1, 1, 2}, 0
			initial := snapshot(0, api.VMReady)
			initial.VMs = []api.VM{{ID: "victim", State: api.VMReady, Desired: api.DesiredRunning, Generation: 7, Rack: "r2"}}
			initial.Counters.Headroom, initial.Counters.Shed = -8, 1
			final := initial
			final.At, final.VMs = 90000, append([]api.VM(nil), initial.VMs...)
			final.VMs[0].Generation = 8
			f := &fakeFleet{initial: initial, frames: []api.Snapshot{final}}
			// Materialized actions and expectations share the victim cohort.
			if err := run.Timeline[0].Do(context.Background(), f); err != nil {
				t.Fatal(err)
			}
			f.events = []api.Event{
				{Kind: api.EventTransition, At: 12000, VMID: "new-before-detection", To: api.VMBooting},
				{Kind: api.EventTransition, At: 15000, VMID: "victim", To: api.VMLost},
				{Kind: api.EventTransition, At: 15000, VMID: "victim", To: api.VMPending},
			}
			switch fault {
			case "new placement while waiting":
				f.events = append(f.events, api.Event{Kind: api.EventTransition, At: 25000, VMID: "new-before-power-returns", To: api.VMBooting})
			case "victim shed":
				f.events = append(f.events, api.Event{Kind: api.EventTransition, At: 25000, VMID: "victim", To: api.VMFailed, Detail: api.ReasonShed})
			case "wrong queued class":
				pending := initial
				pending.At, pending.VMs = 30000, append([]api.VM(nil), initial.VMs...)
				pending.VMs[0].State = api.VMPending
				pending.Queue = []api.QueueEntry{{VMID: "victim", Reason: api.QueueReplace, Priority: api.PriorityInteractive, EffectivePriority: api.PriorityInteractive}}
				f.frames = append([]api.Snapshot{pending}, f.frames...)
			case "missing final victim":
				f.frames[0].VMs = nil
			}
			f.events = append(f.events,
				api.Event{Kind: api.EventTransition, At: 45000, VMID: "victim", To: api.VMBooting},
				api.Event{Kind: api.EventTransition, At: 46000, VMID: "new-after-replacement", To: api.VMBooting})
			r, err := Run(context.Background(), run, func(context.Context, Scenario, int64, float64) (System, func(), error) { return f, func() {}, nil }, 1, 5, t.TempDir())
			if err != nil || r.Passed != (fault == "") {
				t.Fatalf("passed=%t checks=%+v err=%v", r.Passed, r.Checks, err)
			}
		})
	}
}

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
		seq := int64(0)
		encode := func(at int64) {
			frame := snapshot(at, api.VMReady)
			seq++
			frame.Recent = []api.Event{{Seq: seq, At: at, Kind: api.EventTransition}}
			if err := json.NewEncoder(&frames).Encode(frame); err != nil {
				t.Fatal(err)
			}
		}
		for at := int64(0); at <= 10300; at += 100 {
			encode(at)
		}
		encode(10345)
		for at := int64(11000); at <= 20000; at += 300 {
			encode(at) // Irregular publication cadence must not change sim-time thinning.
		}
		encode(20045)
		if err := os.WriteFile(filepath.Join(dir, "snapshots.jsonl"), frames.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("a-seed2", "guestd-dies", 2)
	write("z-seed1", "guestd-dies", 1)
	if _, _, err := Bundle(input, output); err == nil {
		t.Fatal("accepted an incomplete catalog")
	}
	for _, s := range Catalog()[1:] {
		write(s.Name, s.Name, 4)
	}
	original, _ := os.ReadFile(filepath.Join(input, "z-seed1", "snapshots.jsonl"))
	count, size, err := Bundle(input, output)
	if err != nil || count != len(Catalog()) || size <= 0 || size >= 100_000_000 {
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
	lastEvent := int64(0)
	d := json.NewDecoder(bytes.NewReader(b))
	for {
		var frame api.Snapshot
		if err := d.Decode(&frame); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		kept[frame.At] = true
		for _, e := range frame.Recent {
			if e.Seq != lastEvent+1 {
				t.Fatalf("playback lost event %d: next=%d", lastEvent+1, e.Seq)
			}
			lastEvent = e.Seq
		}
	}
	if lastEvent != 137 {
		t.Fatalf("playback retained %d events, want all 137", lastEvent)
	}
	for _, at := range []int64{0, 1500, 3500, 5500, 6100, 8100, 10100, 12200, 14300, 16400, 18500, 20045} {
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
	create  func(string) (api.CreateVMResponse, error)
	get     func(string) (api.VM, error)
}

func (f *fakeFleet) Latest() api.Snapshot { return f.initial }
func (f *fakeFleet) Do(context.Context, string, api.ChaosRequest) (api.ChaosResult, error) {
	return api.ChaosResult{}, nil
}
func (f *fakeFleet) Topology(context.Context, string, api.TopologyRequest) (api.ChaosResult, error) {
	return api.ChaosResult{}, nil
}
func (f *fakeFleet) CreateVM(_ context.Context, key string, _ api.CreateVMRequest) (api.CreateVMResponse, error) {
	if f.create != nil {
		return f.create(key)
	}
	return api.CreateVMResponse{ID: "vm-1"}, nil
}
func (f *fakeFleet) GetVM(_ context.Context, id string) (api.VM, error) { return f.get(id) }
func (f *fakeFleet) Subscribe() (<-chan api.Snapshot, <-chan api.Event, func()) {
	s := make(chan api.Snapshot, len(f.frames))
	for _, v := range f.frames {
		s <- v
	}
	close(s)
	events := make(chan api.Event, len(f.events))
	for i, ev := range f.events {
		if ev.Seq == 0 {
			ev.Seq = int64(i + 1)
		}
		events <- ev
	}
	close(events)
	return s, events, func() {}
}
func snapshot(at int64, state api.VMState) api.Snapshot {
	s := api.Snapshot{Seq: at/1000 + 1, At: at, Scheduler: api.SchedulerUp, Racks: []api.Rack{{ID: "r1"}}, Hosts: []api.Host{{ID: "r1-h1", SlotCount: 2}}, VMs: []api.VM{{ID: "vm-1", Desired: api.DesiredRunning, State: state}}, Counters: api.Counters{QueueCap: 32}}
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
	for _, name := range api.AllInvariants {
		if name == api.InvStaleStopped || name == api.InvPendingPlaced {
			continue
		}
		t.Run(string(name), func(t *testing.T) {
			safetyFault := func(_ context.Context, _ Scenario, _ int64, speed float64) (System, func(), error) {
				snap := snapshot(1000, api.VMReady)
				if speed == 50 {
					for i := range snap.Invariants {
						if snap.Invariants[i].Name == name {
							snap.Invariants[i].Violations = 1
						}
					}
				}
				return &fakeFleet{initial: snapshot(0, api.VMReady), frames: []api.Snapshot{snap}}, func() {}, nil
			}
			if _, err := Calibrate(context.Background(), []Scenario{s}, safetyFault, Options{Repeat: 1, Parallel: 1, Output: t.TempDir()}, io.Discard); err == nil || !strings.Contains(err.Error(), "cannot cap safety") {
				t.Fatalf("suppressed %s safety failure: %v", name, err)
			}
		})
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
	baselineFailure := func(_ context.Context, _ Scenario, _ int64, speed float64) (System, func(), error) {
		snap := snapshot(1000, api.VMReady)
		if speed == 5 {
			for i := range snap.Invariants {
				if snap.Invariants[i].Name == api.InvPendingPlaced {
					snap.Invariants[i].Violations = 1
				}
			}
		}
		return &fakeFleet{initial: snapshot(0, api.VMReady), frames: []api.Snapshot{snap}}, func() {}, nil
	}
	if _, err := Calibrate(context.Background(), []Scenario{s}, baselineFailure, Options{Repeat: 1, Parallel: 1, Output: t.TempDir()}, io.Discard); err == nil {
		t.Fatal("discarded a failed 5x baseline because 50x passed")
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
