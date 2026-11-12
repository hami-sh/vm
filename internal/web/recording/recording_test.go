package recording

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hami-sh/vm/internal/api"
)

func readLines[T any](t *testing.T, path string) []T {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []T
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var v T
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		out = append(out, v)
	}
	return out
}

func TestWriterRoundTrip(t *testing.T) {
	root := t.TempDir()
	w, err := Create(filepath.Join(root, "run-a"))
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 3; i++ {
		if err := w.Snapshot(api.Snapshot{Seq: i, At: i * 100, Scheduler: api.SchedulerUp}); err != nil {
			t.Fatal(err)
		}
	}
	// The event stream and a snapshot's Recent list overlap; each event is
	// written once.
	for _, seq := range []int64{1, 2, 2, 1, 3} {
		if err := w.Event(api.Event{Seq: seq, Kind: api.EventChaos}); err != nil {
			t.Fatal(err)
		}
	}
	// A run has no report until it finishes, and unfinished runs aren't listed.
	if runs, _ := List(root); len(runs) != 0 {
		t.Errorf("unfinished run listed: %+v", runs)
	}
	if _, err := Report(root, "run-a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Report before WriteReport: err = %v, want ErrNotFound", err)
	}
	if err := w.WriteReport(api.RunReport{RunSummary: api.RunSummary{Scenario: "split-brain", Seed: 9, Passed: true}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Snapshot(api.Snapshot{}); err == nil {
		t.Error("Snapshot after Close: want an error")
	}

	snaps := readLines[api.Snapshot](t, filepath.Join(root, "run-a", SnapshotsFile))
	if len(snaps) != 3 || snaps[0].Seq != 1 || snaps[2].At != 300 {
		t.Errorf("snapshots.jsonl = %+v", snaps)
	}
	events := readLines[api.Event](t, filepath.Join(root, "run-a", EventsFile))
	if len(events) != 3 || events[0].Seq != 1 || events[1].Seq != 2 || events[2].Seq != 3 {
		t.Errorf("events.jsonl = %+v, want seq 1, 2, 3", events)
	}
	rep, err := Report(root, "run-a")
	if err != nil || rep.ID != "run-a" || rep.Scenario != "split-brain" || rep.Seed != 9 || !rep.Passed {
		t.Errorf("Report = %+v, %v (ID defaults to the directory name)", rep, err)
	}
	f, err := OpenSnapshots(root, "run-a")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestListNewestFirstAndSkipsBrokenRuns(t *testing.T) {
	root := t.TempDir()
	for id, started := range map[string]string{"a": "2026-11-12T01:00:00Z", "b": "2026-11-12T03:00:00Z", "c": "2026-11-12T02:00:00Z"} {
		dir := filepath.Join(root, id)
		os.MkdirAll(dir, 0o755)
		if err := WriteReport(dir, api.RunReport{RunSummary: api.RunSummary{ID: id, StartedAt: started}}); err != nil {
			t.Fatal(err)
		}
	}
	os.MkdirAll(filepath.Join(root, "half-written"), 0o755)
	os.WriteFile(filepath.Join(root, "half-written", ReportFile), []byte(`{"id":`), 0o644)
	os.WriteFile(filepath.Join(root, "stray-file"), []byte("x"), 0o644)

	runs, err := List(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 3 || runs[0].ID != "b" || runs[1].ID != "c" || runs[2].ID != "a" {
		t.Errorf("List = %+v, want b, c, a", runs)
	}
	if runs, err := List(filepath.Join(root, "missing")); err != nil || runs == nil || len(runs) != 0 {
		t.Errorf("List of a missing directory = %v, %v; want an empty list", runs, err)
	}
}

// A run ID from a URL must never reach outside the runs directory.
func TestRunIDCannotEscapeRoot(t *testing.T) {
	root := t.TempDir()
	runs := filepath.Join(root, "runs")
	os.MkdirAll(filepath.Join(root, "secret"), 0o755)
	if err := WriteReport(filepath.Join(root, "secret"), api.RunReport{}); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(root, "secret", SnapshotsFile), []byte("{}\n"), 0o644)
	os.MkdirAll(runs, 0o755)
	for _, id := range []string{"../secret", "..", ".", "", "a/b", `..\secret`, "/etc"} {
		if _, err := Report(runs, id); !errors.Is(err, ErrNotFound) {
			t.Errorf("Report(%q): err = %v, want ErrNotFound", id, err)
		}
		if f, err := OpenSnapshots(runs, id); !errors.Is(err, ErrNotFound) {
			t.Errorf("OpenSnapshots(%q): err = %v, want ErrNotFound", id, err)
			if f != nil {
				f.Close()
			}
		}
	}
}

type chanSource struct {
	snaps  chan api.Snapshot
	events chan api.Event
}

func (c chanSource) Latest() api.Snapshot { return api.Snapshot{} }
func (c chanSource) Subscribe() (<-chan api.Snapshot, <-chan api.Event, func()) {
	return c.snaps, c.events, func() {}
}

// Follow records the events a snapshot carries in Recent as well, because a
// subscription drops events when its reader is slow.
func TestFollowRecordsRecentEventsOnce(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	w, err := Create(dir)
	if err != nil {
		t.Fatal(err)
	}
	src := chanSource{snaps: make(chan api.Snapshot), events: make(chan api.Event)}
	done := make(chan error, 1)
	go func() { done <- w.Follow(context.Background(), src) }()

	src.events <- api.Event{Seq: 1}
	src.snaps <- api.Snapshot{Seq: 1, Recent: []api.Event{{Seq: 1}, {Seq: 2}}} // 2 was dropped from the channel
	src.events <- api.Event{Seq: 3}
	src.snaps <- api.Snapshot{Seq: 2, Recent: []api.Event{{Seq: 3}}}
	close(src.snaps) // the fleet closed

	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	events := readLines[api.Event](t, filepath.Join(dir, EventsFile))
	if len(events) != 3 || events[0].Seq != 1 || events[1].Seq != 2 || events[2].Seq != 3 {
		t.Errorf("events.jsonl = %+v, want seq 1, 2, 3", events)
	}
	if snaps := readLines[api.Snapshot](t, filepath.Join(dir, SnapshotsFile)); len(snaps) != 2 {
		t.Errorf("snapshots.jsonl has %d lines, want 2", len(snaps))
	}
}
