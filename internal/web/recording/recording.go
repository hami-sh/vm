// Package recording writes and reads run recordings in runs/<id>/:
// snapshots.jsonl and events.jsonl (one JSON value per line, in the order
// they were published) and report.json. W5's scenario runner and scenarios
// started from the dashboard write with Writer; the web server's Runs
// endpoints read with List, Report and OpenSnapshots.
package recording

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"

	"github.com/hami-sh/vm/internal/api"
)

// File names inside a run directory.
const (
	SnapshotsFile = "snapshots.jsonl"
	EventsFile    = "events.jsonl"
	ReportFile    = "report.json"
)

// Writer appends snapshots and events to one run directory. It is safe for
// concurrent use: a runner may write events and snapshots from different
// goroutines.
type Writer struct {
	dir string

	mu        sync.Mutex
	snapsF    *os.File
	eventsF   *os.File
	snaps     *bufio.Writer
	events    *bufio.Writer
	snapEnc   *json.Encoder
	eventEnc  *json.Encoder
	lastEvent int64
	err       error
}

// Create makes dir (and parents) and opens its snapshot and event logs for
// appending.
func Create(dir string) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	open := func(name string) (*os.File, error) {
		return os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	}
	sf, err := open(SnapshotsFile)
	if err != nil {
		return nil, err
	}
	ef, err := open(EventsFile)
	if err != nil {
		sf.Close()
		return nil, err
	}
	w := &Writer{dir: dir, snapsF: sf, eventsF: ef,
		snaps: bufio.NewWriterSize(sf, 256<<10), events: bufio.NewWriterSize(ef, 64<<10)}
	w.snapEnc = json.NewEncoder(w.snaps)
	w.eventEnc = json.NewEncoder(w.events)
	return w, nil
}

// Dir is the run directory.
func (w *Writer) Dir() string { return w.dir }

// Snapshot appends one snapshot line.
func (w *Writer) Snapshot(s api.Snapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		w.err = w.snapEnc.Encode(s)
	}
	return w.err
}

// Event appends one event line. Events already written (by Seq) are skipped,
// so a caller can feed both the event stream and each snapshot's Recent list
// without duplicating lines.
func (w *Writer) Event(e api.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e.Seq != 0 && e.Seq <= w.lastEvent {
		return w.err
	}
	if e.Seq > w.lastEvent {
		w.lastEvent = e.Seq
	}
	if w.err == nil {
		w.err = w.eventEnc.Encode(e)
	}
	return w.err
}

// Flush writes buffered lines to disk.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

func (w *Writer) flushLocked() error {
	if w.err != nil {
		return w.err
	}
	if err := w.snaps.Flush(); err != nil {
		w.err = err
	} else if err := w.events.Flush(); err != nil {
		w.err = err
	}
	return w.err
}

// Close flushes and closes both logs.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.flushLocked()
	err = errors.Join(err, w.snapsF.Close(), w.eventsF.Close())
	if w.err == nil {
		w.err = errors.New("recording: writer closed")
	}
	return err
}

// WriteReport writes report.json atomically (write to a temp file, then
// rename), so the Runs page never reads a half-written report.
func (w *Writer) WriteReport(r api.RunReport) error {
	if err := w.Flush(); err != nil {
		return err
	}
	return WriteReport(w.dir, r)
}

// Follow records every snapshot and event src publishes until ctx is done,
// then flushes. Run it in its own goroutine; Close the writer afterwards.
// Subscribing is lossy for slow readers, so Follow also records each
// snapshot's Recent events (Event drops duplicates by Seq).
func (w *Writer) Follow(ctx context.Context, src api.SnapshotSource) error {
	snaps, events, cancel := src.Subscribe()
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return w.Flush()
		case e, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			if err := w.Event(e); err != nil {
				return err
			}
		case s, ok := <-snaps:
			if !ok {
				return w.Flush()
			}
			for _, e := range s.Recent {
				if err := w.Event(e); err != nil {
					return err
				}
			}
			if err := w.Snapshot(s); err != nil {
				return err
			}
		}
	}
}

// WriteReport writes dir/report.json atomically.
func WriteReport(dir string, r api.RunReport) error {
	if err := DeriveMetrics(dir, &r); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".report-*.json")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(dir, ReportFile))
}

// ---------------------------------------------------------------- reading

var validID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ErrNotFound is returned for a run ID that is malformed or has no report.
var ErrNotFound = errors.New("run not found")

// runDir resolves id inside root, rejecting anything that could escape it.
func runDir(root, id string) (string, error) {
	if !validID.MatchString(id) || id == "." || id == ".." {
		return "", ErrNotFound
	}
	return filepath.Join(root, id), nil
}

// Report reads root/<id>/report.json.
func Report(root, id string) (api.RunReport, error) {
	var r api.RunReport
	dir, err := runDir(root, id)
	if err != nil {
		return r, err
	}
	b, err := os.ReadFile(filepath.Join(dir, ReportFile))
	if errors.Is(err, os.ErrNotExist) {
		return r, ErrNotFound
	}
	if err != nil {
		return r, err
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return r, fmt.Errorf("%s: %w", id, err)
	}
	if r.ID == "" {
		r.ID = id
	}
	// Recompute legacy reports too; no migration or mutation on a GET.
	err = DeriveMetrics(dir, &r)
	return r, err
}

// List returns the summary of every run under root that has a report,
// newest first. Runs still recording (no report yet) and unreadable reports
// are skipped.
func List(root string) ([]api.RunSummary, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return []api.RunSummary{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := []api.RunSummary{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		r, err := Report(root, e.Name())
		if err != nil {
			continue
		}
		out = append(out, r.RunSummary)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].StartedAt != out[j].StartedAt {
			return out[i].StartedAt > out[j].StartedAt
		}
		return out[i].ID > out[j].ID
	})
	return out, nil
}

// OpenSnapshots opens root/<id>/snapshots.jsonl.
func OpenSnapshots(root, id string) (io.ReadSeekCloser, error) {
	dir, err := runDir(root, id)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(dir, SnapshotsFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return f, err
}
