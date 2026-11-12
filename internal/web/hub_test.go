package web

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
)

// stubSource is a SnapshotSource the test feeds by hand. Its channels are
// unbuffered: a send returns only once the hub has taken the value, which
// keeps the tests free of sleeps.
type stubSource struct {
	mu     sync.Mutex
	latest api.Snapshot
	snaps  chan api.Snapshot
	events chan api.Event
}

func newStubSource(latest api.Snapshot) *stubSource {
	return &stubSource{latest: latest, snaps: make(chan api.Snapshot), events: make(chan api.Event)}
}

func (s *stubSource) Latest() api.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

func (s *stubSource) Subscribe() (<-chan api.Snapshot, <-chan api.Event, func()) {
	return s.snaps, s.events, func() {}
}

// publish makes snap the latest and hands it to the hub.
func (s *stubSource) publish(snap api.Snapshot) {
	s.mu.Lock()
	s.latest = snap
	s.mu.Unlock()
	s.snaps <- snap
}

// stream is one open GET /v1/events connection.
type stream struct {
	t      *testing.T
	frames chan string
	close  func()
}

// openStream connects and splits the body into frames at blank lines.
func openStream(t *testing.T, url string, headers ...string) *stream {
	t.Helper()
	req, err := http.NewRequest("GET", url+"/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	st := &stream{t: t, frames: make(chan string, 64), close: func() { resp.Body.Close() }}
	t.Cleanup(st.close)
	go func() {
		defer close(st.frames)
		r := bufio.NewReader(resp.Body)
		var frame strings.Builder
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			frame.WriteString(line)
			if line == "\n" {
				st.frames <- frame.String()
				frame.Reset()
			}
		}
	}()
	return st
}

// next returns the next frame exactly as it was written.
func (st *stream) next() string {
	st.t.Helper()
	select {
	case f, ok := <-st.frames:
		if !ok {
			st.t.Fatal("stream closed")
		}
		return f
	case <-clock.Real{}.After(5 * time.Second):
		st.t.Fatal("no SSE frame within 5 s")
	}
	return ""
}

// nextSnapshot reads a "snapshot" frame and checks its exact shape: an event
// line, no id, and the whole snapshot as JSON on one data line.
func (st *stream) nextSnapshot() api.Snapshot {
	st.t.Helper()
	frame := st.next()
	data, ok := strings.CutPrefix(frame, "event: snapshot\ndata: ")
	data, ok2 := strings.CutSuffix(data, "\n\n")
	if !ok || !ok2 || strings.Contains(data, "\n") {
		st.t.Fatalf("snapshot frame = %q", frame)
	}
	var snap api.Snapshot
	if err := json.Unmarshal([]byte(data), &snap); err != nil {
		st.t.Fatalf("snapshot data: %v", err)
	}
	return snap
}

func newSSEServer(t *testing.T, src *stubSource) (*httptest.Server, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(time.Unix(1_800_000_000, 0))
	s := New(Options{Snapshots: src, Config: config.Default(), Clock: clk, RunsDir: t.TempDir(), Logf: t.Logf})
	ts := httptest.NewServer(s)
	t.Cleanup(func() {
		ts.CloseClientConnections()
		s.Close()
		ts.Close()
	})
	return ts, clk
}

func TestSSEFrameFormat(t *testing.T) {
	got := string(sseFrame("event", 41, api.Event{Seq: 41, At: 5, Kind: api.EventFencing, VMID: "vm-0008", Detail: "stale\ngen"}))
	want := "event: event\nid: 41\ndata: {\"seq\":41,\"at\":5,\"kind\":\"fencing\",\"vm_id\":\"vm-0008\",\"detail\":\"stale\\ngen\"}\n\n"
	if got != want {
		t.Errorf("event frame:\n got %q\nwant %q", got, want)
	}
	// No id line without an id: snapshots must not move Last-Event-ID.
	got = string(sseFrame("snapshot", 0, map[string]int{"seq": 7}))
	if want := "event: snapshot\ndata: {\"seq\":7}\n\n"; got != want {
		t.Errorf("snapshot frame:\n got %q\nwant %q", got, want)
	}
}

func TestSSEStream(t *testing.T) {
	src := newStubSource(api.Snapshot{Seq: 7, Scheduler: api.SchedulerUp, Racks: []api.Rack{{ID: "r1"}}})
	ts, clk := newSSEServer(t, src)
	every := config.Default().SSEEvery

	st := openStream(t, ts.URL)
	if got := st.next(); got != "retry: 2000\n\n" {
		t.Fatalf("first frame = %q, want the retry hint", got)
	}
	// The latest snapshot arrives at once, before any tick.
	if snap := st.nextSnapshot(); snap.Seq != 7 || snap.Scheduler != api.SchedulerUp || len(snap.Racks) != 1 {
		t.Fatalf("initial snapshot = %+v", snap)
	}

	// An event is forwarded as it is published, with its Seq as the SSE id.
	src.events <- api.Event{Seq: 41, At: 5, Kind: api.EventFencing, VMID: "vm-0008", Detail: "stale gen 3"}
	want := "event: event\nid: 41\ndata: {\"seq\":41,\"at\":5,\"kind\":\"fencing\",\"vm_id\":\"vm-0008\",\"detail\":\"stale gen 3\"}\n\n"
	if got := st.next(); got != want {
		t.Fatalf("event frame:\n got %q\nwant %q", got, want)
	}

	// A snapshot's Recent list backs up the lossy event channel: event 42
	// only arrives this way, and 41 is not sent twice.
	src.publish(api.Snapshot{Seq: 8, Recent: []api.Event{{Seq: 41, Kind: api.EventFencing}, {Seq: 42, Kind: api.EventChaos, Detail: "kill-host r2-h4"}}})
	if got := st.next(); !strings.HasPrefix(got, "event: event\nid: 42\ndata: {\"seq\":42,") {
		t.Fatalf("after a snapshot with recent events: frame = %q, want event 42", got)
	}

	// Snapshots are paced: two published within one tick send only the newer.
	src.publish(api.Snapshot{Seq: 9})
	clk.Advance(every)
	if snap := st.nextSnapshot(); snap.Seq != 9 {
		t.Fatalf("snapshot after tick: seq %d, want 9", snap.Seq)
	}

	// An unchanged snapshot is not sent again: the next frame after two more
	// ticks is the next event, not a repeat of snapshot 9.
	clk.Advance(every)
	clk.Advance(every)
	src.events <- api.Event{Seq: 43, Kind: api.EventAdmission, Detail: "queued"}
	if got := st.next(); !strings.HasPrefix(got, "event: event\nid: 43\n") {
		t.Fatalf("frame = %q, want event 43", got)
	}

	// A new page gets the backlog after the snapshot; a reconnecting one
	// (Last-Event-ID) only what it missed.
	fresh := openStream(t, ts.URL)
	fresh.next()
	fresh.nextSnapshot()
	for _, id := range []string{"41", "42", "43"} {
		if got := fresh.next(); !strings.HasPrefix(got, "event: event\nid: "+id+"\n") {
			t.Fatalf("backlog frame = %q, want event %s", got, id)
		}
	}
	back := openStream(t, ts.URL, "Last-Event-ID", "42")
	back.next()
	back.nextSnapshot()
	src.events <- api.Event{Seq: 44, Kind: api.EventChaos, Detail: "heal-rack r2"}
	for _, id := range []string{"43", "44"} {
		if got := back.next(); !strings.HasPrefix(got, "event: event\nid: "+id+"\n") {
			t.Fatalf("reconnect frame = %q, want event %s", got, id)
		}
	}
}

func TestSSEKeepAlive(t *testing.T) {
	src := newStubSource(api.Snapshot{Seq: 1})
	ts, clk := newSSEServer(t, src)
	st := openStream(t, ts.URL)
	st.next()
	st.nextSnapshot()
	// The hub's ticker and this stream's keep-alive ticker are both armed
	// once the first frames are out; wait for the second before advancing.
	for range 5000 {
		if clk.Pending() >= 2 {
			break
		}
		clock.Real{}.Sleep(time.Millisecond)
	}
	clk.Advance(keepAlive)
	if got := st.next(); got != ": keepalive\n\n" {
		t.Fatalf("idle stream sent %q, want a keep-alive comment", got)
	}
}

// The backlog is bounded: the oldest events fall off, and their Seq can no
// longer suppress a later event.
func TestHubBacklogIsBounded(t *testing.T) {
	h := newHub(nil, clock.NewFake(time.Unix(0, 0)), time.Second)
	for i := 1; i <= backlogEvents+10; i++ {
		h.event(api.Event{Seq: int64(i)})
	}
	_, missed := h.add(0)
	if len(missed) != backlogEvents || missed[0].Seq != 11 || missed[len(missed)-1].Seq != backlogEvents+10 {
		t.Fatalf("backlog holds %d events from %d, want %d from 11", len(missed), missed[0].Seq, backlogEvents)
	}
	if len(h.seen) != backlogEvents {
		t.Errorf("dedupe set holds %d seqs, want %d", len(h.seen), backlogEvents)
	}
	ch, _ := h.add(0)
	h.event(api.Event{Seq: int64(backlogEvents + 10)}) // duplicate of the newest
	if len(ch) != 0 {
		t.Error("a duplicate event was broadcast")
	}
}
