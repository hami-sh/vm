package web

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
)

// hub fans one SnapshotSource subscription out to every SSE client. Each
// client gets the latest snapshot and the recent event backlog on connect,
// then one "snapshot" message every SSEEvery of wall time (when it changed)
// and every "event" as it is published. A slow client misses messages rather
// than slowing the others: the next snapshot supersedes whatever it missed.
type hub struct {
	src   api.SnapshotSource
	clk   clock.Clock
	every time.Duration

	mu      sync.Mutex
	clients map[chan []byte]struct{}
	// backlog holds the last backlogEvents events, oldest first, so a page
	// opened mid-run has history for its event log and detail panel.
	backlog []api.Event
	seen    map[int64]struct{} // Seq of every event in backlog
}

const (
	// clientBuffer is how many messages a client may fall behind before drops.
	clientBuffer = 256
	// backlogEvents is how many events a new client is sent on connect.
	backlogEvents = 500
	// keepAlive is the wall time between SSE comments on an idle stream, so
	// proxies don't close it.
	keepAlive = 15 * time.Second
	// resubscribeAfter is the wall-time pause before subscribing again when
	// the source closes its channels.
	resubscribeAfter = 250 * time.Millisecond
)

func newHub(src api.SnapshotSource, clk clock.Clock, every time.Duration) *hub {
	return &hub{src: src, clk: clk, every: every,
		clients: map[chan []byte]struct{}{}, seen: map[int64]struct{}{}}
}

// sseFrame formats one SSE message: "event: <name>", an optional "id: <id>"
// and one "data: <json>" line, ended by a blank line. encoding/json never
// emits a raw newline, so the payload always fits on one data line.
func sseFrame(event string, id int64, v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var buf bytes.Buffer
	buf.Grow(len(b) + 48)
	buf.WriteString("event: ")
	buf.WriteString(event)
	if id > 0 {
		buf.WriteString("\nid: ")
		buf.WriteString(strconv.FormatInt(id, 10))
	}
	buf.WriteString("\ndata: ")
	buf.Write(b)
	buf.WriteString("\n\n")
	return buf.Bytes()
}

// add registers a client and returns the backlog events after afterSeq, in
// one step, so the client sees every event exactly once.
func (h *hub) add(afterSeq int64) (chan []byte, []api.Event) {
	ch := make(chan []byte, clientBuffer)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[ch] = struct{}{}
	var missed []api.Event
	for _, e := range h.backlog {
		if e.Seq > afterSeq {
			missed = append(missed, e)
		}
	}
	return ch, missed
}

func (h *hub) remove(ch chan []byte) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
}

func (h *hub) broadcastLocked(frame []byte) {
	if frame == nil {
		return
	}
	for ch := range h.clients {
		select {
		case ch <- frame:
		default: // slow client: drop
		}
	}
}

func (h *hub) snapshot(s api.Snapshot) {
	frame := sseFrame("snapshot", 0, s)
	h.mu.Lock()
	h.broadcastLocked(frame)
	h.mu.Unlock()
}

// event broadcasts e unless it was already sent. The source's event channel
// drops events when it is full, so the hub also feeds every snapshot's
// Recent list through here; Seq makes the second copy a no-op.
func (h *hub) event(e api.Event) {
	frame := sseFrame("event", e.Seq, e)
	h.mu.Lock()
	defer h.mu.Unlock()
	if e.Seq != 0 {
		if _, dup := h.seen[e.Seq]; dup {
			return
		}
		h.seen[e.Seq] = struct{}{}
	}
	h.backlog = append(h.backlog, e)
	if over := len(h.backlog) - backlogEvents; over > 0 {
		for _, old := range h.backlog[:over] {
			delete(h.seen, old.Seq)
		}
		h.backlog = append(h.backlog[:0], h.backlog[over:]...)
	}
	h.broadcastLocked(frame)
}

// run owns the subscription until ctx ends, resubscribing if the source
// closes its channels (e.g. the fleet was rebuilt).
func (h *hub) run(ctx context.Context) {
	if h.src == nil {
		return
	}
	for ctx.Err() == nil {
		h.follow(ctx)
		select {
		case <-ctx.Done():
		case <-h.clk.After(resubscribeAfter):
		}
	}
}

func (h *hub) follow(ctx context.Context) {
	snaps, events, cancel := h.src.Subscribe()
	defer cancel()
	tick := h.clk.NewTicker(h.every)
	defer tick.Stop()
	var latest api.Snapshot
	have, sentSeq := false, int64(-1)
	for {
		select {
		case <-ctx.Done():
			return
		case s, ok := <-snaps:
			if !ok {
				return
			}
			for _, e := range s.Recent {
				h.event(e)
			}
			latest, have = s, true
		case e, ok := <-events:
			if !ok {
				return
			}
			h.event(e)
		case <-tick.C():
			if have && latest.Seq != sentSeq {
				sentSeq = latest.Seq
				h.snapshot(latest)
			}
		}
	}
}

// events serves GET /v1/events: the latest snapshot, the event backlog (only
// events after Last-Event-ID when the browser is reconnecting), then live
// messages.
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	if s.o.Snapshots == nil {
		writeError(w, s.unavailable("snapshot source"))
		return
	}
	rc := http.NewResponseController(w)
	rc.SetWriteDeadline(time.Time{}) // a stream outlives any server write timeout
	hd := w.Header()
	hd.Set("Content-Type", "text/event-stream")
	hd.Set("Cache-Control", "no-cache")
	hd.Set("Connection", "keep-alive")
	hd.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	after, _ := strconv.ParseInt(r.Header.Get("Last-Event-ID"), 10, 64)
	ch, missed := s.hub.add(after)
	defer s.hub.remove(ch)
	w.Write([]byte("retry: 2000\n\n"))
	w.Write(sseFrame("snapshot", 0, s.o.Snapshots.Latest()))
	for _, e := range missed {
		w.Write(sseFrame("event", e.Seq, e))
	}
	if rc.Flush() != nil {
		return
	}
	ka := s.o.Clock.NewTicker(keepAlive)
	defer ka.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.done:
			return
		case frame := <-ch:
			if _, err := w.Write(frame); err != nil {
				return
			}
			// Coalesce whatever else is already queued into one flush.
			for n := len(ch); n > 0; n-- {
				w.Write(<-ch)
			}
		case <-ka.C():
			if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
				return
			}
		}
		if rc.Flush() != nil {
			return
		}
	}
}
