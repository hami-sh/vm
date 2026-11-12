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
// client gets the latest snapshot on connect, then one "snapshot" message
// every SSEEvery of wall time (when it changed) and every "event" as it is
// published. A slow client misses messages rather than slowing the others:
// the next snapshot supersedes whatever it missed.
type hub struct {
	src   api.SnapshotSource
	clk   clock.Clock
	every time.Duration

	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

// clientBuffer is how many messages a client may fall behind before drops.
const clientBuffer = 256

func newHub(src api.SnapshotSource, clk clock.Clock, every time.Duration) *hub {
	return &hub{src: src, clk: clk, every: every, clients: map[chan []byte]struct{}{}}
}

// sseFrame formats one SSE message. JSON never contains a raw newline, so
// data fits on one line.
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

func (h *hub) add() chan []byte {
	ch := make(chan []byte, clientBuffer)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) remove(ch chan []byte) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
}

func (h *hub) broadcast(frame []byte) {
	if frame == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- frame:
		default: // slow client: drop
		}
	}
}

// run owns the subscription until ctx ends, resubscribing if the source
// closes its channels (e.g. the fleet was rebuilt by a reset).
func (h *hub) run(ctx context.Context) {
	if h.src == nil {
		return
	}
	for ctx.Err() == nil {
		h.follow(ctx)
		select {
		case <-ctx.Done():
		case <-h.clk.After(250 * time.Millisecond):
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
			latest, have = s, true
		case e, ok := <-events:
			if !ok {
				return
			}
			h.broadcast(sseFrame("event", e.Seq, e))
		case <-tick.C():
			if have && latest.Seq != sentSeq {
				sentSeq = latest.Seq
				h.broadcast(sseFrame("snapshot", 0, latest))
			}
		}
	}
}

// keepAlive is the wall time between SSE comments on an idle stream, so
// proxies don't close it.
const keepAlive = 15 * time.Second

// events serves GET /v1/events.
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	if s.o.Snapshots == nil {
		writeError(w, s.unavailable("snapshot source"))
		return
	}
	rc := http.NewResponseController(w)
	rc.SetWriteDeadline(time.Time{}) // a stream outlives any server write timeout
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	ch := s.hub.add()
	defer s.hub.remove(ch)
	w.Write([]byte("retry: 2000\n\n"))
	w.Write(sseFrame("snapshot", 0, s.o.Snapshots.Latest()))
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
