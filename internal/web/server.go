// Package web serves the HTTP API, the SSE hub and the embedded dashboard. Owner: W4.
//
// Handlers never touch component state: they call the api.ControlPlane,
// api.ChaosEngine and api.SnapshotSource interfaces (which message actors)
// or read published snapshots.
package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/web/static"
)

// Options wires a Server to the running system.
type Options struct {
	ControlPlane api.ControlPlane
	Chaos        api.ChaosEngine
	Snapshots    api.SnapshotSource
	Config       config.Config
	// Clock is wall time for SSE pacing and the per-IP chaos limit. Default
	// clock.Real{}. (Sim time comes from snapshots.)
	Clock clock.Clock
	// RunsDir holds runs/<id>/ recordings. Default "runs".
	RunsDir string
	// Metrics serves /metrics (the scheduler's Prometheus handler). When nil,
	// a minimal exposition is derived from the latest snapshot.
	Metrics http.Handler
	// Static overrides the embedded dashboard, e.g. os.DirFS("web/static")
	// for live editing.
	Static fs.FS
	// Logf logs handler failures. Default log.Printf.
	Logf func(format string, args ...any)
}

// Server is the HTTP front of the control plane.
//
// The idle auto-reset from the plan's public-URL hardening is not here: the
// chaos engine already resets itself after config.IdleResetAfter of wall time
// without chaos, and a second timer in this package would reset twice.
type Server struct {
	o       Options
	mux     *http.ServeMux
	hub     *hub
	limiter *ipLimiter
	cancel  context.CancelFunc
	done    chan struct{} // closed when the hub has stopped
}

// New builds a Server and starts its SSE hub. Call Close to stop the hub.
func New(o Options) *Server {
	if o.Clock == nil {
		o.Clock = clock.Real{}
	}
	if o.RunsDir == "" {
		o.RunsDir = "runs"
	}
	if o.Static == nil {
		o.Static = static.FS
	}
	if o.Logf == nil {
		o.Logf = log.Printf
	}
	if o.Config.SSEEvery <= 0 {
		o.Config = config.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{o: o, mux: http.NewServeMux(), cancel: cancel, done: make(chan struct{}),
		limiter: newIPLimiter(o.Config.ChaosPerIP)}
	s.hub = newHub(o.Snapshots, o.Clock, o.Config.SSEEvery)
	go func() {
		defer close(s.done)
		s.hub.run(ctx)
	}()
	s.routes()
	return s
}

// Close stops the SSE hub; open event streams end.
func (s *Server) Close() {
	s.cancel()
	<-s.done
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) routes() {
	m := s.mux
	m.HandleFunc("POST /v1/vms", s.createVM)
	m.HandleFunc("GET /v1/vms", s.listVMs)
	m.HandleFunc("GET /v1/vms/{id}", s.getVM)
	m.HandleFunc("DELETE /v1/vms/{id}", s.deleteVM)
	m.HandleFunc("GET /v1/hosts", s.listHosts)
	m.HandleFunc("GET /v1/queue", s.queue)
	m.HandleFunc("POST /v1/queue/{vm_id}/priority", s.setPriority)
	m.HandleFunc("GET /v1/events", s.events)
	m.HandleFunc("GET /v1/snapshot", s.snapshot)
	m.HandleFunc("GET /v1/chaos/actions", s.chaosActions)
	m.HandleFunc("POST /v1/chaos/{action}", s.limited(s.chaosDo))
	m.HandleFunc("POST /v1/sim/{action}", s.sim)
	m.HandleFunc("POST /v1/topology/{action}", s.topology)
	m.HandleFunc("GET /v1/runs", s.listRuns)
	m.HandleFunc("GET /v1/runs/{id}", s.getRun)
	m.HandleFunc("GET /v1/runs/{id}/snapshots", s.runSnapshots)
	m.HandleFunc("GET /metrics", s.metrics)
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, "ok")
	})
	m.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, &api.Error{Status: http.StatusNotFound, Msg: "no route " + r.Method + " " + r.URL.Path})
	})
	files := http.FileServerFS(s.o.Static)
	// "GET /" would conflict with the method-less "/v1/" catch-all above
	// (fewer methods, more general path), so the method is checked here.
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeError(w, &api.Error{Status: http.StatusMethodNotAllowed, Msg: "method not allowed"})
			return
		}
		// The dashboard is tiny and changes with each deploy: always revalidate.
		w.Header().Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})
}

// ------------------------------------------------------------------ errors

// writeError maps err to a status and an ErrorResponse body. *api.Error
// carries its own status; 429 and 503 also get Retry-After.
func writeError(w http.ResponseWriter, err error) {
	status, msg, retry := http.StatusInternalServerError, err.Error(), 0
	var ae *api.Error
	switch {
	case errors.As(err, &ae):
		status, msg, retry = ae.Status, ae.Msg, ae.RetryAfterS
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		status = 499 // client went away; nobody reads this
	}
	if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
		if retry <= 0 {
			retry = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(retry))
	} else {
		retry = 0
	}
	writeJSON(w, status, api.ErrorResponse{Error: msg, RetryAfterS: retry})
}

func badRequest(msg string) error { return &api.Error{Status: http.StatusBadRequest, Msg: msg} }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.Encode(v)
}

// decode reads an optional JSON body into v. An empty body leaves v zero.
func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return badRequest("bad JSON body: " + err.Error())
	}
	return nil
}

func (s *Server) unavailable(what string) error {
	return &api.Error{Status: http.StatusServiceUnavailable, Msg: what + " not wired", RetryAfterS: 5}
}

// ---------------------------------------------------------------- VMs

func parsePriority(p string) (api.Priority, bool) {
	switch p {
	case "interactive":
		return api.PriorityInteractive, true
	case "standard", "":
		return api.PriorityStandard, true
	case "batch":
		return api.PriorityBatch, true
	}
	return 0, false
}

func (s *Server) createVM(w http.ResponseWriter, r *http.Request) {
	if s.o.ControlPlane == nil {
		writeError(w, s.unavailable("control plane"))
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, badRequest("Idempotency-Key header is required"))
		return
	}
	var req api.CreateVMRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if _, ok := parsePriority(req.Priority); !ok {
		writeError(w, badRequest(`priority must be "interactive", "standard" or "batch"`))
		return
	}
	resp, err := s.o.ControlPlane.CreateVM(r.Context(), key, req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, resp)
}

func (s *Server) listVMs(w http.ResponseWriter, r *http.Request) {
	s.cp(w, func() (any, error) { return s.o.ControlPlane.ListVMs(r.Context()) })
}

func (s *Server) getVM(w http.ResponseWriter, r *http.Request) {
	s.cp(w, func() (any, error) { return s.o.ControlPlane.GetVM(r.Context(), r.PathValue("id")) })
}

func (s *Server) listHosts(w http.ResponseWriter, r *http.Request) {
	s.cp(w, func() (any, error) { return s.o.ControlPlane.ListHosts(r.Context()) })
}

func (s *Server) queue(w http.ResponseWriter, r *http.Request) {
	s.cp(w, func() (any, error) { return s.o.ControlPlane.Queue(r.Context()) })
}

// cp runs a read against the control plane and writes 200 with the result.
func (s *Server) cp(w http.ResponseWriter, fn func() (any, error)) {
	if s.o.ControlPlane == nil {
		writeError(w, s.unavailable("control plane"))
		return
	}
	v, err := fn()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) deleteVM(w http.ResponseWriter, r *http.Request) {
	if s.o.ControlPlane == nil {
		writeError(w, s.unavailable("control plane"))
		return
	}
	v, err := s.o.ControlPlane.DeleteVM(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, v)
}

func (s *Server) setPriority(w http.ResponseWriter, r *http.Request) {
	if s.o.ControlPlane == nil {
		writeError(w, s.unavailable("control plane"))
		return
	}
	var req api.SetPriorityRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	p, ok := parsePriority(req.Priority)
	if !ok || req.Priority == "" {
		writeError(w, badRequest(`priority must be "interactive", "standard" or "batch"`))
		return
	}
	if err := s.o.ControlPlane.SetPriority(r.Context(), r.PathValue("vm_id"), p); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ------------------------------------------------------- chaos, sim, topology

func (s *Server) chaosActions(w http.ResponseWriter, _ *http.Request) {
	if s.o.Chaos == nil {
		writeError(w, s.unavailable("chaos engine"))
		return
	}
	c := s.o.Chaos.Catalog()
	if c.Actions == nil {
		c.Actions = []api.ChaosAction{}
	}
	if c.Presets == nil {
		c.Presets = []api.ChaosAction{}
	}
	writeJSON(w, http.StatusOK, c)
}

func (s *Server) chaosDo(w http.ResponseWriter, r *http.Request) {
	if s.o.Chaos == nil {
		writeError(w, s.unavailable("chaos engine"))
		return
	}
	var req api.ChaosRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	res, err := s.o.Chaos.Do(r.Context(), r.PathValue("action"), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) sim(w http.ResponseWriter, r *http.Request) {
	if s.o.Chaos == nil {
		writeError(w, s.unavailable("chaos engine"))
		return
	}
	var req api.SimRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	action := r.PathValue("action")
	if action == "speed" && (req.Speed <= 0 || req.Speed > s.o.Config.MaxSpeed) {
		writeError(w, badRequest(fmt.Sprintf("speed must be in (0, %g]", s.o.Config.MaxSpeed)))
		return
	}
	if req.Intensity != nil && (*req.Intensity < 0 || *req.Intensity > 1) {
		writeError(w, badRequest("intensity must be in [0, 1]"))
		return
	}
	if req.LoadRate != nil && *req.LoadRate < 0 {
		writeError(w, badRequest("load_rate must be >= 0"))
		return
	}
	st, err := s.o.Chaos.Sim(r.Context(), action, req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) topology(w http.ResponseWriter, r *http.Request) {
	if s.o.Chaos == nil {
		writeError(w, s.unavailable("chaos engine"))
		return
	}
	var req api.TopologyRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	res, err := s.o.Chaos.Topology(r.Context(), r.PathValue("action"), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// limited applies the per-IP chaos rate limit (config.ChaosPerIP, on wall
// time) to POST /v1/chaos/{action} only. Sim controls, topology changes and
// the VM API are not chaos and are not limited.
func (s *Server) limited(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if wait := s.limiter.allow(clientIP(r), s.o.Clock.Now()); wait > 0 {
			secs := int((wait + time.Second - 1) / time.Second)
			writeError(w, &api.Error{Status: http.StatusTooManyRequests,
				Msg: fmt.Sprintf("chaos rate limit: one action per %s per client", s.o.Config.ChaosPerIP), RetryAfterS: secs})
			return
		}
		h(w, r)
	}
}

// clientIP prefers the first X-Forwarded-For hop (Railway's proxy sets it).
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first, _, _ := strings.Cut(xff, ",")
		return strings.TrimSpace(first)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type ipLimiter struct {
	every time.Duration
	mu    sync.Mutex
	last  map[string]time.Time
}

func newIPLimiter(every time.Duration) *ipLimiter {
	return &ipLimiter{every: every, last: map[string]time.Time{}}
}

// allow records an action from ip at now, or returns how long to wait.
func (l *ipLimiter) allow(ip string, now time.Time) time.Duration {
	if l.every <= 0 {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if t, ok := l.last[ip]; ok && now.Sub(t) < l.every {
		return l.every - now.Sub(t)
	}
	if len(l.last) > 4096 {
		for k, t := range l.last {
			if now.Sub(t) >= l.every {
				delete(l.last, k)
			}
		}
	}
	l.last[ip] = now
	return 0
}

// ------------------------------------------------------------- snapshots

func (s *Server) snapshot(w http.ResponseWriter, _ *http.Request) {
	if s.o.Snapshots == nil {
		writeError(w, s.unavailable("snapshot source"))
		return
	}
	writeJSON(w, http.StatusOK, s.o.Snapshots.Latest())
}

// --------------------------------------------------------------- metrics

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	if s.o.Metrics != nil {
		s.o.Metrics.ServeHTTP(w, r)
		return
	}
	if s.o.Snapshots == nil {
		writeError(w, s.unavailable("snapshot source"))
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writeSnapshotMetrics(w, s.o.Snapshots.Latest())
}

// writeSnapshotMetrics is the fallback exposition when no scheduler metrics
// handler is wired: gauges and counters read off the latest snapshot.
func writeSnapshotMetrics(w io.Writer, snap api.Snapshot) {
	c := snap.Counters
	fmt.Fprintf(w, "# TYPE queue_depth gauge\nqueue_depth %d\n", c.QueueDepth)
	fmt.Fprintf(w, "# TYPE admissions_total counter\n")
	for _, kv := range []struct {
		k string
		v int
	}{{"placed", c.Placed}, {"queued", c.Queued}, {"rejected_429", c.Rejected429}, {"shed", c.Shed}} {
		fmt.Fprintf(w, "admissions_total{result=%q} %d\n", kv.k, kv.v)
	}
	fmt.Fprintf(w, "# TYPE recoveries_total counter\n")
	for _, k := range sortedKeys(c.Recoveries) {
		fmt.Fprintf(w, "recoveries_total{kind=%q} %d\n", k, c.Recoveries[k])
	}
	fmt.Fprintf(w, "# TYPE vms gauge\n")
	for _, st := range []api.VMState{api.VMPending, api.VMBooting, api.VMReady, api.VMUnhealthy,
		api.VMLost, api.VMTerminating, api.VMTerminated, api.VMFailed} {
		fmt.Fprintf(w, "vms{state=%q} %d\n", st, c.ByState[st])
	}
	fmt.Fprintf(w, "# TYPE rack_loss_headroom gauge\nrack_loss_headroom %d\n", c.Headroom)
	fmt.Fprintf(w, "# TYPE invariant_violations_total counter\n")
	for _, inv := range snap.Invariants {
		fmt.Fprintf(w, "invariant_violations_total{name=%q} %d\n", inv.Name, inv.Violations)
	}
	fmt.Fprintf(w, "# TYPE security_blocked_total counter\n")
	for _, k := range sortedKeys(snap.Security.ByAttack) {
		fmt.Fprintf(w, "security_blocked_total{attack=%q} %d\n", k, snap.Security.ByAttack[k])
	}
	fmt.Fprintf(w, "# TYPE time_to_ready_p99_seconds gauge\n")
	for _, k := range sortedKeys(snap.Latency) {
		fmt.Fprintf(w, "time_to_ready_p99_seconds{class=%q} %g\n", k, float64(snap.Latency[k].P99Ms)/1000)
	}
}

func sortedKeys[V any](m map[string]V) []string { return slices.Sorted(maps.Keys(m)) }
