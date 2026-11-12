package web

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/web/fake"
	"github.com/hami-sh/vm/internal/web/recording"
)

const fixturePath = "../../fixtures/snapshot.json"

// stubPlane is a ControlPlane whose answers the test sets. It records what
// the handler passed through.
type stubPlane struct {
	api.ControlPlane // any method a test didn't expect panics
	err              error
	key              string
	create           api.CreateVMRequest
	priorityVM       string
	priority         api.Priority
}

func (p *stubPlane) CreateVM(_ context.Context, key string, req api.CreateVMRequest) (api.CreateVMResponse, error) {
	p.key, p.create = key, req
	return api.CreateVMResponse{ID: "vm-0042", State: api.VMPending, Generation: 0}, p.err
}

func (p *stubPlane) GetVM(context.Context, string) (api.VM, error) { return api.VM{}, p.err }

func (p *stubPlane) DeleteVM(_ context.Context, id string) (api.VM, error) {
	return api.VM{ID: id, State: api.VMTerminating, Desired: api.DesiredDeleted}, p.err
}

func (p *stubPlane) SetPriority(_ context.Context, id string, pr api.Priority) error {
	p.priorityVM, p.priority = id, pr
	return p.err
}

type harness struct {
	t     *testing.T
	srv   *Server
	fleet *fake.Fleet
	clk   *clock.Fake
}

// newHarness serves the fixture-backed fake on a fake wall clock.
func newHarness(t *testing.T, mut func(*Options)) *harness {
	t.Helper()
	base, err := fake.LoadFixture(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	f := fake.New(base)
	clk := clock.NewFake(time.Unix(1_800_000_000, 0))
	o := Options{ControlPlane: f, Chaos: f, Snapshots: f, Config: config.Default(), Clock: clk,
		RunsDir: t.TempDir(), Logf: t.Logf}
	if mut != nil {
		mut(&o)
	}
	s := New(o)
	t.Cleanup(s.Close)
	return &harness{t: t, srv: s, fleet: f, clk: clk}
}

// do sends one request. headers are key, value pairs.
func (h *harness) do(method, path, body string, headers ...string) *httptest.ResponseRecorder {
	h.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, req)
	return rec
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	return v
}

func wantStatus(t *testing.T, rec *httptest.ResponseRecorder, status int) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, status, rec.Body.String())
	}
}

func TestCreateVM(t *testing.T) {
	plane := &stubPlane{}
	h := newHarness(t, func(o *Options) { o.ControlPlane = plane })

	rec := h.do("POST", "/v1/vms", `{"pool":"ios-xcode16-m","priority":"interactive"}`, "Idempotency-Key", "k-1")
	wantStatus(t, rec, http.StatusAccepted)
	if got := decodeBody[api.CreateVMResponse](t, rec); got.ID != "vm-0042" || got.State != api.VMPending {
		t.Errorf("response = %+v", got)
	}
	if plane.key != "k-1" || plane.create != (api.CreateVMRequest{Pool: "ios-xcode16-m", Priority: "interactive"}) {
		t.Errorf("control plane got key %q, request %+v", plane.key, plane.create)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}

	bad := []struct {
		name, body string
		headers    []string
	}{
		{"no idempotency key", `{"pool":"p"}`, nil},
		{"unknown priority", `{"pool":"p","priority":"urgent"}`, []string{"Idempotency-Key", "k"}},
		{"replacement class is not requestable", `{"pool":"p","priority":"replacement"}`, []string{"Idempotency-Key", "k"}},
		{"unknown field", `{"pool":"p","prio":"batch"}`, []string{"Idempotency-Key", "k"}},
		{"not JSON", `pool=p`, []string{"Idempotency-Key", "k"}},
	}
	for _, tc := range bad {
		rec := h.do("POST", "/v1/vms", tc.body, tc.headers...)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", tc.name, rec.Code)
		}
		if decodeBody[api.ErrorResponse](t, rec).Error == "" {
			t.Errorf("%s: empty error body", tc.name)
		}
	}
}

// Every *api.Error keeps its status; 429 and 503 always carry Retry-After,
// in the header and the body, and nothing else does.
func TestErrorMapping(t *testing.T) {
	cases := []struct {
		err        error
		status     int
		retryAfter string
	}{
		{&api.Error{Status: 429, Msg: "queue full", RetryAfterS: 7}, 429, "7"},
		{&api.Error{Status: 503, Msg: "scheduler down", RetryAfterS: 3}, 503, "3"},
		{&api.Error{Status: 503, Msg: "scheduler down"}, 503, "1"},
		{&api.Error{Status: 422, Msg: "key reused", RetryAfterS: 9}, 422, ""},
		{&api.Error{Status: 404, Msg: "vm not found"}, 404, ""},
		{context.DeadlineExceeded, 504, ""},
		{io.ErrUnexpectedEOF, 500, ""},
	}
	for _, tc := range cases {
		h := newHarness(t, func(o *Options) { o.ControlPlane = &stubPlane{err: tc.err} })
		rec := h.do("POST", "/v1/vms", `{"pool":"p"}`, "Idempotency-Key", "k")
		if rec.Code != tc.status {
			t.Errorf("%v: status = %d, want %d", tc.err, rec.Code, tc.status)
		}
		if got := rec.Header().Get("Retry-After"); got != tc.retryAfter {
			t.Errorf("%v: Retry-After = %q, want %q", tc.err, got, tc.retryAfter)
		}
		body := decodeBody[api.ErrorResponse](t, rec)
		wantBody := 0
		if tc.retryAfter != "" {
			wantBody = int(tc.retryAfter[0] - '0')
		}
		if body.RetryAfterS != wantBody || body.Error == "" {
			t.Errorf("%v: body = %+v, want retry_after_s %d", tc.err, body, wantBody)
		}
	}
}

func TestVMRoutes(t *testing.T) {
	h := newHarness(t, nil)

	vms := decodeBody[[]api.VM](t, h.do("GET", "/v1/vms", ""))
	if len(vms) != 19 {
		t.Fatalf("GET /v1/vms: %d VMs, want the fixture's 19", len(vms))
	}
	vm := decodeBody[api.VM](t, h.do("GET", "/v1/vms/vm-0005", ""))
	if vm.State != api.VMUnhealthy || vm.HostID != "r1-h4" || vm.Generation != 3 {
		t.Errorf("GET /v1/vms/vm-0005 = %+v", vm)
	}
	wantStatus(t, h.do("GET", "/v1/vms/vm-9999", ""), http.StatusNotFound)

	rec := h.do("DELETE", "/v1/vms/vm-0001", "")
	wantStatus(t, rec, http.StatusAccepted)
	if got := decodeBody[api.VM](t, rec); got.State != api.VMTerminating || got.Desired != api.DesiredDeleted {
		t.Errorf("DELETE = %+v", got)
	}
	// Idempotent: a second delete returns the VM unchanged.
	rec = h.do("DELETE", "/v1/vms/vm-0001", "")
	wantStatus(t, rec, http.StatusAccepted)
	if got := decodeBody[api.VM](t, rec); got.State != api.VMTerminating {
		t.Errorf("second DELETE = %+v", got)
	}

	if hosts := decodeBody[[]api.Host](t, h.do("GET", "/v1/hosts", "")); len(hosts) != 12 {
		t.Errorf("GET /v1/hosts: %d hosts, want 12", len(hosts))
	}
	queue := decodeBody[[]api.QueueEntry](t, h.do("GET", "/v1/queue", ""))
	if len(queue) != 3 || queue[0].VMID != "vm-0016" {
		t.Errorf("GET /v1/queue = %+v, want 3 entries led by the replacement vm-0016", queue)
	}
}

func TestSetPriority(t *testing.T) {
	plane := &stubPlane{}
	h := newHarness(t, func(o *Options) { o.ControlPlane = plane })

	rec := h.do("POST", "/v1/queue/vm-0015/priority", `{"priority":"batch"}`)
	wantStatus(t, rec, http.StatusNoContent)
	if plane.priorityVM != "vm-0015" || plane.priority != api.PriorityBatch {
		t.Errorf("control plane got %q, class %d; want vm-0015, class 3", plane.priorityVM, plane.priority)
	}
	h.do("POST", "/v1/queue/vm-0015/priority", `{"priority":"interactive"}`)
	if plane.priority != api.PriorityInteractive {
		t.Errorf("interactive mapped to class %d, want 1", plane.priority)
	}
	// Class 0 is reserved for replacements, and a move must name a class.
	for _, body := range []string{`{"priority":"replacement"}`, `{"priority":""}`, `{}`, `{"priority":0}`} {
		plane.priorityVM = ""
		if rec := h.do("POST", "/v1/queue/vm-0015/priority", body); rec.Code != http.StatusBadRequest || plane.priorityVM != "" {
			t.Errorf("%s: status = %d (reached control plane: %v), want 400", body, rec.Code, plane.priorityVM != "")
		}
	}
}

func TestChaosCatalogAndAction(t *testing.T) {
	h := newHarness(t, nil)

	cat := decodeBody[api.ChaosCatalog](t, h.do("GET", "/v1/chaos/actions", ""))
	if len(cat.Actions) == 0 || len(cat.Presets) != 6 {
		t.Fatalf("catalog: %d actions, %d presets", len(cat.Actions), len(cat.Presets))
	}

	rec := h.do("POST", "/v1/chaos/partition-rack", `{"target":"r2"}`)
	wantStatus(t, rec, http.StatusOK)
	if got := decodeBody[api.ChaosResult](t, rec); got.Action != "partition-rack" || got.Target != "r2" {
		t.Errorf("result = %+v", got)
	}
	snap := decodeBody[api.Snapshot](t, h.do("GET", "/v1/snapshot", ""))
	if !snap.Racks[1].Partitioned || snap.Racks[0].Partitioned {
		t.Errorf("after partition-rack r2: racks = %+v", snap.Racks)
	}

	h.clk.Advance(time.Second)
	wantStatus(t, h.do("POST", "/v1/chaos/no-such-action", `{}`), http.StatusUnprocessableEntity)
	h.clk.Advance(time.Second)
	wantStatus(t, h.do("POST", "/v1/chaos/kill-host", `{"target":"r9-h9"}`), http.StatusNotFound)
}

// One chaos action per ChaosPerIP of wall time for each client, and only for
// POST /v1/chaos/*.
func TestChaosRateLimitPerIP(t *testing.T) {
	h := newHarness(t, nil)
	every := config.Default().ChaosPerIP
	chaos := func(headers ...string) *httptest.ResponseRecorder {
		return h.do("POST", "/v1/chaos/cordon", `{"target":"r1-h1"}`, headers...)
	}

	wantStatus(t, chaos(), http.StatusOK)
	h.clk.Advance(every - time.Millisecond)
	rec := chaos()
	wantStatus(t, rec, http.StatusTooManyRequests)
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
	// A refused request doesn't push the window out.
	h.clk.Advance(time.Millisecond)
	wantStatus(t, chaos(), http.StatusOK)

	// Behind Railway's proxy every request has the same RemoteAddr; the
	// client is the first X-Forwarded-For hop.
	h.clk.Advance(every)
	wantStatus(t, chaos("X-Forwarded-For", "203.0.113.7, 10.0.0.1"), http.StatusOK)
	wantStatus(t, chaos("X-Forwarded-For", "203.0.113.8, 10.0.0.1"), http.StatusOK)
	wantStatus(t, chaos("X-Forwarded-For", "203.0.113.7, 10.0.0.2"), http.StatusTooManyRequests)

	// The limit is for chaos only: the same client can read the catalogue,
	// change the topology, drive the sim and use the VM API at once.
	h.clk.Advance(every)
	wantStatus(t, chaos(), http.StatusOK)
	wantStatus(t, h.do("GET", "/v1/chaos/actions", ""), http.StatusOK)
	wantStatus(t, h.do("POST", "/v1/topology/add-host", `{"rack":"r1"}`), http.StatusOK)
	wantStatus(t, h.do("POST", "/v1/topology/add-host", `{"rack":"r1"}`), http.StatusOK)
	wantStatus(t, h.do("POST", "/v1/sim/pause", ``), http.StatusOK)
	wantStatus(t, h.do("POST", "/v1/sim/play", ``), http.StatusOK)
	wantStatus(t, h.do("DELETE", "/v1/vms/vm-0002", ""), http.StatusAccepted)
}

func TestSimAndTopology(t *testing.T) {
	h := newHarness(t, nil)

	st := decodeBody[api.SimStatus](t, h.do("POST", "/v1/sim/pause", ""))
	if !st.Paused {
		t.Errorf("pause: %+v", st)
	}
	st = decodeBody[api.SimStatus](t, h.do("POST", "/v1/sim/speed", `{"speed":10}`))
	if st.Speed != 10 || !st.Paused {
		t.Errorf("speed: %+v", st)
	}
	st = decodeBody[api.SimStatus](t, h.do("POST", "/v1/sim/chaos", `{"intensity":0.5,"load_rate":0,"filters":["kill-host"]}`))
	if st.Intensity != 0.5 || st.LoadRate != 0 || len(st.Filters) != 1 || st.Filters[0] != "kill-host" {
		t.Errorf("chaos settings: %+v", st)
	}
	for _, tc := range []struct{ action, body string }{
		{"speed", `{"speed":0}`},
		{"speed", `{"speed":-1}`},
		{"speed", `{"speed":50.5}`}, // config.MaxSpeed is 50
		{"chaos", `{"intensity":1.01}`},
		{"chaos", `{"intensity":-0.1}`},
		{"chaos", `{"load_rate":-1}`},
	} {
		if rec := h.do("POST", "/v1/sim/"+tc.action, tc.body); rec.Code != http.StatusBadRequest {
			t.Errorf("sim/%s %s: status = %d, want 400", tc.action, tc.body, rec.Code)
		}
	}
	wantStatus(t, h.do("POST", "/v1/sim/speed", `{"speed":50}`), http.StatusOK)

	wantStatus(t, h.do("POST", "/v1/topology/add-rack", `{"hosts":2}`), http.StatusOK)
	snap := decodeBody[api.Snapshot](t, h.do("GET", "/v1/snapshot", ""))
	if len(snap.Racks) != 4 || len(snap.Racks[3].Hosts) != 2 || len(snap.Hosts) != 14 {
		t.Errorf("after add-rack: %d racks, %d hosts", len(snap.Racks), len(snap.Hosts))
	}
	wantStatus(t, h.do("POST", "/v1/topology/add-host", `{"rack":"r7"}`), http.StatusNotFound)
}

func TestUnwiredBackendsAre503(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.ControlPlane, o.Chaos, o.Snapshots = nil, nil, nil })
	for _, r := range [][2]string{
		{"GET", "/v1/vms"}, {"POST", "/v1/vms"}, {"GET", "/v1/hosts"}, {"GET", "/v1/queue"},
		{"GET", "/v1/chaos/actions"}, {"POST", "/v1/chaos/reset"}, {"POST", "/v1/sim/pause"},
		{"POST", "/v1/topology/add-rack"}, {"GET", "/v1/events"}, {"GET", "/v1/snapshot"}, {"GET", "/metrics"},
	} {
		rec := h.do(r[0], r[1], "")
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
			t.Errorf("%s %s: status = %d, Retry-After %q; want 503 with Retry-After", r[0], r[1], rec.Code, rec.Header().Get("Retry-After"))
		}
	}
}

func TestHealthStaticAndUnknownRoutes(t *testing.T) {
	h := newHarness(t, nil)

	rec := h.do("GET", "/healthz", "")
	if rec.Code != 200 || rec.Body.String() != "ok" {
		t.Errorf("/healthz = %d %q", rec.Code, rec.Body.String())
	}
	rec = h.do("GET", "/", "")
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") ||
		!strings.Contains(rec.Body.String(), "<title>") {
		t.Errorf("/ = %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("/ Cache-Control = %q", cc)
	}
	rec = h.do("GET", "/v1/nope", "")
	wantStatus(t, rec, http.StatusNotFound)
	if decodeBody[api.ErrorResponse](t, rec).Error == "" {
		t.Error("unknown /v1 route: want a JSON error body")
	}
}

func TestMetrics(t *testing.T) {
	h := newHarness(t, nil)
	rec := h.do("GET", "/metrics", "")
	wantStatus(t, rec, http.StatusOK)
	for _, line := range []string{
		"queue_depth 3\n",
		`admissions_total{result="rejected_429"} 4` + "\n",
		`vms{state="Ready"} 8` + "\n",
		`invariant_violations_total{name="one_authoritative_copy"} 0` + "\n",
		`security_blocked_total{attack="east_west"} 3` + "\n",
		`time_to_ready_p99_seconds{class="interactive"} 9.1` + "\n",
	} {
		if !strings.Contains(rec.Body.String(), line) {
			t.Errorf("fallback /metrics is missing %q", line)
		}
	}

	// A wired scheduler handler replaces the fallback.
	h = newHarness(t, func(o *Options) {
		o.Metrics = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "from_scheduler 1\n") })
	})
	if got := h.do("GET", "/metrics", "").Body.String(); got != "from_scheduler 1\n" {
		t.Errorf("passthrough /metrics = %q", got)
	}
}

// writeRun records n snapshots, each carrying one event, and a report.
func writeRun(t *testing.T, root, id string, n int, report api.RunReport) {
	t.Helper()
	w, err := recording.Create(root + "/" + id)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		e := api.Event{Seq: int64(i + 1), At: int64(i * 100), Kind: api.EventTransition, Detail: "e"}
		if err := w.Snapshot(api.Snapshot{Seq: int64(i), At: int64(i * 100), Recent: []api.Event{e}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	report.ID = id
	if err := recording.WriteReport(root+"/"+id, report); err != nil {
		t.Fatal(err)
	}
}

func readSnapshots(t *testing.T, r io.Reader) []api.Snapshot {
	t.Helper()
	var out []api.Snapshot
	dec := json.NewDecoder(r)
	for dec.More() {
		var s api.Snapshot
		if err := dec.Decode(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func TestRuns(t *testing.T) {
	h := newHarness(t, nil)
	root := h.srv.o.RunsDir

	rec := h.do("GET", "/v1/runs", "")
	if wantStatus(t, rec, http.StatusOK); strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Errorf("no runs: body = %q, want []", rec.Body.String())
	}

	writeRun(t, root, "split-brain-1", 10, api.RunReport{
		RunSummary: api.RunSummary{Scenario: "split-brain", Seed: 1, Passed: true, StartedAt: "2026-11-12T01:00:00Z"},
		Checks:     []api.CheckResult{{Name: "no violations", Passed: true}},
	})
	writeRun(t, root, "bad-day-2", 3, api.RunReport{
		RunSummary: api.RunSummary{Scenario: "bad-day", Seed: 2, Violations: 4, StartedAt: "2026-11-12T02:00:00Z"},
	})

	runs := decodeBody[[]api.RunSummary](t, h.do("GET", "/v1/runs", ""))
	if len(runs) != 2 || runs[0].ID != "bad-day-2" || runs[1].ID != "split-brain-1" {
		t.Fatalf("runs = %+v, want newest first", runs)
	}
	if runs[0].Passed || runs[0].Violations != 4 || !runs[1].Passed {
		t.Errorf("summaries = %+v", runs)
	}

	rep := decodeBody[api.RunReport](t, h.do("GET", "/v1/runs/split-brain-1", ""))
	if rep.Scenario != "split-brain" || len(rep.Checks) != 1 || !rep.Checks[0].Passed {
		t.Errorf("report = %+v", rep)
	}

	rec = h.do("GET", "/v1/runs/split-brain-1/snapshots", "")
	wantStatus(t, rec, http.StatusOK)
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-ndjson" {
		t.Errorf("Content-Type = %q", ct)
	}
	if got := readSnapshots(t, rec.Body); len(got) != 10 || got[9].Seq != 9 {
		t.Errorf("snapshots: %d lines", len(got))
	}

	// ?max=4 over 10 lines keeps every third line and the last.
	rec = h.do("GET", "/v1/runs/split-brain-1/snapshots?max=4", "", "Accept-Encoding", "gzip")
	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatal("want gzip when the client accepts it")
	}
	gz, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	var seqs []int64
	for _, s := range readSnapshots(t, gz) {
		seqs = append(seqs, s.Seq)
	}
	if want := []int64{0, 3, 6, 9}; !equalInt64(seqs, want) {
		t.Errorf("?max=4 kept snapshots %v, want %v", seqs, want)
	}

	for _, path := range []string{"/v1/runs/nope", "/v1/runs/nope/snapshots", "/v1/runs/%2e%2e", "/v1/runs/%2e%2e/snapshots"} {
		if rec := h.do("GET", path, ""); rec.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", path, rec.Code)
		}
	}
	// A literal ".." never reaches a handler: ServeMux redirects to the
	// cleaned path first. Only the escaped form above tests our own check.
	if rec := h.do("GET", "/v1/runs/..", ""); rec.Code < 300 || rec.Code > 399 {
		t.Errorf("GET /v1/runs/.. = %d, want a redirect to the cleaned path", rec.Code)
	}
	for _, q := range []string{"every=0", "every=x", "max=-3"} {
		if rec := h.do("GET", "/v1/runs/split-brain-1/snapshots?"+q, ""); rec.Code != http.StatusBadRequest {
			t.Errorf("?%s = %d, want 400", q, rec.Code)
		}
	}
}

func equalInt64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Thinning keeps the first and last snapshots and moves the events of every
// skipped snapshot into the next kept one, so playback loses no event.
func TestThinSnapshotsKeepsEveryEvent(t *testing.T) {
	for n := 1; n <= 12; n++ {
		for every := 1; every <= 5; every++ {
			var in strings.Builder
			for i := range n {
				// Snapshot i carries events 2i+1 and 2i+2; snapshot 0 has none.
				s := api.Snapshot{Seq: int64(i), Scheduler: api.SchedulerUp}
				if i > 0 {
					s.Recent = []api.Event{{Seq: int64(2*i + 1)}, {Seq: int64(2*i + 2)}}
				}
				b, _ := json.Marshal(s)
				in.Write(b)
				in.WriteString("\n")
			}
			var out strings.Builder
			if err := thinSnapshots(&out, strings.NewReader(in.String()), every); err != nil {
				t.Fatalf("n=%d every=%d: %v", n, every, err)
			}
			got := readSnapshots(t, strings.NewReader(out.String()))
			wantLines := (n-1)/every + 1
			if (n-1)%every != 0 {
				wantLines++ // the final snapshot is kept as well
			}
			if len(got) != wantLines || got[0].Seq != 0 || got[len(got)-1].Seq != int64(n-1) {
				t.Errorf("n=%d every=%d: kept %d lines (want %d), first %d, last %d",
					n, every, len(got), wantLines, got[0].Seq, got[len(got)-1].Seq)
			}
			next := int64(3)
			for _, s := range got {
				if s.Scheduler != api.SchedulerUp {
					t.Errorf("n=%d every=%d: snapshot %d lost its other fields", n, every, s.Seq)
				}
				for _, e := range s.Recent {
					if e.Seq != next {
						t.Fatalf("n=%d every=%d: event %d at snapshot %d, want event %d", n, every, e.Seq, s.Seq, next)
					}
					next++
				}
				// An event is never shown before the snapshot that recorded it.
				if next-1 > 2*s.Seq+2 {
					t.Errorf("n=%d every=%d: snapshot %d carries a later snapshot's event %d", n, every, s.Seq, next-1)
				}
			}
			if want := int64(2*n + 1); n > 1 && next != want {
				t.Errorf("n=%d every=%d: events up to %d, want up to %d", n, every, next-1, want-1)
			}
		}
	}
}
