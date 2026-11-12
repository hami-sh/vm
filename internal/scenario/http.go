package scenario

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/hami-sh/vm/internal/api"
)

// HTTPSystem adapts the public API to the same contracts as an in-process
// fleet. Each instance belongs to an isolated server process, not the demo.
type HTTPSystem struct {
	URL       string
	Client    *http.Client
	mu        sync.Mutex
	latest    api.Snapshot
	snapshots chan api.Snapshot
	events    chan api.Event
	cancel    context.CancelFunc
}

func (f *HTTPSystem) request(ctx context.Context, method, path, key string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	r, err := http.NewRequestWithContext(ctx, method, f.URL+path, body)
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	resp, err := f.Client.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		var detail api.ErrorResponse
		if json.Unmarshal(b, &detail) == nil && detail.Error != "" {
			return &api.Error{Status: resp.StatusCode, Msg: detail.Error, RetryAfterS: detail.RetryAfterS}
		}
		return &api.Error{Status: resp.StatusCode, Msg: string(b)}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
func (f *HTTPSystem) CreateVM(ctx context.Context, key string, r api.CreateVMRequest) (v api.CreateVMResponse, e error) {
	e = f.request(ctx, "POST", "/v1/vms", key, r, &v)
	return
}
func (f *HTTPSystem) GetVM(ctx context.Context, id string) (v api.VM, e error) {
	e = f.request(ctx, "GET", "/v1/vms/"+id, "", nil, &v)
	return
}
func (f *HTTPSystem) ListVMs(ctx context.Context) (v []api.VM, e error) {
	e = f.request(ctx, "GET", "/v1/vms", "", nil, &v)
	return
}
func (f *HTTPSystem) DeleteVM(ctx context.Context, id string) (v api.VM, e error) {
	e = f.request(ctx, "DELETE", "/v1/vms/"+id, "", nil, &v)
	return
}
func (f *HTTPSystem) ListHosts(ctx context.Context) (v []api.Host, e error) {
	e = f.request(ctx, "GET", "/v1/hosts", "", nil, &v)
	return
}
func (f *HTTPSystem) Queue(ctx context.Context) (v []api.QueueEntry, e error) {
	e = f.request(ctx, "GET", "/v1/queue", "", nil, &v)
	return
}
func (f *HTTPSystem) SetPriority(ctx context.Context, id string, p api.Priority) error {
	name := map[api.Priority]string{api.PriorityInteractive: "interactive", api.PriorityStandard: "standard", api.PriorityBatch: "batch"}[p]
	return f.request(ctx, "POST", "/v1/queue/"+id+"/priority", "", api.SetPriorityRequest{Priority: name}, nil)
}
func (f *HTTPSystem) Catalog() api.ChaosCatalog {
	var v api.ChaosCatalog
	_ = f.request(context.Background(), "GET", "/v1/chaos/actions", "", nil, &v)
	return v
}
func (f *HTTPSystem) Do(ctx context.Context, a string, r api.ChaosRequest) (v api.ChaosResult, e error) {
	e = f.request(ctx, "POST", "/v1/chaos/"+a, "", r, &v)
	return
}
func (f *HTTPSystem) Sim(ctx context.Context, a string, r api.SimRequest) (v api.SimStatus, e error) {
	e = f.request(ctx, "POST", "/v1/sim/"+a, "", r, &v)
	return
}
func (f *HTTPSystem) Topology(ctx context.Context, a string, r api.TopologyRequest) (v api.ChaosResult, e error) {
	e = f.request(ctx, "POST", "/v1/topology/"+a, "", r, &v)
	return
}
func (f *HTTPSystem) Latest() api.Snapshot { f.mu.Lock(); defer f.mu.Unlock(); return f.latest }
func (f *HTTPSystem) Subscribe() (<-chan api.Snapshot, <-chan api.Event, func()) {
	return f.snapshots, f.events, f.cancel
}

// Connect validates a complete SSE snapshot before returning. A health-only
// stub fails here rather than silently producing a successful empty run.
func Connect(ctx context.Context, url string) (*HTTPSystem, error) {
	ctx, cancel := context.WithCancel(ctx)
	f := &HTTPSystem{URL: url, Client: &http.Client{}, snapshots: make(chan api.Snapshot, 4096), events: make(chan api.Event, 4096), cancel: cancel}
	r, err := http.NewRequestWithContext(ctx, "GET", url+"/v1/events", nil)
	if err != nil {
		cancel()
		return nil, err
	}
	resp, err := f.Client.Do(r)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("SSE: status %d content-type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	ready := make(chan error, 1)
	go func() {
		defer close(f.snapshots)
		defer close(f.events)
		defer resp.Body.Close()
		first := true
		err := ReadSSE(resp.Body, func(kind string, data []byte) error {
			switch kind {
			case "snapshot":
				var s api.Snapshot
				if err := DecodeSnapshot(data, &s); err != nil {
					return err
				}
				f.mu.Lock()
				f.latest = s
				f.mu.Unlock()
				if first {
					ready <- nil
					first = false
				}
				select {
				case f.snapshots <- s:
				case <-ctx.Done():
					return ctx.Err()
				}
			case "event":
				var ev api.Event
				if err := json.Unmarshal(data, &ev); err != nil {
					return err
				}
				select {
				case f.events <- ev:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
		if first {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			ready <- err
		}
	}()
	select {
	case err := <-ready:
		if err != nil {
			cancel()
			return nil, err
		}
		return f, nil
	case <-ctx.Done():
		cancel()
		return nil, ctx.Err()
	}
}

func DecodeSnapshot(data []byte, s *api.Snapshot) error {
	// Required keys catch an Event or {} mislabelled as a Snapshot; the
	// strict decoder catches incompatible field names at the UI boundary.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, key := range []string{"seq", "at", "scheduler", "sim", "racks", "hosts", "vms", "counters", "invariants", "security"} {
		if _, ok := fields[key]; !ok {
			return fmt.Errorf("snapshot missing %s", key)
		}
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	return d.Decode(s)
}

// ReadSSE handles comments, CRLF and multiline data according to SSE framing.
func ReadSSE(r io.Reader, emit func(string, []byte) error) error {
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 4096), 4<<20)
	kind := "message"
	var data []string
	for scan.Scan() {
		line := scan.Text()
		if line == "" {
			if len(data) > 0 {
				if err := emit(kind, []byte(strings.Join(data, "\n"))); err != nil {
					return err
				}
			}
			kind = "message"
			data = nil
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			kind = value
		case "data":
			data = append(data, value)
		}
	}
	return scan.Err()
}
