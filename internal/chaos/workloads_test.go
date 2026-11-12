package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
)

type workloadBackend struct {
	backend
	snap           api.Snapshot
	deletes        []string
	deleteErrors   []error
	createError    error
	events         []api.Event
	snapshotStates []api.VMState
	deleteDelay    func()
}

func (b *workloadBackend) ControlPlane() api.ControlPlane { return b }
func (b *workloadBackend) Snapshot(context.Context) (api.Snapshot, error) {
	if len(b.snapshotStates) != 0 {
		b.snap.VMs[0].State = b.snapshotStates[0]
		b.snapshotStates = b.snapshotStates[1:]
	}
	return b.snap, nil
}
func (b *workloadBackend) CreateVM(ctx context.Context, key string, q api.CreateVMRequest) (api.CreateVMResponse, error) {
	if b.createError != nil {
		return api.CreateVMResponse{}, b.createError
	}
	b.creates = append(b.creates, q)
	id := fmt.Sprintf("returned-%04d", 1000-len(b.creates))
	b.snap.VMs = append(b.snap.VMs, api.VM{ID: id, State: api.VMBooting})
	return api.CreateVMResponse{ID: id}, nil
}
func (b *workloadBackend) DeleteVM(_ context.Context, id string) (api.VM, error) {
	if b.deleteDelay != nil {
		b.deleteDelay()
	}
	b.deletes = append(b.deletes, id)
	if len(b.deleteErrors) != 0 {
		err := b.deleteErrors[0]
		b.deleteErrors = b.deleteErrors[1:]
		return api.VM{}, err
	}
	return api.VM{}, nil // Deliberately leave the published snapshot stale.
}

func lifetimeState(t *testing.T, seed int64, p api.Lifetime) (*state, *workloadBackend, *ambientClock) {
	t.Helper()
	c := &ambientClock{clock.NewFake(time.Unix(0, 0))}
	b := &workloadBackend{snap: api.Snapshot{Scheduler: api.SchedulerUp, Hosts: []api.Host{{ID: "h"}}}}
	cfg := config.Default()
	s := &state{cfg: cfg, clock: c, wall: c, backend: b, mix: cfg.LoadPriorityMix, previous: c.Now(), emit: func(e api.Event) { b.events = append(b.events, e) }}
	if err := s.sim("seed", api.SimRequest{Seed: seed}); err != nil {
		t.Fatal(err)
	}
	if err := s.sim("chaos", api.SimRequest{Lifetime: &p}); err != nil {
		t.Fatal(err)
	}
	return s, b, c
}

func TestLifetimeStartsAtReadyAndWaitsForRecovery(t *testing.T) {
	for _, recovering := range []api.VMState{api.VMPending, api.VMBooting, api.VMUnhealthy, api.VMLost} {
		t.Run(string(recovering), func(t *testing.T) {
			s, b, c := lifetimeState(t, 7, api.Lifetime{MinMs: 5000, MaxMs: 5000, IdleMs: 7000, ReleaseP: 1})
			ctx := context.Background()
			if err := s.do(ctx, c.Now(), "flood", api.ChaosRequest{Params: map[string]float64{"count": 1}}); err != nil {
				t.Fatal(err)
			}
			id := b.snap.VMs[0].ID
			b.snap.VMs = append(b.snap.VMs, api.VM{ID: "dashboard", State: api.VMReady})
			c.Advance(100 * time.Second)
			s.tick(ctx, c.Now())
			if len(b.deletes) != 0 || len(s.workloadStatus()) != 0 {
				t.Fatal("timer started before Ready")
			}
			b.snap.VMs[0].State = api.VMReady
			s.tick(ctx, c.Now())
			if got := s.workloadStatus(); !reflect.DeepEqual(got, []api.Workload{{VMID: id, Phase: api.WorkloadBusy, EndsAt: 105000}}) {
				t.Fatal(got)
			}
			b.snap.VMs[0].State = recovering
			c.Advance(20 * time.Second)
			s.tick(ctx, c.Now())
			if len(b.deletes) != 0 {
				t.Fatal("deleted recovering VM", b.deletes)
			}
			b.snap.VMs[0].State = api.VMReady
			s.tick(ctx, c.Now())
			s.tick(ctx, c.Now())
			if !reflect.DeepEqual(b.deletes, []string{id}) {
				t.Fatal("wrong or replayed delete", b.deletes)
			}
			var endings []api.Event
			for _, e := range b.events {
				if e.Kind == api.EventWorkload {
					endings = append(endings, e)
				}
			}
			if len(endings) != 1 || endings[0].VMID != id || endings[0].Detail != "completed" || endings[0].At != 120000 {
				t.Fatal(endings)
			}
		})
	}
}

func TestLifetimeReleaseOrIdle(t *testing.T) {
	for _, release := range []float64{0, 1} {
		t.Run(fmt.Sprint(release), func(t *testing.T) {
			s, b, c := lifetimeState(t, 19, api.Lifetime{MinMs: 5000, MaxMs: 5000, IdleMs: 7000, ReleaseP: release})
			ctx := context.Background()
			if err := s.create(ctx); err != nil {
				t.Fatal(err)
			}
			b.snap.VMs[0].State = api.VMReady
			s.tick(ctx, c.Now())
			c.Advance(4999 * time.Millisecond)
			s.tick(ctx, c.Now())
			if len(b.deletes) != 0 {
				t.Fatal("early completion")
			}
			c.Advance(time.Millisecond)
			s.tick(ctx, c.Now())
			if release == 0 {
				got := s.workloadStatus()
				if len(b.deletes) != 0 || len(got) != 1 || got[0].Phase != api.WorkloadIdle || got[0].EndsAt != 12000 {
					t.Fatal(got, b.deletes)
				}
				c.Advance(6999 * time.Millisecond)
				s.tick(ctx, c.Now())
				if len(b.deletes) != 0 {
					t.Fatal("early idle ending")
				}
				b.snap.VMs[0].State = api.VMUnhealthy
				c.Advance(time.Millisecond)
				s.tick(ctx, c.Now())
				if len(b.deletes) != 0 {
					t.Fatal("deleted unhealthy idle VM")
				}
				b.snap.VMs[0].State = api.VMReady
				s.tick(ctx, c.Now())
			}
			want := "completed"
			if release == 0 {
				want = "idle"
			}
			if len(b.deletes) != 1 || len(b.events) != 1 || b.events[0].Kind != api.EventWorkload || b.events[0].Detail != want || len(s.workloadStatus()) != 0 {
				t.Fatal(b.deletes, b.events)
			}
		})
	}
}

func TestLifetimeRetryAndForget(t *testing.T) {
	for _, status := range []int{429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s, b, c := lifetimeState(t, 1, api.Lifetime{MinMs: 5000, MaxMs: 5000, IdleMs: 5000, ReleaseP: 1})
			ctx := context.Background()
			if err := s.create(ctx); err != nil {
				t.Fatal(err)
			}
			b.snap.VMs[0].State = api.VMReady
			s.tick(ctx, c.Now())
			b.deleteErrors = []error{&api.Error{Status: status, RetryAfterS: 3}}
			b.deleteDelay = func() { c.Advance(2 * time.Second) }
			c.Advance(5 * time.Second)
			s.tick(ctx, c.Now())
			c.Advance(2999 * time.Millisecond)
			s.tick(ctx, c.Now())
			if len(b.deletes) != 1 || len(b.events) != 0 {
				t.Fatal("retry before Retry-After or false ending", b.deletes, b.events)
			}
			c.Advance(time.Millisecond)
			s.tick(ctx, c.Now())
			for range 5 {
				c.Advance(time.Second)
				s.tick(ctx, c.Now())
			}
			if len(b.deletes) != 2 || len(b.events) != 1 || b.events[0].At != 12000 {
				t.Fatal("success was lost or replayed", b.deletes, b.events)
			}
		})
	}
	for _, terminal := range []api.VMState{api.VMFailed, api.VMTerminated} {
		s, b, c := lifetimeState(t, 1, api.Lifetime{MinMs: 5000, MaxMs: 5000, IdleMs: 5000, ReleaseP: 1})
		ctx := context.Background()
		if err := s.create(ctx); err != nil {
			t.Fatal(err)
		}
		b.snap.VMs[0].State = terminal
		s.tick(ctx, c.Now())
		b.snap.VMs[0].State = api.VMReady
		c.Advance(time.Hour)
		s.tick(ctx, c.Now())
		if len(b.deletes) != 0 || len(s.workloadStatus()) != 0 {
			t.Fatal("terminal VM retained", terminal)
		}
	}
}

func TestLifetimeSeedAndChaosIsolation(t *testing.T) {
	run := func(seed int64, enabled bool) ([]byte, []api.Event) {
		p := api.Lifetime{}
		if enabled {
			p = api.Lifetime{MinMs: 5000, MaxMs: 17000, IdleMs: 7000, ReleaseP: .45}
		}
		s, b, c := lifetimeState(t, seed, p)
		one, rate := 1.0, 1.0
		if err := s.sim("chaos", api.SimRequest{Intensity: &one, LoadRate: &rate, Filters: []string{"kill-host"}}); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		firstSeen := map[string]bool{}
		durations := map[int64]bool{}
		for range 600 {
			for i := range b.snap.VMs {
				b.snap.VMs[i].State = api.VMReady
			}
			c.Advance(100 * time.Millisecond)
			s.tick(ctx, c.Now())
			for _, w := range s.workloadStatus() {
				if !firstSeen[w.VMID] {
					firstSeen[w.VMID] = true
					d := w.EndsAt - c.Now().UnixMilli()
					if d < p.MinMs || d > p.MaxMs {
						t.Fatal("busy duration outside policy", d)
					}
					durations[d] = true
				}
			}
		}
		if enabled && len(durations) < 5 {
			t.Fatal("busy durations not sampled", durations)
		}
		var endings []api.Event
		for _, e := range b.events {
			if e.Kind == api.EventWorkload {
				endings = append(endings, e)
			}
		}
		if !enabled && len(b.deletes) != 0 {
			t.Fatal("off deleted VMs", b.deletes)
		}
		sequence, err := json.Marshal(struct {
			Faults  []string
			Creates []api.CreateVMRequest
		}{b.faults, b.creates})
		if err != nil {
			t.Fatal(err)
		}
		return sequence, endings
	}
	off, _ := run(77, false)
	on, endings := run(77, true)
	again, repeated := run(77, true)
	_, other := run(78, true)
	if string(off) != string(on) || string(on) != string(again) {
		t.Fatal("lifetime draws perturbed chaos/load sequence")
	}
	if len(endings) < 5 || !reflect.DeepEqual(endings, repeated) || reflect.DeepEqual(endings, other) {
		t.Fatal("seed does not control endings", endings, repeated, other)
	}
	completed, idle := 0, 0
	for _, e := range endings {
		if e.Detail == "completed" {
			completed++
		}
		if e.Detail == "idle" {
			idle++
		}
	}
	if completed == 0 || idle == 0 {
		t.Fatal("release probability not sampled", completed, idle)
	}
}

func TestLifetimeBoundsStatusAndReset(t *testing.T) {
	cfg := config.Default()
	minMs, maxMs := cfg.LifetimeMin.Milliseconds(), cfg.LifetimeMax.Milliseconds()
	valid := api.Lifetime{MinMs: minMs, MaxMs: maxMs, IdleMs: minMs, ReleaseP: .6}
	for _, field := range []string{"min", "max", "order", "idle-low", "idle-high", "prob-low", "prob-high", "nan", "inf"} {
		p := valid
		switch field {
		case "min":
			p.MinMs--
		case "max":
			p.MaxMs++
		case "order":
			p.MinMs = p.MaxMs + 1
		case "idle-low":
			p.IdleMs--
		case "idle-high":
			p.IdleMs = maxMs + 1
		case "prob-low":
			p.ReleaseP = -.01
		case "prob-high":
			p.ReleaseP = 1.01
		case "nan":
			p.ReleaseP = math.NaN()
		case "inf":
			p.ReleaseP = math.Inf(1)
		}
		s, _, _ := lifetimeState(t, 1, valid)
		if err := s.sim("chaos", api.SimRequest{Lifetime: &p}); err == nil {
			t.Fatal("accepted invalid", field)
		} else if e, ok := err.(*api.Error); !ok || e.Status != 422 {
			t.Fatal(err)
		}
		if s.status.Lifetime != valid {
			t.Fatal("invalid update changed policy")
		}
	}
	for _, action := range []string{"off", "reset"} {
		s, b, c := lifetimeState(t, 1, valid)
		ctx := context.Background()
		if err := s.flood(ctx, api.ChaosRequest{Params: map[string]float64{"count": 3}}); err != nil {
			t.Fatal(err)
		}
		for i := range b.snap.VMs {
			b.snap.VMs[i].State = api.VMReady
		}
		s.tick(ctx, c.Now())
		got := s.handle(c.Now(), request{kind: "sim", action: "status"}).status
		if got.Lifetime != valid || len(got.Workloads) != 3 || !slices.IsSortedFunc(got.Workloads, func(a, b api.Workload) int { return strings.Compare(a.VMID, b.VMID) }) {
			t.Fatal(got)
		}
		got.Workloads[0].VMID = "corrupted"
		if s.workloadStatus()[0].VMID == "corrupted" {
			t.Fatal("status aliases actor state")
		}
		if action == "off" {
			p := api.Lifetime{}
			if err := s.sim("chaos", api.SimRequest{Lifetime: &p}); err != nil {
				t.Fatal(err)
			}
		} else if err := s.reset(ctx); err != nil {
			t.Fatal(err)
		}
		c.Advance(time.Hour)
		s.tick(ctx, c.Now())
		if s.status.Lifetime.MaxMs != 0 || len(s.workloadStatus()) != 0 || len(b.deletes) != 0 {
			t.Fatal("off/reset retained workloads")
		}
	}
}

func TestLifetimeTracksOnlyNewLoadAndSurvivesMissingVM(t *testing.T) {
	ctx := context.Background()
	s, b, c := lifetimeState(t, 3, api.Lifetime{})
	if err := s.create(ctx); err != nil {
		t.Fatal(err)
	} // Load before enabling is not enrolled.
	p := api.Lifetime{MinMs: 5000, MaxMs: 5000, IdleMs: 7000, ReleaseP: 1}
	if err := s.sim("chaos", api.SimRequest{Lifetime: &p}); err != nil {
		t.Fatal(err)
	}
	b.snap.VMs = append(b.snap.VMs, api.VM{ID: "api-created", State: api.VMReady})
	if err := s.create(ctx); err != nil {
		t.Fatal(err)
	}
	for i := range b.snap.VMs {
		b.snap.VMs[i].State = api.VMReady
	}
	s.tick(ctx, c.Now())
	owned := b.snap.VMs[len(b.snap.VMs)-1]
	b.snap.VMs = b.snap.VMs[:len(b.snap.VMs)-1]
	c.Advance(time.Minute)
	s.tick(ctx, c.Now())
	if len(b.deletes) != 0 {
		t.Fatal("deleted missing or foreign VM", b.deletes)
	}
	b.snap.VMs = append(b.snap.VMs, owned)
	s.tick(ctx, c.Now())
	if !slices.Equal(b.deletes, []string{owned.ID}) {
		t.Fatal("timer lost across missing inventory", b.deletes)
	}
}

func TestLifetimeRechecksReadyImmediatelyBeforeDelete(t *testing.T) {
	s, b, c := lifetimeState(t, 1, api.Lifetime{MinMs: 5000, MaxMs: 5000, IdleMs: 5000, ReleaseP: 1})
	ctx := context.Background()
	if err := s.create(ctx); err != nil {
		t.Fatal(err)
	}
	b.snap.VMs[0].State = api.VMReady
	s.tick(ctx, c.Now())
	c.Advance(5 * time.Second)
	b.snapshotStates = []api.VMState{api.VMReady, api.VMUnhealthy}
	s.tick(ctx, c.Now())
	if len(b.deletes) != 0 || len(b.events) != 0 {
		t.Fatal("used an earlier Ready observation", b.deletes, b.events)
	}
	b.snap.VMs[0].State = api.VMReady
	s.tick(ctx, c.Now())
	s.tick(ctx, c.Now())
	if len(b.deletes) != 1 || len(b.events) != 1 || b.events[0].Kind != api.EventWorkload || b.events[0].VMID != b.deletes[0] || b.events[0].Detail != "completed" {
		t.Fatal(b.deletes, b.events)
	}
}
