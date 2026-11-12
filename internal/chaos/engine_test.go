package chaos

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
)

type controlPlane = api.ControlPlane
type backend struct {
	controlPlane
	faults  []string
	creates []api.CreateVMRequest
	attacks []string
	reset   chan struct{}
}

func (b *backend) Apply(_ context.Context, a string, q api.ChaosRequest) error {
	b.faults = append(b.faults, a+":"+q.Target)
	return nil
}
func (b *backend) Attack(_ context.Context, id, a string) error {
	b.attacks = append(b.attacks, a)
	return nil
}
func (b *backend) Topology(_ context.Context, a string, q api.TopologyRequest) error {
	b.faults = append(b.faults, a)
	return nil
}
func (b *backend) Reset(context.Context) error {
	b.faults = nil
	b.creates = nil
	b.attacks = nil
	if b.reset != nil {
		select {
		case b.reset <- struct{}{}:
		default:
		}
	}
	return nil
}
func (b *backend) Snapshot(context.Context) (api.Snapshot, error) {
	return api.Snapshot{Racks: []api.Rack{{ID: "r1"}, {ID: "r2"}, {ID: "r3"}}, Hosts: []api.Host{{ID: "h1", Rack: "r1"}, {ID: "h2", Rack: "r2"}, {ID: "h3", Rack: "r3"}}, VMs: []api.VM{{ID: "v1", Rack: "r1"}, {ID: "v2", Rack: "r2"}, {ID: "v3", Rack: "r3"}}}, nil
}
func (b *backend) ControlPlane() api.ControlPlane { return b }
func (b *backend) CreateVM(_ context.Context, _ string, q api.CreateVMRequest) (api.CreateVMResponse, error) {
	b.creates = append(b.creates, q)
	return api.CreateVMResponse{ID: "new"}, nil
}
func (b *backend) DeleteVM(context.Context, string) (api.VM, error) {
	return api.VM{State: api.VMTerminated}, nil
}

func TestEntireCatalogDispatches(t *testing.T) {
	ctx := context.Background()
	if len(catalogue) != 26 || len(presets) != 6 {
		t.Fatal("catalogue is incomplete")
	}
	for _, a := range append(append(append([]api.ChaosAction{}, catalogue...), recoveryActions...), presets...) {
		t.Run(a.Name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			c := clock.NewScaled(time.Unix(0, 0), 1)
			c.Pause()
			b := &backend{}
			e := New(ctx, config.Default(), c, b, 42, nil)
			defer func() { cancel(); e.Close() }()
			if _, err := e.Do(ctx, a.Name, api.ChaosRequest{Target: "v1"}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestSeedFiltersAndLoad(t *testing.T) {
	run := func() ([]string, []api.CreateVMRequest) {
		ctx := context.Background()
		cfg := config.Default()
		c := clock.NewScaled(time.Unix(0, 0), 1)
		c.Pause()
		b := &backend{}
		s := &state{cfg: cfg, clock: c, wall: clock.NewFake(time.Unix(0, 0)), backend: b, mix: cfg.LoadPriorityMix, previous: c.Now()}
		one := 1.0
		rate := 2.5
		if err := s.sim("seed", api.SimRequest{Seed: 17}); err != nil {
			t.Fatal(err)
		}
		if err := s.sim("chaos", api.SimRequest{Intensity: &one, Filters: []string{"kill-host", "wipe-store"}, Racks: []string{"r2"}, LoadRate: &rate}); err != nil {
			t.Fatal(err)
		}
		if err := s.do(ctx, c.Now(), "compromise", api.ChaosRequest{Target: "v1"}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 1000; i++ {
			c.Step(cfg.AmbientEvery)
			s.tick(ctx, c.Now())
		}
		return b.faults, b.creates
	}
	faults, creates := run()
	f2, c2 := run()
	if !reflect.DeepEqual(faults, f2) || !reflect.DeepEqual(creates, c2) {
		t.Fatal("seed did not reproduce decisions")
	}
	if len(creates) != 250 {
		t.Fatalf("2.5/s for 100s made %d", len(creates))
	}
	if len(faults) < 2 {
		t.Fatal("ambient never fired")
	}
	for _, f := range faults {
		if f != "kill-host:h2" && !strings.HasPrefix(f, "compromise:") {
			t.Fatal("filter escaped", f)
		}
	}
}

func TestExplicitZeroSeedAndSnapshotOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := clock.NewScaled(time.Unix(0, 0), 1)
	c.Pause()
	e := New(ctx, config.Default(), c, &backend{}, 42, nil)
	defer func() { cancel(); e.Close() }()
	s, err := e.Sim(ctx, "seed", api.SimRequest{Seed: 0})
	if err != nil || s.Seed != 0 {
		t.Fatal(s, err)
	}
	s, err = e.Sim(ctx, "chaos", api.SimRequest{Seed: 99, Racks: []string{"r2"}, Filters: []string{"kill-host"}})
	if err != nil || s.Seed != 0 {
		t.Fatal("settings implicitly reseeded", s, err)
	}
	s.Racks[0] = "r1"
	s.Filters[0] = "wipe-store"
	s, err = e.Status(ctx)
	if err != nil || s.Racks[0] != "r2" || s.Filters[0] != "kill-host" {
		t.Fatal("status aliases actor state", s, err)
	}
}

func TestIdleResetUsesWallTimeWhilePaused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c := clock.NewScaled(time.Unix(0, 0), 1)
	c.Pause()
	cfg := config.Default()
	cfg.IdleResetAfter = 20 * time.Millisecond
	b := &backend{reset: make(chan struct{}, 1)}
	e := New(ctx, cfg, c, b, 42, nil)
	defer func() { cancel(); e.Close() }()
	select {
	case <-b.reset:
	case <-ctx.Done():
		t.Fatal("paused simulation disabled idle reset")
	}
	status, err := e.Status(ctx)
	if err != nil || status.Seed != 42 || !status.Paused {
		t.Fatal(status, err)
	}
}
