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
	return nil
}
func (b *backend) Snapshot(context.Context) (api.Snapshot, error) {
	return api.Snapshot{Racks: []api.Rack{{ID: "r1"}, {ID: "r2"}, {ID: "r3"}}, Hosts: []api.Host{{ID: "h1"}, {ID: "h2"}, {ID: "h3"}}, VMs: []api.VM{{ID: "v1"}, {ID: "v2"}, {ID: "v3"}}}, nil
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
func TestRateLimitUsesIPNotPortAndWallTime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := clock.NewScaled(time.Unix(0, 0), 1)
	c.Pause()
	e := New(ctx, config.Default(), c, &backend{}, 42, nil)
	defer func() { cancel(); e.Close() }()
	if _, err := e.DoFromIP(ctx, "192.0.2.1:1000", "kill-host", api.ChaosRequest{Target: "h1"}); err != nil {
		t.Fatal(err)
	}
	c.Step(time.Hour)
	if _, err := e.DoFromIP(ctx, "192.0.2.1:2000", "kill-host", api.ChaosRequest{Target: "h1"}); err == nil {
		t.Fatal("IP bypassed through port or sim time")
	}
	if _, err := e.DoFromIP(ctx, "192.0.2.2:1000", "kill-host", api.ChaosRequest{Target: "h1"}); err != nil {
		t.Fatal("different IP rejected", err)
	}
}
func TestSeedFiltersLoadAndAttacker(t *testing.T) {
	run := func() ([]string, []api.CreateVMRequest, []string) {
		ctx := context.Background()
		cfg := config.Default()
		c := clock.NewScaled(time.Unix(0, 0), 1)
		c.Pause()
		b := &backend{}
		s := &state{cfg: cfg, clock: c, wall: clock.NewFake(time.Unix(0, 0)), backend: b, status: api.SimStatus{Seed: 17}, last: map[string]time.Time{}, attackers: map[string]attacker{}, mix: cfg.LoadPriorityMix, previous: c.Now()}
		one := 1.0
		rate := 2.5
		if err := s.sim("chaos", api.SimRequest{Seed: 17, Intensity: &one, Filters: []string{"kill-host"}, LoadRate: &rate}); err != nil {
			t.Fatal(err)
		}
		if err := s.do(ctx, c.Now(), "compromise", api.ChaosRequest{Target: "v1"}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 1000; i++ {
			c.Step(cfg.AmbientEvery)
			s.tick(ctx, c.Now())
		}
		return b.faults, b.creates, b.attacks
	}
	faults, creates, attacks := run()
	f2, c2, a2 := run()
	if !reflect.DeepEqual(faults, f2) || !reflect.DeepEqual(creates, c2) || !reflect.DeepEqual(attacks, a2) {
		t.Fatal("seed did not reproduce decisions")
	}
	if len(creates) != 250 {
		t.Fatalf("2.5/s for 100s made %d", len(creates))
	}
	if len(faults) < 2 {
		t.Fatal("ambient never fired")
	}
	for _, f := range faults {
		if !strings.HasPrefix(f, "kill-host:") && !strings.HasPrefix(f, "compromise:") {
			t.Fatal("filter escaped", f)
		}
	}
	want := []string{api.AttackImpersonate, api.AttackReplay, api.AttackFlood, api.AttackEastWest, api.AttackControl, api.AttackEgress, api.AttackDiskFill}
	if len(attacks) < len(want) || !reflect.DeepEqual(attacks[:len(want)], want) {
		t.Fatal("incorrect attack cycle", attacks)
	}
}
