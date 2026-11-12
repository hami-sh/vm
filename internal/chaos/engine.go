package chaos

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"time"

	"github.com/hami-sh/vm/internal/actor"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
)

// Backend is the integration seam, not a second control plane. Fleet maps it
// onto the scheduler/agent/driver/network fault methods in one place.
type Backend interface {
	Apply(context.Context, string, api.ChaosRequest) error
	Attack(context.Context, string, string) error
	Topology(context.Context, string, api.TopologyRequest) error
	Reset(context.Context) error
	Snapshot(context.Context) (api.Snapshot, error)
	ControlPlane() api.ControlPlane
}
type request struct {
	ctx          context.Context
	kind, action string
	chaos        api.ChaosRequest
	sim          api.SimRequest
	topology     api.TopologyRequest
	reply        chan<- response
}
type response struct {
	result api.ChaosResult
	status api.SimStatus
	err    error
}
type scheduled struct {
	at     time.Time
	action string
	req    api.ChaosRequest
}
type Engine struct {
	ctx   context.Context
	inbox chan request
	done  chan struct{}
}
type state struct {
	cfg      config.Config
	clock    clock.Controllable
	wall     clock.Clock
	backend  Backend
	emit     func(api.Event)
	status   api.SimStatus
	rng      *rand.Rand
	idle     time.Time
	mix      [3]float64
	credit   float64
	previous time.Time
	tasks    []scheduled
	serial   uint64
}

func New(ctx context.Context, cfg config.Config, c clock.Controllable, b Backend, seed int64, emit func(api.Event)) *Engine {
	e := &Engine{ctx: ctx, inbox: make(chan request), done: make(chan struct{})}
	s := &state{cfg: cfg, clock: c, wall: clock.Real{}, backend: b, emit: emit, status: api.SimStatus{Seed: seed}, rng: rand.New(rand.NewSource(seed)), mix: cfg.LoadPriorityMix, previous: c.Now()}
	s.idle = s.wall.Now()
	go func() {
		defer close(e.done)
		tick := c.NewTicker(cfg.AmbientEvery)
		defer tick.Stop()
		idle := s.wall.NewTimer(cfg.IdleResetAfter)
		defer idle.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case r := <-e.inbox:
				r.reply <- s.handle(c.Now(), r)
			case <-tick.C():
				s.tick(ctx, c.Now())
			case <-idle.C():
				remaining := cfg.IdleResetAfter - s.wall.Now().Sub(s.idle)
				if remaining <= 0 {
					if err := s.reset(ctx); err != nil {
						s.event("reset failed: "+err.Error(), "")
					}
					remaining = cfg.IdleResetAfter
				}
				idle.Reset(remaining)
			}
		}
	}()
	return e
}
func (e *Engine) Close() { <-e.done }
func (e *Engine) Catalog() api.ChaosCatalog {
	// Copy nested slices too: callers must never mutate the registry.
	a := append(slices.Clone(catalogue), recoveryActions...)
	for i := range a {
		a[i].Params = slices.Clone(a[i].Params)
	}
	return api.ChaosCatalog{Actions: a, Presets: slices.Clone(presets)}
}
func (e *Engine) ask(ctx context.Context, r request) (response, error) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(e.ctx, cancel)
	defer stop()
	defer cancel()
	r.ctx = ctx
	select {
	case <-e.done:
		return response{}, context.Canceled
	default:
	}
	return actor.Ask(ctx, e.inbox, func(ch chan<- response) request { r.reply = ch; return r })
}
func (e *Engine) Do(ctx context.Context, a string, r api.ChaosRequest) (api.ChaosResult, error) {
	v, err := e.ask(ctx, request{kind: "do", action: a, chaos: r})
	if err != nil {
		return v.result, err
	}
	return v.result, v.err
}

func (e *Engine) Sim(ctx context.Context, a string, r api.SimRequest) (api.SimStatus, error) {
	v, err := e.ask(ctx, request{kind: "sim", action: a, sim: r})
	if err != nil {
		return v.status, err
	}
	return v.status, v.err
}
func (e *Engine) Status(ctx context.Context) (api.SimStatus, error) {
	return e.Sim(ctx, "status", api.SimRequest{})
}
func (e *Engine) Topology(ctx context.Context, a string, r api.TopologyRequest) (api.ChaosResult, error) {
	v, err := e.ask(ctx, request{kind: "topology", action: a, topology: r})
	if err != nil {
		return v.result, err
	}
	return v.result, v.err
}
func invalid(msg string) error { return &api.Error{Status: 422, Msg: msg} }
func finite(x float64) bool    { return !math.IsNaN(x) && !math.IsInf(x, 0) }
func (s *state) handle(now time.Time, r request) response {
	var err error
	switch r.kind {
	case "do":
		err = s.do(r.ctx, now, r.action, r.chaos)
	case "sim":
		err = s.sim(r.action, r.sim)
	case "topology":
		if r.action != "add-rack" && r.action != "add-host" && r.action != "drain" {
			err = invalid("unknown topology action")
		} else if r.topology.Hosts < 0 {
			err = invalid("negative host count")
		} else {
			err = s.backend.Topology(r.ctx, r.action, r.topology)
		}
	}
	s.status.Speed = s.clock.Speed()
	s.status.Paused = s.clock.Paused()
	status := s.status
	status.Filters = slices.Clone(status.Filters)
	status.Racks = slices.Clone(status.Racks)
	return response{result: api.ChaosResult{Action: r.action, Target: r.chaos.Target, Detail: "applied"}, status: status, err: err}
}
func (s *state) sim(action string, r api.SimRequest) error {
	switch action {
	case "status":
	case "seed":
		s.status.Seed = r.Seed
		s.rng = rand.New(rand.NewSource(r.Seed))
	case "pause":
		s.clock.Pause()
	case "play":
		s.clock.Play()
	case "speed":
		if !finite(r.Speed) || r.Speed <= 0 || r.Speed > s.cfg.MaxSpeed {
			return invalid("speed outside configured range")
		}
		s.clock.SetSpeed(r.Speed)
	case "step":
		if !s.clock.Paused() || r.StepMs <= 0 || r.StepMs > math.MaxInt64/int64(time.Millisecond) {
			return invalid("step requires paused clock and positive duration")
		}
		if c, ok := s.clock.(interface{ Step(time.Duration) }); ok {
			c.Step(time.Duration(r.StepMs) * time.Millisecond)
		} else if c, ok := s.clock.(interface{ Advance(time.Duration) }); ok {
			c.Advance(time.Duration(r.StepMs) * time.Millisecond)
		} else {
			return invalid("clock cannot step")
		}
	case "chaos":
		if r.Intensity != nil && (!finite(*r.Intensity) || *r.Intensity < 0 || *r.Intensity > 1) {
			return invalid("intensity outside 0..1")
		}
		if r.LoadRate != nil && (!finite(*r.LoadRate) || *r.LoadRate < 0 || *r.LoadRate > float64(s.cfg.MaxVMs)) {
			return invalid("invalid load rate")
		}
		for _, f := range r.Filters {
			if !slices.ContainsFunc(catalogue, func(a api.ChaosAction) bool { return a.Name == f && a.Ambient }) {
				return invalid("unknown ambient filter: " + f)
			}
		}
		if r.Intensity != nil {
			s.status.Intensity = *r.Intensity
		}
		if r.LoadRate != nil {
			s.status.LoadRate = *r.LoadRate
		}
		if r.Filters != nil {
			s.status.Filters = slices.Clone(r.Filters)
		}
		if r.Racks != nil {
			s.status.Racks = slices.Clone(r.Racks)
		}
	default:
		return invalid("unknown sim action")
	}
	return nil
}
func (s *state) event(detail, target string) {
	if s.emit != nil {
		s.emit(api.Event{At: s.clock.Now().UnixMilli(), Kind: api.EventChaos, Detail: detail + " " + target})
	}
}
func (s *state) reset(ctx context.Context) error {
	if err := s.backend.Reset(ctx); err != nil {
		return err
	}
	s.tasks = nil
	s.credit = 0
	s.status.Intensity = 0
	s.status.LoadRate = 0
	s.status.Scenario = ""
	s.previous = s.clock.Now()
	s.rng = rand.New(rand.NewSource(s.status.Seed))
	s.idle = s.wall.Now()
	s.event("reset", "")
	return nil
}
func (s *state) do(ctx context.Context, now time.Time, action string, r api.ChaosRequest) error {
	if !slices.ContainsFunc(append(append(slices.Clone(catalogue), recoveryActions...), presets...), func(a api.ChaosAction) bool { return a.Name == action }) {
		return invalid("unknown chaos action: " + action)
	}
	for _, x := range r.Params {
		if !finite(x) {
			return invalid("non-finite parameter")
		}
	}
	var err error
	switch action {
	case "reset":
		return s.reset(ctx)
	case "noise":
		x := r.Params["intensity"]
		err = s.sim("chaos", api.SimRequest{Intensity: &x})
	case "sustained-load":
		rate := r.Params["rate"]
		mix := [3]float64{r.Params["interactive"], r.Params["standard"], r.Params["batch"]}
		if mix[0] < 0 || mix[1] < 0 || mix[2] < 0 {
			return invalid("negative priority weight")
		}
		if err = s.sim("chaos", api.SimRequest{LoadRate: &rate}); err != nil {
			return err
		}
		if mix[0]+mix[1]+mix[2] > 0 {
			s.mix = mix
		}
	case "flood":
		err = s.flood(ctx, r)
	case "delete":
		_, err = s.backend.ControlPlane().DeleteVM(ctx, r.Target)
	case "impostor-register":
		err = s.backend.Attack(ctx, r.Target, api.AttackImpersonate)
	case "add-rack", "add-host", "remove-rack", "drain":
		err = s.backend.Topology(ctx, action, api.TopologyRequest{Rack: r.Target, Hosts: int(r.Params["hosts"])})
	case "split-brain", "gray-failure", "thundering-herd", "rolling-maintenance", "hostile-tenant", "bad-day":
		err = s.preset(ctx, now, action)
	case "burst-after-rack-loss":
		err = s.backend.Apply(ctx, "power-off-rack", r)
		if err == nil {
			err = s.flood(ctx, r)
		}
	default:
		err = s.backend.Apply(ctx, action, r)
	}
	if err == nil {
		s.idle = s.wall.Now()
		s.event(action, r.Target)
	}
	return err
}
func (s *state) flood(ctx context.Context, r api.ChaosRequest) error {
	n := r.Params["count"]
	if n == 0 {
		n = float64(s.cfg.MaxVMs)
	}
	if n < 0 || n > float64(s.cfg.MaxVMs) || n != math.Trunc(n) {
		return invalid("count outside fleet request cap")
	}
	oldMix := s.mix
	if r.Params["interactive"] > 0 {
		s.mix = [3]float64{1, 0, 0}
	}
	defer func() { s.mix = oldMix }()
	for i := 0; i < int(n); i++ {
		if err := s.create(ctx); err != nil {
			if e, ok := err.(*api.Error); !ok || (e.Status != 429 && e.Status != 503) {
				return err
			}
		}
	}
	return nil
}
func (s *state) create(ctx context.Context) error {
	x := s.rng.Float64() * (s.mix[0] + s.mix[1] + s.mix[2])
	p := "batch"
	if x < s.mix[0] {
		p = "interactive"
	} else if x < s.mix[0]+s.mix[1] {
		p = "standard"
	}
	s.serial++
	_, err := s.backend.ControlPlane().CreateVM(ctx, fmt.Sprintf("chaos-%d-%d", s.status.Seed, s.serial), api.CreateVMRequest{Pool: config.Pools[0].Name, Priority: p})
	return err
}
func (s *state) tick(ctx context.Context, now time.Time) {
	dt := now.Sub(s.previous).Seconds()
	s.previous = now
	tasks := s.tasks
	s.tasks = nil
	for _, t := range tasks {
		if !t.at.After(now) {
			if err := s.do(ctx, now, t.action, t.req); err != nil {
				s.event("failed "+t.action+": "+err.Error(), t.req.Target)
			}
		} else {
			s.tasks = append(s.tasks, t)
		}
	}
	s.credit += dt * s.status.LoadRate
	for n := 0; s.credit >= 1 && n < s.cfg.MaxVMs; n++ {
		s.credit--
		if err := s.create(ctx); err != nil {
			s.event("load: "+err.Error(), "")
		}
	}
	if s.status.Intensity > 0 {
		choices := []api.ChaosAction{}
		total := 0.0
		for _, a := range catalogue {
			if a.Ambient && (len(s.status.Filters) == 0 || slices.Contains(s.status.Filters, a.Name)) {
				// Global faults cannot honor a rack scope; do not silently
				// inject them outside the selected failure domains.
				if len(s.status.Racks) > 0 && (a.Target == api.TargetGlobal || a.Target == api.TargetScheduler) {
					continue
				}
				choices = append(choices, a)
				total += s.cfg.ChaosBaseRates[a.Name]
			}
		}
		if len(choices) > 0 && s.rng.Float64() < 1-math.Exp(-s.status.Intensity*total*dt) {
			x := s.rng.Float64() * total
			a := choices[len(choices)-1]
			for _, candidate := range choices {
				x -= s.cfg.ChaosBaseRates[candidate.Name]
				if x < 0 {
					a = candidate
					break
				}
			}
			snap, err := s.backend.Snapshot(ctx)
			if err != nil {
				return
			}
			if len(s.status.Racks) > 0 {
				snap.VMs = slices.DeleteFunc(slices.Clone(snap.VMs), func(v api.VM) bool { return !slices.Contains(s.status.Racks, v.Rack) })
				snap.Hosts = slices.DeleteFunc(slices.Clone(snap.Hosts), func(h api.Host) bool { return !slices.Contains(s.status.Racks, h.Rack) })
				snap.Racks = slices.DeleteFunc(slices.Clone(snap.Racks), func(r api.Rack) bool { return !slices.Contains(s.status.Racks, r.ID) })
			}
			target := ""
			switch a.Target {
			case api.TargetVM:
				if len(snap.VMs) == 0 {
					return
				}
				target = snap.VMs[s.rng.Intn(len(snap.VMs))].ID
			case api.TargetHost:
				if len(snap.Hosts) == 0 {
					return
				}
				target = snap.Hosts[s.rng.Intn(len(snap.Hosts))].ID
			case api.TargetRack:
				if len(snap.Racks) == 0 {
					return
				}
				target = snap.Racks[s.rng.Intn(len(snap.Racks))].ID
			}
			if err := s.do(ctx, now, a.Name, api.ChaosRequest{Target: target}); err != nil {
				s.event("ambient failed: "+err.Error(), target)
			}
		}
	}
}
func (s *state) preset(ctx context.Context, now time.Time, name string) error {
	snap, err := s.backend.Snapshot(ctx)
	if err != nil {
		return err
	}
	if len(snap.Racks) == 0 {
		return invalid("preset requires a rack")
	}
	rack := snap.Racks[min(1, len(snap.Racks)-1)].ID
	add := func(delay time.Duration, a, target string) {
		s.tasks = append(s.tasks, scheduled{at: now.Add(delay), action: a, req: api.ChaosRequest{Target: target}})
	}
	heal := s.cfg.PresetHealAfter
	switch name {
	case "split-brain":
		if err = s.do(ctx, now, "partition-rack", api.ChaosRequest{Target: rack}); err == nil {
			add(heal, "heal-rack", rack)
		}
	case "gray-failure":
		for _, h := range snap.Hosts[:min(3, len(snap.Hosts))] {
			if err = s.do(ctx, now, "pause-heartbeats", api.ChaosRequest{Target: h.ID}); err != nil {
				return err
			}
			add(heal, "resume-heartbeats", h.ID)
		}
	case "thundering-herd":
		if err = s.flood(ctx, api.ChaosRequest{Params: map[string]float64{"count": math.Floor(float64(len(snap.Hosts)*s.cfg.SlotsPerHost) * .85)}}); err == nil {
			add(s.cfg.PresetStepEvery, "power-off-rack", rack)
			s.tasks = append(s.tasks, scheduled{at: now.Add(s.cfg.PresetStepEvery), action: "flood", req: api.ChaosRequest{Params: map[string]float64{"count": float64(s.cfg.MaxVMs), "interactive": 1}}})
		}
	case "rolling-maintenance":
		for i, r := range snap.Racks {
			add(time.Duration(i)*heal, "remove-rack", r.ID)
		}
	case "hostile-tenant":
		for _, v := range snap.VMs[:min(3, len(snap.VMs))] {
			if err = s.do(ctx, now, "compromise", api.ChaosRequest{Target: v.ID}); err != nil {
				return err
			}
		}
		err = s.flood(ctx, api.ChaosRequest{})
	case "bad-day":
		s.status.Intensity = .5
		if err = s.do(ctx, now, "latency-spike", api.ChaosRequest{Params: map[string]float64{"ms": float64(s.cfg.HostHeartbeat.Milliseconds()) / 2}}); err != nil {
			return err
		}
		err = s.flood(ctx, api.ChaosRequest{})
		add(s.cfg.PresetStepEvery, "kill-scheduler", "")
		add(s.cfg.PresetStepEvery*2, "restart-scheduler", "")
	}
	if err == nil {
		s.status.Scenario = name
	}
	return err
}

var _ api.ChaosEngine = (*Engine)(nil)
