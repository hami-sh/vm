package agent_test

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/agent"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/driver"
	"github.com/hami-sh/vm/internal/simnet"
)

type harness struct {
	t         *testing.T
	c         clock.Clock
	cfg       config.Config
	n         *simnet.Network
	d         *driver.Fake
	a         *agent.Agent
	scheduler api.Endpoint
	host      api.Addr
	ctx       context.Context
	messages  []any
}

type observedEndpoint struct {
	api.Endpoint
	inbox chan api.Envelope
}

func (e observedEndpoint) Inbox() <-chan api.Envelope { return e.inbox }

func newHarness(t *testing.T, observe ...func(api.Envelope)) *harness {
	return newHarnessClock(t, clock.NewFake(time.Unix(1000, 0)), config.Default(), observe...)
}
func newHarnessClock(t *testing.T, c clock.Clock, cfg config.Config, observe ...func(api.Envelope)) *harness {
	t.Helper()
	baseline := runtime.NumGoroutine()
	n := simnet.New(c, cfg, 37)
	s, err := n.Attach(api.Addr{Kind: api.KindScheduler, ID: "scheduler"})
	if err != nil {
		t.Fatal(err)
	}
	host := api.Addr{Kind: api.KindAgent, ID: "h1", Host: "h1", Rack: "r1"}
	ep, err := n.Attach(host)
	if err != nil {
		t.Fatal(err)
	}
	d := driver.NewFake(c, cfg, n, 42)
	ctx, cancel := context.WithCancel(context.Background())
	observedDone := make(chan struct{})
	if len(observe) > 0 {
		source := ep
		tapped := observedEndpoint{Endpoint: ep, inbox: make(chan api.Envelope)}
		ep = tapped
		go func() {
			defer close(observedDone)
			defer close(tapped.inbox)
			for {
				select {
				case <-ctx.Done():
					return
				case e, ok := <-source.Inbox():
					if !ok {
						return
					}
					select {
					case tapped.inbox <- e:
						observe[0](e)
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	} else {
		close(observedDone)
	}
	a := agent.New(cfg, c, ep, s.Addr(), d)
	go a.Run(ctx)
	h := &harness{t: t, c: c, cfg: cfg, n: n, d: d, a: a, scheduler: s, host: host, ctx: ctx}
	t.Cleanup(func() {
		cancel()
		deadline := clock.Real{}.NewTimer(5 * time.Second)
		defer deadline.Stop()
		select {
		case <-a.Done():
		case <-deadline.C():
			t.Fatal("agent shutdown hung")
		}
		d.Close()
		n.Close()
		<-observedDone
		for range 100 {
			if runtime.NumGoroutine() <= baseline {
				return
			}
			settle := clock.Real{}.NewTimer(time.Millisecond)
			<-settle.C()
			settle.Stop()
		}
		stack := make([]byte, 1<<16)
		count := runtime.Stack(stack, true)
		t.Errorf("goroutine leak: before=%d after=%d\n%s", baseline, runtime.NumGoroutine(), stack[:count])
	})
	h.snapshot()
	return h
}
func (h *harness) drain() {
	for {
		select {
		case e := <-h.scheduler.Inbox():
			h.messages = append(h.messages, e.Msg)
		default:
			return
		}
	}
}
func (h *harness) snapshot() agent.Snapshot {
	h.t.Helper()
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	s, err := h.a.Snapshot(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	h.drain()
	if len(s.Host.Slots) != 2 || len(s.Inventory) > 2 || len(s.Actual) > 2 {
		h.t.Fatalf("capacity violated: %+v", s)
	}
	if s.Security.Succeeded != 0 {
		h.t.Fatalf("successful attack: %+v", s.Security)
	}
	return s
}
func (h *harness) step(d time.Duration) {
	if fake, ok := h.c.(*clock.Fake); ok {
		fake.Advance(d)
		d = 100 * time.Microsecond
	}
	// A fake clock does not schedule goroutines. Give every actor wall time
	// before moving the next simulated deadline; don't race through TTLs.
	timer := clock.Real{}.NewTimer(d)
	<-timer.C()
	timer.Stop()
	h.drain()
}
func (h *harness) until(check func(agent.Snapshot) bool) agent.Snapshot {
	h.t.Helper()
	for range 3000 {
		s := h.snapshot()
		if check(s) {
			return s
		}
		h.step(min(100*time.Millisecond, h.cfg.GuestHeartbeat/2))
	}
	h.t.Fatalf("condition not reached: %+v; messages=%+v", h.snapshot(), h.messages)
	return agent.Snapshot{}
}
func (h *harness) send(m any) {
	h.t.Helper()
	if err := h.scheduler.Send(h.host, api.PortControl, m); err != nil {
		h.t.Fatal(err)
	}
}
func start(id string, gen int64) api.StartVM {
	return api.StartVM{VMID: id, Generation: gen, Token: fmt.Sprintf("token-%s-%d", id, gen), Epoch: 1, Spec: api.VMSpec{Pool: "ios-xcode16-m", DiskMB: 100}}
}
func ready(s agent.Snapshot, id string, gen int64) bool {
	for _, v := range s.Inventory {
		if v.VMID == id && v.Generation == gen && v.State == api.VMReady {
			return true
		}
	}
	return false
}
func (h *harness) boot(id string, gen int64) {
	h.send(start(id, gen))
	h.until(func(s agent.Snapshot) bool { return ready(s, id, gen) })
}
func (h *harness) guest(id string, m any) {
	h.t.Helper()
	if err := h.d.GuestSend(id, h.host, api.PortVsock, m); err != nil {
		h.t.Fatal(err)
	}
}

func TestDuplicateStartVM(t *testing.T) {
	h := newHarness(t)
	h.n.SetDuplicate(1)
	for range 5 {
		h.send(start("a", 1))
	}
	h.until(func(s agent.Snapshot) bool { return ready(s, "a", 1) })
	s := h.snapshot()
	if len(s.Actual) != 1 || !s.Actual[0].Running || len(s.Inventory) != 1 || s.Inventory[0].Pool != "ios-xcode16-m" {
		t.Fatalf("duplicate boot or lost pool: %+v", s)
	}
	h.send(api.StopVM{VMID: "a", Generation: 1, Epoch: 1})
	h.until(func(s agent.Snapshot) bool { return len(s.Actual) == 0 && len(s.Inventory) == 0 })
	h.send(start("a", 1))
	for range 10 {
		h.step(time.Second)
	}
	if len(h.snapshot().Actual) != 0 {
		t.Fatal("reordered start resurrected stopped generation")
	}
}
func TestLateStopDoesNotKillNewerGeneration(t *testing.T) {
	h := newHarness(t)
	h.boot("a", 1)
	h.boot("a", 2)
	h.send(api.StopVM{VMID: "a", Generation: 1, Epoch: 1})
	h.send(api.StopVM{VMID: "a", Generation: 2, Epoch: 0})
	for range 20 {
		h.step(time.Second / 2)
	}
	s := h.snapshot()
	if !ready(s, "a", 2) || len(s.Actual) != 1 || s.Actual[0].Generation != 2 {
		t.Fatalf("late command killed newer generation: %+v", s)
	}
}
func TestDeleteDuringBoot(t *testing.T) {
	h := newHarness(t)
	h.d.HangNextBoot()
	h.send(start("a", 1))
	h.until(func(s agent.Snapshot) bool { return len(s.Actual) == 1 })
	h.send(api.StopVM{VMID: "a", Generation: 1, Epoch: 1, Reason: api.ReasonDeleted})
	h.until(func(s agent.Snapshot) bool { return len(s.Inventory) == 0 && len(s.Actual) == 0 })
	for _, m := range h.messages {
		if r, ok := m.(api.ReadyReport); ok && r.VMID == "a" {
			t.Fatal("deleted guest became ready")
		}
	}
	h.boot("b", 1)
}
func TestSlowBootKeepsHeartbeats(t *testing.T) {
	h := newHarness(t)
	h.d.SlowBoot(2)
	h.send(start("a", 1))
	h.until(func(s agent.Snapshot) bool { return len(s.Actual) == 1 })
	last := h.c.Now()
	count := 0
	for range 5 {
		h.step(h.cfg.HostHeartbeat)
		h.until(func(s agent.Snapshot) bool {
			for _, m := range h.messages {
				if hb, ok := m.(api.HostHeartbeat); ok && int(hb.Seq) > count {
					count = int(hb.Seq)
					if gap := h.c.Now().Sub(last); gap > h.cfg.HostHeartbeat+50*time.Millisecond {
						t.Fatalf("heartbeat gap %v", gap)
					}
					last = h.c.Now()
					return true
				}
			}
			return false
		})
	}
	if count < 5 {
		t.Fatalf("heartbeats stalled during slow boot: %d", count)
	}
	h.until(func(s agent.Snapshot) bool { return ready(s, "a", 1) })
}
func TestGuestCannotImpersonate(t *testing.T) {
	h := newHarness(t)
	h.boot("attacker", 1)
	h.d.HangNextBoot()
	h.send(start("victim", 1))
	h.until(func(s agent.Snapshot) bool { return len(s.Actual) == 2 })
	h.guest("attacker", api.GuestRegister{VMID: "victim", Generation: 1, Token: start("victim", 1).Token})
	s := h.until(func(s agent.Snapshot) bool { return s.Security.ByAttack[api.AttackImpersonate] == 1 })
	if ready(s, "victim", 1) || !ready(s, "attacker", 1) {
		t.Fatalf("channel identity not enforced: %+v", s)
	}
	// An agent command carried on vsock must never reach the command handler.
	h.guest("attacker", api.StopVM{VMID: "victim", Generation: 1, Epoch: 100})
	h.until(func(s agent.Snapshot) bool { return s.Security.ByAttack[api.AttackControl] == 1 })
}
func TestReplayStaleToken(t *testing.T) {
	h := newHarness(t)
	h.boot("a", 1)
	h.boot("a", 2)
	h.guest("a", api.GuestRegister{VMID: "a", Generation: 2, Token: start("a", 1).Token})
	h.guest("a", api.GuestRegister{VMID: "a", Generation: 3, Token: start("a", 2).Token})
	s := h.until(func(s agent.Snapshot) bool { return s.Security.ByAttack[api.AttackReplay] == 2 })
	if !ready(s, "a", 2) {
		t.Fatal("stale token affected current incarnation")
	}
}
func TestRejectedRegisterGetsVerdict(t *testing.T) {
	h := newHarness(t)
	h.d.HangNextBoot()
	h.send(start("a", 1))
	h.until(func(s agent.Snapshot) bool { return len(s.Actual) == 1 })
	ep, err := h.n.Attach(api.Addr{Kind: api.KindGuest, ID: "a", Host: "h1", Rack: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Close()
	for _, reg := range []api.GuestRegister{
		{VMID: "victim", Generation: 1, Token: start("a", 1).Token},
		{VMID: "a", Generation: 1, Token: "wrong"},
		{VMID: "a", Generation: 2, Token: start("a", 1).Token},
	} {
		if err := ep.Send(h.host, api.PortVsock, reg); err != nil {
			t.Fatal(err)
		}
		h.until(func(agent.Snapshot) bool { return len(ep.Inbox()) > 0 })
		e := <-ep.Inbox()
		r, ok := e.Msg.(api.GuestRegisterResult)
		if !ok || r.Accepted || r.Reason == "" || e.From != h.host || e.To != ep.Addr() {
			t.Fatalf("bad rejection verdict: %+v", e)
		}
	}
}

func TestRestartRejectsStartsUntilSchedulerAck(t *testing.T) {
	h := newHarness(t)
	cmd := start("a", 5)
	cmd.Epoch = 9
	h.send(cmd)
	h.until(func(s agent.Snapshot) bool { return ready(s, "a", 5) })
	h.send(api.StopVM{VMID: "a", Generation: 5, Epoch: 9})
	h.until(func(s agent.Snapshot) bool { return len(s.Inventory) == 0 && len(s.Actual) == 0 })
	if err := h.a.Restart(h.ctx); err != nil {
		t.Fatal(err)
	}
	old := start("a", 4)
	old.Epoch = 3
	before := len(h.messages)
	h.send(old)
	h.until(func(s agent.Snapshot) bool {
		for _, m := range h.messages[before:] {
			if r, ok := m.(api.StartVMResult); ok && r.VMID == "a" && r.Generation == 4 {
				if r.Admitted || len(s.Inventory) != 0 || len(s.Actual) != 0 {
					t.Fatalf("delayed start resurrected stopped VM after restart: result=%+v snapshot=%+v", r, s)
				}
				return true
			}
		}
		return false
	})
	// Another host's ack cannot restore our scheduler fence, even when its
	// epoch is higher than the current scheduler's.
	h.send(api.HeartbeatAck{HostID: "other", Epoch: 100})
	fresh := start("a", 6)
	fresh.Epoch = 10
	before = len(h.messages)
	h.send(fresh)
	h.until(func(agent.Snapshot) bool {
		for _, m := range h.messages[before:] {
			if r, ok := m.(api.StartVMResult); ok && r.VMID == "a" && r.Generation == 6 {
				if r.Admitted {
					t.Fatal("wrong-host ack allowed admission")
				}
				return true
			}
		}
		return false
	})
	h.send(api.HeartbeatAck{HostID: "h1", Epoch: 10})
	h.until(func(s agent.Snapshot) bool { return s.Epoch == 10 })
	h.send(old)
	h.send(fresh)
	s := h.until(func(s agent.Snapshot) bool { return ready(s, "a", 6) })
	if len(s.Actual) != 1 || s.Actual[0].Generation != 6 || !s.Actual[0].Running {
		t.Fatalf("scheduler ack did not restore safe admission: %+v", s)
	}
}

func TestAgentRestartPreservesDriverVMs(t *testing.T) {
	h := newHarness(t)
	h.boot("a", 1)
	h.boot("b", 1)
	if err := h.a.Restart(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.until(func(s agent.Snapshot) bool { return ready(s, "a", 1) && ready(s, "b", 1) })
	for _, m := range h.messages {
		if r, ok := m.(api.StoppedReport); ok {
			t.Fatalf("process restart stopped a VM: %+v", r)
		}
	}
	s := h.snapshot()
	for _, v := range s.Actual {
		if !v.Running || v.Generation != 1 {
			t.Fatalf("restart changed actual VM: %+v", v)
		}
	}
	for _, v := range s.Inventory {
		if v.Pool == "" {
			t.Fatal("restart lost pool")
		}
	}
}

func TestInventoryRoundTripsSchedulerIdentity(t *testing.T) {
	h := newHarness(t)
	cmd := start("a", 1)
	cmd.Epoch = 17
	cmd.IdempotencyKey = "opaque/key:α"
	cmd.RequestHash = "opaque-hash-not-interpreted"
	h.send(cmd)
	h.until(func(s agent.Snapshot) bool { return ready(s, "a", 1) })
	check := func(items []api.InventoryItem) bool {
		if len(items) != 1 {
			return false
		}
		got := items[0]
		if got.Pool != cmd.Spec.Pool || got.IdempotencyKey != cmd.IdempotencyKey || got.RequestHash != cmd.RequestHash {
			t.Fatalf("identity changed: %+v", got)
		}
		return true
	}
	h.until(func(s agent.Snapshot) bool {
		for _, m := range h.messages {
			if hb, ok := m.(api.HostHeartbeat); ok && hb.Epoch == 17 && check(hb.Inventory) {
				return true
			}
		}
		return false
	})
	h.send(api.HeartbeatAck{HostID: "h1", Epoch: 23})
	h.until(func(s agent.Snapshot) bool { return s.Epoch == 23 })
	stale := start("stale", 1)
	stale.Epoch = 22
	h.send(stale)
	h.send(api.StopVM{VMID: "a", Generation: 1, Epoch: 22})
	h.until(func(s agent.Snapshot) bool {
		for _, m := range h.messages {
			if hb, ok := m.(api.HostHeartbeat); ok && hb.Epoch == 23 && check(hb.Inventory) {
				return true
			}
		}
		return false
	})
	if !ready(h.snapshot(), "a", 1) || len(h.snapshot().Actual) != 1 {
		t.Fatal("old scheduler epoch changed actual VMs")
	}
	before := len(h.messages)
	if err := h.a.Restart(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.until(func(s agent.Snapshot) bool {
		for _, m := range h.messages[before:] {
			if r, ok := m.(api.RegisterHost); ok && check(r.Inventory) {
				return true
			}
		}
		return false
	})
	h.until(func(s agent.Snapshot) bool { return ready(s, "a", 1) })
}

func TestReregisterAfterDroppedRegistration(t *testing.T) {
	h := newHarness(t)
	// The stub scheduler drops the initial registration without acknowledging it.
	h.until(func(agent.Snapshot) bool {
		for _, m := range h.messages {
			if _, ok := m.(api.RegisterHost); ok {
				return true
			}
		}
		return false
	})
	cmd := start("a", 4)
	cmd.Epoch = 17
	cmd.IdempotencyKey = "original-key"
	cmd.RequestHash = "original-hash"
	h.send(cmd)
	h.until(func(s agent.Snapshot) bool { return ready(s, "a", 4) })
	h.d.FillDisk(true)
	before := len(h.messages)
	h.send(api.HeartbeatAck{HostID: "h1", Epoch: 18, Reregister: true})
	count := func() int {
		n := 0
		for _, m := range h.messages[before:] {
			if r, ok := m.(api.RegisterHost); ok {
				n++
				if r.HostID != "h1" || r.Rack != "r1" || r.Slots != 2 || r.Epoch != 18 || !r.DiskFull || len(r.Inventory) != 1 {
					t.Fatalf("incomplete recovery registration: %+v", r)
				}
				want := api.InventoryItem{VMID: "a", Generation: 4, State: api.VMReady, Pool: cmd.Spec.Pool, IdempotencyKey: cmd.IdempotencyKey, RequestHash: cmd.RequestHash}
				if r.Inventory[0] != want {
					t.Fatalf("recovery inventory: got %+v, want %+v", r.Inventory[0], want)
				}
			}
		}
		return n
	}
	h.until(func(agent.Snapshot) bool { return count() >= 1 })
	// A lower epoch or a different host's ack cannot stop retries.
	h.send(api.HeartbeatAck{HostID: "h1", Epoch: 17})
	h.send(api.HeartbeatAck{HostID: "other", Epoch: 19})
	n := count()
	h.until(func(agent.Snapshot) bool { return count() > n })
	h.send(api.HeartbeatAck{HostID: "h1", Epoch: 19})
	h.until(func(s agent.Snapshot) bool { return s.Epoch == 19 })
	n = count()
	for range 3 {
		h.step(h.cfg.HostHeartbeat)
		h.snapshot()
	}
	if count() != n {
		t.Fatal("normal acknowledgment did not stop re-registration")
	}
}

func TestCompromiseUsesRealEndpointAndStops(t *testing.T) {
	for _, cause := range []string{"stop", "crash", "quarantine"} {
		t.Run(cause, func(t *testing.T) {
			h := newHarness(t)
			if err := h.d.Compromise("missing", func(context.Context, api.Endpoint) {}); err == nil {
				t.Fatal("compromised a missing guest")
			}
			h.boot("a", 1)
			verdict := make(chan api.GuestRegisterResult, 1)
			stopped := make(chan struct{})
			err := h.d.Compromise("a", func(ctx context.Context, ep api.Endpoint) {
				defer close(stopped)
				_ = ep.Send(h.host, api.PortVsock, api.GuestRegister{VMID: "a", Generation: 1, Token: "bad"})
				for {
					select {
					case <-ctx.Done():
						return
					case e := <-ep.Inbox():
						if r, ok := e.Msg.(api.GuestRegisterResult); ok && !r.Accepted {
							select {
							case verdict <- r:
							default:
							}
							<-ctx.Done()
							return
						}
					}
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			h.until(func(agent.Snapshot) bool { return len(verdict) > 0 })
			if r := <-verdict; r.Accepted || r.Reason != api.AttackReplay {
				t.Fatalf("attacker did not receive denial: %+v", r)
			}
			switch cause {
			case "stop":
				h.send(api.StopVM{VMID: "a", Generation: 1, Epoch: 1})
			case "crash":
				h.d.Crash("a")
			case "quarantine":
				for range h.cfg.QuarantineAttempts - 1 {
					h.guest("a", api.GuestRegister{VMID: "a", Generation: 1, Token: "bad"})
				}
			}
			h.until(func(agent.Snapshot) bool {
				select {
				case <-stopped:
					return true
				default:
					return false
				}
			})
			if cause != "crash" {
				h.until(func(s agent.Snapshot) bool { return len(s.Actual) == 0 })
			}
		})
	}
}

func TestFloodDoesNotStarveNeighbours(t *testing.T) {
	arrivals := make(chan time.Time, config.Default().EventRetention)
	cfg := config.Default()
	// Wall-clock observation avoids fake-clock stepping quantization. Scale
	// the test fixture consistently to the configured maximum demo speed.
	factor := time.Duration(cfg.MaxSpeed)
	cfg.BootMin /= factor
	cfg.BootMax /= factor
	cfg.GuestHeartbeat /= factor
	cfg.GuestLeaseTTL /= factor
	cfg.HostHeartbeat /= factor
	cfg.InvariantEvery /= factor
	cfg.GuestMsgPerSec *= int(factor)
	cfg.QuarantineWindow /= factor
	var h *harness
	h = newHarnessClock(t, clock.Real{}, cfg, func(e api.Envelope) {
		if _, ok := e.Msg.(api.GuestHeartbeat); ok && e.From.ID == "neighbour" {
			arrivals <- h.c.Now()
		}
	})
	h.boot("attacker", 1)
	h.boot("neighbour", 1)
	p99 := func() time.Duration {
		var samples []time.Time
		for len(arrivals) > 0 {
			samples = append(samples, <-arrivals)
		}
		if len(samples) < 10 {
			t.Fatalf("too few heartbeat observations: %d", len(samples))
		}
		var gaps []time.Duration
		for i := 1; i < len(samples); i++ {
			gaps = append(gaps, samples[i].Sub(samples[i-1]))
		}
		sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
		return gaps[(len(gaps)*99-1)/100]
	}
	for range 40 {
		h.step(h.cfg.GuestHeartbeat / 2)
		h.snapshot()
	}
	baseline := p99()
	// Both oversized traffic and rate-limit violations must be blocked through
	// the real VM endpoint, while the neighbour keeps renewing its lease.
	h.guest("attacker", strings.Repeat("x", h.cfg.GuestMsgMaxBytes+1))
	for range h.cfg.GuestMsgPerSec + h.cfg.QuarantineAttempts {
		_ = h.d.GuestSend("attacker", h.host, api.PortVsock, api.GuestHeartbeat{VMID: "attacker", Generation: 1})
	}
	h.until(func(s agent.Snapshot) bool {
		return len(s.Inventory) == 1 && s.Security.ByAttack[api.AttackFlood] >= h.cfg.QuarantineAttempts
	})
	for range 40 {
		h.step(h.cfg.GuestHeartbeat / 2)
		if !ready(h.snapshot(), "neighbour", 1) {
			t.Fatal("neighbour lease starved")
		}
	}
	flood := p99()
	if flood > baseline+50*time.Millisecond {
		t.Fatalf("neighbour heartbeat p99 changed: baseline=%v flood=%v", baseline, flood)
	}
	t.Logf("neighbour heartbeat p99: baseline=%v flood=%v", baseline, flood)
	for _, m := range h.messages {
		if r, ok := m.(api.UnhealthyReport); ok && r.VMID == "neighbour" {
			t.Fatalf("neighbour affected: %+v", r)
		}
	}
}
func TestDiskFillContained(t *testing.T) {
	h := newHarness(t)
	h.boot("a", 1)
	h.boot("b", 1)
	if err := h.d.WriteDisk("a", 100); err != nil {
		t.Fatal(err)
	}
	if err := h.d.WriteDisk("a", 1); err == nil {
		t.Fatal("quota bypassed")
	}
	if err := h.d.WriteDisk("b", 73); err != nil {
		t.Fatal("neighbour quota affected", err)
	}
	h.d.FillDisk(true)
	h.send(api.StopVM{VMID: "a", Generation: 1, Epoch: 1})
	h.until(func(s agent.Snapshot) bool { return len(s.Inventory) == 1 })
	h.send(start("c", 1))
	h.until(func(s agent.Snapshot) bool {
		for _, m := range h.messages {
			if r, ok := m.(api.StartVMResult); ok && r.VMID == "c" && !r.Admitted && r.Reason == "disk_full" {
				return true
			}
		}
		return false
	})
	if !ready(h.snapshot(), "b", 1) {
		t.Fatal("disk fill hurt neighbour")
	}
	h.d.FillDisk(false)
	h.boot("c", 1)
}
func TestGuestLeaseAndControlPlaneOutage(t *testing.T) {
	h := newHarness(t)
	h.boot("a", 1)
	h.n.PartitionRack("r1", true)
	for range 20 {
		h.step(time.Second)
		if !ready(h.snapshot(), "a", 1) {
			t.Fatal("control-plane outage killed healthy guest")
		}
	}
	h.d.KillGuest("a")
	h.until(func(s agent.Snapshot) bool {
		for _, v := range s.Inventory {
			if v.State == api.VMUnhealthy {
				return true
			}
		}
		return false
	})
	h.n.PartitionRack("r1", false)
	h.send(start("a", 2))
	h.until(func(s agent.Snapshot) bool { return ready(s, "a", 2) })
}
func TestAdmissionCapAndHostFaults(t *testing.T) {
	h := newHarness(t)
	h.boot("a", 1)
	h.boot("b", 1)
	h.send(start("c", 1))
	h.until(func(s agent.Snapshot) bool {
		for _, m := range h.messages {
			if r, ok := m.(api.StartVMResult); ok && r.VMID == "c" && r.Reason == "full" && !r.Admitted {
				return true
			}
		}
		return false
	})
	if err := h.a.SetClockSkew(h.ctx, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := h.a.PauseHeartbeats(h.ctx, true); err != nil {
		t.Fatal(err)
	}
	h.snapshot()
	for range 12 {
		h.step(time.Second)
	}
	if !ready(h.snapshot(), "b", 1) {
		t.Fatal("clock skew/paused host heartbeat killed guest")
	}
	if err := h.a.Kill(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.until(func(s agent.Snapshot) bool { return len(s.Actual) == 0 && len(s.Inventory) == 0 })
	if err := h.a.Restart(h.ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.a.PauseHeartbeats(h.ctx, false); err != nil {
		t.Fatal(err)
	}
	h.send(api.HeartbeatAck{HostID: "h1", Epoch: 1})
	h.until(func(s agent.Snapshot) bool { return s.Epoch == 1 })
	h.boot("a", 2)
}

type listGate struct {
	api.Driver
	entered chan struct{}
	release chan struct{}
}

func (d listGate) List(ctx context.Context) ([]api.DriverVM, error) {
	select {
	case d.entered <- struct{}{}:
	default:
	}
	select {
	case <-d.release:
		return d.Driver.List(ctx)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func TestRestartReturnsAfterInventoryRecovery(t *testing.T) {
	cfg := config.Default()
	c := clock.NewFake(time.Unix(100, 0))
	n := simnet.New(c, cfg, 1)
	s, err := n.Attach(api.Addr{Kind: api.KindScheduler, ID: "scheduler"})
	if err != nil {
		t.Fatal(err)
	}
	ep, err := n.Attach(api.Addr{Kind: api.KindAgent, ID: "h1", Host: "h1", Rack: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	f := driver.NewFake(c, cfg, n, 2)
	gate := listGate{Driver: f, entered: make(chan struct{}, 1), release: make(chan struct{})}
	a := agent.New(cfg, c, ep, s.Addr(), gate)
	ctx, cancel := context.WithCancel(context.Background())
	go a.Run(ctx)
	t.Cleanup(func() { cancel(); <-a.Done(); f.Close(); n.Close() })
	returned := make(chan error, 1)
	go func() { returned <- a.Restart(ctx) }()
	<-gate.entered
	timer := clock.Real{}.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-returned:
		t.Fatalf("restart returned before driver inventory was read: %v", err)
	case <-timer.C():
	}
	close(gate.release)
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
	if _, err := a.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
}
