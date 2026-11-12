// Package driver implements simulated and Tart hypervisors.
package driver

import (
	"context"
	"fmt"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/guest"
	"math/rand"
	"sync"
	"time"
)

type machine struct {
	p           api.BootParams
	spec        api.VMSpec
	cancel      context.CancelFunc
	done        chan struct{}
	guestCancel context.CancelFunc
	ep          api.Endpoint
	running     bool
	disk, quota int
	results     []api.GuestRegisterResult
	ctx         context.Context
	swap        chan replacement
}
type replacement struct {
	run   func(context.Context, api.Endpoint)
	ready chan struct{}
}
type Fake struct {
	mu       sync.Mutex
	clock    clock.Clock
	cfg      config.Config
	net      api.Transport
	rng      *rand.Rand
	vms      map[string]*machine
	exits    chan api.DriverExit
	hang     bool
	slow     float64
	diskFull bool
	closed   bool
}

func NewFake(c clock.Clock, cfg config.Config, n api.Transport, seed int64) *Fake {
	return &Fake{clock: c, cfg: cfg, net: n, rng: rand.New(rand.NewSource(seed)), vms: make(map[string]*machine), exits: make(chan api.DriverExit, cfg.GuestMsgMaxBytes), slow: 1}
}
func (f *Fake) HangNextBoot()           { f.mu.Lock(); defer f.mu.Unlock(); f.hang = true }
func (f *Fake) SlowBoot(factor float64) { f.mu.Lock(); defer f.mu.Unlock(); f.slow = max(1, factor) }
func (f *Fake) FillDisk(full bool)      { f.mu.Lock(); defer f.mu.Unlock(); f.diskFull = full }
func (f *Fake) DiskFull() bool          { f.mu.Lock(); defer f.mu.Unlock(); return f.diskFull }
func (f *Fake) Start(ctx context.Context, s api.VMSpec, p api.BootParams) error {
	f.mu.Lock()
	if f.closed || f.diskFull {
		f.mu.Unlock()
		return fmt.Errorf("disk_full or driver closed")
	}
	if old := f.vms[p.VMID]; old != nil {
		f.mu.Unlock()
		if old.p.Generation == p.Generation {
			return nil
		}
		return fmt.Errorf("VM already exists")
	}
	bootCtx, cancel := context.WithCancel(ctx)
	m := &machine{p: p, spec: s, ctx: bootCtx, cancel: cancel, done: make(chan struct{}), quota: s.DiskMB, swap: make(chan replacement)}
	f.vms[p.VMID] = m
	delay := f.cfg.BootMin
	if f.cfg.BootMax > delay {
		delay += time.Duration(f.rng.Int63n(int64(f.cfg.BootMax - delay)))
	}
	delay = time.Duration(float64(delay) * f.slow)
	hang := f.hang
	f.hang = false
	f.mu.Unlock()
	defer close(m.done)
	timer := f.clock.NewTimer(delay)
	defer timer.Stop()
	if hang {
		<-bootCtx.Done()
	} else {
		select {
		case <-bootCtx.Done():
		case <-timer.C():
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if bootCtx.Err() != nil {
		return bootCtx.Err()
	}
	ep, err := f.net.Attach(api.Addr{Kind: api.KindGuest, ID: p.VMID, Host: p.Host, Rack: p.Rack})
	if err != nil {
		return err
	}
	m.ep = ep
	m.running = true
	// The boot context belongs to the agent's incarnation and remains alive
	// until Stop, not merely until Start returns.
	guestDone := make(chan struct{})
	m.done = guestDone
	go f.supervise(m, guestDone, func(ctx context.Context, ep api.Endpoint) {
		guest.RunObserved(ctx, f.clock, f.cfg, ep, p, func(e api.Envelope) {
			if r, ok := e.Msg.(api.GuestRegisterResult); ok {
				f.mu.Lock()
				if len(m.results) < f.cfg.GuestMsgMaxBytes {
					m.results = append(m.results, r)
				}
				f.mu.Unlock()
			}
		})
	})
	return nil
}
func (f *Fake) supervise(m *machine, done chan struct{}, run func(context.Context, api.Endpoint)) {
	defer close(done)
	defer m.ep.Close()
	launch := func(fn func(context.Context, api.Endpoint)) (context.CancelFunc, chan struct{}) {
		ctx, cancel := context.WithCancel(m.ctx)
		finished := make(chan struct{})
		f.mu.Lock()
		m.guestCancel = cancel
		f.mu.Unlock()
		go func() { defer close(finished); fn(ctx, m.ep) }()
		return cancel, finished
	}
	cancel, finished := launch(run)
	for {
		select {
		case <-m.ctx.Done():
			cancel()
			<-finished
			return
		case r := <-m.swap:
			cancel()
			<-finished
			cancel, finished = launch(r.run)
			close(r.ready)
		}
	}
}

// Compromise replaces guestd without replacing its authenticated connection.
// The attacker must respect ctx; Stop/Crash/quarantine cancel that lifetime.
func (f *Fake) Compromise(id string, attacker func(context.Context, api.Endpoint)) error {
	f.mu.Lock()
	m := f.vms[id]
	if m == nil || !m.running || m.ctx.Err() != nil || attacker == nil {
		f.mu.Unlock()
		return fmt.Errorf("guest not running")
	}
	f.mu.Unlock()
	r := replacement{run: attacker, ready: make(chan struct{})}
	select {
	case m.swap <- r:
	case <-m.ctx.Done():
		return fmt.Errorf("guest stopped")
	}
	select {
	case <-r.ready:
		return nil
	case <-m.ctx.Done():
		return fmt.Errorf("guest stopped")
	}
}
func (f *Fake) Stop(ctx context.Context, id string, gen int64) error {
	f.mu.Lock()
	m := f.vms[id]
	if m == nil || m.p.Generation > gen {
		f.mu.Unlock()
		return nil
	}
	m.cancel()
	done := m.done
	f.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	f.mu.Lock()
	if f.vms[id] == m {
		delete(f.vms, id)
	}
	f.mu.Unlock()
	return nil
}
func (f *Fake) List(ctx context.Context) ([]api.DriverVM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []api.DriverVM
	for _, m := range f.vms {
		out = append(out, api.DriverVM{VMID: m.p.VMID, Generation: m.p.Generation, Running: m.running})
	}
	return out, ctx.Err()
}
func (f *Fake) Exits() <-chan api.DriverExit { return f.exits }

// BootInfo is host-owned launch metadata, not a guest claim. It is the local
// recovery adapter because api.Driver.List intentionally carries no secrets.
func (f *Fake) BootInfo(id string, gen int64) (api.BootParams, api.VMSpec, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.vms[id]
	if m == nil || m.p.Generation != gen {
		return api.BootParams{}, api.VMSpec{}, false
	}
	return m.p, m.spec, true
}

// GuestResults drains independent verdicts received by the real guest loop.
func (f *Fake) GuestResults(id string) []api.GuestRegisterResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m := f.vms[id]; m != nil {
		out := m.results
		m.results = nil
		return out
	}
	return nil
}

// GuestSend uses the real VM endpoint; From cannot be chosen by the caller.
func (f *Fake) GuestSend(id string, to api.Addr, port api.Port, msg any) error {
	f.mu.Lock()
	m := f.vms[id]
	var ep api.Endpoint
	if m != nil {
		ep = m.ep
	}
	f.mu.Unlock()
	if ep == nil {
		return fmt.Errorf("guest not running")
	}
	return ep.Send(to, port, msg)
}
func (f *Fake) KillGuest(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m := f.vms[id]; m != nil && m.guestCancel != nil {
		m.guestCancel()
	}
}
func (f *Fake) Crash(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m := f.vms[id]; m != nil {
		m.cancel()
		m.running = false
		select {
		case f.exits <- api.DriverExit{VMID: id, Generation: m.p.Generation, Reason: "crash"}:
		default:
		}
	}
}

// WriteDisk accounts only this VM's writable clone. Exhaustion cannot consume
// another VM's quota or the host's admission budget.
func (f *Fake) WriteDisk(id string, mb int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.vms[id]
	if m == nil {
		return fmt.Errorf("unknown VM")
	}
	if mb < 0 || mb > m.quota-m.disk {
		return fmt.Errorf("disk quota exceeded")
	}
	m.disk += mb
	return nil
}
func (f *Fake) Close() {
	f.mu.Lock()
	f.closed = true
	ids := make(map[string]int64)
	for id, m := range f.vms {
		ids[id] = m.p.Generation
	}
	f.mu.Unlock()
	for id, gen := range ids {
		_ = f.Stop(context.Background(), id, gen)
	}
}

var _ api.Driver = (*Fake)(nil)
