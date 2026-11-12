// Package agent owns host admission, incarnation fencing and guest leases.
package agent

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"github.com/hami-sh/vm/internal/actor"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"sort"
	"sync"
	"time"
)

type instance struct {
	start     api.StartVM
	slot      api.SlotState
	state     api.VMState
	last      time.Time
	window    time.Time
	count     int
	blocked   []time.Time
	cancel    context.CancelFunc
	bootDone  chan struct{}
	recovered bool
	pending   *api.StartVM
	reason    string
}
type Effect struct {
	To      api.Addr
	Port    api.Port
	Message any
	Work    func(context.Context) any
}
type completion struct {
	revision uint64
	id       string
	gen      int64
	stop     bool
	err      error
}
type tick struct{}
type recovered struct {
	revision uint64
	powerOff bool
	reply    chan<- error
	vms      []api.DriverVM
	params   map[string]api.StartVM
	err      error
}
type query struct{ reply chan<- Snapshot }
type vmGeneration struct {
	id  string
	gen int64
}
type control struct {
	paused, cordoned *bool
	down             *bool
	skew             *time.Duration
	attack, vm       string
	reply            chan<- error
}
type Snapshot struct {
	Host      api.Host
	Inventory []api.InventoryItem
	Actual    []api.DriverVM
	ClockSkew time.Duration
	Security  api.SecurityStatus
	Epoch     int64
}

type Agent struct {
	cfg              config.Config
	clock            clock.Clock
	ep               api.Endpoint
	scheduler        api.Addr
	driver           api.Driver
	ctx              context.Context
	inbox            chan any
	done             chan struct{}
	workers          sync.WaitGroup
	vms              map[string]*instance
	highest          map[string]int64
	quarantined      map[vmGeneration]api.InventoryItem
	epoch            int64
	seq              uint64
	paused, cordoned bool
	reregister       bool
	awaitingAck      bool
	down             bool
	skew             time.Duration
	security         api.SecurityStatus
	revision         uint64
}

// New constructs the actor; call Run exactly once. Endpoint identity must be
// provisioned by the trusted host, never by a guest-supplied payload.
func New(cfg config.Config, c clock.Clock, ep api.Endpoint, scheduler api.Addr, d api.Driver) *Agent {
	return &Agent{cfg: cfg, clock: c, ep: ep, scheduler: scheduler, driver: d, inbox: make(chan any, cfg.GuestMsgMaxBytes), done: make(chan struct{}), vms: make(map[string]*instance), highest: make(map[string]int64), quarantined: make(map[vmGeneration]api.InventoryItem), security: api.SecurityStatus{ByAttack: make(map[string]int)}}
}
func (a *Agent) Run(ctx context.Context) {
	a.ctx = ctx
	defer close(a.done)
	ticker := a.clock.NewTicker(a.cfg.HostHeartbeat)
	defer ticker.Stop()
	leases := a.clock.NewTicker(a.cfg.InvariantEvery)
	defer leases.Stop()
	a.execute([]Effect{a.registration()})
	for {
		// Give deadlines priority even with a permanently readable guest inbox.
		select {
		case <-ticker.C():
			a.execute(a.heartbeat())
		default:
		}
		select {
		case <-ctx.Done():
			for _, v := range a.vms {
				v.cancel()
			}
			a.workers.Wait()
			for id, v := range a.vms {
				_ = a.driver.Stop(context.Background(), id, v.start.Generation)
			}
			a.ep.Close()
			return
		case <-ticker.C():
			a.execute(a.heartbeat())
		case <-leases.C():
			a.execute(a.handle(a.clock.Now(), tick{}))
		case m := <-a.inbox:
			a.execute(a.handle(a.clock.Now(), m))
		case e, ok := <-a.ep.Inbox():
			if !ok {
				return
			}
			a.execute(a.handle(a.clock.Now(), e))
		case e := <-a.driver.Exits():
			a.execute(a.handle(a.clock.Now(), e))
		}
	}
}
func (a *Agent) Done() <-chan struct{} { return a.done }
func (a *Agent) Snapshot(ctx context.Context) (Snapshot, error) {
	s, err := actor.Ask(ctx, a.inbox, func(r chan<- Snapshot) any { return query{r} })
	if err != nil {
		return s, err
	}
	s.Actual, err = a.driver.List(ctx)
	return s, err
}
func (a *Agent) Kill(ctx context.Context) error {
	on := true
	result, err := actor.Ask(ctx, a.inbox, func(reply chan<- error) any { return control{down: &on, reply: reply} })
	if err != nil {
		return err
	}
	return result
}
func (a *Agent) Restart(ctx context.Context) error {
	on := false
	result, err := actor.Ask(ctx, a.inbox, func(reply chan<- error) any { return control{down: &on, reply: reply} })
	if err != nil {
		return err
	}
	return result
}
func (a *Agent) SetClockSkew(ctx context.Context, skew time.Duration) error {
	return a.command(ctx, control{skew: &skew})
}
func (a *Agent) PauseHeartbeats(ctx context.Context, on bool) error {
	return a.command(ctx, control{paused: &on})
}
func (a *Agent) Cordon(ctx context.Context, on bool) error {
	return a.command(ctx, control{cordoned: &on})
}

// RecordBlocked is a trusted host-side hook for simnet/driver denied attacks.
// It is not a wire message a guest can forge.
func (a *Agent) RecordBlocked(ctx context.Context, id, attack string) error {
	return a.command(ctx, control{vm: id, attack: attack})
}
func (a *Agent) command(ctx context.Context, m any) error {
	select {
	case a.inbox <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-a.done:
		return context.Canceled
	}
}
func (a *Agent) report(m any) Effect {
	return Effect{To: a.scheduler, Port: api.PortControl, Message: m}
}
func (a *Agent) registration() Effect {
	return a.report(api.RegisterHost{HostID: a.ep.Addr().ID, Rack: a.ep.Addr().Rack, Slots: min(2, a.cfg.SlotsPerHost), DiskFull: a.diskFull(), Inventory: a.inventory(), Epoch: a.epoch})
}
func (a *Agent) diskFull() bool {
	if d, ok := a.driver.(interface{ DiskFull() bool }); ok {
		return d.DiskFull()
	}
	return false
}
func (a *Agent) inventory() []api.InventoryItem {
	out := make([]api.InventoryItem, 0, len(a.vms)+len(a.quarantined))
	for id, v := range a.vms {
		out = append(out, api.InventoryItem{VMID: id, Generation: v.start.Generation, State: v.state, Pool: v.start.Spec.Pool, IdempotencyKey: v.start.IdempotencyKey, RequestHash: v.start.RequestHash})
	}
	for _, item := range a.quarantined {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].VMID == out[j].VMID {
			return out[i].Generation < out[j].Generation
		}
		return out[i].VMID < out[j].VMID
	})
	return out
}
func (a *Agent) heartbeat() []Effect {
	if a.paused || a.down {
		return nil
	}
	a.seq++
	out := []Effect{a.report(api.HostHeartbeat{HostID: a.ep.Addr().ID, Seq: a.seq, DiskFull: a.diskFull(), Inventory: a.inventory(), Epoch: a.epoch})}
	if a.reregister {
		out = append(out, a.registration())
	}
	return out
}
func (a *Agent) execute(effects []Effect) {
	for _, e := range effects {
		if e.Work != nil {
			a.workers.Add(1)
			actor.Work(a.ctx, a.inbox, func(ctx context.Context) any { defer a.workers.Done(); return e.Work(ctx) })
		} else if !a.down {
			_ = a.ep.Send(e.To, e.Port, e.Message)
		}
	}
}
func (a *Agent) result(m api.StartVM, admitted bool, reason string) Effect {
	return a.report(api.StartVMResult{VMID: m.VMID, Generation: m.Generation, HostID: a.ep.Addr().ID, Admitted: admitted, Reason: reason})
}
func (a *Agent) start(now time.Time, m api.StartVM) []Effect {
	if v := a.vms[m.VMID]; v != nil {
		if m.Generation < v.start.Generation {
			return []Effect{a.result(m, false, api.ReasonStale)}
		}
		if m.Generation == v.start.Generation {
			return []Effect{a.result(m, v.slot != api.SlotReleasing, api.ReasonStale)}
		}
		if v.pending == nil || m.Generation > v.pending.Generation {
			copy := m
			v.pending = &copy
		}
		return a.release(v, api.ReasonStale)
	}
	if m.Generation <= a.highest[m.VMID] {
		return []Effect{a.result(m, false, api.ReasonStale)}
	}
	if a.cordoned {
		return []Effect{a.result(m, false, "cordoned")}
	}
	if a.diskFull() {
		return []Effect{a.result(m, false, "disk_full")}
	}
	if len(a.vms) >= min(2, a.cfg.SlotsPerHost) {
		return []Effect{a.result(m, false, "full")}
	}
	bootCtx, cancel := context.WithCancel(a.ctx)
	v := &instance{start: m, slot: api.SlotReserved, state: api.VMBooting, last: now, cancel: cancel, bootDone: make(chan struct{})}
	a.vms[m.VMID] = v
	a.highest[m.VMID] = m.Generation
	p := api.BootParams{VMID: m.VMID, Generation: m.Generation, Token: m.Token, Host: a.ep.Addr().ID, Rack: a.ep.Addr().Rack}
	revision := a.revision
	return []Effect{a.result(m, true, ""), {Work: func(context.Context) any {
		defer close(v.bootDone)
		var err error
		if d, ok := a.driver.(interface {
			StartCommand(context.Context, api.StartVM, api.BootParams) error
		}); ok {
			err = d.StartCommand(bootCtx, m, p)
		} else {
			err = a.driver.Start(bootCtx, m.Spec, p)
		}
		return completion{revision: revision, id: m.VMID, gen: m.Generation, err: err}
	}}}
}
func (a *Agent) release(v *instance, reason string) []Effect {
	if v.slot == api.SlotReleasing {
		return nil
	}
	v.cancel()
	v.slot = api.SlotReleasing
	v.state = api.VMTerminating
	v.reason = reason
	id, gen, done := v.start.VMID, v.start.Generation, v.bootDone
	revision := a.revision
	return []Effect{{Work: func(ctx context.Context) any {
		<-done
		err := a.driver.Stop(ctx, id, gen)
		return completion{revision: revision, id: id, gen: gen, stop: true, err: err}
	}}}
}
func (a *Agent) unhealthy(v *instance, reason string) []Effect {
	v.state = api.VMUnhealthy
	return []Effect{a.report(api.UnhealthyReport{HostID: a.ep.Addr().ID, VMID: v.start.VMID, Generation: v.start.Generation, Reason: reason})}
}
func (a *Agent) blocked(now time.Time, v *instance, attack string) []Effect {
	a.security.Blocked++
	a.security.ByAttack[attack]++
	cut := now.Add(-a.cfg.QuarantineWindow)
	kept := v.blocked[:0]
	for _, t := range v.blocked {
		if t.After(cut) {
			kept = append(kept, t)
		}
	}
	v.blocked = append(kept, now)
	quarantine := len(v.blocked) >= a.cfg.QuarantineAttempts
	out := []Effect{a.report(api.SecurityReport{HostID: a.ep.Addr().ID, VMID: v.start.VMID, Generation: v.start.Generation, Attack: attack, Quarantined: quarantine})}
	if quarantine {
		out = append(out, a.release(v, api.ReasonQuarantined)...)
	}
	return out
}
func (a *Agent) handle(now time.Time, msg any) (effects []Effect) {
	switch m := msg.(type) {
	case api.Envelope:
		if _, ok := m.Msg.(api.GuestRegister); ok && m.From.Kind == api.KindGuest && m.Port == api.PortVsock && !a.down {
			defer func() {
				reason := "unknown_or_releasing_guest"
				for _, e := range effects {
					if _, ok := e.Message.(api.GuestRegisterResult); ok {
						return
					}
					if r, ok := e.Message.(api.SecurityReport); ok {
						reason = r.Attack
					}
				}
				effects = append(effects, Effect{To: m.From, Port: api.PortVsock, Message: api.GuestRegisterResult{Accepted: false, Reason: reason}})
			}()
		}
		if a.down {
			return nil
		}
		if m.From == a.scheduler && m.Port == api.PortControl {
			switch cmd := m.Msg.(type) {
			case api.StartVM:
				if cmd.Epoch < a.epoch {
					return nil
				}
				if a.awaitingAck {
					return []Effect{a.result(cmd, false, "awaiting_scheduler_ack")}
				}
				a.epoch = cmd.Epoch
				return a.start(now, cmd)
			case api.StopVM:
				if cmd.Epoch < a.epoch {
					return nil
				}
				a.epoch = cmd.Epoch
				a.highest[cmd.VMID] = max(a.highest[cmd.VMID], cmd.Generation)
				if v := a.vms[cmd.VMID]; v != nil {
					if v.pending != nil && v.pending.Generation <= cmd.Generation {
						v.pending = nil
					}
					if v.start.Generation <= cmd.Generation {
						return a.release(v, cmd.Reason)
					}
					return nil
				}
				return []Effect{a.report(api.StoppedReport{HostID: a.ep.Addr().ID, VMID: cmd.VMID, Generation: cmd.Generation, Reason: cmd.Reason})}
			case api.HeartbeatAck:
				if cmd.Epoch < a.epoch || cmd.HostID != a.ep.Addr().ID {
					return nil
				}
				a.epoch = cmd.Epoch
				a.awaitingAck = false
				a.reregister = cmd.Reregister
				var out []Effect
				if a.reregister {
					out = append(out, a.registration())
				}
				for _, stale := range cmd.Stale {
					delete(a.quarantined, vmGeneration{stale.VMID, stale.Generation})
					if v := a.vms[stale.VMID]; v != nil && v.start.Generation <= stale.Generation {
						out = append(out, a.release(v, api.ReasonStale)...)
					}
				}
				return out
			}
		}
		if m.From.Kind != api.KindGuest || m.From.Host != a.ep.Addr().ID || m.Port != api.PortVsock {
			return nil
		}
		v := a.vms[m.From.ID]
		if v == nil || v.slot == api.SlotReleasing {
			return nil
		}
		if now.Sub(v.window) >= time.Second {
			v.window = now
			v.count = 0
		}
		v.count++
		if v.count > a.cfg.GuestMsgPerSec {
			return a.blocked(now, v, api.AttackFlood)
		}
		b, err := json.Marshal(m.Msg)
		if err != nil || len(b) > a.cfg.GuestMsgMaxBytes {
			return a.blocked(now, v, api.AttackFlood)
		}
		switch g := m.Msg.(type) {
		case api.GuestRegister:
			if g.VMID != m.From.ID {
				return a.blocked(now, v, api.AttackImpersonate)
			}
			if g.Generation != v.start.Generation || subtle.ConstantTimeCompare([]byte(g.Token), []byte(v.start.Token)) != 1 {
				return a.blocked(now, v, api.AttackReplay)
			}
			if v.state == api.VMUnhealthy {
				return nil
			}
			v.state = api.VMReady
			v.slot = api.SlotOccupied
			v.last = now
			return []Effect{a.report(api.ReadyReport{HostID: a.ep.Addr().ID, VMID: m.From.ID, Generation: v.start.Generation}), {To: m.From, Port: api.PortVsock, Message: api.GuestRegisterResult{Accepted: true}}}
		case api.GuestHeartbeat:
			if g.VMID != m.From.ID {
				return a.blocked(now, v, api.AttackImpersonate)
			}
			if g.Generation != v.start.Generation {
				return a.blocked(now, v, api.AttackReplay)
			}
			if v.state == api.VMReady {
				v.last = now
			}
			if v.recovered {
				v.recovered = false
				v.state = api.VMReady
				v.last = now
				return []Effect{a.report(api.ReadyReport{HostID: a.ep.Addr().ID, VMID: m.From.ID, Generation: v.start.Generation})}
			}
		default:
			return a.blocked(now, v, api.AttackControl)
		}
	case completion:
		if m.revision != a.revision {
			return nil
		}
		v := a.vms[m.id]
		if v == nil || v.start.Generation != m.gen {
			return nil
		}
		if m.stop {
			if m.err != nil {
				return nil
			} // Never reclaim a disk whose cleanup failed.
			delete(a.vms, m.id)
			if v.reason == api.ReasonQuarantined {
				a.quarantined[vmGeneration{m.id, m.gen}] = api.InventoryItem{VMID: m.id, Generation: m.gen, State: api.VMFailed, Pool: v.start.Spec.Pool, IdempotencyKey: v.start.IdempotencyKey, RequestHash: v.start.RequestHash}
			}
			out := []Effect{a.report(api.StoppedReport{HostID: a.ep.Addr().ID, VMID: m.id, Generation: m.gen, Reason: v.reason})}
			if v.pending != nil {
				out = append(out, a.start(now, *v.pending)...)
			}
			return out
		}
		if v.slot == api.SlotReleasing {
			return nil
		}
		if m.err != nil {
			out := a.unhealthy(v, "driver_start: "+m.err.Error())
			return append(out, a.release(v, "driver_start")...)
		}
		v.slot = api.SlotOccupied
	case api.DriverExit:
		if v := a.vms[m.VMID]; v != nil && v.start.Generation == m.Generation && v.slot != api.SlotReleasing {
			return a.unhealthy(v, m.Reason)
		}
	case tick:
		var out []Effect
		for _, v := range a.vms {
			if v.state == api.VMReady && now.Sub(v.last) >= a.cfg.GuestLeaseTTL {
				out = append(out, a.unhealthy(v, api.ReasonGuestLease)...)
			}
		}
		return out
	case control:
		if m.skew != nil {
			a.skew = *m.skew
		}
		if m.down != nil {
			powerOff, reply := *m.down, m.reply
			var boots []<-chan struct{}
			if powerOff {
				for _, v := range a.vms {
					v.cancel()
					boots = append(boots, v.bootDone)
				}
			}
			// Lose actor memory, but never cancel an incarnation on process
			// restart. The driver, not scheduler memory, supplies recovery data.
			a.down = true
			a.revision++
			revision := a.revision
			clear(a.vms)
			clear(a.highest)
			// Quarantine delivery survives packet loss, not a process restart.
			clear(a.quarantined)
			a.epoch = 0
			a.seq = 0
			a.paused = false
			a.cordoned = false
			a.reregister = false
			// Recovered live VMs do not preserve stopped-generation fences.
			// Restore the scheduler epoch from an ack before admitting starts.
			a.awaitingAck = true
			return []Effect{{Work: func(ctx context.Context) any {
				for _, done := range boots {
					<-done
				}
				vms, err := a.driver.List(ctx)
				if powerOff && err == nil {
					for _, v := range vms {
						if stopErr := a.driver.Stop(ctx, v.VMID, v.Generation); stopErr != nil {
							err = stopErr
							break
						}
					}
					vms = nil
				}
				params := make(map[string]api.StartVM)
				if source, ok := a.driver.(interface {
					LaunchInfo(string, int64) (api.StartVM, bool)
				}); ok {
					for _, v := range vms {
						if cmd, ok := source.LaunchInfo(v.VMID, v.Generation); ok {
							params[v.VMID] = cmd
						}
					}
				}
				return recovered{revision: revision, powerOff: powerOff, reply: reply, vms: vms, params: params, err: err}
			}}}
		}
		if m.paused != nil {
			a.paused = *m.paused
		}
		if m.cordoned != nil {
			a.cordoned = *m.cordoned
		}
		if v := a.vms[m.vm]; v != nil && m.attack != "" && v.slot != api.SlotReleasing {
			return a.blocked(now, v, m.attack)
		}
	case recovered:
		if m.revision != a.revision {
			m.reply <- context.Canceled
			return nil
		}
		if m.err != nil {
			m.reply <- m.err
			return nil
		}
		if m.powerOff {
			m.reply <- nil
			return nil
		}
		for _, actual := range m.vms {
			cmd, ok := m.params[actual.VMID]
			if !ok {
				cmd = api.StartVM{VMID: actual.VMID, Generation: actual.Generation}
			}
			done := make(chan struct{})
			close(done)
			a.vms[actual.VMID] = &instance{start: cmd, slot: api.SlotOccupied, state: api.VMBooting, last: now, cancel: func() {}, bootDone: done, recovered: actual.Running}
			a.highest[actual.VMID] = actual.Generation
		}
		a.down = false
		m.reply <- nil
		return []Effect{a.registration()}
	case query:
		h := api.Host{ID: a.ep.Addr().ID, Rack: a.ep.Addr().Rack, SlotCount: min(2, a.cfg.SlotsPerHost), AdminState: api.AdminActive, Lease: api.LeaseHealthy}
		if a.cordoned {
			h.AdminState = api.AdminCordoned
		}
		inv := a.inventory()
		for _, item := range inv {
			if item.State == api.VMFailed {
				continue
			}
			h.Slots = append(h.Slots, api.Slot{Index: len(h.Slots), State: a.vms[item.VMID].slot, VMID: item.VMID, Generation: item.Generation})
		}
		for len(h.Slots) < h.SlotCount {
			h.Slots = append(h.Slots, api.Slot{Index: len(h.Slots), State: api.SlotFree})
		}
		sec := a.security
		sec.ByAttack = make(map[string]int)
		for k, v := range a.security.ByAttack {
			sec.ByAttack[k] = v
		}
		m.reply <- Snapshot{Host: h, Inventory: inv, Security: sec, Epoch: a.epoch, ClockSkew: a.skew}
	}
	return nil
}
