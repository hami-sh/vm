package fleet

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"time"

	"github.com/hami-sh/vm/internal/actor"
	"github.com/hami-sh/vm/internal/agent"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/chaos"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/driver"
	"github.com/hami-sh/vm/internal/invariant"
	"github.com/hami-sh/vm/internal/simnet"
)

var schedulerAddr = api.Addr{Kind: api.KindScheduler, ID: "scheduler"}

type host struct {
	addr     api.Addr
	agent    *agent.Agent
	driver   *driver.Fake
	cancel   context.CancelFunc
	admin    api.AdminState
	faults   []string
	security api.SecurityStatus
}
type operation struct {
	ctx   context.Context
	fn    func(context.Context) (any, error)
	reply chan<- result
}
type result struct {
	value any
	err   error
}
type observed struct {
	snapshot api.Snapshot
	raw      []invariant.Observation
	epoch    int64
	keys     map[string]string
}
type eventBatch struct {
	events    []api.Event
	truncated bool
}

// Complete event reads are independent of lossy UI subscriptions and snapshot
// Recent limits. The cursor belongs to the invariant/publisher actor.
func (r *runtime) events(ctx context.Context, after int64) ([]api.Event, bool, error) {
	v, err := r.call(ctx, func(ctx context.Context) (any, error) {
		if r.down {
			return eventBatch{}, nil
		}
		events, truncated, err := r.scheduler.EventsSince(ctx, after)
		return eventBatch{events, truncated}, err
	})
	if err != nil {
		return nil, false, err
	}
	batch := v.(eventBatch)
	return batch.events, batch.truncated, nil
}

type runtime struct {
	ctx                context.Context
	cfg                config.Config
	clock              clock.Clock
	options            Options
	inbox              chan operation
	done               chan struct{}
	net                *simnet.Network
	transport          api.Transport
	hosts              map[string]*host
	racks              []api.Rack
	nextRack, nextHost int
	scheduler          Scheduler
	schedulerCancel    context.CancelFunc
	schedulerClose     func()
	down               bool
	logIncarnation     int64
	compromised        map[string]bool
	success            api.SecurityStatus
	security           api.SecurityStatus
	keys               map[string]string
	cached             api.Snapshot
	emit               func(api.Event)
}

func newRuntime(ctx context.Context, cfg config.Config, c clock.Clock, o Options, emit func(api.Event)) (*runtime, error) {
	r := &runtime{ctx: ctx, cfg: cfg, clock: c, options: o, inbox: make(chan operation), done: make(chan struct{}), hosts: map[string]*host{}, compromised: map[string]bool{}, success: api.SecurityStatus{ByAttack: map[string]int{}}, security: api.SecurityStatus{ByAttack: map[string]int{}}, keys: map[string]string{}, emit: emit}
	if r.options.SchedulerFactory == nil {
		r.options.SchedulerFactory = newScheduler
	}
	if err := r.start(); err != nil {
		r.stop()
		return nil, err
	}
	go func() {
		defer close(r.done)
		defer r.stop()
		actor.Run(ctx, r.inbox, func(op operation) { op.reply <- r.handle(c.Now(), op) })
	}()
	return r, nil
}
func (r *runtime) handle(now time.Time, op operation) result {
	v, err := op.fn(op.ctx)
	return result{v, err}
}
func (r *runtime) call(ctx context.Context, fn func(context.Context) (any, error)) (any, error) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer stop()
	defer cancel()
	v, err := actor.Ask(ctx, r.inbox, func(ch chan<- result) operation { return operation{ctx: ctx, fn: fn, reply: ch} })
	if err != nil {
		return nil, err
	}
	return v.value, v.err
}
func (r *runtime) start() error {
	r.net = simnet.New(r.clock, r.cfg, r.options.Seed)
	r.transport = r.net
	if err := r.restart(false); err != nil {
		return err
	}
	for i := 0; i < r.cfg.Racks; i++ {
		if err := r.addRack("", r.cfg.HostsPerRack); err != nil {
			return err
		}
	}
	return nil
}
func (r *runtime) stop() {
	if r.schedulerCancel != nil {
		r.schedulerCancel()
	}
	if r.schedulerClose != nil {
		r.schedulerClose()
		r.schedulerClose = nil
	}
	for _, h := range r.hosts {
		h.cancel()
	}
	for _, h := range r.hosts {
		<-h.agent.Done()
		h.driver.Close()
	}
	if r.net != nil {
		r.net.Close()
	}
}
func (r *runtime) restart(wipe bool) error {
	if r.schedulerCancel != nil {
		r.schedulerCancel()
	}
	if r.schedulerClose != nil {
		r.schedulerClose()
		r.schedulerClose = nil
	}
	r.down = true
	if wipe {
		r.logIncarnation++
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err := os.Remove(r.options.StorePath + suffix); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	ctx, cancel := context.WithCancel(r.ctx)
	r.schedulerCancel = cancel
	s, close, err := r.options.SchedulerFactory(ctx, r.cfg, r.clock, r.transport, r.options.StorePath)
	if err != nil {
		cancel()
		return err
	}
	r.scheduler = s
	r.schedulerClose = close
	r.down = false
	return nil
}
func (r *runtime) addRack(id string, n int) error {
	if len(r.hosts)+n > r.cfg.MaxVMs {
		return &api.Error{Status: 429, Msg: "topology cap reached"}
	}
	if id == "" {
		r.nextRack++
		id = fmt.Sprintf("r%d", r.nextRack)
	}
	if slices.ContainsFunc(r.racks, func(x api.Rack) bool { return x.ID == id }) {
		return &api.Error{Status: 422, Msg: "rack exists"}
	}
	r.racks = append(r.racks, api.Rack{ID: id})
	for i := 0; i < n; i++ {
		if err := r.addHost(id, ""); err != nil {
			return err
		}
	}
	return nil
}
func (r *runtime) addHost(rack, id string) error {
	idx := slices.IndexFunc(r.racks, func(x api.Rack) bool { return x.ID == rack })
	if idx < 0 {
		return &api.Error{Status: 404, Msg: "rack not found"}
	}
	if len(r.hosts) >= r.cfg.MaxVMs {
		return &api.Error{Status: 429, Msg: "topology cap reached"}
	}
	if id == "" {
		for ordinal := 1; ; ordinal++ {
			id = fmt.Sprintf("%s-h%d", rack, ordinal)
			if r.hosts[id] == nil {
				break
			}
		}
	}
	if r.hosts[id] != nil {
		return &api.Error{Status: 422, Msg: "host exists"}
	}
	r.nextHost++
	addr := api.Addr{Kind: api.KindAgent, ID: id, Host: id, Rack: rack}
	ep, err := r.transport.Attach(addr)
	if err != nil {
		return err
	}
	d := driver.NewFake(r.clock, r.cfg, r.transport, r.options.Seed+int64(r.nextHost))
	a := agent.New(r.cfg, r.clock, ep, schedulerAddr, d)
	ctx, cancel := context.WithCancel(r.ctx)
	r.hosts[id] = &host{addr: addr, agent: a, driver: d, cancel: cancel, admin: api.AdminActive}
	r.racks[idx].Hosts = append(r.racks[idx].Hosts, id)
	go a.Run(ctx)
	// Reusing a removed host ID must also restore scheduler admission. Attach
	// the endpoint first so any queued placement has a live destination.
	if r.scheduler != nil {
		err := r.scheduler.SetHostAdmin(ctx, id, api.AdminActive)
		var apiErr *api.Error
		if err != nil && (!errors.As(err, &apiErr) || apiErr.Status != 404) {
			return err
		}
	}
	return nil
}
func (r *runtime) ControlPlane() api.ControlPlane { return r }
func (r *runtime) Snapshot(ctx context.Context) (api.Snapshot, error) {
	o, err := r.observe(ctx)
	return o.snapshot, err
}
func (r *runtime) observe(ctx context.Context) (observed, error) {
	v, err := r.call(ctx, func(ctx context.Context) (any, error) { return r.read(ctx) })
	if err != nil {
		return observed{}, err
	}
	o := v.(observed)
	return o, nil
}
func (r *runtime) read(ctx context.Context) (observed, error) {
	s := clone(r.cached)
	if r.scheduler != nil && !r.down {
		var err error
		s, err = r.scheduler.Snapshot(ctx)
		if err != nil {
			return observed{}, err
		}
		r.cached = clone(s)
	} else {
		s.Scheduler = api.SchedulerDown
	}
	s.At = r.clock.Now().UnixMilli()
	leases := map[string]api.Host{}
	for _, h := range s.Hosts {
		leases[h.ID] = h
	}
	var raw []invariant.Observation
	ids := make([]string, 0, len(r.hosts))
	for id := range r.hosts {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		h := r.hosts[id]
		a, err := h.agent.Snapshot(ctx)
		if err != nil {
			return observed{}, err
		}
		// Agent counters are monotonic over this host actor's lifetime. Fold
		// their deltas before removal; reusing a host ID starts a new baseline.
		r.security.Blocked += a.Security.Blocked - h.security.Blocked
		for attack, count := range a.Security.ByAttack {
			r.security.ByAttack[attack] += count - h.security.ByAttack[attack]
		}
		h.security = a.Security
		if h.admin == api.AdminDraining && s.Scheduler == api.SchedulerUp {
			empty := len(a.Inventory) == 0 &&
				!slices.ContainsFunc(a.Actual, func(v api.DriverVM) bool { return v.Running }) &&
				!slices.ContainsFunc(a.Host.Slots, func(slot api.Slot) bool { return slot.State != api.SlotFree }) &&
				!slices.ContainsFunc(s.VMs, func(v api.VM) bool { return v.HostID == id && v.State != api.VMTerminated })
			if empty {
				if err := r.scheduler.SetHostAdmin(ctx, id, api.AdminRemoved); err != nil {
					return observed{}, err
				}
				h.cancel()
				<-h.agent.Done()
				h.driver.Close()
				delete(r.hosts, id)
				for i := range r.racks {
					r.racks[i].Hosts = slices.DeleteFunc(r.racks[i].Hosts, func(host string) bool { return host == id })
				}
				r.emit(api.Event{At: s.At, Kind: api.EventTopology, HostID: id, Rack: h.addr.Rack, Detail: "drain completed"})
				continue
			}
		}
		sh, ok := leases[id]
		connected := ok && sh.Lease == api.LeaseHealthy &&
			!slices.Contains(h.faults, "kill-host") && !slices.Contains(h.faults, "one-way-partition") &&
			!slices.ContainsFunc(r.racks, func(rack api.Rack) bool { return rack.ID == h.addr.Rack && rack.Partitioned })
		a.Host.AdminState = h.admin
		a.Host.Lease = sh.Lease
		a.Host.Faults = slices.Clone(h.faults)
		raw = append(raw, invariant.Observation{Host: a.Host, Actual: a.Actual, Connected: connected})
		if connected {
			for _, actual := range a.Actual {
				if actual.Running && r.keys[actual.VMID] == "" {
					if launch, ok := h.driver.LaunchInfo(actual.VMID, actual.Generation); ok {
						r.keys[actual.VMID] = launch.IdempotencyKey
					}
				}
			}
		}
		if !ok {
			s.Hosts = append(s.Hosts, a.Host)
		}
	}
	s.Security = api.SecurityStatus{Blocked: r.security.Blocked, Succeeded: r.success.Succeeded, ByAttack: maps.Clone(r.security.ByAttack)}
	s.Hosts = slices.DeleteFunc(s.Hosts, func(h api.Host) bool { return r.hosts[h.ID] == nil })
	for i := range s.Hosts {
		if h := r.hosts[s.Hosts[i].ID]; h != nil {
			s.Hosts[i].Faults = slices.Clone(h.faults)
		}
	}
	r.racks = slices.DeleteFunc(r.racks, func(rack api.Rack) bool { return len(rack.Hosts) == 0 })
	s.Racks = slices.Clone(r.racks)
	for i := range s.Racks {
		s.Racks[i].Hosts = slices.Clone(s.Racks[i].Hosts)
	}
	for i := range s.VMs {
		s.VMs[i].Compromised = r.compromised[s.VMs[i].ID]
	}
	return observed{snapshot: s, raw: raw, epoch: r.logIncarnation, keys: maps.Clone(r.keys)}, nil
}
func (r *runtime) Apply(ctx context.Context, action string, req api.ChaosRequest) error {
	_, err := r.call(ctx, func(ctx context.Context) (any, error) { return nil, r.apply(ctx, action, req) })
	return err
}
func (r *runtime) apply(ctx context.Context, a string, q api.ChaosRequest) (err error) {
	param := func(k string, def float64) float64 {
		if v, ok := q.Params[k]; ok {
			return v
		}
		return def
	}
	h := r.hosts[q.Target]
	if h == nil && q.Target != "" && r.scheduler != nil && !r.down {
		if v, err := r.scheduler.GetVM(ctx, q.Target); err == nil {
			h = r.hosts[v.HostID]
		}
	}
	switch a {
	case "kill-scheduler":
		if r.scheduler != nil && !r.down {
			if s, err := r.scheduler.Snapshot(ctx); err == nil {
				r.cached = clone(s)
			}
			_ = r.scheduler.SetDown(ctx, true)
			r.schedulerCancel()
			r.schedulerClose()
			r.schedulerClose = nil
		}
		r.down = true
		return nil
	case "restart-scheduler":
		return r.restart(false)
	case "wipe-store":
		return r.restart(true)
	case "slow-store":
		return r.scheduler.SetStoreDelay(ctx, time.Duration(param("ms", 200))*time.Millisecond)
	case "latency-spike":
		ms := param("ms", 200)
		if ms < 0 || ms > float64(r.cfg.HostLeaseTTL.Milliseconds()) {
			return &api.Error{Status: 422, Msg: "invalid latency"}
		}
		r.net.SetLatency(time.Duration(ms) * time.Millisecond)
		return nil
	case "packet-loss":
		p := param("percent", 10)
		if p < 0 || p > 100 {
			return &api.Error{Status: 422, Msg: "invalid loss"}
		}
		r.net.SetPacketLoss(p / 100)
		return nil
	case "duplicate-messages":
		p := param("percent", 100)
		if p < 0 || p > 100 {
			return &api.Error{Status: 422, Msg: "invalid duplicates"}
		}
		r.net.SetDuplicate(p / 100)
		return nil
	case "reorder-messages":
		r.net.SetReorder(param("on", 1) != 0)
		return nil
	case "partition-rack", "heal-rack", "power-off-rack":
		i := slices.IndexFunc(r.racks, func(x api.Rack) bool { return x.ID == q.Target })
		if i < 0 {
			return &api.Error{Status: 404, Msg: "rack not found"}
		}
		if a == "power-off-rack" {
			for _, id := range r.racks[i].Hosts {
				if err := r.apply(ctx, "kill-host", api.ChaosRequest{Target: id}); err != nil {
					return err
				}
			}
		} else {
			on := a == "partition-rack"
			r.net.PartitionRack(q.Target, on)
			r.racks[i].Partitioned = on
		}
		return nil
	}
	if h == nil && (a == "cordon" || a == "uncordon") {
		for _, rack := range r.racks {
			if rack.ID != q.Target {
				continue
			}
			for _, id := range rack.Hosts {
				if err := r.apply(ctx, a, api.ChaosRequest{Target: id}); err != nil {
					return err
				}
			}
			return nil
		}
	}
	if h == nil {
		return &api.Error{Status: 404, Msg: "host or placed VM not found"}
	}
	// Only persistent host conditions belong in Faults, never VM actions or
	// the recovery commands themselves. Failed actions don't change badges.
	fault, on := "", false
	switch a {
	case "kill-host", "pause-heartbeats", "cordon":
		fault, on = a, true
	case "resume-heartbeats":
		fault = "pause-heartbeats"
	case "uncordon":
		fault = "cordon"
	case "clock-skew":
		fault, on = a, param("ms", 30000) != 0
	case "fill-disk", "one-way-partition":
		fault, on = a, param("on", 1) != 0
	case "slow-boot":
		fault, on = a, param("factor", 3) != 1
	}
	defer func() {
		if err != nil || fault == "" {
			return
		}
		if !on {
			h.faults = slices.DeleteFunc(h.faults, func(x string) bool { return x == fault })
		} else if !slices.Contains(h.faults, fault) {
			h.faults = append(h.faults, fault)
		}
	}()
	switch a {
	case "kill-host":
		return h.agent.Kill(ctx)
	case "restart-host":
		h.faults = nil
		r.net.PartitionOneWay(h.addr, schedulerAddr, false)
		r.net.PartitionOneWay(schedulerAddr, h.addr, false)
		h.driver.FillDisk(false)
		h.driver.SlowBoot(1)
		return h.agent.Restart(ctx)
	case "pause-heartbeats":
		return h.agent.PauseHeartbeats(ctx, true)
	case "resume-heartbeats":
		return h.agent.PauseHeartbeats(ctx, false)
	case "clock-skew":
		ms := param("ms", 30000)
		if math.Abs(ms) > 30000 {
			return &api.Error{Status: 422, Msg: "clock skew exceeds 30 seconds"}
		}
		return h.agent.SetClockSkew(ctx, time.Duration(ms)*time.Millisecond)
	case "fill-disk":
		on := param("on", 1) != 0
		h.driver.FillDisk(on)
	case "cordon", "uncordon":
		h.admin = api.AdminCordoned
		if a == "uncordon" {
			h.admin = api.AdminActive
		}
		if err := h.agent.Cordon(ctx, a == "cordon"); err != nil {
			return err
		}
		return r.scheduler.SetHostAdmin(ctx, h.addr.ID, h.admin)
	case "one-way-partition":
		if param("on", 1) == 0 {
			// Off heals either direction, including both injected together.
			r.net.PartitionOneWay(h.addr, schedulerAddr, false)
			r.net.PartitionOneWay(schedulerAddr, h.addr, false)
		} else if param("reverse", 0) != 0 {
			r.net.PartitionOneWay(schedulerAddr, h.addr, true)
		} else {
			r.net.PartitionOneWay(h.addr, schedulerAddr, true)
		}
	case "slow-boot":
		factor := param("factor", 3)
		if factor < 1 || factor > r.cfg.MaxSpeed {
			return &api.Error{Status: 422, Msg: "invalid boot factor"}
		}
		h.driver.SlowBoot(factor)
	case "hang-next-boot":
		h.driver.HangNextBoot()
	case "kill-guestd":
		h.driver.KillGuest(q.Target)
	case "crash-vm":
		h.driver.Crash(q.Target)
	case "compromise":
		v, err := r.scheduler.GetVM(ctx, q.Target)
		if err != nil {
			return err
		}
		if v.State != api.VMReady {
			return &api.Error{Status: 422, Msg: "compromise requires a Ready VM"}
		}
		err = h.driver.Compromise(q.Target, func(guestCtx context.Context, ep api.Endpoint) {
			chaos.RunAttacker(guestCtx, r.clock, r.cfg, ep, v.Generation, func(mb int) error { return h.driver.WriteDisk(v.ID, mb) }, func(out chaos.AttackOutcome) { r.attackOutcome(guestCtx, h, out) })
		})
		if err == nil {
			r.compromised[q.Target] = true
		}
		return err
	default:
		return &api.Error{Status: 422, Msg: "unknown component fault"}
	}
	return nil
}
func (r *runtime) attackOutcome(ctx context.Context, h *host, out chaos.AttackOutcome) {
	_, err := r.call(ctx, func(ctx context.Context) (any, error) {
		if out.Succeeded {
			r.success.Succeeded++
			r.success.ByAttack[out.Attack]++
		}
		if out.Blocked && !out.AgentCounted {
			return nil, h.agent.RecordBlocked(ctx, out.VMID, out.Attack)
		}
		return nil, nil
	})
	if err != nil {
		return
	}
	verdict := "unknown"
	if out.Blocked {
		verdict = "blocked"
	}
	if out.Succeeded {
		verdict = "SUCCEEDED"
	}
	r.emit(api.Event{At: r.clock.Now().UnixMilli(), Kind: api.EventSecurity, VMID: out.VMID, HostID: h.addr.ID, Rack: h.addr.Rack, Detail: verdict + " " + out.Attack})
}
func (r *runtime) Topology(ctx context.Context, a string, q api.TopologyRequest) error {
	_, err := r.call(ctx, func(ctx context.Context) (any, error) {
		switch a {
		case "add-rack":
			n := q.Hosts
			if n == 0 {
				n = r.cfg.HostsPerRack
			}
			if n < 1 || n > r.cfg.MaxVMs {
				return nil, &api.Error{Status: 422, Msg: "invalid hosts"}
			}
			return nil, r.addRack(q.Rack, n)
		case "add-host":
			return nil, r.addHost(q.Rack, q.Host)
		case "drain", "remove-rack":
			var ids []string
			if q.Host == "" && r.hosts[q.Rack] != nil {
				q.Host = q.Rack
			}
			if r.hosts[q.Host] != nil {
				ids = []string{q.Host}
			} else {
				for _, rack := range r.racks {
					if rack.ID == q.Rack {
						ids = rack.Hosts
					}
				}
			}
			if len(ids) == 0 {
				return nil, &api.Error{Status: 404, Msg: "drain target not found"}
			}
			for _, id := range ids {
				h := r.hosts[id]
				h.admin = api.AdminDraining
				if err := h.agent.Cordon(ctx, true); err != nil {
					return nil, err
				}
				if err := r.scheduler.SetHostAdmin(ctx, id, api.AdminDraining); err != nil {
					return nil, err
				}
			}
			return nil, nil
		default:
			return nil, &api.Error{Status: 422, Msg: "unknown topology action"}
		}
	})
	return err
}
func (r *runtime) Reset(ctx context.Context) error {
	_, err := r.call(ctx, func(ctx context.Context) (any, error) {
		r.stop()
		r.hosts = map[string]*host{}
		r.racks = nil
		r.nextRack = 0
		r.nextHost = 0
		r.logIncarnation++
		r.cached = api.Snapshot{}
		r.compromised = map[string]bool{}
		r.success = api.SecurityStatus{ByAttack: map[string]int{}}
		r.security = api.SecurityStatus{ByAttack: map[string]int{}}
		r.keys = map[string]string{}
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err := os.Remove(r.options.StorePath + suffix); err != nil && !os.IsNotExist(err) {
				return nil, err
			}
		}
		return nil, r.start()
	})
	return err
}
func (r *runtime) Attack(ctx context.Context, id, attack string) error {
	_, err := r.call(ctx, func(ctx context.Context) (any, error) {
		v, err := r.scheduler.GetVM(ctx, id)
		if err != nil {
			return nil, err
		}
		h := r.hosts[v.HostID]
		if h == nil {
			return nil, &api.Error{Status: 404, Msg: "VM host not found"}
		}
		var outcome error
		switch attack {
		case api.AttackImpersonate:
			outcome = h.driver.GuestSend(id, h.addr, api.PortVsock, api.GuestRegister{VMID: "another-tenant", Generation: v.Generation, Token: "attacker"})
		case api.AttackReplay:
			outcome = h.driver.GuestSend(id, h.addr, api.PortVsock, api.GuestRegister{VMID: id, Generation: v.Generation - 1, Token: "old-token"})
		case api.AttackFlood:
			outcome = h.driver.GuestSend(id, h.addr, api.PortVsock, make([]byte, r.cfg.GuestMsgMaxBytes+1))
		case api.AttackEastWest:
			outcome = h.driver.GuestSend(id, api.Addr{Kind: api.KindGuest, ID: "neighbour", Host: h.addr.ID, Rack: h.addr.Rack}, api.PortGuest, []byte("attack"))
		case api.AttackControl:
			outcome = h.driver.GuestSend(id, schedulerAddr, api.PortControl, api.HostHeartbeat{HostID: h.addr.ID})
		case api.AttackEgress:
			outcome = h.driver.GuestSend(id, api.Addr{Kind: api.KindInternet, ID: "attacker.invalid"}, api.PortHTTPS, []byte("exfiltrate"))
		case api.AttackDiskFill:
			outcome = h.driver.WriteDisk(id, math.MaxInt32)
		default:
			return nil, &api.Error{Status: 422, Msg: "unknown attack"}
		}
		if attack == api.AttackImpersonate || attack == api.AttackReplay || attack == api.AttackFlood {
			return nil, outcome
		} // verdict is produced by the agent, never inferred from a permitted vsock send
		blocked := errors.Is(outcome, api.ErrDenied) || (attack == api.AttackDiskFill && outcome != nil)
		if blocked {
			return nil, h.agent.RecordBlocked(ctx, id, attack)
		}
		if outcome == nil {
			r.success.Succeeded++
			r.success.ByAttack[attack]++
			return nil, nil
		}
		return nil, outcome
	})
	return err
}

func (r *runtime) CreateVM(ctx context.Context, key string, q api.CreateVMRequest) (api.CreateVMResponse, error) {
	v, err := r.call(ctx, func(ctx context.Context) (any, error) {
		if r.down {
			return nil, &api.Error{Status: 503, Msg: "scheduler down", RetryAfterS: 1}
		}
		response, err := r.scheduler.CreateVM(ctx, key, q)
		if err == nil && r.keys[response.ID] == "" {
			r.keys[response.ID] = key
		}
		return response, err
	})
	if err != nil {
		return api.CreateVMResponse{}, err
	}
	return v.(api.CreateVMResponse), nil
}
func (r *runtime) GetVM(ctx context.Context, id string) (api.VM, error) {
	v, err := r.call(ctx, func(ctx context.Context) (any, error) {
		if r.down {
			return nil, &api.Error{Status: 503, Msg: "scheduler down"}
		}
		return r.scheduler.GetVM(ctx, id)
	})
	if err != nil {
		return api.VM{}, err
	}
	return v.(api.VM), nil
}
func (r *runtime) ListVMs(ctx context.Context) ([]api.VM, error) {
	v, err := r.call(ctx, func(ctx context.Context) (any, error) {
		if r.down {
			return nil, &api.Error{Status: 503, Msg: "scheduler down"}
		}
		return r.scheduler.ListVMs(ctx)
	})
	if err != nil {
		return nil, err
	}
	return v.([]api.VM), nil
}
func (r *runtime) DeleteVM(ctx context.Context, id string) (api.VM, error) {
	v, err := r.call(ctx, func(ctx context.Context) (any, error) {
		if r.down {
			return nil, &api.Error{Status: 503, Msg: "scheduler down"}
		}
		return r.scheduler.DeleteVM(ctx, id)
	})
	if err != nil {
		return api.VM{}, err
	}
	return v.(api.VM), nil
}
func (r *runtime) ListHosts(ctx context.Context) ([]api.Host, error) {
	s, err := r.Snapshot(ctx)
	return s.Hosts, err
}
func (r *runtime) Queue(ctx context.Context) ([]api.QueueEntry, error) {
	v, err := r.call(ctx, func(ctx context.Context) (any, error) {
		if r.down {
			return nil, &api.Error{Status: 503, Msg: "scheduler down"}
		}
		return r.scheduler.Queue(ctx)
	})
	if err != nil {
		return nil, err
	}
	return v.([]api.QueueEntry), nil
}
func (r *runtime) SetPriority(ctx context.Context, id string, p api.Priority) error {
	_, err := r.call(ctx, func(ctx context.Context) (any, error) {
		if r.down {
			return nil, &api.Error{Status: 503, Msg: "scheduler down"}
		}
		return nil, r.scheduler.SetPriority(ctx, id, p)
	})
	return err
}
