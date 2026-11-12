package fleet

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"time"

	"github.com/hami-sh/vm/internal/actor"
	"github.com/hami-sh/vm/internal/agent"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/driver"
	"github.com/hami-sh/vm/internal/invariant"
	"github.com/hami-sh/vm/internal/simnet"
)

var schedulerAddr = api.Addr{Kind: api.KindScheduler, ID: "scheduler"}

type host struct {
	addr   api.Addr
	agent  *agent.Agent
	driver *driver.Fake
	cancel context.CancelFunc
	admin  api.AdminState
	faults []string
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
	epoch              int64
	compromised        map[string]bool
	success            api.SecurityStatus
}

func newRuntime(ctx context.Context, cfg config.Config, c clock.Clock, o Options) (*runtime, error) {
	r := &runtime{ctx: ctx, cfg: cfg, clock: c, options: o, inbox: make(chan operation), done: make(chan struct{}), hosts: map[string]*host{}, compromised: map[string]bool{}, success: api.SecurityStatus{ByAttack: map[string]int{}}}
	if r.options.SchedulerFactory == nil {
		r.options.SchedulerFactory = newStandIn
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
	r.epoch++
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
		for {
			r.nextHost++
			id = fmt.Sprintf("%s-h%d", rack, r.nextHost)
			if r.hosts[id] == nil {
				break
			}
		}
	}
	if r.hosts[id] != nil {
		return &api.Error{Status: 422, Msg: "host exists"}
	}
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
	return nil
}
func (r *runtime) ControlPlane() api.ControlPlane { return r }
func (r *runtime) Snapshot(ctx context.Context) (api.Snapshot, error) {
	s, _, _, err := r.observe(ctx)
	return s, err
}
func (r *runtime) observe(ctx context.Context) (api.Snapshot, []invariant.Observation, int64, error) {
	v, err := r.call(ctx, func(ctx context.Context) (any, error) { return r.read(ctx) })
	if err != nil {
		return api.Snapshot{}, nil, 0, err
	}
	o := v.(observed)
	return o.snapshot, o.raw, o.epoch, nil
}
func (r *runtime) read(ctx context.Context) (observed, error) {
	s := api.Snapshot{}
	if r.scheduler != nil && !r.down {
		var err error
		s, err = r.scheduler.Snapshot(ctx)
		if err != nil {
			return observed{}, err
		}
	} else {
		s.Scheduler = api.SchedulerDown
	}
	s.Racks = append([]api.Rack(nil), r.racks...)
	s.At = r.clock.Now().UnixMilli()
	leases := map[string]api.Host{}
	for _, h := range s.Hosts {
		leases[h.ID] = h
	}
	var raw []invariant.Observation
	s.Security = api.SecurityStatus{ByAttack: map[string]int{}, Succeeded: r.success.Succeeded}
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
		sh, ok := leases[id]
		connected := ok && sh.Lease == api.LeaseHealthy
		a.Host.AdminState = h.admin
		a.Host.Lease = sh.Lease
		a.Host.Faults = slices.Clone(h.faults)
		raw = append(raw, invariant.Observation{Host: a.Host, Actual: a.Actual, Connected: connected, Security: a.Security})
		if !ok {
			s.Hosts = append(s.Hosts, a.Host)
		}
		s.Security.Blocked += a.Security.Blocked
		s.Security.Succeeded += a.Security.Succeeded
		for k, n := range a.Security.ByAttack {
			s.Security.ByAttack[k] += n
		}
	}
	for i := range s.Hosts {
		if h := r.hosts[s.Hosts[i].ID]; h != nil {
			s.Hosts[i].Faults = slices.Clone(h.faults)
		}
	}
	for i := range s.VMs {
		s.VMs[i].Compromised = r.compromised[s.VMs[i].ID]
	}
	return observed{snapshot: s, raw: raw, epoch: r.epoch}, nil
}
func (r *runtime) Apply(ctx context.Context, action string, req api.ChaosRequest) error {
	_, err := r.call(ctx, func(ctx context.Context) (any, error) { return nil, r.apply(ctx, action, req) })
	return err
}
func (r *runtime) apply(ctx context.Context, a string, q api.ChaosRequest) error {
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
		if s, ok := r.scheduler.(interface {
			SetStoreDelay(context.Context, time.Duration) error
		}); ok {
			return s.SetStoreDelay(ctx, time.Duration(param("ms", 200))*time.Millisecond)
		}
		return &api.Error{Status: 501, Msg: "scheduler slow-store hook pending"}
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
				if err := r.hosts[id].agent.Kill(ctx); err != nil {
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
	if h == nil {
		return &api.Error{Status: 404, Msg: "host or placed VM not found"}
	}
	if !slices.Contains(h.faults, a) {
		h.faults = append(h.faults, a)
	}
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
		h.faults = slices.DeleteFunc(h.faults, func(x string) bool { return x == "pause-heartbeats" || x == "resume-heartbeats" })
		return h.agent.PauseHeartbeats(ctx, false)
	case "clock-skew":
		ms := param("ms", 30000)
		if math.Abs(ms) > 30000 {
			return &api.Error{Status: 422, Msg: "clock skew exceeds 30 seconds"}
		}
		return h.agent.SetClockSkew(ctx, time.Duration(ms)*time.Millisecond)
	case "fill-disk":
		h.driver.FillDisk(param("on", 1) != 0)
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
		if param("reverse", 0) != 0 {
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
		r.compromised[q.Target] = true
	default:
		return &api.Error{Status: 422, Msg: "unknown component fault"}
	}
	return nil
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
		r.compromised = map[string]bool{}
		r.success = api.SecurityStatus{ByAttack: map[string]int{}}
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
		return r.scheduler.CreateVM(ctx, key, q)
	})
	if err != nil {
		return api.CreateVMResponse{}, err
	}
	return v.(api.CreateVMResponse), nil
}
func (r *runtime) GetVM(ctx context.Context, id string) (api.VM, error) {
	v, err := r.call(ctx, func(ctx context.Context) (any, error) { return r.scheduler.GetVM(ctx, id) })
	if err != nil {
		return api.VM{}, err
	}
	return v.(api.VM), nil
}
func (r *runtime) ListVMs(ctx context.Context) ([]api.VM, error) {
	v, err := r.call(ctx, func(ctx context.Context) (any, error) { return r.scheduler.ListVMs(ctx) })
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
	v, err := r.call(ctx, func(ctx context.Context) (any, error) { return r.scheduler.Queue(ctx) })
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
