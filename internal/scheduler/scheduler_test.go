package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/store"
	"github.com/hami-sh/vm/internal/vm"
)

type harness struct {
	t  *testing.T
	s  *Scheduler
	c  *clock.Fake
	db *store.Store
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	db, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	c := clock.NewFake(time.Unix(1000, 0))
	d, epoch, e := db.Start(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	s := &Scheduler{cfg: config.Default(), clock: c, store: db, state: newState(d, epoch, c.Now(), config.Default())}
	s.state.fresh = true
	return &harness{t, s, c, db}
}
func (h *harness) step(msg any) Effect {
	h.t.Helper()
	e := h.s.handle(h.c.Now(), msg)[0]
	if err := h.db.Commit(context.Background(), h.s.state.data, e.next.data, e.events, h.s.cfg.EventRetention); err != nil {
		h.t.Fatal(err)
	}
	h.s.state = e.next
	for _, ev := range e.events {
		if ev.Kind == api.EventTransition && !vm.Legal(ev.From, ev.To) {
			h.t.Fatalf("illegal event %+v", ev)
		}
	}
	if len(h.s.state.data.Queue) > h.s.cfg.QueueCap {
		h.t.Fatal("queue overflow")
	}
	return e
}
func (h *harness) agent(id, rack string, msg any) Effect {
	return h.step(api.Envelope{From: api.Addr{Kind: api.KindAgent, ID: id, Host: id, Rack: rack}, Port: api.PortControl, Msg: msg})
}
func (h *harness) register(id, rack string, slots int, items ...api.InventoryItem) Effect {
	return h.agent(id, rack, api.RegisterHost{HostID: id, Rack: rack, Slots: slots, Inventory: items})
}
func (h *harness) create(key, pr string) api.VM {
	h.t.Helper()
	e := h.step(request{op: "create", key: key, arg: api.CreateVMRequest{Pool: config.Pools[0].Name, Priority: pr}})
	if e.response.err != nil {
		h.t.Fatal(e.response.err)
	}
	id := e.response.value.(api.CreateVMResponse).ID
	return h.s.state.data.VMs[id].VM
}
func (h *harness) ready(v api.VM) {
	h.agent(v.HostID, v.Rack, api.StartVMResult{VMID: v.ID, Generation: v.Generation, HostID: v.HostID, Admitted: true})
	h.agent(v.HostID, v.Rack, api.ReadyReport{VMID: v.ID, Generation: v.Generation, HostID: v.HostID})
}
func status(t *testing.T, e error, want int) {
	t.Helper()
	var a *api.Error
	if !errors.As(e, &a) || a.Status != want {
		t.Fatalf("error %v, want HTTP %d", e, want)
	}
	if (want == 429 || want == 503) && a.RetryAfterS < 1 {
		t.Fatal("missing Retry-After")
	}
}
func hasStop(e Effect, id string, gen int64) bool {
	for _, d := range e.sends {
		if m, ok := d.msg.(api.StopVM); ok && m.VMID == id && m.Generation == gen {
			return true
		}
	}
	return false
}

func TestPlacementSpreadsEachPoolAndBoundsCapacity(t *testing.T) {
	h := newHarness(t)
	for r := 1; r <= 3; r++ {
		for i := 1; i <= 4; i++ {
			h.register(fmt.Sprintf("r%d-h%d", r, i), fmt.Sprintf("r%d", r), 2)
		}
	}
	for n := 0; n < 30; n++ {
		p := config.Pools[n%2].Name
		e := h.step(request{op: "create", arg: api.CreateVMRequest{Pool: p}})
		if e.response.err != nil {
			t.Fatal(e.response.err)
		}
		v := h.s.state.data.VMs[e.response.value.(api.CreateVMResponse).ID].VM
		if v.State == api.VMBooting {
			h.ready(v)
		}
	}
	s := h.step(request{op: "snapshot"}).response.value.(api.Snapshot)
	if s.Counters.ByState[api.VMReady] != 24 || len(s.Queue) != 6 {
		t.Fatalf("ready=%d queued=%d", s.Counters.ByState[api.VMReady], len(s.Queue))
	}
	counts := map[string]int{}
	for _, v := range s.VMs {
		if v.State == api.VMReady {
			counts[v.Pool+v.Rack]++
		}
	}
	for _, p := range config.Pools {
		for _, r := range []string{"r1", "r2", "r3"} {
			if counts[p.Name+r] != 4 {
				t.Fatalf("per-pool rack imbalance: %v", counts)
			}
		}
	}
	for _, host := range s.Hosts {
		if len(host.Slots) != 2 {
			t.Fatalf("host overfull %+v", host)
		}
	}
}

func TestQueueAgingOrderingSheddingAndPriority(t *testing.T) {
	h := newHarness(t)
	// FIFO must also hold when the demo clock is paused.
	first := h.create("fifo-first", "standard")
	second := h.create("fifo-second", "standard")
	fifo := h.step(request{op: "queue"}).response.value.([]api.QueueEntry)
	if fifo[0].VMID != first.ID || fifo[1].VMID != second.ID {
		t.Fatalf("same-millisecond FIFO broken: %+v", fifo)
	}
	h.step(request{op: "delete", id: first.ID})
	h.step(request{op: "delete", id: second.ID})
	old := h.create("old", "batch")
	h.c.Advance(2 * h.s.cfg.AgingStep)
	interactive := h.create("interactive", "interactive")
	q := h.step(request{op: "queue"}).response.value.([]api.QueueEntry)
	if q[0].VMID != old.ID || q[0].EffectivePriority != 1 || q[1].VMID != interactive.ID {
		t.Fatalf("aging/order %+v", q)
	}
	var newest api.VM
	for n := 2; n < 32; n++ {
		h.c.Advance(time.Millisecond)
		newest = h.create(fmt.Sprint(n), "batch")
	}
	e := h.step(request{op: "create", arg: api.CreateVMRequest{Pool: old.Pool, Priority: "batch"}})
	status(t, e.response.err, 429)
	accepted := h.create("high", "interactive")
	if h.s.state.data.VMs[newest.ID].VM.LastReason != api.ReasonShed {
		t.Fatal("did not shed newest worst request")
	}
	e = h.step(request{op: "priority", id: accepted.ID, arg: api.PriorityReplacement})
	status(t, e.response.err, 422)
	e = h.step(request{op: "priority", id: accepted.ID, arg: api.PriorityBatch})
	if e.response.err != nil {
		t.Fatal(e.response.err)
	}
	if h.s.state.data.VMs[accepted.ID].VM.Priority != api.PriorityInteractive {
		t.Fatal("changed requested priority")
	}
	if len(h.s.state.data.Queue) != 32 {
		t.Fatal("queue bound")
	}
}

func TestIdempotentCreateAndDeletePending(t *testing.T) {
	h := newHarness(t)
	v := h.create("same", "standard")
	other := h.create("same", "standard")
	if v.ID != other.ID || len(h.s.state.data.VMs) != 1 {
		t.Fatal("duplicate VM")
	}
	e := h.step(request{op: "create", key: "same", arg: api.CreateVMRequest{Pool: v.Pool, Priority: "batch"}})
	status(t, e.response.err, 422)
	h.step(request{op: "delete", id: v.ID})
	if h.s.state.data.VMs[v.ID].VM.State != api.VMTerminated || len(h.s.state.data.Queue) != 0 {
		t.Fatal("pending delete did not atomically dequeue")
	}
	if got := h.create("same", "standard"); got.ID != v.ID || got.State != api.VMTerminated {
		t.Fatal("deleted key reused")
	}
}

func TestDeleteBootingRejectsLateReadyAndHoldsSlot(t *testing.T) {
	h := newHarness(t)
	h.register("h1", "r1", 1)
	v := h.create("v", "standard")
	e := h.step(request{op: "delete", id: v.ID})
	if !hasStop(e, v.ID, v.Generation) {
		t.Fatal("no stop")
	}
	e = h.agent("h1", "r1", api.ReadyReport{HostID: "h1", VMID: v.ID, Generation: v.Generation})
	if !hasStop(e, v.ID, v.Generation) || h.s.state.data.VMs[v.ID].VM.State != api.VMTerminating {
		t.Fatal("late ready resurrected deleted VM")
	}
	waiting := h.create("waiting", "standard")
	if waiting.State != api.VMPending {
		t.Fatal("reused releasing slot")
	}
	h.register("h1", "r1", 1, api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMReady})
	if h.s.state.data.VMs[v.ID].VM.State != api.VMTerminating {
		t.Fatal("terminated before clean report")
	}
	h.agent("h1", "r1", api.StoppedReport{HostID: "h1", VMID: v.ID, Generation: v.Generation})
	if h.s.state.data.VMs[v.ID].VM.State != api.VMTerminated || h.s.state.data.VMs[waiting.ID].VM.State != api.VMBooting {
		t.Fatal("slot leaked")
	}
}

func TestHostLossReplacementFencesReturningCopy(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	h.register("b", "r2", 1)
	v := h.create("v", "batch")
	h.ready(v)
	h.c.Advance(h.s.cfg.HostLeaseTTL)
	h.step(tick{})
	if h.s.state.hosts[v.HostID].lease != api.LeaseSuspect {
		t.Fatal("host not suspect")
	}
	// Refresh only the surviving host so it can take the replacement.
	h.register("b", "r2", 1)
	h.c.Advance(h.s.cfg.HostLostGrace)
	h.agent("b", "r2", api.HostHeartbeat{HostID: "b", Seq: 1})
	h.step(tick{})
	next := h.s.state.data.VMs[v.ID].VM
	if next.Generation != v.Generation+1 || next.HostID != "b" {
		t.Fatalf("replacement %+v", next)
	}
	h.ready(next)
	e := h.register("a", "r1", 1, api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMReady, Pool: v.Pool})
	if !hasStop(e, v.ID, v.Generation) {
		t.Fatal("returning stale copy not stopped")
	}
	h.agent("a", "r1", api.ReadyReport{HostID: "a", VMID: v.ID, Generation: v.Generation})
	h.agent("a", "r1", api.StoppedReport{HostID: "a", VMID: v.ID, Generation: v.Generation})
	if got := h.s.state.data.VMs[v.ID].VM; got.Generation != next.Generation || got.State != api.VMReady || got.HostID != "b" {
		t.Fatalf("stale reports corrupted current VM %+v", got)
	}
}

func TestReplacementNeverShedAndBootRetryBudget(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	v := h.create("v", "standard")
	h.ready(v)
	for i := 0; i < 32; i++ {
		h.c.Advance(time.Millisecond)
		h.create(fmt.Sprint(i), "batch")
	}
	h.c.Advance(h.s.cfg.HostLeaseTTL + h.s.cfg.HostLostGrace)
	h.step(tick{})
	q := h.s.state.data.Queue[v.ID]
	if q.Reason != api.QueueReplace || q.Priority != 0 || len(h.s.state.data.Queue) != 32 {
		t.Fatalf("replacement %+v", q)
	}
	for n := 0; n < 3; n++ {
		h.step(request{op: "create", arg: api.CreateVMRequest{Pool: v.Pool, Priority: "interactive"}})
	}
	if h.s.state.data.VMs[v.ID].VM.State != api.VMPending {
		t.Fatal("replacement shed")
	}

	b := newHarness(t)
	b.register("a", "r1", 1)
	b.register("b", "r2", 1)
	boot := b.create("boot", "standard")
	for attempt := 1; attempt <= b.s.cfg.MaxAttempts; attempt++ {
		cur := b.s.state.data.VMs[boot.ID].VM
		if cur.Attempts != attempt {
			t.Fatalf("attempt %+v", cur)
		}
		b.agent(cur.HostID, cur.Rack, api.StartVMResult{HostID: cur.HostID, VMID: cur.ID, Generation: cur.Generation, Admitted: true})
		b.c.Advance(b.s.cfg.BootDeadline)
		// Keep the stub hosts alive while their boot hangs.
		for _, id := range []string{"a", "b"} {
			hh := b.s.state.hosts[id]
			b.agent(id, hh.record.Rack, api.HostHeartbeat{HostID: id, Seq: uint64(attempt * 2), Inventory: hh.inventory})
		}
		e := b.step(tick{})
		if !hasStop(e, cur.ID, cur.Generation) {
			t.Fatal("deadline did not stop boot")
		}
		b.agent(cur.HostID, cur.Rack, api.StoppedReport{HostID: cur.HostID, VMID: cur.ID, Generation: cur.Generation})
		if attempt < b.s.cfg.MaxAttempts {
			q := b.s.state.data.Queue[boot.ID]
			if q.Priority != api.PriorityStandard || q.Reason != api.QueueRetry || q.EnqueuedAt != boot.CreatedAt {
				t.Fatal("retry gained priority or lost arrival")
			}
			b.c.Advance(b.s.cfg.BackoffMax)
			for _, id := range []string{"a", "b"} {
				hh := b.s.state.hosts[id]
				b.agent(id, hh.record.Rack, api.HostHeartbeat{HostID: id, Seq: uint64(attempt*2 + 1), Inventory: hh.inventory})
			}
			b.step(tick{})
		}
	}
	if got := b.s.state.data.VMs[boot.ID].VM; got.State != api.VMFailed || got.Attempts != 3 || got.LastReason != api.ReasonRetryBudget {
		t.Fatalf("retry not bounded %+v", got)
	}
}

func TestReconcileAdoptStopMissingAndHigherGeneration(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 2)
	v := h.create("v", "standard")
	h.ready(v)
	item := api.InventoryItem{VMID: v.ID, Generation: v.Generation, State: api.VMReady, Pool: v.Pool}
	h.register("a", "r1", 2, item)
	if h.s.state.data.VMs[v.ID].VM.Generation != v.Generation {
		t.Fatal("adoption unnecessarily bumped generation")
	}
	item.Generation = 9
	e := h.register("a", "r1", 2, item)
	if h.s.state.data.VMs[v.ID].VM.Generation != 9 {
		t.Fatal("did not adopt higher generation")
	}
	alert := false
	for _, e := range e.events {
		alert = alert || e.Kind == api.EventAlert
	}
	if !alert {
		t.Fatal("no store-loss alert")
	}
	e = h.register("b", "r2", 2, item)
	if !hasStop(e, v.ID, 9) || h.s.state.data.VMs[v.ID].VM.HostID != "a" {
		t.Fatal("equal generation copy stole ownership")
	}
	item.Generation = 10
	h.register("b", "r2", 2, item)
	if h.s.state.data.VMs[v.ID].VM.HostID != "b" || h.s.state.data.VMs[v.ID].VM.Generation != 10 {
		t.Fatal("highest generation did not win")
	}
	h.register("b", "r2", 2)
	got := h.s.state.data.VMs[v.ID].VM
	if got.Generation != 11 || got.State != api.VMBooting {
		t.Fatalf("missing VM not re-placed %+v", got)
	}
	e = h.register("c", "r3", 2, api.InventoryItem{VMID: "unknown", Generation: 2, State: api.VMBooting})
	if !hasStop(e, "unknown", 2) {
		t.Fatal("unknown boot not stopped")
	}
}

func TestFreshStoreAdoptionAndDown(t *testing.T) {
	h := newHarness(t)
	item := api.InventoryItem{VMID: "vm-old-id", Generation: 45, State: api.VMReady, Pool: config.Pools[0].Name}
	e := h.register("a", "r1", 2, item)
	if got := h.s.state.data.VMs[item.VMID].VM; got.State != api.VMReady || got.Generation != 45 {
		t.Fatalf("fresh adoption %+v", got)
	}
	found := false
	for _, ev := range e.events {
		if ev.Kind == api.EventAlert && strings.Contains(ev.Detail, "idempotency key lost") {
			found = true
		}
	}
	if !found {
		t.Fatal("missing key-loss alert")
	}
	h.s.state.fresh = false
	e = h.register("b", "r2", 1, api.InventoryItem{VMID: "unknown-ready", Generation: 1, State: api.VMReady, Pool: item.Pool})
	if !hasStop(e, "unknown-ready", 1) {
		t.Fatal("nonfresh store adopted unknown")
	}
	h.step(request{op: "down", arg: true})
	e = h.step(request{op: "create", arg: api.CreateVMRequest{Pool: item.Pool}})
	status(t, e.response.err, 503)
}

// testNetwork stamps sender identity and delivers asynchronously through inboxes.
// The stub agent below speaks only the frozen protocol, not scheduler internals.
type testNetwork struct {
	mu        sync.Mutex
	endpoints map[string]*testEndpoint
}
type testEndpoint struct {
	n    *testNetwork
	addr api.Addr
	in   chan api.Envelope
}

func (n *testNetwork) Attach(a api.Addr) (api.Endpoint, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.endpoints == nil {
		n.endpoints = map[string]*testEndpoint{}
	}
	key := string(a.Kind) + a.ID
	if n.endpoints[key] != nil {
		return nil, fmt.Errorf("already attached")
	}
	e := &testEndpoint{n, a, make(chan api.Envelope, 1024)}
	n.endpoints[key] = e
	return e, nil
}
func (e *testEndpoint) Addr() api.Addr             { return e.addr }
func (e *testEndpoint) Inbox() <-chan api.Envelope { return e.in }
func (e *testEndpoint) Close() {
	e.n.mu.Lock()
	defer e.n.mu.Unlock()
	key := string(e.addr.Kind) + e.addr.ID
	if e.n.endpoints[key] == e {
		delete(e.n.endpoints, key)
		close(e.in)
	}
}
func (e *testEndpoint) Send(to api.Addr, port api.Port, msg any) error {
	e.n.mu.Lock()
	defer e.n.mu.Unlock()
	dest := e.n.endpoints[string(to.Kind)+to.ID]
	if dest == nil {
		return nil
	}
	select {
	case dest.in <- api.Envelope{From: e.addr, To: to, Port: port, Msg: msg}:
		return nil
	default:
		return fmt.Errorf("test inbox full")
	}
}

func await(t *testing.T, ctx context.Context, fn func() bool) {
	t.Helper()
	for !fn() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
			runtime.Gosched()
		}
	}
}

func TestActorConcurrentIdempotencyAndRealRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := clock.NewFake(time.Unix(1000, 0))
	net := &testNetwork{}
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := net.Attach(api.Addr{Kind: api.KindAgent, ID: "a", Host: "a", Rack: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	sctx, kill := context.WithCancel(ctx)
	defer kill()
	s, err := New(sctx, config.Default(), c, net, db)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { kill(); <-s.Done(); db.Close() }()
	// Stub runs like a real agent: answers Start and Stop using only transport.
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	defer cancel()
	registrations := make(chan []api.InventoryItem)
	register := func(items ...api.InventoryItem) {
		select {
		case registrations <- items:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	go func() {
		defer wg.Done()
		var maxEpoch int64
		metadata := map[string]api.StartVM{}
		for {
			select {
			case <-ctx.Done():
				return
			case inventory := <-registrations:
				for j, i := range inventory {
					if m, ok := metadata[i.VMID]; ok {
						inventory[j].IdempotencyKey = m.IdempotencyKey
						inventory[j].RequestHash = m.RequestHash
					}
				}
				_ = agent.Send(Address, api.PortControl, api.RegisterHost{HostID: "a", Rack: "r1", Slots: 2, Epoch: maxEpoch, Inventory: inventory})
			case e := <-agent.Inbox():
				switch m := e.Msg.(type) {
				case api.StartVM:
					if m.Epoch < maxEpoch {
						continue
					}
					maxEpoch = m.Epoch
					metadata[m.VMID] = m
					_ = agent.Send(Address, api.PortControl, api.StartVMResult{VMID: m.VMID, Generation: m.Generation, HostID: "a", Admitted: true})
					_ = agent.Send(Address, api.PortControl, api.ReadyReport{VMID: m.VMID, Generation: m.Generation, HostID: "a"})
				case api.StopVM:
					if m.Epoch < maxEpoch {
						continue
					}
					maxEpoch = m.Epoch
					_ = agent.Send(Address, api.PortControl, api.StoppedReport{VMID: m.VMID, Generation: m.Generation, HostID: "a"})
				case api.HeartbeatAck:
					if m.Epoch > maxEpoch {
						maxEpoch = m.Epoch
					}
				}
			}
		}
	}()
	c.Advance(config.Default().HostLeaseTTL)
	register()
	await(t, ctx, func() bool { hs, e := s.ListHosts(ctx); return e == nil && len(hs) == 1 })
	var calls sync.WaitGroup
	ids := make(chan string, 20)
	for i := 0; i < 20; i++ {
		calls.Go(func() {
			v, e := s.CreateVM(ctx, "same", api.CreateVMRequest{Pool: config.Pools[0].Name})
			if e != nil {
				t.Error(e)
			}
			ids <- v.ID
		})
	}
	calls.Wait()
	close(ids)
	var id string
	for v := range ids {
		if id != "" && id != v {
			t.Fatal("concurrent duplicate creates")
		}
		id = v
	}
	await(t, ctx, func() bool { v, e := s.GetVM(ctx, id); return e == nil && v.State == api.VMReady })
	before, err := s.GetVM(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := s.Snapshot(ctx)
	kill()
	<-s.Done()
	_, err = s.CreateVM(ctx, "new", api.CreateVMRequest{Pool: before.Pool})
	status(t, err, 503)
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	sctx, kill = context.WithCancel(ctx)
	defer kill()
	s, err = New(sctx, config.Default(), c, net, db)
	if err != nil {
		t.Fatal(err)
	}
	after, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Epoch != first.Epoch+1 || len(after.Hosts) != 1 || after.Hosts[0].Lease != api.LeaseSuspect {
		t.Fatalf("restart not loaded from SQLite %+v", after)
	}
	register(api.InventoryItem{VMID: id, Generation: before.Generation, State: api.VMReady, Pool: before.Pool})
	await(t, ctx, func() bool { hs, e := s.ListHosts(ctx); return e == nil && hs[0].Lease == api.LeaseHealthy })
	v, err := s.GetVM(ctx, id)
	if err != nil || v.Generation != before.Generation || v.State != api.VMReady {
		t.Fatalf("generation changed across restart %+v %v", v, err)
	}
	duplicate, err := s.CreateVM(ctx, "same", api.CreateVMRequest{Pool: before.Pool})
	if err != nil || duplicate.ID != id {
		t.Fatal("idempotency identity lost on restart")
	}
	// Missing inventory after another host registration must boot at gen+1.
	register()
	await(t, ctx, func() bool {
		v, e := s.GetVM(ctx, id)
		return e == nil && v.Generation == before.Generation+1 && v.State == api.VMReady
	})
	// Several real restarts in the exact same paused millisecond put the
	// epoch ahead of any clock-derived counter. The agent retains its fence.
	for n := 0; n < 3; n++ {
		current, _ := s.GetVM(ctx, id)
		old, _ := s.Snapshot(ctx)
		kill()
		<-s.Done()
		db.Close()
		db, err = store.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		sctx, kill = context.WithCancel(ctx)
		defer kill()
		s, err = New(sctx, config.Default(), c, net, db)
		if err != nil {
			t.Fatal(err)
		}
		register(api.InventoryItem{VMID: id, Generation: current.Generation, State: api.VMReady, Pool: current.Pool})
		await(t, ctx, func() bool {
			hs, e := s.ListHosts(ctx)
			return e == nil && len(hs) == 1 && hs[0].Lease == api.LeaseHealthy
		})
		next, _ := s.Snapshot(ctx)
		if next.Epoch <= old.Epoch || next.At != old.At {
			t.Fatal("paused restart changed time or regressed epoch")
		}
	}
	// A wiped file starts at epoch 1; the reported fence must advance and be
	// persisted before either the ack or any new StartVM can reach the agent.
	beforeWipe, _ := s.GetVM(ctx, id)
	beforeSnap, _ := s.Snapshot(ctx)
	kill()
	<-s.Done()
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	sctx, kill = context.WithCancel(ctx)
	defer kill()
	s, err = New(sctx, config.Default(), c, net, db)
	if err != nil {
		t.Fatal(err)
	}
	register(api.InventoryItem{VMID: id, Generation: beforeWipe.Generation, State: api.VMReady, Pool: before.Pool})
	await(t, ctx, func() bool {
		v, e := s.GetVM(ctx, id)
		return e == nil && v.Generation == beforeWipe.Generation && v.State == api.VMReady
	})
	wiped, _ := s.Snapshot(ctx)
	if wiped.Epoch <= beforeSnap.Epoch {
		t.Fatal("wiped-store epoch went backwards")
	}
	_, err = s.CreateVM(ctx, "same", api.CreateVMRequest{Pool: before.Pool})
	status(t, err, 503)
	c.Advance(config.Default().HostLeaseTTL - time.Millisecond)
	_, err = s.CreateVM(ctx, "same", api.CreateVMRequest{Pool: before.Pool})
	status(t, err, 503)
	c.Advance(time.Millisecond)
	register(api.InventoryItem{VMID: id, Generation: beforeWipe.Generation, State: api.VMReady, Pool: before.Pool})
	recovered, err := s.CreateVM(ctx, "same", api.CreateVMRequest{Pool: before.Pool})
	if err != nil || recovered.ID != id {
		t.Fatalf("wiped-store idempotency not recovered: %+v %v", recovered, err)
	}
	_, err = s.CreateVM(ctx, "same", api.CreateVMRequest{Pool: before.Pool, Priority: "batch"})
	status(t, err, 422)
	newVM, err := s.CreateVM(ctx, "after-wipe", api.CreateVMRequest{Pool: before.Pool})
	if err != nil {
		t.Fatal(err)
	}
	await(t, ctx, func() bool { v, e := s.GetVM(ctx, newVM.ID); return e == nil && v.State == api.VMReady })
}

func TestReservationTimeoutRefusalAndRecycle(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	h.register("b", "r2", 1)
	v := h.create("v", "standard")
	h.c.Advance(h.s.cfg.ReservationTTL)
	for _, id := range []string{"a", "b"} {
		hh := h.s.state.hosts[id]
		h.agent(id, hh.record.Rack, api.HostHeartbeat{HostID: id, Seq: 1})
	}
	e := h.step(tick{})
	if !hasStop(e, v.ID, v.Generation) || h.s.state.data.VMs[v.ID].VM.LastReason != api.ReasonReservationTTL {
		t.Fatal("reservation did not expire")
	}
	h.c.Advance(h.s.cfg.BackoffMax)
	for _, id := range []string{"a", "b"} {
		hh := h.s.state.hosts[id]
		h.agent(id, hh.record.Rack, api.HostHeartbeat{HostID: id, Seq: 2, Inventory: hh.inventory})
	}
	h.step(tick{})
	retry := h.s.state.data.VMs[v.ID].VM
	if retry.HostID == v.HostID || retry.Generation != v.Generation+1 {
		t.Fatalf("did not exclude timed-out host: %+v", retry)
	}
	e = h.agent(retry.HostID, retry.Rack, api.StartVMResult{HostID: retry.HostID, VMID: v.ID, Generation: retry.Generation, Admitted: false, Reason: "full"})
	if h.s.state.data.VMs[v.ID].VM.State != api.VMPending {
		t.Fatal("refusal not retried")
	}
	if h.s.state.data.Queue[v.ID].Reason != api.QueueRetry || !hasStop(e, v.ID, retry.Generation) {
		t.Fatal("refusal got replacement priority")
	}

	r := newHarness(t)
	r.register("a", "r1", 1)
	good := r.create("ready", "interactive")
	r.ready(good)
	r.agent("a", "r1", api.UnhealthyReport{HostID: "a", VMID: good.ID, Generation: good.Generation, Reason: api.ReasonGuestLease})
	recycled := r.s.state.data.VMs[good.ID].VM
	if recycled.Generation != good.Generation+1 || recycled.HostID != "a" || recycled.State != api.VMBooting || len(r.s.state.data.Queue) != 0 {
		t.Fatalf("recycle %+v", recycled)
	}
	r.ready(recycled)
	s := r.step(request{op: "snapshot"}).response.value.(api.Snapshot)
	if len(s.Hosts[0].Slots) != 1 {
		t.Fatal("recycle double-counted slot")
	}
}

func TestFailedCommitSendsNoStartAndReturns503(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	net := &testNetwork{}
	agent, err := net.Attach(api.Addr{Kind: api.KindAgent, ID: "a", Host: "a", Rack: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	c := clock.NewFake(time.Unix(1000, 0))
	s, err := New(ctx, config.Default(), c, net, db)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); <-s.Done() }()
	c.Advance(config.Default().HostLeaseTTL)
	_ = agent.Send(Address, api.PortControl, api.RegisterHost{HostID: "a", Rack: "r1", Slots: 1})
	await(t, ctx, func() bool { hs, e := s.ListHosts(ctx); return e == nil && len(hs) == 1 })
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = s.CreateVM(ctx, "fail", api.CreateVMRequest{Pool: config.Pools[0].Name})
	status(t, err, 503)
	snap, err := s.Snapshot(ctx)
	if err != nil || snap.Scheduler != api.SchedulerDown || len(snap.VMs) != 0 {
		t.Fatalf("tentative intent was published: %+v %v", snap, err)
	}
	for {
		select {
		case e := <-agent.Inbox():
			if _, ok := e.Msg.(api.StartVM); ok {
				t.Fatal("StartVM sent before durable generation")
			}
		default:
			return
		}
	}
}

func TestAdoptedIdempotencyKeyChoosesHighestGeneration(t *testing.T) {
	for _, sameReport := range []bool{false, true} {
		t.Run(fmt.Sprint(sameReport), func(t *testing.T) {
			h := newHarness(t)
			key := "duplicate-key"
			low := api.InventoryItem{VMID: "low", Generation: 3, State: api.VMReady, Pool: config.Pools[0].Name, IdempotencyKey: key, RequestHash: "low-hash"}
			high := low
			high.VMID = "high"
			high.Generation = 8
			high.RequestHash = "high-hash"
			if sameReport {
				h.register("a", "r1", 2, low, high)
			} else {
				h.register("a", "r1", 2, low)
				h.register("b", "r2", 2, high)
			}
			if h.s.state.data.VMs[low.VMID].Key != "" || h.s.state.data.VMs[high.VMID].Key != key {
				t.Fatal("higher-generation claim did not win")
			}
			loaded, _, err := h.db.Start(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if loaded.VMs[high.VMID].Key != key || loaded.VMs[high.VMID].Hash != "high-hash" || loaded.VMs[low.VMID].Key != "" {
				t.Fatal("recovered key claim not persisted")
			}
		})
	}
}

func TestEventsSinceRetentionAndRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := config.Default()
	cfg.EventRetention = 80
	c := clock.NewFake(time.Unix(1000, 0))
	net := &testNetwork{}
	path := filepath.Join(t.TempDir(), "events.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, kill := context.WithCancel(ctx)
	defer kill()
	s, err := New(runCtx, cfg, c, net, db)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { kill(); <-s.Done(); db.Close() }()
	c.Advance(cfg.HostLeaseTTL)
	ids := []string{}
	for n := 0; n < 30; n++ {
		v, e := s.CreateVM(ctx, fmt.Sprint(n), api.CreateVMRequest{Pool: config.Pools[0].Name})
		if e != nil {
			t.Fatal(e)
		}
		ids = append(ids, v.ID)
	}
	events, truncated, err := s.EventsSince(ctx, 0)
	if err != nil || truncated || len(events) != 60 {
		t.Fatalf("flood audit incomplete: %d %v %v", len(events), truncated, err)
	}
	snap, _ := s.Snapshot(ctx)
	if len(snap.Recent) != cfg.RecentEventLimit {
		t.Fatal("UI tail is not capped independently")
	}
	for _, id := range ids {
		if _, err = s.DeleteVM(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	events, truncated, err = s.EventsSince(ctx, 0)
	if err != nil || !truncated || len(events) != 80 {
		t.Fatalf("retention gap not reported: %d %v %v", len(events), truncated, err)
	}
	first, last := events[0].Seq, events[len(events)-1].Seq
	for i, e := range events {
		if e.Seq != first+int64(i) {
			t.Fatal("unordered audit log")
		}
	}
	_, truncated, err = s.EventsSince(ctx, first-1)
	if err != nil || truncated {
		t.Fatal("cursor on retention boundary marked truncated")
	}
	_, truncated, err = s.EventsSince(ctx, first-2)
	if err != nil || !truncated {
		t.Fatal("cursor before retention boundary not marked truncated")
	}
	// Returned data must not alias the actor's log.
	events[0].Detail = "caller mutation"
	again, _, _ := s.EventsSince(ctx, first-1)
	if again[0].Detail == "caller mutation" {
		t.Fatal("audit slice aliases state")
	}
	kill()
	<-s.Done()
	db.Close()
	db, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, kill = context.WithCancel(ctx)
	defer kill()
	s, err = New(runCtx, cfg, c, net, db)
	if err != nil {
		t.Fatal(err)
	}
	events, truncated, err = s.EventsSince(ctx, first-1)
	if err != nil || truncated || len(events) != 80 || events[0].Seq != first || events[len(events)-1].Seq != last {
		t.Fatal("audit retention not restored")
	}
	if _, err = s.CreateVM(ctx, "after-restart", api.CreateVMRequest{Pool: config.Pools[0].Name}); err != nil {
		t.Fatal(err)
	}
	events, truncated, err = s.EventsSince(ctx, last)
	if err != nil || truncated || len(events) != 2 || events[0].Seq != last+1 {
		t.Fatalf("sequence repeated after restart: %+v %v %v", events, truncated, err)
	}
}

func TestInventoryProvesAdmissionWhenReplyIsLost(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	v := h.create("v", "standard")
	h.c.Advance(h.s.cfg.ReservationTTL)
	h.agent("a", "r1", api.HostHeartbeat{HostID: "a", Seq: 1, Inventory: []api.InventoryItem{{VMID: v.ID, Generation: v.Generation, State: api.VMBooting, Pool: v.Pool}}})
	h.step(tick{})
	if got := h.s.state.data.VMs[v.ID].VM; got.State != api.VMBooting || got.Generation != v.Generation {
		t.Fatalf("running boot retried after lost admit reply: %+v", got)
	}
}

func TestRestartSuspectHostGetsFreshLostGrace(t *testing.T) {
	h := newHarness(t)
	h.register("a", "r1", 1)
	v := h.create("v", "standard")
	h.ready(v)
	d, epoch, err := h.db.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	h.s.state = newState(d, epoch, h.c.Now(), h.s.cfg)
	h.c.Advance(h.s.cfg.HostLostGrace - time.Millisecond)
	h.step(tick{})
	if h.s.state.hosts["a"].lease != api.LeaseSuspect || h.s.state.data.VMs[v.ID].VM.State != api.VMReady {
		t.Fatal("restart grace ended early")
	}
	h.c.Advance(time.Millisecond)
	h.step(tick{})
	if h.s.state.hosts["a"].lease != api.LeaseLost || h.s.state.data.Queue[v.ID].Reason != api.QueueReplace {
		t.Fatal("restart gave suspect host another healthy TTL")
	}
}
