// Package simnet provides an authenticated, fault-injectable in-memory network.
package simnet

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
)

type policy struct {
	from, to api.AddrKind
	port     api.Port
}
type link struct{ from, to api.Addr }
type delivery struct {
	at       time.Time
	to       *endpoint
	envelope api.Envelope
}

type Network struct {
	mu              sync.Mutex
	clock           clock.Clock
	rng             *rand.Rand
	endpoints       map[api.Addr]*endpoint
	policies        map[policy]bool
	racks           map[string]bool
	links           map[link]bool
	egress          map[string]bool
	latency         time.Duration
	loss, duplicate float64
	reorder         bool
	reorderWindow   time.Duration
	pending         []delivery
	wake            chan struct{}
	stop            chan struct{}
	done            chan struct{}
	closed          bool
	capacity        int
}

func New(c clock.Clock, cfg config.Config, seed int64) *Network {
	n := &Network{clock: c, rng: rand.New(rand.NewSource(seed)), endpoints: make(map[api.Addr]*endpoint), policies: make(map[policy]bool), racks: make(map[string]bool), links: make(map[link]bool), egress: make(map[string]bool), wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), capacity: cfg.GuestMsgMaxBytes, reorderWindow: cfg.InvariantEvery}
	n.policies[policy{api.KindScheduler, api.KindAgent, api.PortControl}] = true
	n.policies[policy{api.KindAgent, api.KindScheduler, api.PortControl}] = true
	n.policies[policy{api.KindGuest, api.KindAgent, api.PortVsock}] = true
	n.policies[policy{api.KindAgent, api.KindGuest, api.PortVsock}] = true
	go n.run()
	return n
}

func (n *Network) Attach(a api.Addr) (api.Endpoint, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, fmt.Errorf("network closed")
	}
	if _, ok := n.endpoints[a]; ok {
		return nil, fmt.Errorf("endpoint already attached: %v", a)
	}
	e := &endpoint{network: n, addr: a, inbox: make(chan api.Envelope, n.capacity)}
	n.endpoints[a] = e
	return e, nil
}

// Fault methods are safe to call while traffic is flowing. Rates are 0..1.
func (n *Network) SetLatency(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.latency = max(0, d)
}
func (n *Network) SetPacketLoss(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.loss = min(1, max(0, p))
}
func (n *Network) SetDuplicate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.duplicate = min(1, max(0, p))
}
func (n *Network) SetReorder(on bool) { n.mu.Lock(); defer n.mu.Unlock(); n.reorder = on }
func (n *Network) PartitionRack(rack string, on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.racks[rack] = on
}
func (n *Network) PartitionOneWay(from, to api.Addr, on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.links[link{from, to}] = on
}
func (n *Network) SetPolicy(from, to api.AddrKind, port api.Port, allow bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.policies[policy{from, to, port}] = allow
}

// AllowEgress only allows an exact proxy destination, never a suffix match.
func (n *Network) AllowEgress(host string, allow bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.egress[host] = allow
}

func (n *Network) blocked(from, to api.Addr) bool {
	return n.links[link{from, to}] || (from.Rack != to.Rack && (n.racks[from.Rack] || n.racks[to.Rack]))
}

type endpoint struct {
	network *Network
	addr    api.Addr
	inbox   chan api.Envelope
	closed  bool
}

func (e *endpoint) Addr() api.Addr             { return e.addr }
func (e *endpoint) Inbox() <-chan api.Envelope { return e.inbox }
func (e *endpoint) Close() {
	n := e.network
	n.mu.Lock()
	defer n.mu.Unlock()
	if !e.closed {
		e.closed = true
		delete(n.endpoints, e.addr)
		close(e.inbox)
	}
}
func (e *endpoint) Send(to api.Addr, port api.Port, msg any) error {
	n := e.network
	n.mu.Lock()
	defer n.mu.Unlock()
	if e.closed || n.closed {
		return fmt.Errorf("endpoint closed")
	}
	allow := n.policies[policy{e.addr.Kind, to.Kind, port}]
	if e.addr.Kind == api.KindGuest {
		allow = (allow && to.Kind == api.KindAgent && port == api.PortVsock && to.ID == e.addr.Host) || (to.Kind == api.KindInternet && port == api.PortHTTPS && n.egress[to.ID])
	}
	if !allow {
		return api.ErrDenied
	}
	target := n.endpoints[to]
	if target == nil || n.blocked(e.addr, to) || n.rng.Float64() < n.loss {
		return nil
	}
	// Copy the payload at the boundary, as a real serialized transport would.
	if msg != nil {
		b, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		v := reflect.New(reflect.TypeOf(msg))
		if err = json.Unmarshal(b, v.Interface()); err != nil {
			return err
		}
		msg = v.Elem().Interface()
	}
	env := api.Envelope{From: e.addr, To: to, Port: port, Msg: msg}
	copies := 1
	if n.rng.Float64() < n.duplicate {
		copies++
	}
	for range copies {
		delay := n.latency
		if n.reorder {
			delay += time.Duration(n.rng.Int63n(int64(max(n.latency, n.reorderWindow))))
		}
		if delay == 0 {
			n.deliver(target, env)
		} else if len(n.pending) < n.capacity {
			n.pending = append(n.pending, delivery{n.clock.Now().Add(delay), target, env})
		}
	}
	select {
	case n.wake <- struct{}{}:
	default:
	}
	return nil
}
func (n *Network) deliver(e *endpoint, env api.Envelope) {
	if !e.closed && !n.blocked(env.From, env.To) {
		select {
		case e.inbox <- env:
		default:
		}
	}
}
func (n *Network) run() {
	defer close(n.done)
	for {
		n.mu.Lock()
		now := n.clock.Now()
		var next time.Time
		sort.SliceStable(n.pending, func(i, j int) bool { return n.pending[i].at.Before(n.pending[j].at) })
		keep := n.pending[:0]
		for _, d := range n.pending {
			if !d.at.After(now) {
				n.deliver(d.to, d.envelope)
			} else {
				keep = append(keep, d)
				if next.IsZero() || d.at.Before(next) {
					next = d.at
				}
			}
		}
		n.pending = keep
		var timer clock.Timer
		var tick <-chan time.Time
		if !next.IsZero() {
			timer = n.clock.NewTimer(max(0, next.Sub(n.clock.Now())))
			tick = timer.C()
			// Time may advance between computing the delay and arming the
			// timer. Recheck the absolute deadline so due packets cannot wait
			// for another clock advance (or unrelated Send) to wake us.
			if !next.After(n.clock.Now()) {
				timer.Stop()
				n.mu.Unlock()
				continue
			}
		}
		n.mu.Unlock()
		select {
		case <-n.stop:
			if timer != nil {
				timer.Stop()
			}
			return
		case <-n.wake:
		case <-tick:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}
func (n *Network) Close() {
	n.mu.Lock()
	if !n.closed {
		n.closed = true
		close(n.stop)
		for _, e := range n.endpoints {
			if !e.closed {
				e.closed = true
				close(e.inbox)
			}
		}
		clear(n.endpoints)
		n.pending = nil
	}
	n.mu.Unlock()
	<-n.done
}

var _ api.Transport = (*Network)(nil)
