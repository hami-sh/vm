package simnet_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/simnet"
)

func attach(t *testing.T, n *simnet.Network, a api.Addr) api.Endpoint {
	t.Helper()
	ep, err := n.Attach(a)
	if err != nil {
		t.Fatal(err)
	}
	return ep
}
func denied(t *testing.T, e api.Endpoint, to api.Addr, p api.Port) {
	t.Helper()
	if err := e.Send(to, p, "attack"); !errors.Is(err, api.ErrDenied) {
		t.Fatalf("route unexpectedly allowed: %v %v: %v", to, p, err)
	}
}
func TestEastWestBlocked(t *testing.T) {
	n := simnet.New(clock.NewFake(time.Unix(0, 0)), config.Default(), 1)
	defer n.Close()
	g := attach(t, n, api.Addr{Kind: api.KindGuest, ID: "a", Host: "h1", Rack: "r1"})
	for _, host := range []string{"h1", "h2"} {
		for _, p := range []api.Port{api.PortGuest, api.PortVsock, api.PortControl, api.PortHTTPS} {
			denied(t, g, api.Addr{Kind: api.KindGuest, ID: "b", Host: host}, p)
		}
	}
}
func TestControlPlaneUnreachable(t *testing.T) {
	n := simnet.New(clock.NewFake(time.Unix(0, 0)), config.Default(), 1)
	defer n.Close()
	g := attach(t, n, api.Addr{Kind: api.KindGuest, ID: "a", Host: "h1"})
	for _, to := range []api.Addr{{Kind: api.KindScheduler, ID: "scheduler"}, {Kind: api.KindAgent, ID: "h2", Host: "h2"}} {
		for _, p := range []api.Port{api.PortControl, api.PortVsock, api.PortHTTPS} {
			denied(t, g, to, p)
		}
	}
	denied(t, g, api.Addr{Kind: api.KindAgent, ID: "h1", Host: "h1"}, api.PortControl)
	host := attach(t, n, api.Addr{Kind: api.KindAgent, ID: "h1", Host: "h1"})
	if err := g.Send(host.Addr(), api.PortVsock, api.GuestHeartbeat{VMID: "claimed", Generation: 2}); err != nil {
		t.Fatal(err)
	}
	e := <-host.Inbox()
	if e.From != g.Addr() {
		t.Fatalf("sender identity forged: %+v", e)
	}
}
func TestEgressAllowlist(t *testing.T) {
	n := simnet.New(clock.NewFake(time.Unix(0, 0)), config.Default(), 1)
	defer n.Close()
	g := attach(t, n, api.Addr{Kind: api.KindGuest, ID: "a", Host: "h1"})
	allowed := attach(t, n, api.Addr{Kind: api.KindInternet, ID: "github.com"})
	n.AllowEgress("github.com", true)
	if err := g.Send(allowed.Addr(), api.PortHTTPS, "fetch"); err != nil {
		t.Fatal(err)
	}
	if (<-allowed.Inbox()).From != g.Addr() {
		t.Fatal("egress identity lost")
	}
	for _, host := range []string{"github.com.evil.test", "evil.test", "api.github.com"} {
		denied(t, g, api.Addr{Kind: api.KindInternet, ID: host}, api.PortHTTPS)
	}
	denied(t, g, allowed.Addr(), api.PortControl)
	n.AllowEgress("github.com", false)
	denied(t, g, allowed.Addr(), api.PortHTTPS)
}
func TestFaultsAndPayloadIsolation(t *testing.T) {
	c := clock.NewFake(time.Unix(100, 0))
	n := simnet.New(c, config.Default(), 11)
	defer n.Close()
	s := attach(t, n, api.Addr{Kind: api.KindScheduler, ID: "scheduler"})
	h := attach(t, n, api.Addr{Kind: api.KindAgent, ID: "h1", Host: "h1", Rack: "r1"})
	empty := func(e api.Endpoint) {
		t.Helper()
		select {
		case got := <-e.Inbox():
			t.Fatalf("unexpected packet: %+v", got)
		default:
		}
	}
	n.PartitionRack("r1", true)
	_ = s.Send(h.Addr(), api.PortControl, "blocked")
	empty(h)
	n.PartitionRack("r1", false)
	n.PartitionOneWay(s.Addr(), h.Addr(), true)
	_ = s.Send(h.Addr(), api.PortControl, "blocked")
	empty(h)
	_ = h.Send(s.Addr(), api.PortControl, "reverse")
	if (<-s.Inbox()).Msg != "reverse" {
		t.Fatal("one-way blocked reverse link")
	}
	n.PartitionOneWay(s.Addr(), h.Addr(), false)
	n.SetPacketLoss(1)
	_ = s.Send(h.Addr(), api.PortControl, "lost")
	empty(h)
	n.SetPacketLoss(0)
	n.SetLatency(time.Second)
	n.SetDuplicate(1)
	n.SetReorder(true)
	msg := api.HeartbeatAck{HostID: "h1", Stale: []api.InventoryItem{{VMID: "original", Generation: 1}}}
	_ = s.Send(h.Addr(), api.PortControl, msg)
	msg.Stale[0].VMID = "mutated"
	empty(h)
	c.Advance(2 * time.Second)
	// Advancing the clock makes packets due, not the network goroutine
	// finished. Synchronize on delivery instead of counting scheduler yields.
	for range 2 {
		e := <-h.Inbox()
		if e.Msg.(api.HeartbeatAck).Stale[0].VMID != "original" {
			t.Fatal("payload mutated across boundary")
		}
	}
}

func TestReorderWithoutLatency(t *testing.T) {
	c := clock.NewFake(time.Unix(100, 0))
	cfg := config.Default()
	n := simnet.New(c, cfg, 22)
	defer n.Close()
	s := attach(t, n, api.Addr{Kind: api.KindScheduler, ID: "scheduler"})
	h := attach(t, n, api.Addr{Kind: api.KindAgent, ID: "h1", Host: "h1", Rack: "r1"})
	n.SetReorder(true)
	for i := range 20 {
		if err := s.Send(h.Addr(), api.PortControl, i); err != nil {
			t.Fatal(err)
		}
	}
	c.Advance(cfg.InvariantEvery)
	ordered := true
	for i := range 20 {
		if (<-h.Inbox()).Msg.(int) != i {
			ordered = false
		}
	}
	if ordered {
		t.Fatal("reorder fault preserved the original order")
	}
}
