// Package guest is the guestd protocol loop shared by fake and real guests.
package guest

import (
	"context"
	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"time"
)

type Effect struct{ Message any }
type loop struct {
	params     api.BootParams
	registered bool
}

func (g *loop) handle(_ time.Time, msg any) []Effect {
	if r, ok := msg.(api.GuestRegisterResult); ok {
		g.registered = r.Accepted
		return nil
	}
	if !g.registered {
		return []Effect{{api.GuestRegister{VMID: g.params.VMID, Generation: g.params.Generation, Token: g.params.Token, Checks: []string{"simctl"}}}}
	}
	return []Effect{{api.GuestHeartbeat{VMID: g.params.VMID, Generation: g.params.Generation}}}
}

// Run retries the boot registration until accepted, then sends heartbeats.
// An authenticated rejection ends this boot's loop. The caller owns the
// endpoint; cancelling Run never closes somebody else's connection.
func Run(ctx context.Context, c clock.Clock, cfg config.Config, ep api.Endpoint, p api.BootParams) {
	RunObserved(ctx, c, cfg, ep, p, nil)
}

// RunObserved also exposes replies received on the actual guest channel for
// the simulator's independent security observer. It never changes verdicts.
func RunObserved(ctx context.Context, c clock.Clock, cfg config.Config, ep api.Endpoint, p api.BootParams, observe func(api.Envelope)) {
	g := loop{params: p}
	host := api.Addr{Kind: api.KindAgent, ID: p.Host, Host: p.Host, Rack: p.Rack}
	send := func() {
		for _, e := range g.handle(c.Now(), nil) {
			_ = ep.Send(host, api.PortVsock, e.Message)
		}
	}
	send()
	ticker := c.NewTicker(cfg.GuestHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-ep.Inbox():
			if !ok {
				return
			}
			if observe != nil {
				observe(e)
			}
			if r, ok := e.Msg.(api.GuestRegisterResult); ok && e.From == host && e.Port == api.PortVsock {
				if !r.Accepted {
					return
				}
				g.handle(c.Now(), r)
			}
		case <-ticker.C():
			send()
		}
	}
}
