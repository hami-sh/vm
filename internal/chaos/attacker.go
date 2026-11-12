package chaos

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
)

type AttackOutcome struct {
	VMID, Attack string
	// Unknown is deliberately separate from Blocked: packet loss and timeouts
	// are not evidence of isolation. AgentCounted prevents double counting.
	Blocked, Succeeded, Unknown, AgentCounted bool
}

// RunAttacker replaces only the honest guest loop, on the same authenticated
// endpoint. One in-flight register plus its reply is enough to correlate the
// verdict; valid heartbeats continue while the guest waits for that reply.
func RunAttacker(ctx context.Context, c clock.Clock, cfg config.Config, ep api.Endpoint, gen int64, writeDisk func(int) error, report func(AttackOutcome)) {
	id := ep.Addr().ID
	host := api.Addr{Kind: api.KindAgent, ID: ep.Addr().Host, Host: ep.Addr().Host, Rack: ep.Addr().Rack}
	hb := c.NewTicker(cfg.GuestHeartbeat)
	defer hb.Stop()
	attackTick := c.NewTicker(cfg.AttackerEvery)
	defer attackTick.Stop()
	timeout := c.NewTimer(cfg.GuestLeaseTTL)
	timeout.Stop()
	defer timeout.Stop()
	attacks := []string{api.AttackImpersonate, api.AttackReplay, api.AttackFlood, api.AttackEastWest, api.AttackControl, api.AttackEgress, api.AttackDiskFill}
	index := 0
	pending := ""
	// Discard the honest loop's initial registration reply before the first
	// attack. Compromise is admitted only for a Ready VM.
	for {
		select {
		case _, ok := <-ep.Inbox():
			if !ok {
				return
			}
		default:
			goto drained
		}
	}
drained:
	for {
		select {
		case <-ctx.Done():
			return
		case <-hb.C():
			_ = ep.Send(host, api.PortVsock, api.GuestHeartbeat{VMID: id, Generation: gen})
		case envelope, ok := <-ep.Inbox():
			if !ok {
				return
			}
			if result, ok := envelope.Msg.(api.GuestRegisterResult); ok && pending != "" && envelope.From == host {
				report(AttackOutcome{VMID: id, Attack: pending, Blocked: !result.Accepted, Succeeded: result.Accepted, AgentCounted: true})
				pending = ""
				timeout.Stop()
			}
		case <-timeout.C():
			if pending != "" {
				report(AttackOutcome{VMID: id, Attack: pending, Unknown: true})
				pending = ""
			}
		case <-attackTick.C():
			if pending != "" {
				continue
			}
			a := attacks[index%len(attacks)]
			index++
			out := AttackOutcome{VMID: id, Attack: a}
			var err error
			switch a {
			case api.AttackImpersonate:
				err = ep.Send(host, api.PortVsock, api.GuestRegister{VMID: "another-tenant", Generation: gen, Token: "forged"})
			case api.AttackReplay:
				err = ep.Send(host, api.PortVsock, api.GuestRegister{VMID: id, Generation: gen - 1, Token: "old-token"})
			case api.AttackFlood:
				err = ep.Send(host, api.PortVsock, api.GuestRegister{VMID: id, Generation: gen, Token: strings.Repeat("x", cfg.GuestMsgMaxBytes+1)})
			case api.AttackEastWest:
				err = ep.Send(api.Addr{Kind: api.KindGuest, ID: "neighbour", Host: host.ID, Rack: host.Rack}, api.PortGuest, []byte("attack"))
			case api.AttackControl:
				err = ep.Send(api.Addr{Kind: api.KindScheduler, ID: "scheduler"}, api.PortControl, api.HostHeartbeat{HostID: host.ID})
			case api.AttackEgress:
				err = ep.Send(api.Addr{Kind: api.KindInternet, ID: "attacker.invalid"}, api.PortHTTPS, []byte("exfiltrate"))
			case api.AttackDiskFill:
				err = writeDisk(math.MaxInt32)
			}
			if a == api.AttackImpersonate || a == api.AttackReplay || a == api.AttackFlood {
				if err == nil {
					pending = a
					timeout.Stop()
					select {
					case <-timeout.C():
					default:
					}
					timeout.Reset(cfg.GuestLeaseTTL)
					continue
				}
				out.Unknown = true
			} else if errors.Is(err, api.ErrDenied) || (a == api.AttackDiskFill && err != nil) {
				out.Blocked = true
			} else if err == nil {
				out.Succeeded = true
			} else {
				out.Unknown = true
			}
			report(out)
		}
	}
}
