package chaos

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/simnet"
)

func TestAttackerMeasuresBlockedSuccessfulAndUnknownOutcomes(t *testing.T) {
	for _, mode := range []string{"blocked", "broken-isolation", "missing-reply"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			cfg := config.Default()
			cfg.AttackerEvery = 20 * time.Millisecond
			cfg.GuestHeartbeat = 5 * time.Millisecond
			cfg.GuestLeaseTTL = 35 * time.Millisecond
			c := clock.Real{}
			n := simnet.New(c, cfg, 1)
			defer n.Close()
			hostAddr := api.Addr{Kind: api.KindAgent, ID: "h", Host: "h", Rack: "r"}
			host, err := n.Attach(hostAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()
			guest, err := n.Attach(api.Addr{Kind: api.KindGuest, ID: "v", Host: "h", Rack: "r"})
			if err != nil {
				t.Fatal(err)
			}
			defer guest.Close()
			if mode == "broken-isolation" {
				n.AllowEgress("attacker.invalid", true)
			}
			var heartbeats atomic.Int64
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				for {
					select {
					case <-ctx.Done():
						return
					case e := <-host.Inbox():
						switch e.Msg.(type) {
						case api.GuestHeartbeat:
							heartbeats.Add(1)
						case api.GuestRegister:
							if mode != "missing-reply" {
								_ = host.Send(guest.Addr(), api.PortVsock, api.GuestRegisterResult{Accepted: mode == "broken-isolation", Reason: "test-verdict"})
							}
						}
					}
				}
			}()
			out := make(chan AttackOutcome, 20)
			done := make(chan struct{})
			go func() {
				defer close(done)
				RunAttacker(ctx, c, cfg, guest, 4, func(int) error {
					if mode == "broken-isolation" {
						return nil
					}
					return fmt.Errorf("quota exceeded")
				}, func(o AttackOutcome) {
					select {
					case out <- o:
					case <-ctx.Done():
					}
				})
			}()
			defer func() { cancel(); <-done; <-serverDone }()
			want := []string{api.AttackImpersonate, api.AttackReplay, api.AttackFlood, api.AttackEastWest, api.AttackControl, api.AttackEgress, api.AttackDiskFill}
			for i, a := range want {
				select {
				case got := <-out:
					if got.Attack != a {
						t.Fatalf("cycle %d: %+v", i, got)
					}
					// Guest-to-guest and guest-to-control are unconditionally denied
					// by simnet even with an allow policy. Break reply, egress and
					// quota decisions instead, and keep those two negative controls.
					if mode == "broken-isolation" && i != 3 && i != 4 && !got.Succeeded {
						t.Fatalf("failed to detect successful attack: %+v", got)
					}
					if mode == "broken-isolation" && (i == 3 || i == 4) && !got.Blocked {
						t.Fatalf("control policy escaped: %+v", got)
					}
					if mode == "blocked" && !got.Blocked {
						t.Fatalf("did not measure block: %+v", got)
					}
					if mode == "missing-reply" && i < 3 && (!got.Unknown || got.Blocked || got.Succeeded) {
						t.Fatalf("timeout invented a verdict: %+v", got)
					}
				case <-ctx.Done():
					t.Fatal("attacker stalled")
				}
			}
			if heartbeats.Load() < 7 {
				t.Fatal("attacker starved valid heartbeats")
			}
		})
	}
}
