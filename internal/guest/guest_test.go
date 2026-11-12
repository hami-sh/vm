package guest_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/guest"
	"github.com/hami-sh/vm/internal/simnet"
)

func TestRegistrationAcknowledgment(t *testing.T) {
	for _, reason := range []string{"", api.AttackReplay, "unknown_or_releasing_guest"} {
		t.Run("rejection="+reason, func(t *testing.T) {
			c := clock.Real{}
			cfg := config.Default()
			cfg.GuestHeartbeat = 10 * time.Millisecond
			n := simnet.New(c, cfg, 17)
			defer n.Close()
			host := api.Addr{Kind: api.KindAgent, ID: "h1", Host: "h1", Rack: "r1"}
			h, err := n.Attach(host)
			if err != nil {
				t.Fatal(err)
			}
			g, err := n.Attach(api.Addr{Kind: api.KindGuest, ID: "vm", Host: "h1", Rack: "r1"})
			if err != nil {
				t.Fatal(err)
			}
			other, err := n.Attach(api.Addr{Kind: api.KindAgent, ID: "h2", Host: "h2", Rack: "r1"})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() {
				defer close(done)
				guest.Run(ctx, c, cfg, g, api.BootParams{VMID: "vm", Generation: 2, Token: "boot-token", Host: "h1", Rack: "r1"})
			}()
			defer func() { cancel(); <-done }()
			timeout := c.NewTimer(5 * time.Second)
			defer timeout.Stop()
			next := func() any {
				t.Helper()
				select {
				case e := <-h.Inbox():
					return e.Msg
				case <-done:
					t.Fatal("guest stopped without an authenticated rejection")
				case <-timeout.C():
					t.Fatal("guest stopped making progress")
				}
				return nil
			}
			want := api.GuestRegister{VMID: "vm", Generation: 2, Token: "boot-token", Checks: []string{"simctl"}}
			for range 2 {
				// Lose the first registration acknowledgment. Retrying must
				// preserve the token and generation, not send Heartbeat yet.
				if got := next(); !reflect.DeepEqual(got, want) {
					t.Fatalf("unacknowledged boot message: %+v", got)
				}
			}
			for _, accepted := range []bool{true, false} {
				if err := other.Send(g.Addr(), api.PortVsock, api.GuestRegisterResult{Accepted: accepted}); err != nil {
					t.Fatal(err)
				}
				if got := next(); !reflect.DeepEqual(got, want) {
					t.Fatalf("unauthenticated result changed registration: %+v", got)
				}
			}
			if err := h.Send(g.Addr(), api.PortVsock, api.GuestRegisterResult{Accepted: reason == "", Reason: reason}); err != nil {
				t.Fatal(err)
			}
			if reason != "" {
				select {
				case <-done:
				case <-timeout.C():
					t.Fatal("rejected boot kept retransmitting")
				}
				return
			}
			// A tick already queued before acceptance can still register.
			for {
				if _, ok := next().(api.GuestHeartbeat); ok {
					break
				}
			}
			if got := next(); got != (api.GuestHeartbeat{VMID: "vm", Generation: 2}) {
				t.Fatalf("accepted boot did not keep heartbeating: %+v", got)
			}
		})
	}
}
