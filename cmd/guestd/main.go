// Command guestd is the real in-VM daemon for darwin/arm64, wrapping
// internal/guest. Owner: W2.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/guest"
)

func main() {
	boot := flag.String("boot", "/Volumes/My Shared Files/boot/boot.json", "host-provisioned boot parameters")
	socket := flag.String("socket", "/var/run/guestd-agent.sock", "host-provisioned vsock bridge Unix socket")
	flag.Parse()
	b, err := os.ReadFile(*boot)
	if err != nil {
		log.Fatal(err)
	}
	var p api.BootParams
	if err = json.Unmarshal(b, &p); err != nil {
		log.Fatal(err)
	}
	if p.VMID == "" || p.Host == "" || p.Token == "" || p.Generation < 1 {
		log.Fatal("incomplete boot parameters")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err = exec.CommandContext(ctx, "xcrun", "simctl", "list", "devices").Run(); err != nil {
		log.Fatalf("simctl readiness check: %v", err)
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", *socket)
	if err != nil {
		log.Fatal(err)
	}
	ep := guest.NewStreamEndpoint(conn, api.Addr{Kind: api.KindGuest, ID: p.VMID, Host: p.Host, Rack: p.Rack})
	defer ep.Close()
	// Cancellation closes a blocked stream write as well as stopping timers.
	go func() { <-ctx.Done(); ep.Close() }()
	guest.Run(ctx, clock.Real{}, config.Default(), ep, p)
}
