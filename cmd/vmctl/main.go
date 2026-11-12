// Command vmctl verifies isolated in-process fleets and records evidence.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/scenario"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 || (os.Args[1] != "verify" && os.Args[1] != "e2e") {
		return fmt.Errorf("usage: vmctl {verify|e2e} [flags]")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ContinueOnError)
	repeat := fs.Int("repeat", 20, "runs per scenario")
	parallel := fs.Int("parallel", 4, "isolated concurrent servers")
	seed := fs.Int64("seed", 1, "first seed")
	speed := fs.Float64("speed", 5, "sim speed; 0 calibrates at 5x and 50x")
	output := fs.String("output", "runs", "recording directory")
	filter := fs.String("scenario", "", "run one scenario by name")
	server := fs.String("server", "", "controlplane binary (default builds current source)")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if *server == "" && os.Args[1] == "e2e" {
		dir, err := os.MkdirTemp("", "vmctl-build-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		*server = filepath.Join(dir, "controlplane")
		cmd := exec.CommandContext(ctx, "go", "build", "-o", *server, "./cmd/controlplane")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return err
		}
	}
	var err error
	factory := scenario.RealFleet
	if *server != "" {
		abs, e := filepath.Abs(*server)
		if e != nil {
			return e
		}
		factory = serverFactory(abs)
	}
	if os.Args[1] == "e2e" {
		f, stop, err := factory(ctx, scenario.Catalog()[0], 1, 5)
		if err != nil {
			return err
		}
		defer stop()
		if _, err = f.Do(ctx, "flood", api.ChaosRequest{Params: map[string]float64{"count": 1}}); err != nil {
			return fmt.Errorf("HTTP chaos: %w", err)
		}
		if f.Latest().Scheduler == "" {
			return fmt.Errorf("SSE snapshot has no scheduler status")
		}
		fmt.Println("e2e: server booted, HTTP chaos accepted, SSE snapshot matches api.Snapshot")
		return nil
	}
	scenarios := scenario.Catalog()
	if *filter != "" {
		var selected []scenario.Scenario
		for _, s := range scenarios {
			if s.Name == *filter {
				selected = append(selected, s)
			}
		}
		if len(selected) == 0 {
			return fmt.Errorf("unknown scenario %q", *filter)
		}
		scenarios = selected
	}
	opt := scenario.Options{Repeat: *repeat, Parallel: *parallel, Seed: *seed, Speed: *speed, Output: *output}
	if *repeat < 1 || *parallel < 1 || *speed < 0 {
		return fmt.Errorf("repeat and parallel must be positive; speed must not be negative")
	}
	if opt.Speed == 0 {
		opt.Speed, err = scenario.Calibrate(ctx, scenarios, factory, opt, os.Stdout)
		if err != nil {
			return err
		}
	}
	_, err = scenario.Suite(ctx, scenarios, factory, opt, os.Stdout)
	return err
}

func serverFactory(binary string) scenario.Factory {
	return func(ctx context.Context, s scenario.Scenario, seed int64, speed float64) (scenario.System, func(), error) {
		// Reserve an ephemeral loopback address, then hand it to the child.
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, nil, err
		}
		addr := listener.Addr().String()
		listener.Close()
		dir, err := os.MkdirTemp("", "vmctl-server-")
		if err != nil {
			return nil, nil, err
		}
		log, err := os.Create(filepath.Join(dir, "server.log"))
		if err != nil {
			os.RemoveAll(dir)
			return nil, nil, err
		}
		childCtx, kill := context.WithCancel(ctx)
		cmd := exec.CommandContext(childCtx, binary, "-addr", addr)
		cmd.Dir = dir
		cmd.Stdout = log
		cmd.Stderr = log
		if err := cmd.Start(); err != nil {
			kill()
			log.Close()
			os.RemoveAll(dir)
			return nil, nil, err
		}
		stop := func() { kill(); _ = cmd.Wait(); log.Close(); os.RemoveAll(dir) }
		client := &http.Client{Timeout: time.Second}
		deadline := scenario.WallClock.NewTimer(10 * time.Second)
		defer deadline.Stop()
		tick := scenario.WallClock.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		url := "http://" + addr
		for {
			resp, e := client.Get(url + "/healthz")
			if e == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					break
				}
			}
			select {
			case <-ctx.Done():
				stop()
				return nil, nil, ctx.Err()
			case <-deadline.C():
				b, _ := os.ReadFile(filepath.Join(dir, "server.log"))
				stop()
				return nil, nil, fmt.Errorf("server startup: %s", b)
			case <-tick.C():
			}
		}
		// The stream lives for the whole run; a separate watchdog cancels a
		// server that never sends its first snapshot.
		connectCtx, connectCancel := context.WithCancel(childCtx)
		done := make(chan struct{})
		go func() {
			timer := scenario.WallClock.NewTimer(10 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C():
				connectCancel()
			case <-done:
			case <-connectCtx.Done():
			}
		}()
		f, err := scenario.Connect(connectCtx, url)
		close(done)
		if err != nil {
			connectCancel()
			stop()
			return nil, nil, fmt.Errorf("server SSE: %w", err)
		}
		if _, err = f.Sim(ctx, "chaos", api.SimRequest{Seed: seed}); err == nil {
			_, err = f.Sim(ctx, "speed", api.SimRequest{Speed: speed})
		}
		if err != nil {
			connectCancel()
			stop()
			return nil, nil, err
		}
		return f, func() { connectCancel(); stop() }, nil
	}
}
