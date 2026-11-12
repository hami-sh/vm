// Command controlplane runs the scheduler, the simulated hosts and the web
// dashboard in one process:
//
//	go run ./cmd/controlplane --racks 3 --hosts-per-rack 4 --seed 1
//
// The fleet is the real scheduler on a SQLite store, real host agents and
// guests on the simulated network, and the chaos engine, all on one scaled
// clock. internal/web serves the API, the SSE stream and the dashboard over
// it. It listens on --addr, or on $PORT (default 8080).
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/clock"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/fleet"
	"github.com/hami-sh/vm/internal/scheduler"
	"github.com/hami-sh/vm/internal/store"
	"github.com/hami-sh/vm/internal/web"
)

func main() {
	cfg := config.Default()
	addr := flag.String("addr", "", "listen address (default :$PORT or :8080)")
	racks := flag.Int("racks", cfg.Racks, "racks in the simulated fleet")
	hostsPerRack := flag.Int("hosts-per-rack", cfg.HostsPerRack, "hosts in each rack")
	seed := flag.Int64("seed", 1, "seed for chaos decisions")
	speed := flag.Float64("speed", cfg.DefaultSpeed, "starting sim speed multiplier")
	storePath := flag.String("store", envOr("STORE_PATH", filepath.Join(os.TempDir(), "controlplane", "fleet.db")),
		"scheduler SQLite file on local disk; recreated on every start")
	runsDir := flag.String("runs", envOr("RUNS_DIR", "runs"), "directory of recorded runs for the Runs page")
	flag.Parse()
	if *addr == "" {
		*addr = ":" + envOr("PORT", "8080")
	}

	// The hosts and their VMs live in this process, so a store left by an
	// earlier process describes a fleet that no longer exists, at sim times
	// this clock has not reached. Start from an empty one.
	if err := os.MkdirAll(filepath.Dir(*storePath), 0o755); err != nil {
		log.Fatalf("store directory: %v", err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(*storePath + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Fatalf("stale store: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Fleet can replace its scheduler on restart or wipe. Publish the current
	// actor, rather than retaining the first scheduler's metrics endpoint.
	var metrics atomic.Pointer[scheduler.Scheduler]
	f, err := fleet.New(ctx, fleet.Options{
		Config:       cfg,
		Racks:        *racks,
		HostsPerRack: *hostsPerRack,
		Seed:         *seed,
		Speed:        *speed,
		StorePath:    *storePath,
		SchedulerFactory: func(ctx context.Context, cfg config.Config, c clock.Clock, transport api.Transport, path string) (fleet.Scheduler, func(), error) {
			db, err := store.Open(path)
			if err != nil {
				return nil, nil, err
			}
			s, err := scheduler.New(ctx, cfg, c, transport, db)
			if err != nil {
				db.Close()
				return nil, nil, err
			}
			metrics.Store(s)
			return s, func() {
				metrics.CompareAndSwap(s, nil)
				<-s.Done()
				db.Close()
			}, nil
		},
	})
	if err != nil {
		log.Fatalf("fleet: %v", err)
	}
	dashboard := web.New(web.Options{
		ControlPlane: f.Control,
		Chaos:        f.Chaos,
		Snapshots:    f,
		Config:       cfg,
		RunsDir:      *runsDir,
		Metrics: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s := metrics.Load()
			if s == nil {
				http.Error(w, "scheduler unavailable", http.StatusServiceUnavailable)
				return
			}
			text, err := s.Metrics(r.Context())
			if err != nil {
				http.Error(w, "scheduler unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
			w.Write([]byte(text))
		}),
	})
	server := &http.Server{Addr: *addr, Handler: dashboard}

	failed := make(chan error, 1)
	go func() { failed <- server.ListenAndServe() }()
	log.Printf("controlplane: %d racks × %d hosts, seed %d, store %s, runs %s, listening on %s",
		*racks, *hostsPerRack, *seed, *storePath, *runsDir, *addr)

	select {
	case err = <-failed:
	case <-ctx.Done():
		err = nil
	}
	// Event streams never finish on their own, so close connections instead
	// of waiting for them to drain.
	server.Close()
	dashboard.Close()
	f.Close()
	if err != nil {
		log.Fatalf("controlplane: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
