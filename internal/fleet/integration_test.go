package fleet

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/config"
	"github.com/hami-sh/vm/internal/invariant"
)

type scenarioFleet struct {
	t   *testing.T
	ctx context.Context
	f   *Fleet
	cfg config.Config
}

func newScenarioFleet(t *testing.T) *scenarioFleet {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	t.Cleanup(cancel)
	cfg := config.Default()
	f, err := New(ctx, Options{Config: cfg, Seed: 73, Speed: 5})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.Close)
	h := &scenarioFleet{t, ctx, f, cfg}
	h.wait(2*cfg.HostLeaseTTL, "initial registration", func(s api.Snapshot) bool {
		return len(s.Hosts) == cfg.Racks*cfg.HostsPerRack &&
			!slices.ContainsFunc(s.Hosts, func(h api.Host) bool { return h.Lease != api.LeaseHealthy })
	})
	return h
}

func (h *scenarioFleet) wait(within time.Duration, detail string, done func(api.Snapshot) bool) api.Snapshot {
	h.t.Helper()
	tick := h.f.Clock.NewTicker(h.cfg.InvariantEvery)
	defer tick.Stop()
	timeout := h.f.Clock.NewTimer(within)
	defer timeout.Stop()
	for {
		s := h.f.Latest()
		for _, inv := range s.Invariants {
			if inv.Violations != 0 {
				h.t.Fatalf("%s: invariant %+v; recent=%+v", detail, inv, s.Recent)
			}
		}
		if done(s) {
			return s
		}
		select {
		case <-tick.C():
		case <-timeout.C():
			h.t.Fatalf("%s: timed out; states=%v queue=%d VMs=%+v", detail, s.Counters.ByState, len(s.Queue), s.VMs)
		case <-h.ctx.Done():
			h.t.Fatal(h.ctx.Err())
		}
	}
}
func (h *scenarioFleet) create(n int) {
	h.t.Helper()
	tick := h.f.Clock.NewTicker(h.cfg.InvariantEvery)
	defer tick.Stop()
	for i := 0; i < n; {
		_, err := h.f.Control.CreateVM(h.ctx, fmt.Sprintf("work-%d", i), api.CreateVMRequest{Pool: config.Pools[i%len(config.Pools)].Name})
		if err == nil {
			i++
			continue
		}
		var unavailable *api.Error
		if !errors.As(err, &unavailable) || unavailable.Status != 503 {
			h.t.Fatal(err)
		}
		select {
		case <-tick.C():
		case <-h.ctx.Done():
			h.t.Fatal(h.ctx.Err())
		}
	}
}
func (h *scenarioFleet) ready(n int) api.Snapshot {
	h.t.Helper()
	return h.wait(2*h.cfg.BootDeadline, "VMs Ready", func(s api.Snapshot) bool { return s.Counters.ByState[api.VMReady] == n })
}
func (h *scenarioFleet) fault(action, target string) {
	h.t.Helper()
	if _, err := h.f.Chaos.Do(h.ctx, action, api.ChaosRequest{Target: target}); err != nil {
		h.t.Fatal(action, err)
	}
}

func TestRealFleetFailureScenarios(t *testing.T) {
	for _, name := range []string{"guestd-dies", "boot-hangs", "host-dies", "overload", "rack-partition-heal", "scheduler-crash"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newScenarioFleet(t)
			switch name {
			case "guestd-dies":
				h.create(1)
				v := h.ready(1).VMs[0]
				h.fault("kill-guestd", v.ID)
				h.wait(h.cfg.GuestLeaseTTL+2*h.cfg.BootDeadline, "guest recycled in place", func(s api.Snapshot) bool {
					return len(s.VMs) == 1 && s.VMs[0].State == api.VMReady && s.VMs[0].Generation == v.Generation+1 && s.VMs[0].HostID == v.HostID
				})
			case "boot-hangs":
				for _, host := range h.f.Latest().Hosts {
					h.fault("hang-next-boot", host.ID)
					// Retries may return to a host whose one-shot hang was spent.
					// Keep those later boots beyond the unchanged deadline too.
					if _, err := h.f.Chaos.Do(h.ctx, "slow-boot", api.ChaosRequest{Target: host.ID, Params: map[string]float64{"factor": h.cfg.MaxSpeed}}); err != nil {
						t.Fatal(err)
					}
				}
				h.create(1)
				s := h.wait(time.Duration(h.cfg.MaxAttempts)*(h.cfg.BootDeadline+h.cfg.BackoffMax), "hung boot retry budget", func(s api.Snapshot) bool {
					return len(s.VMs) == 1 && s.VMs[0].State == api.VMFailed
				})
				if s.VMs[0].Attempts != h.cfg.MaxAttempts || s.VMs[0].Generation != int64(h.cfg.MaxAttempts) {
					t.Fatalf("unbounded or missing retries: %+v", s.VMs[0])
				}
			case "host-dies":
				h.create(8)
				before := h.ready(8)
				host := before.VMs[0].HostID
				h.fault("kill-host", host)
				h.wait(h.cfg.HostLeaseTTL+h.cfg.HostLostGrace+2*h.cfg.BootDeadline, "dead host replaced", func(s api.Snapshot) bool {
					if s.Counters.ByState[api.VMReady] != 8 {
						return false
					}
					for _, old := range before.VMs {
						if old.HostID == host && !slices.ContainsFunc(s.VMs, func(v api.VM) bool { return v.ID == old.ID && v.HostID != host && v.Generation == old.Generation+1 }) {
							return false
						}
					}
					return true
				})
			case "overload":
				h.create(24)
				h.ready(24)
				for i := range h.cfg.QueueCap {
					if _, err := h.f.Control.CreateVM(h.ctx, fmt.Sprintf("batch-%d", i), api.CreateVMRequest{Pool: config.Pools[0].Name, Priority: "batch"}); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := h.f.Chaos.Do(h.ctx, "flood", api.ChaosRequest{Params: map[string]float64{"count": 60, "interactive": 1}}); err != nil {
					t.Fatal(err)
				}
				h.wait(h.cfg.HostHeartbeat, "bounded queue and priority shedding", func(s api.Snapshot) bool {
					return s.Counters.ByState[api.VMReady] == 24 && len(s.Queue) == h.cfg.QueueCap && s.Counters.Shed >= h.cfg.QueueCap && s.Counters.Rejected429 > 0 &&
						!slices.ContainsFunc(s.Queue, func(q api.QueueEntry) bool { return q.Priority != api.PriorityInteractive })
				})
				_, err := h.f.Control.CreateVM(h.ctx, "rejected", api.CreateVMRequest{Pool: config.Pools[0].Name, Priority: "batch"})
				var limited *api.Error
				if !errors.As(err, &limited) || limited.Status != 429 || limited.RetryAfterS == 0 {
					t.Fatalf("missing retryable overload response: %v", err)
				}
			case "rack-partition-heal":
				h.create(8)
				before := h.ready(8)
				h.fault("partition-rack", "r2")
				h.wait(h.cfg.HostLeaseTTL+h.cfg.HostLostGrace+2*h.cfg.BootDeadline, "rack replacements Ready", func(s api.Snapshot) bool {
					if s.Counters.ByState[api.VMReady] != 8 {
						return false
					}
					for _, old := range before.VMs {
						if old.Rack == "r2" && !slices.ContainsFunc(s.VMs, func(v api.VM) bool { return v.ID == old.ID && v.Rack != "r2" && v.Generation == old.Generation+1 }) {
							return false
						}
					}
					return true
				})
				h.fault("heal-rack", "r2")
				h.wait(3*h.cfg.HostHeartbeat, "stale copies fenced after healing", func(s api.Snapshot) bool {
					return !slices.ContainsFunc(s.Hosts, func(h api.Host) bool { return h.Rack == "r2" && (h.Lease != api.LeaseHealthy || len(h.Stale) != 0) })
				})
			case "scheduler-crash":
				h.create(4)
				before := h.ready(4)
				h.fault("kill-scheduler", "")
				_, err := h.f.Control.CreateVM(h.ctx, "during-outage", api.CreateVMRequest{Pool: config.Pools[0].Name})
				var unavailable *api.Error
				if !errors.As(err, &unavailable) || unavailable.Status != 503 {
					t.Fatalf("outage create: %v", err)
				}
				until := h.f.Clock.Now().Add(2 * h.cfg.GuestLeaseTTL).UnixMilli()
				h.wait(3*h.cfg.GuestLeaseTTL, "data plane remains alive", func(s api.Snapshot) bool { return s.At >= until })
				_, raw, _, err := h.f.runtime.observe(h.ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, old := range before.VMs {
					if !slices.ContainsFunc(raw, func(o invariant.Observation) bool {
						return slices.ContainsFunc(o.Actual, func(v api.DriverVM) bool { return v.VMID == old.ID && v.Generation == old.Generation && v.Running })
					}) {
						t.Fatalf("VM stopped during scheduler outage: %s", old.ID)
					}
				}
				h.fault("restart-scheduler", "")
				h.wait(3*h.cfg.HostHeartbeat, "SQLite reconciliation", func(s api.Snapshot) bool {
					return s.Epoch > before.Epoch && s.Counters.ByState[api.VMReady] == 4 && !slices.ContainsFunc(s.VMs, func(v api.VM) bool {
						return !slices.ContainsFunc(before.VMs, func(old api.VM) bool { return old.ID == v.ID && old.Generation == v.Generation })
					})
				})
			}
		})
	}
}
