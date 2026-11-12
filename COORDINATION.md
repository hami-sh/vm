# Coordination log

Append-only. Each entry: time (AEDT), author (workstream), and one of STATUS / BLOCKER / CONTRACT-REQUEST / DECISION. Newest at the bottom.

## Gates

| Gate | Target | Status |
| --- | --- | --- |
| 0:30 Contracts frozen; stubs compile | Phase 0 | in review |
| 1:15 `make verify` end to end; W1+W2 over simnet: 30 creates → 24 Ready across racks | W5, W1, W2 | |
| 2:00 W3 wired; scenarios 1–6 green | all | |
| 2:30 W4 on live SSE | W4 | |
| 3:00 Deployed, demo-able, core frozen | W5 | |

## Contract summary (Phase 0)

- `internal/api/types.go`: `VM`, `Host`, `Slot`, `Rack`, `QueueEntry`, states, priorities, reasons.
- `internal/api/protocol.go`: agent↔scheduler (`RegisterHost`, `HostHeartbeat`, `HeartbeatAck`, `StartVM`, `StartVMResult`, `StopVM`, `ReadyReport`, `UnhealthyReport`, `StoppedReport`, `SecurityReport`) and guestd→agent (`GuestRegister`, `GuestHeartbeat`, `GuestRegisterResult`). `(vm_id, gen)` is the idempotency key.
- `internal/api/interfaces.go`: `Transport`/`Endpoint` (sender identity is `Envelope.From`, stamped by the transport) and `Driver`.
- `internal/api/snapshot.go`: `Event`, `Snapshot` (what the UI renders and runs record), invariant names.
- `internal/api/http.go`: public API bodies, chaos catalogue, run reports.
- `internal/clock`: `Clock`, `Controllable`, `Real`, `Fake`. W3 adds the scaled demo clock implementing `Controllable`.
- `internal/vm/legal.go`: the legal-move table (invariant 5).
- `fixtures/snapshot.json`: generated from the types; `make fixture` regenerates, and a test fails if it drifts.

## Log

- 2026-11-12, lead, DECISION: Go 1.27 (plan said 1.23).
- 2026-11-12, lead, DECISION: `events.kind` gains `alert` (wipe-store higher-generation path) and `reconcile`. Without them those rows would violate the CHECK and roll back the transition they're written with.
- 2026-11-12, lead, DECISION: agent rules live in AGENTS.md (no CLAUDE.md).
- 2026-11-12, lead, DECISION: dashboard speed may exceed 10× (up to `config.MaxSpeed` = 50) subject to the calibration rule.
- 2026-11-12, lead, DECISION: legal moves add `Pending → Failed` (shed / budget), `Pending → Terminated` (delete while queued), `Ready/Unhealthy → Failed` (quarantine), `Failed → Terminated` (delete), and `None → Ready` only for adopt-after-wiped-store.
- 2026-11-12, lead, STATUS: Railway project `vm-control-plane` created (env `production`). Deploy token is on the lead's orb only; W5 asks the lead to deploy, or gets the token via Amp project secret.
- 2026-11-12, lead, DECISION: added `internal/api/fleet.go`: `ControlPlane` (W1, scheduler), `ChaosEngine` (W3), `SnapshotSource` (W3, `internal/fleet` assembles everything), and `api.Error` for HTTP status. W4 serves HTTP against these; W5's runner drives a fleet through them.
- 2026-11-12, lead, DECISION: integration is done by the lead. Workstreams push their own branch and report at each gate; the lead reviews (fencing, reconcile, placement by hand) and merges to `main`.
- 2026-11-12, lead, STATUS: Phase 0 merged to main. W1–W5 started, each in its own thread and orb.
- 2026-11-12, lead, DECISION (W1 CONTRACT-REQUEST): `InventoryItem` gains optional `Pool` (agents copy it from `StartVM.Spec.Pool`). No `Spec` field: the spec is derived from the pool.
- 2026-11-12, lead, DECISION: wiped-store reconcile policy (W1 implements, W2 populates `Pool`):
  - The store is "fresh" when it had no `scheduler_epoch` on open. Only a fresh store adopts VMs it has no row for.
  - Unknown VM, fresh store, reported `Ready` with a `Pool`: emit an `alert` event, insert it as `None → Ready` at the reported generation, `desired=running`, on the reporting host. Its idempotency key is lost; say so in the alert.
  - Unknown VM, fresh store, reported in any other state or with no `Pool`: alert, then `StopVM` (we can't adopt a boot we didn't start).
  - Unknown VM, store not fresh: `StopVM` (plan: unknown → stop). VM rows are never pruned, so an unknown ID here really is unknown.
  - Known VM reported at a higher generation than stored (partial loss): alert, adopt at the reported generation.
  - The same VM reported by two hosts: keep the highest generation and stop the others (fencing). Equal generations on two hosts: keep the first report, stop the second, alert.
  - New VM IDs must never collide with adopted ones: the ID allocator moves past every adopted ID, or IDs are random.
- 2026-11-12, lead, DECISION: scheduler surface for fleet assembly is approved: `store.Open(path)`, `scheduler.New(ctx, cfg, clock, transport, store)` implementing `api.ControlPlane`, plus `Snapshot(ctx)`, `SetDown(ctx, bool)` and `SetHostAdmin(ctx, id, state)`. "Kill scheduler" must drop in-memory state: W3's fleet cancels the scheduler's ctx and restart builds a new scheduler from the same store file, so restarts really read SQLite. "Wipe store" deletes the file before the restart. `SetDown` is only the 503 window between the two.

### W2 surface — 2026-11-12 AEDT, W2, STATUS (early assembly surface)

```go
// internal/simnet
func New(c clock.Clock, cfg config.Config, seed int64) *Network
func (*Network) Attach(api.Addr) (api.Endpoint, error)
func (*Network) SetLatency(time.Duration)
func (*Network) SetPacketLoss(float64) // fraction 0..1
func (*Network) SetDuplicate(float64)  // fraction 0..1
func (*Network) SetReorder(bool)
func (*Network) PartitionRack(rack string, on bool) // false heals
func (*Network) PartitionOneWay(from, to api.Addr, on bool) // false heals
func (*Network) SetPolicy(from, to api.AddrKind, port api.Port, allow bool)
func (*Network) AllowEgress(host string, allow bool) // exact proxy destination
func (*Network) Close()

// internal/agent
func New(cfg config.Config, c clock.Clock, ep api.Endpoint, scheduler api.Addr, d api.Driver) *Agent
func (*Agent) Run(context.Context) // caller starts goroutine, once per instance
func (*Agent) Done() <-chan struct{}
func (*Agent) Kill(context.Context) error // disables commands/HBs, cancels guests, cleans disks
func (*Agent) Restart(context.Context) error // re-registers, preserves generation fences
func (*Agent) PauseHeartbeats(context.Context, bool) error // false resumes; guests continue
func (*Agent) Cordon(context.Context, bool) error
func (*Agent) SetClockSkew(context.Context, time.Duration) error
func (*Agent) RecordBlocked(ctx context.Context, vm, attack string) error
func (*Agent) Snapshot(context.Context) (Snapshot, error)
type Snapshot struct {
    Host api.Host // actual actor-owned slots, including Releasing
    Inventory []api.InventoryItem // includes Spec.Pool
    Actual []api.DriverVM // Driver.List independent of scheduler; Running identifies live VMs
    Security api.SecurityStatus // Blocked, Succeeded, ByAttack
    Epoch int64
    ClockSkew time.Duration // lease durations stay monotonic despite wall-clock skew
}

// internal/driver
func NewFake(c clock.Clock, cfg config.Config, n api.Transport, seed int64) *Fake
// *Fake implements api.Driver (Start, Stop, List, Exits).
func (*Fake) KillGuest(vm string)
func (*Fake) Crash(vm string)
func (*Fake) HangNextBoot()
func (*Fake) SlowBoot(factor float64) // persistent per-host factor; 1 resets
func (*Fake) FillDisk(full bool) // host admission refusal; false heals
func (*Fake) DiskFull() bool
func (*Fake) WriteDisk(vm string, mb int) error // per-VM quota
func (*Fake) GuestSend(vm string, to api.Addr, port api.Port, msg any) error
func (*Fake) Close()
```

Fleet wiring: attach scheduler at `api.Addr{Kind: api.KindScheduler, ID: "scheduler"}`. Agent addresses must use `api.Addr{Kind: api.KindAgent, ID: hostID, Host: hostID, Rack: rackID}`. Guest addresses are created by Fake from BootParams. For a one-way host partition call PartitionOneWay(agentAddr,schedulerAddr,true) or its reverse; latency/loss/duplicate/reorder are global. For compromise, W3 cycles attacks through GuestSend: it uses the VM's existing real endpoint and cannot spoof Envelope.From. Report ErrDenied network attempts and WriteDisk quota errors with Agent.RecordBlocked; do not count successful attacks as blocked. Guest-protocol attacks are counted by the agent itself. Snapshot's actor/driver reads are separate observations (allow one heartbeat of cross-component slack). Cancel Run and await Done before Network.Close. Kill/Restart are physical-host faults; PauseHeartbeats is the gray failure retaining live VMs.

Implementation is compiling under Go 1.27.4; security/lifecycle regression tests and Tart/guestd are still in progress. No done-when claim yet.
- 2026-11-12, lead, DECISION (bug in the wiped-store policy): agents ignore commands from an epoch lower than the highest they've seen, and a wiped store would restart `scheduler_epoch` at 1, so every agent would ignore the new scheduler and the fleet would freeze. On every start the scheduler now sets `scheduler_epoch = max(stored + 1, sim-clock unix ms)` and persists it before sending anything. The sim clock only moves forward, so the epoch keeps increasing across a wipe. W1 implements; W2 needs no change.
- 2026-11-12, lead, DECISION (replaces the `max(stored+1, sim ms)` epoch rule; W1 found it can go backwards after several restarts within one paused millisecond): `RegisterHost` and `HostHeartbeat` gain `Epoch`, the highest scheduler_epoch the agent has accepted (W2 fills it). On start the scheduler uses stored+1 (1 for a fresh store). Whenever a host reports an epoch >= its own, the scheduler sets epoch = reported+1, persists it, and only then sends commands or acks to anyone. This is the same "move the counter past the highest seen" rule the plan uses for generations; no epoch floor lives outside the store and the fleet doesn't touch the clock.
- 2026-11-12, lead, DECISION (W1 request): `config.EventRetention = 10000`, `config.RecentEventLimit = 50`.
- 2026-11-12, lead, DECISION (Hamish approved): idempotency keys survive a wiped store.
  - `StartVM` and `InventoryItem` gain `IdempotencyKey` and `RequestHash`. W1 sends them on every StartVM; W2's agent keeps them per VM and echoes them in every inventory item, without interpreting them.
  - On adopting an unknown Ready VM from a fresh store, W1 restores both on the `vms` row. A retried create with the same key returns that VM; a different body still gets 422. Two adopted VMs claiming the same key: keep the higher generation's claim, alert.
  - Recovery window: after a fresh-store start, `CreateVM` returns 503 with Retry-After for `config.HostLeaseTTL` of sim time, so every live host has reported before a retried key can be mistaken for a new one. Hosts silent past that are presumed lost anyway.
  - Not covered, by design: a VM whose host is lost at the same time as the wipe. Its retry creates a new VM, which is still exactly one running copy.
  - Test (W1): run, create with key K, wipe, New, retry K during the window (503), then after it (same VM ID, 202), and with a different body (422).
- 2026-11-12, lead, DECISION (W3 CONTRACT-REQUEST): `SimRequest.Racks` and `SimStatus.Racks` added (ambient chaos scope, empty = all). Seed stays `int64`; it is read only by the explicit `seed` action, so 0 is a valid seed there.
- 2026-11-12, lead, DECISION: the per-IP chaos rate limit is HTTP hardening and lives in W4's middleware (wall time via `clock.Real`), not in the engine. `api.ChaosEngine.Do` stays identity-free; no `DoFromIP`.
- 2026-11-12, lead, BLOCKER (W2): `TestAdmissionCapAndHostFaults` is flaky under -race (3/10 on w2-host 7ba3020 alone, 20/30 after merging main). 7ba3020 is held off main until it passes `-race -count=50`.
- 2026-11-12, W3, CONTRACT-REQUEST: fleet needs W2 constructors and fault hooks for agent kill/restart/pause-heartbeats/skew/disk-full, driver crash/hang/slow-boot/kill-guestd, simnet partition/heal/one-way/latency/loss/duplicate/reorder, and attacks 1,2,4–8 executed through the real guest/network/driver boundary. Please provide the surface in COORDINATION.md. W3 keeps a local Backend adapter (`Apply`, `Attack`, `Topology`, `Reset`, `Snapshot`, `ControlPlane`) so frozen api interfaces remain unchanged. Also request config-owned ambient base rate, attacker interval, load mix defaults, and preset fault duration; until approved these derive from existing heartbeat/lease/boot timings rather than adding cross-owned config fields.
- 2026-11-12, W3, DECISION (lead approved in thread): added config AmbientEvery=100ms, per-mode ChaosBaseRates (attempts/s at intensity 1), AttackerEvery=2s, PresetHealAfter=30s, PresetStepEvery=15s, LoadPriorityMix=[.2,.5,.3]. Existing timings/limits unchanged. Ambient selection uses seeded weighted rates and intensity scales their total linearly. Real-time per-IP limits and idle reset use clock.Real, independent of pause/speed.
- 2026-11-12, W3, STATUS: real W2 Network/Agent/Fake wired in internal/fleet. fleet.New(ctx, Options{Config,Racks,HostsPerRack,Seed,Speed,Clock,StorePath,SchedulerFactory}) returns (*Fleet,error); Fleet exposes Control, Chaos, Clock, Latest, Subscribe and Close. SchedulerFactory is temporary until W1 lands and otherwise returns explicit 503 writes. Invariants 1–3 now read raw independent agent slots/driver inventory; no scheduler-belief fallback. Scaled clock and deliberately broken-state invariant tests pass. Full scenario report.json evidence is not available until W5's runner lands.
- 2026-11-12, lead, DECISION (corrects the lead's epoch rule; found in review of w1-core bdf572f): the scheduler moves its epoch only when a host reports one *strictly greater* than its own: `if reported > epoch { epoch = reported + 1 }`. With `>=`, every heartbeat echoing the current epoch bumps it, the ack carries the new one, and the next heartbeat bumps it again: one SQLite commit and one reconcile event per heartbeat, forever. Equal epochs are safe because agents accept commands at their own epoch and the previous scheduler instance is dead.
- 2026-11-12, W3, CONTRACT-REQUEST: SimRequest and SimStatus need optional Racks []string for ambient scope filtering (nil/empty = all racks), which the plan requires but frozen bodies cannot express. Also SimRequest.Seed int64 cannot distinguish explicit seed 0 from omitted; keeping current behavior (nonzero reseeds) unless the lead approves a pointer or separate reseed action. W4 can call Chaos.DoFromIP(ctx, RemoteAddr, action, req) through an optional local interface for real-time per-IP limiting; api.ChaosEngine.Do has no caller identity.
- 2026-11-12, W3, STATUS (supersedes earlier rate-limit/seed proposal): removed DoFromIP and its rate buckets; W4 owns per-IP HTTP hardening. Explicit Sim("seed") supports 0; Sim("chaos") never reseeds. Ambient scope honors SimRequest.Racks; global/scheduler faults are skipped when a rack scope is selected because W2's global fault hooks cannot isolate those effects to one rack. Config owns all chaos cadence/rates.
- 2026-11-12, W3, STATUS (lead approved EventsSince hook): fleet checks the complete scheduler EventsSince log, separately from lossy Subscribe and UI Recent. A truncated read emits an invariant-5 event/counter with detail "events lost". Tests place an illegal transition outside the 50-event UI window and verify it is still detected. Attacker tests exercise accepted forged-register replies, allowed exfiltration and accepted over-quota writes as successful attacks; missing replies are unknown, and valid heartbeats continue. Real Compromise callback is waiting for W2's race-fix merge to main.
- 2026-11-12, W3, GATE: three consecutive fresh `GOFLAGS=-count=1 make verify` runs pass, plus ten race runs of chaos/fleet/invariant. Scenarios 1–6 remain blocked on W1 scheduler and W5 scenario runner landing on main. Compromise remains an explicit 501 until W2's approved hook lands; slow-store likewise awaits SetStoreDelay. Drain dispatch/cordon are wired, but graceful removal and rolling-maintenance completion still need real-scheduler integration validation.
