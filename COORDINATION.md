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
