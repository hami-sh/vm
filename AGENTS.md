# macOS VM control plane

A Go control plane that places macOS VMs across a simulated fleet (3 racks × 4 hosts × 2 slots) and stays correct when guests, hosts, racks or the scheduler fail. The full plan is [docs/plan.md](docs/plan.md); read the sections for your workstream before starting.

## Layout and ownership

| Package | Owner |
| --- | --- |
| `internal/api`, `internal/clock` (interface, Real, Fake), `internal/actor`, `internal/config`, `internal/vm/legal.go`, `internal/store/schema.sql` | Phase 0 (lead). Request changes in COORDINATION.md |
| `internal/vm`, `internal/scheduler`, `internal/store` | W1 Core |
| `internal/agent`, `internal/driver`, `internal/guest`, `internal/simnet`, `cmd/guestd` | W2 Host side |
| `internal/chaos`, `internal/invariant`, scaled clock in `internal/clock`, `/v1/sim`, `/v1/topology` | W3 Chaos + sim |
| `internal/web`, `web/static` | W4 Web |
| `internal/scenario`, `cmd/vmctl`, `Dockerfile`, `README.md`, Makefile `verify` | W5 Ship |

Edit only packages your workstream owns. If you need a contract change, append a request to COORDINATION.md and use a local adapter until it is approved.

## Runtime rules

- Every component is an actor: one goroutine owns its state; logic lives in `handle(now, msg) []Effect` so tests can call it directly. See `internal/actor`.
- Never call `time.Now`, `time.After`, `time.Sleep` or `time.NewTimer` outside `internal/clock`. Use the injected `clock.Clock`. Tests use `clock.NewFake`.
- Slow work (driver calls) runs via `actor.Work`, never inside `handle`.
- HTTP handlers never touch component state; they `actor.Ask` or read published snapshots.
- Persist, then act: the generation bump is committed before `StartVM` is sent.
- Every timing and limit lives in `internal/config`.

## Verification rules

1. Run `make verify` after every change, and read `runs/<id>/report.json`, not just the exit code.
2. Work isn't done until `make verify` passes three times in a row.
3. Never loosen a check, timeout or threshold to make a run pass without writing down why in COORDINATION.md. Loosening a check is a decision, not a fix.
4. When a run fails: open its `report.json`, find the first violation, read the events leading up to it, fix the cause, and rerun that one scenario with `--repeat 50` before the full suite.
5. If a failure only shows up at high speed, rerun at 5× and 1×. If it disappears, the timeouts are too tight for that speed: cap the speed rather than ignore the failure.

## Git

- Work on your workstream branch (`w1-core`, `w2-host`, `w3-chaos`, `w4-web`, `w5-ship`). Rebase on `origin/main` before each gate.
- Nothing merges to `main` unless `go test -race ./...` passes.
- Commit at reasonable boundaries with clear messages.
- Read COORDINATION.md at the start of each task; append to it when you finish a gate or hit a blocker.
