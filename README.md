# macOS VM control plane

A Go control plane for macOS VMs across three racks, four hosts per rack and two VM slots per host. It demonstrates lifecycle correctness under guest, host, rack and scheduler failures: readiness comes from a checked guest registration, placement is persisted before commands are sent, and generations fence stale copies. Live deployment: **https://controlplane-production-0652d599.up.railway.app** (`/healthz`). The initial deployment is the Phase 0 health stub; the dashboard and assembled fleet are being integrated. Built as five parallel workstreams; see [COORDINATION.md](COORDINATION.md).

## Try this: a 60-second tour

Once the dashboard is integrated:

1. Flood the 24 slots. Watch admission queue by priority, then 429s or shedding at the 32-entry cap.
2. Kill a host. Its lease becomes Suspect, then Lost; replacement VMs receive a new generation.
3. Partition rack r2. Replacements use r1 and r3; the isolated copies may still be running.
4. Heal r2. Look for the fencing event and stale copies being stopped.
5. Open a recorded run to review snapshots, chaos markers and the machine-readable verdict.

For local development, install Go 1.27.4 and run `make run`. The binary listens on `$PORT` (8080 by default); `-addr` overrides it.

## Verify and record

`make verify` runs vet, race tests, every scenario 20 times and the real-server SSE/HTTP smoke test. **It is intentionally red while the server exposes only `/healthz`.** Fakes test harness semantics, never substitute for the real fleet at the verification gate.

```sh
make verify
go run ./cmd/vmctl verify --scenario rack-partition --repeat 50 --speed 5
go run ./cmd/vmctl verify --scenario rack-partition --repeat 50 --speed 1
go run ./cmd/vmctl e2e
```

The default runner calibrates the complete suite at 5× and 50× with the same seeds. Different per-scenario pass rates select 5×. A failed run is still a failure; failures at both speeds stop calibration. Use `--speed` for an explicit diagnostic override, `--seed` to choose the seed range, `--parallel` to cap isolated systems and `--output` to choose the recording directory. Seeds reproduce fault choices, not goroutine scheduling.

Each `runs/<id>/` contains:

| File | Contents |
| --- | --- |
| `events.jsonl` | Events and scenario action markers |
| `snapshots.jsonl` | Published `api.Snapshot` frames |
| `metrics.json` | Observed measurements (absent samples are not zero-latency successes) |
| `report.json` | `api.RunReport`: checks, first failing sequence, seed, speed and verdict |

Read the first failed report and its preceding events, fix the cause, then rerun that scenario with `--repeat 50`. A gate requires three consecutive successful `make verify` runs. Never relax a threshold just to turn it green; record decisions in COORDINATION.md.

## Architecture

See the [design plan](docs/plan.md#architecture) for the architecture diagram and detailed contracts.

| Component | Responsibility |
| --- | --- |
| Scheduler | Single owner of lifecycle, persisted generations, admission and placement |
| Host agent | Final admission decision, slot lifecycle, guest leases and fencing |
| guestd | Proves readiness (including `simctl`), then renews its guest lease |
| Driver | Hypervisor boundary: simulated FakeDriver or future real Tart integration |
| Simnet | Authenticated endpoint identity, guest isolation and injected network failures |
| Fleet / chaos | Assembly, snapshots, seeded faults and invariant evidence |
| Web | HTTP API, SSE snapshots, dashboard and recorded-run playback |
| Scenario / vmctl | Headless load, timelines, Always / Eventually / Metric checks |

Each component is an actor with one state-owning goroutine. Slow driver work runs outside the actor. All deadlines use an injected clock. Racks are failure domains, not separate scheduler tiers. guestd talks to its host agent rather than the scheduler: local readiness and guest leases stay close to the hypervisor and identity-bearing channel.

## Lifecycle and identity

The normal path is Pending → Booting → Ready. Guest failures go through Unhealthy; host loss goes through Lost. Deletion goes through Terminating → Terminated. Bounded retries can end in Failed. The authoritative transition table is [internal/vm/legal.go](internal/vm/legal.go), including queued deletion, shedding and quarantine edges.

Three IDs have different jobs:

- **Idempotency key:** one logical create; reuse with a different body is 422.
- **Boot token:** fresh per boot, checked alongside channel identity; never a public credential.
- **Generation:** persisted monotonic fencing token. Delayed messages cannot revive or stop a newer copy.

Desired state is separate from observed state, so a deleted VM is not resurrected by failover. A partition can leave two physical copies running: the guarantee is one **authoritative** copy, with stale copies stopped once communication resumes.

## Failure modes

These are intended contracts, not claims that the current health-only deployment already passes them.

| Failure | Detection | Response | Guarantee |
| --- | --- | --- | --- |
| guestd dies | Agent guest lease expires | Unhealthy; recycle on the same host | Only checked, Ready VMs receive work |
| Boot hangs | Boot deadline | Stop, back off, retry elsewhere; fail after bounded attempts | Bounded boot time and attempts |
| Host dies | Scheduler host lease expires | Suspect → Lost; replace at generation +1 | Desired VMs are not forgotten |
| Overload | Slot pressure and bounded queue | Priority admission, batch shedding, 429 + Retry-After | Bounded queue memory; interactive work prioritized |
| Rack partition / heal | All rack host leases expire | Replace elsewhere; fence stale inventory on heal | One authoritative copy; eventual stale-copy stop |
| Scheduler crash | Scheduler unavailable / restart | Creates return 503; running guests continue; reconcile inventory | Data plane survives control-plane outage |
| Concurrent duplicate create | Atomic idempotency lookup | Return the same VM | Exactly one logical VM per key |
| Impostor guest | Channel/token/generation mismatch | Reject, count, log; repeated abuse quarantined | Claimed message identity cannot mark another VM Ready |

The six scenario presets are split brain, gray failure, thundering herd, rolling maintenance, hostile tenant and bad day. See `internal/scenario` for their explicit timelines. These are integration specifications; passing the harness's fake-fleet unit tests does not certify the system against them.

## Security: guests are untrusted

Identity comes from the transport-stamped sending channel, never a guest-supplied VM ID. Registration also requires the current generation and boot token. StartVM and StopVM are idempotent for `(vm, generation)`; a stale stop must never kill a newer boot.

The simulated policy denies guest-to-guest and guest-to-control-plane access. Guests may speak vsock only to their own host agent; permitted egress goes through an allowlist. The evidence panel counts blocked attacks and successful attacks separately; any success fails verification. Rate limiting, bounded messages, disk limits and quarantine constrain abuse. These are modeled controls, not proof of a hardened macOS boundary.

The public demo has no authentication and must contain no secrets or tenant workloads. Real deployment requires mTLS between agents and scheduler, OIDC/RBAC on the API, real vsock channel binding, actual network sandboxing, quotas and audit retention. Global demo caps and per-IP chaos limits are part of the integration plan, not security guarantees of the initial stub.

## Scaling

Rack spreading reduces correlated loss; reserve headroom if a rack failure must be absorbed without shedding. A scheduler per rack would fragment capacity and complicate ownership. Scale by independent cells with a routing layer, then regions. Image distribution, boot latency, storage throughput and the single durable writer are likely limits before placement scoring. Measure before adding hierarchy.

## Simulated versus real

Railway runs a Linux container, **not macOS VMs**. The fake hypervisor models boots, crashes, leases and failure injection on Go goroutines. Real macOS boots take roughly 30–60 seconds, not the shortened simulation timings. Tart / Virtualization.framework integration requires Apple hardware and real guest readiness plumbing. Apple's macOS licence generally permits up to two additional virtual instances on qualifying Apple hardware; check the applicable licence before deployment.

## Deployment

The multi-stage Dockerfile builds a static `CGO_ENABLED=0` binary with `golang:1.27`, then runs it as non-root in `gcr.io/distroless/static`. Railway serves `$PORT`, checks `/healthz`, runs one replica and has sleep disabled. SQLite and recordings live on ephemeral local disk, without a volume: redeploys lose demo state. This is deliberate and not a production durability design. Deploy credentials stay outside the repository and container.

## What's next

Warm pools; scheduler HA through leader election and epoch fencing; mTLS and OIDC; real vsock; image caching and locality-aware placement; quotas per pool; a deterministic simulator. Snapshot/resume is a stretch goal only after the correctness gates pass.
