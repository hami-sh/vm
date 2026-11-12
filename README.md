# macOS VM control plane

A Go control plane for macOS VMs across three racks, four hosts per rack and two VM slots per host. It demonstrates lifecycle correctness under guest, host, rack and scheduler failures: readiness comes from a checked guest registration, placement is persisted before commands are sent, and generations fence stale copies. Live deployment: **https://controlplane-production-0652d599.up.railway.app** (`/healthz`). The initial deployment is the Phase 0 health stub; the source now serves the dashboard and assembled fleet, pending release. Built as five parallel workstreams; see [COORDINATION.md](COORDINATION.md).

## Try this: a 60-second tour

In the local dashboard (or the live deployment after release):

1. Flood the 24 slots. Watch admission queue by priority, then 429s or shedding at the 32-entry cap.
2. Kill a host. Its lease becomes Suspect, then Lost; replacement VMs receive a new generation.
3. Partition rack r2. Replacements use r1 and r3; the isolated copies may still be running.
4. Heal r2. Look for the fencing event and stale copies being stopped.
5. Open a recorded run to review snapshots, chaos markers and the machine-readable verdict.

For local development, install Go 1.27.4 and run `make run`. The binary listens on `$PORT` (8080 by default); `-addr` overrides it.

## Verify and record

`make verify` runs vet, race tests, the real-server SSE/HTTP end-to-end check and every scenario 20 times against isolated real fleets (scheduler, SQLite, agents, drivers, simnet and checker) in-process under the race detector. Fakes test harness semantics, never substitute for the real fleet at this gate. `make e2e` also runs the HTTP/SSE check independently.

```sh
make verify
go run ./cmd/vmctl verify --scenario rack-partition --repeat 50 --speed 5
go run ./cmd/vmctl verify --scenario rack-partition --repeat 50 --speed 1
make e2e
```

The default is four concurrent isolated fleets. On the 16-vCPU / 31-GiB verification orb, use `make verify VERIFY_PARALLEL=8`; this changes concurrency only, not speeds, repeats or thresholds.

The runner defaults to **5×**, including under `-race`. Explicit `--speed 0` calibrates the complete suite at 5× and 50× with the same seeds. Different liveness pass rates select 5×; this includes time-bounded invariants 2 (stale-copy stop deadline) and 8 (prompt placement). Any violation of invariant 1 or 3–7 at either speed fails calibration: reducing speed must never conceal those safety bugs. Failures at both speeds also stop calibration. Use `--speed` for an explicit diagnostic override, `--seed` to choose the seed range, `--parallel` to cap isolated systems and `--output` to choose the recording directory. Seeds reproduce fault choices, not goroutine scheduling. Recovery latency is measured from Lost/Unhealthy to Ready transition events, not snapshots, which can miss transient lifecycle states.

Each recorded run contains the following files. `vmctl verify` writes `runs/<id>/` by default; `make verify` keeps each full batch under `runs/verify-*/` and bundles one passing run per scenario from the latest successful batch into `runs/latest/`. The bundle keeps every twentieth frame, both endpoints, and every frame within ±2 simulated seconds of a chaos/violation marker. Reports, metrics and event logs are unchanged. Bundling fails above 100 MB; failed batches never replace the last green image evidence. `vmctl bundle --input runs/verify-...` can regenerate the bundle without rerunning the suite.

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
| Host powers off | Scheduler host lease expires; its VMs die with the host | Suspect → Lost; re-place each affected VM at generation +1 in replacement class 0, ahead of queued work | Replacement priority; no stale copy remains to stop on return |
| Overload | Slot pressure and bounded queue | Priority admission, batch shedding, 429 + Retry-After | Bounded queue memory; interactive work prioritized |
| Rack partition / heal | All rack host leases expire | Replace elsewhere; fence stale inventory on heal | One authoritative copy; eventual stale-copy stop |
| Gray failure / restart-host | Heartbeats stop while VM copies keep running; old inventory returns | Replace at newer generations; fence and stop stale copies on reconnection | Retained old copies cannot remain authoritative |
| Scheduler crash | Scheduler unavailable / restart | Creates return 503; running guests continue; reconcile inventory | Data plane survives control-plane outage |
| Wipe store | Fresh store; hosts report surviving inventory | Adopt only Ready VMs with pool metadata; restore their idempotency keys and request hashes; stop unknown booting copies | Queued and booting work is lost, not recovered; retried creates for adopted Ready VMs return the same VM |
| Concurrent duplicate create | Atomic idempotency lookup | Return the same VM | Exactly one logical VM per key |
| Impostor guest | Channel/token/generation mismatch | Reject, count, log; repeated abuse quarantined | Claimed message identity cannot mark another VM Ready |

Power-off is not a fencing test: the original VMs are gone. Stale-copy fencing is exercised by partition/heal, paused heartbeats (gray failure), and restart-host with retained VM copies. Store-wipe adoption follows the [coordination policy](COORDINATION.md): only an unknown Ready VM with pool metadata can be adopted into a fresh store; unknown booting or pool-less copies are stopped, and unknown copies in a non-fresh store are never adopted. During the initial host-lease recovery window, creates return 503 + Retry-After so live hosts can report their surviving keys before retries are accepted. A VM lost together with its host at the time of the wipe cannot be adopted.

The six scenario presets are split brain, gray failure, thundering herd, rolling maintenance, hostile tenant and bad day. See `internal/scenario` for their explicit timelines. These are integration specifications; passing the harness's fake-fleet unit tests does not certify the system against them.

## Security: guests are untrusted

Identity comes from the transport-stamped sending channel, never a guest-supplied VM ID. Registration also requires the current generation and boot token. StartVM and StopVM are idempotent for `(vm, generation)`; a stale stop must never kill a newer boot.

The simulated policy denies guest-to-guest and guest-to-control-plane access. Guests may speak vsock only to their own host agent; permitted egress goes through an allowlist. The evidence panel counts blocked attacks and successful attacks separately; any success fails verification. Rate limiting, bounded messages, disk limits and quarantine constrain abuse. These are modeled controls, not proof of a hardened macOS boundary.

On real Macs, Virtualization.framework's default NAT puts a host's guests on a shared subnet; NAT alone is not tenant isolation. Replace the simulated policy with per-VM pf rules or a separate userspace network stack per VM, plus an allowlisted egress proxy. Hypervisor escapes are outside this simulator's security model.

The public demo has no authentication and must contain no secrets or tenant workloads. Real deployment requires mTLS between agents and scheduler, OIDC/RBAC on the API, real vsock channel binding, actual network sandboxing, quotas and audit retention. Global demo caps and per-IP chaos limits are part of the integration plan, not security guarantees of the initial stub.

## Scaling

Rack spreading reduces correlated loss; reserve headroom if a rack failure must be absorbed without shedding. A scheduler per rack would fragment capacity and complicate ownership. Scale by independent cells with a routing layer, then regions. Image distribution, boot latency, storage throughput and the single durable writer are likely limits before placement scoring. Measure before adding hierarchy.

## Simulated versus real

Railway runs a Linux container, **not macOS VMs**. The fake hypervisor models boots, crashes, leases and failure injection on Go goroutines. Real macOS boots take roughly 30–60 seconds, not the shortened simulation timings. Tart / Virtualization.framework integration requires Apple hardware and real guest readiness plumbing. Apple's macOS licence generally permits up to two additional virtual instances on qualifying Apple hardware; check the applicable licence before deployment.

## Deployment

The multi-stage Dockerfile builds a static `CGO_ENABLED=0` binary with `golang:1.27`, then runs it as non-root in `gcr.io/distroless/static`. Railway serves `$PORT`, checks `/healthz`, runs one replica and has sleep disabled. SQLite and recordings live on ephemeral local disk, without a volume: redeploys lose demo state. This is deliberate and not a production durability design. Deploy credentials stay outside the repository and container.

Run `make verify` in the deployment build context before building or uploading the image. The Dockerfile embeds `runs/latest/` at `/runs` and sets `RUNS_DIR=/runs`; `.dockerignore` excludes earlier/failed batches and Git metadata. The files are owned by the non-root runtime user so dashboard-started recordings can share that directory. A source-only build still works but has no bundled evidence; a Git-only deploy cannot include ignored local recordings. For an approved release, upload the verified workspace with `railway up --no-gitignore --service controlplane`; `.railwayignore` includes only the latest batch while excluding historical runs, databases and local scratch files. Without `--no-gitignore`, the CLI's normal Git-ignore handling excludes the recordings.

The 0%-chaos interactive baseline at 5× under `-race` is p99 **6017 ms** (340 boots, 20 seeds, 17/24 slots per seed). The configured chart target is twice that, **12034 ms** of simulation time. The separate, opt-in `target-50pct-chaos` profile offers 17 interactive VMs on 24 slots (70.8%), with 50% ambient guest/host/network/control faults enabled before boot. Only this measurement filters load/topology injections, so its offered load and capacity stay fixed. The default `bad-day` preset keeps unfiltered 50% ambient chaos, the flood, latency spike and scheduler restart mid-flood. The measurement reports first-Ready p99, sample/request counts and target attainment in `report.json`; missing/unready requests prevent a target-met result. The target is reported, never used to waive or add an invariant gate. Run it separately with `go run -race ./cmd/vmctl verify --scenario target-50pct-chaos --repeat 20 --parallel 8 --speed 5 --output runs/target-50pct-chaos`.

Measured across seeds 1–20 at 5× under `-race` (parallel2): **17925 ms pooled p99**, above the **12034 ms** target, with **339/340** initial requests observed Ready; the missing request is not counted as a zero-latency success. Four of 20 runs missed the target (worst per-run p99 **32209 ms**). The scenario safety verdict was **19/20**, with one invariant-2 stale-copy stop deadline violation; this measurement is not a green correctness gate. Reports and full event/snapshot evidence remain under `runs/target-50pct-chaos/` on the verification orb.

Known limitation: the 5× repeat50 diagnostic (seeds 20–69) passed **49/50**, with all **12 invariant-2 events from seed 21**. The last event was **3994 ms after gen2 placement**, or **1994 ms past the 2 s bound**; this is the last observed violation, not the driver's stop time. At 2× the same seed range passed **49/50**, with 16 invariant-2 events from seed 21 (last violation 4314 ms after placement, 2314 ms past the bound). At 1× it passed **50/50**, with zero violations. The exact seed-20 reruns also pass at 2× and 1×. These are diagnostics at the stated speeds, not a relaxed bound or proof of a bounded driver stop. The measurement stays outside the default gate and release image.

The stale-copy bound (one heartbeat) is checked only while the host's heartbeat lease is healthy. With paused heartbeats or ack loss, an old copy is stopped by the next delivered StartVM or heartbeat ack; the agent does not self-fence on missed acks, so stop time on a heartbeat-silent host is not bounded or observed by these recordings. One authoritative copy held in every run.

### Release sequence (approval required)

Upload only after release approval and a passing `make verify` (including e2e). Use a production-scoped project token in `RAILWAY_TOKEN`, not the workspace `RAILWAY_API_TOKEN`; do not paste either into the repository or a thread. Run this sequence in the verified workspace containing the ignored recordings, not a fresh Git-only checkout.

```sh
make verify VERIFY_PARALLEL=8
docker build -t vm-controlplane:verified .
test "$(find runs/latest -mindepth 2 -maxdepth 2 -name report.json | wc -l)" -eq 14
: "${RAILWAY_TOKEN:?Set the production project token securely before uploading}"
env -u RAILWAY_API_TOKEN npm exec --yes --package @railway/cli -- railway up \
  --project bdb8a2f4-7793-4941-a9c6-8caf5ed9f35b \
  --environment production --service controlplane --no-gitignore --detach
```

The upload command starts a deployment; it is not a health verdict. After the deployment reports success, check `/healthz`, the dashboard, `/v1/events` and `/v1/runs` at the live URL and open a bundled recording. Local playback should set `RUNS_DIR=runs/latest`; the image sets it to `/runs` automatically.

## What's next

Warm pools; scheduler HA through leader election and epoch fencing; mTLS and OIDC; real vsock; image caching and locality-aware placement; quotas per pool; a deterministic simulator. Snapshot/resume is a stretch goal only after the correctness gates pass.
