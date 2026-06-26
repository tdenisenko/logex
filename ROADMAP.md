# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed log segments, and serves verified logs through the dashboard, query APIs, and live transfer notifications.

The active work is PR #96 on branch `perf/historical-sync-live-scheduler`. This branch focuses on making historical EL sync stable under peer-tail latency while preserving ordered cryptographic verification and storage writes.

The Mac mini client runs from `/Volumes/SSD 4TB/LogEx` on HTTP port `18683`. The current production candidate routes dense historical body/receipt batches through the live chunk-owned scheduler instead of the older decoupled dense fast path. The decoupled path reached higher bursts in some samples, but repeatedly stalled behind under-owned prefix chunks. The live path now treats stale in-flight expected-prefix roles as prefix-critical work before falling back to normal prefix reassignment and salvage, and keeps a healthy floor of six active body/receipt fetches. The latest kept five-minute remote sample measured `187.5` actual blocks/sec with `1` low window and `0` zero-progress windows. A later bounded slot-overdraft admission experiment regressed to `144.5` actual blocks/sec with `5` low windows and `3` zero-progress windows, so it was reverted.

## Completed Since Last Run

- Switched dense historical body/receipt plans to the live scheduler path.
  - Reason: diagnostics showed the decoupled dense path could have available peers and bandwidth while the earliest required prefix chunk had too few in-flight owners.
  - Result: remote validation improved the latest candidate from `101.3` blocks/sec with `11` low windows and `2` zero windows to `135.5` blocks/sec with `4` low windows and `1` startup zero window.
- Removed the diagnostic-only status API additions from the final local patch.
  - Reason: the prefix wait/owner fields were useful to identify the decoupled-path bottleneck, but would be misleading once that path is no longer selected.
- Updated reservation tests so dense plans now assert live-scheduler routing and no longer expect decoupled spare-window reservations.
- Validated locally:
  - `cargo test -p logex-sync`
  - `cargo fmt --check`
  - `cargo clippy -p logex-sync -- -D warnings`
- Deployed the cleaned source to the Mac mini, rebuilt `logex-node --release`, restarted LogEx, and confirmed `/status` responds after peer warm-up.
- Tightened live prefix repair:
  - Earliest missing prefix roles now become prefix-critical only when an in-flight role is stale.
  - Exhausted candidate lists are left to the normal exhausted-prefix removal and reassignment path, which avoids retry amplification.
  - Remote validation improved from `120.6` blocks/sec, `10` low windows, and `4` zero windows to `170.7` blocks/sec, `4` low windows, and `0` zero windows.
- Raised the healthy historical body/receipt active-fetch floor from four to six.
  - Reason: after prefix retry amplification was removed, active fetches could still drain too low during otherwise healthy samples.
  - Result: the kept remote sample improved to `187.5` blocks/sec with `1` low window and `0` zero windows.
- Rejected a bounded slot-overdraft admission experiment.
  - Reason: it increased active fetches but reintroduced zero-progress windows and lower throughput.
  - Result: the rejected sample measured `144.5` blocks/sec with `5` low windows and `3` zero windows; the code was reverted locally and the Mac mini was redeployed to the kept candidate.
- Rejected slot-margin concurrency capping for queued ready plans.
  - Reason: reducing a blocked ready plan's live chunk concurrency to current body/receipt slot margins avoided overdraft, but did not materially improve end-to-end floor advancement.
  - Result: the five-minute sample measured `193.1` blocks/sec with `5` low windows and `0` zero windows, only slightly above the kept `187.5` blocks/sec baseline and with worse low-window behavior; the code was reverted and the Mac mini was restored to the kept candidate.
- Added focused live scheduler refill diagnostics to `/status` and the dashboard advanced section.
  - Reason: the remaining low-progress windows need to be classified before another architectural change; raw logs/sec alone cannot distinguish request-slot pressure, refill policy denial, write backpressure, or prepare pressure.
  - Result: status now exposes scheduler pipeline depth, buffer depth, critical/write refill limits, body/receipt slot margins, and write backpressure. The current remote run is `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260626-163104.log`.

## Remaining TODOs

1. Validate the live scheduler over a long historical run.
   - Reason: five-minute dense samples prove the stall mode improved, but full-run performance varies by log density and peer mix.
   - Completion criteria: record start-to-genesis time, p50/p90/max logs/sec, actual floor blocks/sec, low/zero-progress windows, bandwidth, CPU, memory, disk, peer counts, resets, and failures.
   - Current progress: the latest dense sample had `0` zero-progress windows and sustained high receive bandwidth, but a full fresh run is still required before closing this TODO.

2. Remove or formally rework the disabled decoupled dense path.
   - Reason: the active production path no longer selects it, but its helper code remains in the file for now to avoid mixing a large deletion with the scheduler routing change.
   - Completion criteria: either delete the decoupled-only executor/tests after the live scheduler full-run validation, or reintroduce it only if it is redesigned with explicit prefix ownership and proves faster than the live path without recurring stalls.

3. Complete the live request scheduler admission design.
   - Reason: the current live scheduler is better than the decoupled path, but still relies on conservative slot admission and can leave useful bandwidth idle when peer-tail latency rises. The rejected overdraft experiment showed that simply borrowing more slots increases duplicate pressure and hurts end-to-end progress.
   - Completion criteria: implement admission that keeps enough independent prefix work active without overfilling per-peer request slots; expose focused debug metrics for live backlog, prefix wait age, retry/hedge counts, peer timeout share, bandwidth use, and write/prepare pressure; validate against the kept baseline with longer samples and no recurring zero-progress windows.
   - Current progress: refill decision diagnostics are now exposed. The next change should be a deliberate scheduler architecture pass, not another small threshold experiment.

4. Complete EL production hardening.
   - Reason: scheduler changes must not weaken checkpoint freshness, forward sync, reorg handling, restart safety, low-disk behavior, query correctness, or dashboard access.
   - Completion criteria: tests or smokes cover fresh-checkpoint enforcement, stale restart rejection, CL tracking, EL forward sync, EL reverse sync, invalid peer data, reorg handling, low disk behavior, authenticated dashboard access, and a clean full-sync candidate run.

## Design Decisions

- Dense historical body/receipt batches now use the live chunk-owned scheduler by default.
  - Why: it keeps per-chunk body/receipt state, tracks in-flight role ownership, and can repair prefix-critical gaps without making progress depend on separate body and receipt loops completing at the same time.
  - Alternative considered: keep tuning the decoupled dense fast path. It was rejected for now because repeated samples showed prefix-tail stalls even when later work and peers were available.
  - Tradeoff: peak logs/sec may be lower than the best decoupled bursts, but sustained progress is smoother and less vulnerable to long zero-progress windows.
- Historical sync remains EL-driven after a valid recent CL checkpoint.
  - Why: CL forward tracking is required for live head and reorg safety, but historical reverse sync should not wait on CL head movement once the pivot is validated.
- Downloads, extraction, and writes may overlap, but verified storage advancement remains ordered.
  - Why: this preserves block parent-chain and receipt-root verification semantics while still allowing async work to keep the machine busy.
- Machine-specific routing scripts are operational tooling, not repository behavior.
  - Why: LogEx should run correctly regardless of whether this Mac mini uses full VPS routing, dashboard-only forwarding, or direct local networking.

## Challenges and Resolutions

- Challenge: historical progress had recurring spikes and plunges despite available peers.
  - Resolution: instrumented the scheduler, identified under-owned prefix chunks in the decoupled dense path, switched dense plans to the live scheduler path, made stale in-flight live prefix roles prefix-critical before salvage, and raised the healthy active-fetch floor to six.
  - Remaining: full-run validation is still required before concluding PR #96.
- Challenge: several small scheduler experiments improved isolated metrics but regressed end-to-end samples.
  - Resolution: rejected and reverted candidates that increased duplicate pressure, reduced dense batch efficiency, or produced more low/zero-progress windows, including bounded slot overdraft and slot-margin concurrency capping.
  - Remaining: future work should stop one-line tuning and move to a deliberate admission/scheduler change compared against the kept live-scheduler baseline.
- Challenge: low-progress windows still need a precise cause after the live scheduler milestone.
  - Resolution: added status/UI diagnostics for pipeline depth, buffer depth, refill limits, slot margins, and write backpressure so the next architectural change can be based on the scheduler's actual decisions.
  - Remaining: use those diagnostics to implement the next admission/refill redesign and validate it over longer samples.
- Challenge: the roadmap had accumulated too much experiment-by-experiment detail.
  - Resolution: condensed it to current state, decisions, and remaining work.

## Dead Code and Obsolescence Cleanup

- Inspected the decoupled dense body/receipt path after selecting the live scheduler for production dense plans.
- Removed diagnostic-only status fields from the final patch because they were tied to the rejected decoupled path.
- Rechecked the stale remote warning about obsolete body/receipt metric fields; local code no longer contains those fields and the Mac mini source was synchronized to match.
- Retained the disabled decoupled dense executor for now because deleting it is a larger cleanup best done after the live scheduler has a long-run validation baseline.
- `.DS_Store` remains untracked and unrelated.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`.
- New branch created this run: no.
- Commits made during this run: `perf: route dense history through live scheduler`; `perf: stabilize live historical scheduler`; `docs: record rejected scheduler admission cap`; pending commit for refill diagnostics.
- Pull request status: PR #96 remains the active draft performance PR.
- Merge status: not ready as a production-complete scheduler; can be accepted only as a measured live-scheduler milestone before the larger admission redesign.
- Blockers: none.

## Known Issues or Risks

- The live scheduler candidate improves the observed stall mode, but the latest validation is still a short sample.
- The disabled decoupled dense path should not remain indefinitely without either deletion or a redesigned prefix-ownership implementation.
- Peer mix and network routing materially affect measurements; benchmark notes must record routing mode, serving peers, and bandwidth.
