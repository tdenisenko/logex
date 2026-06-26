# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed log segments, and serves verified logs through the dashboard, query APIs, and live transfer notifications.

The active work is PR #96 on branch `perf/historical-sync-live-scheduler`. This branch focuses on making historical EL sync stable under peer-tail latency while preserving ordered cryptographic verification and storage writes.

The Mac mini client runs from `/Volumes/SSD 4TB/LogEx` on HTTP port `18683`. The current production candidate routes dense historical body/receipt batches through the live chunk-owned scheduler instead of the older decoupled dense fast path. The decoupled path reached higher bursts in some samples, but repeatedly stalled behind under-owned prefix chunks. The live path now treats stale or candidate-exhausted expected-prefix roles as prefix-critical work before falling back to salvage. The latest five-minute remote sample measured `195.4` actual blocks/sec with `1` low window and `0` zero-progress windows, improving over the previous live-scheduler sample of `125.5` actual blocks/sec with `8` low windows and `3` zero-progress windows.

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
  - Earliest missing prefix roles now become prefix-critical when the active role is stale or all current candidates are exhausted, not only after a buffered suffix exists.
  - This lets the live scheduler extend fresh candidates and use the bounded prefix-critical role lane before the slower salvage fallback.
  - Added tests for stale in-flight prefix roles, fresh in-flight roles, and exhausted prefix candidate lists.
  - Remote validation improved the five-minute sample from `125.5` blocks/sec, `8` low windows, and `3` zero windows to `195.4` blocks/sec, `1` low window, and `0` zero windows.

## Remaining TODOs

1. Validate the live scheduler over a long historical run.
   - Reason: five-minute dense samples prove the stall mode improved, but full-run performance varies by log density and peer mix.
   - Completion criteria: record start-to-genesis time, p50/p90/max logs/sec, actual floor blocks/sec, low/zero-progress windows, bandwidth, CPU, memory, disk, peer counts, resets, and failures.
   - Current progress: the latest dense sample had `0` zero-progress windows and sustained high receive bandwidth, but a full fresh run is still required before closing this TODO.

2. Remove or formally rework the disabled decoupled dense path.
   - Reason: the active production path no longer selects it, but its helper code remains in the file for now to avoid mixing a large deletion with the scheduler routing change.
   - Completion criteria: either delete the decoupled-only executor/tests after the live scheduler full-run validation, or reintroduce it only if it is redesigned with explicit prefix ownership and proves faster than the live path without recurring stalls.

3. Add bandwidth-aware scheduler diagnostics and admission.
   - Reason: current metrics expose request pressure and role counters, but not enough to quickly separate peer tail latency, network saturation, write backpressure, and local processing limits.
   - Completion criteria: status/debug metrics expose live backlog, prefix wait age, retry/hedge counts, peer timeout share, bandwidth use, and write/prepare pressure; admission uses those signals without dashboard noise.

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
  - Resolution: instrumented the scheduler, identified under-owned prefix chunks in the decoupled dense path, switched dense plans to the live scheduler path, and made stale/exhausted live prefix roles prefix-critical before salvage.
  - Remaining: full-run validation is still required before concluding PR #96.
- Challenge: several small scheduler experiments improved isolated metrics but regressed end-to-end samples.
  - Resolution: rejected and reverted candidates that increased duplicate pressure, reduced dense batch efficiency, or produced more low/zero-progress windows.
  - Remaining: future scheduler changes should be compared against the live-scheduler baseline over longer samples.
- Challenge: the roadmap had accumulated too much experiment-by-experiment detail.
  - Resolution: condensed it to current state, decisions, and remaining work.

## Dead Code and Obsolescence Cleanup

- Inspected the decoupled dense body/receipt path after selecting the live scheduler for production dense plans.
- Removed diagnostic-only status fields from the final patch because they were tied to the rejected decoupled path.
- Retained the disabled decoupled dense executor for now because deleting it is a larger cleanup best done after the live scheduler has a long-run validation baseline.
- `.DS_Store` remains untracked and unrelated.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`.
- New branch created this run: no.
- Commits made during this run: `perf: route dense history through live scheduler`; pending prefix-repair commit.
- Pull request status: PR #96 remains the active draft performance PR.
- Merge status: not ready until the live scheduler candidate has longer validation or the user accepts this milestone as the PR boundary.
- Blockers: none.

## Known Issues or Risks

- The live scheduler candidate improves the observed stall mode, but the latest validation is still a short sample.
- The disabled decoupled dense path should not remain indefinitely without either deletion or a redesigned prefix-ownership implementation.
- Peer mix and network routing materially affect measurements; benchmark notes must record routing mode, serving peers, and bandwidth.
