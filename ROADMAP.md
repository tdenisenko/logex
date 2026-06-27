# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed log segments, and serves verified logs through the dashboard, query APIs, and live transfer notifications.

The active work is PR #96 on branch `perf/historical-sync-live-scheduler`. This branch focuses on making historical EL sync stable under peer-tail latency while preserving ordered cryptographic verification and storage writes.

The Mac mini client runs from `/Volumes/SSD 4TB/LogEx` on HTTP port `18683`. The current production candidate routes dense historical body/receipt batches through the live chunk-owned scheduler instead of the older decoupled dense fast path. The live scheduler now consumes body/receipt reservations when requests actually start, discards mismatched expected-sequence attempts without resetting lookahead, and refills a missing expected sequence from inside the wait loop instead of waiting for the timed head-of-line retry path. The latest five-minute remote sample measured `290.0` actual blocks/sec with `0` low-progress windows and `0` zero-progress windows while physical receive bandwidth stayed high.

## Completed Since Last Run

- Fixed a live-scheduler head-of-line idle path.
  - Reason: after a mismatched expected-sequence attempt was discarded, the expected sequence could have no active attempt while later work stayed buffered, leaving progress dependent on the slower timed retry path.
  - Result: the wait loop now refills only the missing expected fetch immediately and preserves buffered lookahead.
- Validated locally with `cargo fmt --check`, `cargo test -p logex-sync`, and `cargo clippy -p logex-sync -- -D warnings`.
- Deployed the exact changed source file to the Mac mini, rebuilt `logex-node --release`, restarted LogEx without clearing data, and sampled the remote run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260626-184907.log`.
- Remote validation: five minutes, `83,816` blocks advanced, `290.0` actual blocks/sec, `0` low-progress windows, `0` zero-progress windows, and no destructive historical fetch pipeline reset recurrence.

## Remaining TODOs

1. Validate the live scheduler over a long historical run.
   - Reason: five-minute dense samples prove the stall mode improved, but full-run performance varies by log density and peer mix.
   - Completion criteria: record start-to-genesis time, p50/p90/max logs/sec, actual floor blocks/sec, low/zero-progress windows, bandwidth, CPU, memory, disk, peer counts, resets, and failures.
   - Current progress: the latest dense sample had `0` low/zero-progress windows and sustained high receive bandwidth, but a full fresh run is still required before closing this TODO.

2. Remove or formally rework the disabled decoupled dense path.
   - Reason: the active production path no longer selects it, but its helper code remains in the file for now to avoid mixing a large deletion with the scheduler routing change.
   - Completion criteria: either delete the decoupled-only executor/tests after the live scheduler full-run validation, or reintroduce it only if it is redesigned with explicit prefix ownership and proves faster than the live path without recurring stalls.

3. Complete the live request scheduler admission design.
   - Reason: the current live scheduler is better than the decoupled path, but still relies on conservative slot admission and can leave useful bandwidth idle when peer-tail latency rises. The rejected overdraft experiment showed that simply borrowing more slots increases duplicate pressure and hurts end-to-end progress.
   - Completion criteria: implement admission that keeps enough independent prefix work active without overfilling per-peer request slots; expose focused debug metrics for live backlog, prefix wait age, retry/hedge counts, peer timeout share, bandwidth use, and write/prepare pressure; validate against the kept baseline with longer samples and no recurring zero-progress windows.
   - Current progress: refill diagnostics exposed reservation overcounting, mismatched expected-attempt resets, and a missing-expected-fetch wait-loop gap. These are fixed and remotely validated; the remaining work is long-run validation plus any admission redesign that is justified by measured idle bandwidth or CPU.

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
  - Resolution: diagnostics identified reservation double-counting, stale expected-sequence attempts, and a wait-loop missing-expected-fetch gap; all are now handled without dropping valid lookahead.
  - Remaining: validate over a longer run and continue admission work only where bandwidth or CPU is demonstrably idle.
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
- Commits made during this run: `perf: route dense history through live scheduler`; `perf: stabilize live historical scheduler`; `docs: record rejected scheduler admission cap`; `feat: expose historical scheduler refill diagnostics`; `fix: preserve lookahead on stale historical attempts`; pending commit for wait-loop expected-fetch refill.
- Pull request status: PR #96 remains the active draft performance PR.
- Merge status: not ready as a production-complete scheduler; can be accepted only as a measured live-scheduler milestone before the larger admission redesign.
- Blockers: none.

## Known Issues or Risks

- The live scheduler candidate improves the observed stall mode, but the latest validation is still a short sample.
- The disabled decoupled dense path should not remain indefinitely without either deletion or a redesigned prefix-ownership implementation.
- Peer mix and network routing materially affect measurements; benchmark notes must record routing mode, serving peers, and bandwidth.
