# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed CL pivot, tracks the live execution head, reverse-syncs EL history toward genesis, and serves verified logs through the dashboard and query APIs. PR #95 is merged as the current historical sync baseline; this branch starts the larger geth/Nethermind-style live request scheduler milestone.

The Mac mini run is active on `/Volumes/SSD 4TB/LogEx` with HTTP port `18683`. A previous full-sync data directory is preserved at `/Volumes/SSD 4TB/LogEx-full-sync-20260621-231449`. If the current historical sync reaches genesis during performance work, stop the client cleanly, move `/Volumes/SSD 4TB/LogEx` to a timestamped backup directory on the same storage, recreate `/Volumes/SSD 4TB/LogEx`, restore peer metadata if available, and continue testing from a fresh run.

## Completed Since Last Run

- Merged PR #95 into `master` as the new historical sync baseline after local validation and passing GitHub CI.
- Created `perf/historical-sync-live-scheduler` for the next scheduler-focused performance pass.
- Benchmarked and rejected five narrow scheduler/window tweaks on the Mac mini:
  - Increasing the critical historical fetch refill width from 2 to 4 kept more fetches buffered but did not materially improve completed logs/sec and increased local extraction/write pressure.
  - Lowering the high-fanout body/receipt threshold from 32 peers to 16 peers increased request pressure and timeouts without improving completed throughput.
  - Adding per-plan least-loaded peer selection to the decoupled dense body/receipt path did not materially beat the baseline completed-batch rate once peers recovered.
  - Raising the dense return threshold from 100 to 300 rows/block caused larger header requests but only slightly larger verified prefixes in the current range, so completed logs/sec stayed near baseline.
  - Raising the medium-peer fetch pipeline depth from 4 to 6 increased max active fetches but did not raise average active fetches or completed logs/sec enough to justify the added memory pressure.
- Restored the remote Mac mini client to the clean PR #95 baseline and left it running on `/Volumes/SSD 4TB/LogEx`.

## Remaining TODOs

1. Replace static historical body/receipt plan boundaries with a live request scheduler.
   - Reason: current dense sync is still peer-tail bound. A slow prefix request can stall completed batches even when later work or other peers are available.
   - Completion criteria: implement or prove unnecessary a geth/Nethermind-style queue that reserves chunks for idle peers, reassigns timed-out work without resetting useful lookahead, preserves ordered verified ingestion, and improves sustained full-run throughput without more timeout churn.

2. Improve dense historical sync benchmark stability.
   - Reason: peak logs/sec can be high, but low-throughput windows still make the full-sync ETA too long.
   - Completion criteria: sustained benchmark windows show materially lower max gaps and higher completed logs/sec while tracking active fetch depth, body/receipt latency, failures, serving peers, CPU, memory, disk, and network.

3. Complete EL production hardening.
   - Reason: performance work must not weaken restart safety, checkpoint freshness, forward sync, or verified query correctness.
   - Completion criteria: tests or smokes cover recent-checkpoint enforcement, restart/resume, CL tracking, EL forward sync, EL reverse sync, invalid peer data, reorg handling, low disk behavior, authenticated dashboard access, and a clean full-sync candidate run.

## Design Decisions

- Historical reverse sync remains independent of CL live-head tracking after a valid checkpoint-backed pivot exists.
- Keep changes only when live benchmarks beat the current baseline on sustained throughput and tail behavior, not peak logs/sec alone.
- Dense body/receipt plans should prefer full verified prefixes when sufficiently peered; half-prefix acceptance was rejected because residual repair serialized the pipeline.
- Simple duplicate-request pressure at low peer counts is not beneficial on the current Mac mini run; it increased failures and reduced completed throughput.
- PR #95 is the new comparison baseline for future historical sync experiments.
- The next meaningful performance path is scheduler architecture, not more local threshold tweaks.
- Geth and Nethermind both use peer allocation based on measured speed and allocation/idle state; LogEx should follow that direction with a live body/receipt work queue rather than more static fanout or timeout tuning.
- Medium-density batches are currently limited by low-peer fetch windows and prefix completion behavior more than by the dense-row threshold alone; changing only the threshold is insufficient.

## Challenges and Resolutions

- Challenge: historical sync could idle when connected peers dropped below four.
  - Resolution: lowered the historical backfill connected-peer floor cap to `1`.
  - Remaining: throughput can still dip when the active body/receipt prefix is held by slow peers.
- Challenge: request-level hedging and partial-prefix experiments looked plausible but worsened real runs.
  - Resolution: reverted all unhelpful experiments locally and remotely after measurement.
  - Remaining: a live work queue with per-peer assignment and reassignment is still needed to attack peer-tail latency cleanly.
- Challenge: `master` needed a same-data comparison before concluding this branch.
  - Resolution: temporarily deployed `master` (`b504fe4`) to the Mac mini, measured completed-batch throughput, then restored the branch source and binary.
  - Remaining: none for this PR.
- Challenge: small scheduler threshold changes were tempting but did not improve sustained throughput.
  - Resolution: tested each change on the live Mac mini data dir with isolated logs, then reverted locally and remotely when completed-batch windows failed to beat baseline materially.
  - Remaining: implement a real live request scheduler that can keep useful lookahead while reassigning slow prefix work.
- Challenge: decoupled dense body/receipt plans looked under-balanced by rotation-only peer assignment.
  - Resolution: tested least-loaded per-plan peer selection; the live run stayed near baseline and was reverted.
  - Remaining: peer assignment needs to be coordinated across active fetch plans, not only inside one plan.
- Challenge: 1024-block completions looked like a possible medium-density bottleneck.
  - Resolution: tested a higher dense-row threshold; it did not produce materially larger completed prefixes or better sustained throughput in the current range and was reverted.
  - Remaining: any larger-window work should be tied to a bounded live scheduler and memory-aware row targets.
- Challenge: medium-peer runs appeared capped at four active fetches.
  - Resolution: tested a depth-6 medium-peer pipeline; max active fetches rose to 6, but average active fetches stayed near 2.2 because prepared buffers filled, and completed logs/sec did not materially improve.
  - Remaining: deeper lookahead alone is not enough; scheduling must coordinate fetch, prepare, and ingest pressure together.

## Dead Code and Obsolescence Cleanup

- Inspected `crates/logex-sync/src/engine/anchored.rs`; rejected timeout/window/depth experiments were reverted, and duplicated density-branch logic was removed for clippy.
- Inspected `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected decoupled redundancy, half-prefix, timeout, and larger-prefix experiments were reverted.
- Inspected peer request scoring in `crates/logex-sync/src/p2p/peer_manager/state.rs`; existing EWMA speed, active-load adjustment, serving bonus, and timeout penalty remain in use.
- Rejected refill-width, fanout-threshold, decoupled least-loaded peer assignment, dense-return-threshold, and medium-depth experiments were reverted locally and remotely; no obsolete experimental code from this pass remains in the local diff.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`
- New branch created this run: yes
- Commits made during this run: pending
- Pull request status: pending draft PR for scheduler work
- Merge status: PR #95 merged into `master`; scheduler branch not merged
- Validation: `cargo fmt --check`; `cargo test -p logex-sync historical_critical_refill --quiet`; `cargo test -p logex-sync historical_fetch_budget_keeps_active_downloads_full_when_memory_is_healthy --quiet`; `cargo test -p logex-sync request_window_limit --quiet`; `cargo test -p logex-sync decoupled_prefix --quiet`; `cargo test -p logex-sync decoupled --quiet`; `cargo test -p logex-sync body_receipt_return_blocks --quiet`; `cargo test -p logex-sync body_receipt_min_accepted_prefix --quiet`; `cargo test -p logex-sync --quiet`. Rejected experiments were benchmarked live and reverted.
- Blockers: none; implementation is pending.

## Known Issues or Risks

- Current historical sync remains peer-tail bound and can still show low-throughput windows.
- The running Mac mini data directory must be backed up and rotated if it reaches genesis during continued optimization.
- A larger request-scheduler refactor may be required to approach the 4-hour full-sync target.
