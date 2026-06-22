# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed CL pivot, tracks the live execution head, reverse-syncs EL history toward genesis, and serves verified logs through the dashboard and query APIs. The current historical sync branch is ready as a mergeable baseline improvement; the next performance milestone is a larger geth/Nethermind-style live request scheduler.

The Mac mini run is active on `/Volumes/SSD 4TB/LogEx` with HTTP port `18683`. A previous full-sync data directory is preserved at `/Volumes/SSD 4TB/LogEx-full-sync-20260621-231449`. If the current historical sync reaches genesis during performance work, stop the client cleanly, move `/Volumes/SSD 4TB/LogEx` to a timestamped backup directory on the same storage, recreate `/Volumes/SSD 4TB/LogEx`, restore peer metadata if available, and continue testing from a fresh run.

## Completed Since Last Run

- Confirmed the branch still beats current `master` on the Mac mini data dir: `master` sampled at about 105k completed logs/sec with a 28s max gap, while the retained branch baseline sampled about 124k-175k completed logs/sec with lower max gaps in comparable windows.
- Rejected and reverted additional small tuning experiments that did not beat the retained baseline: 3s pipelined timeout, larger dense return windows, and lower high-memory pipeline threshold.
- Fixed a clippy `if_same_then_else` warning in historical density sizing without changing behavior.
- Restored the remote Mac mini source and running binary to the retained branch baseline after comparison. The client is running in `tmux` and remains on port `18683`.

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
- The current branch should merge as the new baseline because it improves over `master` and contains no retained failed experiments.
- The next meaningful performance path is scheduler architecture, not more local threshold tweaks.

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

## Dead Code and Obsolescence Cleanup

- Inspected `crates/logex-sync/src/engine/anchored.rs`; rejected timeout/window/depth experiments were reverted, and duplicated density-branch logic was removed for clippy.
- Inspected `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected decoupled redundancy, half-prefix, timeout, and larger-prefix experiments were reverted.
- Inspected peer request scoring in `crates/logex-sync/src/p2p/peer_manager/state.rs`; existing EWMA speed, active-load adjustment, serving bonus, and timeout penalty remain in use.
- No obsolete experimental code from this pass remains in the local diff.

## Git Workflow

- Current branch: `perf/historical-sync-throughput-v3`
- New branch created this run: no
- Commits made during this run: pending
- Pull request status: draft PR open at `https://github.com/tdenisenko/logex/pull/95`; ready to update and merge after this roadmap/clippy commit.
- Merge status: pending
- Validation: `cargo fmt --check`, `cargo test --workspace --quiet`, and `cargo clippy --workspace --all-targets -- -D warnings` pass locally.
- Blockers: none for this PR; the 4-hour target remains follow-up scheduler work.

## Known Issues or Risks

- Current historical sync remains peer-tail bound and can still show low-throughput windows.
- The running Mac mini data directory must be backed up and rotated if it reaches genesis during continued optimization.
- A larger request-scheduler refactor may be required to approach the 4-hour full-sync target.
