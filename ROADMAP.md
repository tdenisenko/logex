# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed CL pivot, tracks the live execution head, reverse-syncs EL history toward genesis, and serves verified logs through the dashboard and query APIs. PR #95 is merged as the current historical sync baseline; this branch starts the larger geth/Nethermind-style live request scheduler milestone.

The Mac mini client is currently stopped while routing is switched away from full-VPS egress. The active data directory is `/Volumes/SSD 4TB/LogEx`, and the latest full-sync backup is preserved at `/Volumes/SSD 4TB/LogEx-full-sync-20260622-154101`. Before restarting performance tests, run the local dashboard-only routing script so only the dashboard is exposed through the VPS and historical download traffic uses the local connection.

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
- Added ordered historical write overlap and bounded prepared-batch coalescing:
  - While a prepared batch is being written, the engine now continues draining fetch outcomes and spawning/refilling ready prepare work.
  - Consecutive already-prepared batches with no residual gap are coalesced into one ordered storage write, capped at four batches or roughly 500k rows.
  - Live Mac mini benchmarks improved completed block throughput in the older, lower-log-density range from roughly 390-480 blocks/sec baseline windows to roughly 660 blocks/sec in longer coalesced windows.
- Tested and rejected raising the coalescing cap from four to six batches; it increased write/process latency and max gaps without enough throughput gain.
- Fixed the write-overlap loop so it does not await new fetch planning while an ordered storage write is already ready to complete.
- Added memory-bounded post-ingest fetch top-up so the historical downloader refills toward the computed pipeline depth after ordered progress instead of staying capped at two active fetches.
  - The mature Mac mini top-up run reached six active fetches and 17 queued fetches, eliminated 20s+ gaps in parsed windows, and held roughly 530-575 completed blocks/sec in the current low-log-density range while the dashboard EWMA climbed above 850 blocks/sec.
- Tested and rejected sparse reverse-header lookahead caching; it reduced some header fetches but increased refill latency and long gaps in live benchmark windows.
- Created gitignored local routing scripts in `local-ops/` to switch between full-VPS mode and dashboard-only VPS mode from the project checkout. The Mac mini and VPS are still in full-VPS mode until the dashboard-only script is run with local admin privileges.
- Stopped the Mac mini LogEx process cleanly so it does not continue using VPS egress while the routing mode is pending.

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
- Historical storage writes can safely overlap with continued fetch/prepare orchestration because the ordered storage write owns an `Arc` clone and the floor advances only after the write succeeds.
- Consecutive prepared historical batches may be coalesced only when they are already completed, sequence-contiguous, and have no residual header gap; this preserves ordered verification and avoids delaying residual repair.
- Ordered-write overlap loops should only drain/promote already available fetch outcomes; top-up to the full memory-aware fetch depth belongs after ordered progress, where the pipeline can safely spend time planning more work without delaying a completed write.
- Wider reverse-header lookahead is not a substitute for a live scheduler; in sparse ranges it can create stale or unused lookahead and worsen tail latency.
- Performance experiments should run in dashboard-only VPS mode unless public P2P exposure through the VPS is explicitly needed; this keeps dashboard access public while avoiding paid VPS egress for historical downloads.

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
- Challenge: the engine paused orchestration while awaiting each ordered storage write.
  - Resolution: changed the write await into a select loop that continues draining fetch outcomes and refilling prepare work, then coalesces already-ready contiguous prepared batches into one bounded ordered write.
  - Remaining: active fetches can still dip when local commits are large; a true live request queue is still needed for peer-tail latency.
- Challenge: larger coalesced writes looked like a possible storage optimization.
  - Resolution: tested a six-batch coalescing cap and reverted it because it increased write/process latency and max gaps relative to the four-batch cap.
  - Remaining: future coalescing changes should be adaptive and benchmarked against full-run time, not just peak dashboard rates.
- Challenge: the first write-overlap implementation could still block on refill planning before observing that the storage write had completed.
  - Resolution: changed the write-await select loop to promote already-fetched work without refilling during the write, then perform a bounded full-depth top-up after ordered progress.
  - Remaining: storage/extraction and ordered commit latency still pace low-density ranges; a deeper live request scheduler is still needed for the 4-hour target.
- Challenge: sparse reverse-header lookahead looked like a low-risk way to reduce serial header fetches.
  - Resolution: benchmarked and reverted it because mature windows showed higher refill latency and more long gaps than the accepted baseline.
  - Remaining: header planning needs to be part of a real live scheduler, not a wider cache on the current ordered pipeline.
- Challenge: remote restarts can look stuck after HTTP/gRPC stop while the P2P network drains sessions.
  - Resolution: restored the client with a longer restart grace window and confirmed the process exits cleanly.
  - Remaining: in-flight historical work should become more promptly cancelable during shutdown.
- Challenge: the Mac mini was still full-tunneled through the VPS, which would make continued benchmarking expensive.
  - Resolution: added project-local, gitignored scripts to switch between full-VPS and dashboard-only routing, verified the Mac and VPS are still full-tunneled, and stopped LogEx pending the local privileged switch.
  - Remaining: run `local-ops/logex-dashboard-only-routing.sh` with sudo, verify routes/NAT, then restart LogEx without VPS P2P NAT advertising.

## Dead Code and Obsolescence Cleanup

- Inspected `crates/logex-sync/src/engine/anchored.rs`; rejected timeout/window/depth experiments were reverted, duplicated density-branch logic was removed for clippy, and the obsolete `historical_backfill_has_no_queued_work` helper was removed after post-ingest top-up replaced it. The newest speculative prepare-order change remains uncommitted until it is benchmarked in dashboard-only routing mode.
- Inspected `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected decoupled redundancy, half-prefix, timeout, and larger-prefix experiments were reverted.
- Inspected peer request scoring in `crates/logex-sync/src/p2p/peer_manager/state.rs`; existing EWMA speed, active-load adjustment, serving bonus, and timeout penalty remain in use.
- Rejected refill-width, fanout-threshold, decoupled least-loaded peer assignment, dense-return-threshold, medium-depth, prepare-buffer-depth, and six-batch coalescing experiments were reverted locally and remotely.
- Rejected sparse reverse-header lookahead caching after live benchmarks showed worse refill latency and more long gaps.
- Kept code was inspected for obsolete experiment leftovers; only ordered write overlap and bounded four-batch coalescing remain in the local diff.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`
- New branch created this run: yes
- Commits made during this run: `578b643 docs: record scheduler benchmark findings`; `cd4f074 perf: overlap historical writes with fetch work`; `6e907cc test: cover historical write coalescing`; `69373e8 perf: refill historical pipeline after writes`; `964c219 docs: record rejected scheduler lookahead`
- Pull request status: draft PR #96 open for scheduler work
- Merge status: PR #95 merged into `master`; scheduler branch not merged
- Validation: `cargo fmt --check`; `git diff --check`; `cargo clippy -p logex-sync --all-targets -- -D warnings`; `cargo test -p logex-sync historical_critical_refill --quiet`; `cargo test -p logex-sync historical_fetch_budget_keeps_active_downloads_full_when_memory_is_healthy --quiet`; `cargo test -p logex-sync request_window_limit --quiet`; `cargo test -p logex-sync decoupled_prefix --quiet`; `cargo test -p logex-sync decoupled --quiet`; `cargo test -p logex-sync body_receipt_return_blocks --quiet`; `cargo test -p logex-sync body_receipt_min_accepted_prefix --quiet`; `cargo test -p logex-sync historical_fetch_buffer --quiet`; `cargo test -p logex-sync historical_fetch_refill --quiet`; `cargo test -p logex-sync --quiet`. Rejected experiments were benchmarked live and reverted.
- Blockers: dashboard-only routing requires local admin privileges; LogEx remains stopped until the user runs the gitignored routing script and the routes/NAT are verified.

## Known Issues or Risks

- Current historical sync remains peer-tail bound and can still show low-throughput windows.
- The current Mac mini data directory is fresh for the next run; the latest full-sync backup is `/Volumes/SSD 4TB/LogEx-full-sync-20260622-154101`.
- A larger request-scheduler refactor may be required to approach the 4-hour full-sync target.
- Shutdown is clean but can take longer than short restart scripts expect while the P2P network drains sessions.
