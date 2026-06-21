# Roadmap

## Current Status

LogEx verifies consensus-layer checkpoints, uses CL-authenticated execution headers as the EL pivot, follows new head blocks, reverse-syncs EL history to genesis, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput-v3`

Draft PR: `https://github.com/tdenisenko/logex/pull/95`

The Mac mini production-like run is active on `/Volumes/SSD 4TB/LogEx` with HTTP port `18683`. Historical sync is progressing through older, sparse blocks. Recent live samples show the old multi-minute zero-progress condition is addressed, but throughput still has sawtooth behavior caused by body/receipt peer tail latency and low useful serving-peer count during parts of the run.

## Completed Since Last Run

- Fixed a false partial-prefix condition in historical body/receipt plans.
  - The engine now compares fetched blocks against the planned chunk boundary instead of the full header request size.
  - This avoids treating normal accepted prefixes as broken pipeline results.
- Added residual-gap fill before queued lookahead is accepted.
  - If a body/receipt plan returns a verified prefix shorter than the planned boundary, the missing residual range is fetched and verified before the next queued historical lookahead is ingested.
  - This avoids full pipeline resets for valid partial progress.
- Bounded critical-path historical fetch refills.
  - Hot ingest paths now refill only a small amount of fetch work when queues are empty, rather than doing deep sequential planning in the same turn.
  - A live run previously showed a 35s refill stall; the current samples did not reproduce that stall.
- Added adaptive timeout pauses for repeated body/receipt peer timeouts.
  - Timeout pauses now scale with repeated failures up to a cap, reducing rapid reuse of repeatedly slow peers without permanently removing them.
- Verified the remote client remains live on port `18683`.
  - Short live sample: historical floor moved from `4,823,839` to `4,804,572`; throughput recovered from about `11k` logs/sec to about `219k` logs/sec as serving peers rose from `8` to `11`.
- Added and ran focused tests for planned body/receipt prefix accounting and timeout pause scaling.

## Remaining TODOs

1. Reduce peer-tail sawtooth in dense historical sync.
   - Reason: The full-sync target is below 4 hours, and current throughput still dips when body/receipt peers time out or under-serve.
   - Completion criteria: Sustained benchmark windows show materially lower low-throughput minutes without increasing timeout churn, memory pressure, or invalid partial batches. Track logs/sec, blocks/sec, body/receipt p95 latency, active fetch depth, timeout-penalized peers, serving-peer count, CPU, memory, disk, and network.

2. Decide whether the EL request scheduler needs an actor split.
   - Reason: Reverse fetch tasks are asynchronous, but planning, ingestion, and peer accounting still share mutable `SyncEngine`/`PeerManager` ownership.
   - Completion criteria: Either live benchmarks prove cooperative scheduling is sufficient, or a request-scheduler actor is implemented and benchmarked against the current branch.

3. Match production-client peer behavior where it improves measured throughput.
   - Reason: Geth and Nethermind dominate the network, and LogEx needs comparable peer retention and request behavior.
   - Completion criteria: Apply only changes that beat the current baseline in live or controlled benchmarks. Avoid keeping plausible but unproven peer-scoring experiments.

4. Complete release hardening.
   - Reason: Production readiness requires predictable restart behavior and data integrity beyond performance.
   - Completion criteria: Tests or smokes cover bootstrap, recent-checkpoint enforcement, CL tracking, EL forward sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, public listener policy, and a clean full-sync candidate run.

## Design Decisions

- Historical reverse sync remains independent from CL live-head tracking after a recent checkpoint-backed pivot exists. Fresh startup still requires a valid recent checkpoint or a persisted verified floor.
- Benchmark changes by sustained throughput and tail behavior, not peak logs/sec alone. A short 800k+ logs/sec spike is not enough if the same change increases low-throughput minutes.
- Dense historical batches should use fast compacted writes; sparse historical ranges should retain coalescing to avoid tiny segment growth.
- Residual historical gaps should be filled before queued lookahead rather than resetting the whole pipeline when the verified prefix is valid.
- Critical-path refills should be bounded. Deep historical priming is allowed when the engine has no ready work, but ingest should not synchronously spend many seconds planning headers.
- Timeout handling should treat repeated body/receipt timeouts as capacity feedback. Peers are paused and down-ranked before being reused, rather than immediately dropped for every timeout.
- If historical sync reaches genesis during optimization work, stop the client cleanly, copy `/Volumes/SSD 4TB/LogEx` to a backup directory on the same storage, then recreate a fresh `/Volumes/SSD 4TB/LogEx` for continued performance experiments.

## Challenges and Resolutions

- Challenge: Historical logs/sec could hit zero even though peers were connected.
  - Resolution: Added queue diagnostics in previous runs, removed CL/live-head gating for resumed historical backfill, drained ready historical work before forward work, and now fixed false partial-prefix resets plus hot-path deep refill stalls.
  - Remaining: Body/receipt request tails still cause throughput valleys.

- Challenge: Normal chunk-boundary prefixes were interpreted as incomplete full-header requests.
  - Resolution: The request plan now reports the planned prefix boundary, and the engine uses that boundary for partial-prefix decisions.

- Challenge: Valid partial prefixes could leave a gap before queued lookahead.
  - Resolution: Added residual header batches and residual ingestion before queued lookahead can advance the floor.

- Challenge: Queue refill could run expensive header planning on the ingest path.
  - Resolution: Added a limited refill path for critical sections while preserving full priming outside the hot path.

- Challenge: Slow body/receipt peers could be reused too quickly after repeated timeouts.
  - Resolution: Timeout pauses now scale with repeated timeouts up to a capped duration.

## Dead Code and Obsolescence Cleanup

- Inspected the historical scheduler changes in `crates/logex-sync/src/engine/anchored.rs`; no rejected deep-refill variant remains.
- Inspected the body/receipt request accounting path in `crates/logex-sync/src/p2p/peer_manager/requests.rs`; the obsolete `return_blocks()` accessor was replaced with planned-prefix accounting.
- Inspected timeout handling in `crates/logex-sync/src/p2p/peer_manager/state.rs`; the retained change is limited to adaptive pause duration and focused unit coverage.
- No remote data directories were removed during this run. The main data directory remains `/Volumes/SSD 4TB/LogEx`.

## Git Workflow

- Current branch: `perf/historical-sync-throughput-v3`
- New branch created this run: no
- Commits made during this run: `43e67bf perf: reduce historical sync scheduler stalls`; pending commit `perf: reduce historical sync idle gaps`
- Pull request status: draft PR open at `https://github.com/tdenisenko/logex/pull/95`
- Merge status: not merged
- Blockers: performance target is not met yet; live benchmarking still needs peer-tail improvements.

## Known Issues or Risks

- Historical sync can still show low-throughput windows when serving peers are few or body/receipt requests time out in clusters.
- Current samples are from old sparse block ranges, so logs/sec can understate block throughput. Dense-range changes still need longer benchmark windows.
- The remote run should not be reset unless a code change requires it or full historical sync reaches genesis and is backed up first.
- The full request-scheduler actor split may be necessary if cooperative scheduling cannot keep downloads saturated.
