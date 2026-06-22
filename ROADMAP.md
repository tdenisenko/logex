# Roadmap

## Current Status

LogEx verifies consensus-layer checkpoints, uses CL-authenticated execution headers as the EL pivot, follows new head blocks, reverse-syncs EL history to genesis, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput-v3`

Draft PR: `https://github.com/tdenisenko/logex/pull/95`

The Mac mini production-like run is active on `/Volumes/SSD 4TB/LogEx` with HTTP port `18683`. A full historical sync reached genesis and was preserved at `/Volumes/SSD 4TB/LogEx-full-sync-20260621-231449`; a fresh sync run is now testing dense recent-block throughput with the same peer metadata restored.

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
- Fixed historical lookahead refill cursor selection while prepared batches are queued.
  - Hot-path refills now continue from the fetch pipeline cursor instead of comparing against the older storage floor and resetting useful lookahead.
  - The critical refill limit is now two fetches so ingestion can keep downloads ahead without deep synchronous planning.
- Deployed the scheduler candidate on the Mac mini without resetting `/Volumes/SSD 4TB/LogEx`.
  - Live log check: `0` stale historical fetch discards and `0` historical reset logs after deployment.
- Added parallel reverse-header page fetches for multi-page historical windows.
  - Header pages are requested concurrently by block number and then validated as one chained prefix against the known child header before any body/receipt data is trusted.
  - Live log check: `0` stale fetch discards, `0` historical reset logs, and `0` parallel header validation failures after deployment.
- Fixed restart warm-up and residual tail handling in historical sync.
  - The historical density estimate now starts empty instead of assuming dense blocks, so sparse-range resumes do not begin with bad window sizing.
  - Residual body/receipt validation failures are retried with the bad peer excluded before resetting lookahead.
  - Critical refill now keeps active network fetches above the minimum before treating prepared work as enough buffer.
- Completed and preserved a full historical sync.
  - The completed data directory was moved to `/Volumes/SSD 4TB/LogEx-full-sync-20260621-231449`.
  - A fresh `/Volumes/SSD 4TB/LogEx` was created, peer metadata was restored, and the client was restarted on port `18683`.
- Tightened dense body/receipt prefix acceptance.
  - Dense decoupled body/receipt plans now require a full planned prefix once enough peers are available instead of accepting a half-window and forcing residual repair.
  - If the decoupled plan cannot produce a valid prefix, the paired fallback still runs instead of returning an empty result.
  - Live dense-range samples recovered into the `270k`-`546k` logs/sec range with no zero-throughput period in the latest short window.

## Remaining TODOs

1. Reduce peer-tail sawtooth in dense historical sync.
   - Reason: The full-sync target is below 4 hours, and current throughput still dips when body/receipt peers time out or under-serve.
   - Completion criteria: Sustained benchmark windows show materially lower low-throughput minutes without increasing timeout churn, memory pressure, or invalid partial batches. Track logs/sec, blocks/sec, body/receipt p95 latency, decoupled fallback frequency, active fetch depth, timeout-penalized peers, serving-peer count, CPU, memory, disk, and network.

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
- Critical-path refills should be bounded but continuous. Deep historical priming is allowed when the engine has no ready work, while ingest now tops up a small amount of fetch work from the pipeline cursor.
- Multi-page reverse-header windows can be fetched concurrently by number, provided the concatenated result is validated against the known child header before use.
- Timeout handling should treat repeated body/receipt timeouts as capacity feedback. Peers are paused and down-ranked before being reused, rather than immediately dropped for every timeout.
- If historical sync reaches genesis during optimization work, stop the client cleanly, move `/Volumes/SSD 4TB/LogEx` to a timestamped backup directory on the same storage, then recreate a fresh `/Volumes/SSD 4TB/LogEx` for continued performance experiments.
- Dense decoupled body/receipt fetches should prefer full planned prefixes when at least four peers can serve both roles. Accepting half-prefixes looked productive in isolation but created serialized residual repairs and worse end-to-end throughput.

## Challenges and Resolutions

- Challenge: Historical logs/sec could hit zero even though peers were connected.
  - Resolution: Added queue diagnostics in previous runs, removed CL/live-head gating for resumed historical backfill, drained ready historical work before forward work, and now fixed false partial-prefix resets plus hot-path deep refill stalls.
  - Remaining: Body/receipt request tails still cause throughput valleys.

- Challenge: Normal chunk-boundary prefixes were interpreted as incomplete full-header requests.
  - Resolution: The request plan now reports the planned prefix boundary, and the engine uses that boundary for partial-prefix decisions.

- Challenge: Valid partial prefixes could leave a gap before queued lookahead.
  - Resolution: Added residual header batches and residual ingestion before queued lookahead can advance the floor.

- Challenge: Queue refill could run expensive header planning on the ingest path.
  - Resolution: Added a limited refill path for critical sections, then fixed it to follow the fetch pipeline cursor while prepared batches are queued.

- Challenge: Sequential 1024-header pages still made refill planning expensive.
  - Resolution: Added concurrent reverse-header page requests for multi-page windows, with full chain validation before the header batch is accepted.

- Challenge: Slow body/receipt peers could be reused too quickly after repeated timeouts.
  - Resolution: Timeout pauses now scale with repeated timeouts up to a capped duration.

- Challenge: Dense recent ranges still show body/receipt tail latency.
  - Resolution: Active fetch starvation was fixed, and dense decoupled plans now reject partial prefixes when enough peers exist.
  - Remaining: Paired fallback can still return short prefixes during bad peer/tail episodes; benchmark better fallback retry and peer-selection behavior.

## Dead Code and Obsolescence Cleanup

- Inspected the historical scheduler changes in `crates/logex-sync/src/engine/anchored.rs`; no rejected deep-refill or storage-floor-reset variant remains.
- Inspected the body/receipt request accounting path in `crates/logex-sync/src/p2p/peer_manager/requests.rs`; the obsolete `return_blocks()` accessor was replaced with planned-prefix accounting.
- Inspected the new reverse-header page request path; the old sequential path remains as fallback for low-peer, single-page, or failed parallel cases.
- Inspected current historical scheduler changes after the full-sync backup; no obsolete experiment files or dead branches were added locally.
- Inspected timeout handling in `crates/logex-sync/src/p2p/peer_manager/state.rs`; the retained change is limited to adaptive pause duration and focused unit coverage.
- Inspected dense body/receipt request handling in `crates/logex-sync/src/p2p/peer_manager/requests.rs`; the retained change removes the obsolete early half-prefix acceptance for adequately peered dense plans.
- The completed remote data directory was moved to `/Volumes/SSD 4TB/LogEx-full-sync-20260621-231449`; `/Volumes/SSD 4TB/LogEx` now contains the fresh active run.

## Git Workflow

- Current branch: `perf/historical-sync-throughput-v3`
- New branch created this run: no
- Commits made during this run: `4f6c6fe perf: reduce historical sync idle gaps`, `f9d115e perf: keep historical fetch refills ahead`, `544d79d perf: parallelize historical header pages`; pending commit for dense body/receipt full-prefix acceptance
- Pull request status: draft PR open at `https://github.com/tdenisenko/logex/pull/95`
- Merge status: not merged
- Blockers: performance target is not met yet; live benchmarking still needs peer-tail improvements.

## Known Issues or Risks

- Historical sync can still show low-throughput windows when serving peers are few or body/receipt requests time out in clusters.
- Dense recent blocks still expose body/receipt peer-tail latency and residual-gap serialization; this is the current bottleneck.
- The current fresh remote run should not be reset again unless a code change requires it or full historical sync reaches genesis and is backed up first.
- The full request-scheduler actor split may be necessary if cooperative scheduling cannot keep downloads saturated.
