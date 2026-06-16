# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-queue-v2`. Draft PR: https://github.com/tdenisenko/logex/pull/93

The Mac mini benchmark is running on `/Volumes/SSD 4TB/LogEx` through the VPS full tunnel on HTTP port `18683`, with the dashboard exposed at `http://157.245.195.72:18683/`. The data directory was reset once for dense-range benchmarking and must not be reset again until this sync reaches genesis. Historical sync is still dominated by EL body/receipt peer churn and request tail latency rather than CPU, RAM, or disk IO. The latest retained pipeline overlaps validation/extraction for later completed fetch windows while preserving ordered writes and allows spare hedge attempts for dense prefix gaps when enough serving peers are available. The most recent remote stop was caused by the macOS soft file-descriptor limit, not data corruption; the client was restarted with a raised per-process limit.

## Completed Since Last Run

- Added out-of-order historical prepare overlap: completed later fetch windows can be validated/extracted before the current sequence finishes, while storage commits remain strictly ordered.
- Benchmarked the prepare-overlap change on the Mac mini run and kept it after it improved plan time and sustained logs/sec versus the retained baseline.
- Added dense-prefix hedge spare capacity so early blocking body/receipt chunks can be duplicated without waiting for later prefix chunks to finish.
- Rejected deeper fetch depth, residual-gap pipelining, smaller dense windows, and smaller initial body/receipt request limits after live benchmarks showed worse tail latency, stale fetch resets, or no material throughput gain.
- Diagnosed the remote stop as `Too many open files (os error 24)`, restarted the client without resetting data, and confirmed storage integrity plus resumed forward and historical sync.
- Fixed the remaining clippy failure by boxing the rare sourced receipt-count mismatch error path instead of suppressing `result_large_err`.

## Remaining TODOs

1. Replace batch-level body/receipt waiting with a geth/nethermind-style task scheduler.
   - Reason: The current historical pipeline still loses throughput when one slow peer blocks the contiguous prefix of a batch. System CPU, RAM, and disk are not saturated, so the bottleneck is peer tail latency and underused serving peers.
   - Completion criteria: Historical sync assigns body and receipt subtasks from an idle-peer queue, scores peers by recent response latency/failure rate, hedges or abandons stragglers without waiting for the whole batch, keeps validation and ordered commits intact, and shows a sustained dense-range throughput improvement over the retained baseline without higher residual gaps or peer churn.

2. Add a repeatable historical-sync bottleneck report.
   - Reason: Matching geth/nethermind performance requires measuring the real limiter after each architectural change instead of relying on dashboard averages alone.
   - Completion criteria: A benchmark report captures body/receipt request latency distributions, timeout/hedge counts, serving-peer counts, validation/extraction/write time, CPU, memory, disk, network, open file descriptors, logs/sec, blocks/sec, and wall-clock progress against the retained baseline.

3. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a live release-candidate run from a clean data directory.

## Design Decisions

- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.
- Checkpoints are accepted only when recent relative to a checkpoint-sync endpoint. Persisted state that is too stale requires a fresh recent checkpoint.
- Historical data may be downloaded ahead of the current write point, but rows are committed only after cryptographic validation and in chain order.
- Later completed historical fetch windows may be prepared out of order, but writes still occur only in sequence order.
- Dense historical ranges stay capped at 512 fetched blocks for now. Larger dense windows regressed peer-tail behavior on the live benchmark.
- Dense historical ranges accept verified prefixes down to half a chunk. This avoids discarding cryptographically verified progress when a dense request returns 64-127 contiguous blocks, while sparse windows still require larger prefixes.
- Body/receipt request timeout remains 6 seconds with a 3-second hedge delay. Shorter timeout/hedge experiments increased churn or failed to improve sustained throughput.
- Very large sorted candidate lists are trimmed to the fastest measured body/receipt peers to avoid repeatedly assigning chunks to slow tail peers.
- Dense prefix hedge attempts may temporarily exceed the base in-flight request count when at least 16 body/receipt-capable peers are available. This trades a small amount of redundant network traffic for lower contiguous-prefix tail latency.
- Future historical-sync optimization should be scheduler-driven rather than constant-driven: keep peers busy with independent body/receipt work, but only commit verified chain data in order.
- Mac mini benchmark runs should raise the process file-descriptor limit before startup until the launcher is made permanent; the default soft limit of 256 is too low for the current segment count and peer count.

## Challenges and Resolutions

- Challenge: A restart exposed a native-storage hot segment where some column files had advanced beyond the catalog while WAL replay was still pending.
  - Resolution: Startup now rebuilds partially applied hot segments before replaying and truncating the WAL.

- Challenge: Residual body/receipt gaps could strand otherwise valid historical progress.
  - Resolution: Residual completion now accepts a smaller verified prefix and continues from the remaining gap boundary.

- Challenge: Plausible throughput tweaks improved short dashboard bursts but hurt wall-clock progress.
  - Resolution: Dense 1,024-block windows and 2-second hedging were reverted after log parsing showed worse plan time, failures, or residual churn.

- Challenge: In-order fetch completion left validation/extraction idle while a slow earlier prefix was still pending.
  - Resolution: Completed later fetch windows are now prepared opportunistically and held until their ordered commit turn.

- Challenge: Filling every in-flight slot with distinct dense prefix chunks left no room to hedge the earliest unresolved gap.
  - Resolution: Dense body/receipt plans now reserve spare hedge attempts once the serving peer pool is large enough.

- Challenge: Smaller dense windows and smaller initial request limits improved some request-latency counters but reduced sustained logs/sec.
  - Resolution: Both experiments were reverted; the retained configuration keeps 512-block dense windows and 48-block initial body/receipt request limits.

- Challenge: Cleaning remote build artifacts exposed a missing `protoc` dependency.
  - Resolution: Installed `protobuf` on the Mac mini and used an explicit `PROTOC=/usr/local/bin/protoc` for the clean release build.

- Challenge: The source sync deleted old benchmark logs stored under the remote source `run/` directory.
  - Resolution: Current experiment logs were parsed immediately; future deploy syncs should exclude `run/` or write retained benchmark logs outside the source tree.

- Challenge: The live remote client stopped after hitting the macOS soft file-descriptor limit.
  - Resolution: Confirmed storage integrity on restart and relaunched the client with `ulimit -n 65536`; a permanent launcher-level limit should be part of deployment hardening.

- Challenge: Clippy rejected a large sourced receipt-count mismatch error tuple.
  - Resolution: Boxed only that rare error path so the normal success path and protocol-breach handling remain unchanged.

## Dead Code and Obsolescence Cleanup

- Rechecked the current diff and retained only the request-scheduler hedge-capacity change plus the clippy-required boxed error path from this pass.
- Rejected deeper pipeline depth, residual-gap pipelining, 256-block dense windows, and 24-block initial body/receipt request limits after benchmarking.

## Git Workflow

- Current branch: `perf/historical-sync-queue-v2`
- New branch created this run: no
- Commits made during this run: prepare-overlap checkpoint; prefix-hedge checkpoint; clippy/roadmap checkpoint
- Pull request status: draft PR #93 open
- Merge status: not merged
- Blockers: PR #93 can be merged after the clippy/roadmap checkpoint is pushed and GitHub checks are green or otherwise confirmed mergeable.

## Known Issues or Risks

- Historical sync remains body/receipt peer-churn and fetch-tail bound; a larger scheduler rewrite may be required for another step-change improvement.
- Remote benchmark samples after restarts are not comparable until the peer pool warms up.
- Repeated restarts depress serving-peer counts, so further experiments should be larger and better justified than simple constant changes.
- Verification-critical security review is still required before a production-ready release.
