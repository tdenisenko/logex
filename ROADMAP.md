# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput`. The branch is focused on EL historical sync throughput, checkpoint freshness safety, and peer-retention stability.

Current benchmark state: the Mac mini remote is running on `/Volumes/SSD 4TB/LogEx` through the VPS full tunnel on HTTP port `18683`. Historical sync remains fetch-bound by body/receipt peer response tails, not CPU, RAM, or disk IO. The retained pipeline uses 6-second body/receipt request timeouts, early peer accounting for asynchronous fetch outcomes, macOS memory-aware sizing, density-aware fetch windows, a density-gated deeper fetch pipeline for dense-but-not-extreme ranges, and split active/completed fetch budgeting when memory is healthy.

Draft PR: https://github.com/tdenisenko/logex/pull/92

## Completed Since Last Run

- Created the draft PR for the current historical sync throughput work.
- Audited the stale `fix/historical-fetch-stalls` PR and retained only the storage/cache/logging changes that still apply cleanly to the newer downloader.
- Avoided rebuilding the full partition view after historical segment writes by appending the newly written segment metadata directly.
- Preallocated receipt bloom caches and reduced CL block-response log noise from info to debug.
- Added strict recent-checkpoint validation for CLI checkpoint values, checkpoint URLs, and persisted restart state.
- Updated the dashboard CL label so it no longer claims head tracking unless the CL anchor is actually caught up.
- Split historical sync into fetch, prepare, and ordered write stages so body/receipt downloads can continue while validation/extraction and writes run.
- Added residual-gap handling for accepted partial body/receipt prefixes; verified residual gaps are fetched, validated, and written without discarding the full lookahead queue.
- Removed the dense-range lookahead reset that discarded healthy in-flight fetches after transient peer-count drops.
- Benchmarked and rejected the depth-8 downloader experiment because it reintroduced below-prefix resets without a sustained throughput gain.
- Added macOS memory probes for historical fetch/write sizing so the Mac mini test host uses real available-memory data instead of falling back to conservative unknown-memory behavior.
- Reduced pipelined body/receipt chunk request timeout from the global request timeout to 6 seconds after live testing showed better progress at low serving-peer counts.
- Benchmarked and rejected larger dial fanout, one-peer chunk attempts, larger completed-fetch buffering, and shorter body/receipt plan timeout because they increased residuals or stalls without sustained throughput improvement.
- Capped medium-density fetch windows by target log count instead of allowing 5,000-block batches through dense ranges that repeatedly hit peer tail latency.
- Lowered the dense-range threshold and enabled an 8-deep fetch pipeline only for dense-but-not-extreme ranges when memory and serving-peer counts are healthy.
- Reworked residual repair to consume verified partial prefixes in a loop instead of falling back to large sequential body/receipt requests after a partial response.
- Benchmarked and rejected speculative prefix hedging because it reduced some fetch timings but increased residual churn and did not improve sustained progress.
- Added WAL replay recovery for the crash window where hot column files advanced but the manifest, canonical bitmap, or WAL truncation did not complete before shutdown.
- Split active fetch depth from completed fetch buffering under healthy memory so completed batches do not prematurely throttle new body/receipt downloads.
- Benchmarked and rejected one paired body/receipt chunk per peer; it slightly reduced fetch p50 but lowered active fetch utilization and did not improve sustained logs/sec.
- Applied body/receipt peer success/failure accounting as soon as asynchronous historical fetch outcomes complete, so timed-out peers are paused, demoted, or quarantined before later queued results are consumed.
- Benchmarked and rejected a deeper prepare lookahead and a 4-second body/receipt request timeout with a 2-second hedge delay; neither produced a sustained improvement over the accounting-only build.
- Added a scoped clippy allowance around generated tonic protobuf code after GitHub nightly started flagging `tonic::Status` in generated service traits as `result_large_err`.

## Remaining TODOs

1. Improve body/receipt peer-tail handling.
   - Reason: Historical sync is still limited by slow or timeout-prone peers delaying contiguous prefixes.
   - Completion criteria: A benchmark shows a sustained, meaningful improvement over the retained density-aware pipeline without increasing validation risk, memory risk, or peer churn.

2. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a live release-candidate run from a clean data directory.

## Design Decisions

- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.
- Checkpoints are accepted only when they are recent relative to a checkpoint-sync endpoint. Persisted state that is too stale now requires a fresh recent checkpoint.
- Historical body/receipt sync may download below a residual gap before that gap is written, but data is still committed only after cryptographic validation and in chain order.
- Dense historical ranges use smaller 512-block fetch windows with bounded lookahead, because this reduced queue loss in partial-prefix cases while keeping memory use controlled.
- Global depth-8 fetching was rejected, but density-gated depth-8 fetching is retained for dense-but-not-extreme ranges after live testing showed zero residual batches in the sampled window and better body/receipt p50 than the prior retained run.
- Active downloads and completed fetch buffering are budgeted separately while available memory is healthy; low-memory mode still uses the conservative combined pending cap.
- Body/receipt chunk attempts still keep intra-chunk fallback peers, because one-peer chunk attempts returned failures faster but caused residual gaps and near-stalls.
- The paired body/receipt plan window still allows roughly two paired chunks per peer; a one-paired-chunk-per-peer policy was closer to geth's busy-peer model but did not improve sustained remote throughput in this workload.
- The body/receipt plan timeout stays at 45 seconds, because an 18-second cap increased partial/residual work in dense ranges.
- Body/receipt request timeout remains 6 seconds with a 3-second hedge delay. A 4-second timeout lowered some short samples but increased timeout density, peer churn, and p50/p90 fetch latency over a larger sample.
- Peer accounting is applied when fetch outcomes are received, not only when they are ingested in order, because queued fetch plans otherwise reused peers that had already timed out in completed asynchronous work.
- Stale PR #91 was audited instead of merged because its large downloader changes would discard the newer residual-gap and overlap architecture; only low-risk pieces with direct tests were retained.

## Challenges and Resolutions

- Challenge: Partial body/receipt prefixes caused long queue resets even when hundreds of contiguous blocks were valid.
  - Resolution: Accepted one full chunk as recoverable progress, repaired the residual gap, and continued lookahead below the residual boundary.

- Challenge: Healthy dense lookahead was reset when transient peer-count changes lowered the computed buffer depth.
  - Resolution: Removed that reset path; only memory pressure can now force a healthy lookahead reset.

- Challenge: Increasing active fetch depth to 8 looked promising but increased timeout churn and below-prefix failures.
  - Resolution: Reverted the broad depth-8 change, then retained a narrower density-gated depth-8 path for dense ranges where the 512-block cap keeps memory bounded.

- Challenge: An older open performance PR contained a mix of obsolete downloader changes and useful small optimizations.
  - Resolution: Kept the storage metadata append optimization, receipt bloom cache preallocation, and CL log-level downgrade; rejected the stale downloader diff.

- Challenge: Several plausible peer-tail mitigations improved one metric while hurting ordered progress.
  - Resolution: Reverted larger dial fanout, depth-8 fetches, one-peer chunk attempts, larger fetch buffers, and an 18-second plan timeout after live benchmarks showed worse residuals or stalls.

- Challenge: Medium-density ranges still produced large partial body/receipt tails.
  - Resolution: Capped medium-density batches by target row count, treated 300+ rows/block as dense, and changed residual repair to keep verified partial progress instead of restarting with sequential fetches.

- Challenge: A graceful-restart test exposed a storage recovery gap where WAL replay had already advanced hot columns but canonical bitmap repair did not complete before startup integrity verification.
  - Resolution: WAL replay now verifies already-applied hot rows against the WAL before repairing metadata, and startup repairs recoverable hot canonical bitmap length mismatches before integrity verification.

- Challenge: Completed fetch buffers could fill and throttle new downloads even when CPU, disk, and memory were healthy.
  - Resolution: Split the healthy-memory budget so active downloads can stay full while completed fetches wait to be ingested; low-memory mode keeps the previous combined cap.

- Challenge: Reducing each body/receipt plan to one paired chunk per peer reduced some fetch-tail latency but also lowered active fetch utilization.
  - Resolution: Reverted the experiment after remote samples failed to show a sustained logs/sec improvement.

- Challenge: Slow peers were scored only when queued fetch outcomes were consumed in sequence, allowing timed-out peers to appear in multiple future plans.
  - Resolution: Drained success/failure accounting when fetch outcomes arrive, while preserving ordered validation and writes.

- Challenge: Shorter body/receipt timeouts looked promising in isolated samples but made the run more bursty.
  - Resolution: Reverted the 4-second timeout and 2-second hedge delay after the larger sample regressed to 8.3s p50 and 18.3s p90 body/receipt latency.

- Challenge: GitHub CI clippy failed on generated tonic code, not handwritten application code.
  - Resolution: Added a module-scoped generated-code allowance for `clippy::result_large_err` at the protobuf include boundary.

## Dead Code and Obsolescence Cleanup

- Reverted rejected broad depth-8, prefix-hedge, dial-fanout, one-peer chunk, one-paired-chunk-per-peer, larger-buffer, shorter-plan-timeout, deeper-prepare, and shorter-request-timeout experiments before leaving the remote running.
- Rechecked the historical fetch/prepare/residual code paths and retained only changes that improved correctness or benchmark stability.
- Compared the stale performance PR against the current branch and did not carry over obsolete downloader code.
- Removed stray remote-root source copies created by a mistaken rsync destination during deployment.

## Git Workflow

- Current branch: `perf/historical-sync-throughput`
- New branch created this run: no
- Commits made during this run: `perf: improve historical downloader overlap`; `perf: salvage storage write optimizations`; `docs: record stale performance pr cleanup`; `perf: tune historical fetch tail handling`; `perf: reduce historical residual churn`; `fix: recover hot segment wal replay`; `perf: keep historical fetches active`; `perf: apply historical peer accounting early`; `fix: allow generated tonic clippy lint`
- Pull request status: draft PR #92 open
- Merge status: not merged
- Stale PR cleanup: PR #91 was closed and remote branch `fix/historical-fetch-stalls` was deleted after useful changes were salvaged.
- Blockers: remaining EL historical sync performance work is still in progress.

## Known Issues or Risks

- Historical sync is still body/receipt fetch-tail bound; peer timeout clusters can hold back contiguous progress.
- The remote benchmark after restarts needs warm peer pools before logs/sec samples are comparable.
- Verification-critical security review is still required before a production-ready release.
