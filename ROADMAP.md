# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput`. The branch is focused on EL historical sync throughput, checkpoint freshness safety, and peer-retention stability.

Current benchmark state: the Mac mini remote is running on `/Volumes/SSD 4TB/LogEx` through the VPS full tunnel on HTTP port `18683`. Historical sync remains fetch-bound by body/receipt peer response tails, not CPU, RAM, or disk IO. The retained pipeline uses shorter per-request body/receipt timeouts and macOS memory-aware sizing; broader fetch-depth, buffer-depth, and plan-timeout experiments were rejected after live tests.

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

## Remaining TODOs

1. Improve body/receipt peer-tail handling.
   - Reason: Historical sync is still limited by slow or timeout-prone peers delaying contiguous prefixes.
   - Completion criteria: A benchmark shows a sustained, meaningful improvement over the retained depth-6/no-reset pipeline without increasing validation risk, memory risk, or peer churn.

2. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a live release-candidate run from a clean data directory.

## Design Decisions

- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.
- Checkpoints are accepted only when they are recent relative to a checkpoint-sync endpoint. Persisted state that is too stale now requires a fresh recent checkpoint.
- Historical body/receipt sync may download below a residual gap before that gap is written, but data is still committed only after cryptographic validation and in chain order.
- Dense historical ranges use smaller 512-block fetch windows with more bounded lookahead, because this reduced queue loss in partial-prefix cases while keeping memory use controlled.
- Depth-6 is currently the retained high-memory downloader depth for this machine. Depth-8 was tested and rejected.
- Body/receipt chunk attempts still keep intra-chunk fallback peers, because one-peer chunk attempts returned failures faster but caused residual gaps and near-stalls.
- The body/receipt plan timeout stays at 45 seconds, because an 18-second cap increased partial/residual work in dense ranges.
- Stale PR #91 was audited instead of merged because its large downloader changes would discard the newer residual-gap and overlap architecture; only low-risk pieces with direct tests were retained.

## Challenges and Resolutions

- Challenge: Partial body/receipt prefixes caused long queue resets even when hundreds of contiguous blocks were valid.
  - Resolution: Accepted one full chunk as recoverable progress, repaired the residual gap, and continued lookahead below the residual boundary.

- Challenge: Healthy dense lookahead was reset when transient peer-count changes lowered the computed buffer depth.
  - Resolution: Removed that reset path; only memory pressure can now force a healthy lookahead reset.

- Challenge: Increasing active fetch depth to 8 looked promising but increased timeout churn and below-prefix failures.
  - Resolution: Reverted to depth-6 after live comparison.

- Challenge: An older open performance PR contained a mix of obsolete downloader changes and useful small optimizations.
  - Resolution: Kept the storage metadata append optimization, receipt bloom cache preallocation, and CL log-level downgrade; rejected the stale downloader diff.

- Challenge: Several plausible peer-tail mitigations improved one metric while hurting ordered progress.
  - Resolution: Reverted larger dial fanout, depth-8 fetches, one-peer chunk attempts, larger fetch buffers, and an 18-second plan timeout after live benchmarks showed worse residuals or stalls.

## Dead Code and Obsolescence Cleanup

- Reverted rejected depth-8, dial-fanout, one-peer chunk, larger-buffer, and shorter-plan-timeout experiments before leaving the remote running.
- Rechecked the historical fetch/prepare/residual code paths and retained only changes that improved correctness or benchmark stability.
- Compared the stale performance PR against the current branch and did not carry over obsolete downloader code.

## Git Workflow

- Current branch: `perf/historical-sync-throughput`
- New branch created this run: no
- Commits made during this run: `perf: improve historical downloader overlap`; `perf: salvage storage write optimizations`; `docs: record stale performance pr cleanup`; `perf: tune historical fetch tail handling`
- Pull request status: draft PR #92 open
- Merge status: not merged
- Stale PR cleanup: PR #91 was closed and remote branch `fix/historical-fetch-stalls` was deleted after useful changes were salvaged.
- Blockers: remaining EL historical sync performance work is still in progress.

## Known Issues or Risks

- Historical sync is still body/receipt fetch-tail bound; peer timeout clusters can hold back contiguous progress.
- The remote benchmark after restarts needs warm peer pools before logs/sec samples are comparable.
- Verification-critical security review is still required before a production-ready release.
