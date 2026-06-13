# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput`. The branch is focused on EL historical sync throughput, checkpoint freshness safety, and peer-retention stability.

Current benchmark state: the Mac mini remote is running on `/Volumes/SSD 4TB/LogEx` through the VPS full tunnel on HTTP port `18683`. Historical sync remains fetch-bound by body/receipt peer response tails, not CPU, RAM, or disk IO. The latest retained pipeline changes remove healthy-lookahead resets and keep lower downloads active while residual gaps are repaired.

## Completed Since Last Run

- Added strict recent-checkpoint validation for CLI checkpoint values, checkpoint URLs, and persisted restart state.
- Updated the dashboard CL label so it no longer claims head tracking unless the CL anchor is actually caught up.
- Split historical sync into fetch, prepare, and ordered write stages so body/receipt downloads can continue while validation/extraction and writes run.
- Added residual-gap handling for accepted partial body/receipt prefixes; verified residual gaps are fetched, validated, and written without discarding the full lookahead queue.
- Removed the dense-range lookahead reset that discarded healthy in-flight fetches after transient peer-count drops.
- Benchmarked and rejected the depth-8 downloader experiment because it reintroduced below-prefix resets without a sustained throughput gain.

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

## Challenges and Resolutions

- Challenge: Partial body/receipt prefixes caused long queue resets even when hundreds of contiguous blocks were valid.
  - Resolution: Accepted one full chunk as recoverable progress, repaired the residual gap, and continued lookahead below the residual boundary.

- Challenge: Healthy dense lookahead was reset when transient peer-count changes lowered the computed buffer depth.
  - Resolution: Removed that reset path; only memory pressure can now force a healthy lookahead reset.

- Challenge: Increasing active fetch depth to 8 looked promising but increased timeout churn and below-prefix failures.
  - Resolution: Reverted to depth-6 after live comparison.

## Dead Code and Obsolescence Cleanup

- Reverted the rejected depth-8 change before leaving the remote running.
- Rechecked the historical fetch/prepare/residual code paths and retained only changes that improved correctness or benchmark stability.
- No obsolete production files were removed in this pass.

## Git Workflow

- Current branch: `perf/historical-sync-throughput`
- New branch created this run: no
- Commits made during this run: `perf: improve historical downloader overlap`
- Pull request status: not ready
- Merge status: not merged
- Blockers: remaining EL historical sync performance work is still in progress.

## Known Issues or Risks

- Historical sync is still body/receipt fetch-tail bound; peer timeout clusters can hold back contiguous progress.
- The remote benchmark after restarts needs warm peer pools before logs/sec samples are comparable.
- Verification-critical security review is still required before a production-ready release.
