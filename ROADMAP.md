# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-queue-v2`. Draft PR: https://github.com/tdenisenko/logex/pull/93

The Mac mini benchmark is running on `/Volumes/SSD 4TB/LogEx` through the VPS full tunnel on HTTP port `18683`. The data directory was reset once for dense-range benchmarking and must not be reset again until this sync reaches genesis. Historical sync is still dominated by EL body/receipt peer tail latency rather than CPU, RAM, or disk IO.

## Completed Since Last Run

- Added hot-segment WAL replay recovery for partially applied native-storage writes seen during restart testing.
- Added residual historical body/receipt prefix completion so small verified residual gaps can make progress instead of resetting the whole lookahead.
- Increased initial body/receipt request limits to 48 with wider latency thresholds, and trimmed very large body/receipt candidate lists to the fastest measured peers.
- Added low-volume body/receipt plan diagnostics used to reject unhelpful performance experiments.
- Benchmarked and rejected two new experiments:
  - Dense 1,024-block fetch windows: increased plan time, residual gaps, and request failures.
  - 2-second body/receipt hedge delay: did not improve sustained wall-clock throughput and nearly doubled request failures.
- Restored the remote run to the stable 512-block dense fetch cap and 3-second hedge delay.

## Remaining TODOs

1. Improve body/receipt peer-tail handling.
   - Reason: Historical sync still waits on slow or timeout-prone peers for contiguous body/receipt prefixes.
   - Completion criteria: A benchmark shows sustained improvement over the retained pipeline without increasing validation risk, memory risk, residual gaps, or peer churn. The next meaningful candidate is a geth-style idle-peer/capacity queue for historical body and receipt tasks.

2. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a live release-candidate run from a clean data directory.

## Design Decisions

- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.
- Checkpoints are accepted only when recent relative to a checkpoint-sync endpoint. Persisted state that is too stale requires a fresh recent checkpoint.
- Historical data may be downloaded ahead of the current write point, but rows are committed only after cryptographic validation and in chain order.
- Dense historical ranges stay capped at 512 fetched blocks for now. Larger dense windows regressed peer-tail behavior on the live benchmark.
- Body/receipt request timeout remains 6 seconds with a 3-second hedge delay. Shorter timeout/hedge experiments increased churn or failed to improve sustained throughput.
- Very large sorted candidate lists are trimmed to the fastest measured body/receipt peers to avoid repeatedly assigning chunks to slow tail peers.

## Challenges and Resolutions

- Challenge: A restart exposed a native-storage hot segment where some column files had advanced beyond the catalog while WAL replay was still pending.
  - Resolution: Startup now rebuilds partially applied hot segments before replaying and truncating the WAL.

- Challenge: Residual body/receipt gaps could strand otherwise valid historical progress.
  - Resolution: Residual completion now accepts a smaller verified prefix and continues from the remaining gap boundary.

- Challenge: Plausible throughput tweaks improved short dashboard bursts but hurt wall-clock progress.
  - Resolution: Dense 1,024-block windows and 2-second hedging were reverted after log parsing showed worse plan time, failures, or residual churn.

## Dead Code and Obsolescence Cleanup

- Reverted rejected dense-window and hedge-delay experiments locally and on the remote.
- Removed stray remote-root source copies created by an incorrect rsync destination.
- Rechecked the current diff and retained only the storage recovery, residual-prefix, request-limit, fast-peer pool, and diagnostic changes.

## Git Workflow

- Current branch: `perf/historical-sync-queue-v2`
- New branch created this run: no
- Commits made during this run: pending
- Pull request status: draft PR #93 open
- Merge status: not merged
- Blockers: remaining EL historical sync performance work is still in progress.

## Known Issues or Risks

- Historical sync remains body/receipt fetch-tail bound; a larger scheduler rewrite may be required for another step-change improvement.
- Remote benchmark samples after restarts are not comparable until the peer pool warms up.
- Repeated restarts depress serving-peer counts, so further experiments should be larger and better justified than simple constant changes.
- Verification-critical security review is still required before a production-ready release.
