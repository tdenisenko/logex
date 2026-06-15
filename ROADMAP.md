# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-queue-v2`. Draft PR: https://github.com/tdenisenko/logex/pull/93

The Mac mini benchmark is running on `/Volumes/SSD 4TB/LogEx` through the VPS full tunnel on HTTP port `18683`, with the dashboard exposed at `http://157.245.195.72:18683/`. The data directory was reset once for dense-range benchmarking and must not be reset again until this sync reaches genesis. Historical sync is still dominated by EL body/receipt peer churn and request tail latency rather than CPU, RAM, or disk IO.

## Completed Since Last Run

- Resumed the interrupted Mac mini benchmark through Tailscale and confirmed the public VPS dashboard path is working.
- Kept the decoupled queue early-exit change after it removed unnecessary waiting once all prefix chunks were filled.
- Accepted smaller dense body/receipt prefixes so verified 64-127 block prefixes advance instead of resetting the lookahead.

## Remaining TODOs

1. Improve body/receipt peer-tail handling.
   - Reason: Historical sync still depends on a small serving-peer set and loses throughput when sessions churn or slow chunk requests dominate a plan.
   - Completion criteria: A benchmark shows sustained improvement over the retained pipeline without increasing validation risk, memory risk, residual gaps, or peer churn. The next meaningful candidate is a geth-style idle-peer/capacity queue for historical body and receipt tasks.

2. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a live release-candidate run from a clean data directory.

## Design Decisions

- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.
- Checkpoints are accepted only when recent relative to a checkpoint-sync endpoint. Persisted state that is too stale requires a fresh recent checkpoint.
- Historical data may be downloaded ahead of the current write point, but rows are committed only after cryptographic validation and in chain order.
- Dense historical ranges stay capped at 512 fetched blocks for now. Larger dense windows regressed peer-tail behavior on the live benchmark.
- Dense historical ranges accept verified prefixes down to half a chunk. This avoids discarding cryptographically verified progress when a dense request returns 64-127 contiguous blocks, while sparse windows still require larger prefixes.
- Body/receipt request timeout remains 6 seconds with a 3-second hedge delay. Shorter timeout/hedge experiments increased churn or failed to improve sustained throughput.
- Very large sorted candidate lists are trimmed to the fastest measured body/receipt peers to avoid repeatedly assigning chunks to slow tail peers.

## Challenges and Resolutions

- Challenge: A restart exposed a native-storage hot segment where some column files had advanced beyond the catalog while WAL replay was still pending.
  - Resolution: Startup now rebuilds partially applied hot segments before replaying and truncating the WAL.

- Challenge: Residual body/receipt gaps could strand otherwise valid historical progress.
  - Resolution: Residual completion now accepts a smaller verified prefix and continues from the remaining gap boundary.

- Challenge: Plausible throughput tweaks improved short dashboard bursts but hurt wall-clock progress.
  - Resolution: Dense 1,024-block windows and 2-second hedging were reverted after log parsing showed worse plan time, failures, or residual churn.

- Challenge: Cleaning remote build artifacts exposed a missing `protoc` dependency.
  - Resolution: Installed `protobuf` on the Mac mini and used an explicit `PROTOC=/usr/local/bin/protoc` for the clean release build.

- Challenge: The source sync deleted old benchmark logs stored under the remote source `run/` directory.
  - Resolution: Current experiment logs were parsed immediately; future deploy syncs should exclude `run/` or write retained benchmark logs outside the source tree.

## Dead Code and Obsolescence Cleanup

- Rechecked the current diff and retained only the dense-prefix threshold change in this checkpoint.
- Rejected the earlier decoupled minimum-peer threshold experiment and kept the threshold at 12.

## Git Workflow

- Current branch: `perf/historical-sync-queue-v2`
- New branch created this run: no
- Commits made during this run: `perf: stop completed decoupled fetch queues early`; dense-prefix checkpoint pending commit
- Pull request status: draft PR #93 open
- Merge status: not merged
- Blockers: remaining EL historical sync performance work is still in progress.

## Known Issues or Risks

- Historical sync remains body/receipt peer-churn and fetch-tail bound; a larger scheduler rewrite may be required for another step-change improvement.
- Remote benchmark samples after restarts are not comparable until the peer pool warms up.
- Repeated restarts depress serving-peer counts, so further experiments should be larger and better justified than simple constant changes.
- Verification-critical security review is still required before a production-ready release.
