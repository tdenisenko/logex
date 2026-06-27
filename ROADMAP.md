# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed verified logs, and serves dashboard, SQL query, JSON-RPC, gRPC, and live ERC20 transfer subscription APIs.

Active branch: `perf/fresh-historical-baseline`. When outside the home network, Mac mini operations must use `ssh -J pi-remote gremlinmaster@192.168.50.44`. The active remote run uses `/Users/gremlinmaster/logex-baseline-src`, data dir `/Volumes/SSD 4TB/LogEx`, HTTP port `18683`, and tmux session `logex`.

Historical sync can still reach high instantaneous throughput, but ordered floor movement remains bursty. The latest fix prevents a missing expected historical fetch from leaving the downloader busy on non-advancing stale work.

## Completed Since Last Run

- Reproduced the zero-progress stall through the Pi jump host: the floor stayed pinned while peers and active downloads remained present.
- Added critical-path repair for missing expected historical fetches without resetting buffered lookahead.
- Aborted stale historical fetch work below the expected sequence so obsolete attempts release request reservations.
- Confirmed the patch moved the remote floor off the pinned block and restored high instantaneous throughput; warmed samples still show bursty ordered progress.
- Validated locally with focused historical sequence-gap and historical fetch tests.

## Remaining TODOs

1. Complete the live historical request scheduler.
   - Reason: downloads, prepare, and ordered writes still move in bursts; one-interval zero-progress windows remain even after the hard stall was fixed.
   - Completion criteria: expected-sequence work is always prioritized, stale/non-advancing work cannot consume critical slots, and warmed remote samples show sustained floor movement without repeated low/zero windows.

2. Establish a production baseline from a fresh dense-range run.
   - Reason: short samples prove regressions or fixes, but the PR needs end-to-end sync time against the 4 hour target.
   - Completion criteria: record wall-clock sync time, logs/sec, blocks/sec, peer counts, bandwidth, CPU, memory, disk, low/zero-progress windows, routing mode, and failures for a fresh pivot-to-genesis run.

3. Continue peer and bandwidth utilization work only from measured bottlenecks.
   - Reason: recent constants-only experiments produced mixed results or regressions.
   - Completion criteria: keep only changes that improve longer remote samples without increasing low/zero-progress windows or weakening validation.

## Design Decisions

- Historical backfill keeps ordered verification as the commit boundary.
  - Why: logs are valid only after block/receipt data is cryptographically checked and written in canonical reverse order.
  - Tradeoff: unordered downloads can run ahead, but the scheduler must explicitly protect the next-needed sequence.

- Missing expected historical fetches are refilled without discarding buffered lookahead.
  - Why: resetting all lookahead wastes useful work and creates more burstiness.
  - Alternative considered: full pipeline reset, which fixed some gaps but caused avoidable churn.

- Stale fetch work below the expected sequence is aborted.
  - Why: those results can no longer advance the floor and otherwise keep body/receipt reservations occupied.
  - Tradeoff: a small amount of already-started network work may be discarded to keep critical slots available.

## Challenges and Resolutions

- Challenge: direct Mac mini access failed outside the home network.
  - Resolution: reran checks and throughput samples through `pi-remote`.
  - Remaining: use the jump host unless direct LAN access is confirmed.

- Challenge: the scheduler entered a state with active fetches but no active expected fetch, no prepare-ready work, and no floor movement.
  - Resolution: added stale-work cleanup and missing-expected refill.
  - Remaining: single-interval zero windows still occur, so the broader live scheduler is not complete.

## Dead Code and Obsolescence Cleanup

- Reverted rejected chunk-size and partial-flush timing experiments before this pass.
- Current branch contains only the previously accepted scheduler commits plus the new stale-work critical-refill fix.
- No production code was identified as safe to remove beyond stale experiment cleanup.

## Git Workflow

- Current branch: `perf/fresh-historical-baseline`.
- New branch created this run: no, continuing the active performance branch.
- Commits made during this run: stale historical fetch critical refill.
- Pull request status: not created yet; branch remains in performance validation.
- Merge status: not merged.
- Blockers: none known.

## Known Issues or Risks

- Current samples are shorter than a full sync and still show bursty floor movement.
- Peer count and routing mode affect comparability; record both for every benchmark.
- The next larger scheduler change may require restructuring download, prepare, and ordered write coordination rather than tuning constants.
