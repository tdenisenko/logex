# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed verified logs, and serves dashboard, SQL query, JSON-RPC, gRPC, and live ERC20 transfer subscription APIs.

Active branch: `perf/fresh-historical-baseline`. When outside the home network, Mac mini operations must use `ssh -J pi-remote gremlinmaster@192.168.50.44`. The active remote run uses `/Users/gremlinmaster/logex-baseline-src`, data dir `/Volumes/SSD 4TB/LogEx`, HTTP port `18683`, and tmux session `logex`.

Historical sync can still reach high instantaneous throughput, but ordered floor movement remains bursty in dense log ranges. The latest fixes keep the critical historical fetch active, allow cheap ready fetch plans to queue ahead of slow header windows, prioritize missing expected fetches before lookahead prepare work, avoid blocking ordered writes on expensive post-write refill when prepared batches are already queued, and force a limited post-write refill when active body/receipt downloads fall below the write-path floor.

## Completed Since Last Run

- Reproduced the zero-progress stall through the Pi jump host: the floor stayed pinned while peers and active downloads remained present.
- Added critical-path repair for missing expected historical fetches without resetting buffered lookahead.
- Aborted stale historical fetch work below the expected sequence so obsolete attempts release request reservations.
- Confirmed the patch moved the remote floor off the pinned block and restored high instantaneous throughput; warmed samples still show bursty ordered progress.
- Split cheap ready fetch plan buffering from the active/heavy historical fetch pipeline.
- Measured the ready-plan change on the remote warmed run: average floor movement improved from `161` to `199` blocks/sec, low windows fell from `12` to `6`, and zero windows fell from `3` to `1`.
- Prioritized missing expected fetch refill ahead of lookahead prepare work so later buffered work cannot delay the next block range needed to advance the verified floor.
- Avoided blocking the ordered write loop on post-write pipeline refill when prepared historical batches are already queued.
- Added proactive missing-expected refill immediately after ordered writes advance the historical cursor.
- Measured the latest remote warmed run at `273` blocks/sec average with `1` low window and `0` zero windows after peers warmed to the low/mid 30s.
- Added an active-download-aware post-write refill gate so prepared backlog can no longer hide an empty active body/receipt pipeline.
- Measured the follow-up remote warmed run at `326` blocks/sec average with `0` low windows and `0` zero windows after peers warmed past 20 serving peers.
- Validated locally with focused scheduler, sequence-gap, historical fetch tests, and `cargo check` for touched crates.

## Remaining TODOs

1. Complete the live historical request scheduler.
   - Reason: downloads, prepare, and ordered writes still move in bursts; the latest active-refill gate stabilizes floor movement but active downloads can still hover near the lower write-path floor instead of the full pipeline depth.
   - Completion criteria: active downloads stay near the chosen pipeline depth while memory is healthy, stale/non-advancing work cannot consume critical slots, and longer warmed remote samples keep sustained floor movement without repeated low/zero windows.

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

- Ready fetch plans use a separate cheap buffer from active/heavy fetched data.
  - Why: slow reverse-header planning windows were letting active body/receipt downloads run dry even when memory and bandwidth were available.
  - Tradeoff: the scheduler keeps more header/plan metadata in memory, while active receipt/body downloads remain capped by the pipeline depth.

- Ordered writes no longer wait on non-critical post-write refill when prepared batches are ready.
  - Why: a measured stall spent about 24 seconds in refill after a batch was already written, preventing the next verified prepared batch from advancing the floor.
  - Tradeoff: refill may run slightly later when there is prepared backlog, so active download depth still needs a follow-up refill policy that keeps the network busier without increasing memory pressure.

- Post-write refill uses active body/receipt depth, not only buffered inventory.
  - Why: ready/completed/prepared backlog can look healthy while active network downloads have drained.
  - Tradeoff: the write loop may briefly block on a limited write-path refill when active downloads are below the floor, but it avoids multi-window floor stalls.

## Challenges and Resolutions

- Challenge: direct Mac mini access failed outside the home network.
  - Resolution: reran checks and throughput samples through `pi-remote`.
  - Remaining: use the jump host unless direct LAN access is confirmed.

- Challenge: the scheduler entered a state with active fetches but no active expected fetch, no prepare-ready work, and no floor movement.
  - Resolution: added stale-work cleanup and missing-expected refill.
  - Remaining: single-interval zero windows still occur, so the broader live scheduler is not complete.

- Challenge: active body/receipt downloads ran low while waiting for slow reverse-header planning.
  - Resolution: added a separate ready-plan buffer so cheap queued plans can hide header latency.
  - Remaining: ordered prepare/write still causes shorter burstiness.

- Challenge: expected-sequence holes were detected only after several prepared lookahead batches had accumulated.
  - Resolution: moved missing-expected refill ahead of lookahead prepare work and added a proactive refill after ordered writes advance the cursor.
  - Remaining: dense ranges can still drain active downloads while many prepared batches wait to be written.

- Challenge: prepared backlog hid active body/receipt download starvation.
  - Resolution: post-write refill now considers active body/receipt fetch count and forces a limited write-path refill when active downloads fall below the floor.
  - Remaining: the next architectural pass should make refill planning more independent so active downloads target the full pipeline depth, not just the floor.

## Dead Code and Obsolescence Cleanup

- Reverted rejected chunk-size and partial-flush timing experiments before this pass.
- Current branch contains only accepted scheduler changes from this pass: stale-work critical refill, ready-plan buffering, expected-fetch priority, non-blocking post-write refill, proactive expected refill, and active-download-aware post-write refill.
- No production code was identified as safe to remove beyond stale experiment cleanup.

## Git Workflow

- Current branch: `perf/fresh-historical-baseline`.
- New branch created this run: no, continuing the active performance branch.
- Commits made during this run: stale historical fetch critical refill; ready-plan buffering; expected-fetch priority and non-blocking refill.
- Pull request status: not created yet; branch remains in performance validation.
- Merge status: not merged.
- Blockers: none known.

## Known Issues or Risks

- Current samples are shorter than a full sync and still show bursty floor movement in dense log ranges.
- Peer count and routing mode affect comparability; record both for every benchmark.
- The next larger scheduler change may require restructuring download, prepare, and ordered write coordination rather than tuning constants.
