# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed verified logs, and serves dashboard, SQL query, JSON-RPC, gRPC, and live ERC20 transfer subscription APIs.

Active branch: `perf/fresh-historical-baseline`. When outside the home network, Mac mini operations must use `ssh -J pi-remote gremlinmaster@192.168.50.44`. The active remote run uses `/Users/gremlinmaster/logex-baseline-src`, data dir `/Volumes/SSD 4TB/LogEx`, HTTP port `18683`, and tmux session `logex`.

Historical sync can still reach high instantaneous throughput, but ordered floor movement remains bursty in dense log ranges. The accepted scheduler keeps the critical historical fetch active, buffers cheap ready fetch plans ahead of slow header windows, prioritizes missing expected fetches before lookahead prepare work, avoids blocking ordered writes on expensive post-write refill when prepared batches are already queued, forces a limited post-write refill when active body/receipt downloads fall below the write-path floor, discards same-sequence work planned against stale expected child headers, and lets write-path refills fill the adaptive active pipeline. Recent local salvage/refill tweaks failed to beat the accepted baseline, so the next meaningful performance work should be a global live body/receipt request scheduler rather than more constants-only tuning.

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
- Re-ran remote validation through `pi-remote` after direct network access failed outside the home network.
- Rejected a completed-buffer overflow experiment: it measured `279` blocks/sec with `3` low windows and `1` zero window.
- Rejected an eight-lane active-target experiment: it measured `326` blocks/sec with `1` low window and `1` zero window versus the accepted baseline at `324` blocks/sec with `0` low windows and `0` zero windows on the same route.
- Added active fetch child-header tracking so expected-sequence work can be discarded immediately when ordered writes advance to a different child header.
- Increased write-path refill headroom so active body/receipt downloads can refill to the adaptive pipeline depth instead of staying capped at four total refill slots.
- Measured the active-refill/stale-child build at `264` blocks/sec average with `4` low windows and `1` zero window; it reduced active-depth collapse but did not eliminate peer-tail stalls.
- Rejected a shorter expected-fetch hedge delay after it produced `2` zero windows within the first few minutes despite more than 25 serving peers.
- Re-ran the remote tests through `pi-remote` after the Mac mini direct route became unreachable outside the home network.
- Rejected a lookahead-promotion experiment for missing expected historical fetches: it measured `126.2` blocks/sec with `8` low windows and `2` zero windows, below the accepted baseline.
- Rejected a partial-prefix salvage skip experiment: it measured `107.9` blocks/sec with `7` low windows and `1` zero window, and changed burst shape without improving floor movement.
- Restored and rebuilt the accepted baseline on the Mac mini tmux session after each rejected experiment.
- Validated locally with focused scheduler, sequence-gap, historical fetch tests, and `cargo check` for touched crates.

## Remaining TODOs

1. Complete the live historical request scheduler.
   - Reason: downloads, prepare, and ordered writes still move in bursts; local refill, promotion, active-depth, and salvage changes did not beat the accepted scheduler baseline.
   - Completion criteria: either prove the current scheduler is the practical baseline under the available network, or replace the plan-level fetch model with a global live chunk scheduler that fairly allocates body/receipt lanes across expected and lookahead work and improves longer warmed remote samples without increasing low/zero-progress windows.

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

- Expected-sequence active fetch attempts remember their planned child header.
  - Why: ordered coalescing can advance the expected child while a same-sequence fetch planned from the old child is still active or queued.
  - Tradeoff: the scheduler may discard a small amount of in-flight work, but it avoids waiting for a fetch that cannot advance the floor.

- Rejected active-depth-only tuning as a production strategy.
  - Why: both a completed-buffer overflow gate and an eight-lane active target failed to improve the accepted warmed baseline without adding low/zero windows.
  - Alternative considered: keep the constants-only changes; rejected because the improvement was not meaningful and stability regressed.

- Rejected shorter expected-fetch hedge timing.
  - Why: duplicating the head-of-line fetch earlier increased zero-progress windows under high serving-peer counts.
  - Alternative considered: keep the 2 second hedge; rejected in favor of the previous 4 second head-of-line delay.

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
  - Remaining: any next architectural pass should be a global live chunk scheduler or a full-run benchmark proving the current scheduler is bounded by network/runtime conditions.

- Challenge: active-depth experiments looked promising in spot metrics but failed warmed samples.
  - Resolution: reverted both rejected experiments locally and remotely, restored the accepted baseline, and left the Mac mini client running on the baseline build.
  - Remaining: compare future changes only against the accepted baseline and keep only changes that improve longer samples.

- Challenge: same-sequence fetches sometimes remained active after the expected child header changed.
  - Resolution: active attempts now store their planned child header and the cursor-advance path discards mismatched expected-sequence queued, completed, and active work.
  - Remaining: peer-tail body/receipt responses can still block the ordered floor even when active depth is healthy.

## Dead Code and Obsolescence Cleanup

- Reverted rejected chunk-size and partial-flush timing experiments before this pass.
- Current branch contains only accepted scheduler changes: stale-work critical refill, ready-plan buffering, expected-fetch priority, non-blocking post-write refill, proactive expected refill, active-download-aware post-write refill, expected-child mismatch cleanup, and adaptive write-path active refill.
- Reverted rejected completed-buffer overflow, eight-lane active-target, and shorter expected-hedge experiments before committing.
- Reverted rejected lookahead-promotion and partial-prefix salvage skip experiments locally and remotely.
- No production code was identified as safe to remove beyond stale experiment cleanup.

## Git Workflow

- Current branch: `perf/fresh-historical-baseline`.
- New branch created this run: no, continuing the active performance branch.
- Commits made during this run: `docs: record rejected scheduler experiments`.
- Pull request status: not created yet; branch remains in performance validation.
- Merge status: not merged.
- Blockers: none known.

## Known Issues or Risks

- Current samples are shorter than a full sync and still show bursty floor movement in dense log ranges.
- Peer count and routing mode affect comparability; record both for every benchmark.
- A global live chunk scheduler would be a material architecture change; do not start it without accepting the larger refactor risk or first proving the current scheduler is the practical baseline with a full-run benchmark.
