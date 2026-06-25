# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, and serves verified logs through the dashboard and query APIs.

PR #96, on branch `perf/historical-sync-live-scheduler`, is the active historical sync scheduler/performance pass. The Mac mini client is running from `/Volumes/SSD 4TB/LogEx` through full VPS routing for useful P2P coverage. The accepted scheduler keeps the paired body/receipt prefix model, schedules body and receipt roles live at the plan level, releases per-role peer ownership as soon as each role completes, retries stale missing roles without redownloading fast halves, reports scheduler/backpressure metrics, and keeps ordered verified ingestion intact.

Reverse historical header page downloads now have an owned async reservation path. Header page network I/O can run outside the synchronous refill loop, then materialize into the existing body/receipt fetch plan with the same validation and peer accounting. The next meaningful change is extending the bounded queue/backpressure model across body/receipt reservations, not more timeout/fanout/lookahead tuning.

## Completed Since Last Run

- Implemented async historical header reservations:
  - Added a header reservation channel/handle in the sync engine.
  - Reverse header page request plans now execute outside the synchronous refill loop.
  - Header outcomes are completed through the existing peer accounting, validated with the existing reverse-page verifier, and then materialized into the existing body/receipt fetch plan.
  - Failed header reservations do not consume fetch sequence numbers, so they cannot create permanent ordered-ingest gaps.
- Wired the new header reservation channel into refill, prepare, wait, and write loops so completed header reservations are materialized while other work is in flight.
- Preserved the synchronous header/body/receipt path as the fallback for low-peer or small-window cases.
- Validation passed:
  - `cargo fmt --check`
  - `cargo check -p logex-sync`
  - `cargo test -p logex-sync reverse_header_page_requests_split_descending_pages`
  - `cargo test -p logex-sync historical_fetch`
  - `cargo clippy -p logex-sync -- -D warnings`
  - `cargo test -p logex-sync`
- Deployed to the Mac mini and restarted the tmux-managed client:
  - Active log: `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-114539.log`.
  - Remote release build passed.
  - Smoke status showed the client running, historical floor moving, and no panics or header validation failures in the new log.

## Remaining TODOs

1. Extend the bounded queued live request scheduler across body/receipt reservations.
   - Reason: historical sync is still peer-tail bound; a slow prefix chunk can stall contiguous verified progress while other peers and later work are available.
   - Completion criteria: body/receipt reservations are decoupled from verification/ingest behind a bounded memory-aware queue; prefix-critical chunks can be reassigned while later completed chunks remain buffered; ordered verified ingestion is preserved; useful network utilization stays high during peer churn; sustained full-run throughput improves without extra peer churn; and the design avoids the rejected broad role-split, duplicate whole-window, and unbounded request-pressure failure modes.

2. Complete scheduler-level backpressure.
   - Reason: the next scheduler needs to distinguish true network saturation, peer-tail stalls, prepared-buffer pressure, and ordered-write pressure.
   - Completion criteria: scheduler decisions consume live reservation depth, prefix-critical waits, stale role reassignments, active fetches, prepared backlog, ordered write pressure, bandwidth, peer request latency, and dropped/retried work; the dashboard remains concise and non-spammy.

3. Validate full-run historical sync performance.
   - Reason: short samples can be misleading across log-dense and sparse ranges.
   - Completion criteria: a fresh full-run benchmark records start-to-genesis time, p50/p90/max logs/sec, actual floor blocks/sec, zero-progress windows, bandwidth, CPU, memory, disk, peer counts, and any resets/failures. The target remains a materially lower full-sync time, with the long-term goal of four hours on the current class of machine/network if the network and peers allow it.

4. Complete EL production hardening.
   - Reason: scheduler work must not weaken restart safety, checkpoint freshness, forward sync, reorg handling, low-disk behavior, query correctness, or dashboard access.
   - Completion criteria: tests or smokes cover recent-checkpoint enforcement, stale restart rejection, CL tracking, EL forward sync, EL reverse sync, invalid peer data, reorg handling, low disk behavior, authenticated dashboard access, and a clean full-sync candidate run.

## Design Decisions

- Keep performance changes only when live Mac mini benchmarks show sustained improvement in useful verified floor progress, not just higher logs/sec peaks or higher bandwidth.
- Historical reverse sync remains independent of CL live-head tracking after a valid recent checkpoint-backed pivot exists.
- Logs/sec is useful for dense ranges, but block/sec, rows/block, peer count, body/receipt latency, and bandwidth must be evaluated together.
- Static timeout, fanout, lookahead, and buffer tuning has mostly reached diminishing returns. Future work should focus on scheduler architecture: chunk ownership, reservation expiry, measured peer speed, prefix-critical reassignment, and bounded queues.
- The accepted live role scheduler preserves the paired prefix model while releasing body and receipt peer ownership independently. This avoids the rejected broad role-split failure mode where bandwidth was spent on partial chunks that did not advance the contiguous verified floor.
- Small scheduler experiments are no longer the right path. The bounded stale-prefix refill trial also regressed, so the next implementation should be the larger queued live scheduler with explicit reservation/backpressure semantics.
- Reverse header page downloads are now represented as owned plans with a separate completion/accounting step. This is the boundary needed before the engine can run header planning as part of a bounded async reservation queue.
- Header page network I/O now runs as an async engine reservation before body/receipt planning. Sequence numbers are only consumed after a header outcome successfully becomes a body/receipt fetch plan, which keeps ordered verification recoverable after header-peer failures.
- Transient request transport failures pause and demote peers for that request kind instead of forcing immediate local peer removal. Bad protocol responses and unsupported capabilities still receive strict reputation penalties.
- Full VPS routing is currently used for benchmark-quality P2P coverage. Dashboard-only routing exists for cost control, but it is not the current benchmark mode.

## Challenges and Resolutions

- Challenge: many small scheduler experiments improved one metric while reducing actual contiguous floor progress.
  - Resolution: reverted every candidate that did not beat the accepted baseline in live Mac mini sampling.
  - Remaining: stop broad tuning and build the bounded queued scheduler directly.

- Challenge: the old fallback path made the body/receipt scheduler harder to reason about after the live role scheduler was accepted.
  - Resolution: removed the fallback path, fallback-only helpers, and fallback-only tests.
  - Remaining: the live scheduler still needs cross-window reservations and backpressure.

- Challenge: adaptive ordered-write coalescing looked like it could reduce write overhead but introduced a zero-progress window.
  - Resolution: reverted the change and kept the accepted write/refill behavior.
  - Remaining: write-side changes should be tied to real backpressure signals, not static coalescing.

- Challenge: logs/sec alone can mislead in sparse ranges or when network saturation changes.
  - Resolution: benchmarks now compare actual historical floor movement and low/zero-progress windows alongside logs/sec.
  - Remaining: use the new scheduler metrics to drive the queued scheduler instead of relying on ad hoc log parsing.

- Challenge: broadening stale-prefix refill to later prefix chunks looked like a small way to reduce idle time, but live testing introduced a zero-progress window.
  - Resolution: reverted the behavior and kept only the observability counters.
  - Remaining: build the full queue/reservation scheduler rather than adding more local refill heuristics.

- Challenge: skipping write-time refill when buffers looked healthy made short-run behavior worse and produced repeated zero-progress windows.
  - Resolution: reverted the guard and restored the committed baseline on the Mac mini.
  - Remaining: solve refill stalls with explicit asynchronous reservation/planning instead of suppressing refill from the write path.

- Challenge: reverse header page downloads were still awaited in the synchronous refill loop even after being split into owned plans.
  - Resolution: added an async header reservation channel that materializes completed header pages into body/receipt fetch plans without consuming a sequence on reservation failure.
  - Remaining: apply the same bounded reservation/backpressure model to body/receipt work and measure full-run impact.

## Dead Code and Obsolescence Cleanup

- Inspected `crates/logex-sync/src/engine/mod.rs`, `crates/logex-sync/src/engine/anchored.rs`, `crates/logex-sync/src/p2p/peer_manager/mod.rs`, and `crates/logex-sync/src/p2p/peer_manager/requests.rs`.
- Removed obsolete paired body/receipt fallback code after the live role scheduler was validated and accepted.
- Removed unused fallback metadata from live plan chunk tracking.
- Removed obsolete fallback-specific tests and kept tests covering live role capacity, chunk accounting, missing-prefix reassignment, and scheduling predicates.
- Removed the rejected bounded stale-prefix refill experiment before committing.
- Removed the rejected write-time refill guard before committing.
- Removed the obsolete direct reverse-header-pages wrapper after the engine moved to the owned plan API.
- No additional obsolete scheduler code was found that could be safely removed in the async header reservation pass; the synchronous path remains the required low-peer/small-window fallback.
- Could not safely remove the untracked `.DS_Store` without a destructive filesystem action; it remains untracked and was not staged.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`
- New branch created this run: no
- Commits made during this run: `perf: schedule historical header reservations`.
- Pull request status: draft PR #96 remains open for scheduler work.
- Merge status: not merged; bounded queued scheduler/backpressure work remains incomplete.
- Validation run this pass: `cargo fmt --check`; `cargo check -p logex-sync`; `cargo test -p logex-sync reverse_header_page_requests_split_descending_pages`; `cargo test -p logex-sync historical_fetch`; `cargo clippy -p logex-sync -- -D warnings`; `cargo test -p logex-sync`; remote release build and smoke on the Mac mini.
- Blockers: no external blocker. The remaining work is a larger scheduler architecture change.

## Known Issues or Risks

- Historical sync is still peer-tail bound and can show low-throughput windows even when active fetches are full.
- Current performance is sensitive to block log density, peer mix, warm-up state, and the 300/300 Mbps network link.
- Full VPS routing improves P2P coverage but has VPS bandwidth cost.
- The next improvement is not another small tuning pass; it is the bounded queued scheduler with explicit reservation and backpressure semantics.
