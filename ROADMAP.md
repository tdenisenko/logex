# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, and serves verified logs through the dashboard and query APIs.

PR #96, on branch `perf/historical-sync-live-scheduler`, is the active historical sync scheduler/performance pass. The Mac mini client is running from `/Volumes/SSD 4TB/LogEx` through full VPS routing for useful P2P coverage. The accepted scheduler keeps the paired body/receipt prefix model, schedules body and receipt roles live at the plan level, releases per-role peer ownership as soon as each role completes, reserves first-wave queued body/receipt peer work before network tasks start, supports bounded duplicate attempts for a stalled expected fetch, gates refill by active body/receipt request pressure, bounds prepared body/receipt plans by per-role request windows, reports scheduler/backpressure metrics, and keeps ordered verified ingestion intact.

Reverse historical header page downloads now have an owned async reservation path. Header page network I/O can run outside the synchronous refill loop, then materialize into the existing body/receipt fetch plan with the same validation and peer accounting. Body/receipt fetch plans now reserve queued peer load up front so later queued plans do not over-select the same fastest peers before request-start accounting catches up. Completed body/receipt chunks that sit behind a prefix gap are now carried into residual repair and verified/written once they become contiguous instead of being discarded and refetched. Prefix-critical body/receipt role retries are now bounded and candidate-expanded only when later chunks are already buffered behind a prefix gap. Stalled expected fetches can now launch one bounded duplicate attempt without discarding the original attempt or resetting lookahead. The scheduler now counts active fetch attempts instead of only sequence handles and stops refilling when body/receipt subrequest pressure is already above the ready peer pool target. The next meaningful change is finishing the broader bounded queue/backpressure scheduler, not more timeout/fanout/lookahead tuning.

The historical fetch refill policy is now centralized behind an explicit scheduler snapshot and refill scope. Full, critical-path, and write-path refill callers consume the same pressure, memory, pipeline-depth, and buffer-depth decision logic. Write-path refill can temporarily expand queued fetch admission when the buffer is low, but remains bounded by request pressure and memory limits.

Latest live scheduler work fixed a zero-prefix reset path in the body/receipt plan. Completed suffix chunks now count as prefix-critical pressure even when they begin at or beyond the accepted prefix boundary, exhausted prefix chunks can be removed from active ownership and reassigned, and a bounded final prefix salvage lane can recover missing prefix chunks before the plan returns. A broader full-window body/receipt expansion was tested and rejected because it increased short-sample throughput but still produced zero-prefix resets.

## Completed Since Last Run

- Preserved completed body/receipt chunks after residual prefix gaps:
  - Body/receipt request completion now splits contiguous prefix blocks from later completed chunks.
  - Residual historical batches carry prefetched later chunks through fetch, prepare, write, and repair boundaries.
  - Residual repair validates prefetched chunks against their headers before writing and only appends them after missing prefix gaps are filled.
  - Duplicate residual chunks keep the already-buffered copy so retries cannot overwrite earlier completed work.
- Validation passed:
  - `cargo fmt --check`
  - `cargo check -p logex-sync`
  - targeted residual chunk preservation tests
  - `cargo clippy -p logex-sync -- -D warnings`
  - `cargo test -p logex-sync`
- Deployed to the Mac mini and restarted the tmux-managed client:
  - Active log: `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-122747.log`.
  - Remote release build passed.
  - Smoke status showed the client running, historical floor moving, queued fetches active, full VPS egress active, no head-of-line block after warmup, and residual gaps being filled without validation resets.
- Added a bounded prefix-critical retry lane for body/receipt plans:
  - Detects when completed suffix chunks are buffered behind a missing prefix chunk.
  - Expands body/receipt candidate pools for that stalled prefix chunk only.
  - Keeps the extra retry budget bounded and reports those retries as prefix reassignments.
- Validation passed for the prefix-critical retry lane:
  - `cargo fmt --check`
  - `cargo check -p logex-sync`
  - targeted prefix-critical scheduler tests
  - `cargo clippy -p logex-sync -- -D warnings`
  - `cargo test -p logex-sync`
- Deployed the prefix-critical retry lane to the Mac mini and restarted the tmux-managed client:
  - Active log: `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-124218.log`.
  - Remote release build passed.
  - Smoke status showed prefix reassignments increasing, no head-of-line block, active fetches present, and the historical floor moving.
- Tested refill-during-prepare as a candidate overlap improvement and rejected it:
  - Local format, check, and targeted scheduler tests passed.
  - Live A/B sampling showed the candidate introduced pipeline reset pressure and many more request timeouts than the committed branch baseline.
  - The Mac mini was restored to the committed branch baseline without resetting the data dir.
- Added bounded duplicate attempts for the expected historical fetch:
  - A stalled expected sequence can now run at most two active attempts.
  - The first valid outcome wins, all duplicate reservations are released, and late outcomes are discarded.
  - Remote smoke testing showed no pipeline resets or head-of-line resets; the duplicate path did not trigger in the short sample, so this is accepted as a recovery primitive rather than a measured throughput win.
- Validation passed for the bounded duplicate attempt slice:
  - `cargo fmt --check`
  - `cargo check -p logex-sync`
  - `cargo clippy -p logex-sync -- -D warnings`
  - `cargo test -p logex-sync`
- Added scheduler-level request pressure backpressure:
  - Historical fetch active/pending metrics now count active request attempts, not only sequence handles.
  - Bounded duplicate attempts can begin with one buffered suffix fetch, while destructive reset still requires stronger head-of-line evidence.
  - Fetch refill now pauses when active/reserved body or receipt subrequests exceed the target per ready peer.
  - Mac mini smoke testing showed no head-of-line resets or pipeline resets in the pressure-gated sample and materially fewer timeout bursts than the immediately preceding overloaded run.
- Added plan-level body/receipt request windows:
  - Body/receipt plans now carry per-role request windows computed at preparation time.
  - Dense decoupled body/receipt execution and queued reservations now spend from the same per-role windows instead of recomputing from global constants.
  - Prefix redundancy remains bounded by spare role capacity.
  - A first version that subtracted all active peer load was rejected after live smoke testing reduced active fetch depth to 1-3; the accepted version preserves the minimum prefix window needed for forward progress and only trims spare capacity under load.
- Validation passed for the plan-level window slice:
  - `cargo fmt --check`
  - `cargo check -p logex-sync`
  - scheduler-focused body/receipt request window and reservation tests
  - `cargo clippy -p logex-sync -- -D warnings`
  - `cargo test -p logex-sync`
- Deployed the corrected plan-level window build to the Mac mini and restarted the tmux-managed client:
  - Active log: `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-143416.log`.
  - Remote release build passed.
  - Five-minute smoke showed the starvation regression fixed, historical sync around 273k logs/sec and 726 blocks/sec with 21 serving peers, 50 completed batches, no pipeline resets, and no duplicate churn.
- Tested latency/failure scoring as a candidate peer selector improvement and rejected it:
  - Local validation passed, but live Mac mini sampling regressed to about 88k logs/sec and 273 blocks/sec with fewer serving peers and a lookahead reset.
  - The candidate was reverted locally and the Mac mini was restored to the accepted branch state without resetting the data dir.
- Preserved useful partial-prefix completions when buffered suffix chunks already exist:
  - Body/receipt completion now lowers the accepted prefix floor to the residual-repair floor only when later completed chunks are buffered behind the prefix.
  - This keeps already verified contiguous prefix progress and lets residual repair fill the remaining gap, while the normal completion threshold still applies when no suffix work exists.
  - Local validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, and targeted residual prefix tests.
  - Remote smoke log `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-145746.log` showed partial-prefix completions turning into residual verified ingestion, 58 completed batches, and one early reset followed by sustained historical floor movement.
- Added in-place recovery for missing expected historical fetches:
  - A missing expected fetch sequence with later buffered fetches now refills only that expected sequence when the expected child header is still known.
  - Prepare/materialization gaps still reset because they indicate ordered validated state is missing, not only a download hole.
  - Local validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, scheduler gap tests, `cargo clippy -p logex-sync -- -D warnings`, and `cargo test -p logex-sync`.
  - Remote smoke log `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-151439.log` showed the client running after restart, 37 completed batches after warmup, zero reset/head-of-line reset logs, partial-prefix residual repair still active, and no short-sample regression.
- Centralized historical fetch refill decisions:
  - Added an explicit scheduler snapshot and refill scope so full, critical-path, and write-path refill decisions share the same pressure and memory gates.
  - Write-path refill can expand from the critical-path limit only when the fetch/prepare buffer is low and request pressure allows more work.
  - Local validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, scheduler decision tests, `cargo clippy -p logex-sync -- -D warnings`, and `cargo test -p logex-sync`.
  - Remote smoke log `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-152839.log` warmed to about 368k logs/sec and 1,014 blocks/sec with 10 serving peers, six active fetches, zero reset/head-of-line reset logs, and active residual repair.
- Stabilized live prefix repair in the paired body/receipt scheduler:
  - Rejected an aggressive full paired-window expansion after remote smoke showed it could still complete suffix chunks with zero contiguous prefix and reset lookahead.
  - Fixed buffered-suffix detection so completed chunks beyond the accepted prefix boundary still make the missing prefix critical.
  - Added exhausted-prefix cleanup and a bounded final prefix salvage lane before live plans return suffix-only results.
  - Local validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, body/receipt scheduler tests, `cargo clippy -p logex-sync -- -D warnings`, and `cargo test -p logex-sync`.
  - Remote smoke log `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-163335.log` ran for the extended sample with 88 completed batches, zero resets, zero zero-prefix resets, and continued historical floor movement.

## Remaining TODOs

1. Finish the bounded queued live request scheduler.
   - Reason: historical sync is still peer-tail bound; a slow prefix chunk can stall contiguous verified progress while other peers and later work are available.
   - Completion criteria: stale prefix-critical chunks can be reassigned from bounded queued work; downloads, verification, and ordered writes are overlapped with explicit memory/backpressure limits; ordered verified ingestion is preserved; useful network utilization stays high during peer churn; sustained full-run throughput improves without extra peer churn; and the design avoids the rejected broad role-split, duplicate whole-window, and unbounded request-pressure failure modes.

2. Complete scheduler-level backpressure.
   - Reason: the next scheduler needs to distinguish true network saturation, peer-tail stalls, prepared-buffer pressure, and ordered-write pressure.
   - Completion criteria: scheduler decisions consume live reservation depth, prefix-critical waits, stale role reassignments, active fetch attempts, prepared backlog, ordered write pressure, bandwidth, peer request latency, and dropped/retried work; the dashboard remains concise and non-spammy.

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
- Queued body/receipt plans reserve only their first-wave role requests. This gives the peer scorer immediate backpressure for queued work without treating every fallback candidate as already loaded.
- Completed body/receipt chunks behind a prefix gap are retained as residual-prefetched chunks. They are not trusted or written early; residual repair validates them against the already-verified header chain when they become contiguous.
- Prefix-critical extra body/receipt retries are enabled only when later chunks are already buffered behind a prefix gap. The retry lane expands only that prefix chunk's candidates and remains bounded so it cannot become an unbounded duplicate-request strategy.
- Refill-during-prepare is not accepted as a standalone optimization. It can improve apparent overlap, but live testing showed that adding critical-path refill from the prepare wait loop without a true central scheduler/backpressure model increases request pressure and pipeline resets.
- Expected-fetch retries now use a bounded duplicate attempt instead of aborting the original in-flight request. This preserves lookahead and avoids converting a slow expected peer into a full pipeline reset; the tradeoff is bounded extra network pressure when the duplicate path is triggered.
- Scheduler refill now treats active/reserved body and receipt subrequests as a first-class backpressure signal. This avoids keeping the sequence pipeline full by overloading a small ready peer pool and turning useful overlap into timeout bursts.
- Prepared body/receipt plans preserve enough per-role request capacity to fetch their required prefix even when existing active/reserved load is high; active load only trims spare and duplicate-prefix capacity. This avoids starving the queue while still preventing redundant work from bypassing backpressure.
- Partial-prefix completion may use the smaller residual-repair threshold only when suffix chunks are already buffered. This avoids throwing away validated contiguous prefix work while preserving the stricter normal threshold for isolated short responses.
- A missing expected fetch sequence is now treated as recoverable download-state loss when later fetches are buffered and the expected child header is known. The engine refills that sequence in place instead of resetting the entire lookahead queue; ordered prepare/write gaps still reset.
- Historical fetch refill now uses explicit scheduler scopes. The write path may admit a few more fetches than the critical path when buffers are low, but all scopes share request pressure, memory pressure, and pipeline-depth gates.
- Prefix-critical body/receipt repair is triggered by any completed suffix chunk while contiguous progress is below the accepted floor, not only suffix chunks below that floor. A completed chunk past the accepted floor is still useful evidence that the missing prefix should be recovered rather than resetting the whole plan.
- The live body/receipt plan now has a bounded final prefix salvage lane. It reuses the normal body/receipt request and count-validation path, but caps chunks, peers, and elapsed time so it remains a recovery lane rather than an unbounded serial fallback.
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
  - Remaining: measure full-run impact with the body/receipt reservation layer now in place.

- Challenge: queued body/receipt fetch plans could be prepared faster than request-start accounting reached the peer scorer.
  - Resolution: added conservative first-wave body/receipt reservations that are visible to scoring/status immediately and are released on all fetch lifecycle exits.
  - Remaining: use the reservation/backpressure signals to drive the final prefix-critical queued scheduler.

- Challenge: residual prefix repair discarded later completed body/receipt chunks after a gap, forcing the client to refetch data it had already downloaded.
  - Resolution: split completions into contiguous prefix blocks plus residual-prefetched chunks, carry those chunks through the batch lifecycle, and verify/write them once residual repair fills the gap.
  - Remaining: build stale prefix reassignment on top of this buffered residual model.

- Challenge: an active prefix chunk could exhaust its initial candidate pool while later chunks were already buffered, leaving ordered progress dependent on a stale prefix role.
  - Resolution: added a bounded prefix-critical retry lane that expands candidate pools only for the earliest stalled prefix chunk when buffered suffix chunks prove useful downloaded work is waiting.
  - Remaining: move this primitive into the broader bounded queue/backpressure scheduler.

- Challenge: refilling historical fetches while prepare tasks were still running looked like a small overlap improvement.
  - Resolution: tested it live against the committed branch baseline and rejected it after the candidate produced more timeouts and pipeline resets.
  - Remaining: implement overlap through the bounded queued scheduler instead of adding another critical-path refill hook.

- Challenge: an active expected historical fetch can still become the ordered progress gate even when later fetches are available.
  - Resolution: added a bounded duplicate attempt lane for the expected sequence; first valid completion wins and stale duplicate outcomes are ignored.
  - Remaining: integrate this primitive into the larger live queue/backpressure scheduler so duplicate attempts are driven by measured peer latency and memory pressure.

- Challenge: the engine could schedule more historical fetch windows while body/receipt subrequests were already far above the ready peer pool.
  - Resolution: added active/reserved body/receipt request pressure checks to the central refill loop.
  - Remaining: replace static request-pressure limits with measured bandwidth/latency feedback in the final scheduler.

- Challenge: the first plan-level request window implementation subtracted existing active peer load too aggressively and starved queued plans.
  - Resolution: rejected that smoke result, then changed per-plan windows to preserve the required prefix capacity and trim only spare duplicate work.
  - Remaining: use measured peer latency and bandwidth to drive the next larger queued scheduler instead of static windows alone.

- Challenge: naive peer latency/failure scoring looked like an obvious selector improvement but reduced serving-peer coverage and throughput in live sampling.
  - Resolution: reverted the selector scoring candidate and kept the simpler accepted peer scoring path.
  - Remaining: measured peer behavior should feed the central bounded scheduler, not be bolted onto peer selection as an isolated score tweak.

- Challenge: completed prefix blocks were still being discarded when the completion was below the normal minimum but had buffered suffix chunks that could be repaired.
  - Resolution: accept the smaller residual prefix threshold only when suffix chunks are already buffered and will be verified by residual repair.
  - Remaining: integrate this with stale-prefix queue ownership so partial progress is a scheduler-controlled path, not only a completion-time rescue.

- Challenge: the engine reset historical lookahead when the expected fetch sequence was missing but later fetches were already buffered.
  - Resolution: added a recovery action that refills the missing expected sequence in place when it is still safe to do so.
  - Remaining: move more of the fetch/prepare/write pressure policy into the bounded scheduler state so these recovery decisions are driven by measured pressure rather than scattered call sites.

- Challenge: fetch refill policy was duplicated across full priming, critical-path refill, and write-time refill.
  - Resolution: centralized the refill snapshot and decision logic, then made write-time refill a bounded scoped decision instead of a separate heuristic.
  - Remaining: move from scoped refill calls to a continuously managed outer request scheduler.

- Challenge: live body/receipt plans could complete many suffix chunks while returning zero contiguous prefix blocks, causing lookahead resets even with active peer capacity.
  - Resolution: corrected the buffered-suffix predicate, removed exhausted prefix ownership so missing prefixes can be reassigned, and added bounded final prefix salvage before returning a suffix-only plan.
  - Remaining: the broader queued live scheduler should make this proactive instead of relying on end-of-plan salvage.

## Dead Code and Obsolescence Cleanup

- Inspected `crates/logex-sync/src/engine/mod.rs`, `crates/logex-sync/src/engine/anchored.rs`, `crates/logex-sync/src/p2p/peer_manager/mod.rs`, and `crates/logex-sync/src/p2p/peer_manager/requests.rs`.
- Removed obsolete paired body/receipt fallback code after the live role scheduler was validated and accepted.
- Removed unused fallback metadata from live plan chunk tracking.
- Removed obsolete fallback-specific tests and kept tests covering live role capacity, chunk accounting, missing-prefix reassignment, and scheduling predicates.
- Removed the rejected bounded stale-prefix refill experiment before committing.
- Removed the rejected write-time refill guard before committing.
- Removed the rejected refill-during-prepare experiment before committing.
- Removed the obsolete direct reverse-header-pages wrapper after the engine moved to the owned plan API.
- Removed obsolete contiguous-prefix wrapper helpers after residual chunk splitting replaced them.
- Replaced the single-attempt historical fetch handle with a per-sequence attempt map so duplicate expected attempts can be tracked, aborted, and released safely.
- Inspected scheduler accounting and peer-manager request state after adding pressure backpressure; no obsolete low-peer/small-window fallback code could be safely removed yet.
- Inspected body/receipt plan scheduling after adding per-role request windows; no additional obsolete scheduler paths were safe to remove in this slice.
- Removed the rejected latency/failure scoring experiment before committing.
- Inspected partial-prefix completion and residual repair helpers after accepting buffered partial-prefix preservation; no obsolete residual paths were safe to remove yet.
- Inspected sequence-gap recovery after adding in-place expected-fetch refill; prepare-gap reset behavior remains required and was kept.
- Removed the obsolete standalone request-pressure refill check after replacing it with centralized scheduler-snapshot decisions.
- Rejected and removed the aggressive full paired-window expansion after remote smoke showed it still produced a zero-prefix reset.
- Inspected live body/receipt prefix repair helpers after the accepted fix; the bounded salvage lane remains required until the broader queued scheduler owns prefix-critical work proactively.
- Could not safely remove the untracked `.DS_Store` without a destructive filesystem action; it remains untracked and was not staged.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`
- New branch created this run: no
- Commits made during this run: `perf: preserve residual body receipt chunks`; `perf: add prefix critical receipt retries`; `docs: record scheduler refill experiment`; `perf: add bounded expected fetch retries`; `perf: gate historical refill by request pressure`; `perf: bound body receipt plan windows`; `perf: preserve buffered partial prefixes`; `perf: refill missing expected historical fetches`; `perf: centralize historical refill scheduling`; `perf: stabilize live prefix scheduler`.
- Pull request status: draft PR #96 remains open for scheduler work.
- Merge status: not merged; bounded queued scheduler/backpressure work remains incomplete.
- Validation run this pass: `cargo fmt --check`; `cargo check -p logex-sync`; residual, prefix-critical, refill/backpressure, bounded expected-fetch retry, scheduler decision, request-pressure, and body/receipt prefix salvage tests; `cargo clippy -p logex-sync -- -D warnings`; `cargo test -p logex-sync`; remote release builds and smokes on the Mac mini.
- Blockers: no external blocker. The remaining work is a larger scheduler architecture change.

## Known Issues or Risks

- Historical sync is still peer-tail bound and can show low-throughput windows even when active fetches are full.
- Current performance is sensitive to block log density, peer mix, warm-up state, and the 300/300 Mbps network link.
- Full VPS routing improves P2P coverage but has VPS bandwidth cost.
- Request pressure backpressure reduced obvious overfill, but the next improvement is still the bounded queued scheduler with explicit reservation, measured peer latency, and bandwidth-aware dispatch.
