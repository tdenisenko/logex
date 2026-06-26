# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, and serves verified logs through the dashboard and query APIs.

PR #96, on branch `perf/historical-sync-live-scheduler`, is the active historical sync scheduler/performance pass. The Mac mini client is running from `/Volumes/SSD 4TB/LogEx` through full VPS routing for useful P2P coverage. The accepted scheduler keeps the paired body/receipt prefix model, schedules body and receipt roles live at the plan level, releases per-role peer ownership as soon as each role completes, reserves first-wave queued body/receipt peer work before network tasks start, supports bounded duplicate attempts for a stalled expected fetch, gates refill by active body/receipt request pressure, bounds prepared body/receipt plans by per-role request windows, reports scheduler/backpressure metrics, and keeps ordered verified ingestion intact.

Reverse historical header page downloads now have an owned async reservation path. Header page network I/O can run outside the synchronous refill loop, then materialize into the existing body/receipt fetch plan with the same validation and peer accounting. Body/receipt fetch plans now reserve queued peer load up front so later queued plans do not over-select the same fastest peers before request-start accounting catches up. Completed body/receipt chunks that sit behind a prefix gap are now carried into residual repair and verified/written once they become contiguous instead of being discarded and refetched. Prefix-critical body/receipt role retries are now bounded and candidate-expanded only when later chunks are already buffered behind a prefix gap. Stalled expected fetches can now launch one bounded duplicate attempt without discarding the original attempt or resetting lookahead. The scheduler now counts active fetch attempts instead of only sequence handles and stops refilling when body/receipt subrequest pressure is already above the ready peer pool target. The next meaningful change is finishing the broader bounded queue/backpressure scheduler, not more timeout/fanout/lookahead tuning.

The historical fetch refill policy is now centralized behind an explicit scheduler snapshot and refill scope. Full, critical-path, and write-path refill callers consume the same pressure, memory, pipeline-depth, and buffer-depth decision logic. Write-path refill can temporarily expand queued fetch admission when the buffer is low, but remains bounded by request pressure and memory limits.

Latest live scheduler work fixed a zero-prefix reset path in the body/receipt plan. Completed suffix chunks now count as prefix-critical pressure even when they begin at or beyond the accepted prefix boundary, exhausted prefix chunks can be removed from active ownership and reassigned, and a bounded final prefix salvage lane can recover missing prefix chunks before the plan returns. A broader full-window body/receipt expansion was tested and rejected because it increased short-sample throughput but still produced zero-prefix resets.

The latest scheduler slice splits live body/receipt work into explicit prefix and background lanes. The prefix lane keeps the contiguous verification target reserved, while the background lane can use only spare per-role capacity after the prefix lane is protected. This is accepted as a correctness-preserving architecture step, not as the final queued scheduler: live smoke in the dense recent range showed zero resets and strong throughput, but no background lane activation because those batches returned exactly the protected 512-block prefix window.

The expected-fetch recovery path now treats a slow or missing expected sequence as a bounded recoverable condition before falling back to destructive lookahead reset. Missing expected fetches can be refilled immediately, active expected attempts get time to finish, and duplicate expected attempts suppress head-of-line reset while they are still active.

Background body/receipt work is no longer discarded immediately when the protected prefix completes. A completed prefix now gives already in-flight background chunks a tiny bounded drain window, and the completion range only expands to background chunks that actually arrived. This preserves safe spare-capacity work without forcing residual repair over blocks that were never downloaded.

The engine now prepares completed historical lookahead fetches out of order while an earlier expected fetch is still running. This frees fetch-buffer slots and overlaps validation/extraction with peer-tail waits, while storage writes remain strictly ordered by sequence.

The live body/receipt scheduler now carries role-specific peer metrics into each request plan. Plan-local dispatch ranks body and receipt candidates by measured role rate plus active/reserved/local load, keeps deterministic chunk rotation as an equal-score tie-breaker, and uses bounded adaptive request deadlines only for peers predicted to complete inside the normal timeout window. A permissive timeout variant was rejected after remote smoke showed slow peers lingering too long; the accepted version restored useful floor progress without resets.

Expected-fetch duplicate recovery now uses the duplicate threshold consistently. One completed later fetch plus request-pressure headroom can launch the bounded duplicate attempt after the head-of-line delay, while destructive reset still requires the stronger reset threshold and zero active attempts.

Historical fetch refill now uses adaptive body/receipt request-slot capacity derived from the peer manager's warmed request limits. This makes queue admission respond to proven peer behavior instead of treating every ready peer as identical, while preserving an absolute pressure floor for small peer pools.

Dense decoupled body/receipt requests now return as soon as the accepted contiguous prefix is complete instead of waiting for tail chunks outside that accepted prefix. This reduces slow-peer tail latency while preserving ordered verification: only contiguous body/receipt pairs are returned, and residual gaps still flow through the existing repair path before storage advances.

The engine now has an explicit ready body/receipt plan queue between header materialization and network execution. A materialized header window can wait for queue-wide body/receipt slot margin and write/prepare pressure instead of being spawned immediately or refetched later, and `/status` exposes the queued-plan count, body/receipt slot margins, and write-backpressure state for advanced diagnostics.

Primary body/receipt completions now emit only the contiguous prefix that was actually returned. Residual chunk preservation remains limited to the explicit residual-repair path. Live Mac mini smoke on `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-212122.log` showed zero artificial residual repairs across the sampled batches, no zero-progress windows, and the remaining bottleneck shifted back to queue depth, peer-tail timeouts, and refill gaps with active fetches around seven despite much larger ready peer capacity.

Two standalone scheduler candidates were tested after that baseline and rejected. Reusing validated header suffixes without a queue-wide owner model measured below the accepted baseline, and adding an active-fetch floor reduced zero windows but still measured below baseline. The feature is not complete until the scheduler owns cross-plan body/receipt dispatch, stale-prefix reassignment, and queue-wide request pressure directly.

## Completed Since Last Run

- Evaluated and rejected two standalone live-scheduler candidates:
  - Validated header-suffix reuse plus request-prefix truncation passed local `logex-sync` tests/clippy but measured about 215 blocks/sec with low windows on the Mac mini.
  - Active-fetch floor refill passed local `logex-sync` tests/clippy but measured about 219 blocks/sec with low windows on the Mac mini.
  - The Mac mini was restored to the accepted scheduler build and restarted on `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-225443.log`.
- Eliminated artificial primary residual repairs:
  - Normal body/receipt completions now report exactly the contiguous returned prefix as their planned range.
  - Only explicit residual-repair completions preserve suffix chunks for later ordered repair.
  - Added focused tests for primary prefix-only completion and residual suffix preservation.
  - Local validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, and `cargo test -p logex-sync`.
  - Remote release build passed; live smoke on the Mac mini showed `partial=0`, `residual_ingested=0`, `residual_scheduled=0`, 56 completed batches, one low-progress window, and zero zero-progress windows in the sample.
- Added a bounded queued body/receipt admission layer:
  - Header fetch outcomes and synchronous header batches now materialize into a ready body/receipt plan before execution.
  - Ready plans keep their sequence ownership and are only started when body/receipt slot margins and write/prepare pressure allow it.
  - Historical sequence-gap detection and pending-fetch accounting now treat the ready plan as real pipeline work, so it cannot be skipped or mistaken for a missing sequence.
  - Advanced dashboard/status metrics now include ready fetch count, body/receipt slot margins, and write-backpressure state.
  - Validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, `cargo clippy -p logex-sync -- -D warnings`, `cargo test -p logex-sync`, and the focused status endpoint test.
  - Remote release build passed and the Mac mini was restarted on `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-201150.log`; the five-minute smoke showed no reset/error/zero-prefix lines and about 200k logs/sec in the current range with 37 serving peers.
- Replaced the rejected poll-refill candidate with an accepted dense scheduler completion change:
  - Removed the uncommitted wait-loop refill experiment because live sampling showed it did not solve active-fetch drain and lowered short-sample throughput.
  - Dense decoupled body/receipt chunk schedulers now stop once their accepted contiguous prefix is complete, even when the full planned dense window has slower tail chunks.
  - Focused local validation passed with `cargo fmt --check`, `cargo test -p logex-sync decoupled_dense -- --nocapture`, and `cargo test -p logex-sync historical_scheduler_decision -- --nocapture`.
  - Remote release build passed and the Mac mini was restarted on `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-194439.log`.
  - Live sample showed no reset/error lines and recovered to about 492k logs/sec and 1,637 blocks/sec with 26 serving peers after warmup.
- Added adaptive scheduler-level request pressure:
  - The peer manager now exposes body/receipt request-slot capacity scaled from per-peer block request limits that increase on fast complete responses and shrink on slow, partial, or unproven peers.
  - Historical fetch scheduler snapshots now use that adaptive capacity when deciding whether more queued body/receipt plans can be admitted.
  - Local validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, `cargo clippy -p logex-sync -- -D warnings`, and `cargo test -p logex-sync`.
  - Remote smoke log `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-191918.log` showed the client running after restart, live head current, no reset or zero-prefix logs in the sample, and the historical floor moving with about 280k logs/sec at 26 serving peers.
- Fixed the expected-fetch duplicate retry threshold:
  - Active expected fetches no longer wait for the destructive-reset threshold before launching a bounded duplicate.
  - One completed later fetch is enough to prove useful suffix work is buffered, but reset remains gated by the stricter two-fetch threshold and no active expected attempts.
  - Local validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, `cargo clippy -p logex-sync -- -D warnings`, and `cargo test -p logex-sync`.
  - Remote smoke log `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-190227.log` showed no resets or zero-prefix failures; the duplicate path did not trigger in the short sample, so this is retained as a correctness fix for the scheduler threshold rather than a measured throughput win.
- Added plan-local peer-tail admission for live body/receipt plans:
  - Request plans now snapshot body/receipt peer rates, active load, reserved load, serving state, and timeout pressure.
  - Body and receipt roles rank candidates independently using role-specific measured throughput and load, while equal-score peers still rotate by chunk for distribution.
  - Adaptive request deadlines now add bounded jitter room only for peers predicted to complete within the baseline timeout; predicted slow peers keep the normal timeout and can be retried/penalized quickly.
  - Local validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, `cargo clippy -p logex-sync -- -D warnings`, and `cargo test -p logex-sync`.
  - Remote smoke log `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-184939.log` showed the accepted version running without resets or zero-prefix failures, with about 214k-404k logs/sec in short samples despite only 8-12 serving peers.
- Added bounded out-of-order lookahead preparation for historical fetches:
  - Completed fetch windows after the ordered gate can now be validated/extracted under the existing prepare-buffer limits while the expected fetch remains in flight.
  - Ordered writes are unchanged; prepared lookahead batches wait for their sequence before ingestion.
  - Remote smoke log `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-182033.log` showed 11 lookahead prepare events in the sample, no pipeline resets, no zero-prefix failures, and continued floor movement.
  - Validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, `cargo clippy -p logex-sync -- -D warnings`, and `cargo test -p logex-sync`.
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
- Added explicit prefix/background lanes to the live body/receipt scheduler:
  - Prefix chunks are scheduled from a protected prefix queue up to the contiguous progress target.
  - Background chunks are scheduled from the same plan only when per-role request windows have spare capacity beyond the prefix lane.
  - Local validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, targeted lane tests, `cargo clippy -p logex-sync -- -D warnings`, and `cargo test -p logex-sync`.
  - Remote smoke log `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-165511.log` showed 90 completed batches, zero resets, zero zero-prefix failures, and about 572k logs/sec at the sample point with 12 serving peers. The background lane did not activate in that dense range because the planner returned 512-block prefix windows with no spare returned suffix range.
- Hardened expected-fetch recovery:
  - Missing expected fetch sequences can now be refilled even when no duplicate attempt is active.
  - Slow expected sequences can launch a bounded duplicate earlier when later fetches are buffered and body/receipt request pressure allows the refill.
  - Full lookahead reset now requires zero active attempts for the expected sequence, preventing active duplicates from triggering destructive reset.
  - Local validation passed with `cargo fmt --check`, `cargo check -p logex-sync`, `cargo clippy -p logex-sync -- -D warnings`, and `cargo test -p logex-sync`.
  - Remote smoke log `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-172228.log` showed bounded duplicate recovery completing without pipeline resets or zero-prefix failures in the sample.
- Preserved in-flight background lane work after prefix completion:
  - Live body/receipt plans now drain already-launched background chunks for a bounded 250 ms after the protected prefix completes.
  - The planned completion range grows only to completed background chunks, avoiding residual work over undownloaded suffixes.
  - A soft partial-prefix return experiment was rejected after live smoke produced many partial-prefix batches, more residual repair, and lower useful throughput despite higher serving-peer count.
  - Local validation passed with `cargo fmt --check`, focused scheduler tests, `cargo clippy -p logex-sync -- -D warnings`, and `cargo test -p logex-sync`.
  - Remote smoke log `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260625-174853.log` showed the restored build running with zero resets and no zero-prefix failures after the rejected path was removed.

## Remaining TODOs

1. Finish the bounded queued live request scheduler.
   - Reason: historical sync is still peer-tail bound; a slow prefix chunk can stall contiguous verified progress while other peers and later work are available.
   - Completion criteria: stale prefix-critical chunks can be reassigned from bounded queued work; downloads, verification, and ordered writes are overlapped with explicit memory/backpressure limits; ordered verified ingestion is preserved; useful network utilization stays high during peer churn; sustained full-run throughput improves without extra peer churn; and the design avoids the rejected broad role-split, duplicate whole-window, and unbounded request-pressure failure modes. Out-of-order lookahead preparation, plan-local peer-tail admission, adaptive request pressure, dense accepted-prefix early return, primary residual suppression, and single ready-plan admission are complete; cross-plan body/receipt dispatch and queue-wide stale-prefix ownership remain.

2. Complete scheduler-level backpressure.
   - Reason: the next scheduler needs to distinguish true network saturation, peer-tail stalls, prepared-buffer pressure, and ordered-write pressure.
   - Completion criteria: scheduler decisions consume live reservation depth, prefix-critical waits, stale role reassignments, active fetch attempts, ready-plan backlog, prepared backlog, ordered write pressure, bandwidth, peer request latency, and dropped/retried work; the dashboard remains concise and non-spammy. Ready-plan backlog, slot margins, and write-pressure diagnostics are complete; bandwidth/latency-driven queue admission remains.

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
- The live body/receipt plan now separates protected prefix work from background returned-range work. Background work is allowed only when the per-role request window has spare capacity after the prefix lane is reserved, preventing a repeat of the rejected broad full-window expansion.
- Dense decoupled body/receipt plans may now return when the accepted contiguous prefix is complete, even if tail chunks outside that accepted prefix are still missing. This keeps the verifier moving without weakening correctness because only contiguous matching body/receipt chunks are materialized.
- Materialized header windows now enter a ready body/receipt plan queue before network execution. The tradeoff is a slightly more explicit engine state machine, but it prevents header refetch churn and lets admission use queue-wide body/receipt slot margins and write-pressure state.
- Expected-fetch recovery now prefers bounded refill/duplicate recovery over destructive reset. Reset is reserved for cases where no expected-sequence attempt is active and enough buffered later work proves the ordered pipeline is blocked.
- Background lane work is opportunistic only. The plan may briefly drain already-started background chunks after prefix completion, but it must not delay ordered progress beyond the bounded grace window or extend residual repair to data that was not actually fetched.
- Completed lookahead fetches may be prepared out of order when the expected fetch is still in flight. This overlaps validation/extraction with peer-tail waits and frees fetch-buffer capacity, but writes remain sequence-ordered so verified storage coverage stays contiguous.
- Body/receipt request plans may use measured role-specific peer throughput and active load for plan-local dispatch. Adaptive request deadlines are deliberately conservative: they only extend the baseline timeout for peers predicted to complete inside that baseline, because live smoke showed that protecting predicted-slow peers worsens prefix tails.
- Expected-fetch duplicate retry and destructive reset deliberately use different thresholds. A single completed later fetch can justify one bounded duplicate under request-pressure headroom; destructive reset still requires stronger evidence and no active expected attempt.
- Transient request transport failures pause and demote peers for that request kind instead of forcing immediate local peer removal. Bad protocol responses and unsupported capabilities still receive strict reputation penalties.
- Full VPS routing is currently used for benchmark-quality P2P coverage. Dashboard-only routing exists for cost control, but it is not the current benchmark mode.
- Primary body/receipt completions must not create residual repair over undownloaded blocks. The normal path now advances by the actual contiguous prefix, while the explicit residual path remains the only path that preserves suffix chunks for repair.

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

- Challenge: the dense decoupled scheduler could keep waiting for slow tail chunks after the accepted prefix was already available.
  - Resolution: changed the dense early-stop rule to return each side once the accepted contiguous prefix is complete; residual repair still handles any missing suffix before ordered storage advances.
  - Remaining: extend this same explicit admission model to cross-plan queued body/receipt dispatch.

- Challenge: a header window could be materialized when body/receipt request slots were saturated, leaving no durable engine state between header fetch and immediate body/receipt spawn.
  - Resolution: added a ready body/receipt plan queue with sequence-aware accounting and slot-margin admission.
  - Remaining: extend from one ready plan to cross-plan stale-prefix ownership and bandwidth/latency-driven dispatch.

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

- Challenge: using spare body/receipt capacity without starving the prefix path requires a structural boundary, not another full-window expansion.
  - Resolution: added explicit prefix and background lanes; the background lane spends only role-window spare capacity and stayed inactive in the dense smoke where no safe suffix return window existed.
  - Remaining: the broader queued scheduler still needs to move this from per-plan lanes into cross-plan reservation and dispatch.

- Challenge: the first earlier expected-fetch retry variant still reset the whole lookahead while a duplicate expected attempt was active.
  - Resolution: changed the reset predicate to require zero active expected-sequence attempts and added tests for active duplicate reset suppression.
  - Remaining: the broader queued scheduler should make duplicate/refill timing driven by measured peer latency and queue pressure rather than fixed delays alone.

- Challenge: returning partial prefixes earlier looked like a way to avoid long peer tails, but it fragmented ordered progress into residual repair and reduced useful throughput in live smoke testing.
  - Resolution: rejected and removed the soft partial-prefix return path; kept the stricter prefix target and only preserved already-started background work.
  - Remaining: solve peer-tail stalls with the cross-plan queued scheduler rather than weakening the prefix target.

- Challenge: completed lookahead fetches could sit behind a slow expected fetch without using CPU validation capacity.
  - Resolution: added a bounded out-of-order prepare queue for completed lookahead fetches while preserving ordered writes.
  - Remaining: body/receipt downloads still need cross-plan dispatch and better peer-tail admission control.

- Challenge: a first adaptive timeout variant reduced immediate retries but let predicted-slow peers occupy request pressure for too long.
  - Resolution: kept role-specific peer scoring but changed adaptive deadlines to extend only peers expected to complete within the baseline timeout; slow peers keep the baseline timeout so prefix work can retry quickly.
  - Remaining: move from plan-local admission to the queue-wide scheduler so cross-plan requests share the same latency and pressure model.

- Challenge: expected-fetch duplicate retry used the stronger destructive-reset completed-fetch threshold even though the duplicate retry threshold was intentionally lower.
  - Resolution: changed the retry predicate to use `HISTORICAL_FETCH_HEAD_OF_LINE_DUPLICATE_MIN_COMPLETED`; reset behavior still uses `HISTORICAL_FETCH_HEAD_OF_LINE_MIN_COMPLETED`.
  - Remaining: make duplicate/refill timing queue-wide instead of a local predicate once the central scheduler owns cross-plan dispatch.

- Challenge: primary body/receipt completions still created residual repair for the difference between the planned dense window and the shorter contiguous prefix a peer actually returned.
  - Resolution: changed primary completion to emit only the actual contiguous prefix; residual suffix preservation is now restricted to explicit residual repair.
  - Remaining: use the freed scheduler time for more queue-wide body/receipt dispatch so active fetch depth scales with available ready peers.

- Challenge: validated-header suffix reuse and active-fetch floor refill looked like direct fixes for observed idle windows, but each regressed sustained floor progress as a standalone patch.
  - Resolution: rejected both candidates after live Mac mini sampling and restored the accepted scheduler build.
  - Remaining: implement the queue-wide live request scheduler instead of more local refill heuristics.

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
- Inspected live lane scheduling after adding prefix/background queues; no existing helper became obsolete because the salvage and prefix-critical retry paths remain required recovery mechanisms.
- Inspected expected-fetch retry and head-of-line reset helpers after hardening recovery; destructive reset remains required for unrecoverable ordered-state gaps, so no reset path was removed in this slice.
- Removed the rejected soft partial-prefix return experiment after live smoke showed it worsened useful throughput. The retained background-drain helper and tests remain because they preserve already-launched background work without changing the prefix target.
- Inspected historical fetch and prepare helpers after adding out-of-order lookahead preparation. No existing ordered-ingest or reset path was removed because writes still need contiguous sequence safety.
- Inspected live body/receipt request dispatch after adding role-specific peer metrics. No obsolete recovery path was removed because prefix salvage, prefix-critical retries, and ordered residual repair are still needed until the cross-plan scheduler owns those decisions.
- Inspected expected-fetch retry/reset predicates after aligning duplicate retry with its lower threshold. No reset path was removed because destructive reset remains required for unrecoverable ordered-state gaps.
- Inspected scheduler admission after adding adaptive request-slot capacity. No obsolete refill or reset path could be removed because the broader queued scheduler still depends on existing recovery paths until it owns cross-plan dispatch.
- Removed the obsolete direct historical fetch spawn helper after header outcomes began entering the ready body/receipt plan queue.
- Inspected ready-plan accounting, sequence-gap detection, reset handling, status wiring, and dashboard diagnostics after adding queued body/receipt admission. Cross-plan dispatch and stale-prefix ownership remain required before more recovery helpers can be removed safely.
- Removed the obsolete contiguous body/receipt prefix wrapper after extracting testable completion chunk handling.
- Could not safely remove the untracked `.DS_Store` without a destructive filesystem action; it remains untracked and was not staged.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`
- New branch created this run: no
- Commits made during this run: `docs: record live scheduler candidate results`.
- Pull request status: draft PR #96 remains open for scheduler work.
- Merge status: not merged; bounded queued scheduler/backpressure work remains incomplete.
- Validation run this pass: `cargo fmt --check`; `cargo check -p logex-sync`; ready-plan admission/status tests; lookahead sequence, residual, prefix-critical, refill/backpressure, bounded expected-fetch retry, scheduler decision, request-pressure, body/receipt prefix salvage, live lane, peer-score, and request-timeout tests; `cargo clippy -p logex-sync -- -D warnings`; `cargo test -p logex-sync`; focused status endpoint test; remote release builds and smokes on the Mac mini.
- Blockers: no external blocker. The remaining work is a larger scheduler architecture change.

## Known Issues or Risks

- Historical sync is still peer-tail bound and can show low-throughput windows even when active fetches are full.
- Current performance is sensitive to block log density, peer mix, warm-up state, and the 300/300 Mbps network link.
- Full VPS routing improves P2P coverage but has VPS bandwidth cost.
- Request pressure backpressure reduced obvious overfill, but the next improvement is still the bounded queued scheduler with explicit reservation, measured peer latency, and bandwidth-aware dispatch.
