# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed log segments, and serves verified logs through the dashboard, query APIs, and live transfer notifications.

The active work is PR #96 on branch `perf/historical-sync-live-scheduler`. This branch is focused on making historical EL sync stable under peer-tail latency. The latest accepted scheduler checkpoint keeps ordered verification/writes intact while improving downloader continuity: reverse header batches can materialize multiple body/receipt plans, ordered coalesced writes advance the fetch cursor immediately, ready plans count toward active body/receipt work, low-peer prefix hedging starts earlier, reverse header pages can race a small batch of candidates, paired body/receipt plans no longer reserve redundant prefix work that the executor does not start, write-path refill begins before the storage write instead of waiting for the write timer or write completion, and live body/receipt chunks stop reusing peers that already failed the same role inside the current plan.

The Mac mini client is running from `/Volumes/SSD 4TB/LogEx` on HTTP port `18683`. The latest accepted warm five-minute sample measured `3,321.9` actual blocks/sec with `0` low windows and `0` zero-progress windows while connected peers grew from 44 to 68 and serving peers grew from 20 to 32. A follow-up sparse-tail sample measured `3,599.7` actual blocks/sec with `0` low windows and `0` zero-progress windows, and that run reached genesis.

After genesis was reached, the old full-sync backup was removed, the completed data dir was preserved as `/Volumes/SSD 4TB/LogEx-full-sync-20260626-170349`, and a fresh `/Volumes/SSD 4TB/LogEx` was created with only EL/CL peer caches and discovery secrets. Dense startup validation then exposed stale queued-plan peer load as the next scheduler bottleneck. The current build refreshes queued body/receipt plan peer snapshots immediately before spawn and load-adjusts the serving-peer score; on `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260626-103920.log`, the dense five-minute sample improved from `82.7` to `136.6` actual blocks/sec, with zero-progress windows reduced from `2` to `0`.

## Completed Since Last Run

- Kept the conservative live scheduler checkpoint:
  - Advanced the historical fetch cursor after ordered coalesced writes so already-written batches do not block new fetch planning.
  - Counted active body/receipt fetch work separately from header fetch work so the scheduler does not mistake a header task for active download capacity.
  - Preserved active body/receipt floor logic without adding an overactive refill timer.
  - Lowered low-peer body/receipt prefix redundancy to begin hedging at useful peer counts.
  - Added small parallel candidate racing for reverse header pages.
- Rejected the aggressive retry/timer experiment:
  - A `4s` expected retry, third duplicate attempt, and wait-loop critical refill produced `226.4` blocks/sec with `5` zero-progress windows in a five-minute sample.
  - Reverted that candidate to an `8s` bounded expected retry, two attempts, and the existing ready-plan spawn path.
- Removed a paired-plan over-reservation path:
  - Paired body/receipt plans were reserving redundant prefix roles that the executor could not actually start because duplicate chunk starts are ignored.
  - Removing that mismatch reduced scheduler pressure and improved downloader continuity.
- Added immediate write-path refill:
  - After ordered/coalesced writes advance the historical fetch cursor, the engine now attempts a bounded write-path refill before the storage write starts.
  - This overlaps fetch admission with short writes instead of waiting for the write-loop timer or post-write refill.
- Validated the accepted checkpoint:
  - `cargo fmt --check`
  - `cargo check -p logex-sync`
  - `cargo clippy -p logex-sync -- -D warnings`
  - `cargo test -p logex-sync`
  - Remote release build on the Mac mini
  - Remote five-minute throughput smoke on `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260626-092820.log`
- Tightened the live body/receipt role scheduler:
  - Role scheduling now re-checks the plan-level bad-peer set before assigning a prebuilt candidate to another chunk.
  - This prevents a peer that timed out or disconnected for bodies/receipts from being reused elsewhere in the same live plan.
  - Remote validation on `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260626-094234.log` produced a warm five-minute sample of `3,321.9` actual blocks/sec with `0` low windows and `0` zero-progress windows.
- Completed and preserved the latest full historical sync:
  - The accepted scheduler build reached historical floor `0`.
  - A sparse-tail validation sample produced `3,599.7` actual blocks/sec with `0` low windows and `0` zero-progress windows.
  - The completed data dir was moved to `/Volumes/SSD 4TB/LogEx-full-sync-20260626-170349`.
  - A fresh data dir was created and seeded with `known-peers.json`, `discovery-secret`, `cl/known-peers.json`, and `cl/discovery-secret`.
  - The old backup `/Volumes/SSD 4TB/LogEx-full-sync-20260625-102716` was removed, restoring about `1.0T` free space on the external SSD.
- Reduced stale queued-plan peer overload:
  - Queued historical body/receipt plans now refresh peer sender/version/load snapshots immediately before reservation and execution.
  - Serving-peer preference is now load-adjusted so an overloaded serving peer no longer outranks an idle candidate solely because it previously served successfully.
  - Dense five-minute validation improved from `82.7` actual blocks/sec, `13` low windows, and `2` zero-progress windows to `136.6` actual blocks/sec, `6` low windows, and `0` zero-progress windows.

## Remaining TODOs

1. Finish the bounded live request scheduler.
   - Reason: floor progress can still be limited by the slowest required body/receipt prefix chunk even while later work or peers are available.
   - Completion criteria: expected-sequence body/receipt chunks have explicit priority, stale prefix-critical chunks are reassigned without destructive resets, active body/receipt lanes stay occupied under healthy peer/memory pressure, zero-progress windows remain rare in long samples, and ordered verified ingestion is unchanged.
   - Current gap: stale queued-plan load was reduced, but dense-range logs still show occasional head-of-line plans completing with only a small prefix after peer timeouts. The next scheduler step is an explicit queue-owned critical lane or equivalent so later completed work cannot sit behind a weak expected sequence.

2. Add bandwidth-aware scheduler diagnostics and admission.
   - Reason: current metrics distinguish some request pressure, but not enough to tell whether a slowdown is peer tail latency, network saturation, backpressure, or local processing.
   - Completion criteria: status/debug metrics expose useful live request backlog, prefix wait age, retry/hedge counts, peer timeout share, bandwidth use, and write/prepare pressure; scheduler admission uses those signals without spamming the dashboard.

3. Validate a full historical sync run.
   - Reason: short samples can mislead across log-dense and sparse block ranges.
   - Completion criteria: record start-to-genesis time, p50/p90/max logs/sec, actual floor blocks/sec, zero-progress windows, bandwidth, CPU, memory, disk, peer counts, resets, and failures. The target remains a materially lower full-sync time, with a long-term goal of about four hours on this class of machine/network if peers and bandwidth allow it. The latest partial-resume run completed genesis, but a fresh start-to-genesis run is still needed for a comparable full-sync number.

4. Complete EL production hardening.
   - Reason: scheduler improvements must not weaken checkpoint freshness, forward sync, reorg handling, restart safety, low-disk behavior, query correctness, or dashboard access.
   - Completion criteria: tests or smokes cover fresh-checkpoint enforcement, stale restart rejection, CL tracking, EL forward sync, EL reverse sync, invalid peer data, reorg handling, low disk behavior, authenticated dashboard access, and a clean full-sync candidate run.

## Design Decisions

- Historical sync remains EL-driven after a valid recent CL checkpoint. CL forward tracking is required for live head/reorg safety, but historical reverse sync should not wait on CL head movement once the pivot is validated.
- Scheduler changes must preserve cryptographic ordering: downloads and extraction can overlap, but verified storage writes advance strictly by sequence.
- Keep bounded duplication only for critical expected/prefix recovery. Unbounded duplicate window fetching was rejected because it can waste bandwidth and increase peer pressure.
- Prefer measured, revertible scheduler checkpoints over stacking speculative tuning. The accepted branch should keep smoother sustained progress even if a short peak looks higher.
- For this Mac mini setup, networking scripts may switch between full VPS routing and dashboard-only routing, but repository code should not depend on that machine-specific arrangement.

## Challenges and Resolutions

- Challenge: aggressive expected-fetch retry and wait-loop critical refill reduced throughput and reintroduced zero-progress windows.
  - Resolution: reverted to the conservative expected retry policy and removed the wait-loop refill path.
  - Remaining: implement a proper queue-owned critical lane instead of timer-driven refill pressure.
- Challenge: active fetch counts included header fetches, hiding body/receipt download starvation.
  - Resolution: scheduler snapshots now use active body/receipt fetch counts for active-lane decisions.
  - Remaining: expose enough diagnostics to identify prefix wait and peer-tail causes quickly.
- Challenge: ordered coalesced writes could leave the fetch cursor behind already-written batches.
  - Resolution: the cursor now advances after ordered coalesced writes, with tests for ahead/behind cases.
  - Remaining: watch long runs for any remaining fetch cursor stalls.
- Challenge: paired body/receipt plans reserved redundant prefix requests that were never started.
  - Resolution: removed the paired redundant reservation/scheduling branch and kept redundancy in the decoupled dense path where it is actually executed.
  - Remaining: continue testing long runs to ensure the lower reservation pressure does not reduce resilience in sparse peer conditions.
- Challenge: write-path refill could happen only after a timer tick or after write completion.
  - Resolution: added an immediate bounded refill after the ordered fetch cursor advances and before the write task starts.
  - Remaining: validate that the improvement holds over longer dense and sparse ranges.
- Challenge: timed-out body/receipt peers could remain in other chunks' prebuilt candidate lists inside the same live plan.
  - Resolution: live role scheduling now skips role-bad peers at assignment time, not only when candidate lists are initially built or extended.
  - Remaining: confirm with longer runs that this removes repeated peer-tail stalls without reducing recovery options in poor peer conditions.
- Challenge: full-sync backups were consuming nearly all external disk space.
  - Resolution: after the new run reached genesis, removed the old backup, preserved the completed data dir under a dated full-sync backup, and restarted from a fresh data dir seeded with peer identity/cache files.
  - Remaining: track whether the fresh run's filesystem allocation remains close to the logical storage metric after compression and compaction settle.
- Challenge: queued dense body/receipt plans reused stale peer-load snapshots and could concentrate work on already-busy serving peers.
  - Resolution: refresh plan peer snapshots at spawn time and make the serving-peer score decay with active/reserved role load.
  - Remaining: add a stronger head-of-line recovery lane for dense ranges where a plan returns only a small prefix after multiple peer timeouts.

## Dead Code and Obsolescence Cleanup

- Inspected paired body/receipt reservation and scheduling paths.
- Removed obsolete paired redundant-prefix helper logic and its tests because the paired executor rejects duplicate chunk starts.
- `.DS_Store` remains untracked and unrelated.
- Remaining verification: once the scheduler is complete, search for obsolete status fields or debug-only metrics introduced during performance work.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`.
- New branch created this run: no.
- Commits made during this run: `perf: reduce historical scheduler idle gaps`; `perf: avoid retrying bad live role peers`; `docs: record scheduler validation and backup rotation`; pending commit for queued-plan peer-load refresh.
- Pull request status: PR #96 remains the active draft performance PR.
- Merge status: not ready; live scheduler work is substantially improved, but longer validation is still needed before concluding PR #96.
- Blockers: none for the current checkpoint.

## Known Issues or Risks

- The current accepted checkpoint improves stability but does not yet hit the target throughput. More architectural scheduler work is still required.
- Five-minute samples are useful for regressions but not enough to prove full-run performance.
- Peer mix and network routing can materially affect measurements; benchmark notes must record routing mode, serving peers, and bandwidth.
- After full sync, EL live head advanced correctly, but the node state briefly reported `Waiting For Consensus` while CL optimistic update RPCs were backing off. This needs follow-up if it persists in fresh runs.
