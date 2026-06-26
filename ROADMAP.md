# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed log segments, and serves verified logs through the dashboard, query APIs, and live transfer notifications.

The active work is PR #96 on branch `perf/historical-sync-live-scheduler`. This branch is focused on making historical EL sync stable under peer-tail latency. The latest accepted scheduler checkpoint keeps ordered verification/writes intact while improving downloader continuity: reverse header batches can materialize multiple body/receipt plans, ordered coalesced writes advance the fetch cursor immediately, ready plans count toward active body/receipt work, low-peer prefix hedging starts earlier, reverse header pages can race a small batch of candidates, paired body/receipt plans no longer reserve redundant prefix work that the executor does not start, and write-path refill begins before the storage write instead of waiting for the write timer or write completion.

The Mac mini client is running from `/Volumes/SSD 4TB/LogEx` on HTTP port `18683`. The latest accepted five-minute sample measured `531.2` actual blocks/sec with `2` low windows and `1` zero-progress window during peer warm-up. The remaining performance gap is still scheduler cadence under peer-tail/head-of-line pressure, not storage integrity or consensus validation.

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

## Remaining TODOs

1. Finish the bounded live request scheduler.
   - Reason: floor progress can still be limited by the slowest required body/receipt prefix chunk even while later work or peers are available.
   - Completion criteria: expected-sequence body/receipt chunks have explicit priority, stale prefix-critical chunks are reassigned without destructive resets, active body/receipt lanes stay occupied under healthy peer/memory pressure, zero-progress windows remain rare in long samples, and ordered verified ingestion is unchanged.
   - Current gap: the latest checkpoint improves active-lane continuity, but one zero-progress floor window still appeared in a five-minute sample, so the scheduler is not complete.

2. Add bandwidth-aware scheduler diagnostics and admission.
   - Reason: current metrics distinguish some request pressure, but not enough to tell whether a slowdown is peer tail latency, network saturation, backpressure, or local processing.
   - Completion criteria: status/debug metrics expose useful live request backlog, prefix wait age, retry/hedge counts, peer timeout share, bandwidth use, and write/prepare pressure; scheduler admission uses those signals without spamming the dashboard.

3. Validate a full historical sync run.
   - Reason: short samples can mislead across log-dense and sparse block ranges.
   - Completion criteria: record start-to-genesis time, p50/p90/max logs/sec, actual floor blocks/sec, zero-progress windows, bandwidth, CPU, memory, disk, peer counts, resets, and failures. The target remains a materially lower full-sync time, with a long-term goal of about four hours on this class of machine/network if peers and bandwidth allow it.

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
  - Remaining: remove the final zero-progress cadence window by tightening the live expected-prefix lane.

## Dead Code and Obsolescence Cleanup

- Inspected paired body/receipt reservation and scheduling paths.
- Removed obsolete paired redundant-prefix helper logic and its tests because the paired executor rejects duplicate chunk starts.
- `.DS_Store` remains untracked and unrelated.
- Remaining verification: once the scheduler is complete, search for obsolete status fields or debug-only metrics introduced during performance work.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`.
- New branch created this run: no.
- Commits made during this run: `perf: reduce historical scheduler idle gaps`.
- Pull request status: PR #96 remains the active draft performance PR.
- Merge status: not ready; live scheduler work is improved but not complete.
- Blockers: none for the current checkpoint.

## Known Issues or Risks

- The current accepted checkpoint improves stability but does not yet hit the target throughput. More architectural scheduler work is still required.
- Five-minute samples are useful for regressions but not enough to prove full-run performance.
- Peer mix and network routing can materially affect measurements; benchmark notes must record routing mode, serving peers, and bandwidth.
