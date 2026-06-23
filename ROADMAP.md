# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, and serves verified logs through the dashboard/query APIs. PR #95 is merged as the current `master` baseline. This branch, `perf/historical-sync-live-scheduler`, is the active historical sync scheduler/performance pass.

The Mac mini client is running from `/Volumes/SSD 4TB/LogEx` through the full VPS route for useful P2P coverage. The current accepted branch state improves dense historical sync by retrying stalled expected fetches sooner, avoiding over-deep dense low-peer fetch lookahead, batching body/receipt request accounting, coalescing duplicate request penalties per peer/role, restoring the safer 45s plan timeout, allowing two chunk requests per peer once there are 16 candidates, reducing dense body/receipt planned windows to 512 blocks, using 7 dense lookahead fetches when memory and peer count allow, capping dense body/receipt chunks to reduce peer-tail latency, refilling the critical fetch path while ordered writes are in progress, and enabling the dense lookahead boost earlier when enough ready peers exist.

## Completed Since Last Run

- Confirmed full VPS routing and public NAT behavior; the remote client advertises `157.245.195.72` for EL/CL P2P and the dashboard remains on port `18683`.
- Benchmarked the referenced high-throughput run at `/Users/gremlinmaster/logex-src/run/logex-pr96-spec-prepare-fresh-20260622-154837.log`: recent dense range p50 ~488k logs/sec, p90 ~702k, max ~1.01m.
- Accepted duplicate request-failure coalescing after live logs showed the same slow peer could receive many body/receipt penalties in one request wave.
- Accepted 16-peer chunk fanout:
  - Comparable run before fanout: last-30 average ~164k logs/sec, p90 ~297k, max ~390k.
  - Fanout run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-094825.log`: last-30 average ~221k logs/sec, p90 ~445k, max ~535k.
- Rejected 2s request timeout with 16-peer fanout because serving peers collapsed and logs/sec dropped near zero; restored 4s.
- Rejected partial-prefix early return because it caused residual backfill churn and dropped average throughput.
- Accepted 512-block dense body/receipt planned windows:
  - Baseline lower-density run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-103208.log`: average ~81k logs/sec, p50 ~83k, body/receipt avg ~20.1s, decoupled failures avg ~34/plan.
  - 512-window run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-105803.log`: average ~129k logs/sec, p50 ~110k, body/receipt avg ~15.2s, decoupled failures avg ~6/plan.
- Rejected 384-block dense windows because smaller batches reduced overall logs/sec and block/sec compared with the 512-window candidate.
- Accepted dense lookahead depth 7:
  - Depth 6 with 512-window cap: steady average ~135k logs/sec, ~379 blocks/sec.
  - Depth 7 run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-114125.log`: steady average ~216k logs/sec, ~576 blocks/sec, no plan timeouts or pipeline failures.
  - Depth 8 was rejected because it raised timeout churn and produced plan timeouts/pipeline failure despite higher bursts.
- Rejected additional static tuning that did not beat the depth-7 baseline: 20s expected-fetch retry delay, 4s/30s request pause durations, a larger completed-fetch buffer, and a larger critical refill limit.
- Accepted adaptive dense body/receipt chunk caps:
  - Baseline depth-7 run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-114125.log`: average ~221k logs/sec, p50 ~185k, ~603 blocks/sec, 1813 timeout mentions.
  - Adaptive chunk-cap run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-131037.log`: average ~251k logs/sec, p50 ~240k, ~739 blocks/sec, 742 timeout mentions.
- Deployed the accepted adaptive chunk-cap variant and confirmed the client resumed historical sync after restart.
- Rejected timed duplicate prefix hedging because it reduced body/receipt latency but lowered sustained progress by reducing useful active fetch depth.
- Rejected naive write-time refill after it caused sequence-gap resets; kept the root-cause fix that treats the sequence currently being written as owned by the pipeline.
- Accepted conservative write-time critical refill after the sequence fix:
  - Baseline run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-135947.log`: average ~197k logs/sec, p50 ~161k, 29 low windows under 100k, refill p90 ~7.7s.
  - Write-refill run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-142945.log`: average ~229k logs/sec, p50 ~188k, 5 low windows under 100k, refill p90 ~6.9s, zero sequence resets.
- Replaced the generic write-time refill guard with a write-specific refill path that keeps active downloads full even when prepared work is buffered:
  - Direct write-refill run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-145347.log`: average ~311k logs/sec, p50 ~266k, p90 ~582k, refill p90 ~3.1s, zero sequence resets.
- Rejected 32-block dense chunk caps for medium-density ranges. The run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-151531.log` reduced refill time but also shrank batches, lowered average throughput, and did not improve the end-to-end rate.
- Rejected re-testing dense lookahead depth 8 after write-time refill. The run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-153321.log` filled the network more aggressively but had worse body/receipt latency and lower average progress than the accepted depth-7 build.
- Accepted a separate 16-peer dense pipeline activation threshold while keeping the wider high-peer fetch-window threshold at 20:
  - Restored baseline `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-154649.log`: average ~266k logs/sec, p50 ~225k, p90 ~483k.
  - Threshold run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-155604.log`: average ~342k logs/sec, p50 ~309k, p90 ~576k, with no sequence resets or plan failures.
  - The tradeoff is more request timeout churn and higher body/receipt tail latency, but sustained floor progress improved under the current 300/300 Mbps network constraint.
- Rejected lowering the decoupled dense body/receipt minimum peer count from 8 to 4. The experiment made more plans use the decoupled path and reduced body/receipt plan latency, but it overfilled prepared work, reduced active download depth, and lowered warmed progress versus the committed baseline.

## Remaining TODOs

1. Build a live body/receipt request scheduler.
   - Reason: historical sync is still peer-tail bound; static fetch plans can stall on a slow prefix while other peers and later work are available.
   - Completion criteria: chunks are assigned to idle peers, timed-out work is reassigned without discarding useful lookahead, ordered verified ingestion is preserved, and sustained full-run throughput improves without extra peer churn.

2. Improve full-run historical sync stability and throughput.
   - Reason: peaks can reach the 800k+ logs/sec range, but low-throughput windows still keep the end-to-end sync time above the target.
   - Completion criteria: benchmark windows show materially lower max gaps and sustained throughput near the target while tracking active fetches, body/receipt latency, failures, serving peers, CPU, memory, disk, and network.

3. Complete EL production hardening.
   - Reason: performance work must not weaken restart safety, checkpoint freshness, forward sync, reorg handling, or query correctness.
   - Completion criteria: tests or smokes cover recent-checkpoint enforcement, stale restart rejection, CL tracking, EL forward sync, EL reverse sync, invalid peer data, reorg handling, low disk behavior, authenticated dashboard access, and a clean full-sync candidate run.

## Design Decisions

- Keep performance changes only when live Mac mini benchmarks show sustained improvement, not just higher peaks.
- Historical reverse sync remains independent of CL live-head tracking after a valid recent checkpoint-backed pivot exists.
- Logs/sec is useful for dense ranges, but block/sec and rows/block must be considered in lower-density historical ranges.
- Static global timeout reductions are rejected for now; the 2s timeout was too aggressive with higher fanout and caused peer churn. Future timeout work should be adaptive per peer/request kind.
- Earlier expected-fetch retries are worthwhile because they preserve ordered ingestion while reducing time spent waiting for one stale prefix when later lookahead already completed.
- Body/receipt plans use a 45s plan timeout with faster fanout; shorter global plan timeouts caused avoidable plan failures in live runs.
- Candidate pools switch to two chunk requests per peer at 16 peers because dense gas-bounded windows otherwise took multiple request waves even with idle local CPU and disk.
- Dense body/receipt planned windows are capped at 512 blocks. This keeps the request plan complete, avoids residual backfill, and reduces slow-tail chunk latency better than partial-prefix early return or 384-block windows.
- Dense lookahead caps at 7 active fetches. Depth 8 improves bursts but increases pending backlog and timeout churn; depth 7 provided the better sustained/stability balance in live tests.
- Dense body/receipt chunk caps now shrink with log density when at least 16 peers are available: sparse ranges keep 128-block chunks, dense ranges cap at 48 blocks, and very dense ranges cap at 32 blocks. Live testing showed this reduced tail timeouts and improved sustained logs/sec.
- During ordered historical writes, the engine uses a write-specific bounded refill path. It keeps active downloads full without treating a full prepared queue as sufficient by itself, while the normal fetch buffer still enforces memory limits. The in-progress ingest sequence is counted as pipeline-owned so refill checks do not mistake a currently written batch for a sequence gap.
- Dense lookahead can now activate at 16 serving peers, but wider high-peer fetch windows still require 20 serving peers. This keeps the downloader busy earlier without broadening request windows too aggressively.
- The next meaningful path remains a geth/Nethermind-style live scheduler with peer allocation, reassignment, and measured peer speed, not broad static timeout changes.

## Challenges and Resolutions

- Challenge: the referenced 1m logs/sec run occurred in a much denser block range than the current resumed run.
  - Resolution: compared rows/block, body/receipt latency, active fetch depth, failures, and serving peer counts instead of judging by logs/sec alone.
  - Remaining: future benchmark reports should normalize by density or include both logs/sec and block/sec.
- Challenge: larger dense fetch payloads looked like a way to amortize slow peer round trips.
  - Resolution: reverted after live testing showed long active-fetch stalls and readiness drops.
  - Remaining: larger payloads should only be reconsidered inside a live scheduler that can reassign slow chunks.
- Challenge: shorter request timeouts can clear slow chunks faster but over-penalize peers when combined with higher fanout.
  - Resolution: rejected the `2s` fanout experiment and restored the stable `4s` request timeout.
  - Remaining: implement adaptive per-peer timeout/backoff if further evidence supports it.
- Challenge: high-peer depth `8` could not be proven because the run did not reach the activation threshold.
  - Resolution: reverted the unproven change rather than leaving speculative code in the branch.
  - Remaining: retest higher depth only with a controlled run that actually reaches high serving-peer counts.
- Challenge: the expected historical fetch can block the contiguous floor while later lookahead is complete.
  - Resolution: accepted an earlier selective retry of only the expected fetch after live benchmarking showed better sustained progress and fewer fallback/residual repairs.
  - Remaining: a live scheduler should reduce this further by reassigning stale chunks instead of retrying whole fetch windows.
- Challenge: dense windows were still taking multiple request waves at 16-24 serving peers.
  - Resolution: lowered the two-request fanout threshold from 32 to 16 peers after live testing showed better sustained throughput and fewer residual gaps.
  - Remaining: rework scheduling so blocking prefix chunks are reassigned while other chunks continue downloading.
- Challenge: lower-density historical ranges had high body/receipt tail latency and many decoupled request failures.
  - Resolution: reduced dense planned windows from 1024 to 512 blocks after live testing showed higher sustained throughput, lower fetch latency, and fewer per-plan failures.
  - Remaining: a live scheduler is still needed because low-throughput windows remain when peer tail latency spikes.
- Challenge: increasing dense lookahead to 8 produced high bursts but worsened timeout churn and plan stability.
  - Resolution: tested depth 7 as an intermediate setting and accepted it after live benchmarking showed higher sustained throughput than depth 6 without the depth-8 plan failures.
  - Remaining: replace static depth tuning with a live scheduler that reacts to peer tail latency and resource pressure.
- Challenge: several simple scheduler knobs improved one metric but worsened sustained logs/sec.
  - Resolution: rejected 20s expected-fetch retry, shorter request pauses, larger fetch buffering, and broader critical refills after live benchmarks trailed the accepted baseline.
  - Remaining: fix active-fetch dips with chunk-level reassignment rather than whole-window retry/buffering.
- Challenge: dense body/receipt chunks still had peer-tail latency, especially around 48-64 block requests.
  - Resolution: accepted adaptive dense chunk caps after live testing showed higher average/median logs/sec and fewer timeout mentions.
  - Remaining: make chunk sizing more fully adaptive once the live scheduler exists.
- Challenge: refilling during writes previously caused false historical sequence-gap resets.
  - Resolution: counted the active ingest sequence as owned by the pipeline and added regression coverage before accepting bounded write-time refill. A follow-up write-specific refill path reduced refill tail latency further by not letting a full prepared queue starve active downloads.
  - Remaining: refill tails still exist at high peer counts; chunk-level reassignment remains the larger scheduler task.
- Challenge: after the write-refill fix, remaining low-throughput windows can be network-bound rather than scheduler-idle.
  - Resolution: sampled interface counters during live sync. The WireGuard tunnel reached the effective 300 Mbps class ceiling during some lower logs/sec windows, so logs/sec must be interpreted alongside bandwidth, blocks/sec, and rows/block.
  - Remaining: add better durable bandwidth/resource telemetry before making broad scheduler changes that depend on saturation signals.
- Challenge: lowering the existing high-pipeline peer threshold globally would also widen fetch windows too early.
  - Resolution: introduced a separate dense pipeline activation threshold and kept the existing high-window threshold unchanged.
  - Remaining: live scheduling should eventually replace these static thresholds.
- Challenge: forcing more request plans through the decoupled dense path can outrun ordered prepare/write ingestion.
  - Resolution: rejected the 4-peer decoupled eligibility experiment after live testing showed lower progress despite lower request-plan latency.
  - Remaining: further improvements should keep fetch, prepare, and write stages balanced instead of optimizing request latency alone.

## Dead Code and Obsolescence Cleanup

- Inspected `crates/logex-sync/src/engine/anchored.rs`; rejected deeper dense low-peer lookahead was reverted to 4 active fetches, while the accepted separate dense activation threshold remains.
- Inspected `crates/logex-sync/src/engine/anchored.rs`; rejected dense lookahead depth 8 was replaced by accepted depth 7.
- Inspected `crates/logex-sync/src/engine/anchored.rs`; rejected 20s retry, larger fetch buffer, timed duplicate prefix hedge, and unguarded critical-refill experiments were reverted or replaced by the bounded write-refill implementation.
- Inspected `crates/logex-sync/src/engine/anchored.rs` and `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected depth-8 and 32-block dense chunk retests were reverted and the remote client was restored to the accepted depth-7 / 48-block dense chunk build.
- Inspected `crates/logex-sync/src/p2p/peer_manager/mod.rs`; rejected shorter request-pause experiment was reverted.
- Inspected `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected 2s fanout timeout, partial-prefix early return, and 384-window experiments were reverted. Duplicate failure coalescing, 16-peer fanout, the 512-window cap, and adaptive dense chunk caps remain.
- Inspected `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected lower decoupled dense peer eligibility and restored the committed request-plan baseline on the remote client.
- No obsolete experiment code remains in the local worktree.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`
- New branch created this run: no
- Commits made during this run: checkpoint commit for duplicate failure coalescing and 16-peer fanout; accepted 512-block dense window cap; accepted dense lookahead depth 7 after live benchmarking; accepted adaptive dense chunk caps; accepted bounded write-time refill and sequence ownership hardening; accepted direct write-time refill; accepted separate 16-peer dense pipeline activation.
- Pull request status: draft PR #96 remains open for scheduler work.
- Merge status: not merged; throughput target and scheduler work remain incomplete.
- Validation run this pass: `cargo fmt --check`; `cargo check -p logex-sync`; `cargo test -p logex-sync`; `cargo clippy -p logex-sync -- -D warnings`; multiple remote release builds and live Mac mini benchmark samples.
- Blockers: no external blocker; the remaining work is architectural scheduler work.

## Known Issues or Risks

- Historical sync is still peer-tail bound and can show low-throughput windows even when active fetches are full.
- Current performance is sensitive to block log density, peer mix, warm-up state, and the 300/300 Mbps network link; short samples and logs/sec alone can be misleading.
- Full VPS routing is currently required for useful P2P benchmarking, but it has VPS bandwidth cost.
- A larger scheduler refactor is likely required to reach the target full-sync time.
