# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, and serves verified logs through the dashboard/query APIs. PR #95 is merged as the current `master` baseline. This branch, `perf/historical-sync-live-scheduler`, is the active historical sync scheduler/performance pass.

The Mac mini client is running from `/Volumes/SSD 4TB/LogEx` through the full VPS route for useful P2P coverage. A completed full-sync snapshot is preserved at `/Volumes/SSD 4TB/LogEx-full-sync-20260624-123315`; the active `LogEx` data dir was reset with only peer metadata copied forward for continued dense-range testing. The current accepted branch state improves dense historical sync by retrying stalled expected fetches sooner, avoiding over-deep dense low-peer fetch lookahead, batching body/receipt request accounting, coalescing duplicate request penalties per peer/role, restoring the safer 45s plan timeout, allowing two chunk requests per peer once there are 16 candidates, reducing dense body/receipt planned windows to 512 blocks, using 7 dense lookahead fetches when memory and peer count allow, capping dense body/receipt chunks to reduce peer-tail latency, refilling the critical fetch path while ordered writes are in progress, enabling the dense lookahead boost earlier when enough ready peers exist, treating transient request transport closures as soft per-kind pauses instead of immediate peer removals, and keeping six dense active fetch windows once at least eight serving peers are available.

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
- Accepted a larger healthy-memory prepare buffer while keeping the low-memory path unchanged:
  - Restored baseline `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-163936.log`: last-60 average ~329k logs/sec, p50 ~295k, p90 ~536k.
  - Prepare-buffer run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-164912.log`: last-60 average ~334k logs/sec, p50 ~301k, p90 ~517k, with fewer low windows and no sequence resets or plan failures.
  - The change is modest but useful: it smooths write/download overlap without hiding the remaining active-fetch scheduling bottleneck.
- Rejected changing the healthy-memory fetch budget from completed-only to total outstanding work. The run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-170413.log` overfilled prepared work, dropped active fetches, and reduced last-60 progress to ~143k logs/sec versus ~334k on the accepted baseline.
- Accepted preserving performance-sorted peer order for body/receipt chunk attempts instead of rotating equally loaded peers by chunk index:
  - Restored baseline `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-171504.log`: last-60 average ~228k logs/sec, p50 ~180k, 6 low windows under 100k, body/receipt avg ~8.3s.
  - Peer-ordering run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-172437.log`: last-100 average ~321k logs/sec, p50 ~298k, zero low windows under 100k, active fetch avg ~6.9/7, body/receipt avg ~8.1s and paired plan avg ~5.1s.
  - This matches Nethermind's fast-block preference for measured faster peers while still balancing per-plan in-flight requests.
- Confirmed the latest full sync reached genesis, rotated the completed data dir into `/Volumes/SSD 4TB/LogEx-full-sync-20260624-123315`, deleted the superseded backup, and restarted fresh from peer metadata only.
- Replaced the stale default checkpoint source in the remote benchmark script with the agreeing Beacon API quorum `https://ethereum-beacon-api.publicnode.com,https://lodestar-mainnet.chainsafe.io`; stale checkpoint rejection correctly prevented startup with the old SIGP checkpoint.
- Benchmarked the fresh accepted build under the current 300/300 Mbps network:
  - `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-054601.log`: last-60 average ~414k logs/sec, p50 ~361k, p90 ~677k, max ~970k, no sequence resets or pipeline failures.
  - RX bandwidth usually ranged ~115-214 Mbps during dense sync, so the bottleneck is still peer/request tail behavior rather than disk or CPU, with the link becoming relevant during stronger windows.
- Rejected decoupled dense peer-order preservation after live testing:
  - The experiment improved bursts but produced near-zero progress windows while active fetches and RX remained high.
  - Restored the accepted scheduler on the remote and confirmed historical progress resumed from the same data dir.
- Rejected lowering dense high-depth activation from 16 to 12 serving peers:
  - The experiment increased active depth and block throughput in some windows, but raised timeout churn and produced a body/receipt pipeline failure.
  - The accepted scheduler was restored on the remote and restarted from the same data dir.
- Accepted soft handling for transient body/receipt request transport failures:
  - Before the change, correlated `Disconnected` / channel-close request failures could amplify into peer-pool collapse even while the network itself remained usable.
  - After deployment in `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-070206.log`, serving-peer p50 improved from ~16 to ~31, fetch-capacity p50 from ~17 to ~31, progress p50 from ~351k to ~397k logs/sec, and p90 from ~608k to ~748k logs/sec.
  - The remaining low windows now correlate with long body/receipt plan tails and ordered prefix refill waits, not loss of the peer pool.
- Rejected widening the fast body/receipt candidate pool from 24 to 36 peers because the candidate did not activate in the observed window and had no proven benefit.
- Rejected raising high-peer per-peer request fanout from 2 to 3:
  - Accepted baseline `/private/tmp/logex-perf-logs/logex-accepted-after-revert-20260624-072956.log`: average ~465k logs/sec, p50 ~423k, p90 ~787k, paired plan p50 ~6.1s.
  - Fanout-3 run `/private/tmp/logex-perf-logs/logex-fanout3-20260624-074158.log`: average ~418k logs/sec, p50 ~343k, p90 ~794k, with higher request failure count.
  - The change was reverted because it increased pressure without improving sustained throughput.
- Accepted denser low-peer lookahead after live testing:
  - Change: use six dense active fetch windows once at least eight serving peers are available.
  - Candidate run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-080230.log`: average ~493k logs/sec, p50 ~485k, p90 ~747k, active fetch p50/p90 6/7, paired plan p50/p90 ~5.2s/~10.1s, zero historical pipeline resets.
  - The previous accepted comparison averaged ~465k logs/sec with p50 ~423k and paired plan p50/p90 ~6.1s/~12.5s.
- Rejected retesting dense lookahead depth 8 on the new baseline:
  - The run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-082530.log` increased active depth and RX bursts, but average progress fell to ~437k logs/sec, p50 fell to ~398k, and paired plan p90 worsened to ~14.7s.
  - The change was reverted and the remote client was restored to the accepted depth-7 build.
- Rejected increasing the dense fetch row target from 250k to 350k rows:
  - The run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-084124.log` raised RX utilization but over-buffered prepared work, produced one pipeline failure line, and trailed the accepted baseline at ~472k average logs/sec and ~449k p50.
  - The change was reverted and the remote client was restored to the accepted 250k-row dense window target.
- Rejected reducing the body/receipt hedge delay from 1.5s to 1.0s:
  - The run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-085853.log` lowered some plan latency but raised duplicate/wasted request pressure, produced one pipeline failure line, and dropped progress to ~392k average logs/sec and ~375k p50.
  - The change was reverted and the remote client was restored to the accepted 1.5s hedge delay.
- Rejected disabling the decoupled dense body/receipt path:
  - The run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-091648.log` had more serving peers but worse useful throughput at ~442k average logs/sec and ~401k p50, with paired plan p90 worsening to ~17.0s and a larger prepared backlog.
  - The change was reverted because the decoupled path is still useful for keeping verified ingestion moving under peer-tail latency.
- Rejected paired-plan lookahead hedging:
  - The run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-094038.log` kept RX bandwidth high but increased disconnect/timeout churn, produced three body/receipt pipeline failure lines, and hit a 45s plan timeout.
  - The change was reverted and the remote client was restored to the accepted scheduler in `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-094842.log`.
- Rejected broadening the decoupled dense prefix request path:
  - The run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-095322.log` increased buffered prepared work but reduced useful progress to ~258k average logs/sec and ~221k p50, with 285 parallel request failures.
  - The change was reverted because it filled the prepared queue ahead of ordered writes while active fetch utilization fell; the accepted scheduler was restored on the remote client.
- Rejected stopping decoupled dense role downloads at the accepted prefix:
  - The run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-101621.log` used ~213 Mbps average RX but only reached ~248k average progress and ~198k p50, with 22 residual batches in a short run.
  - The change was reverted because it spent bandwidth on more partial-prefix/residual churn instead of improving verified ingestion.
- Rejected single-shot paired body/receipt chunk attempts:
  - The run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-103452.log` reduced the worst paired-plan tail but did not materially improve batch throughput, worsened paired-plan p50, and increased pipeline failure lines.
  - The change was reverted because it moved retry work to the scheduler without enough evidence of better end-to-end sync time.
- Rejected the role-level paired body/receipt scheduler:
  - Candidate run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-105311.log`: average ~60k logs/sec, p50 ~55k, p90 ~108k while still using ~198 Mbps average RX.
  - Restored accepted build `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-110035.log`: recovery sample averaged ~424k logs/sec, p50 ~436k, p90 ~623k at ~205 Mbps average RX.
  - The change was reverted because it reused partial body/receipt halves but produced small contiguous prefixes, high timeout pressure, and poor useful ingestion per downloaded byte.
- Rejected near-full decoupled dense prefix acceptance:
  - Accepted-control run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-110035.log`: average ~465k logs/sec, p50 ~473k, p90 ~741k at ~235 Mbps average RX.
  - Candidate run `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260624-111704.log`: average ~285k logs/sec, p50 ~286k, p90 ~488k at ~188 Mbps average RX.
  - The change was reverted because accepting a 64-block tail gap reduced active useful work and worsened paired-plan latency instead of avoiding expensive fallback.

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
- Dense low-peer lookahead now expands to six active fetch windows after eight serving peers are available. This uses idle bandwidth earlier in warm peer pools without the failure rate increase seen from raising per-peer request fanout.
- Healthy-memory historical prepare buffering can hold up to 12 completed fetches, while low-memory refill behavior remains at the smaller prepare lookahead floor. Live testing showed this modestly improves write/download overlap without creating the prepared-work backlog seen in the rejected decoupled request experiment.
- Body/receipt chunk attempts preserve the performance-sorted peer order and use in-plan load balancing inside that order. This favors measured faster peers for prefix-critical chunks while still spreading requests as per-peer in-flight counts rise.
- The optimization target is sustained use of the available 300/300 Mbps link with low idle time, not maximizing brief logs/sec peaks.
- Candidate performance is judged by useful verified ingestion per network budget. A scheduler that keeps RX high but lowers contiguous floor progress is a regression even if it increases request concurrency.
- Benchmark runs should use fresh recent checkpoint quorum sources when the default endpoint is stale; stale checkpoint rejection must not be bypassed for tests.
- Transient request transport failures (`Disconnected`, `ChannelClosed`, `ConnectionDropped`) pause and demote the peer for that request kind instead of forcing immediate local peer removal. Bad protocol responses and unsupported capabilities still receive strict reputation penalties and are dropped.
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
- Challenge: the accepted prepare-buffer experiment improved overlap but did not keep active downloads full with 20+ serving peers.
  - Resolution: kept the small buffer increase because it did not regress safety or low-memory behavior.
  - Remaining: active fetch dips still require chunk-level scheduling/reassignment, not more static buffering.
- Challenge: allowing more total outstanding fetch work looked like a way to keep downloads active.
  - Resolution: reverted after live testing showed prepared backlog saturation, worse refill latency, and lower throughput.
  - Remaining: the next scheduler should track block-range status and peer speed, similar to Nethermind's pending/sent/inserted fast-block feed, instead of relying on larger buffers.
- Challenge: equal-load chunk rotation could assign prefix-critical body/receipt chunks to slower peers despite existing peer speed scoring.
  - Resolution: preserved score order inside chunk attempts and relied on in-flight counts for fairness; live testing improved warmed progress and reduced low windows.
  - Remaining: a full scheduler should still track individual block-range status and reassign slow chunks.
- Challenge: the old checkpoint endpoint returned a stale finalized slot for a fresh data dir.
  - Resolution: startup correctly rejected the stale checkpoint; the benchmark script now uses two agreeing Beacon API endpoints.
  - Remaining: implement the project's own recent-checkpoint source later, as already planned outside this PR.
- Challenge: preserving fast-peer order in the decoupled dense path looked promising but caused severe low-progress windows under live load.
  - Resolution: reverted the experiment and restored the accepted remote build.
  - Remaining: decoupled scheduling needs chunk-level reassignment/hedging rather than simply removing peer rotation.
- Challenge: lowering dense high-depth activation to use more bandwidth at 12-15 peers increased request pressure too early.
  - Resolution: reverted the experiment after a body/receipt pipeline failed below the accepted prefix.
  - Remaining: activation should become adaptive to timeout/tail-latency signals, not just a lower static peer-count threshold.
- Challenge: request failure waves could turn a transport hiccup into local peer-pool collapse.
  - Resolution: transient request transport failures now pause and demote peers instead of disconnecting them from the local peer set; live testing showed the serving pool survived timeout waves and climbed into the 40+ peer range.
  - Remaining: request-tail latency still causes ordered-prefix waits, so chunk-level reassignment remains the main scheduler task.
- Challenge: after peer retention improved, dense historical sync still underfilled active downloads at 8-15 serving peers.
  - Resolution: accepted a denser low-peer lookahead boost that keeps six fetch windows active once eight serving peers are ready; live testing improved average and median throughput with no pipeline resets.
  - Remaining: low windows still occur under timeout waves, so chunk-level reassignment remains necessary.
- Challenge: increasing bandwidth pressure can make the link busier without improving useful verified ingestion.
  - Resolution: rejected depth-8 lookahead, 350k dense-row windows, and 1.0s hedging because they raised RX, bursts, or duplicate pressure while worsening median throughput or stability.
  - Remaining: future changes should reduce peer-tail waste, not just increase bytes downloaded.
- Challenge: removing the decoupled dense path simplified request flow but made body/receipt plan tails and prepared backlog worse.
  - Resolution: restored the accepted decoupled path after live testing showed paired-only scheduling trailed the accepted baseline despite a healthy serving-peer count.
  - Remaining: replace static whole-window request plans with a live scheduler that can reassign slow chunks without over-buffering prepared work.
- Challenge: pre-hedging later paired-plan prefix chunks looked like a narrow way to smooth stalls.
  - Resolution: reverted after live testing showed extra duplicate pressure, more disconnect churn, and body/receipt plan failures instead of better sustained progress.
  - Remaining: implement true queue/reservation reassignment rather than adding more duplicate in-flight requests to the current batch planner.
- Challenge: broadening the decoupled prefix request path downloaded more ahead-of-prefix work but did not improve verified ingestion.
  - Resolution: reverted after live testing showed prepared backlog growth, lower active fetch utilization, and worse sustained logs/sec.
  - Remaining: the scheduler needs explicit chunk reservations, expiry, and reassignment so peer-tail work can move independently without over-buffering prepared windows.
- Challenge: letting decoupled dense role downloads stop as soon as the accepted prefix was available reduced waiting in theory but increased residual churn in practice.
  - Resolution: reverted after live testing showed lower useful logs/sec despite high RX utilization.
  - Remaining: partial-prefix progress should only be used when a scheduler can keep the leftover range queued without fragmenting ordered ingestion.
- Challenge: making paired chunk attempts single-shot exposed failures to the scheduler sooner but also removed useful in-attempt fallback.
  - Resolution: reverted after live testing showed no sustained batch-throughput gain and more pipeline failure lines.
  - Remaining: a live scheduler needs per-role task state so it can reuse a successful body or receipt half instead of redownloading the whole chunk on every retry.
- Challenge: role-level body/receipt retry state looked like the next step after single-shot attempts, but the first implementation converted bandwidth into many partial chunks rather than contiguous verified progress.
  - Resolution: reverted after live testing showed similar RX usage to the accepted build with much lower logs/sec and more timeout pressure.
  - Remaining: future scheduler work should reserve prefix-critical chunks, reassign stale work, and keep a contiguous-progress budget before increasing role-level concurrency.
- Challenge: accepting near-full decoupled prefixes looked like a bounded way to avoid refetching one missing dense tail chunk.
  - Resolution: reverted after live testing showed lower useful throughput, lower RX, and worse paired-plan latency than the accepted control.
  - Remaining: residual-tail reuse should be part of a real reservation queue, not a relaxed acceptance threshold that fragments the ordered pipeline.

## Dead Code and Obsolescence Cleanup

- Inspected `crates/logex-sync/src/engine/anchored.rs`; rejected deeper dense low-peer lookahead was reverted to 4 active fetches, while the accepted separate dense activation threshold remains.
- Inspected `crates/logex-sync/src/engine/anchored.rs`; rejected dense lookahead depth 8 was replaced by accepted depth 7.
- Inspected `crates/logex-sync/src/engine/anchored.rs`; rejected 20s retry, larger fetch buffer, timed duplicate prefix hedge, and unguarded critical-refill experiments were reverted or replaced by the bounded write-refill implementation.
- Inspected `crates/logex-sync/src/engine/anchored.rs` and `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected depth-8 and 32-block dense chunk retests were reverted and the remote client was restored to the accepted depth-7 / 48-block dense chunk build.
- Inspected `crates/logex-sync/src/p2p/peer_manager/mod.rs`; rejected shorter request-pause experiment was reverted.
- Inspected `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected 2s fanout timeout, partial-prefix early return, and 384-window experiments were reverted. Duplicate failure coalescing, 16-peer fanout, the 512-window cap, and adaptive dense chunk caps remain.
- Inspected `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected lower decoupled dense peer eligibility and restored the committed request-plan baseline on the remote client.
- Inspected `crates/logex-sync/src/engine/anchored.rs`; accepted the healthy-memory prepare-buffer increase and kept the low-memory floor unchanged.
- Inspected and reverted `crates/logex-sync/src/engine/anchored.rs`; the rejected outstanding-work budget experiment left no code changes in the worktree.
- Inspected `crates/logex-sync/src/p2p/peer_manager/requests.rs`; accepted performance-order chunk peer assignment and updated the stale rotation-focused test.
- Inspected `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected and reverted the decoupled dense fast-peer-order experiment after live testing showed severe progress collapse.
- Inspected `crates/logex-sync/src/engine/anchored.rs`; rejected and reverted the 12-peer dense high-depth activation experiment after live testing showed a body/receipt pipeline failure.
- Inspected `crates/logex-sync/src/p2p/peer_manager/state.rs`; added a small tested classification helper for request-error disposition and kept strict protocol-error drops intact.
- Inspected `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected and reverted the unproven 36-peer fast-pool experiment and the regressing three-request fanout experiment.
- Inspected `crates/logex-sync/src/engine/anchored.rs`; accepted the dense low-peer lookahead expansion after live testing.
- Inspected and reverted `crates/logex-sync/src/engine/anchored.rs`; the rejected depth-8 and 350k dense-row experiments left no code changes in the worktree or remote build.
- Inspected and reverted `crates/logex-sync/src/p2p/peer_manager/requests.rs`; the rejected 1.0s hedge-delay experiment left no code changes in the worktree or remote build.
- Inspected and reverted `crates/logex-sync/src/p2p/peer_manager/requests.rs`; the rejected paired-only experiment left the accepted decoupled dense body/receipt path enabled locally and on the remote build.
- Inspected and reverted `crates/logex-sync/src/p2p/peer_manager/requests.rs`; the rejected paired-plan lookahead hedge experiment left no code changes locally or on the remote build.
- Inspected and reverted `crates/logex-sync/src/p2p/peer_manager/requests.rs`; the rejected broad decoupled-prefix experiment left no code changes locally or on the remote build.
- Inspected and reverted `crates/logex-sync/src/p2p/peer_manager/requests.rs`; the rejected accepted-prefix early-stop experiment left no code changes locally or on the remote build.
- Inspected and reverted `crates/logex-sync/src/p2p/peer_manager/requests.rs`; the rejected single-shot paired-attempt experiment left no code changes locally or on the remote build.
- Inspected and reverted `crates/logex-sync/src/p2p/peer_manager/requests.rs`; the rejected role-level paired scheduler experiment left no code changes locally or on the remote build.
- Inspected and reverted `crates/logex-sync/src/p2p/peer_manager/requests.rs`; the rejected near-full decoupled prefix experiment left no code changes locally or on the remote build.
- No obsolete experiment code remains in the local worktree.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`
- New branch created this run: no
- Commits made during this run: checkpoint commit for duplicate failure coalescing and 16-peer fanout; accepted 512-block dense window cap; accepted dense lookahead depth 7 after live benchmarking; accepted adaptive dense chunk caps; accepted bounded write-time refill and sequence ownership hardening; accepted direct write-time refill; accepted separate 16-peer dense pipeline activation; accepted healthy-memory prepare-buffer expansion; accepted performance-ordered body/receipt chunk peer assignment; roadmap update for full-sync backup rotation, checkpoint source replacement, rejected decoupled dense experiment, rejected 12-peer dense activation experiment, transient transport soft-failure handling, dense low-peer lookahead expansion, rejected paired-plan lookahead hedge, rejected broad decoupled-prefix testing, rejected accepted-prefix early-stop testing, rejected single-shot paired-attempt testing, rejected role-level paired scheduler testing, and rejected near-full decoupled prefix testing.
- Pull request status: draft PR #96 remains open for scheduler work.
- Merge status: not merged; throughput target and scheduler work remain incomplete.
- Validation run this pass: `cargo fmt --check`; `cargo check -p logex-sync`; `cargo test -p logex-sync`; `cargo clippy -p logex-sync -- -D warnings`; multiple remote release builds and live Mac mini benchmark samples. The rejected decoupled experiment passed local `cargo fmt --check` and `cargo test -p logex-sync` before live testing, then was reverted. The transport-error retention fix passed `cargo fmt --check`, `cargo test -p logex-sync`, and `cargo clippy -p logex-sync -- -D warnings`, then was deployed to the Mac mini for a live benchmark. The dense low-peer lookahead expansion passed `cargo fmt --check` and `cargo test -p logex-sync`, then was live-tested on the Mac mini. The rejected broad decoupled-prefix experiment passed `cargo fmt --check` and targeted `cargo test -p logex-sync` coverage before live testing, then was reverted. The rejected accepted-prefix early-stop experiment passed `cargo fmt --check`, `cargo test -p logex-sync decoupled_dense`, and `cargo test -p logex-sync body_receipt` before live testing, then was reverted. The rejected single-shot paired-attempt experiment passed `cargo fmt --check` and `cargo test -p logex-sync body_receipt` before live testing, then was reverted. The rejected role-level paired scheduler experiment passed `cargo fmt --check`, `cargo test -p logex-sync`, and `cargo clippy -p logex-sync -- -D warnings` before live testing, then was reverted locally and on the remote. The rejected near-full decoupled prefix experiment passed `cargo fmt --check`, `cargo test -p logex-sync`, and `cargo clippy -p logex-sync -- -D warnings` before live testing, then was reverted locally and on the remote.
- Blockers: no external blocker; the remaining work is architectural scheduler work.

## Known Issues or Risks

- Historical sync is still peer-tail bound and can show low-throughput windows even when active fetches are full.
- Current performance is sensitive to block log density, peer mix, warm-up state, and the 300/300 Mbps network link; short samples and logs/sec alone can be misleading.
- Full VPS routing is currently required for useful P2P benchmarking, but it has VPS bandwidth cost.
- A larger scheduler refactor is likely required to reach the target full-sync time.
