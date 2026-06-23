# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, and serves verified logs through the dashboard/query APIs. PR #95 is merged as the current `master` baseline. This branch, `perf/historical-sync-live-scheduler`, is the active historical sync scheduler/performance pass.

The Mac mini client is running from `/Volumes/SSD 4TB/LogEx` through the full VPS route for useful P2P coverage. The accepted branch state has improved dense historical sync by reducing head-of-line stalls, avoiding sparse decoupled receipt fetches, increasing dense low-peer lookahead to six active fetches, and retrying stalled expected fetches sooner when lookahead has completed. The latest accepted experiment run is `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260623-012756.log`.

## Completed Since Last Run

- Confirmed full VPS routing and public NAT behavior after the network handoff; the remote client advertises `157.245.195.72` for EL/CL P2P and the dashboard remains on port `18683`.
- Benchmarked the referenced high-throughput run at `/Users/gremlinmaster/logex-src/run/logex-pr96-spec-prepare-fresh-20260622-154837.log`:
  - Recent dense range: p50 ~482k logs/sec, p90 ~702k, max ~1.01m.
  - Current lower-density range is not directly comparable by logs/sec alone because rows/block are roughly half the dense-run value.
- Kept the existing accepted scheduler improvement already committed as `57aa4a1 perf: deepen dense historical fetch lookahead`.
- Tested and rejected three additional experiments:
  - Larger dense fetch payloads (`500k` target rows, `1536` max blocks) caused long active-fetch stalls and peer readiness drops.
  - Longer global body/receipt chunk timeouts (`3s` and `2.5s`) reduced some prefix failures but lowered sustained p50 throughput or created zero-progress windows.
  - High-peer-only dense depth `8` did not activate in the measured run and produced no proven improvement.
- Restored the remote Mac mini client and local worktree to the accepted baseline after each rejected experiment.
- Accepted an earlier historical head-of-line retry threshold:
  - The expected fetch is now retried after `4s` once at least two later fetches have completed.
  - In the current resumed range this raised progress p50 from ~333k to ~360k logs/sec, p95 from ~600k to ~795k logs/sec, lowered prefix fallback markers, and reduced residual-block repair work.

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
- Static global timeout increases are rejected for now; future timeout work should be adaptive per peer/request kind.
- Earlier expected-fetch retries are worthwhile because they preserve ordered ingestion while reducing time spent waiting for one stale prefix when later lookahead already completed.
- The next meaningful path remains a geth/Nethermind-style live scheduler with peer allocation, reassignment, and measured peer speed, not broad static timeout or payload-size changes.

## Challenges and Resolutions

- Challenge: the referenced 1m logs/sec run occurred in a much denser block range than the current resumed run.
  - Resolution: compared rows/block, body/receipt latency, active fetch depth, failures, and serving peer counts instead of judging by logs/sec alone.
  - Remaining: future benchmark reports should normalize by density or include both logs/sec and block/sec.
- Challenge: larger dense fetch payloads looked like a way to amortize slow peer round trips.
  - Resolution: reverted after live testing showed long active-fetch stalls and readiness drops.
  - Remaining: larger payloads should only be reconsidered inside a live scheduler that can reassign slow chunks.
- Challenge: longer request timeouts reduced some hard prefix failures but made slow peers hold work longer.
  - Resolution: reverted `3s` and `2.5s` timeout experiments and restored the accepted `2s` timeout.
  - Remaining: implement adaptive per-peer timeout/backoff if further evidence supports it.
- Challenge: high-peer depth `8` could not be proven because the run did not reach the activation threshold.
  - Resolution: reverted the unproven change rather than leaving speculative code in the branch.
  - Remaining: retest higher depth only with a controlled run that actually reaches high serving-peer counts.
- Challenge: the expected historical fetch can block the contiguous floor while later lookahead is complete.
  - Resolution: accepted an earlier selective retry of only the expected fetch after live benchmarking showed better sustained progress and fewer fallback/residual repairs.
  - Remaining: a live scheduler should reduce this further by reassigning stale chunks instead of retrying whole fetch windows.

## Dead Code and Obsolescence Cleanup

- Inspected `crates/logex-sync/src/engine/anchored.rs`; rejected dense-window and high-peer-depth experiments were reverted locally and remotely, and the accepted head-of-line retry threshold remains.
- Inspected `crates/logex-sync/src/p2p/peer_manager/requests.rs`; rejected `3s` and `2.5s` timeout experiments were reverted locally and remotely.
- No additional dead production code was removed in this pass; no rejected experiment code remains in the local worktree.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`
- New branch created this run: no
- Commits made during this run: roadmap checkpoint documenting rejected experiments; accepted head-of-line retry threshold.
- Pull request status: draft PR #96 remains open for scheduler work.
- Merge status: not merged; throughput target and scheduler work remain incomplete.
- Validation run this pass: `cargo fmt --check`; `cargo check -p logex-sync`; `cargo test -p logex-sync`; multiple remote release builds and live Mac mini benchmark samples.
- Blockers: no external blocker; the remaining work is architectural scheduler work.

## Known Issues or Risks

- Historical sync is still peer-tail bound and can show low-throughput windows even when active fetches are full.
- Current performance is sensitive to block log density, peer mix, and warm-up state; short samples can be misleading.
- Full VPS routing is currently required for useful P2P benchmarking, but it has VPS bandwidth cost.
- A larger scheduler refactor is likely required to reach the target full-sync time.
