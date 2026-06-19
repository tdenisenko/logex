# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput-v3`.

Historical sync performance work is focused on body/receipt fetch tail latency. Preserved Mac mini logs confirm earlier runs reached 800k+ logs/sec bursts before the later request-cooldown and early-prefix experiments. Those experiments have been removed from the working tree; relative to `master`, the remaining code change is the scheduler refill fix that keeps historical fetch lookahead populated after prepared batches are ingested.

Live benchmarking is currently blocked by the Mac mini WireGuard tunnel: LogEx is running locally, but the VPS cannot reach `10.66.0.2:18683`, the Mac cannot ping `10.66.0.1`, and the client has zero EL peers. Throughput samples are not meaningful until the tunnel is healthy again.

## Completed Since Last Run

- Investigated the performance regression against preserved remote logs.
- Confirmed prior high-throughput samples reached roughly 800k-970k logs/sec bursts.
- Removed the unproven shared body/receipt cooldown experiment from the current branch.
- Restored dense prefix redundancy behavior to the PR #93/master baseline.
- Kept the historical fetch-pipeline refill fix as the only performance code change relative to `master`.
- Validated the code with formatting and `logex-sync` tests.
- Staged `/Users/gremlinmaster/logex-gateway/repair-logex-wireguard-stale.sh` on the Mac mini to repair the stale VPS tunnel once sudo is available.
- Deployed and rebuilt the cleaned scheduler baseline on the Mac mini, then restarted LogEx in tmux without resetting the data directory.

## Remaining TODOs

1. Repair the Mac mini VPS tunnel and rerun live benchmarks.
   - Reason: Peer and throughput measurements are invalid while WireGuard is stale and the client has zero peers.
   - Completion criteria: VPS access to `http://10.66.0.2:18683/status` works, the Mac can reach `10.66.0.1`, peers warm normally, and the refill-only build is benchmarked in dense historical ranges.

2. Reduce body/receipt fetch tail latency.
   - Reason: Dense historical sync still stalls behind slow body/receipt request plans when peers are available but chunks complete unevenly.
   - Completion criteria: Implement and benchmark a scheduling, hedging, or peer-selection change that materially improves sustained logs/sec without increasing stalls, memory churn, or peer churn.

3. Match production-client P2P behavior more closely.
   - Reason: The target is Geth/Nethermind-class peer retention and sync performance.
   - Completion criteria: Audit relevant Geth/Nethermind peer scoring, request scheduling, and timeout behavior; apply compatible improvements only when live benchmarks justify them.

4. Complete release hardening.
   - Reason: Production readiness depends on verification safety, restart safety, and predictable operations.
   - Completion criteria: Smokes or tests cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a clean full-sync release-candidate run.

## Design Decisions

- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.
- Checkpoints are accepted only when recent relative to a checkpoint-sync endpoint. Persisted state that is too stale requires a fresh recent checkpoint.
- Keep the PR #93/master body/receipt request scheduling baseline until a live benchmark proves a replacement is better.
- Do not keep request-cooldown or earlier prefix-hedge experiments without evidence from healthy-network runs.

## Challenges and Resolutions

- Challenge: Later body/receipt scheduling experiments correlated with performance below the earlier 800k+ logs/sec bursts.
  - Resolution: Reverted those experiments locally and reduced the branch delta to the validated refill fix.

- Challenge: Completed historical prepare results could consume buffered batches without scheduling the next fetch window.
  - Resolution: Carried the next child header through prepared/written batches and refilled the fetch pipeline after completed prepare ingestion.

- Challenge: Current remote benchmark path is unhealthy.
  - Resolution: Identified stale WireGuard as the blocker, staged a repair script on the Mac mini, and confirmed remote sudo requires a password; live throughput work should resume only after the tunnel and peer discovery recover.

## Dead Code and Obsolescence Cleanup

- Inspected the historical scheduler and body/receipt request-layer changes from this branch.
- Removed the obsolete shared request cooldown state, helper functions, and tests.
- Removed the unproven low-peer prefix redundancy change by restoring the prior threshold.
- No additional dead production code was identified in the touched paths.

## Git Workflow

- Current branch: `perf/historical-sync-throughput-v3`
- New branch created this run: no
- Commits made during this run: `1efa95e` (`fix: restore historical sync scheduling baseline`), `d9f041a` (`docs: record historical sync regression status`)
- Pull request status: not created
- Merge status: not merged
- Blockers: live benchmarking is blocked by the unhealthy WireGuard tunnel; the repair script requires local sudo on the Mac mini.

## Known Issues or Risks

- Historical sync still needs a healthy-network benchmark before this branch can be considered ready.
- The current performance target remains 800k+ logs/sec in dense ranges and materially shorter full-sync time.
- The Mac mini network mode must provide both VPS public P2P/dashboard access and a reliable management path before long benchmarks are useful.
- The Mac mini is currently running the cleaned build, but EL peers remain at zero until the VPS tunnel is repaired.
