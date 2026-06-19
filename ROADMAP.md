# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput-v3`.

Historical sync performance work is focused on body/receipt fetch tail latency and keeping the downloader active while validation/write work drains. Preserved Mac mini logs confirm earlier runs reached 800k+ logs/sec bursts; later low-throughput runs showed fewer active fetches, more body/receipt chunks per batch, and longer body/receipt plans. The current branch removes unproven cooldown/prefix experiments, removes the failed decoupled dense body/receipt pre-pass, and keeps bounded scheduler changes that refill lookahead, rotate retries, skip serial receipt fallbacks on transport-tail failures, and keep active downloads full when completed buffers are healthy.

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
- Added a bounded receipt-fallback change for paired body/receipt chunks: transport-level receipt failures now end that chunk attempt so the outer hedge/retry scheduler can rotate peers instead of serially waiting on more receipt fallbacks inside one future.
- Validated the fallback change with `cargo fmt --all -- --check`, `cargo test -p logex-sync`, and `cargo clippy -p logex-sync --all-targets -- -D warnings`.
- Deployed and rebuilt the fallback-tail candidate on the Mac mini, then restarted LogEx in tmux without resetting the data directory.
- Added retry-ordinal peer rotation for paired, decoupled, and generic parallel chunk retries so a failed chunk retry does not select the same first peer again when the chunk count is a multiple of the peer count.
- Validated retry rotation with `cargo fmt --all -- --check`, `cargo test -p logex-sync`, and `cargo clippy -p logex-sync --all-targets -- -D warnings`.
- Deployed and rebuilt the retry-rotation candidate on the Mac mini, then restarted LogEx in tmux without resetting the data directory.
- Compared PR #93-era and current preserved logs. The strongest baseline samples reached ~970k logs/sec bursts with shorter body/receipt plans, while degraded v3 samples had lower active fetch counts and much longer plan times in dense ranges.
- Updated the healthy-memory fetch budget so completed buffered fetches do not block the scheduler from keeping the active download window full; total outstanding fetches remain bounded by buffer depth plus pipeline depth.
- Validated the active-download budget change with `cargo fmt --all -- --check`, `cargo test -p logex-sync`, and `cargo clippy -p logex-sync --all-targets -- -D warnings`.
- Deployed and rebuilt the active-download budget candidate on the Mac mini, then restarted LogEx in tmux without resetting the data directory.
- Compared retained high-throughput and degraded dense logs for request-plan shape. Degraded dense runs averaged roughly twice as many planned chunks per 512-block batch, which points at adaptive request-limit shrinkage or peer-tail retries as a likely remaining bottleneck.
- Added debug-only body/receipt plan diagnostics for planned chunk count, prefix chunk count, active in-flight cap, and min/avg/max planned chunk size so the next healthy run can identify whether request limits are over-fragmenting dense ranges.
- Validated the diagnostics change with `cargo fmt --all -- --check`, `cargo test -p logex-sync`, and `cargo clippy -p logex-sync --all-targets -- -D warnings`.
- Identified `perf: split dense body receipt fetches` as a strong regression candidate: preserved degraded logs repeatedly showed the decoupled dense pre-pass failing to produce a prefix before falling back to paired body/receipt fetching.
- Removed the decoupled dense body/receipt pre-pass and its unused helpers so dense ranges use the paired scheduler directly.
- Validated the cleanup with `cargo fmt --all -- --check`, `cargo test -p logex-sync`, and `cargo clippy -p logex-sync --all-targets -- -D warnings`.

## Remaining TODOs

1. Repair the Mac mini VPS tunnel and rerun live benchmarks.
   - Reason: Peer and throughput measurements are invalid while WireGuard is stale and the client has zero peers.
   - Completion criteria: VPS access to `http://10.66.0.2:18683/status` works, the Mac can reach `10.66.0.1`, peers warm normally, and the current scheduler candidate is benchmarked in dense historical ranges.

2. Reduce body/receipt fetch tail latency.
   - Reason: Dense historical sync still stalls behind slow body/receipt request plans when peers are available but chunks complete unevenly.
   - Completion criteria: Benchmark the paired-only dense path on a healthy run, use the new chunk diagnostics to confirm the remaining bottleneck, then implement and benchmark a scheduling, hedging, request-limit, or peer-selection change that materially improves sustained logs/sec without increasing stalls, memory churn, or peer churn.

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
- Paired body/receipt chunk attempts should not serially try extra receipt peers after timeout/disconnect/channel-close failures; those are transport-tail failures better handled by the outer hedge/retry scheduler with rotated candidates. Serial fallback remains allowed for incomplete, mismatched, or bad-protocol receipt responses where cached bodies can still avoid a full body refetch.
- Chunk retry/hedge scheduling uses retry ordinal as the peer-rotation offset rather than multiplying by the number of ranges. Multiplying by range count can repeatedly select the same first peer when `range_count % peer_count == 0`, which wastes hedges on the exact slow peer path.
- Under healthy memory, completed fetch buffers are allowed to fill without starving active downloads; the scheduler can keep up to one pipeline window active beyond the completed buffer limit, preserving a bounded memory budget while reducing spike/plunge behavior.
- Request-limit changes should wait for healthy-network diagnostics. Preserved logs show degraded dense runs over-fragmented body/receipt plans, but raising limits without live confirmation may increase peer tail timeouts.
- Remove the decoupled dense body/receipt pre-pass until a benchmark proves it helps. It was all-or-nothing, frequently failed to assemble a prefix in degraded logs, and then repeated the work through the paired fallback path. The paired scheduler is the safer baseline because retained high-throughput logs reached 800k+ logs/sec without relying on that pre-pass.

## Challenges and Resolutions

- Challenge: Later body/receipt scheduling experiments correlated with performance below the earlier 800k+ logs/sec bursts.
  - Resolution: Reverted those experiments locally and reduced the branch delta to the validated refill fix.

- Challenge: Completed historical prepare results could consume buffered batches without scheduling the next fetch window.
  - Resolution: Carried the next child header through prepared/written batches and refilled the fetch pipeline after completed prepare ingestion.

- Challenge: Current remote benchmark path is unhealthy.
  - Resolution: Identified stale WireGuard as the blocker, staged a repair script on the Mac mini, and confirmed remote sudo requires a password; live throughput work should resume only after the tunnel and peer discovery recover.

- Challenge: A paired body/receipt chunk future could spend extra time on serial receipt fallbacks after a transport-level receipt failure.
  - Resolution: Added a local fallback classifier so slow transport failures return to the outer scheduler for hedged/retry rotation, while data-shape failures can still use cached bodies.

- Challenge: Chunk retries and hedges could rotate by a multiple of the peer count and reuse the same first peer.
  - Resolution: Retried chunks now advance by retry ordinal, with tests covering bounded hedge indexes.

- Challenge: Completed historical fetches could fill the buffer and stop new downloads even when active in-flight fetches dropped below the pipeline target.
  - Resolution: Healthy-memory fetch budgeting now allows active downloads to refill until total outstanding fetches reaches buffer depth plus pipeline depth.

- Challenge: Current degraded dense logs have about twice as many planned body/receipt chunks per 512-block batch as retained high-throughput logs.
  - Resolution: Added debug diagnostics to capture planned chunk sizing and prefix scheduling in the existing plan-completion log before changing request-limit heuristics.

- Challenge: The decoupled dense body/receipt path often failed before paired fallback, creating duplicate request work and longer dense batch tails.
  - Resolution: Removed the decoupled pre-pass and its dead helper code; dense historical fetches now go straight to the paired scheduler.

## Dead Code and Obsolescence Cleanup

- Inspected the historical scheduler and body/receipt request-layer changes from this branch.
- Removed the obsolete shared request cooldown state, helper functions, and tests.
- Removed the unproven low-peer prefix redundancy change by restoring the prior threshold.
- Inspected paired body/receipt fallback behavior and kept the new change scoped to transport-tail fallback gating.
- Inspected retry-index construction across paired, decoupled, and generic parallel chunk request paths; replaced repeated multiplier expressions with one retry-index helper.
- Inspected historical fetch budget logic and updated the existing capacity helper instead of adding a parallel scheduler path.
- Inspected body/receipt chunk sizing, adaptive request limits, and local geth request scheduling references. No obsolete code was safe to remove; the next risky change is request-limit tuning and needs healthy-network evidence.
- Removed the obsolete decoupled dense body/receipt pre-pass, decoupled scheduling helpers, sourced body assembly helper, and source-count validation helper.
- No additional dead production code was identified in the touched paths.

## Git Workflow

- Current branch: `perf/historical-sync-throughput-v3`
- New branch created this run: no
- Commits made during this run: `fix: restore historical sync scheduling baseline`, `docs: record historical sync regression status`, `perf: bound receipt fallback tail latency`, `perf: rotate chunk retries by attempt`, `perf: keep historical downloads active`, `perf: add historical chunk plan diagnostics`; decoupled dense cleanup pending commit
- Pull request status: not created
- Merge status: not merged
- Blockers: live benchmarking is blocked by the unhealthy WireGuard tunnel; the repair script requires local sudo on the Mac mini.

## Known Issues or Risks

- Historical sync still needs a healthy-network benchmark before this branch can be considered ready.
- The current performance target remains 800k+ logs/sec in dense ranges and materially shorter full-sync time.
- The Mac mini network mode must provide both VPS public P2P/dashboard access and a reliable management path before long benchmarks are useful.
- The Mac mini is currently running the cleaned build, but EL peers remain at zero until the VPS tunnel is repaired.
