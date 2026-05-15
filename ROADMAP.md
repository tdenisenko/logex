# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, follows CL head/finality over P2P, uses CL-verified execution headers as the EL pivot, then syncs EL forward to head and backward toward genesis. EL historical sync verifies header ancestry, bodies, receipt roots, cumulative gas, and log blooms without executing the EVM. Queryable log coverage expands as verified segments are stored.

Active branch: `feature/el-reverse-sync` / PR #76. The remote test client is running on `root@165.22.64.42` with HTTP on `18683` and data in `/var/lib/logex/mainnet`.

The remote EL validation run reached genesis, kept live head tracking afterward, and survived a graceful service restart with historical floor still at `0`. Warmed samples held strong peer retention, zero raw compression backlog, and roughly 300k-450k historical logs/sec in dense ranges, then accelerated across sparse pre-Merge history. CPU profiles show the remaining hot path is mostly required receipt verification work, especially receipt-root Keccak. The two extra mounted volumes are being used for a machine-specific symlink relocation of sealed historical segments; this is not product storage behavior.

## Completed Since Last Run

- Sampled the live remote client after the latest EL performance deploy. Peer retention, live head tracking, compression backlog, and active-disk headroom remain healthy.
- Profiled the current hot path and confirmed the dominant remaining cost is receipt/body validation, not peer count, disk I/O, or raw-segment compression.
- Switched storage dictionary compression from the standard randomized hasher to `FxHashMap` for per-segment address/topic dictionary building.
- Aligned the paired body/receipt pipeline return cap with the 10,000-block medium/sparse historical fetch window so widened sparse windows are actually consumed.
- Deployed the widened sparse-window return cap to the remote run; the first post-warm sample improved to about 560k logs/sec and roughly 2.05h ETA with peer warm-up still in progress.
- Normalized dashboard CPU utilization by logical core capacity while keeping raw process CPU in advanced status data.
- Added local completion time beside the historical sync time remaining.
- Added a sparse-range receipt validation fast path for empty receipt sets, avoiding generic trie construction while still enforcing gas, empty receipt root, and zero logs bloom.
- Fixed the GitHub test failure in historical ingest coalescing by making the row-limit test deterministic across different CI runner memory sizes.
- Reverted the uncommitted depth-6 fetch-pipeline trial after warmed samples failed to beat the known depth-5 baseline.
- Relocated 6,000 sealed remote segment directories onto the mounted test volumes and replaced them with symlinks, restoring root write headroom without touching the active hot segment.
- Completed the remote EL historical sync to genesis, confirmed live head tracking continued, and verified restart/resume after completion.
- Added a local fresh-run monitor script and runbook under `/private/tmp` for future destructive benchmark runs that wipe the remote data directory, prepare extra volumes, monitor sync to genesis, and write a report.
- Validated the branch with the CI commands `cargo fmt --all -- --check`, `cargo check --workspace`, `cargo clippy --workspace -- -D warnings`, and `cargo test --workspace`; targeted server checks also covered the new CPU metric and ETA display data path.

## Remaining TODOs

1. Replace the temporary checkpoint source and stale-checkpoint policy.
   - Reason: Weak-subjectivity safety requires a recent checkpoint and clear stale-checkpoint rejection.
   - Completion criteria: LogEx has its own recent-checkpoint source or verified multi-source flow, and stale checkpoints force a fresh checkpointed resync.

2. Complete release validation and hardening.
   - Reason: Trustless log validity depends on correct verification, storage canonicality, query limits, auth, graceful shutdown, and exposed listener safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, query caps/pagination, and public deployment safety.

## Design Decisions

- CL sync is forward-only from a recent checkpoint. EL historical sync walks backward from the CL-authenticated pivot to genesis.
- Logs are valid only inside the verified contiguous stored range.
- Historical EL validation verifies parent-hash ancestry, body commitments, receipt roots, cumulative gas, and logs bloom against each header.
- Historical chunks whose headers prove empty transaction, receipt, ommer, and withdrawal roots can be ingested header-only because the empty body and receipt tries are uniquely determined by those roots.
- Historical ETA is log-based when log-rate data is available; block/sec remains an advanced diagnostic because block density varies heavily across history.
- Dashboard CPU is shown as capacity utilization across logical CPUs; raw multi-core process CPU remains available in advanced status data.
- Query responses keep a hard `10,000` row cap and dashboard pagination defaults to `50` rows.
- Historical storage writes sealed compacted segments directly, avoiding raw segment buildup during normal reverse sync.
- Dashboard storage uses the normal user model: one data directory, one writable disk-free value. Multi-volume server hacks are not part of the main UI.
- Historical fetch windows scale by serving peers, memory, and observed log density. Experiments that improve one range but regress RSS, peer usefulness, or logs/sec should be reverted.

## Challenges and Resolutions

- Challenge: Block/sec made ETA misleading because older blocks are much less log-dense than recent blocks.
  - Resolution: Added logs/sec tracking and a log-count based ETA estimate.

- Challenge: Dense historical validation spent avoidable CPU rebuilding repeated receipt-bloom components.
  - Resolution: Added a bounded receipt-bloom cache for eth/69 and eth/70 responses while preserving receipt-root and logs-bloom verification.

- Challenge: Lower-log-density pre-Merge ranges make per-batch overhead more visible.
  - Resolution: Historical fetch windows now adapt to peer count, memory, and observed log density, and the body/receipt pipeline can return the widened sparse-window range.

- Challenge: Profiling after the latest deploy still showed small standard-hasher overhead in storage dictionary compression.
  - Resolution: Switched the hot per-segment dictionary maps to `FxHashMap`.

- Challenge: Sparse historical ranges still pay fixed validation overhead for empty receipt sets inside otherwise non-empty batches.
  - Resolution: Added a direct empty-root/zero-bloom validation path for empty receipts.

- Challenge: The historical ingest coalescing test depended on the host's available memory, so GitHub's higher-memory runner used a larger row threshold than the local machine.
  - Resolution: Added an explicit row-limit coalescing helper for deterministic unit coverage while leaving the production memory-adaptive limit intact.

- Challenge: The full-history run needed more disk headroom than the primary remote volume alone could provide.
  - Resolution: Kept the product UI/data-dir model single-disk oriented and used a test-machine-only sealed-segment symlink relocation for extra mounted volumes.

## Dead Code and Obsolescence Cleanup

- Pruned stale temporary worktree metadata and rechecked the active performance changes against the live profile. No obsolete EL sync path was found in this pass.
- Previous reverted experiments remain out of the branch: higher body/receipt request caps, 50/50 outbound split, 64-task validation fanout, and overly deep high-memory lookahead including the uncommitted depth-6 fetch-pipeline trial.
- Remaining cleanup risk is limited to future profiling discoveries; current changed paths were exercised by the completed remote run.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: none; continuing the EL reverse-sync PR branch.
- Commits made during this run: `1c1889d`, `0fb7326`, `e6e1d1b`, `41cf8f0`, `2270dcc`, `72c6b74`, `4c8ab5f`, `d4ca40f`, plus this roadmap update.
- Pull request status: PR #76 is ready to merge after this final roadmap update is pushed and checks pass.
- Merge status: pending final push/checks.
- Blockers: none known.

## Known Issues or Risks

- The log-count ETA uses a reference total and recent-block average above block `25,093,066`; it is better than block/sec ETA but still an estimate.
- Full sync performance is now mostly sensitive to receipt verification CPU and body/receipt response latency. Further optimization should be handled in a new focused PR only if fresh-run measurements show a meaningful regression or clear upside.
- Extra server volumes are a test-environment workaround and not a product storage allocator.
- HTTP Basic auth is not transport encryption; public deployments need localhost binding, firewalling, SSH tunneling, or TLS termination.
- gRPC exposure still needs a clear auth/bind/disable policy before public deployment.
- Verification-critical security review is still required before a production-ready release.
