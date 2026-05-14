# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, follows CL head/finality over P2P, uses CL-verified execution headers as the EL pivot, then syncs EL forward to head and backward toward genesis. EL historical sync verifies header ancestry, bodies, receipt roots, cumulative gas, and log blooms without executing the EVM. Queryable log coverage expands as verified segments are stored.

Active branch: `feature/el-reverse-sync` / draft PR #76. The remote test client is running on `root@165.22.64.42` with HTTP on `18683` and data in `/var/lib/logex/mainnet`.

The latest remote build is sustaining the improved EL reverse-sync pipeline. After restart warm-up, `/status` reported roughly 735 historical blocks/sec, 269k logs/sec, a log-based historical ETA near 3 hours, and zero raw log segment backlog. Peer retention is currently good enough for the observed pace; the remaining limiter is the combined fetch/process pipeline around body/receipt latency, receipt-root validation, log extraction, and compacted storage writes.

## Completed Since Last Run

- Deployed the kept EL performance changes to the remote client:
  - Linux/glibc builds use jemalloc for the node runtime.
  - Dense historical validation/extraction work is split by estimated transaction/log work instead of only block count.
  - Sparse low-log-density ranges can use deeper fetch lookahead when peers and memory are healthy.
- Reverted the higher body/receipt request cap experiment after it reduced throughput.
- Added historical logs/sec tracking to sync status and dashboard metrics.
- Changed historical ETA to prefer estimated remaining logs divided by logs/sec, using the known total of `6,780,563,686` logs through block `25,093,066` and `733` logs/block above that reference point.
- Changed the dashboard’s main rate and performance chart to logs/sec while keeping block/sec in advanced metrics.
- Fixed a flaky storage-metrics test that assumed filesystem free-space probes are byte-identical during a test run.
- Validated with `cargo test -p logex-server --lib`, `cargo test -p logex-sync --lib`, and `cargo clippy -p logex-node -p logex-sync -p logex-server --all-targets -- -D warnings`.

## Remaining TODOs

1. Finish EL historical sync production validation.
   - Reason: EL validation target is genesis, including pre-Merge blocks.
   - Completion criteria: The remote run reaches genesis, continues live head tracking, and restart/resume remains correct across the post-Merge, Merge, pre-Merge, and genesis ranges.

2. Continue performance work only where measurements show meaningful upside.
   - Reason: The target is a predictable full-history sync under 6 hours on adequate hardware without destabilizing memory, disk, or peer behavior.
   - Completion criteria: Logs/sec ETA stays below target after warm-up, serving-peer collapse does not recur, and any new optimization is kept only if it improves measured logs/sec or stability.

3. Replace the temporary checkpoint source and stale-checkpoint policy.
   - Reason: Weak-subjectivity safety requires a recent checkpoint and clear stale-checkpoint rejection.
   - Completion criteria: LogEx has its own recent-checkpoint source or verified multi-source flow, and stale checkpoints force a fresh checkpointed resync.

4. Complete release validation and hardening.
   - Reason: Trustless log validity depends on correct verification, storage canonicality, query limits, auth, and shutdown behavior.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, query caps/pagination, and exposed listener safety.

## Design Decisions

- CL sync is forward-only from a recent checkpoint. EL historical sync walks backward from the CL-authenticated pivot to genesis.
- Logs are valid only inside the verified contiguous stored range.
- Historical EL validation verifies parent-hash ancestry, body commitments, receipt roots, cumulative gas, and logs bloom against each header.
- Historical ETA is log-based when log-rate data is available; block/sec remains an advanced diagnostic because block density varies heavily across history.
- Query responses keep a hard `10,000` row cap and dashboard pagination defaults to `50` rows.
- Historical storage writes sealed compacted segments directly, avoiding raw segment buildup during normal reverse sync.
- Dashboard storage uses the normal user model: one data directory, one writable disk-free value. Multi-volume server hacks are not part of the main UI.
- Historical fetch windows scale by serving peers, memory, and observed log density. Experiments that improve one range but regress RSS, peer usefulness, or logs/sec should be reverted.

## Challenges and Resolutions

- Challenge: Block/sec made ETA misleading because older blocks are much less log-dense than recent blocks.
  - Resolution: Added logs/sec tracking and a log-count based ETA estimate.

- Challenge: A higher body/receipt request cap appeared useful but collapsed throughput during the live run.
  - Resolution: Reverted it to the known-good cap and kept the service on the stable path.

- Challenge: Dense batches spent avoidable time in task scheduling and allocation.
  - Resolution: Validation/extraction jobs are now grouped by estimated work, reducing fragmentation without changing cryptographic verification.

- Challenge: Dense historical batches left high RSS after data was freed.
  - Resolution: Linux/glibc node builds now use jemalloc, which removed allocator churn from the measured hot path and reduced RSS in remote samples.

- Challenge: A storage-metrics test compared two live filesystem free-space probes exactly.
  - Resolution: The test now allows a small tolerance while preserving the same semantic checks.

## Dead Code and Obsolescence Cleanup

- Removed/reverted the higher body/receipt request-cap experiment because it hurt the remote run.
- Inspected the status/dashboard performance path and versioned the browser performance sample key so old block/sec samples are not reused as logs/sec samples.
- Inspected storage-metrics tests after validation failure and removed the brittle exact free-space comparison.
- No additional obsolete EL sync paths were removed in this pass; remaining changes are active code paths used by the remote run.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: none; continuing the EL reverse-sync PR branch.
- Commits made during this run: `perf: trim allocator during dense historical sync`; `perf: relax dense historical window cap`; `perf: reduce dense validation task fragmentation`; `perf: boost sparse historical lookahead`; pending checkpoint for log-based ETA/dashboard status.
- Pull request status: draft PR #76 remains open.
- Merge status: not ready; EL production validation through genesis and final performance review remain incomplete.
- Blockers: none known.

## Known Issues or Risks

- The latest deployed build still needs to run through Merge, pre-Merge, and genesis on the remote server.
- The log-count ETA uses a reference total and recent-block average above block `25,093,066`; it is better than block/sec ETA but still an estimate until exact persisted rows approach the target range.
- Full sync performance is still sensitive to body/receipt response latency and dense receipt/log processing.
- Extra server volumes are a test-environment workaround and not a product storage allocator.
- HTTP Basic auth is not transport encryption; public deployments need localhost binding, firewalling, SSH tunneling, or TLS termination.
- gRPC exposure still needs a clear auth/bind/disable policy before public deployment.
- Verification-critical security review is still required before a production-ready release.
