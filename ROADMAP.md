# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, follows CL head/finality over P2P, uses CL-verified execution headers as the EL pivot, then syncs EL forward to head and backward toward genesis. EL historical sync verifies header ancestry, bodies, receipt roots, cumulative gas, and log blooms without executing the EVM. Queryable log coverage expands as verified segments are stored.

Active branch: `feature/el-reverse-sync` / draft PR #76. The remote test client is running on `root@165.22.64.42` with HTTP on `18683` and data in `/var/lib/logex/mainnet`.

The current remote run has crossed the Merge boundary and is validating pre-Merge history toward genesis. Warmed samples are holding roughly 70+ peers, zero raw compression backlog, and about 500k historical logs/sec in the current range, with ETA fluctuating by block/log density. CPU profiles show the remaining hot path is mostly required receipt verification work, especially receipt-root Keccak. The two extra mounted volumes are reserved for a machine-specific symlink relocation if root write headroom gets low; this is not product storage behavior.

## Completed Since Last Run

- Sampled the live remote client after the latest EL performance deploy. Peer retention, live head tracking, compression backlog, and active-disk headroom remain healthy.
- Profiled the current hot path and confirmed the dominant remaining cost is receipt/body validation, not peer count, disk I/O, or raw-segment compression.
- Switched storage dictionary compression from the standard randomized hasher to `FxHashMap` for per-segment address/topic dictionary building.
- Validated the storage change with `cargo fmt --check`, `cargo test -p logex-storage --lib`, and `cargo clippy -p logex-storage --all-targets -- -D warnings`.

## Remaining TODOs

1. Finish EL historical sync production validation.
   - Reason: EL validation target is genesis, including pre-Merge blocks.
   - Completion criteria: The remote run reaches genesis, continues live head tracking, and restart/resume remains correct across post-Merge, Merge, pre-Merge, and genesis ranges.

2. Continue performance work only where measurements show meaningful upside.
   - Reason: The target is a predictable full-history sync near 2 hours on adequate hardware without destabilizing memory, disk, or peer behavior.
   - Completion criteria: Fresh-run logs/sec ETA approaches the 2-hour target after warm-up, serving-peer collapse does not recur, and any new optimization is kept only if it improves measured logs/sec or stability.

3. Replace the temporary checkpoint source and stale-checkpoint policy.
   - Reason: Weak-subjectivity safety requires a recent checkpoint and clear stale-checkpoint rejection.
   - Completion criteria: LogEx has its own recent-checkpoint source or verified multi-source flow, and stale checkpoints force a fresh checkpointed resync.

4. Complete release validation and hardening.
   - Reason: Trustless log validity depends on correct verification, storage canonicality, query limits, auth, graceful shutdown, and exposed listener safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, query caps/pagination, and public deployment safety.

## Design Decisions

- CL sync is forward-only from a recent checkpoint. EL historical sync walks backward from the CL-authenticated pivot to genesis.
- Logs are valid only inside the verified contiguous stored range.
- Historical EL validation verifies parent-hash ancestry, body commitments, receipt roots, cumulative gas, and logs bloom against each header.
- Historical chunks whose headers prove empty transaction, receipt, ommer, and withdrawal roots can be ingested header-only because the empty body and receipt tries are uniquely determined by those roots.
- Historical ETA is log-based when log-rate data is available; block/sec remains an advanced diagnostic because block density varies heavily across history.
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
  - Resolution: Historical fetch windows now adapt to peer count, memory, and observed log density.

- Challenge: Profiling after the latest deploy still showed small standard-hasher overhead in storage dictionary compression.
  - Resolution: Switched the hot per-segment dictionary maps to `FxHashMap`.

## Dead Code and Obsolescence Cleanup

- Rechecked the active performance changes against the live profile. No obsolete EL sync path was removed in this pass.
- Previous reverted experiments remain out of the branch: higher body/receipt request caps, 50/50 outbound split, 64-task validation fanout, and overly deep high-memory lookahead.
- Remaining cleanup risk is limited to future profiling discoveries; current changed paths are active in the remote run.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: none; continuing the EL reverse-sync PR branch.
- Commits made during this run: pending local commit for storage dictionary hashing and roadmap cleanup.
- Pull request status: draft PR #76 remains open.
- Merge status: not ready; EL production validation through genesis and final performance review remain incomplete.
- Blockers: none known.

## Known Issues or Risks

- The latest deployed build still needs to run through genesis on the remote server, then a fresh run should measure dense recent ranges again.
- The log-count ETA uses a reference total and recent-block average above block `25,093,066`; it is better than block/sec ETA but still an estimate.
- Full sync performance is now mostly sensitive to receipt verification CPU and body/receipt response latency.
- Extra server volumes are a test-environment workaround and not a product storage allocator.
- HTTP Basic auth is not transport encryption; public deployments need localhost binding, firewalling, SSH tunneling, or TLS termination.
- gRPC exposure still needs a clear auth/bind/disable policy before public deployment.
- Verification-critical security review is still required before a production-ready release.
