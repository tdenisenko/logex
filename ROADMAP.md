# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, follows CL head/finality over P2P, uses CL-verified execution headers as the EL pivot, then syncs EL forward to head and backward toward genesis. EL historical sync verifies header ancestry, bodies, receipt roots, cumulative gas, and log blooms without executing the EVM. Queryable log coverage expands as verified segments are stored.

Active branch: `feature/el-reverse-sync` / draft PR #76. The remote test client is running on `root@165.22.64.42` with HTTP on `18683` and data in `/var/lib/logex/mainnet`.

The remote run is validating EL reverse sync toward genesis with the improved pipeline. After the peer-refill hot-loop fix, warmed dense-range samples reached roughly 500k historical logs/sec on the dashboard, with a 2.3-hour remaining ETA and serving peers still warming. The dominant local cost is still receipt/body verification, especially receipt-root Keccak; body/receipt fetch latency can become the wall-clock limiter when peer warm-up is still low. The two extra remote volumes are mounted and reserved for a symlink-only emergency storage workaround if root free space gets low.

## Completed Since Last Run

- Deployed and kept the EL performance changes that improved measured logs/sec:
  - Linux/glibc builds use jemalloc for the node runtime.
  - Dense historical validation/extraction work is split by estimated transaction/log work instead of only block count.
  - Sparse low-log-density ranges can use deeper fetch lookahead when peers and memory are healthy.
  - Historical row buffers are flattened once per storage flush instead of repeatedly appended into a growing batch vector.
  - High-memory historical write chunks scale to 1m rows when Linux reports healthy available memory.
  - Historical body/receipt planning reuses already-validated header hashes instead of hashing headers again.
  - Peer refill no longer blocks each historical batch while the client already has enough serving peers to make progress; event draining still submits pending dials.
  - All-empty historical header chunks can advance the verified floor without body/receipt P2P requests when every body/receipt commitment is the canonical empty root.
  - High-memory historical fetch lookahead depth increased to 5 after the blocking refill fix made the retry beneficial.
- Kept the dense historical fetch window at 5,000 blocks after live samples improved dense-range throughput.
- Raised the medium-dense fetch lookahead cap to 4 while keeping very-dense ranges capped at 3 to avoid unnecessary memory pressure.
- Reduced normal CL light-client log churn and skipped stale finality/optimistic updates before expensive verification.
- Persisted the storage catalog once per historical batch instead of after every sealed segment; segment manifests still allow crash recovery.
- Reverted the higher body/receipt request cap and 50/50 outbound split experiments after they reduced throughput or peer warm-up.
- Rejected and reverted the 64-task validation fanout and depth-6 high-memory lookahead experiments after live logs/sec did not improve.
- Verified a completed full historical sync on the previous run, confirmed live head tracking, then started a fresh run on `root@165.22.64.42`.
- Added historical logs/sec tracking to sync status and dashboard metrics.
- Changed historical ETA to prefer estimated remaining logs divided by logs/sec, using the known total of `6,780,563,686` logs through block `25,093,066` and `733` logs/block above that reference point.
- Changed the dashboard’s main rate and performance chart to logs/sec while keeping block/sec in advanced metrics.
- Fixed a flaky storage-metrics test that assumed filesystem free-space probes are byte-identical during a test run.
- Validated the latest sync changes with `cargo test -p logex-sync --lib` and `cargo clippy -p logex-sync --all-targets -- -D warnings`. Earlier dashboard/status changes were validated with `cargo test -p logex-server --lib` and the combined node/sync/server clippy command.

## Remaining TODOs

1. Finish EL historical sync production validation.
   - Reason: EL validation target is genesis, including pre-Merge blocks.
   - Completion criteria: The remote run reaches genesis, continues live head tracking, and restart/resume remains correct across the post-Merge, Merge, pre-Merge, and genesis ranges.

2. Continue performance work only where measurements show meaningful upside.
   - Reason: The target is a predictable full-history sync near 2 hours on adequate hardware without destabilizing memory, disk, or peer behavior.
   - Completion criteria: Fresh-run logs/sec ETA approaches the 2-hour target after warm-up, serving-peer collapse does not recur, and any new optimization is kept only if it improves measured logs/sec or stability.

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
- Historical chunks whose headers prove empty transaction, receipt, ommer, and withdrawal roots can be ingested header-only because the empty body and receipt tries are uniquely determined by those roots.
- Historical ETA is log-based when log-rate data is available; block/sec remains an advanced diagnostic because block density varies heavily across history.
- Query responses keep a hard `10,000` row cap and dashboard pagination defaults to `50` rows.
- Historical storage writes sealed compacted segments directly, avoiding raw segment buildup during normal reverse sync.
- Dashboard storage uses the normal user model: one data directory, one writable disk-free value. Multi-volume server hacks are not part of the main UI.
- Historical fetch windows scale by serving peers, memory, and observed log density. Experiments that improve one range but regress RSS, peer usefulness, or logs/sec should be reverted.
- Peer refill should not block the historical hot loop once minimum useful serving capacity exists; peer discovery and dialing continue through the normal event-drain path.

## Challenges and Resolutions

- Challenge: Block/sec made ETA misleading because older blocks are much less log-dense than recent blocks.
  - Resolution: Added logs/sec tracking and a log-count based ETA estimate.

- Challenge: A higher body/receipt request cap appeared useful but collapsed throughput during the live run.
  - Resolution: Reverted it to the known-good cap and kept the service on the stable path.

- Challenge: Dense batches spent avoidable time in task scheduling and allocation.
  - Resolution: Validation/extraction jobs are now grouped by estimated work, reducing fragmentation without changing cryptographic verification.

- Challenge: Dense historical batches left high RSS after data was freed.
  - Resolution: Linux/glibc node builds now use jemalloc, which removed allocator churn from the measured hot path and reduced RSS in remote samples.

- Challenge: Historical batches were spending wall-clock time waiting for peer refill toward 80 serving peers even while enough peers were already serving data.
  - Resolution: Changed refill policy so the hot loop only blocks on peer fill below the minimum serving floor. Warmed remote status improved to roughly 500k logs/sec and batch intervals moved closer to local processing time.

- Challenge: Very old empty blocks would still require body/receipt P2P work even when their header roots already prove empty bodies and receipts.
  - Resolution: Added a guarded sequential path that advances the historical floor for all-empty header chunks without body/receipt requests.

- Challenge: The remote host may still need more effective storage than the root volume during fresh dense-range runs.
  - Resolution: Mounted the extra volumes and reserved them for moving immutable sealed segments behind symlinks if root free space falls near the safety threshold.

- Challenge: A storage-metrics test compared two live filesystem free-space probes exactly.
  - Resolution: The test now allows a small tolerance while preserving the same semantic checks.

- Challenge: CL gossip/RPC light-client updates were creating avoidable info-level log volume and duplicate verification work.
  - Resolution: Routine success/error logs are now debug-level, and stale finality/optimistic updates are ignored after decode and before signature verification.

## Dead Code and Obsolescence Cleanup

- Removed/reverted the higher body/receipt request-cap experiment because it hurt the remote run.
- Removed/reverted the 50/50 outbound split experiment because it warmed fewer useful peers than the existing split.
- Removed/reverted the 64-task validation fanout and depth-6 high-memory lookahead experiments because live logs/sec did not improve.
- Removed the now-unused header-based body/receipt planning helper after switching historical planning to reuse validated header hashes.
- Deduplicated historical peer-note collection so repeated peer IDs from the same batch are not carried through the ingest path.
- Inspected the status/dashboard performance path and versioned the browser performance sample key so old block/sec samples are not reused as logs/sec samples.
- Inspected storage-metrics tests after validation failure and removed the brittle exact free-space comparison.
- No additional obsolete EL sync paths were removed in this pass; remaining changes are active code paths used by the remote run.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: none; continuing the EL reverse-sync PR branch.
- Commits made during this run include: `20ead78`, `e7cb97b`, `2cf8120`, `4f9a84f`, `99793ce`, `09dfd57`, `fc3f815`, `5fce0ac`, `a4e599a`, `a977297`, `bd1e16a`, `b9623de`, `ffcee7f`, and `9f62d2f`.
- Pull request status: draft PR #76 remains open.
- Merge status: not ready; EL production validation through genesis and final performance review remain incomplete.
- Blockers: none known.

## Known Issues or Risks

- The latest deployed build still needs to run through genesis on the remote server, then a fresh run should measure the dense recent ranges again with the kept changes.
- The log-count ETA uses a reference total and recent-block average above block `25,093,066`; it is better than block/sec ETA but still an estimate until exact persisted rows approach the target range.
- Full sync performance is still sensitive to body/receipt response latency and dense receipt/log processing.
- Extra server volumes are a test-environment workaround and not a product storage allocator.
- HTTP Basic auth is not transport encryption; public deployments need localhost binding, firewalling, SSH tunneling, or TLS termination.
- gRPC exposure still needs a clear auth/bind/disable policy before public deployment.
- Verification-critical security review is still required before a production-ready release.
