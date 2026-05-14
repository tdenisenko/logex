# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, follows CL head/finality over P2P, uses CL-verified execution headers as the EL pivot, then syncs EL forward to head and backward toward genesis. EL historical sync verifies header ancestry, bodies, receipt roots, and log blooms without executing the EVM, and queryable log coverage expands as verified segments are stored.

Active branch: `feature/el-reverse-sync` / draft PR #76. The remote test client is running on `root@165.22.64.42` with HTTP on `18683` and data in `/var/lib/logex/mainnet`.

The latest stable direction is:
- Peer retention is no longer the primary limiter once the pool warms.
- Disk writes are improved by direct historical compaction and faster adaptive byte-page compression.
- Remaining performance work is mostly body/receipt fetch overlap, useful serving-peer ramp, receipt-root validation, and dense log extraction/storage CPU.
- The dashboard should present normal single-disk semantics: main `Disk free` means the filesystem that receives new LogEx data. Extra mounted test volumes are operational details, not user-facing product metrics.

## Completed Since Last Run

- Changed dashboard storage display so the UI shows only product-relevant storage metrics: storage used, main data-disk free space, and the data directory path.
- Changed `/status.disk_free_bytes` to report the writable data directory filesystem rather than auxiliary mounted volume totals or limiting multi-volume headroom.
- Kept hidden status diagnostics for operators, but removed multi-volume labels from the dashboard.
- Removed the redundant advanced storage free-space row so the UI has one user-facing `Disk free` metric.
- Optimized adaptive variable-byte page compression by using 32-bit offsets directly when the page fits, avoiding a redundant zstd pass over normal log data pages.
- Added a Linux-only allocator trim under sustained historical-sync memory pressure to help return freed dense-batch arenas to the OS without changing sync correctness.
- Raised the high-memory dense historical window cap from 2,048 to 4,096 blocks so recent dense ranges need fewer pipeline turns while low-memory backoff still forces 1,024-block windows.
- Deployed the compression/storage UI changes to the remote client and confirmed the service restarts gracefully with storage integrity passing.
- Tested a deeper dense fetch pipeline; it increased RSS and worsened ETA, so it was reverted.
- Tested prefetch cancellation and write-buffer preallocation; both were reverted because they reduced lookahead or increased memory pressure without improving ETA.

## Remaining TODOs

1. Finish EL historical sync performance work.
   - Reason: Production sync needs predictable full-history completion on adequate hardware.
   - Completion criteria: Fresh remote runs sustain the target pace without peer collapse, OOM risk, low-disk stalls, or repeated request gaps.

2. Stabilize serving-peer ramp and body/receipt throughput.
   - Reason: Connected peers are only useful if enough can serve bodies and receipts fast enough to keep lookahead filled.
   - Completion criteria: Warm runs consistently keep a large serving pool, fetch gaps are bounded, and ETA remains stable instead of oscillating after restarts.

3. Confirm full pre-Merge and genesis-range behavior.
   - Reason: EL validation target is genesis; CL only authenticates the post-Merge pivot and live head.
   - Completion criteria: Reverse EL sync validates ancestry, bodies, receipts, logs, query coverage, and restart/resume across Merge and pre-Merge ranges.

4. Replace the temporary checkpoint source and stale-checkpoint policy.
   - Reason: Weak-subjectivity safety requires a recent checkpoint and clear stale-checkpoint rejection.
   - Completion criteria: LogEx has its own recent-checkpoint source or verified multi-source flow, and stale checkpoints force a fresh checkpointed resync.

5. Complete release validation and hardening.
   - Reason: Trustless log validity depends on correct verification, storage canonicality, query limits, auth, and shutdown behavior.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, query caps/pagination, and exposed listener safety.

## Design Decisions

- CL sync is forward-only from a recent checkpoint. EL historical sync walks backward from the CL-authenticated pivot to genesis.
- Logs are valid only inside the verified contiguous stored range.
- Historical EL validation verifies parent-hash ancestry, body commitments, receipt roots, cumulative gas, and logs bloom against each header.
- Query responses keep a hard `10,000` row cap and dashboard pagination defaults to `50` rows.
- Historical storage writes sealed compacted segments directly, avoiding raw segment buildup during normal reverse sync.
- Active background compaction is best-effort during sync and should not compete with verified ingestion when memory or disk headroom is tight.
- Dashboard storage uses the normal user model: one data directory, one writable disk-free value. Multi-volume server hacks are not part of the main UI.
- Historical fetch windows scale by serving peers, memory, and observed log density. Experiments that improve one range but regress RSS or peer usefulness should be reverted.
- Memory-pressure handling may ask glibc to trim free allocator arenas after dense historical batches; it is rate-limited and disabled on non-glibc targets.
- Dense historical ranges use a 4,096-block cap only when the memory tier would otherwise allow larger windows; lower-memory machines still fall back to smaller windows.

## Challenges and Resolutions

- Challenge: Older runs produced hundreds of raw segments pending compression.
  - Resolution: Historical batches now write compacted sealed segments directly. Remaining raw backlog on the remote is legacy cleanup, not new runaway growth.

- Challenge: Dense log ranges made writes and dashboard responsiveness unstable.
  - Resolution: Storage metrics are cached asynchronously, historical writes flush by row count, and byte-page compression now avoids a redundant zstd pass.

- Challenge: Extra test volumes made dashboard free-space semantics confusing.
  - Resolution: Main UI now reports writable data-disk free space only; extra volume details were removed from the UI.

- Challenge: Increasing dense fetch depth looked like a possible way to hide fetch latency.
  - Resolution: The remote trial raised RSS to about 12 GiB and worsened ETA, so the change was reverted.

- Challenge: Dense historical batches left high RSS after data was freed, which pushed the adaptive sync planner into low-memory mode.
  - Resolution: Added a rate-limited allocator trim for Linux/glibc runs and confirmed it fires only under the low-memory threshold.

- Challenge: Some scheduler and allocation experiments looked promising but hurt the live run.
  - Resolution: Reverted prefetch cancellation because it drained lookahead to depth 1, and reverted dense write-buffer preallocation because it increased RSS and trim frequency.

## Dead Code and Obsolescence Cleanup

- Inspected storage metrics UI code and removed obsolete volume-label rendering, the redundant advanced disk-free row, and related JavaScript.
- Inspected the adaptive byte-page encoder and removed the now-unneeded dual-compression path for normal pages.
- Inspected the dense fetch-depth, prefetch-cancellation, and write-preallocation experiments after measurement and reverted the variants that did not improve the run.
- No additional obsolete EL sync paths were removed in this pass; remaining changes are active code paths used by the remote run.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: none; continuing the EL reverse-sync PR branch.
- Commits made during this run: `perf: trim allocator during dense historical sync`; pending checkpoint for dense-window tuning.
- Pull request status: draft PR #76 remains open.
- Merge status: not ready; EL performance and validation work remain incomplete.
- Blockers: none known.

## Known Issues or Risks

- The remote run is still warming after the latest restart, so post-change ETA must be judged after serving peers recover.
- Full sync performance is still sensitive to body/receipt serving peers and dense receipt/log processing.
- Extra server volumes are a test-environment workaround and not a product storage allocator.
- HTTP Basic auth is not transport encryption; public deployments need localhost binding, firewalling, SSH tunneling, or TLS termination.
- gRPC exposure still needs a clear auth/bind/disable policy before public deployment.
- Verification-critical security review is still required before a production-ready release.
