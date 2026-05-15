# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, follows CL head/finality over P2P, uses CL-verified execution headers as the EL pivot, then syncs EL forward to head and backward toward genesis. EL historical sync verifies header ancestry, bodies, receipt roots, cumulative gas, and log blooms without executing the EVM. Queryable log coverage expands as verified segments are stored.

Active development branch for this run: `feature/query-workbench`. PRs #76, #78, #79, and #81 were merged. The remote test client is running on `root@165.22.64.42` with HTTP on `18683` and data in `/var/lib/logex/mainnet`.

The remote EL validation run reached genesis, kept live head tracking afterward, and survived a graceful service restart with historical floor still at `0`. Warmed samples held strong peer retention, zero raw compression backlog, and roughly 300k-450k historical logs/sec in dense ranges, then accelerated across sparse pre-Merge history. CPU profiles show the remaining hot path is mostly required receipt verification work, especially receipt-root Keccak. The two extra mounted volumes are being used for a machine-specific symlink relocation of sealed historical segments; this is not product storage behavior.

## Completed Since Last Run

- Started the query workbench branch and draft PR scope.
- Added a query execution timer in `MM:SS:mmm` format.
- Added browser-local query history with most recent queries first, expandable SQL detail rows, reuse buttons, and per-query `.sql` export.
- Fixed query history layout so entries wrap within the page instead of requiring horizontal scrolling.
- Made query result cells clickable and keyboard-copyable so field text can be copied to the clipboard.
- Documented query-engine, query-builder, performance, and coverage work as explicit TODOs for this branch.

## Remaining TODOs

1. Replace the temporary checkpoint source and stale-checkpoint policy.
   - Reason: Weak-subjectivity safety requires a recent checkpoint and clear stale-checkpoint rejection.
   - Completion criteria: LogEx has its own recent-checkpoint source or verified multi-source flow, and stale checkpoints force a fresh checkpointed resync.

2. Complete release validation and hardening.
   - Reason: Trustless log validity depends on correct verification, storage canonicality, query limits, auth, graceful shutdown, and exposed listener safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, query caps/pagination, and public deployment safety.

3. Build PostgreSQL-like query introspection for the supported LogEx schema.
   - Reason: Users need to discover available tables and fields without reading source code.
   - Completion criteria: Supported SQL can list available query tables and log table columns with data types, and tests cover the supported introspection queries and rejected out-of-scope system catalog access.

4. Expand query-engine compatibility for Ethereum event-log analysis.
   - Reason: LogEx should feel close to a PostgreSQL-style analytical query surface while staying scoped to verified Ethereum logs.
   - Completion criteria: A broad TDD query suite covers projections, aliases, filters, block ranges, address/topic predicates, ordering, limit/offset caps, aggregates, grouping, distinct values, null handling, invalid SQL, unsupported tables, and deterministic error messages.

5. Add the dashboard query builder.
   - Reason: Non-SQL users need a deterministic way to build common log queries without guessing field names or event predicates.
   - Completion criteria: The UI exposes togglable `logs` fields, block range inputs, and a common ERC20 token selector that generates deterministic SQL and fills the Query Logs editor without executing automatically.

6. Measure and improve query performance on realistic segment access patterns.
   - Reason: Complex queries may touch many compressed segments and expose decompression, scanning, or indexing bottlenecks that small unit fixtures cannot reveal.
   - Completion criteria: Synthetic integration tests cover sparse and dense block ranges, and an optional active benchmark against a full synced data directory records query time, scanned rows/segments, and regressions worth optimizing.

## Design Decisions

- CL sync is forward-only from a recent checkpoint. EL historical sync walks backward from the CL-authenticated pivot to genesis.
- Logs are valid only inside the verified contiguous stored range.
- Historical EL validation verifies parent-hash ancestry, body commitments, receipt roots, cumulative gas, and logs bloom against each header.
- Historical chunks whose headers prove empty transaction, receipt, ommer, and withdrawal roots can be ingested header-only because the empty body and receipt tries are uniquely determined by those roots.
- Historical ETA is log-based when log-rate data is available; block/sec remains an advanced diagnostic because block density varies heavily across history.
- After EL history reaches genesis, the main dashboard switches from historical reverse-sync metrics to live head-gap metrics.
- Dashboard CPU is shown as capacity utilization across logical CPUs; raw multi-core process CPU remains available in advanced status data.
- Query responses keep a hard `10,000` row cap and dashboard pagination defaults to `50` rows.
- Historical storage writes sealed compacted segments directly, avoiding raw segment buildup during normal reverse sync.
- Dashboard storage uses the normal user model: one data directory, one writable disk-free value. Multi-volume server hacks are not part of the main UI.
- Historical fetch windows scale by serving peers, memory, and observed log density. Experiments that improve one range but regress RSS, peer usefulness, or logs/sec should be reverted.
- Dashboard section expansion state is stored in browser `localStorage` because it is a per-browser display preference, not node state.
- Query history is stored only in browser `localStorage`; it is user convenience state and must not be written to the node data directory.
- ERC20 token names in the query builder should map to contract addresses, not event topics. The ERC20 `Transfer` topic0 is shared across tokens, while the log `address` identifies the token contract.
- Query performance validation should combine deterministic synthetic fixtures with optional active full-data benchmarks because repository tests cannot carry the synced mainnet log dataset.

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

- Challenge: Query performance tests need realistic compressed segment access without committing large mainnet data.
  - Resolution: Track deterministic synthetic integration coverage separately from optional active benchmarks against a full synced data directory.

## Dead Code and Obsolescence Cleanup

- Inspected the dashboard query UI path and reused existing localStorage patterns. No backend query-engine code has been removed in this first query-workbench slice.

## Git Workflow

- Current branch: `feature/query-workbench`
- New branch created this run: `feature/query-workbench` from `origin/master`.
- Commits made during this run: initial query workbench roadmap and dashboard history/timer work, plus query history wrapping and result-cell copy fixes.
- Pull request status: draft PR for ongoing query work.
- Merge status: intentionally not merged until user approval.
- Blockers: none known.

## Known Issues or Risks

- The log-count ETA uses a reference total and recent-block average above block `25,093,066`; it is better than block/sec ETA but still an estimate.
- Full sync performance is now mostly sensitive to receipt verification CPU and body/receipt response latency. Further optimization should be handled in a new focused PR only if fresh-run measurements show a meaningful regression or clear upside.
- Extra server volumes are a test-environment workaround and not a product storage allocator.
- HTTP Basic auth is not transport encryption; public deployments need localhost binding, firewalling, SSH tunneling, or TLS termination.
- gRPC exposure still needs a clear auth/bind/disable policy before public deployment.
- Verification-critical security review is still required before a production-ready release.
