# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis, keeps following head, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, and gRPC.

Active branch: `feature/query-workbench`. Draft PR #82 is open and must remain unmerged until the query workbench is approved.

This branch is focused on PostgreSQL-like log querying, query-builder UX, cancellation, and common ERC20 query performance.

## Completed Since Last Run

- Added cancellable SQL execution with a single active dashboard query and `POST /query/cancel`.
- Released the storage read lock before expensive SQL execution by snapshotting segment metadata first.
- Added timestamp/data predicate pushdown and timestamp-aware segment pruning.
- Added a native fast path for ordered bounded `logs` queries so common log scans avoid DataFusion `TopK` overhead.
- Added B-tree index format v2 with direct exact-key lookup while keeping v1 indexes readable.
- Added ERC20 Transfer composite indexes for token-wide and token+from/to queries.
- Made `build-indexes --missing-only` build only missing files instead of rewriting every index in a profile.
- Backfilled the remote full-sync data directory with the new Transfer query indexes for the requested timestamp range.
- Validated the provided bounded USDC Transfer query on the full remote data set: 5 matching rows in 2.26 seconds with `total_scanned = 5`.
- Validated a broader USDC Transfer smoke over the same timestamp range: 500 rows in 1.23 seconds.
- Restored the larger custom date-picker icon while keeping the interactive `showPicker()` click path.
- Added a hand cursor only over the date-picker icon hit area.
- Display query-result `timestamp` fields as local `YYYY-MM-DD HH:mm:ss` values while keeping Unix timestamps as the raw copied/exported value.
- Added exact native `SUM(data)` support for Ethereum `uint256` event data, including `SUM(CAST(data AS NUMERIC))` and PostgreSQL-style `SUM(data::NUMERIC)`.
- Validated `SUM(data)` on the remote full-sync data directory: the exact bounded USDC query scanned the same 5 rows as the non-aggregate query and completed in 0.21-0.24 seconds; a dense one-day USDC range summed 505,388 rows in 1.93 seconds.

## Remaining TODOs

1. Add supported SQL introspection.
   - Reason: Users need to discover LogEx tables and log fields without reading source.
   - Completion criteria: Supported queries can list available tables and log columns with data types, and tests cover accepted and rejected introspection shapes.

2. Broaden query compatibility tests.
   - Reason: The SQL surface should stay close to PostgreSQL-style log analysis while remaining scoped to verified Ethereum logs.
   - Completion criteria: Tests cover projections, aliases, predicates, block/time bounds, ordering, limit/offset, unbounded results, aggregates beyond `SUM(data)`, grouping, distinct, nulls, invalid SQL, unsupported tables, and deterministic errors.

3. Broaden common ERC20 query performance coverage.
   - Reason: Transfer queries over dense mainnet ranges must stay fast for token-wide, sender-filtered, receiver-filtered, amount-filtered, and time-bounded shapes.
   - Completion criteria: Representative ERC20 query-builder outputs are covered by tests or active benchmarks, and any required index backfill path is documented.

4. Replace the temporary checkpoint source.
   - Reason: Weak-subjectivity safety needs a first-party recent-checkpoint flow.
   - Completion criteria: LogEx has its own recent-checkpoint source or verified multi-source flow, and stale checkpoints force a fresh checkpointed resync.

5. Complete release hardening.
   - Reason: Production readiness depends on verification safety, query behavior, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, query cancellation, auth, and exposed listener policy.

## Design Decisions

- SQL responses no longer have a hidden server-side row cap; dashboard-generated queries default to `LIMIT 500`.
- Dashboard query history and query-builder state remain browser-local only.
- Bounded log queries should prune whole segments with metadata first, then use exact indexes where available, then apply row-level checks for correctness.
- The native SQL fast path is limited to simple ordered `logs` queries; more complex SQL continues through DataFusion.
- `SUM(data)` is handled by a dedicated native aggregate path instead of DataFusion because Ethereum event `data` is hex-encoded `uint256`; returning an exact decimal string avoids `u64`/JavaScript precision loss.
- ERC20 Transfer indexing prioritizes `(address, topic0)`, `(address, topic0, topic1)`, and `(address, topic0, topic2)` because those cover token, sender, and receiver filters used by the query builder.
- Query execution snapshots storage metadata before scanning so long queries do not block sync writes.
- Query-builder date/time controls show LogEx's custom calendar icon while clicks in the icon area open the native `datetime-local` picker through `showPicker()`; the hand cursor is limited to that same icon hit area.

## Challenges and Resolutions

- Challenge: Long dashboard queries could continue running and block useful work.
  - Resolution: Added server-side cancellation, a dashboard Stop button, and single-active-query enforcement.

- Challenge: Empty matches surfaced a DataFusion zero-partition planning error.
  - Resolution: Empty scans now return a valid empty single-partition result.

- Challenge: The provided Transfer query was slow because v1 exact-key lookup scanned large index files per segment.
  - Resolution: Added B-tree v2 direct point lookup and native exact Transfer composite lookups.

- Challenge: Timestamp predicates previously could still touch irrelevant segments.
  - Resolution: Added timestamp metadata to segments/manifests and pruned partitions before opening segment data.

- Challenge: DataFusion cannot directly aggregate LogEx's hex-encoded Ethereum `uint256` `data` values without lossy or unsupported casts.
  - Resolution: Added a native `SUM(data)` path that uses the existing predicate/index pruning, reads only matching `data` cells, sums with arbitrary precision, and returns the exact decimal result.

## Dead Code and Obsolescence Cleanup

- Inspected query UI, SQL execution, native scanning, index building, and storage metadata paths.
- Inspected query-result rendering and SQL aggregate execution for obsolete formatting or debug code.
- Kept row-level predicate rechecks for materialized native row queries; the `SUM(data)` path relies on exact candidate row-id filtering and reads only the `data` column.
- No debug-only code is intentionally left in the query path.

## Git Workflow

- Current branch: `feature/query-workbench`
- New branch created this run: no
- Commits made during this run: pending
- Pull request status: draft PR #82 remains open
- Merge status: intentionally not merged
- Blockers: none for committing the current query-indexing milestone; PR remains draft until the query workbench is approved.

## Known Issues or Risks

- Older synced data directories need `build-indexes --missing-only --profile erc20-transfer` before they receive the newest Transfer composite indexes.
- Queries without selective bounds or predicates can still be expensive because unbounded SQL is intentionally allowed.
- The remote data directory still has many older segments without token-wide `(address, topic0)` indexes; current requested and latest-token benchmarks are fast, so full backfill should be driven by measured need.
- HTTP Basic auth is not transport encryption; public deployments need localhost binding, firewalling, SSH tunneling, or TLS termination.
- Verification-critical security review is still required before a production-ready release.
