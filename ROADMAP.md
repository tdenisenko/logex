# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, and gRPC.

Active branch: `feature/query-workbench`. Draft PR #82 is open and must remain unmerged until the query workbench is approved.

## Completed Since Last Run

- Added a native `COUNT(*)` / `COUNT(1)` aggregate path with `GROUP BY source` support so common count queries avoid DataFusion row materialization.
- Validated a broad real-data query matrix on the full remote data set, including introspection, latest rows, bounded Transfer queries, Approval queries, timestamp predicates, exact `SUM(data)` balance queries, distinct values, grouped counts, and empty matches.
- Reduced the recent `GROUP BY source` validation query from ~26s to ~0.3s on the full remote data set.

## Remaining TODOs

1. Replace the temporary checkpoint source.
   - Reason: Weak-subjectivity safety needs a first-party recent-checkpoint flow.
   - Completion criteria: LogEx has its own recent-checkpoint source or verified multi-source flow, and stale checkpoints force a fresh checkpointed resync.

2. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, and exposed listener policy.

## Design Decisions

- SQL responses no longer have a hidden server-side row cap; dashboard-generated queries default to `LIMIT 500`.
- Dashboard query history and query-builder state remain browser-local only.
- Bounded log queries prune whole segments with metadata first, then use indexes where available, then apply row-level checks for correctness.
- Common ERC20 Transfer and Approval topic filtering uses compact per-segment bloom indexes by default instead of large exact composite indexes; this keeps storage growth practical while preserving correctness through row rechecks.
- `SUM(data)` uses a native exact aggregate path because Ethereum event `data` is hex-encoded `uint256`; results are returned as exact decimal strings.
- `COUNT(*)` and `COUNT(1)` over native filters run on a native aggregate path; `GROUP BY source` reads only the compact source column for matching row ids.
- Background indexing builds the compact ERC20 event profile continuously during sync at a conservative batch size, then catches up faster when the node is idle.

## Challenges and Resolutions

- Challenge: Exact ERC20 composite indexes gave good query speed but consumed too much storage.
  - Resolution: Replaced the automatic Transfer profile with compact per-segment bloom indexes and removed obsolete composite backfill code from that profile.

- Challenge: The wide ERC20 balance query touched more than 11,000 segments.
  - Resolution: Added Transfer bloom pruning, moved bloom checks before segment open, and parallelized native aggregate partition scans. The query now completes on real full-sync data instead of timing out.

- Challenge: Approval queries still timed out after Transfer optimization.
  - Resolution: Added a common ERC20 event bloom keyed by event topic, token address, indexed topic position, and indexed topic value. `data != ...` now stays on the native path, and the full-range USDC Approval query completes in seconds on the remote data set.

- Challenge: A slow HTTP query kept the old remote process deactivating during restart.
  - Resolution: HTTP shutdown now marks the active query as canceled before graceful shutdown waits for open requests to finish.

- Challenge: `COUNT(*) ... GROUP BY source` over a recent dense range scanned millions of rows through the generic SQL engine.
  - Resolution: Added a native count aggregate path that uses segment pruning and only reads the `source` column when grouping. The real-data grouped count query now completes in under a second.

- Challenge: A low-space sealed symlink volume stopped the server even though the active write path still had headroom.
  - Resolution: The low-disk guard now probes writable storage roots, not every sealed segment target.

## Dead Code and Obsolescence Cleanup

- Kept the legacy Transfer bloom reader only as a compatibility fallback for old data directories that have not been backfilled yet.
- Removed remote obsolete ERC20 composite and Transfer-only bloom index files after replacing them with compact common event blooms; primary segment data was not removed.
- Searched query execution, index building, background indexing, HTTP shutdown, and disk guard paths for debug-only or superseded code.
- No debug-only code is intentionally left in the query path.

## Git Workflow

- Current branch: `feature/query-workbench`
- New branch created this run: no
- Commits made during this run: pending
- Pull request status: draft PR #82 remains open
- Merge status: intentionally not merged
- Blockers: PR remains draft until the query workbench is approved.

## Known Issues or Risks

- Older synced data directories need `build-indexes --missing-only --profile erc20-transfer` before they receive compact common ERC20 event bloom indexes.
- Very wide selective queries can still spend seconds checking thousands of segment-level skip indexes; exact full-history global indexes would be faster but require substantially more storage.
- Queries without selective bounds or predicates can be expensive because unbounded SQL is intentionally allowed.
- HTTP Basic auth is not transport encryption; public deployments need localhost binding, firewalling, SSH tunneling, or TLS termination.
- Verification-critical security review is still required before a production-ready release.
