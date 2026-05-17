# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, and gRPC.

Active branch: `feature/query-workbench`. Draft PR #82 is open and must remain unmerged until the query workbench is approved.

## Completed Since Last Run

- Completed the query workbench SQL milestone: PostgreSQL-style introspection, broad query compatibility tests, exact `uint256` SUM support, conditional ERC20 balance aggregates, and query-builder-shaped ERC20 tests.
- Replaced the automatic ERC20 Transfer backfill profile with compact per-segment Transfer bloom indexes; fresh syncs now build missing sealed Transfer indexes continuously, and existing data directories can be backfilled with `build-indexes --profile erc20-transfer --missing-only`.
- Added bounded parallel index building and parallel native aggregate partition scans.
- Fixed low-disk shutdown probing so LogEx guards active writable storage roots without stopping because an old sealed-segment symlink target is nearly full.
- Validated representative queries on the remote full-sync data set: introspection in ~0.3s, bounded USDC select/SUM/balance queries in ~0.7-1.7s, and the wide 12,000,000-25,108,000 USDC balance query in ~25s with 184 verified rows scanned.

## Remaining TODOs

1. Replace the temporary checkpoint source.
   - Reason: Weak-subjectivity safety needs a first-party recent-checkpoint flow.
   - Completion criteria: LogEx has its own recent-checkpoint source or verified multi-source flow, and stale checkpoints force a fresh checkpointed resync.

2. Complete release hardening.
   - Reason: Production readiness depends on verification safety, query behavior, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, query cancellation, auth, and exposed listener policy.

## Design Decisions

- SQL responses no longer have a hidden server-side row cap; dashboard-generated queries default to `LIMIT 500`.
- Dashboard query history and query-builder state remain browser-local only.
- Bounded log queries prune whole segments with metadata first, then use indexes where available, then apply row-level checks for correctness.
- Common ERC20 Transfer sender/receiver filtering uses compact per-segment bloom indexes by default instead of large exact composite indexes; this keeps storage growth practical while preserving correctness through row rechecks.
- `SUM(data)` uses a native exact aggregate path because Ethereum event `data` is hex-encoded `uint256`; results are returned as exact decimal strings.
- Background indexing builds the compact ERC20 Transfer profile continuously during sync at a conservative batch size, then catches up faster when the node is idle.

## Challenges and Resolutions

- Challenge: Exact ERC20 composite indexes gave good query speed but consumed too much storage.
  - Resolution: Replaced the automatic Transfer profile with compact per-segment bloom indexes and removed obsolete composite backfill code from that profile.

- Challenge: The wide ERC20 balance query touched more than 11,000 segments.
  - Resolution: Added Transfer bloom pruning, moved bloom checks before segment open, and parallelized native aggregate partition scans. The query now completes on real full-sync data instead of timing out.

- Challenge: A low-space sealed symlink volume stopped the server even though the active write path still had headroom.
  - Resolution: The low-disk guard now probes writable storage roots, not every sealed segment target.

## Dead Code and Obsolescence Cleanup

- Removed the obsolete `build_erc20_transfer_indexes` composite backfill path from the automatic Transfer profile.
- Removed remote obsolete ERC20 composite index files from the test data directory after replacing them with compact bloom indexes; primary segment data was not removed.
- Searched query execution, index building, background indexing, and disk guard paths for debug-only or superseded code.
- No debug-only code is intentionally left in the query path.

## Git Workflow

- Current branch: `feature/query-workbench`
- New branch created this run: no
- Commits made during this run: pending
- Pull request status: draft PR #82 remains open
- Merge status: intentionally not merged
- Blockers: PR remains draft until the query workbench is approved.

## Known Issues or Risks

- Older synced data directories need `build-indexes --missing-only --profile erc20-transfer` before they receive compact Transfer bloom indexes.
- Very wide queries can still spend noticeable time checking thousands of segment-level skip indexes; exact full-history indexes would be faster but require substantially more storage.
- Queries without selective bounds or predicates can be expensive because unbounded SQL is intentionally allowed.
- HTTP Basic auth is not transport encryption; public deployments need localhost binding, firewalling, SSH tunneling, or TLS termination.
- Verification-critical security review is still required before a production-ready release.
