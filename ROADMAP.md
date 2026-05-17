# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, and gRPC.

Active branch: `hardening/release-readiness-smokes`. PR #82 and PR #83 were merged; the current PR covers release listener-policy hardening.

## Completed Since Last Run

- Merged the query workbench PR after CI passed.
- Added comma-separated checkpoint-sync source support so operators can require a quorum of Beacon API/checkpoint-sync endpoints before trusting a resolved checkpoint.
- Fresh checkpoint resolution now uses the lowest finalized slot visible across successful sources, then verifies that the same slot/root reaches quorum.
- User-supplied inline checkpoints are validated against the configured source quorum, and freshness is checked against the newest finalized slot returned by successful sources.
- Made HTTP and gRPC query listeners bind to loopback by default.
- Added explicit public-listener guards: public HTTP requires dashboard auth, and public gRPC requires an explicit allow flag.

## Remaining TODOs

1. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a live release-candidate run from a clean data directory.

## Design Decisions

- SQL responses no longer have a hidden server-side row cap; dashboard-generated queries default to `LIMIT 500`.
- Dashboard query history and query-builder state remain browser-local only.
- Bounded log queries prune whole segments with metadata first, then use indexes where available, then apply row-level checks for correctness.
- Common ERC20 Transfer and Approval topic filtering uses compact per-segment bloom indexes by default instead of large exact composite indexes; this keeps storage growth practical while preserving correctness through row rechecks.
- `SUM(data)` uses a native exact aggregate path because Ethereum event `data` is hex-encoded `uint256`; results are returned as exact decimal strings.
- `COUNT(*)` and `COUNT(1)` over native filters run on a native aggregate path; `GROUP BY source` reads only the compact source column for matching row ids.
- Background indexing builds the compact ERC20 event profile continuously during sync at a conservative batch size, then catches up faster when the node is idle.
- Checkpoint-sync source configuration stays backward-compatible with a single URL, but comma-separated URLs require majority agreement for automatic checkpoint resolution and inline checkpoint validation.
- Query APIs bind to loopback by default. Public HTTP listeners require Basic auth, and public gRPC listeners require an explicit operator opt-in because gRPC is unauthenticated.

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

- Challenge: A single checkpoint-sync endpoint remained a central trust assumption for fresh starts.
  - Resolution: Added a multi-source quorum resolver. With multiple configured URLs, LogEx resolves or validates a checkpoint only after enough sources agree on the same slot/root.

- Challenge: The dashboard and query APIs were easy to expose accidentally because listeners bound to all interfaces by default.
  - Resolution: Changed HTTP and gRPC defaults to loopback and added startup validation for intentionally public listeners.

## Dead Code and Obsolescence Cleanup

- Kept the legacy Transfer bloom reader only as a compatibility fallback for old data directories that have not been backfilled yet.
- Removed remote obsolete ERC20 composite and Transfer-only bloom index files after replacing them with compact common event blooms; primary segment data was not removed.
- Searched checkpoint resolution, query execution, index building, background indexing, listener configuration, HTTP shutdown, and disk guard paths for debug-only or superseded code.
- No debug-only code is intentionally left in the checkpoint or query path.

## Git Workflow

- Current branch: `hardening/release-readiness-smokes`
- New branch created this run: yes
- Commits made during this run: pending
- Pull request status: pending
- Merge status: pending
- Blockers: none currently.

## Known Issues or Risks

- Older synced data directories need `build-indexes --missing-only --profile erc20-transfer` before they receive compact common ERC20 event bloom indexes.
- Very wide selective queries can still spend seconds checking thousands of segment-level skip indexes; exact full-history global indexes would be faster but require substantially more storage.
- Queries without selective bounds or predicates can be expensive because unbounded SQL is intentionally allowed.
- Single checkpoint-sync URL mode remains available for compatibility and has the same trust assumption as before; use comma-separated URLs for quorum-based checkpoint resolution until LogEx operates a first-party checkpoint source.
- HTTP Basic auth is not transport encryption; public HTTP deployments still need firewalling, SSH tunneling, or TLS termination even though localhost binding is now the default.
- Verification-critical security review is still required before a production-ready release.
