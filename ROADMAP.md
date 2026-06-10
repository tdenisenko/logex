# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `fix/dashboard-sync-metrics`. The current branch fixes dashboard sync metric presentation for CPU charts, historical sync progress, and Consensus Layer peer visibility.

## Completed Since Last Run

- Fixed the Historical Sync dashboard card so known zero progress and zero sync rates render as explicit values instead of blank placeholders.
- Kept incomplete historical-sync estimates meaningful while the backend has not yet produced a historical rate.
- Stabilized the CPU chart against a 100% utilization baseline while continuing to use normalized process CPU from the status endpoint.
- Expanded Advanced Metrics with detailed Consensus Layer peer counters from the existing `consensus_network` status payload.

## Remaining TODOs

1. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a live release-candidate run from a clean data directory.

## Design Decisions

- Forward-only sync is a fresh-data-dir mode because switching an existing historical data directory into partial-history mode would make coverage semantics ambiguous.
- The mode marker lives at `sync-mode.json` in the data directory. It is intentionally separate from storage segments so startup can validate the mode before sync begins.
- Restarting without `--disable-historical-sync` removes the marker and resumes default historical sync, preserving the existing pivot/floor metadata as the backfill start point.
- The dashboard hides historical-only details when forward-only mode is active instead of showing zeroed reverse-sync metrics.
- SQL responses no longer have a hidden server-side row cap; dashboard-generated queries default to `LIMIT 500`.
- Dashboard query history and query-builder state remain browser-local only.
- Bounded log queries prune whole segments with metadata first, then use indexes where available, then apply row-level checks for correctness.
- Common ERC20 Transfer and Approval topic filtering uses compact per-segment bloom indexes by default instead of large exact composite indexes; this keeps storage growth practical while preserving correctness through row rechecks.
- `SUM(data)` uses a native exact aggregate path because Ethereum event `data` is hex-encoded `uint256`; results are returned as exact decimal strings.
- Grouped `SUM(data)` balance queries stay on the native exact aggregate path when grouped by `address`, so token balances do not fall back to text-based SQL aggregation.
- `COUNT(*)` and `COUNT(1)` over native filters run on a native aggregate path; `GROUP BY source` reads only the compact source column for matching row ids.
- Background indexing builds the compact ERC20 event profile continuously during sync at a conservative batch size, then catches up faster when the node is idle.
- Checkpoint-sync source configuration stays backward-compatible with a single URL, but comma-separated URLs require majority agreement for automatic checkpoint resolution and inline checkpoint validation.
- Query APIs bind to loopback by default. Public HTTP listeners require Basic auth, and public gRPC listeners require an explicit operator opt-in because gRPC is unauthenticated.
- CLI and README examples should show public HTTP as an explicit operator choice using `--http-host 0.0.0.0` plus `--dashboard-password`.
- WebSocket ERC20 transfer hooks use a dedicated `type: "erc20Transfers"` subscription instead of overloading `eth_getLogs` filters; this keeps wallet/token/amount alert semantics explicit while preserving legacy log streams.
- WebSocket transfer hooks are live-only. Historical data remains available through SQL and JSON-RPC, but backfill does not replay as alert traffic.
- Dashboard amount bounds are entered in token units and converted to raw uint256 values before subscription. If amount bounds are used with token filters, all selected tokens must share the same decimals to avoid ambiguous comparisons.
- ERC20 transfer hooks require at least one filter dimension: wallet addresses, token addresses, or both. Token-only subscriptions intentionally mean every transfer for the selected token contracts.
- Dashboard-created ERC20 transfer sessions are in-memory, browser-id scoped, and expire one minute after the browser WebSocket disconnects; service-created sessions are in-memory and persist until delete or process restart.
- Retained live-transfer notifications are bounded in memory to avoid OOM risk from broad token subscriptions.
- Dashboard CPU charts use normalized process CPU (`raw process CPU / logical core capacity`) and keep a 100% baseline so multi-core hosts are interpreted consistently.
- Historical sync progress displays `0.00%` and `0 logs/s` when those are known values; unknown telemetry still displays `--`.

## Challenges and Resolutions

- Challenge: The main checkout had unrelated local work on another branch.
  - Resolution: Created a separate worktree at `/private/tmp/logex-disable-historical-sync` and branched from `master`.

- Challenge: Forward-only mode still needs a trustworthy pivot and resumable default conversion.
  - Resolution: Kept CL-authenticated forward anchor handling intact and only disabled the reverse historical backfill scheduler.

- Challenge: The live transfer status element reused the generic `.error` class, which hid it and caused the buttons to shift.
  - Resolution: Replaced it with a scoped status modifier class and verified the action row stays stable after validation errors.

- Challenge: The transaction cell needed both row-cell copy behavior and a nested external-link action.
  - Resolution: Kept the cell copyable, added a scoped Etherscan link styled as a compact button, and handled link clicks before the table-level copy handler.

- Challenge: Refresh-resumable live transfer notifications require state outside the browser, but unbounded in-memory retention can exhaust smaller machines.
  - Resolution: Moved live transfer notification retention into server-backed sessions with a bounded history, one-minute post-disconnect dashboard expiry, and explicit service-subscription endpoints.

- Challenge: The Historical Sync card treated valid zero progress/rates as missing telemetry.
  - Resolution: Updated the dashboard formatter and progress rendering so known zero values remain visible while unknown values still use placeholders.

- Challenge: Consensus Layer peer data was available in `/status` but too compressed in Advanced Metrics.
  - Resolution: Added separate CL peer rows for connected, dialing, discovered, dialable, routing, RPC-capable, and pending-RPC counts.

## Dead Code and Obsolescence Cleanup

- Kept the legacy Transfer bloom reader only as a compatibility fallback for old data directories that have not been backfilled yet.
- Removed remote obsolete ERC20 composite and Transfer-only bloom index files after replacing them with compact common event blooms; primary segment data was not removed.
- Searched CLI definitions and README command references for stale or missing parameter documentation.
- Replaced obsolete query row-cap documentation with the current unlimited SQL endpoint behavior and dashboard `LIMIT 500` default.
- Removed obsolete historical WebSocket subscription plumbing from the EL historical ingest path.
- Reused the existing Ethereum address parser for WebSocket subscriptions instead of adding a second parser.
- Searched the touched WebSocket, dashboard, and historical ingest paths for old subscription helpers and stale call signatures.
- Rechecked the live transfer WebSocket and dashboard paths for obsolete empty-filter assumptions.
- Rechecked the live transfer dashboard rendering path and replaced raw title-only address/hash cells with the existing copy-cell pattern.
- Rechecked the live transfer WebSocket path and kept legacy raw log subscriptions on the existing broadcast channel while routing resumable ERC20 sessions through the new session registry.
- Reused the existing ERC20 transfer filter and notification formatter for service subscriptions instead of adding a second notification path.
- Inspected the dashboard sync metric rendering and chart setup paths; no dead code was found in the touched UI-only branch.

## Git Workflow

- Current branch: `fix/dashboard-sync-metrics`
- New branch created this run: yes
- Commits made during this run: `fix: clarify dashboard sync metrics`
- Pull request status: pending push
- Merge status: not merged
- Blockers: none currently.

## Known Issues or Risks

- Forward-only mode intentionally provides recent/live query coverage only until the operator restarts without the flag and completes historical backfill.
- No remote runtime test was run because this task explicitly requested local testing only.
- Older synced data directories need `build-indexes --missing-only --profile erc20-transfer` before they receive compact common ERC20 event bloom indexes.
- Very wide selective queries can still spend seconds checking thousands of segment-level skip indexes; exact full-history global indexes would be faster but require substantially more storage.
- Queries without selective bounds or predicates can be expensive because unbounded SQL is intentionally allowed.
- Single checkpoint-sync URL mode remains available for compatibility and has the same trust assumption as before; use comma-separated URLs for quorum-based checkpoint resolution until LogEx operates a first-party checkpoint source.
- HTTP Basic auth is not transport encryption; public HTTP deployments still need firewalling, SSH tunneling, or TLS termination even though localhost binding is now the default.
- WebSocket transfer hooks notify after verified live block ingestion, not pending mempool transfers.
- WebSocket missed-event replay is not implemented; clients that disconnect should query historical logs for the missed range after reconnecting.
- Verification-critical security review is still required before a production-ready release.
