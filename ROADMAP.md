# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, and gRPC.

Active branch: `fix/live-transfer-hook-controls`. The current branch tightens the live ERC20 transfer hook controls before the feature is finalized.

## Completed Since Last Run

- Changed the live transfer Connect button to show `Connecting...` while opening and `Connected` after the subscription acknowledgement.
- Made live transfer token, sender, recipient, and transaction cells copy their raw values with the same copy affordance as query results.
- Added an Etherscan open button next to each live transfer transaction hash.

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

- Challenge: Token balance queries using `GROUP BY address`, `HAVING`, and `ORDER BY` fell back to DataFusion, which cannot sum hex-encoded `data` as exact uint256 values.
  - Resolution: Extended the native exact aggregate path to group by token contract and apply aggregate filtering and ordering before pagination.

- Challenge: The README and generated help text lagged behind the current listener hardening and index-maintenance parameters.
  - Resolution: Rebuilt the CLI reference from the actual clap definitions and added help-output tests for the important operator controls.

- Challenge: Live ERC20 hooks could accidentally flood subscribers with historical backfill if they reused the existing storage broadcast path unchanged.
  - Resolution: Removed historical subscription broadcasting and kept WebSocket notifications tied to live block ingestion.

- Challenge: Human amount filters are token-decimal dependent, but the server should not need token metadata for correctness.
  - Resolution: The server accepts raw uint256 min/max bounds; the dashboard tester converts human token-unit values using known or custom token decimals before subscribing.

- Challenge: WebSocket hooks needed validation against real live block ingestion rather than only historical query data.
  - Resolution: Ran a live remote probe that selected active recent ERC20 counterparties, subscribed with token and amount bounds, and verified real notifications from newly ingested blocks.

- Challenge: The live transfer status element reused the generic `.error` class, which hid it and caused the buttons to shift.
  - Resolution: Replaced it with a scoped status modifier class and verified the action row stays stable after validation errors.

- Challenge: The transaction cell needed both row-cell copy behavior and a nested external-link action.
  - Resolution: Kept the cell copyable, added a scoped Etherscan button, and handled button clicks before the table-level copy handler.

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

## Git Workflow

- Current branch: `fix/live-transfer-hook-controls`
- New branch created this run: no
- Commits made during this run: pending
- Pull request status: draft PR #88 is open and will be updated after commit/push.
- Merge status: blocked by user approval; this branch must not be merged until confirmed final.
- Blockers: none currently.

## Known Issues or Risks

- Older synced data directories need `build-indexes --missing-only --profile erc20-transfer` before they receive compact common ERC20 event bloom indexes.
- Very wide selective queries can still spend seconds checking thousands of segment-level skip indexes; exact full-history global indexes would be faster but require substantially more storage.
- Queries without selective bounds or predicates can be expensive because unbounded SQL is intentionally allowed.
- Single checkpoint-sync URL mode remains available for compatibility and has the same trust assumption as before; use comma-separated URLs for quorum-based checkpoint resolution until LogEx operates a first-party checkpoint source.
- HTTP Basic auth is not transport encryption; public HTTP deployments still need firewalling, SSH tunneling, or TLS termination even though localhost binding is now the default.
- WebSocket transfer hooks notify after verified live block ingestion, not pending mempool transfers.
- WebSocket missed-event replay is not implemented; clients that disconnect should query historical logs for the missed range after reconnecting.
- Verification-critical security review is still required before a production-ready release.
