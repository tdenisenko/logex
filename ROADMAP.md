# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `feature/disable-historical-sync`. The branch adds an opt-in forward-only sync mode for fresh data directories.

## Completed Since Last Run

- Added `sync --disable-historical-sync` to start a fresh data directory without reverse historical EL backfill.
- Persisted forward-only mode in `sync-mode.json` so restarts preserve the mode only when the flag remains enabled.
- Added startup validation that rejects enabling the flag on data directories already initialized with normal historical sync.
- Allowed a data directory first started with the flag to convert back to normal historical sync when restarted without it.
- Suppressed historical ETA/rate fields in `/status` and switched the dashboard to a forward-sync view while the flag is active.
- Added focused unit tests for CLI parsing, mode transitions, progress state, and status output.
- Validated the branch with formatting, focused affected-package tests, full workspace tests, and full workspace clippy.

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

## Challenges and Resolutions

- Challenge: The main checkout had unrelated local work on another branch.
  - Resolution: Created a separate worktree at `/private/tmp/logex-disable-historical-sync` and branched from `master`.

- Challenge: Forward-only mode still needs a trustworthy pivot and resumable default conversion.
  - Resolution: Kept CL-authenticated forward anchor handling intact and only disabled the reverse historical backfill scheduler.

## Dead Code and Obsolescence Cleanup

- Inspected historical sync scheduling, progress state, status serialization, and dashboard rendering paths touched by the new mode.
- No obsolete production code was removed; the historical backfill path remains the default behavior and is still required.
- No experimental debug code was left in the branch.

## Git Workflow

- Current branch: `feature/disable-historical-sync`
- Worktree: `/private/tmp/logex-disable-historical-sync`
- New branch created this run: yes, from `master`
- Commits made during this run: one commit, `feat: add forward-only sync mode`
- Pull request status: draft PR #89 (`https://github.com/tdenisenko/logex/pull/89`)
- Merge status: pending
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
