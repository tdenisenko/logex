# LogEx Analysis And Roadmap

## Executive Summary

LogEx is already a real multi-crate Rust project, not just a scaffold. The repository contains:

- a standalone Ethereum P2P sync path (`logex-sync`)
- a columnar local storage engine (`logex-storage`)
- per-partition secondary indexes (`logex-index`)
- a LogSQL parser/planner/executor (`logex-query`)
- HTTP, JSON-RPC, gRPC, and WebSocket serving layers (`logex-server`)
- a runnable CLI node binary (`logex-node`)

The strongest architectural shift in the project history was the move from earlier ingestion approaches toward a standalone P2P light-node-style design. Local git history shows that progression clearly:

- `#6` composite indexes
- `#7` ingestion / ExEx-era work
- `#8` LogSQL parser
- `#9` refactor / quality pass
- `#10` query engine
- `#11` JSON-RPC
- `#12` REST API
- `#13` gRPC
- `#14` WebSocket subscriptions
- `#15` CLI polish
- `#16` RPC ingestion validation
- `#17` standalone P2P light node
- `#18` first mainnet sync

That said, the original `README.md` still describes a significantly more ambitious system than the current codebase actually implements. The repo is in a solid development phase, but it is not feature-complete relative to the full vision.

## What Exists Today

### Networking and sync

- DevP2P discovery and `eth` wire usage are implemented.
- The node requests headers, bodies, and receipts from peers.
- Receipt roots are validated against the header `receiptsRoot`.
- Reorg handling exists for the in-memory recent head window.
- The node serves HTTP and gRPC while syncing.

### Storage and indexing

- Logs are flattened into `LogRow`.
- Rows are written into a columnar partition layout on disk.
- Sealed partitions are indexed with address, topic0, block number, and composite indexes.
- Non-canonical rows can be marked during reorg handling.

### Querying and APIs

- LogSQL parsing exists for `SELECT`, `WHERE`, `ORDER BY`, `LIMIT`, `BETWEEN`, `IN`, `latest`, event literals, and address literals.
- Query execution works over local partitions.
- REST `/query`, JSON-RPC `eth_getLogs`, gRPC streaming, and WebSocket subscriptions are present.
- The web UI exists and now shows much more accurate sync/index progress.

## Problems I Found Before Fixing

### 1. Sync head was wrong when blocks had zero logs

Before this pass, the node effectively treated "highest block that produced at least one log row" as the chain head. That caused several problems:

- restart resume point was wrong after empty blocks
- `eth_blockNumber` could lag behind real sync progress
- `latest` in queries could be wrong
- the UI and CLI status could show stale head values

### 2. Query correctness depended on indexes existing

The executor only applied some residual filters. If a filter was extracted into the plan but the needed index was missing or stale, the query could silently return incorrect results.

This was especially risky for:

- fresh hot-partition data before index rebuilds
- `topic1` / `topic2` / `topic3`
- `block_hash`
- any other expression that fell through incomplete residual evaluation

### 3. Hot-partition background indexing broke after sealing

The background indexer tracked only the last indexed row count. When the hot partition rotated and row count reset, it could stop rebuilding indexes for the new hot partition until the new row count exceeded the previous partition's count.

### 4. Status surfaces were incomplete or misleading

- sync target was not updated from peer heights
- ETA and throughput were less useful than they should be
- the UI footer had stale gRPC information
- the UI did not distinguish synced head from indexed head

### 5. `eth_getLogs` missed `blockHash` handling

The filter type had `block_hash`, but matching and SQL pushdown were incomplete.

## Fixes Completed In This Pass

### Sync metadata and resume correctness

- Added persisted sync-head metadata in storage.
- The node now records the latest validated block even if it produced zero logs.
- `head_block()` now prefers persisted sync head over inferred log head.
- Added tests covering persisted sync head without rows and head precedence.

### Query correctness hardening

- Changed query execution to apply the full `WHERE` clause after index prefiltering.
- Added typed row-value evaluation for numeric, hash, address, topic, data, and `latest` expressions.
- Queries now remain correct even when indexes are missing or temporarily stale.
- Added regression tests for no-index filtering on `topic1` and `block_hash`.

### Index maintenance fix

- Reworked hot-partition background indexing to track partition identity, not just row count.
- Added tests proving the indexer rebuilds after partition rotation.

### Status and operator UX

- Sync target is now updated from the highest connected peer height.
- Added blocks-per-minute tracking.
- Improved terminal sync progress logging.
- Improved `/status` output with `synced`, `blocks_per_minute`, `indexed_head_block`, and progress percentage.
- Improved REST query projection for non-`*` selects.
- Updated the web UI to show synced head, indexed head, progress percentage, remaining blocks, logs ingested, and corrected gRPC labeling.

### JSON-RPC filter handling

- Added `blockHash` mutual exclusivity validation with `fromBlock` / `toBlock`.
- Added `blockHash` SQL pushdown and filter matching.

## Verification Performed

I validated the code after the changes with:

- `cargo fmt --all`
- `cargo test --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo run --bin logex -- info --data-dir /tmp/logex-smoke-info`

All of the above passed locally in this environment.

## What Is Still Not Done

These are the main gaps between the current implementation and the full product vision.

### 1. The README overstates current feature completeness

The current repo does not yet implement everything claimed in the existing `README.md`, including:

- ETH transfer trace backfill
- trace RPC startup validation
- beacon / finality integration
- SQL aggregation and decode execution
- a full long-term metadata/checkpoint story for all planned subsystems

### 2. Reorg durability across restart is still incomplete

In-memory reorg tracking exists for the active process, but the repo still does not persist a recent canonical header window. That means deep or restart-boundary reorg recovery still needs more work if the goal is production-grade chain continuity.

### 3. Reth crates are still used directly

The project currently depends on several Reth networking crates. If the product requirement is to avoid Reth crates entirely and only reuse ideas or copied MIT-licensed code, that migration has not happened yet.

### 4. Query language scope needs a product decision

The parser recognizes more SQL-like constructs than the execution layer fully exposes. After this pass, filtering correctness is much better, but true aggregations, decoding, and richer projection semantics still need a deliberate implementation plan.

### 5. No live-network validation happened in this sandbox

I could validate build/test/CLI behavior here, but I could not perform a real Ethereum mainnet sync from this restricted environment. That still needs validation in a network-enabled environment before claiming production readiness.

## Recommended Roadmap

### Phase 1: Productionize the current standalone node

1. Persist a recent canonical header window, not just the latest head.
2. Add restart-safe reorg recovery using persisted ancestor data.
3. Add integration tests that simulate empty blocks, reorgs, and hot-index lag.
4. Add a small on-disk metadata schema version for forward-compatible upgrades.

### Phase 2: Narrow the documentation-to-code gap

1. Decide whether to implement the missing features or trim claims.
2. If SQL is meant to be analytical, implement real aggregation and decode execution.
3. If ETH transfer coverage is core, implement the backfill subsystem as a separate crate and CLI flags.

### Phase 3: Remove unwanted dependency shape

1. Audit every direct Reth crate dependency.
2. Decide which parts should be copied or reimplemented locally under MIT terms.
3. Reduce the project to the minimal standalone networking surface needed for DevP2P + `eth`.

### Phase 4: End-to-end realism

1. Run a long mainnet soak test.
2. Measure sync throughput, partition growth, index build time, and query latency.
3. Add a benchmark document based on real data instead of projected numbers.

## Bottom Line

LogEx now has a more trustworthy core than it had before this pass:

- head tracking is materially more correct
- query correctness is materially more correct
- hot-partition indexing is materially safer
- status reporting is materially more honest

The project is promising and already useful as a development-phase standalone log-ingestion/query node, but it still needs another round of implementation to fully match the complete product story described in the original README.
