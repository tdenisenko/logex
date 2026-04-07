# LogEx

LogEx is an Ethereum log-focused node. It syncs headers, bodies, and receipts from the Ethereum P2P network, validates receipt roots, extracts logs, stores them locally in a columnar format, indexes them, and exposes them through SQL-like queries plus JSON-RPC, REST, gRPC, and WebSocket APIs.

This repository is in active development. The core sync, storage, query, and server pieces are real and runnable, but the implementation is still narrower than the full long-term vision described in the original `README.md`.

## Current Status

What works today:

- standalone DevP2P discovery and receipt-based sync path
- receipt-root validation against headers
- local columnar storage for event logs
- sealed-partition indexing
- LogSQL filtering, ordering, and limiting
- REST, JSON-RPC, gRPC, and WebSocket query interfaces
- web UI for sync/index/query status

What is not fully implemented yet:

- ETH transfer trace backfill
- beacon/finality integration
- full analytical SQL execution (`COUNT`, `GROUP BY`, decode functions, joins)
- restart-durable reorg history beyond the current in-memory tracking window
- removal of direct Reth crate dependencies

## Workspace Layout

- `crates/logex-node`: CLI binary and runtime wiring
- `crates/logex-sync`: P2P networking, receipt validation, sync engine
- `crates/logex-ingestion`: log extraction and ingestion pipeline
- `crates/logex-storage`: columnar on-disk storage and partition manager
- `crates/logex-index`: secondary indexes for partitions
- `crates/logex-query`: LogSQL lexer, parser, planner, executor
- `crates/logex-server`: REST, JSON-RPC, gRPC, WebSocket, and web UI
- `crates/logex-types`: shared types

## Running The Node

Build:

```bash
cargo build
```

Show local storage state:

```bash
cargo run --bin logex -- info
```

Start the node:

```bash
cargo run --bin logex -- sync \
  --data-dir ./logex-data \
  --http-port 8577 \
  --grpc-port 8578 \
  --discovery-port 30303 \
  --max-peers 50
```

While the node is running:

- Web UI: `http://127.0.0.1:8577`
- REST query endpoint: `POST /query`
- JSON-RPC endpoint: `POST /`
- WebSocket live logs: `GET /ws`
- gRPC: configured via `--grpc-port` (default `8578`)

## Querying

The current LogSQL execution path is focused on log retrieval, not full analytics.

Supported today:

- `SELECT *` and selected columns
- `WHERE`
- `ORDER BY`
- `LIMIT`
- `BETWEEN`
- `IN`
- `latest`
- address literals
- event signature literals

Examples:

```sql
SELECT * FROM logs
WHERE address = '0xdAC17F958D2ee523a2206206994597C13D831ec7'
ORDER BY block_number DESC
LIMIT 10;
```

```sql
SELECT block_number, address, topic0, tx_hash
FROM logs
WHERE topic0 = event'Transfer(address,address,uint256)'
  AND block_number >= latest - 5000
ORDER BY block_number DESC
LIMIT 25;
```

```sql
SELECT block_number, address AS emitter
FROM logs
WHERE topic1 = '0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
ORDER BY block_number;
```

Not fully implemented yet:

- aggregation execution
- decode execution
- grouping semantics
- joins

## Storage Model

Each log is flattened into a `LogRow` and written to local storage. The storage engine keeps columns such as:

- `block_number`
- `block_hash`
- `timestamp`
- `tx_hash`
- `tx_index`
- `log_index`
- `address`
- `topic0` through `topic3`
- `data`
- `data_len`
- `source`

Hot data is written to a mutable partition. Sealed partitions are indexed for faster lookup.

## Sync Behavior

The current sync path is receipt-based and does not execute the EVM.

- headers are requested from peers
- bodies are requested to recover transaction hashes
- receipts are requested and validated against the header `receiptsRoot`
- logs are extracted from receipts and written locally
- reorgs are handled during the active process lifetime

The node now also persists the latest validated sync head separately from the last block that emitted a log, which fixes resume and status correctness for empty blocks.

## Web UI And Operator Feedback

When running `sync`, the node now reports:

- synced head
- indexed head
- network target
- blocks per minute
- ETA
- logs ingested
- total stored rows

The same information is exposed in the HTTP status surface used by the web UI.

## Development Notes

This repository currently uses Reth networking crates directly. If the intended direction is to avoid those crates entirely and only reuse MIT-licensed ideas or copied code, that still needs to be done.

The original `README.md` describes several future-facing features that are not in this codebase yet. For a realistic implementation snapshot and next steps, see `ANALYSIS_AND_ROADMAP.md`.

## Quality Checks

The current branch has been validated with:

```bash
cargo fmt --all
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

## Near-Term Roadmap

- persist enough canonical header history to improve restart-boundary reorg handling
- add end-to-end network integration tests
- decide whether to implement or remove the unimplemented analytical SQL features
- implement ETH transfer backfill only if it remains a product requirement
- replace or internalize direct Reth crate dependencies if a dependency-light standalone build is required
