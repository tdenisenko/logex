# LogEx

**A trustless Ethereum log client built for transfer intelligence.**

LogEx is a standalone Ethereum client that joins the consensus-layer and
execution-layer P2P networks, verifies the chain, downloads every event log,
and serves those logs through a local SQL engine. It is designed for the one
Ethereum workload that full nodes, RPC providers, indexers, and data warehouses
all handle awkwardly: fast, verifiable, self-hosted analysis of historical and
live event logs.

Run one binary. No external execution RPC. No hosted indexer. No full EVM
execution engine. LogEx syncs from a recent weak-subjectivity checkpoint, uses
the CL P2P network to authenticate the canonical execution chain, uses the EL
P2P network to fetch headers, bodies, and receipts, verifies receipt roots, and
stores logs in a compact columnar format. Recent logs become queryable soon
after startup while historical coverage expands backward toward genesis.

Today, this makes LogEx a trustless ERC token transfer indexer. After
Glamsterdam/EIP-7708, native ETH transfers and burns emit standard logs too, so
the same dataset can reconcile ETH, ERC-20, ERC-721, ERC-1155, and application
events with one local query engine.

## Why LogEx Exists

Ethereum logs are the source of truth for token transfers, protocol activity,
bridge deposits, NFT movements, swaps, liquidations, and almost every
application-level event. But the common ways to access them force a bad trade:

| Tool | What it is good at | What LogEx changes |
| --- | --- | --- |
| Geth, Nethermind, Reth, Erigon | Full execution, broad JSON-RPC support, validator infrastructure | They sync and store state LogEx does not need. LogEx focuses on logs only. |
| Helios | Trust-minimized latest-state RPC on small devices | Helios still needs external RPC endpoints and is not a full historical log database. LogEx uses P2P directly and stores the full log history. |
| Dune-style warehouses | Rich SQL analytics over indexed chain data | They are hosted data products. LogEx brings a smaller, trustless, self-hosted SQL surface to the operator's machine. |
| Raw `eth_getLogs` | Standard API compatibility | It is a filter API, not an analytics engine. LogEx stores logs as queryable rows. |

LogEx is not trying to replace a full execution client. It intentionally does
not execute the EVM, maintain Ethereum state, produce blocks, manage a txpool,
or expose every JSON-RPC method. It is a purpose-built client for verified log
ingestion and log analytics.

## What LogEx Verifies

LogEx accepts a log only after it can tie that log back to Ethereum consensus:

1. A recent weak-subjectivity checkpoint anchors the consensus view.
2. CL P2P light-client data verifies finality and canonical execution payloads.
3. EL P2P peers provide execution headers, bodies, and receipts.
4. Header ancestry is checked through parent hashes.
5. Bodies are checked against header commitments.
6. Receipts are rebuilt into the receipt trie and checked against each header's
   `receiptsRoot`.
7. Cumulative gas and log bloom commitments are checked for consistency.
8. Only logs inside the verified contiguous coverage range are queryable.

This gives LogEx a different trust model from an RPC-backed indexer. Peers can
withhold data, rate-limit, disconnect, or send invalid data, but they cannot
make LogEx accept invalid receipt logs without breaking the header and receipt
commitments authenticated by consensus.

LogEx does not need to build Ethereum's global state trie because event logs are
already committed in transaction receipts. It only needs the receipt trie for
each block it verifies.

## Sync Model

LogEx syncs in two directions from a CL-authenticated execution pivot:

| Path | Direction | Purpose |
| --- | --- | --- |
| Live path | Pivot to head, then follows new blocks | Makes recent logs queryable quickly and tracks the live chain. |
| Historical path | Pivot back to genesis | Expands query coverage until the full event-log history is available. |

The two paths run concurrently. Live syncing does not wait for historical
completion, and historical syncing keeps working while the node follows new
blocks.

The historical path validates all the way to block 0. Pre-Merge blocks are
handled by execution-layer verification, not by consensus-layer historical
sync. The CL is used to authenticate the starting pivot and ongoing canonical
head/finality; EL verification walks the execution chain backward from there.

On production hardware with good network reachability, LogEx is designed to
sync the full log history in a few hours. Actual runtime depends on CPU, disk,
bandwidth, peer quality, and log density. Dense modern blocks are much more
expensive than early sparse blocks, so LogEx tracks log-rate as well as
block-rate when estimating remaining time.

## Storage And Query Engine

Each Ethereum log becomes a flat row:

```text
block_number
block_hash
timestamp
tx_hash
tx_index
log_index
address
topic0
topic1
topic2
topic3
data
data_len
source
```

Rows are stored in immutable compressed segments once sealed. Repeated
addresses and topics are dictionary-compressed, numeric columns use compact
encodings, and large variable data is paged so queries only read the columns
they need. Historical sync writes compacted segments directly so long runs do
not accumulate an unbounded raw-segment backlog.

The query engine exposes:

- LogSQL over HTTP: `POST /query`
- Ethereum-compatible JSON-RPC log queries: `POST /` with `eth_getLogs`
- WebSocket live log subscriptions: `GET /ws`
- gRPC: `LogExService.Query`, `GetLogs`, `StreamLogs`, `GetHeadBlock`
- Dashboard and metrics: `GET /status`
- Health check: `GET /health`

Query responses are capped at 10,000 rows. The default page size is 50 rows.
Use `limit` and `offset` for pagination.

## Comparison

| System | External RPC required | Full historical logs | Verifies data locally | Executes EVM | SQL analytics | Typical sync/storage profile |
| --- | --- | --- | --- | --- | --- | --- |
| LogEx | No | Yes | Yes, for receipt logs | No | Yes | Few-hour log sync target; log-only storage |
| Helios | Yes, execution RPC; optional consensus RPC | No | Yes, for supported proofs from RPC data | No | No | Seconds; little storage |
| Geth | No | Yes, depending on mode and pruning | Yes | Yes | No | Full/archive node storage and sync cost |
| Nethermind | No | Yes, depending on mode and pruning | Yes | Yes | No | Full/archive node storage and sync cost; strong log index support |
| Reth | No | Yes, depending on mode and pruning | Yes | Yes | No | Full/archive node storage and sync cost |
| Erigon | No | Yes, depending on mode and pruning | Yes | Yes | No | Efficient archive/full storage, still a full execution client |
| Besu | No | Yes, depending on mode and pruning | Yes | Yes | No | Full execution-client storage and sync cost |
| Lighthouse, Prysm, Teku, Nimbus, Lodestar | No | No execution receipts by themselves | Yes, for consensus data | No | No | Consensus clients; pair with an EL for execution data |
| Dune and hosted indexers | Yes, provider-operated | Yes | Trust provider pipeline | Provider-dependent | Yes | No local sync; hosted trust and availability |

Helios is the closest conceptual comparison because it is also a Rust light
client. The difference is product shape. Helios turns an untrusted external RPC
into a safer local RPC and optimizes for latest-state access with almost no
storage. LogEx removes the external RPC dependency and optimizes for owning the
full historical log dataset locally.

These sync comparisons are not apples-to-apples because full execution clients
execute blocks and maintain state, while LogEx verifies headers, bodies, and
receipts for log extraction only. They are still useful for expectations:

| System | Representative sync expectation |
| --- | --- |
| Helios | Seconds, because it does not download history and uses RPC-provided data. |
| Geth | Fast for pruned recent-state sync; archive/history modes are much heavier. Geth documentation describes path-based archive bootstrap around weeks on mainnet-class history. |
| Nethermind | Snap/full sync can complete in hours on high-end hardware; archive sync remains heavier. Recent public benchmarks show roughly two-hour recent-history syncs for tuned configurations. |
| Erigon | Optimized storage and archive operation, but still maintains execution-client data and requires NVMe-class storage. |
| LogEx | Few-hour target for full log history because it skips state execution and stores only verified logs. |

## Install

LogEx is a Rust workspace.

```bash
git clone https://github.com/tdenisenko/logex.git
cd logex
cargo build -p logex-node --release
```

The binary is:

```bash
./target/release/logex
```

For development builds:

```bash
cargo run -p logex-node -- --help
```

## Run

A fresh mainnet data directory needs a recent weak-subjectivity checkpoint.
You can provide one directly, or let LogEx resolve the latest finalized
checkpoint from a trusted checkpoint-sync or Beacon API endpoint.

```bash
./target/release/logex \
  --data-dir ./logex-data \
  --checkpoint-sync-url https://YOUR-CHECKPOINT-ENDPOINT \
  sync \
  --http-port 8577 \
  --grpc-port 8578 \
  --discovery-port 30303 \
  --p2p-port 30303 \
  --cl-discovery-port 9000 \
  --cl-p2p-port 9000 \
  --max-peers 100 \
  --cl-max-peers 32
```

Open the dashboard at:

```text
http://127.0.0.1:8577/
```

For a public server, prefer binding behind a firewall, SSH tunnel, or reverse
proxy with TLS. If exposing the dashboard or APIs outside localhost, set a
password:

```bash
./target/release/logex \
  --data-dir /var/lib/logex/mainnet \
  --checkpoint-sync-url https://YOUR-CHECKPOINT-ENDPOINT \
  sync \
  --dashboard-password 'use-a-long-random-password'
```

Authenticated HTTP requests use Basic auth with username `logex`.

## CLI Reference

Global options:

| Option | Use |
| --- | --- |
| `--data-dir <PATH>` | Storage directory. Defaults to the OS application data directory. Use an explicit path for servers. |
| `--log-level <FILTER>` | Tracing filter. Examples: `info`, `debug`, `info,logex_sync=debug`. |
| `--partition-target-rows <N>` | Rows per segment before sealing. Default is `1000000`; larger values reduce segment count, smaller values seal faster. |
| `--config <PATH>` | Optional TOML config file. CLI flags override only where explicitly supplied by the command shape. |
| `--checkpoint <ROOT>` | Weak-subjectivity checkpoint root, or `slot@root`, or a descriptor file path. Required for a fresh data dir unless `--checkpoint-sync-url` resolves one. |
| `--checkpoint-sync-url <URL>` | Beacon/checkpoint endpoint used to fetch a finalized checkpoint or validate a user-supplied checkpoint for freshness. |

`sync` options:

| Option | Use |
| --- | --- |
| `--http-port <PORT>` | HTTP dashboard, REST, JSON-RPC, and WebSocket port. Default `8577`. |
| `--grpc-port <PORT>` | gRPC server port. Default `8578`. |
| `--discovery-port <PORT>` | EL discv4 UDP discovery port. Default `30303`. |
| `--p2p-port <PORT>` | EL TCP listener port. Default `30303`. |
| `--max-peers <N>` | Maximum EL peer sessions. Default `100`. Higher values help only if CPU, memory, and bandwidth can keep up. |
| `--nat <MODE>` | Advertised EL external address. Use `extip:<ip>` or `extaddr:<domain>` on public servers for better inbound retention. |
| `--cl-discovery-port <PORT>` | CL discv5 UDP discovery port. Default `9000`. |
| `--cl-p2p-port <PORT>` | CL libp2p TCP port advertised in the local ENR. Default `9000`. |
| `--cl-max-peers <N>` | Maximum retained CL peers. Default `32`. |
| `--disable-dashboard` | Disable the HTML dashboard while leaving query APIs available. |
| `--dashboard-password <PASSWORD>` | Protect dashboard, `/status`, `/query`, JSON-RPC, and WebSocket routes with Basic auth. |

Storage commands:

```bash
./target/release/logex --data-dir ./logex-data info
./target/release/logex --data-dir ./logex-data build-indexes
./target/release/logex --data-dir ./logex-data compact --limit 20
```

`info` prints storage and checkpoint state. `build-indexes` rebuilds indexes on
the hot partition. `compact` rewrites eligible sealed segments into the current
compression profile; normal historical sync already writes compacted sealed
segments.

## Config File

Example `logex.toml`:

```toml
data_dir = "/var/lib/logex/mainnet"
log_level = "info"
partition_target_rows = 1000000
checkpoint_sync_url = "https://YOUR-CHECKPOINT-ENDPOINT"
nat = "extip:203.0.113.10"
dashboard_enabled = true
dashboard_password = "use-a-long-random-password"
```

Run with:

```bash
./target/release/logex --config ./logex.toml sync --http-port 8577
```

## Query Examples

HTTP LogSQL:

```bash
curl -u logex:YOUR_PASSWORD \
  -H 'content-type: application/json' \
  http://127.0.0.1:8577/query \
  -d '{
    "sql": "SELECT block_number, tx_hash, address, topic0, topic1, topic2, data FROM logs WHERE topic0 = event'\''Transfer(address,address,uint256)'\'' ORDER BY block_number DESC, tx_index DESC, log_index DESC",
    "limit": 50,
    "offset": 0
  }'
```

Ethereum JSON-RPC compatibility:

```bash
curl -u logex:YOUR_PASSWORD \
  -H 'content-type: application/json' \
  http://127.0.0.1:8577/ \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_getLogs",
    "params": [{
      "fromBlock": "0x0",
      "toBlock": "latest",
      "address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
      "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
      "limit": 50,
      "offset": 0
    }],
    "id": 1
  }'
```

Useful SQL columns:

```sql
SELECT block_number, timestamp, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data
FROM logs
WHERE block_number BETWEEN 18000000 AND 18001000
ORDER BY block_number ASC, tx_index ASC, log_index ASC
LIMIT 50;
```

```sql
SELECT COUNT(*) AS total
FROM logs
WHERE topic0 = event'Transfer(address,address,uint256)'
  AND block_number <= latest;
```

## Operating Notes

- Keep `30303/tcp`, `30303/udp`, `9000/tcp`, and `9000/udp` reachable when
  running a public node. Better reachability improves peer retention.
- Use a recent checkpoint. If a checkpoint or persisted consensus snapshot is
  outside the weak-subjectivity freshness window, LogEx requires a fresh data
  directory and a recent checkpoint.
- Graceful shutdown is supported. Use `Ctrl-C` or `SIGTERM`; LogEx coordinates
  shutdown across sync, HTTP, gRPC, indexing, and storage.
- The low-disk guard stops syncing gracefully before the writable data path is
  exhausted.
- Storage grows with logs, not with Ethereum state. LogEx still needs enough
  disk for the full compressed log history and indexes.
- Blocks per second is not enough to judge sync speed. Older blocks have few
  logs; modern blocks are dense. Watch log-rate, CPU, disk headroom, peer count,
  and historical ETA together.

## Development

Common checks:

```bash
cargo fmt --all --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Focused examples:

```bash
cargo test -p logex-storage --lib
cargo test -p logex-query
cargo test -p logex-server
cargo run -p logex-node -- --help
```

Workspace layout:

| Crate | Role |
| --- | --- |
| `logex-node` | CLI, config, runtime wiring, shutdown, disk guard |
| `logex-cl` | Native consensus light client and CL P2P networking |
| `logex-sync` | EL P2P networking, peer management, live sync, historical sync, validation |
| `logex-storage` | Columnar segments, compression, WAL, metadata, integrity checks |
| `logex-index` | Segment indexes |
| `logex-query` | LogSQL, DataFusion integration, native log filters |
| `logex-server` | Dashboard, REST, JSON-RPC, gRPC, WebSocket |
| `logex-types` | Shared chain, log, consensus, and status types |

## Common Questions

### Is LogEx trustless?

For receipt-derived logs, yes: LogEx verifies the consensus-authenticated
execution chain and each block's receipt commitment. It does not trust an RPC
provider to tell it what happened.

The initial weak-subjectivity checkpoint is the trust root, as with Ethereum
light clients generally. A malicious checkpoint can point a light client at the
wrong chain, so checkpoints must be recent and sourced carefully.

### Why does LogEx need a checkpoint?

Ethereum proof-of-stake light clients need a recent weak-subjectivity anchor.
LogEx can resolve a finalized checkpoint from a checkpoint-sync endpoint or
validate a user-supplied checkpoint against one. Stale checkpoints are rejected.

### Why not just use Helios?

Helios is excellent for lightweight latest-state RPC and proof-backed reads
from an untrusted RPC endpoint. LogEx solves a different problem: owning the
complete historical event-log dataset without depending on an external RPC.

### Why not just use Geth, Nethermind, Reth, or Erigon?

Use a full execution client when you need full Ethereum RPC, transaction
execution, state, tracing, mempool behavior, or validator infrastructure. Use
LogEx when the workload is event logs and transfer analytics. LogEx avoids the
state trie and EVM execution because they are unnecessary for verifying receipt
logs.

### Can LogEx prove a log is valid without replaying the transaction?

Yes. The EVM already placed the log in the transaction receipt, and the receipt
trie root is committed in the block header. Rebuilding the receipt trie and
checking the root proves the receipt data is exactly what the canonical block
committed to.

### Does LogEx index native ETH transfers?

On post-Glamsterdam Ethereum, EIP-7708 makes nonzero ETH transfers and burns
emit logs, so LogEx indexes them through the normal receipt path. Before that
fork, native ETH movements are not generally present in receipts; historical
native ETH reconstruction requires tracing or another execution-derived import
path and has a different trust/performance profile.

### Is LogEx a replacement for Dune?

Not completely. Dune is a hosted analytics platform with curated datasets,
shared dashboards, and a large ecosystem. LogEx is a self-hosted verified log
engine. It is attractive when correctness, privacy, local control, fresh data,
or avoiding provider dependency matters.

### Can LogEx serve normal dapps as an RPC endpoint?

Only for the methods it implements. It supports log-centric JSON-RPC such as
`eth_getLogs` and basic chain metadata, but it is not a full Ethereum RPC node.
Full RPC and EVM execution can be considered in a later version.

### What happens during reorgs?

LogEx tracks CL finality and optimistic head updates, follows the canonical
execution chain, and keeps query coverage tied to verified canonical ranges.
Non-finalized live data can reorg like any Ethereum client; finalized data is
stable under Ethereum's consensus assumptions.

## References

- EIP-7708: https://eips.ethereum.org/EIPS/eip-7708
- Ethereum Glamsterdam roadmap: https://ethereum.org/roadmap/glamsterdam/
- Helios: https://github.com/a16z/helios
- Geth sync modes and archive mode: https://geth.ethereum.org/docs/fundamentals/sync-modes
- Nethermind sync and Log Index: https://docs.nethermind.io/next/fundamentals/sync/
- Erigon hardware requirements: https://docs.erigon.tech/get-started/hardware-requirements
