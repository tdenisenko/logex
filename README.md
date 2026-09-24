# LogEx

**A trustless Ethereum Light Client that indexes ERC transfers and all log data without executing the EVM or relying on an external RPC**

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

For fresh data directories, `sync --disable-historical-sync` can run in
forward-only mode from the checkpoint pivot. This is useful when an operator
only wants verified live and recent logs. The flag cannot be added to a data
directory that was already initialized with normal historical sync. If a data
directory was first initialized with this flag, restarting without it converts
the node back to normal historical sync and begins the reverse backfill path.

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
- Persistent live ERC20 subscription management: `/live/erc20-transfers/subscriptions`
- gRPC: `LogExService.Query`, `GetLogs`, `StreamLogs`, `GetHeadBlock`
- Dashboard and metrics: `GET /status`
- Health check: `GET /health`

Query admission uses one shared concurrency limit across REST SQL, JSON-RPC
`eth_getLogs`, and gRPC SQL/native log queries. Configure it with
`sync --query-max-concurrent <N>` or TOML `query_max_concurrent`; the default is
8 and the value must be positive and within the platform semaphore capacity.
Explicit CLI values override config, including an explicit `8`.

Excess queries fail immediately: REST returns HTTP 503 with `query_capacity`
and resource `concurrency`, JSON-RPC returns `-32005`, and gRPC returns
`RESOURCE_EXHAUSTED`. Slots remain owned through query workers and encoded
response bytes; disconnecting a caller does not free a slot while its worker
continues. Metadata, status, cancellation and subscription operations are
exempt. REST's existing exclusive-query behavior still applies.

DataFusion operators, index candidates, fallback scan reads and scan output buffers
use a shared accounted-memory budget across REST and gRPC SQL queries. Configure it with
`sync --query-memory-bytes <BYTES>` or TOML
`query_memory_bytes`; the default is 1 GiB (`1073741824` bytes). The value must be
positive and fit the platform's signed address space. Explicit CLI values override
config, including the default value. A participating operation that cannot reserve
capacity returns HTTP 503 with `query_capacity` and resource `memory`, or gRPC
`RESOURCE_EXHAUSTED`. A capacity failure does not mark storage unhealthy.

Fallback reads reserve source, page-index, selection, decoding and output buffers
before allocation. Fixed raw reads stop at their captured or selected prefix;
variable reads validate the complete captured offset table. Variable page reads
validate every companion length and copy only selected payloads. Each selected
payload allocation stays charged through its final byte clone or slice, without
retaining unselected payloads. Bundle tables, extent/inline vectors and read caches
keep their charges until their final reader is dropped.
Decoded inputs stay charged during Arrow conversion. Numeric vectors transfer
their reservation into Arrow without copying or charging the same buffer twice.
Scan output charges follow the underlying Arrow buffers through clones, slices
and projections. Text/conversion buffers reserve capacity before construction.
Index input and page caches, decoded bitmaps and set operations reserve their
buffer capacity. Protected range reads select the requested interval; legacy
formats retain complete validation. Candidate IDs are refined in place against
accounted columns and canonical bits. Their charge follows the physical plan
and its executing streams, including retained capacity after a pushed limit.

This budget is not a process-RAM ceiling. Some DataFusion operators account after
allocating; planner/scratch allocations, LogEx native fast paths and
result/response conversion are not yet covered. Captured JSON manifests,
paths, codec contexts, fixed builder-control, map nodes and ownership metadata
are also outside the accounted buffer capacity. Snapshot paths and plan/control
objects scale with the number of captured or selected segments. One query can
still consume substantial unaccounted memory. SQL execution has disk spill
disabled, and neither admission nor memory
rejection silently truncates successful results.

SQL requests are limited to 256 KiB of text, 128 syntax tokens (identifiers,
keywords, operators and opening delimiters) and a parser recursion budget of 16.
Literal values, comments, whitespace and list separators do not consume the
syntax budget, so large flat `IN` lists remain usable. Excessive complexity
returns an explicit query error; simplify deeply nested expressions or long
operator chains. See the [input-limit audit](docs/audit/sql-expression-limits.md).

The SQL endpoint has no hidden server-side row cap. Dashboard-generated queries
default to `LIMIT 500`; remove or change the SQL `LIMIT` deliberately for
larger exports. HTTP query requests can also use `limit` and `offset` for
transport pagination.

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

Local-only dashboard and APIs:

```bash
./target/release/logex \
  sync
```

The default data directory is OS-specific:

| OS | Default data directory |
| --- | --- |
| Linux | `$XDG_DATA_HOME/logex`, or `~/.local/share/logex` when `XDG_DATA_HOME` is unset |
| macOS | `~/Library/Application Support/LogEx` |
| Windows | `%APPDATA%\LogEx` |

Server run with an explicit data directory and public dashboard:

```bash
./target/release/logex \
  --data-dir /var/lib/logex/mainnet \
  sync \
  --http-host 0.0.0.0 \
  --http-port 18683 \
  --dashboard-password 'use-a-long-random-password' \
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

When `--http-host 0.0.0.0` is used, open the dashboard at
`http://SERVER_IP:PORT/`. Public HTTP listeners require
`--dashboard-password`. Authenticated HTTP requests use Basic auth with username
`logex`. Prefer firewalling, SSH tunneling, or TLS termination for public
servers.

## P2P Address Selection And IPv6

By default, `--nat any` chooses the safest reachable P2P mode automatically.
Automatic mode only advertises a locally owned public address after that address
family passes a short outbound reachability probe:

1. If a locally owned public IPv4 address is available and reachable, EL
   advertises IPv4.
2. If no public IPv4 is available but a locally owned public IPv6 address is
   available and reachable, EL advertises IPv6.
3. If no public address is available, LogEx runs outbound-only and relies on
   discovery plus persisted known peers learned during previous runs.

Home-router port forwarding cannot be proven safely from inside the process. If
the machine only has a private LAN address but the router forwards Ethereum P2P
ports from a real public WAN address, pass `--nat extip:<public-ip>` explicitly.

When both IPv4 and IPv6 routes exist, LogEx may still dial outbound peers over
both families even though EL advertises only one public family. CL can advertise
IPv6 while EL advertises IPv4 because the beacon network generally has better
IPv6 reachability than the execution network. In the current release, dual-stack
means dual-family outbound dialing plus the safest advertised family per layer;
it does not create two simultaneous advertised EL inbound identities.

Strict IPv6-only mode is available when the host has public IPv6 reachability:

```bash
./target/release/logex \
  sync \
  --p2p-bind-ip :: \
  --nat extip:YOUR_PUBLIC_IPV6 \
  --execution-bootnode 'enode://PUBKEY@[2001:db8::1]:30303?discport=30303'
```

In strict IPv6 mode LogEx binds, advertises, and dials only IPv6 for EL and CL.
Public EL IPv6 discovery is currently much sparser than IPv4, so reliable IPv6
execution bootnodes or a warmed `known-peers.json` cache are recommended for
production. Once an IPv6 EL peer proves useful, LogEx persists it for restart.

## CLI Reference

Use `--help` at any level:

```bash
./target/release/logex --help
./target/release/logex sync --help
./target/release/logex repair --help
./target/release/logex build-indexes --help
./target/release/logex compact --help
```

Global options:

| Option | Default | Use |
| --- | --- | --- |
| `--data-dir <PATH>` | OS app data directory | Storage directory. Use an explicit path for servers, backups, and systemd services. |
| `--expected-volume-mount <PATH>` | disabled | Require this mounted filesystem before storage access; pair with `--expected-volume-uuid`. See [external-volume services](deploy/README.md). |
| `--expected-volume-uuid <UUID>` | disabled | Require the expected filesystem UUID and monitor volume availability. |
| `--log-level <FILTER>` | `info` | Tracing filter. Examples: `debug`, `info,logex_sync=debug`, `info,discv5=error`. The effective default suppresses noisy discovery warnings. |
| `--partition-target-rows <N>` | `1000000` | Target log rows per storage segment before sealing and compaction. Larger values reduce segment count; smaller values seal sooner. |
| `--config <PATH>` | none | Optional TOML config file. Supported keys are listed below. |
| `--checkpoint <CHECKPOINT>` | none | Weak-subjectivity checkpoint root, `slot@root`, or descriptor file path. Required for a fresh data directory unless `--checkpoint-sync-url` resolves one. |
| `--checkpoint-sync-url <URLS>` | Built-in 2-of-3 mainnet quorum | Beacon/checkpoint endpoint used to fetch or validate a recent finalized checkpoint. Use comma-separated URLs to override the default sources and require multi-source agreement. |
| `--help` | n/a | Print help for the root command or selected subcommand. |
| `--version` | n/a | Print the LogEx binary version. |

`sync` options:

| Option | Default | Use |
| --- | --- | --- |
| `--query-max-concurrent <N>` | `8` | Shared admission limit for REST SQL, JSON-RPC logs and gRPC SQL/native queries. Excess requests fail immediately; does not bound query memory. |
| `--query-memory-bytes <BYTES>` | `1073741824` | Shared accounted-memory budget for DataFusion operators, index candidates, fallback reads and scan output in REST/gRPC SQL queries; not a process-RAM ceiling. Native paths, manifest/control metadata and result conversion are not yet covered. |
| `--http-host <IP>` | `127.0.0.1` | HTTP bind host for dashboard, `/status`, `/query`, JSON-RPC, and WebSocket routes. Use `0.0.0.0` only with `--dashboard-password` and network-level protection. |
| `--http-port <PORT>` | `8577` | HTTP dashboard, REST, JSON-RPC, and WebSocket port. Keep this stable for browser sessions and automation. |
| `--grpc-host <IP>` | `127.0.0.1` | gRPC bind host. gRPC is unauthenticated; public gRPC requires `--allow-public-grpc`. |
| `--grpc-port <PORT>` | `8578` | gRPC server port for `LogExService.Query`, `GetLogs`, `StreamLogs`, and `GetHeadBlock`. |
| `--discovery-port <PORT>` | `30303` | Execution-layer discv4 UDP discovery port for IPv4 execution binds. Strict IPv6 execution binds disable discv4. |
| `--p2p-port <PORT>` | `30303` | Execution-layer TCP listener port for the eth protocol. |
| `--p2p-bind-ip <IP>` | auto | Local bind address for EL and CL P2P listeners. Use `::` with `--nat extip:<ipv6>` to select IPv6; LogEx narrows the listener to the concrete local public IPv6 address when it can verify that address locally. |
| `--max-peers <N>` | `100` | Maximum EL peer sessions. Higher values help only if CPU, memory, bandwidth, and disk can keep up. |
| `--nat <MODE>` | `any` | EL NAT/external address resolver advertised to peers. `any` prefers a locally owned public IPv4, then public IPv6, then outbound-only mode. Supported forms include `any`, `none`, `publicip`, `netif`, `extip:<ip>`, and `extaddr:<domain>`. |
| `--execution-bootnode <ENODE_OR_ENR>` | none | Extra EL bootnode seed. Accepts signed `enr:` records and `enode://` records with IP literals or DNS names. Repeat the flag or use comma-separated values. Useful for strict IPv6 when public EL IPv6 discovery is sparse. |
| `--execution-discv5-port <PORT>` | `9200` | Execution-layer discv5 UDP discovery port used by strict IPv6 execution binds. |
| `--cl-discovery-port <PORT>` | `9000` | Consensus-layer discv5 UDP discovery port. |
| `--cl-p2p-port <PORT>` | `9000` | Consensus-layer libp2p TCP port advertised in the local ENR. |
| `--cl-max-peers <N>` | `32` | Maximum dialable CL peers retained from discovery. |
| `--disable-dashboard` | false | Disable the embedded HTML dashboard while leaving HTTP query APIs available. |
| `--dashboard-password <PASSWORD>` | none | Require HTTP Basic auth for dashboard, `/status`, `/query`, JSON-RPC, and WebSocket routes. Username is `logex`. Required for public HTTP. |
| `--allow-public-grpc` | false | Allow gRPC to bind to a non-loopback host. This only disables LogEx's startup guard; use a private network or firewall. |
| `--disable-historical-sync` | false | Fresh-data-dir only. Follow verified CL anchors forward from the checkpoint pivot and skip reverse historical EL backfill. Restart later without the flag to resume normal historical sync. |
| `--repair-corrupt-segments` | false | Inspect existing storage and run exclusive offline repair before normal startup. Uses the same coordinator and `--repair-*` work limits as `repair`. |

`build-indexes` options:

| Option | Default | Use |
| --- | --- | --- |
| `--sealed` | false | Include sealed historical segments. |
| `--hot` | implied when `--sealed` is absent | Include the active hot segment. When neither `--hot` nor `--sealed` is set, hot is implied. |
| `--profile <PROFILE>` | `all` | Index profile to build. Values: `all`, `log-query`, `erc20-transfer`. |
| `--missing-only` | false | Skip segments that already have every index required by the selected profile. |
| `--limit <N>` | none | Maximum number of matching segments to index. |
| `--jobs <N>` | `1` | Concurrent segment index builds, capped by available CPUs and matching segment count. |
| `--from-block <N>` | none | Only index segments whose block range overlaps this lower bound. |
| `--to-block <N>` | none | Only index segments whose block range overlaps this upper bound. |
| `--from-timestamp <SECONDS>` | none | Only index segments whose timestamp range overlaps this lower UTC Unix timestamp. |
| `--to-timestamp <SECONDS>` | none | Only index segments whose timestamp range overlaps this upper UTC Unix timestamp. |

Other commands:

| Command | Options | Use |
| --- | --- | --- |
| `compact` | `--limit <N>` | Compact eligible sealed storage segments. Omit `--limit` to compact all eligible segments. |
| `info` | none | Show storage, checkpoint, and indexed coverage statistics for the data directory. |
| `repair` | `--dry-run`, `--repair-*`, HTTP and EL options | Inspect or repair an existing dataset while ingestion, queries and indexing are paused. See `repair --help` for work allowances. |

Command samples:

```bash
./target/release/logex --data-dir ./logex-data info
./target/release/logex --data-dir ./recent-only --checkpoint-sync-url https://mainnet.checkpoint.sigp.io sync --disable-historical-sync
./target/release/logex --data-dir ./logex-data build-indexes --sealed --missing-only --profile erc20-transfer --jobs 4
./target/release/logex --data-dir ./logex-data build-indexes --sealed --from-block 12000000 --to-block 25100000
./target/release/logex --data-dir ./logex-data compact --limit 20
```

Normal historical sync already writes compacted sealed segments and continuously
builds the current query index profile. `build-indexes` handles interrupted
indexing and changed index profiles. `compact` handles older representations or
changed compression profiles.

`repair --dry-run` performs a read-only assessment and prints JSON to stdout;
logs and diagnostics go to stderr. Exit 0 means local primary commitments and
required index artifacts passed, 2 means pending recovery or repair work, and 1
means a blocker, a limit or an inspection error. These checks do not establish
chain completeness. An expected-volume dry run requires the correct existing
mount and data directory but performs no write probe or free-space check.

`repair` verifies retained WAL recovery, rebuilds derived indexes locally, and
re-fetches damaged ranges only through retained verified consensus anchors and
the existing EL validators. It does not bootstrap or replace consensus trust;
omit checkpoint settings from its arguments and config. Missing or stale anchors,
indeterminate ranges, unavailable peer history and exceeded work allowances stop
the attempt with a diagnostic. Quarantine originals and journals are retained;
rerun the command after resolving a blocker to resume. Never remove quarantine
artifacts merely because a repair attempt stopped.

During writable repair, `/health` and `/status` return HTTP 503 with repair state.
Status and the dashboard retain the normal password policy; query and subscription
routes return 503 and gRPC is not started. The listener stops before normal sync
starts. Automatic startup inspection/repair is opt-in, with
`sync --repair-corrupt-segments` or `repair_corrupt_segments = true` in TOML.
It adds startup scanning work, not per-batch ingestion work. Runtime storage
failures still stop the node; a subsequent opted-in startup performs repair.
Maintenance allowances bound specific inputs and retained row/data work, not
total process memory or a filesystem reservation. Use the diagnostic and
`repair --help` to adjust a relevant allowance rather than discarding data.

This version uses catalog 13 and segment manifest 11. Start sync in a new data
directory when upgrading from earlier native formats; they are rejected without
migration or reset. Retain the original directory until its replacement is
validated. The version checks also prevent earlier native readers and writers
from silently ignoring the new source metadata.

Indexes require a storage-owned namespace and a commitment to the exact logical
row prefix. Standalone legacy raw sources remain scan-readable, but an index
rebuild cannot establish missing identity. Explicit index builds report this
condition; background indexing skips repeated rebuild attempts and records the
reason at debug log level. A complete standalone raw rewrite can establish a new
identity; there is no native in-place identity migration command.
See the [source identity audit](docs/audit/source-publication-identity.md) for
the format and recovery details.

To roll back, use the matching older binary with a preserved pre-upgrade data
directory or backup. Do not change version fields to bypass compatibility checks;
no downgrade migration is provided.

## Config File

Settings resolve in this order: explicitly supplied command-line options,
config-file values, then built-in defaults. An explicit option still wins when
its value equals the default, such as `--nat any` or `--log-level info`. Global
options can appear before or after the subcommand. Relative paths remain relative
to the process working directory.

A valid `RUST_LOG` filter takes precedence over the resolved log setting. The
plain `info` setting continues to suppress discovery warnings. `--disable-dashboard`
always disables the dashboard; command-line bootnodes replace the configured
extra bootnode list. Unknown config keys are errors. Parse errors identify the
file and location without printing config contents; check the supported keys and
types below when correcting them.

Example `logex.toml`:

```toml
data_dir = "/var/lib/logex/mainnet"
log_level = "info"
partition_target_rows = 1000000
query_max_concurrent = 8
query_memory_bytes = 1073741824
checkpoint_sync_url = "https://YOUR-CHECKPOINT-ENDPOINT"
nat = "extip:203.0.113.10"
p2p_bind_ip = "0.0.0.0"
execution_bootnodes = []
execution_discv5_port = 9200
http_host = "127.0.0.1"
grpc_host = "127.0.0.1"
allow_public_grpc = false
dashboard_enabled = true
dashboard_password = "use-a-long-random-password"
```

Supported config keys:

| Key | Type | Use |
| --- | --- | --- |
| `data_dir` | string path | Storage directory. |
| `log_level` | string | Tracing filter. |
| `partition_target_rows` | integer | Target rows per sealed segment. |
| `query_max_concurrent` | positive integer | Shared query admission limit during sync; default 8. Explicit CLI values override config. |
| `query_memory_bytes` | positive integer | Shared SQL operator/candidate/source/scan-output accounted-memory budget in bytes during sync; default 1073741824. Explicit CLI values override config. Coverage limitations are described above. |
| `checkpoint` | string | Weak-subjectivity checkpoint root, `slot@root`, or descriptor path. |
| `checkpoint_sync_url` | string | Checkpoint-sync or Beacon API URL. Comma-separated URLs require quorum agreement. |
| `nat` | string | EL NAT resolver, such as `any` or `extip:203.0.113.10`. |
| `p2p_bind_ip` | IP string | EL/CL P2P bind family. Use `"::"` with an IPv6 `nat` address to select IPv6; LogEx narrows to the concrete local public IPv6 address when it can verify that address locally. |
| `execution_bootnodes` | string array | Extra EL bootnodes as `enode://...` records with IP literals or DNS names, or signed `enr:...` records. |
| `execution_discv5_port` | integer | EL discv5 UDP port used for IPv6 execution discovery. |
| `http_host` | IP string | HTTP bind host. |
| `grpc_host` | IP string | gRPC bind host. |
| `allow_public_grpc` | boolean | Permit non-loopback gRPC binding. |
| `dashboard_enabled` | boolean | Enable or disable the embedded dashboard. |
| `dashboard_password` | string | HTTP Basic auth password for protected HTTP routes. |
| `repair_corrupt_segments` | boolean | Opt in to exclusive offline inspection/repair before sync. Explicit `--repair-corrupt-segments=false` overrides an enabled config setting. Work allowances use CLI `--repair-*` options. |

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

Send one JSON-RPC 2.0 request object per HTTP request. `eth_getLogs` requires
exactly one positional filter; `eth_blockNumber`, `web3_clientVersion`, and
`net_version` take no arguments (omit `params` or use an empty array/object).
Omitting `id` submits a notification: the server awaits its work and returns
HTTP 204 without a response body. An explicit null ID still receives a response;
string and numeric IDs are echoed without numeric rounding. Batch execution is
unsupported (nonempty arrays return HTTP 422; an empty array is an invalid
request). Invalid method arguments return error code `-32602`; storage and
execution failures return `-32603`.

Filters must be objects. Omitted, null and empty address filters match every
address. Topic positions are combined with AND; hashes within a position are
combined with OR. A null position, an empty OR array, or an OR array containing
null is a wildcard, but that topic position must exist in the log. For example,
`topics: [null, null]` requires at least two topics. Every supplied hash must be
valid, including hashes beside a null wildcard; at most four positions are allowed.
`blockHash` cannot be combined with `fromBlock` or `toBlock`.

Numeric block bounds are inclusive. `earliest` means block zero; HTTP `latest`
uses the head captured for that query. `safe`, `finalized` and `pending` return
unsupported-tag errors. Omitted bounds cover the indexed range. Raw WebSocket
log filters accept numeric bounds and `earliest`; explicit `latest` bounds are
unsupported. The query-only `limit` and `offset` fields do not paginate live
WebSocket notifications.

WebSocket ERC20 transfer hook:

```json
{
  "type": "erc20Transfers",
  "subscriptionScope": "dashboard",
  "subscriptionId": "browser-session-id",
  "addresses": ["0xE6c031F4C63e76e453d9A0aAe566D06236d11F95"],
  "tokenAddresses": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
  "minAmount": "0x0000000000000000000000000000000000000000000000000000000005f5e100",
  "maxAmount": "0x000000000000000000000000000000000000000000000000000000003b9aca00"
}
```

`addresses` are wallet addresses and match either ERC20 `from` or `to`.
`tokenAddresses` filters token contracts. Either `addresses` or `tokenAddresses`
must contain at least one entry; omit `tokenAddresses` to watch all token
contracts for the wallets, or omit `addresses` to watch every transfer for the
selected token contracts. `minAmount` and `maxAmount` are optional raw uint256
base-unit bounds, inclusive at both ends. Use decimal strings or `0x`-prefixed
hex strings for the full uint256 range; hex requires at least one digit. Exact
unsigned JSON integers through `18446744073709551615` are also accepted, while
fractional/exponent numbers are rejected. Omitted, null or blank-string bounds
are unset. Address fields accept a string list or an array of individual address
strings; literal objects are invalid.

Matching notifications are streamed as JSON arrays containing token, sender,
recipient, raw amount, timestamp, block, transaction, and log index fields.
Classification requires the standard Transfer signature, exactly three topics,
properly padded sender/recipient addresses and exactly 32 data bytes. It does
not establish that the emitting contract implements the complete ERC20 standard.
Other valid chain logs remain available through raw log subscriptions and queries. WebSocket subscriptions are for live ingested blocks; historical
backfill remains queryable through SQL and JSON-RPC rather than replayed as
alerts. Legacy raw log subscriptions still work by sending `{ "filter": { ... } }`.

When a canonical reorg retires logs, subscriptions receive `removed: true`
notifications before replacement logs. Identify a log by `blockHash` and
`logIndex`; replacement logs can reuse the same transaction hash. Retained
transfer snapshots remove orphaned entries without adding removal records or
refilling previously evicted history. Retained-session streams can receive
removals outside their current filter, covering earlier deliveries before a
filter change; clients should ignore identities they have not seen. A reorg may
span several notification batches. Delivery is transient and does not provide
replay across a process restart.

If a live subscriber falls behind the broadcast buffer, the server ends that
stream instead of silently skipping batches. It attempts close code `1013` with
a reconnect-and-reconcile reason, then releases the connection. The reason may
not arrive over a stalled or broken transport. After losing continuity, clients
should reconcile stored logs through the query APIs; reconnecting alone does
not guarantee replay of every missed log. Retained transfer history is bounded
to 10,000 notifications and can evict older entries. Healthy sends retain their
existing backpressure; the one-second close grace applies only after a gap is
detected. A blocked ordinary send can delay gap detection.

Dashboard live-transfer sessions send `subscriptionScope: "dashboard"` and a
browser-generated `subscriptionId`. They retain recent notifications in server
memory across page refreshes and expire after about one minute without a visible
browser WebSocket connection. Background or unfocused tabs remain subscribed.
Non-dashboard services can create process-lifetime
subscriptions over HTTP:

```bash
curl -u logex:YOUR_PASSWORD \
  -H 'content-type: application/json' \
  http://127.0.0.1:8577/live/erc20-transfers/subscriptions \
  -d '{
    "subscriptionId": "service-usdc-watch",
    "addresses": ["0xE6c031F4C63e76e453d9A0aAe566D06236d11F95"],
    "tokenAddresses": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]
  }'
```

Use `GET /live/erc20-transfers/subscriptions/{id}` to read retained
notifications, `POST /live/erc20-transfers/subscriptions/{id}/clear` to clear
them, and `DELETE /live/erc20-transfers/subscriptions/{id}` to remove the
subscription. Retained live-transfer notifications are bounded in memory and are
cleared when the LogEx process restarts.

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

SQL results use JSON numbers for integer and finite floating-point values, and
strings for exact decimals (including arithmetic promoted to a decimal type).
Exact sums of the hexadecimal data column return base-ten strings for every
aggregate projection in a supported query, including sums of integer literals
alongside the data total. These sums preserve arbitrary integer precision.
Ordinary numeric-only aggregates retain the SQL engine's result types.
Ordinary integer and decimal sums reject overflow in a running subtotal or partial
result, including totals beyond the declared decimal precision. A later value
that would bring the final total into range does not undo an earlier error;
execution order and partitioning can affect such boundary cases. Sliding windows
also check temporary totals while moving between frames, so they can reject a
transition even when each final frame total fits. Floating-point sums retain the
engine's existing rounding and non-finite behavior.
Decimal and duration averages reject overflowing running subtotals, even when
the mathematical mean would fit the result type. Decimal averages also reject
overflowing scale conversions. Their intermediate sums may exceed input
precision; final result precision remains enforced.
Exact data aggregation supports sums, addition/subtraction of sums, conditional
inputs and optional grouping by address. Other aggregate shapes use the general
SQL engine, where data remains hexadecimal text; they may reject it as nonnumeric.
Arrays and objects retain their nested values and explicit nulls. SQL temporal
values use Arrow's textual format; SQL binary values use hexadecimal without a
prefix. The existing log hash, address and data columns keep their `0x` prefix.
Ordinary string literals compare exactly, including case, prefix and whitespace;
use lowercase `0x`-prefixed literals to match these columns. Explicit
`event'…'` and `address'…'` literals retain their existing rewriting semantics.
Non-finite floating-point results remain JSON null. Use unique output names or
aliases: duplicate fields, unsupported result types and invalid temporal values
return an error instead of a partial or misleading result.

## Operating Notes

- Keep `30303/tcp`, `30303/udp`, `9000/tcp`, and `9000/udp` reachable when
  running a public node. Better reachability improves peer retention.
- With `--nat any`, LogEx advertises one reachable public family. If public
  IPv6 is the advertised family but IPv4 outbound routing exists, EL direct
  dials can still use IPv4 DNS candidates while CL/EL listeners stay on IPv6.
- Use a recent checkpoint. If a checkpoint or persisted consensus snapshot is
  outside the weak-subjectivity freshness window, LogEx requires a fresh data
  directory and a recent checkpoint.
- Consensus state uses checksummed checkpoints and an incremental journal in
  `cl/consensus_state/`. Its `CURRENT` file identifies the committed state; keep
  the whole directory together when preserving it. Missing or damaged committed
  artifacts stop startup and `info` with an error; they do not reset stored data.
  Older `cl/consensus_state.bin` and `cl/consensus_state.json` files are preserved
  but cannot be reopened. Use a recent checkpoint in a fresh data directory.
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
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo test --workspace --doc --locked
cargo build -p logex-node --release --locked
```

Focused examples:

```bash
cargo test -p logex-storage --lib
cargo test -p logex-query
cargo test -p logex-server
cargo run -p logex-node -- --help
```

The staged [code audit](docs/audit/README.md) records subsystem coverage and
open findings. See [benchmark instructions](docs/audit/benchmarks.md) for
deterministic storage, indexing, and query performance comparisons.

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
