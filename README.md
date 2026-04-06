# LogEx: A Purpose-Built Ethereum Light Node for Event Log Queries

## Problem Statement

Ethereum execution clients have improved their log query performance significantly in recent years. Geth v1.15.6 introduced a filtermaps-based log index (inspired by EIP-7745), reducing queries over 100K blocks from minutes to under a second. Nethermind shipped a dedicated Log Index that maps addresses and topics to block numbers. These are major improvements over the old bloom filter approach.

However, even with these improvements, fundamental limitations remain:

1. **The query interface is `eth_getLogs` — and only `eth_getLogs`.** No aggregations, no joins, no ordering by decoded values. You can filter, but you can't analyze.

2. **Dense event queries still hit a wall.** Both Geth filtermaps and Nethermind Log Index ultimately need to fetch and deserialize receipt data for matching blocks. When many blocks match (popular contracts, wide time ranges), the bottleneck shifts from "finding the right blocks" to "loading and parsing receipts." Nethermind's own benchmarks confirm this.

3. **ETH transfers are invisible to the log system.** Native ETH movements — including internal calls between contracts — do not emit logs. Tracking them requires full EVM re-execution via `debug_traceBlock` or `trace_block`, which is orders of magnitude slower than log queries. This forces every exchange, wallet, and block explorer to maintain expensive archive nodes or depend on centralized tracing services.

4. **Running a full node for log queries is wildly over-provisioned.** A Geth archive node requires 14+ TB of storage and weeks of sync time. A pruned full node is still 1–2 TB. If all you need is event log data, you're paying for world state, EVM execution, and state trie maintenance you'll never use.

**EIP-7708** (currently Considered for Inclusion in the Glamsterdam hard fork, expected mid-2026) proposes that all ETH transfers and burns automatically emit a Transfer log — the same event signature ERC-20 tokens use. If adopted, every value movement on Ethereum, whether ETH or any token, becomes a log entry.

**LogEx** is designed to capitalize on this moment. It is a standalone Ethereum light node whose sole mission is answering event log queries as fast as a database answers SQL. It syncs block headers and receipts directly over DevP2P with no external dependencies — no full node, no RPC endpoint, no third-party data provider. It does not maintain world state, does not execute transactions, and does not serve as a block producer. It ingests blocks, extracts logs, stores them in a columnar indexed format, and serves queries — including SQL-like aggregations and joins that `eth_getLogs` cannot express.

Post-EIP-7708, LogEx becomes a **complete, self-hosted transfer tracking system**: one lightweight node with zero external dependencies, one SQL query, all transfers (ETH + tokens), sub-100ms latency.

---

## Design Principles

1. **Logs are first-class data, not a side effect.** Traditional clients treat logs as a byproduct of transaction execution. LogEx treats them as the primary dataset.

2. **Index at write time, not at read time.** Every log is decomposed and indexed the moment it arrives. Queries never touch raw block or receipt data.

3. **Columnar over row-oriented.** Log fields (address, topic0–3, data, blockNumber, txIndex, logIndex) are stored in separate columns so queries only read the columns they filter on.

4. **No state trie, no EVM.** LogEx only needs block headers and transaction receipts. This eliminates the single largest storage and computation cost of a traditional client.

5. **SQL semantics over RPC.** Expose a query language that maps naturally to how developers think about logs, compiled down to index lookups internally. `eth_getLogs` compatibility is a baseline, not the ceiling.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Query Layer                          │
│  SQL Parser ─▸ Query Planner ─▸ Index Scanner ─▸ Formatter  │
│  (gRPC / HTTP / WebSocket / JSON-RPC compatible endpoint)   │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                     Index Engine                            │
│                                                             │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐              │
│  │ B+ Tree    │ │ Roaring    │ │ Composite  │              │
│  │ Indexes    │ │ Bitmap     │ │ Covering   │              │
│  │ (per part) │ │ Indexes    │ │ Indexes    │              │
│  └────────────┘ └────────────┘ └────────────┘              │
│                                                             │
│  ┌─────────────────────────────────────────────┐            │
│  │ Columnar Storage Engine (Log Store)         │            │
│  │ address | topic0 | topic1 | topic2 | topic3 │            │
│  │ data | blockNumber | txHash | txIndex |      │            │
│  │ logIndex | blockHash | timestamp             │            │
│  └─────────────────────────────────────────────┘            │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                   Ingestion Pipeline                        │
│                                                             │
│  Block Sync ─▸ Receipt Extractor ─▸ Log Decomposer ─▸ Writer│
│                                                             │
│  Sources:                                                   │
│    • DevP2P (eth/68+) — direct P2P header & receipt sync    │
│    • Consensus layer beacon API — follow chain head via CL  │
│                                                             │
│  No external RPC, no full node dependency.                  │
└─────────────────────────────────────────────────────────────┘
```

---

## Architecture: Standalone Light Node

LogEx is a standalone Ethereum light node. It connects directly to the Ethereum P2P network via DevP2P (eth/68+), syncs block headers and transaction receipts, and validates receipt roots against header commitments — all without requiring a full node, an external RPC endpoint, or any third-party data provider.

This is a deliberate choice over two alternatives that were considered and rejected:

- **Reth Execution Extension (ExEx):** Running as a plugin inside a Reth node would inherit Reth's P2P sync and receipt validation for free, but would require operators to run a full execution client (~1–2 TB storage, EVM execution overhead) just to get log data. This defeats the purpose of a lightweight log-only node.
- **Trusted RPC:** Pointing at an existing full node's RPC endpoint (`eth_getBlockReceipts`) would be the simplest approach, but introduces an external dependency, makes LogEx useless without a separate node running, and caps throughput at what the RPC can serve.

The standalone approach means LogEx has **zero external dependencies** for block data. An operator runs a single binary, it joins the P2P network, and it starts syncing. The tradeoff is that LogEx must implement its own header chain validation, receipt trie verification, and reorg handling — but the result is a self-contained system with minimal storage requirements (~100–170 GB vs 2+ TB for a full node).

---

## Component Deep-Dive

### 1. Ingestion Pipeline

LogEx does not execute transactions. It only needs two things from the network: **block headers** (for block metadata, timestamps, and the receipts root) and **transaction receipts** (which contain the actual logs).

#### 1.1 Block Sync

LogEx syncs directly over DevP2P (eth/68+). It connects to Ethereum execution-layer peers, downloads block headers and transaction receipts, and validates receipt trie roots against the header's `receiptsRoot` commitment.

| Phase | How it works |
|---|---|
| **Header sync** | Download and validate the header chain. Use a checkpoint sync (trusted block hash) or beacon API to anchor the chain, then verify the full header sequence back to genesis or the checkpoint. |
| **Receipt sync** | Request receipts by block hash via `GetReceipts` (eth/68). Validate the receipt trie root against the header. Realistic throughput: ~200–500 blocks/s from peers. |
| **Live following** | Subscribe to new block announcements via DevP2P. Fetch headers and receipts as new blocks are produced. Consensus layer beacon API provides finality information. |

Header chain validation, receipt trie verification, and reorg detection are all handled internally — no external node is involved.

#### 1.2 Receipt Extractor

For each block, the extractor:

1. Receives the list of transaction receipts.
2. Validates the receipt trie root against the block header's `receiptsRoot` (trustless verification).
3. Iterates each receipt, extracts the `logs[]` array.
4. Passes each log to the decomposer.

#### 1.3 Log Decomposer

Each log entry is decomposed into a flat row:

```
LogRow {
  block_number:  u64
  block_hash:    B256
  timestamp:     u64
  tx_hash:       B256
  tx_index:      u32
  log_index:     u32       // global log index within block
  address:       Address   // 20 bytes, the emitting contract
  topic0:        B256      // event signature hash (nullable)
  topic1:        B256      // first indexed param (nullable)
  topic2:        B256      // second indexed param (nullable)
  topic3:        B256      // third indexed param (nullable)
  data:          Bytes     // non-indexed ABI-encoded params
  data_len:      u32       // for fast filtering without loading data
  source:        u8        // provenance: 0 = receipt (consensus-verified),
                           //             1 = trace (EVM-verified via archive node)
}
```

This row is the atomic unit of storage. No further nesting, no trie traversal ever again.

The `source` field records how the log row was obtained. A value of `0` means the log was extracted from a transaction receipt whose Merkle root was validated against the block header's consensus-attested `receiptsRoot`. This is the strongest trust guarantee — the data is cryptographically verified against Ethereum's proof-of-stake consensus. A value of `1` means the log was synthesized from an EVM execution trace obtained from an archive node via the `trace_block` RPC method. This data is correct if and only if the archive node executed the transaction faithfully — there is no independent cryptographic verification. See section 1.4 for full details on when and why trace-derived rows are created, and the trust implications of each source type.

#### 1.4 ETH Transfer Coverage: The `--backfill-eth-transfers` Flag

This section describes how LogEx handles native ETH transfers — the single most important gap in Ethereum's log system, and the feature that transforms LogEx from a fast log index into a complete transfer tracking system.

##### 1.4.1 The Problem

ERC-20 token transfers emit a `Transfer(address,address,uint256)` log event. This log is recorded in the transaction receipt and is available to any client that can read receipts — including LogEx. No EVM execution is required to access it.

Native ETH transfers do not emit any log. When one account sends ETH to another — whether via a simple transaction, an internal `CALL` between contracts, a `SELFDESTRUCT`, or a contract deployment with value — no record of that transfer appears in the receipt's log array. The only way to discover these transfers is to re-execute the transaction using the EVM, stepping through every opcode, and observing which `CALL`, `CALLCODE`, `DELEGATECALL`, `CREATE`, `CREATE2`, and `SELFDESTRUCT` operations moved ETH between accounts. This requires full world state (the state trie, all account balances, all contract bytecode, all storage slots) at the exact block height of the transaction.

This is why exchanges and wallets that need to track ETH deposits are forced to either run archive nodes with tracing enabled (14+ TB, weeks of sync) or pay centralized providers for trace data. It is the single largest operational pain point in Ethereum infrastructure.

EIP-7708, currently Considered for Inclusion in the Glamsterdam hard fork (expected mid-2026), proposes that the EVM automatically emit a Transfer log for every ETH-moving operation. If adopted, all ETH transfers will appear in receipts as standard log entries from the fork block onward. LogEx ingests them like any other log — no special handling needed.

However, EIP-7708 is not retroactive. Historical blocks (everything before the fork) will never have ETH transfer logs in their receipts. For operators who need complete transfer history — every ETH and token transfer from genesis to present — LogEx provides an opt-in backfill mechanism.

##### 1.4.2 The Flag

```
logex --backfill-eth-transfers --trace-rpc http://your-archive-node:8545
```

The `--backfill-eth-transfers` flag is **off by default**. When off, LogEx indexes only what is present in transaction receipts. For blocks before the EIP-7708 fork, this means ERC-20/721/1155 token transfers are indexed but native ETH transfers are not. For blocks after the fork, everything is indexed. The software is correct and complete for what it claims to cover.

When the flag is set to `true`, LogEx activates a background backfill process that traces historical blocks to extract ETH transfers. This requires the operator to provide a `--trace-rpc` endpoint pointing to an Ethereum archive node that supports the `trace_block` RPC method (OpenEthereum/Erigon trace format) or the `debug_traceBlockByNumber` method (Geth debug format). The archive node performs the actual EVM execution — LogEx never runs the EVM itself.

The two flags must be used together. If `--backfill-eth-transfers` is set without `--trace-rpc`, LogEx refuses to start and prints:

```
Error: --backfill-eth-transfers requires --trace-rpc <url>
  The trace RPC endpoint must point to an Ethereum archive node with
  trace API enabled (Erigon, Geth with --gcmode=archive, or similar).
  LogEx does not execute transactions itself — it extracts ETH transfer
  data from the archive node's trace output.
  Example: logex --backfill-eth-transfers --trace-rpc http://localhost:8545
```

##### 1.4.3 Startup Validation

Before beginning any backfill work, LogEx validates that the provided trace RPC endpoint is functional and capable. The startup validation performs the following checks in order:

**Step 1: Connectivity check.** LogEx calls `eth_blockNumber` on the trace RPC endpoint. If the call fails, LogEx exits with an error indicating the endpoint is unreachable. If the call succeeds, LogEx records the endpoint's current block height.

**Step 2: Archive node verification.** LogEx calls `trace_block` (or `debug_traceBlockByNumber`) for a known early block — specifically block 46147, the first block on Ethereum mainnet that contains an internal ETH transfer (the first transaction ever sent to a contract). If the endpoint returns an error indicating it does not support tracing, or if it returns an error indicating the state is not available (i.e., the node is pruned), LogEx exits with a clear error message:

```
Error: Trace RPC endpoint does not support trace_block or does not have
  historical state available. ETH transfer backfill requires an archive
  node with trace API enabled.
  Tested block: 46147
  Endpoint: http://localhost:8545
  Response: <actual error message from the node>
```

**Step 3: Response format detection.** Different Ethereum clients return trace data in different formats. Erigon and OpenEthereum use the `trace_block` method and return an array of trace objects with `action.callType`, `action.from`, `action.to`, `action.value` fields. Geth uses `debug_traceBlockByNumber` with a tracer configuration and returns a different structure. LogEx parses the response from Step 2 to detect which format the endpoint uses and configures its trace parser accordingly. If the response matches neither known format, LogEx exits with an error suggesting which client versions are supported.

**Step 4: Sanity check.** LogEx verifies that the trace response for block 46147 contains at least one internal ETH transfer with a nonzero value. This confirms that the node is not only returning trace data, but that the data contains the ETH movement information LogEx needs. If the traces are empty or contain no value transfers, LogEx warns the operator but continues — some trace configurations may filter out certain call types.

Only after all four validation steps pass does LogEx proceed with the backfill.

##### 1.4.4 The Backfill Process

The backfill runs as a background process, completely independent of the primary receipt ingestion pipeline. The primary pipeline (receipt extraction, log decomposition, indexing) is never blocked or slowed by the backfill. Operators can begin querying receipt-derived logs immediately while the backfill progresses in the background.

**Backfill scope.** The backfill needs to trace every block from genesis (block 0) up to the EIP-7708 fork block. LogEx determines the fork block from its chain configuration. If the fork has not yet been activated (i.e., the fork block is not yet known because the Glamsterdam fork date hasn't been set), LogEx backfills up to the current chain head and continues tracing new blocks as they arrive until the fork activates. Once the fork is active, the backfill marks itself as complete for all blocks up to the fork block, and no further tracing is performed — new blocks have ETH transfer logs in their receipts natively.

**Block processing.** For each block in the backfill range, LogEx:

1. Calls `trace_block(block_number)` on the trace RPC endpoint.
2. Receives the array of trace objects for every transaction in that block.
3. Walks each trace object and identifies value-transferring operations: any trace where `action.value` is nonzero and the `type` is `call`, `create`, or `suicide`/`selfdestruct`. For `call` types, only `action.callType` values of `call`, `callcode`, and `delegatecall` with nonzero value are included. Static calls (`staticcall`) cannot transfer value and are skipped.
4. For each identified ETH transfer, checks that the trace's `error` field is empty or absent. Failed calls (e.g., out-of-gas reverts) do not actually transfer ETH, even if `action.value` is nonzero. Only successful (non-reverted) transfers are recorded.
5. Constructs a synthetic LogRow for each valid ETH transfer (see section 1.4.5 for the exact format).
6. Writes the synthetic LogRows to the columnar store using the same write path as receipt-derived logs.

**Concurrency.** The backfill can process multiple blocks in parallel. A configurable `--backfill-concurrency` flag (default: 4) controls how many concurrent `trace_block` requests are in flight at once. Higher values increase backfill speed but put more load on the archive node. Operators running their own dedicated archive node can increase this. Operators using a shared or rate-limited archive node should keep it low.

```
logex --backfill-eth-transfers \
      --trace-rpc http://localhost:8545 \
      --backfill-concurrency 8
```

**Rate limiting.** If the trace RPC endpoint returns HTTP 429 (Too Many Requests) or an equivalent rate-limit error, LogEx automatically backs off using exponential backoff with jitter, starting at 1 second and capping at 60 seconds. The backfill continues automatically when the rate limit clears. No operator intervention is required.

**Progress tracking and resumability.** The backfill maintains a checkpoint in LogEx's metadata store: the block number of the last successfully traced and ingested block. If LogEx is stopped and restarted (for any reason — crash, upgrade, maintenance), the backfill resumes from the checkpoint, not from the beginning. No work is repeated.

The checkpoint is updated atomically with the write of the corresponding log rows. This means that if LogEx crashes mid-block, it will re-trace that block on restart rather than risk having partially-written data. Re-tracing a single block is cheap and guarantees consistency.

LogEx logs backfill progress periodically to stdout:

```
INFO [ETH backfill] Progress: block 12,450,000 / 21,000,000 (59.3%)
  Speed: ~850 blocks/sec | ETA: ~2h 48m | Transfers found: 478,231,009
```

**Backfill completion.** When the backfill reaches the EIP-7708 fork block (or the current chain head if the fork hasn't activated yet), it logs a completion message and stops the background process. The `--backfill-eth-transfers` flag can remain set in the configuration without any ongoing cost — it simply has no more work to do.

```
INFO [ETH backfill] Complete. Traced blocks 0 – 21,000,000.
  Total ETH transfers indexed: 823,491,207
  Backfill duration: 14h 22m
  All future ETH transfers will be captured from receipts via EIP-7708.
```

##### 1.4.5 Synthetic LogRow Format for ETH Transfers

When LogEx extracts an ETH transfer from trace data, it constructs a LogRow that is structurally identical to a receipt-derived Transfer log. This is the key design decision that makes ETH transfers queryable with the same SQL syntax as token transfers — there is no separate table, no separate query path, no special handling in the query layer.

The synthetic LogRow fields are populated as follows:

```
LogRow {
  block_number:  <block number where the transfer occurred>
  block_hash:    <hash of the block>
  timestamp:     <block timestamp>
  tx_hash:       <hash of the transaction that caused the transfer>
  tx_index:      <index of the transaction within the block>
  log_index:     <synthetic index, see below>
  address:       0x0000000000000000000000000000000000000000
  topic0:        0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
  topic1:        <sender address, left-padded to 32 bytes>
  topic2:        <recipient address, left-padded to 32 bytes>
  topic3:        <null>
  data:          <transfer amount in wei, ABI-encoded as uint256>
  data_len:      32
  source:        1 (trace-derived)
}
```

**Field-by-field explanation:**

`address` is set to the zero address (`0x000...000`). This is deliberate and serves three purposes. First, it matches the convention proposed in EIP-7708, where system-generated logs use a special address that no real contract can occupy. Second, it makes it impossible for a synthetic ETH transfer log to be confused with a real ERC-20 Transfer log — ERC-20 logs have the token contract's address in this field. Third, it allows operators to distinguish ETH transfers from token transfers in queries: `WHERE address = '0x000...000'` returns only ETH transfers, `WHERE address != '0x000...000'` returns only token transfers, and omitting the address filter returns both.

`topic0` is set to the keccak256 hash of `Transfer(address,address,uint256)`, which is `0xddf252ad...`. This is the same event signature used by ERC-20 tokens. Using the same topic0 means that a query like `WHERE topic0 = event'Transfer(address,address,uint256)' AND topic2 = address'0xMyAddress'` automatically returns both token transfers and ETH transfers to the specified address — which is exactly what an exchange deposit tracker wants.

`topic1` is the sender (the `from` address of the ETH transfer), left-padded to 32 bytes to match the ABI encoding of an indexed `address` parameter. For a simple ETH transaction, this is the transaction's `from` field. For an internal call, this is the `action.from` field from the trace.

`topic2` is the recipient (the `to` address), also left-padded to 32 bytes. For a simple transaction, this is the transaction's `to` field. For an internal call, this is the `action.to` field from the trace. For contract creation with value, this is the address of the newly created contract.

`topic3` is null. The standard ERC-20 Transfer event has only two indexed parameters (from and to), so there is no third topic.

`data` contains the transfer amount in wei, ABI-encoded as a 256-bit unsigned integer (32 bytes, big-endian, left-padded with zeros). This matches how ERC-20 Transfer events encode the `value` parameter in their data field.

`log_index` requires special handling. Receipt-derived logs have natural log indexes assigned by the EVM — each log within a block gets a sequential index (0, 1, 2, ...). Synthetic trace-derived logs are not part of the receipt and do not have natural log indexes. LogEx assigns synthetic log indexes starting from a high offset (e.g., `1,000,000 + trace_index`) to ensure they never collide with receipt-derived log indexes in the same block. The exact offset is a configuration constant. This ensures that the `(block_number, log_index)` pair remains unique across all rows, which the storage engine relies on for deduplication.

`source` is set to `1`, indicating this row was derived from trace data. This allows queries to filter by provenance: `WHERE source = 0` returns only cryptographically verified receipt data, `WHERE source = 1` returns only trace-derived data, and omitting the filter returns both.

##### 1.4.6 Handling Edge Cases

**Block rewards and uncle rewards.** Miners (pre-merge) and validators (post-merge) receive block rewards and priority fees. These are ETH balance changes, but they are not the result of EVM execution — they are protocol-level operations. They do not appear in traces and are not internal calls. LogEx does not index block rewards as transfers. This matches EIP-7708's scope, which also does not cover block rewards (only value-transferring CALLs and SELFDESTRUCTs).

**Zero-value calls.** Some contracts make internal calls with zero value (e.g., calling a function on another contract without sending ETH). These appear in traces but have `action.value = 0`. LogEx skips zero-value calls — they are not transfers.

**Self-transfers.** It is possible for a contract to call itself with value (the sender and recipient are the same address). These are rare but valid. LogEx records them as-is — the query layer does not need to special-case them.

**Failed transactions.** A transaction that reverts entirely (status = 0 in the receipt) does not produce any state changes — no ETH is transferred, no logs are emitted. The traces for such a transaction will show call operations that ultimately failed. LogEx only records ETH transfers from traces where the individual trace's `error` field is absent (meaning that specific call succeeded). However, in a complex transaction, some internal calls may succeed while a later call causes a revert that undoes everything. To handle this correctly, LogEx must check the trace's `result` field — if the trace is marked as having been reverted (some trace formats include a `reverted` flag or omit the `result` block), the transfer is not recorded. The safest approach: only record a transfer if the trace has a non-null `result` and no `error`, and the overall transaction receipt has `status = 1`.

**Pre-Byzantium transactions (before block 4,370,000).** Before the Byzantium hard fork, transaction receipts did not include a `status` field. The only way to determine if a transaction succeeded was to check whether the gas used equals the gas limit (indicating out-of-gas failure). For these blocks, LogEx relies on the trace data's own error reporting rather than the receipt status. If a trace's `error` field is set (e.g., `"out of gas"`, `"reverted"`), the transfer is not recorded.

**SELFDESTRUCT with value.** When a contract self-destructs, it sends its remaining ETH balance to a designated address. This appears as a `suicide` (or `selfdestruct`) type trace with a nonzero value. LogEx records this as a transfer from the self-destructing contract to the beneficiary address.

**Transaction-level ETH transfers (simple sends).** A plain ETH transfer from one EOA to another (the most common type of ETH transfer) appears as a top-level trace with `type = "call"` and a nonzero `action.value`. LogEx captures these the same way it captures internal calls. There is no special case for "simple" vs. "internal" transfers — the trace format represents both uniformly.

##### 1.4.7 Backfill Performance Expectations

The backfill speed is limited by the archive node's trace execution speed, not by LogEx's ingestion speed. Writing synthetic log rows to the columnar store is trivially fast compared to the time the archive node spends re-executing transactions.

Realistic throughput estimates based on common archive node hardware:

| Archive Node | Expected Throughput | Full Backfill Time (20M blocks) |
|---|---|---|
| Erigon on NVMe SSD, dedicated | 500–1,500 blocks/s | 4–12 hours |
| Reth with debug/trace, dedicated | 300–1,000 blocks/s | 6–18 hours |
| Geth archive on NVMe, dedicated | 200–800 blocks/s | 7–24 hours |
| Shared RPC provider with tracing | 50–200 blocks/s | 1–5 days |

These numbers vary significantly based on block density (modern blocks with hundreds of transactions trace much slower per block than early blocks with few transactions), disk speed, available RAM for the archive node's state cache, and network latency between LogEx and the archive node. Running LogEx and the archive node on the same machine or local network eliminates network latency and is strongly recommended.

##### 1.4.8 Post-EIP-7708 Behavior

Once EIP-7708 activates, the Ethereum EVM itself emits Transfer logs for every ETH movement. These logs appear in transaction receipts as standard log entries. LogEx's primary receipt-based ingestion pipeline captures them automatically with `source = 0` (receipt-derived, consensus-verified).

At this point:

- The `--backfill-eth-transfers` flag, if still set, has no effect on new blocks. LogEx detects that the current block is past the EIP-7708 fork height and does not issue any trace calls.
- If the backfill completed successfully (all blocks from genesis to the fork block have been traced), the operator has a complete, gap-free record of every ETH transfer in Ethereum's history.
- If the backfill was not enabled, there is a gap: pre-fork blocks have no ETH transfer data. The operator can enable the flag later and the backfill will start from the beginning (or from its last checkpoint if it was previously started and stopped).
- The `--trace-rpc` endpoint is no longer needed for ongoing operation. The operator can shut down their archive node if it was only being run for the backfill. LogEx will continue to capture all ETH transfers from receipts going forward.

##### 1.4.9 Operator Decision Guide

| Operator Type | Needs Historical ETH? | Recommended Configuration |
|---|---|---|
| Exchange tracking deposits | Yes — must detect all incoming ETH | `--backfill-eth-transfers --trace-rpc <archive-node>` |
| Wallet showing user history | Yes — users expect complete history | `--backfill-eth-transfers --trace-rpc <archive-node>` |
| DeFi protocol analytics | Maybe — depends on whether ETH flows matter | Enable if analyzing ETH-denominated protocols (WETH wrapping, ETH pairs) |
| Token-only analytics (ERC-20/721) | No — receipt logs are sufficient | Default (flag off) |
| Post-fork-only deployment | No — EIP-7708 covers everything going forward | Default (flag off) |
| Block explorer | Yes — must show all transfers | `--backfill-eth-transfers --trace-rpc <archive-node>` |

#### 1.5 Reorg Handling

LogEx subscribes to chain head updates. On a reorg:

1. Identify the fork point (common ancestor block).
2. Mark all log rows whose `block_hash` matches any orphaned block as non-canonical. Note: matching by `block_hash` is essential — both the orphaned and new canonical blocks share the same `block_number`.
3. Re-ingest the new canonical chain from the fork point.
4. Non-canonical rows are deleted in a background compaction pass.

A `canonical` bitmap column allows queries to automatically exclude orphaned logs without deleting them immediately (useful for debugging reorgs).

---

### 2. Columnar Storage Engine (Log Store)

This is the heart of LogEx. Instead of storing logs inside serialized receipt blobs, each field becomes an independent column.

#### 2.1 Storage Layout

The storage engine is inspired by Apache Parquet / ClickHouse MergeTree, adapted for append-mostly blockchain data.

```
data/
├── partitions/
│   ├── p_000000/                    # partition 0 (blocks 0 – ~2M, early era)
│   │   ├── address.col              # sorted, compressed addresses
│   │   ├── topic0.col
│   │   ├── topic1.col
│   │   ├── topic2.col
│   │   ├── topic3.col
│   │   ├── block_number.col
│   │   ├── tx_hash.col
│   │   ├── tx_index.col
│   │   ├── log_index.col
│   │   ├── data.col                 # variable-length, loaded only when needed
│   │   ├── timestamp.col
│   │   ├── block_hash.col
│   │   ├── source.col               # provenance: 0 = receipt, 1 = trace
│   │   ├── canonical.bitmap
│   │   ├── nulls.bitmap             # tracks which topic slots are null
│   │   ├── indexes/                 # per-partition indexes
│   │   │   ├── address.bptree
│   │   │   ├── topic0.bptree
│   │   │   ├── address_topic0.composite
│   │   │   └── block_number.bptree
│   │   └── metadata.json            # row count, min/max block, stats
│   ├── p_000001/                    # partition 1
│   │   └── ...
│   └── latest/                      # hot partition, in-memory + WAL
│       └── ...
├── backfill/
│   └── eth_transfers.json           # backfill checkpoint:
│                                    #   { "last_traced_block": 12450000,
│                                    #     "fork_block": 21000000,
│                                    #     "status": "in_progress",
│                                    #     "transfers_found": 478231009 }
└── wal/
    └── pending.wal                  # write-ahead log for crash recovery
```

Key design decision: **indexes are per-partition**, not global. This means partition pruning (step 1 of the query planner) eliminates both data and index I/O. New partitions can be built and compressed independently. Old partitions are fully immutable once sealed.

#### 2.2 Partitioning Strategy

Partitions are **size-based, not block-range-based**. Each partition targets approximately 50–100 million log rows. This matters because log density has increased dramatically over Ethereum's history:

- Blocks 0–4M (2015–2017): sparse logs, maybe 10–50 per block. A 4M-block partition might hold 50M rows.
- Blocks 18M–19M (2023–2024): dense logs, 500–2000+ per block. A 1M-block partition might hold 500M+ rows.

Fixed block-range partitions would create wildly uneven partition sizes, hurting both compression ratios and query performance. Size-based partitioning keeps each partition's index and column files in a predictable size range (~1–3 GB per partition), optimizing for memory-mapped access and OS page cache efficiency.

Partition metadata records the block range covered, enabling the query planner to prune by block number before opening any partition files.

#### 2.3 Compression

Each column type gets tailored compression:

| Column | Encoding | Rationale |
|---|---|---|
| `address` (20B) | Dictionary + bitpacking | High cardinality overall, but very repetitive within a partition (same contracts emit many logs) |
| `topic0` (32B) | Dictionary + bitpacking | Low cardinality (~50K unique event sigs in practice). Dictionary encoding is extremely effective. |
| `topic1–3` (32B) | Raw + zstd | High cardinality (addresses, token IDs). Dictionary encoding doesn't help. |
| `block_number` (u64) | Delta + bitpacking | Monotonically increasing within a partition, tiny deltas. |
| `tx_index` (u32) | Bitpacking | Small integers. |
| `data` (variable) | LZ4 (fast decompress) | Only loaded on demand, so optimize for decompression speed over ratio. |
| `timestamp` (u64) | Delta-of-delta | 12s cadence post-merge, extremely compressible. |

Expected storage: **~100–150 GB for all mainnet logs** (vs ~2 TB for a full Geth node, 14+ TB for an archive node). This is a rough estimate — actual size depends heavily on compression effectiveness, which needs to be validated with real data during prototyping.

---

### 3. Index Engine

Indexes are what make LogEx faster than even the improved log indexes in Geth and Nethermind. The key difference: Geth filtermaps and Nethermind Log Index point you to *blocks* that contain matching logs, requiring you to then load and parse receipt data. LogEx indexes point directly to *row IDs* in the columnar store, so the query goes straight from index to column data with no intermediate deserialization.

#### 3.1 Primary Indexes (B+ Tree, per-partition)

Each partition has its own B+ tree indexes mapping values to sorted lists of row IDs (stored as roaring bitmaps):

- **`address`** → "Give me all logs from contract 0xdAC17..."
- **`topic0`** → "Give me all Transfer events across all contracts"
- **`block_number`** → "Give me all logs in block 18,500,000"

Per-partition indexes mean that adding a new partition (as the chain grows) never requires rebuilding existing indexes. Old partitions are fully immutable once sealed.

#### 3.2 Composite Indexes

For the most common query patterns, prebuilt composite indexes eliminate the need for bitmap intersection at query time:

- **`(address, topic0)`** → "All Transfer events from USDT" — single index seek.
- **`(address, topic0, block_number)`** → "All Transfer events from USDT in blocks 18M–18.1M" — single range scan.
- **`(topic0, topic1)`** → "All Transfers where `from` is address X" — one seek, no address filter needed.

#### 3.3 Bitmap Indexes

For multi-predicate filtering, roaring bitmaps provide:

- Fast intersection (`AND` → bitmap AND)
- Fast union (`OR` → bitmap OR)
- Fast negation (`NOT` → bitmap XOR with universe)
- Compressed storage (roaring bitmaps are space-efficient for clustered data)

A query like `WHERE address = X AND topic0 = Y AND block_number BETWEEN A AND B` becomes:

```
result = bitmap_and(
  address_index.get(X),
  topic0_index.get(Y),
  block_range_bitmap(A, B)
)
```

This runs in microseconds, not minutes.

---

### 4. Query Layer

#### 4.1 Query Language: LogSQL

A SQL dialect purpose-built for event logs. It compiles to index operations, never to block scans.

```sql
-- Basic: all Transfer events from USDT in the last 1000 blocks
SELECT *
FROM logs
WHERE address = '0xdAC17F958D2ee523a2206206994597C13D831ec7'
  AND topic0 = event'Transfer(address,address,uint256)'
  AND block_number >= latest - 1000;

-- Decoded: automatic ABI decoding of indexed params
SELECT
  topic1 AS from_address,
  topic2 AS to_address,
  decode(data, 'uint256') AS value,
  block_number,
  timestamp
FROM logs
WHERE topic0 = event'Transfer(address,address,uint256)'
  AND topic1 = address'0xBigWhale...'
  AND block_number BETWEEN 18000000 AND 18100000
ORDER BY block_number DESC
LIMIT 50;

-- Aggregation: count events per block
SELECT block_number, COUNT(*) as event_count
FROM logs
WHERE address = '0xUniswapV2Pair...'
  AND topic0 = event'Swap(address,uint256,uint256,uint256,uint256,address)'
GROUP BY block_number
ORDER BY event_count DESC
LIMIT 10;

-- Post-EIP-7708: all ETH and token transfers to a deposit address
SELECT
  address AS token_contract,
  topic1 AS sender,
  decode(data, 'uint256') AS amount,
  block_number,
  timestamp
FROM logs
WHERE topic0 = event'Transfer(address,address,uint256)'
  AND topic2 = address'0xMyDepositAddress...'
  AND block_number >= latest - 50000
ORDER BY block_number DESC;

-- ETH transfers only (system logs from trace backfill or EIP-7708)
SELECT
  topic1 AS sender,
  topic2 AS recipient,
  decode(data, 'uint256') AS wei_amount,
  block_number
FROM logs
WHERE topic0 = event'Transfer(address,address,uint256)'
  AND address = '0x0000000000000000000000000000000000000000'
  AND block_number BETWEEN 18000000 AND 18100000;

-- Token transfers only (exclude ETH)
SELECT * FROM logs
WHERE topic0 = event'Transfer(address,address,uint256)'
  AND address != '0x0000000000000000000000000000000000000000'
  AND topic2 = address'0xMyAddress...';

-- Audit query: show provenance of all data for a given block
SELECT source, COUNT(*) as row_count
FROM logs
WHERE block_number = 18500000
GROUP BY source;
-- Returns: source=0 (receipt-verified): 1847 rows
--          source=1 (trace-derived):     23 rows
```

**Special syntax extensions:**

| Syntax | Meaning |
|---|---|
| `event'Transfer(address,address,uint256)'` | Auto-computes keccak256 topic0 hash |
| `address'0xABC...'` | Left-pads to 32 bytes for topic matching |
| `latest` | Resolves to current chain head block number |
| `decode(data, 'uint256')` | ABI-decodes the data field inline |
| `decode(data, '(uint256,uint256)')` | Decodes tuple data |

**Deliberate limitations of v1:** Self-joins (`JOIN logs ON tx_hash`) are deferred to a future version. Joining a multi-billion-row table to itself on a high-cardinality column like `tx_hash` is an expensive operation that requires careful query planning and potentially a dedicated `tx_hash` index. For v1, correlated queries should be handled application-side: query the first event set, extract the `tx_hash` values, then issue a second query with `WHERE tx_hash IN (...)`.

#### 4.2 Query Planner

The planner takes a parsed AST and decides the cheapest execution path:

```
Input AST
    │
    ▼
┌──────────────────────────┐
│ 1. Partition Pruning     │  Use partition metadata (min/max block)
│    (block_number range)  │  to eliminate irrelevant partitions.
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│ 2. Index Selection       │  Pick the most selective index
│    (per partition)       │  from the partition's local indexes.
│                          │  Prefer composite > single column.
│                          │  Estimate cardinality from stats.
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│ 3. Bitmap Intersection   │  AND/OR the result bitmaps
│    (for multi-predicate) │  from selected indexes.
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│ 4. Column Fetch          │  Only load columns mentioned
│    (projection pushdown) │  in SELECT + ORDER BY.
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│ 5. Post-processing       │  LIMIT, ORDER BY, GROUP BY,
│                          │  ABI decoding, formatting.
└──────────────────────────┘
```

Key optimization: **the `data` column is never loaded unless explicitly selected or decoded.** Since it's the largest column and variable-length, skipping it for filter-only queries saves enormous I/O.

#### 4.3 Serving Interfaces

LogEx exposes its query engine through multiple interfaces:

- **JSON-RPC compatible** (`eth_getLogs` drop-in): LogEx can sit behind any existing dApp's RPC URL as a specialized log endpoint. No application code changes needed. This is the adoption wedge.
- **HTTP REST** (POST a LogSQL query, get JSON results): For applications that want the full SQL query language.
- **gRPC** (streaming results for large result sets): For data pipelines and bulk extraction.
- **WebSocket subscriptions** (live tail: new logs matching a filter streamed in real-time).

---

### 5. Live Streaming Engine

Beyond historical queries, LogEx supports real-time log subscriptions:

```sql
SUBSCRIBE
SELECT *
FROM logs
WHERE address = '0xUniswapV2Router...'
  AND topic0 = event'Swap(...)';
```

Implementation:

1. New blocks arrive via the ingestion pipeline.
2. Logs are decomposed and written to the hot partition.
3. A matcher evaluates each new log against all active subscriptions.
4. Matching logs are pushed to subscribers via WebSocket/gRPC stream.

The matcher uses a **compiled filter trie**: all active subscription filters are merged into a single trie structure so each new log is evaluated against all subscriptions in one pass, not N separate passes.

---

## Sync & Storage Estimates

### Initial Sync (Ethereum Mainnet, ~20M blocks)

| Component | Estimate | Notes |
|---|---|---|
| Total logs on mainnet | ~3–4 billion | Rough estimate, growing ~500M/year |
| Receipt data to download | ~700 GB (compressed) | |
| Sync time (P2P, ~200–500 blocks/s) | 1–3 days | P2P peers rate-limit receipt requests; varies with peer quality |
| Final indexed storage (receipts only) | ~100–150 GB | Needs validation with real data |
| ETH transfer backfill time | 4–24 hours | Depends on archive node; see section 1.4.7 |
| ETH transfer backfill additional storage | ~10–20 GB | ~800M synthetic log rows, highly compressible |
| Final indexed storage (with ETH transfers) | ~110–170 GB | Receipts + trace-derived ETH transfers |

Compare to a Geth full node: **~2 TB storage, days of sync time.** Archive node: **14+ TB, weeks.**

These estimates are based on back-of-envelope calculations and need to be validated during prototyping. In particular, the compression ratios for topic1–3 columns (high cardinality, 32-byte values) may be worse than assumed.

### Query Performance Targets

| Query Type | Target Latency |
|---|---|
| Point lookup (address + topic0 + block) | < 1 ms |
| Range scan, 1K blocks, single contract | < 10 ms |
| Range scan, 100K blocks, single contract | < 100 ms |
| Full table scan, 1M blocks, any filter | < 1 second |
| Aggregation (COUNT/GROUP BY), 1M blocks | < 2 seconds |

---

## Technology Choices

| Component | Recommendation | Rationale |
|---|---|---|
| Language | Rust | Memory safety, zero-cost abstractions, excellent for storage engines. Alloy/reth crates for Ethereum primitives. |
| Columnar engine | Custom, inspired by Apache Arrow | Need tight control over compression and indexing. Arrow for in-memory interchange format. |
| B+ Tree implementation | Custom on-disk B+ tree or adapt `redb` | Need crash-safe, memory-mapped, concurrent-read indexes. |
| Bitmap library | `roaring-rs` | Industry standard for compressed bitmap operations. |
| Compression | `zstd` (cold partitions), `lz4` (hot partition) | Best ratio for cold data, best speed for hot data. |
| P2P networking | `reth-eth-wire`, `reth-network` crates | Battle-tested DevP2P (eth/68+) implementation from Reth, used as a library. |
| RPC framework | `tonic` (gRPC) + `axum` (HTTP) | Async, fast, mature Rust ecosystem. |
| Serialization | Flat binary (no protobuf overhead in storage) | Protobuf only at the API boundary, not in the storage path. |

---

## Competitive Landscape

| Approach | What it does | Limitations from LogEx's perspective |
|---|---|---|
| Geth filtermaps (v1.15.6) | 2D sparse bitmap index, sub-second queries for 100K blocks | Still loads receipts for final results. No SQL. `eth_getLogs` only. |
| Nethermind Log Index | Address/topic → block number mappings | Still reads blocks. Limited improvement for dense queries. |
| Reth (current) | No dedicated log index (open issue #16999) | Slowest `eth_getLogs` of the three major clients. |
| Envio HyperSync | Purpose-built data node, 2000x faster than RPC, field selection | Proprietary hosted service. Not self-hostable. Requires API token. No SQL. |
| Paradigm Cryo | Extract logs to Parquet, query with DuckDB/Polars | Batch-only, no real-time. Requires existing full node. Not a server. |
| Reth ExEx + Postgres | Pipe logs to Postgres via ExEx plugin | Postgres indexes are not optimized for this workload. Requires full Reth node (~2 TB). |
| The Graph | Subgraph-based, GraphQL | Slow. Requires pre-defined schema per use case. Not general-purpose. |
| Dune Analytics | Full SQL over warehouse | Centralized, rate-limited, expensive. Not self-hostable. |
| **LogEx** | Standalone light node, columnar storage + bitmap indexes + SQL, self-hosted | Requires building and maintaining new infrastructure. |

LogEx's unique position: **self-hosted, trustless, SQL-capable log queries without running a full node.** A single binary joins the P2P network, syncs only what it needs (headers + receipts), and serves queries. No existing tool combines all three of: self-hosted sovereignty, SQL expressiveness, and columnar storage performance — with zero external dependencies.

---

## Extension Points

### ABI Registry

An optional module that maps `topic0` hashes to human-readable event signatures and provides automatic decoding of `data` fields. Populated from:
- User-uploaded ABIs
- Etherscan/Sourcify verified contract ABIs (fetched on demand)
- A built-in table of the ~500 most common event signatures

### Materialized Views (v2)

For dashboards or repeated queries, users can define materialized views that are incrementally maintained as new blocks arrive:

```sql
CREATE MATERIALIZED VIEW usdt_large_transfers AS
SELECT block_number, timestamp, topic1 AS sender, topic2 AS receiver,
       decode(data, 'uint256') AS amount
FROM logs
WHERE address = '0xdAC17F...'
  AND topic0 = event'Transfer(address,address,uint256)'
  AND decode(data, 'uint256') > 1000000000000;  -- > 1M USDT
```

### Multi-Chain

The architecture is chain-agnostic. The ingestion pipeline can be swapped for any EVM chain (Polygon, Arbitrum, Base, BSC) — the log format is identical. Storage is partitioned per chain.

### Self-Joins (v2)

Cross-event-type queries within the same transaction (e.g., "find all Swaps that also involved a USDC Transfer") require self-joins on `tx_hash`. This is deferred to v2 because it requires a dedicated `tx_hash` index and careful query planning to avoid full scans on a multi-billion-row table.

---

## Trust Model

LogEx operates with a two-tier trust model. Understanding this model is essential for operators who depend on LogEx as a source of truth for financial operations like exchange deposit tracking.

### Tier 1: Receipt-Derived Logs (source = 0) — Consensus-Verified

All logs extracted from transaction receipts carry the strongest possible trust guarantee. Here is the full verification chain:

1. Block headers are attested by Ethereum's proof-of-stake consensus. At least two-thirds of all staked validators have signed off on each block header, including its `receiptsRoot` field.
2. The `receiptsRoot` is the Merkle Patricia Trie root of all transaction receipts in the block. It is a cryptographic commitment: any modification to any receipt (including adding, removing, or altering any log entry) would produce a different root hash.
3. During ingestion, LogEx reconstructs the receipt trie from the downloaded receipts and verifies that the computed root matches the `receiptsRoot` in the block header. If they do not match, the receipts are rejected.
4. Once verified, the logs are decomposed and stored. The fact that LogEx discards the trie structure afterward does not weaken this guarantee — the verification already happened. The data is correct because it was validated at write time.

This trust model is identical to querying `eth_getLogs` on Geth, Erigon, or any other execution client. When you call `eth_getLogs` on Geth, Geth does not re-verify the receipt trie for every query. You are trusting that Geth validated the data when it ingested it, and that its database has not been corrupted since. LogEx works the same way.

Receipt-derived logs include all ERC-20, ERC-721, ERC-1155 Transfer events, all Swap events, all Approval events, and every other log emitted by smart contracts. Post-EIP-7708, this also includes all native ETH transfers.

### Tier 2: Trace-Derived Logs (source = 1) — EVM-Verified

Synthetic ETH transfer logs created by the `--backfill-eth-transfers` process carry a weaker trust guarantee. Here is why:

1. The trace data is obtained by calling `trace_block` on an archive node. The archive node re-executes the block's transactions using the EVM and reports the internal call tree, including which calls transferred ETH.
2. There is no `tracesRoot` in the Ethereum block header. Unlike receipts, there is no consensus-level cryptographic commitment to trace data. Two different execution clients could theoretically produce different traces for the same block if one of them has a bug in its EVM implementation.
3. Therefore, the correctness of trace-derived data depends entirely on the correctness of the archive node's EVM implementation. If the archive node is running a correct, up-to-date client (Geth, Erigon, Reth), the traces will be correct. If the archive node has a bug or is running a compromised or outdated client, the traces could be wrong.

In practice, this trust level is the same as what every exchange and wallet already has today when they use `trace_block` or `debug_traceBlock` to detect ETH deposits. LogEx does not weaken the existing trust model — it merely stores the result so you do not have to re-trace on every query.

### Querying by Trust Level

The `source` field allows operators to filter by trust level if needed:

```sql
-- Only consensus-verified data (receipts)
SELECT * FROM logs WHERE source = 0 AND topic0 = event'Transfer(...)';

-- Only trace-derived data (ETH transfers from backfill)
SELECT * FROM logs WHERE source = 1 AND topic0 = event'Transfer(...)';

-- Both (the default — most operators will use this)
SELECT * FROM logs WHERE topic0 = event'Transfer(...)';
```

For operators with strict compliance requirements, the `source` field provides an auditable distinction between consensus-verified and EVM-verified data. For most operators, the distinction is academic — both tiers are correct in practice.

### Recommendation for Maximum Trust

For operators who want the strongest possible trust guarantee for their ETH transfer data, the recommended configuration is:

1. Run your own archive node (not a third-party provider). This eliminates trust in any external party's infrastructure.
2. Use a client with a strong track record (Geth or Erigon). These have been battle-tested on mainnet for years.
3. After the backfill completes, you can optionally spot-check trace-derived data by re-tracing a random sample of blocks against a second, independent archive node (different client implementation) and comparing the results. If they match, you have high confidence in the data's correctness.
4. Once EIP-7708 activates, all new ETH transfers become receipt-derived (source = 0), and the trace-derived data is only needed for historical blocks that will never change.

---

## What LogEx Explicitly Does NOT Do

- **No receipt trie proofs.** LogEx decomposes logs and does not retain the original receipt trie structure. It cannot generate Merkle inclusion proofs for individual log entries. If trustless verification of a specific query result is needed, the operator can re-fetch the receipt from any full node and validate it against the block header's `receiptsRoot`, which LogEx does store. A future version could include a convenience RPC method that performs this verification on demand by proxying to a configured full node. Alternatively, EIP-7745 (if adopted at the consensus level) would provide provable log indexes natively, benefiting all clients equally.
- **No EVM execution.** LogEx cannot answer state queries ("what is the balance of X?"), simulate transactions, or produce trace data. When the `--backfill-eth-transfers` flag is enabled, LogEx obtains trace data from an external archive node — it does not execute the EVM itself.
- **No block production.** LogEx is a read-only system. It does not participate in consensus, does not propose blocks, and does not validate state transitions.

---

## Summary

LogEx is not a general-purpose Ethereum client. It is a **standalone light node** and a **log-first, index-first, query-first** system that:

1. Runs as a single binary with zero external dependencies — joins the P2P network directly via DevP2P.
2. Syncs only headers and receipts (~100–170 GB vs 2+ TB for a full node).
3. Decomposes logs into columnar storage with bitmap indexes at write time.
4. Serves SQL-like queries via index seeks, never block scans.
5. Targets sub-100ms latency for queries that take seconds even on clients with modern log indexes.
6. Optionally backfills historical ETH transfers from an archive node's trace data, so operators can track every value movement (ETH + tokens) from genesis without running their own archive node permanently.
7. Post-EIP-7708: captures all ETH transfers from receipts natively, eliminating the need for trace data entirely. The backfill is a one-time cost for historical coverage.

LogEx is designed to be self-contained and trustless. It has no external dependencies beyond the Ethereum P2P network itself — no full node, no RPC endpoint, no third-party data provider. The ETH transfer backfill is opt-in, requires the operator to provide their own archive node endpoint, and records the provenance of every data point so operators always know whether a given log was consensus-verified (from receipts) or EVM-verified (from traces).

It trades generality (no state, no EVM, no block production) for extreme specialization at the one thing wallets, exchanges, block explorers, and data pipelines need most: fast, flexible, self-hosted access to event logs and transfer data.
