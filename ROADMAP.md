# LogEx Roadmap

## Goal

LogEx should become a canonical Ethereum event-log node that:

- uses Ethereum P2P only
- does not execute the EVM
- does not store the full state trie
- stores logs locally and makes them queryable
- relies on a weak subjectivity checkpoint as the only unavoidable trust assumption
- is otherwise theoretically trustless for canonical log verification after that checkpoint

## Direction

- Keep the execution-layer networking close to Reth.
- Do not depend on an external execution RPC or an external consensus RPC for steady-state operation.
- Do not require a local full beacon node like Lighthouse as a runtime dependency.
- Implement an embedded consensus-side light-client path inside LogEx, then bind EL receipt verification to that.
- Use Helios as a reference for light-client architecture and implementation ideas where helpful, but not as a drop-in dependency today.
  - Helios is useful because it already implements a real light-client model.
  - Helios is not a direct fit because its normal Ethereum mode still assumes an execution RPC.
- Keep the existing query/storage roadmap in scope; canonicality work comes first, but SQL/query improvements are still part of v1.
- Do not freeze transitional shortcuts into the final architecture.
  - If a query/storage step is a bridge, call it out explicitly in the roadmap.
  - The target design should be the strongest direct design LogEx can reasonably support, not merely the fastest incremental patch.
- Before v1, it is acceptable to break the current local storage format and require a full resync.
  - Backward compatibility with today’s local storage files is less important than converging on the right storage/query architecture now.
  - Migration shims should only be kept if they materially reduce implementation risk without weakening the final design.

## Important Clarification

- A standard EL <-> CL setup like `Reth + Lighthouse` is not the right target for LogEx.
  - A normal beacon node expects a real execution engine over Engine API.
  - LogEx intentionally does not execute blocks, so it should not pretend to be a full EL for a normal CL.
- The consensus-side primitive LogEx needs is the beacon light-client protocol.
  - Sync committees verify light-client updates and finality/optimistic updates.
  - They do not directly act as a signature over every beacon block in the way a naive gossip-only design would imply.
- Therefore the canonicality path should be:
  1. verify beacon light-client updates from a weak subjectivity checkpoint
  2. verify the SSZ Merkle branch that binds the `execution_payload_header` into the light-client header, the same way Helios validates `execution_branch` against the beacon header `body_root`
  3. treat the `receipts_root` inside that proven execution payload header as the canonical CL-verified receipt commitment for the corresponding execution block hash
  4. fetch raw receipts from EL peers for that execution block hash
  5. locally encode those receipts exactly as Ethereum does and rebuild the execution-layer receipt trie (MPT) from them
  6. require the computed EL receipt-trie root to match the CL-verified `receipts_root` before accepting any logs derived from those receipts

## Already Done

- Standalone LogEx node exists and syncs from EL P2P without any external RPC.
- Reth networking is already integrated for devp2p sessions, discovery, and fetch paths.
- Persistent node identity and productive peer persistence are in place.
- Restart/resume and recent canonical header persistence are in place.
- Ancient and modern receipt-root / bloom verification are in place.
- Historical receipt decoding and partial receipt-response handling are fixed.
- The node can ingest logs locally and expose them through the current query APIs.
- The current EL sync stack is materially closer to a real client than the original prototype:
  - Reth-backed session management and discovery are in place.
  - Reth-backed header/body fetch paths are in place.
  - custom receipt transport remains, because LogEx is intentionally no-execution.
- Operator-facing honesty is much better than before:
  - persistent sync head and recent headers are stored locally
  - restart resumes from durable metadata
  - productive peers are persisted
  - UI/runtime state no longer falsely claims sync completion on missing targets
- Query/storage direction is already established:
  - local storage is the source of truth
  - REST `/query`, gRPC `Query`, and the web UI now share the same DataFusion-backed SQL engine
  - current storage is exposed to DataFusion through a real `TableProvider` scan path rather than preloading candidate rows into a temporary `MemTable`
  - LogEx-specific SQL rewrites such as `event'...'`, `address'...'`, and `latest` still work without changing the on-disk storage format
  - sealed-partition pruning and existing address/topic/block indexes are still reused through the seed-query path
  - hot-partition queries no longer risk missing fresh rows that were written after the last hot-index rebuild
  - the current DataFusion integration is still a transitional hybrid rather than the strongest end-state:
    - DataFusion currently receives rows after a LogEx-owned seed scan and Arrow batch materialization step
    - DataFusion does not yet read partition columns directly from storage
    - DataFusion does not yet own true projection pushdown or native index-aware filter pushdown into LogEx storage
    - this was acceptable as an intermediate migration step to unify behavior without changing the on-disk format
    - it should not be treated as the final query architecture
  - codec scaffolding already exists in the storage crate for dictionary, delta, delta-of-delta, zstd, and lz4, but the active read/write path still writes raw columns today

## Explicitly Not Needed

- A full state trie
- EVM execution
- A local full consensus client as a mandatory dependency
- An external execution RPC URL
- An external consensus RPC URL in the final design
- Engine API compatibility with a standard beacon node as the primary canonicality path

## Prioritized TODO

1. Build the CL light-client foundation first.
   - Add weak subjectivity checkpoint input, persistence, and restart handling.
   - Implement the beacon light-client bootstrap/update/finality flow.
   - Verify sync committee aggregate signatures and committee rotation.
   - Verify the execution payload inclusion proof inside each accepted light-client header, following the same shape Helios uses for `execution_branch` against the beacon header `body_root`.
   - Persist verified consensus outputs that matter to LogEx:
     - finalized execution block hash
     - optimistic/head execution block hash
     - block number
     - receipts root
   - Expose these verified anchors in node status and storage metadata.

2. Adapt EL receipt syncing to consume verified CL anchors.
   - Keep the current Reth-backed devp2p stack.
   - Fetch receipts by block hash for CL-anchored execution blocks.
   - Fetch from multiple EL peers and cross-check for omission or inconsistent receipt sets.
   - Normalize and encode receipts exactly as Ethereum does on the wire and in trie leaves.
   - Reconstruct the execution-layer receipt trie locally and require its root to equal the CL-verified `receipts_root` from the proven execution payload header.
   - Accept logs only as data derived from receipts that passed that root check, never as standalone peer assertions.
   - Distinguish finalized anchors from optimistic/head anchors in persistence and status.
   - Keep comparing the remaining receipt scheduler and retry logic against Reth/geth, because receipts are still the main LogEx-owned sync surface.

3. Add proof-oriented log verification on top of the receipt-root match.
   - Preserve enough local structure to prove receipt inclusion against the verified `receipts_root`.
   - Make it explicit that the trustless proof boundary is receipt inclusion in the trie plus the log's position inside the proven receipt, not a separate "log Merkle tree".
   - Derive or store the data needed to prove a specific log from a proven receipt.
   - Make the proof boundary explicit in APIs and docs.

4. Keep improving EL throughput after the trust anchor exists.
   - Better peer fanout for receipts.
   - Better batching and fallback for large receipt responses.
   - Better blank-dir bootstrap and serving-peer conversion.
   - Better peer scoring for omission or malformed receipt behavior.
   - Revisit batch sizing and fallback behavior for huge receipt/body responses so honest peers are not penalized when soft response limits are hit.

5. Add end-to-end validation coverage for the new trust model.
   - weak subjectivity restart
   - sync committee rotation
   - finalized/optimistic anchor updates
   - receipt-root reconstruction
   - malicious peer mismatch detection
   - bootstrap, restart, shutdown, and resume regressions

6. Build the final v1 storage and query architecture as one system.
   - Stop treating storage, SQL, `eth_getLogs`, gRPC, and compression as separate side topics; design them together around one native LogEx storage engine.
   - Prefer a LogEx-native storage format over bolting on an external database backend.
     - The goal is not “PostgreSQL-compatible storage”.
     - The goal is a storage engine that is fully queryable with modern SQL through DataFusion, serves `eth_getLogs` correctly, serves gRPC correctly, compresses well, and is efficient for long-running sync.
   - It is acceptable to replace the current storage layout incompatibly before v1 if that yields a materially better end-state.

7. Define the final on-disk storage model.
   - Keep an append-friendly hot store for active sync and immutable sealed segments for historical data.
   - Store logs in a columnar layout aligned with DataFusion/Arrow-style scan patterns rather than a row-materialization-first layout.
   - Make segment metadata first-class and durable:
     - block range
     - segment id / generation
     - row count
     - canonicality metadata
     - codec and page metadata
     - index metadata
     - sync / verification anchors needed by LogEx
   - Preserve enough surrounding metadata to support canonical log serving, restart/resume, proof-related features, and future verified APIs.
   - Decide explicitly which non-log metadata must live alongside logs in v1 so the storage engine is not boxed in later.
     - block headers or header references needed for verification and APIs
     - receipt-level metadata needed for proof boundaries and API shaping
     - sync status / finalized / optimistic anchors

8. Make DataFusion the canonical SQL layer over native storage.
   - Keep DataFusion as the main SQL engine for REST, gRPC query endpoints, and the web UI.
   - Build a truly native storage-backed DataFusion provider that reads LogEx partitions directly instead of first reconstructing full `LogRow` sets.
   - Add real projection pushdown so queries that read a few columns do not force full-row materialization.
   - Add real filter pushdown from DataFusion expressions into LogEx planning and indexes wherever those mappings are sound.
   - Preserve partition pruning and existing address/topic/block indexes, but make them native scan capabilities rather than a sidecar prefilter step.
   - Support modern SQL behavior through DataFusion rather than by maintaining a narrow allowlist.
     - aggregates
     - aliases
     - ordering
     - grouping
     - limits
     - richer predicates and expressions where DataFusion supports them
   - Decide whether `decode(...)` belongs in v1 as a UDF or remains explicitly deferred.

9. Make API compatibility a first-class storage requirement.
   - `eth_getLogs` must be served from the same canonical local storage and indexes, with correct Ethereum semantics:
     - block range and block hash filtering
     - address filtering
     - topic filtering including OR semantics and positional rules
     - canonical-only results
     - deterministic ordering
   - gRPC should not become a second-class API because of transport limitations.
     - keep rich query semantics available
     - revisit row-JSON versus a stronger streaming or Arrow-oriented transport for large result sets if needed
   - REST and web UI should remain thin clients over the same canonical query/storage layer, not separate execution paths.

10. Finish the final index and hot/cold query strategy.
   - Keep primary access paths for:
     - block_number
     - block_hash
     - address
     - topic0
     - useful composites such as address+topic0 and any other combinations justified by real workloads
   - Revisit whether additional indexes are needed for proof support, transaction-oriented lookups, or common operational queries.
   - Improve the hot-partition strategy so it can combine an indexed stable prefix with a direct-scan tail, instead of choosing between possibly stale indexes and a broader scan.
   - Ensure indexes remain cheap to maintain during sync and cheap to use during API and SQL queries.

11. Finish the storage compression strategy without harming queryability.
   - Keep the hot store append-friendly and mostly uncompressed; compression should primarily happen when a segment is sealed and immutable.
   - Do not compress an entire column file as one giant blob.
   - Add page- or chunk-based compression inside each sealed column so row IDs produced by indexes can be mapped to a small number of compressed pages instead of forcing full-column decompression.
   - Extend the column format so each compressed column records:
     - codec
     - page boundaries / row ranges
     - byte offsets for each compressed page
     - any codec-specific metadata needed to decode a page independently
   - Teach the reader to dispatch on the stored codec and only decompress the pages needed for the requested row IDs.
   - Keep indexes, null bitmaps, and canonical bitmaps independently readable so query planning and canonical filtering stay cheap.
   - Start with per-column codecs that match the current schema:
     - `block_number`: delta
     - `timestamp`: delta-of-delta
     - `topic0`: dictionary when page cardinality is favorable
     - `address`: adaptive dictionary or zstd depending on page cardinality
     - `block_hash`, `tx_hash`, `topic1`, `topic2`, `topic3`: zstd
     - `data`: lz4 first for fast reads, with the option to benchmark zstd for colder partitions
   - Decide whether older sealed segments should be rewritten by a background compaction job once the final format is stable.
   - Add corruption detection and recovery expectations to the format, not just happy-path compression.

12. Validate the final storage/query design with performance and correctness tests.
   - Benchmark realistic ERC-20 / ERC-721-heavy datasets.
   - Benchmark common indexed queries and broader SQL queries after the native DataFusion path lands.
   - Benchmark `eth_getLogs`-style workloads separately from ad hoc SQL workloads.
   - Test restart and resume across hot/sealed/compressed segment mixes.
   - Test corruption, partial-page decode, and torn-write recovery behavior.
   - Test that API results from REST, gRPC, SQL, and `eth_getLogs` remain consistent over the same stored data.

13. Reconcile the public README with the actual trust model and runtime model.
   - Make it explicit what is verified today.
   - Make it explicit what the weak subjectivity assumption is.
   - Make it explicit that LogEx is not a full execution node and not a standard EL<->CL pair.

## Sequencing Decision

Do the CL implementation first.

Why:

- Without verified CL anchors, EL work improves performance but not canonicality.
- The CL output defines the exact EL data LogEx should trust and verify.
- Once the CL anchor format is fixed, the EL side becomes a much clearer adaptation of the current receipt pipeline.
- This keeps LogEx aligned with its real goal: canonical logs without EVM execution.
- The other TODOs stay in scope; this only sets the order of work.

## Validation Target

LogEx should eventually be able to say all of the following:

- it bootstrapped from a weak subjectivity checkpoint
- it verified beacon light-client updates locally
- it verified the execution payload header against the beacon light-client header with an SSZ Merkle proof
- it learned the canonical execution block hash and receipts root from that proven CL data
- it fetched receipts from EL peers over devp2p
- it re-encoded those receipts and recomputed the execution-layer receipt trie locally
- it only accepted logs whose receipts root matches the verified canonical chain

## Deferred

- Historical ETH transfer backfill remains deferred to a possible v2.
- V2 can expand the same proof-based model beyond event logs by verifying and exposing additional execution payload roots:
  - `state_root` for account balances, nonces, and code hashes
  - `receipts_root` for logs, gas used, and transaction success or revert status
  - `transactions_root` for transaction inclusion proofs
  - `withdrawals_root` for validator withdrawals after Shanghai
