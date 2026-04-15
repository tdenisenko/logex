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
  - the current API/query layering is also still transitional:
    - `eth_getLogs` is still translated into LogSQL plus residual filtering, which is acceptable as a bridge but not the right end-state
    - gRPC currently returns JSON rows for flexible SQL results, which is acceptable as a bridge but not necessarily the final transport design
  - codec scaffolding already exists in the storage crate for dictionary, delta, delta-of-delta, zstd, and lz4, but the active read/write path still writes raw columns today

## Explicitly Not Needed

- A full state trie
- EVM execution
- A local full consensus client as a mandatory dependency
- An external execution RPC URL
- An external consensus RPC URL in the final design
- Engine API compatibility with a standard beacon node as the primary canonicality path

## Current Architecture Decisions

- Keep using Reth crates for EL networking where they give us production-grade devp2p behavior.
  - Discovery, session handling, peer management, and request/response transport should stay as close to Reth as practical.
  - LogEx should not spend engineering effort re-creating networking machinery that Reth already solves well.
- Do not adopt Reth's full execution/storage stack.
  - LogEx is intentionally not a full execution client.
  - The storage and query engine should be rewritten for log-centric verified ingestion rather than bent around a generic execution-node database.
- Keep DataFusion as the SQL engine, but not as the storage engine.
  - DataFusion should plan and execute SQL over native LogEx storage.
  - DataFusion should not be fed by a permanent row-materialization bridge.
- Use one canonical storage/index core, but not one forced execution path for every API.
  - SQL surfaces should use DataFusion over the native storage provider.
  - `eth_getLogs` should become a dedicated native execution path over the same storage and indexes, not a permanent SQL translation layer.
  - gRPC should expose the same canonical data, but its transport can be specialized for typed log streams, SQL result sets, or Arrow-oriented responses as needed.
- Treat the current hybrid DataFusion and `eth_getLogs` paths as temporary bridges.
  - Only fix correctness bugs in those bridges.
  - Do not keep layering new permanent features onto them.
- It is acceptable to rewrite the storage format incompatibly before v1 and require a full resync.

## Remaining Work

1. Canonical Trust Anchor
   - What to build:
     - add weak subjectivity checkpoint input, persistence, and restart handling
     - implement the beacon light-client bootstrap, update, and finality flow
     - verify sync committee signatures and committee rotation
     - verify the execution payload inclusion proof against the beacon header body root, following the Helios-style light-client model
     - persist finalized and optimistic execution anchors, including block hash, block number, and receipts root
   - Caveats and tradeoffs:
     - this is the hardest cryptographic and protocol part of the project
     - it is worth reusing mature SSZ, BLS, and consensus-side primitives where available, but the architecture must stay LogEx-native rather than turning into a full CL node
     - this is the non-negotiable correctness foundation; performance work before this has limited value
   - Done when:
     - LogEx can restart from a stored weak subjectivity checkpoint state
     - LogEx can track finalized and optimistic execution anchors from CL light-client updates
     - the node can point to a CL-verified receipts root for each accepted execution block

2. Canonical EL Receipt Ingestion
   - What to build:
     - keep the current Reth-backed EL network stack
     - fetch receipts by CL-anchored block hash
     - encode receipts exactly as Ethereum does
     - rebuild the receipt trie locally and require the computed root to match the CL-verified receipts root
     - persist whatever receipt-level and header-level metadata is required for canonical log storage and future proof APIs
     - continue improving peer selection, batch sizing, fallback behavior, and retry logic around receipts
   - Caveats and tradeoffs:
     - multi-peer comparison is primarily a liveness and robustness tool; correctness comes from matching the locally rebuilt trie root against the CL-verified root
     - LogEx should not store generic transaction/state data it does not need, but it must store enough receipt-adjacent metadata to keep the proof boundary honest
   - Done when:
     - logs are accepted only from receipts whose rebuilt trie root matches the CL-verified root
     - malformed or incomplete receipt responses are detected and rejected cleanly
     - restart and resume preserve the canonical verified sync position

3. Native Storage Engine Rewrite
   - What to build:
     - rewrite the current storage layout into the final hot-and-sealed segment model instead of incrementally patching the current format
     - keep an append-friendly hot store for active sync and immutable sealed segments for historical data
     - store data in a columnar layout aligned with Arrow/DataFusion scan patterns
     - make segment metadata first-class:
       - block range
       - segment id / generation
       - row count
       - canonicality metadata
       - codec and page metadata
       - index metadata
       - verification / sync anchors needed by LogEx
     - define exactly which non-log metadata lives in storage in v1:
       - headers or header references needed for APIs and verification
       - receipt-level metadata needed for proof boundaries
       - finalized / optimistic sync anchors
     - build the final index strategy and final compression format into this rewrite rather than treating them as add-ons
   - Caveats and tradeoffs:
     - this is a justified rewrite target, not an area for more bridge code
     - the format should be specialized for verified log storage, not optimized for pretending to be a generic relational database or a full execution-node database
     - compression must be page- or chunk-oriented so indexed reads do not force whole-column decompression
     - hot data and sealed historical data should have different physical treatment
   - Done when:
     - the old pre-v1 format can be discarded
     - the new format supports ingestion, restart/resume, canonical filtering, indexing, and compression as one coherent design
     - the storage layer exposes primitives that are sufficient for SQL, `eth_getLogs`, gRPC, and proof-related features without special-case side stores

4. Unified Query and API Layer Over Native Storage
   - What to build:
     - replace the current hybrid DataFusion bridge with a native DataFusion provider over the rewritten storage engine
     - implement real projection pushdown, partition pruning, and index-aware filter pushdown where sound
     - keep DataFusion as the canonical SQL layer for REST `/query`, gRPC SQL-style queries, and the web UI
     - replace the current `eth_getLogs` LogSQL translation with a dedicated native execution path over the same storage and index primitives
     - decide the final gRPC shape:
       - typed log-oriented methods where that is the right abstraction
       - SQL result methods where that is the right abstraction
       - Arrow- or batch-oriented transport if large result sets need it
     - decide whether `decode(...)` belongs in v1 as a UDF or remains explicitly deferred
   - Caveats and tradeoffs:
     - the strongest design is one storage/index core with API-specific execution layers, not one forced adapter for every surface
     - `eth_getLogs` semantics are Ethereum API semantics first, not SQL semantics with a wrapper
     - gRPC JSON rows are flexible but not necessarily the best final transport for large query results
   - Done when:
     - SQL surfaces no longer depend on the row-materialization bridge
     - `eth_getLogs` no longer depends on LogSQL translation
     - REST, gRPC, web UI, and JSON-RPC all read from the same canonical stored data and produce consistent answers

5. Performance, Correctness, and Documentation Gate
   - What to build:
     - end-to-end validation for weak subjectivity restart, sync committee rotation, finalized/optimistic anchor handling, receipt-root reconstruction, malicious peer detection, bootstrap, shutdown, and resume
     - benchmarks for realistic ERC-20 / ERC-721-heavy datasets
     - benchmarks for indexed queries, broader SQL queries, and `eth_getLogs` workloads
     - corruption, partial-page decode, and torn-write recovery tests
     - consistency tests that compare REST, gRPC, SQL, and `eth_getLogs` results over the same stored data
     - README alignment with the actual trust model, runtime model, and verification guarantees
   - Caveats and tradeoffs:
     - this should be treated as a gate, not a cleanup bucket
     - if the rewritten storage/query architecture does not pass this gate, the architecture is not done
   - Done when:
     - the node can sync, restart, query, and serve logs from the rewritten architecture with measured performance and correctness coverage
     - the public docs describe the real guarantees rather than the intended ones

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
