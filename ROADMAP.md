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
- Keep the existing query/storage roadmap in scope; canonicality work comes first, but SQL/query improvements are still in scope now.
- Do not freeze transitional shortcuts into the final architecture.
  - If a query/storage step is a bridge, call it out explicitly in the roadmap.
  - The target design should be the strongest direct design LogEx can reasonably support, not merely the fastest incremental patch.
- At this stage, it is acceptable to break the current local storage format and require a full resync.
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
  - REST `/query`, gRPC `Query`, and the web UI share the same DataFusion-backed SQL engine
  - gRPC now also exposes typed `GetLogs` and `StreamLogs` methods over the same canonical storage/query core
  - the native storage rewrite is materially in place:
    - the catalog tracks hot and sealed segments, manifests, durable sync state, and segment-level metadata
    - the compatibility `PartitionManager` facade now sits on top of native segment storage instead of the old partition implementation
    - hot segment rotation, restart/resume, WAL replay, and canonical row marking are wired through the native path
    - sealed segments are compacted into page-oriented encoded columns with per-column codec and page-index metadata
    - sealed-segment reads are manifest-aware, so indexed lookups do not require whole-column decompression
    - startup now performs a lightweight integrity check over the recent canonical header window and segment metadata/page decode boundaries so obvious local corruption is detected early
    - permanent compaction of sealed history is delayed behind a safety margin from the current head; this is an interim anti-reorg rule that should later be replaced by real CL finality
  - DataFusion now reads projected columns directly from native segment files
    - the old row-materialization seed scan is no longer used by REST `/query`, gRPC `Query`, or the web UI
    - projection pushdown is live
    - exact filter pushdown is live today for block range, block hash, address, and topic0 predicates
    - legacy LogEx syntax such as `event'...'`, `address'...'`, and `latest` still works as a pre-processing layer, but regular SQL is now the primary path
    - SQL execution is explicitly read-only; mutating statements are rejected
  - `eth_getLogs` now executes natively over the shared indexed row-id scan path instead of being translated through SQL
  - index coverage is better than before:
    - block hash is now a first-class index
    - sealed-segment index rebuilds and manifest refresh now stay compatible with compacted storage
  - cross-surface consistency coverage now exists for REST, direct SQL, gRPC typed logs, and `eth_getLogs`
  - a benchmark harness now exists for native log filters and SQL workloads; the remaining benchmark task is feeding it larger realistic datasets and recording target numbers

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
  - `eth_getLogs` should remain a dedicated native execution path over the same storage and indexes, not a SQL translation layer.
  - gRPC should expose the same canonical data through typed log methods and SQL query methods; Arrow-style SQL transport can be added later only if benchmarks justify it.
- It is acceptable to rewrite the storage format incompatibly at this stage and require a full resync.

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
     - keep the catalog + hot/sealed segment model
     - keep manifests and durable sync metadata as the source of truth
     - keep the column layout aligned with Arrow/DataFusion scans
     - keep the physical split between raw hot writes and compacted sealed history
     - replace the current head-distance compaction safety margin with CL-finality-aware compaction once finalized anchors exist
     - finish the metadata set that still needs to live in storage:
       - headers or header references needed for APIs and verification
       - receipt-level metadata needed for proof boundaries
       - finalized / optimistic sync anchors
     - keep tuning codec choices and page sizing based on benchmark results rather than guessing permanently upfront
   - Caveats and tradeoffs:
     - this is a justified rewrite target, not an area for more bridge code
     - the format should be specialized for verified log storage, not optimized for pretending to be a generic relational database or a full execution-node database
     - compression must be page- or chunk-oriented so indexed reads do not force whole-column decompression
     - hot data and sealed historical data should have different physical treatment
     - without CL finality, compaction can only be delayed heuristically rather than proven-finalized
   - Done when:
     - the old pre-native format can be discarded
     - the new format supports ingestion, restart/resume, canonical filtering, indexing, and compression as one coherent design
     - the storage layer exposes primitives that are sufficient for SQL, `eth_getLogs`, gRPC, and proof-related features without special-case side stores

4. Unified Query and API Layer Over Native Storage
   - What to build:
     - keep expanding index-aware filter pushdown where sound:
       - topic1-aware pushdown beyond the current composite cases
       - data-length and source pushdown where useful
       - any additional point/range cases that materially improve real workloads
     - keep DataFusion as the canonical SQL layer for REST `/query`, gRPC SQL-style queries, and the web UI
     - keep the typed gRPC log methods and SQL gRPC methods aligned with the same canonical results
     - add Arrow- or batch-oriented SQL result transport only if large-result benchmarks show the JSON SQL transport is a real bottleneck
     - decide whether `decode(...)` belongs as a SQL UDF or remains explicitly deferred
   - Caveats and tradeoffs:
     - the strongest design is one storage/index core with API-specific execution layers, not one forced adapter for every surface
     - `eth_getLogs` semantics are Ethereum API semantics first, not SQL semantics with a wrapper
     - typed gRPC logs are already the canonical log transport; SQL-over-gRPC transport should evolve only if measured workloads justify it
   - Done when:
     - SQL surfaces no longer depend on the row-materialization bridge
     - `eth_getLogs` no longer depends on SQL translation
     - REST, gRPC, web UI, and JSON-RPC all read from the same canonical stored data and produce consistent answers

5. Performance, Correctness, and Documentation Gate
   - What to build:
     - end-to-end validation for weak subjectivity restart, sync committee rotation, finalized/optimistic anchor handling, receipt-root reconstruction, malicious peer detection, bootstrap, shutdown, and resume once the CL path exists
     - run the benchmark harness on realistic ERC-20 / ERC-721-heavy datasets and record target throughput / latency numbers
     - run the benchmark harness on indexed queries, broader SQL queries, and `eth_getLogs`-style workloads
     - corruption, partial-page decode, and torn-write recovery tests
     - keep the cross-surface consistency coverage in place as APIs evolve
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

## Later Expansion

- Historical ETH transfer backfill remains deferred.
- LogEx can later expand the same proof-based model beyond event logs by verifying and exposing additional execution payload roots:
  - `state_root` for account balances, nonces, and code hashes
  - `receipts_root` for logs, gas used, and transaction success or revert status
  - `transactions_root` for transaction inclusion proofs
  - `withdrawals_root` for validator withdrawals after Shanghai
