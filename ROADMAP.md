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
- The CL-driven storage and runtime boundary is now materially in place:
  - `WeakSubjectivityCheckpoint`, `ExecutionAnchor`, and `ChainAnchors` are shared types used across runtime, sync, storage, and status surfaces
  - `crates/logex-cl` exists and persists checkpoint state plus ordered execution anchors under the data directory
  - the node now accepts `--checkpoint`, persists consensus state on first startup, reloads it on restart, and exposes checkpoint/finality/optimistic/indexed-head status through CLI `info` and `/status`
  - fresh data directories now require a checkpoint before they can start canonical sync
  - storage now persists execution-facing chain anchors separately from log rows
  - finalized-head-aware compaction is live whenever finalized CL anchors exist
  - the anchored EL sync path now fetches headers by block hash, validates the fetched header against the CL-derived execution anchor, validates body/receipts locally, and only then ingests logs
  - anchored sync now rewinds indexed canonical state on optimistic anchor replacement within the persisted recent-header window instead of silently continuing on stale canonical data
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
    - permanent compaction now follows the finalized execution head when CL finality is available; the old safety-margin rule remains only as a fallback for legacy non-checkpointed storage
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
  - former roadmap task 3 is complete for the current architecture scope:
    - native hot/sealed storage, page-oriented sealed-column compaction, manifest-aware reads, and compacted-segment index/query compatibility are in place
  - former roadmap task 4 is complete for the current architecture scope:
    - SQL uses native storage through DataFusion
    - `eth_getLogs` uses the native filter path directly
    - gRPC exposes both SQL queries and typed log methods
    - cross-surface consistency coverage is in place

## Status Right Now

- LogEx now has the correct CL-owned persistence model and the correct EL verification boundary:
  - EL peers no longer decide canonical order once consensus state exists
  - ordered execution anchors decide which EL blocks are eligible for ingestion
  - LogEx persists optimistic, finalized, and indexed execution heads explicitly
- The main blocker is no longer the EL receipt pipeline.
  - The remaining blocker is live production of those execution anchors from a verified beacon light client.
- In other words:
  - anchored EL verification is implemented
  - checkpoint persistence is implemented
  - anchor rewind on optimistic reorg is implemented
  - native CL networking and real light-client update verification are not implemented yet
- This means the current branch is a real architectural shift, but not yet the full end-to-end canonical system.

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

## Remaining Work And Clear TODOs

1. Native Beacon Light Client
   - TODO:
     - implement native CL discovery, peer management, and req/resp for light-client bootstrap, updates-by-range, finality updates, optimistic updates, and beacon block fetches
     - implement fork-aware SSZ decoding for the post-merge beacon light-client objects LogEx needs
     - verify sync committee signatures, committee rotation, and weak-subjectivity bootstrap state exactly enough to match the Helios-style trust model
     - verify `execution_branch` against the beacon header `body_root` for every trusted light-client header
   - Why this is still blocking:
     - the node can persist and consume execution anchors, but it cannot yet generate them itself from a live verified CL network
     - until this lands, checkpointed CL state must be imported rather than learned live
   - Done when:
     - a fresh mainnet sync can start from a weak-subjectivity checkpoint and produce verified optimistic/finalized execution anchors without any external consensus RPC

2. Beacon Block Segment Authentication
   - TODO:
     - fetch full beacon blocks between trusted light-client headers
     - verify each full block body against its signed beacon header by recomputing `body_root`
     - walk parent roots backward from trusted endpoints so every emitted execution anchor is authenticated, ordered, and contiguous
     - derive execution payload headers from the verified block bodies rather than trusting imported execution anchor files forever
   - Why this is still blocking:
     - the current anchor store can hold per-block execution anchors, but LogEx still needs the beacon-side segment walker that creates those anchors from verified CL data
   - Done when:
     - every execution anchor used by EL ingestion can be traced back to verified beacon blocks bounded by verified light-client headers

3. Remove Transitional Consensus Shortcuts
   - TODO:
     - delete the remaining operator-facing language that treats CL networking ports as merely reserved
     - delete the remaining legacy EL-only startup path once live CL anchor production is in place
     - stop relying on imported checkpoint descriptor files as the normal way to feed execution anchors into the node
     - tighten storage/runtime assumptions so checkpointed canonical sync is the only steady-state ingestion path
   - Why this is still blocking:
     - the final architecture should not leave a half-canonical fallback path around after the real CL path exists
   - Done when:
     - canonical sync always means CL-driven sync, not a mix of CL mode and legacy EL-only mode

4. Mainnet Proving Runs And Failure Handling
   - TODO:
     - run long-lived mainnet sync tests from real weak-subjectivity checkpoints
     - verify restart, shutdown, and resume across optimistic updates, finality advances, and anchor replacements
     - test malicious or incomplete EL responses against CL-driven anchors on real network conditions
     - improve recovery behavior for optimistic reorgs deeper than the persisted recent-header window
   - Why this is still blocking:
     - the current code now rewinds correctly within the stored recent-header window, but a deeper optimistic reorg still requires stronger recovery logic or an explicit resync path
   - Done when:
     - LogEx can survive realistic mainnet churn and either recover safely or fail loudly with a precise operator action

5. Release Gate
   - TODO:
     - keep the current receipt-root, restart, corruption-detection, and cross-surface consistency coverage green as the native CL path lands
     - add end-to-end fixtures for checkpoint bootstrap, light-client updates, beacon blocks, execution anchors, EL headers, bodies, and receipts
     - run the benchmark harness on realistic ERC-20 / ERC-721-heavy datasets and record target throughput / latency numbers
     - align README and operator docs with the actual guarantees once the native CL path is complete
   - Done when:
     - the node can sync, restart, query, and serve logs from the final CL-driven architecture with measured performance and truthful documentation

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
