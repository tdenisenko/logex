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

6. Benchmark and tune the post-migration SQL path.
   - Measure common indexed queries after the DataFusion migration so the current fast path does not regress badly.
   - Decide whether more explicit DataFusion-side filter pushdown is worth the added complexity, or whether the current seed-query approach is sufficient for v1.
   - Decide whether `decode(...)` is part of v1 or remains deferred.

7. Finish the storage compression strategy without harming queryability.
   - Keep the hot partition append-friendly and mostly uncompressed; compression should primarily happen when a partition is sealed and becomes immutable.
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
   - Apply compression to sealed partitions first, then decide whether old sealed partitions should be rewritten by a background compaction job.
   - Avoid rewriting the hot partition during active sync except for a deliberate maintenance or compaction pass.
   - Add compatibility handling so old uncompressed partitions and new compressed partitions can coexist during migration.
   - Validate the design with:
     - storage-size benchmarks on realistic ERC-20 / ERC-721-heavy datasets
     - indexed query latency benchmarks
     - restart and resume tests across mixed compressed/uncompressed partitions
     - corruption / partial-page decode tests

8. Reconcile the public README with the actual trust model and runtime model.
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
- A PostgreSQL-compatible storage backend remains out of scope for v1.
