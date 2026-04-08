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
  2. extract verified execution anchors from the corresponding beacon data
  3. fetch raw receipts from EL peers
  4. reconstruct the receipt trie locally
  5. require the computed receipts root to match the verified execution anchor

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
  - current APIs support row/filter queries
  - DataFusion remains the planned path for richer SQL

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
   - Reconstruct the receipt trie locally and require its root to equal the verified receipts root from CL.
   - Distinguish finalized anchors from optimistic/head anchors in persistence and status.
   - Keep comparing the remaining receipt scheduler and retry logic against Reth/geth, because receipts are still the main LogEx-owned sync surface.

3. Add proof-oriented log verification on top of the receipt-root match.
   - Preserve enough local structure to prove receipt inclusion.
   - Derive or store the data needed to prove a specific log against its receipt.
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

6. Replace the current limited SQL gate with a DataFusion-backed query path.
   - Expose current storage as a `TableProvider`.
   - Define an Arrow schema for the current log row model.
   - Convert partition reads into `RecordBatch` output without changing the on-disk storage format.
   - Add aggregates, ordering, aliases, and better SQL coverage without changing the on-disk storage format.
   - Preserve partition pruning and current index advantages.
   - Reuse the existing address/topic/block indexes where the planner can map filters onto them.

7. Preserve LogEx-specific query ergonomics while moving to DataFusion.
   - Keep existing row-query behavior working for `SELECT *`, projected columns, `WHERE`, and non-aggregate `ORDER BY`.
   - Preserve `event'...'`, `address'...'`, and `latest` through rewrites or UDFs.
   - Decide whether `decode(...)` is part of v1 or remains deferred.

8. Add API compatibility and regression coverage for the richer query path.
   - REST, gRPC, and web UI should all use the same query engine.
   - Add tests for aggregates, aliases, mixed filters, ordering, and hot-partition visibility.
   - Benchmark common indexed queries so the current fast path does not regress badly.

9. Reconcile the public README with the actual trust model and runtime model.
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
- it learned the canonical execution block hash and receipts root from that verified CL data
- it fetched receipts from EL peers over devp2p
- it recomputed the receipts trie locally
- it only accepted logs whose receipts root matches the verified canonical chain

## Deferred

- Historical ETH transfer backfill remains deferred to a possible v2.
- V2 can expand the same proof-based model beyond event logs by verifying and exposing additional execution payload roots:
  - `state_root` for account balances, nonces, and code hashes
  - `receipts_root` for logs, gas used, and transaction success or revert status
  - `transactions_root` for transaction inclusion proofs
  - `withdrawals_root` for validator withdrawals after Shanghai
- A PostgreSQL-compatible storage backend remains out of scope for v1.
