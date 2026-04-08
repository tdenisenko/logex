# LogEx Roadmap

## Direction

- Stay close to a real Ethereum execution node on the networking side, while keeping LogEx log-only on storage/execution.
- Use Reth crates where they remove networking risk or missing protocol behavior.
- Do not spend v1 time re-implementing peer discovery, session management, or wire handling that mature clients already ship and test.
- Move the query layer toward Apache DataFusion instead of continuing to grow the custom LogSQL executor by hand.
- Keep the current on-disk storage/index format as the source of truth and make it queriable through DataFusion; do not introduce an external database dependency for v1.
- Revisit internalizing selected networking pieces only after bootstrap, sync stability, and operator UX are solid.

## Done

- Standalone log-only node exists: no external RPC, no EVM execution, logs stored locally and queryable.
- The current query path supports filtered row queries over the local storage engine, but REST/gRPC still intentionally gate queries to the simple non-aggregate subset.
- Sync head persistence, query correctness, index rebuild behavior, and status/UI honesty were fixed in earlier passes.
- Peer identity is persistent via `discovery-secret`; only productive serving peers are persisted in `known-peers.json`.
- Productive peers are now persisted immediately when they first serve sync data, not only on shutdown, and refreshed records overwrite stale endpoint info.
- Bootnodes are discovery-only, not fake sync peers.
- DNS discovery and stricter peer honesty/status reporting are in place.
- Custom outbound-only TCP/discovery code has now been removed.
- LogEx now runs on top of Reth’s actual network manager/session stack:
  - real listener socket
  - real peer/session lifecycle management
  - discv4 + DNS bootstrap under Reth’s manager
  - persisted productive peers reseeded into the live network stack on restart
  - inbound `eth` requests are now wired through Reth's request-handler path
  - recently fetched canonical headers/bodies/receipts are cached in-memory so LogEx can answer recent peer requests instead of behaving like a silent or empty server
- Peer bootstrap is now closer to Reth’s normal node startup path:
  - NAT/external IP resolution is enabled through Reth’s network builder
  - session event buffers scale with peer capacity like Reth’s node config does
  - persisted productive peers are treated as preferred trusted reconnect targets instead of only plain basic nodes
  - ENR fork-ID gating is no longer over-enforced during discovery, which reduces false-negative candidate drops on startup
  - remote `TooManyPeers` churn is logged more honestly and short-lived rejected sessions are no longer recycled into LogEx’s local pending cache
- The remaining pre-Reth mainnet handshake shim has been removed:
  - startup now feeds the local head into Reth through `NetworkConfigBuilder::set_head(...)`
  - live sync now pushes head updates through `NetworkHandle::update_status(...)`
  - old custom `mainnet.rs` bootstrap constants/helpers are gone
- Sync resume metadata is tighter now:
  - the persisted sync head also stores the block timestamp
  - historical sync resumes explicitly from the sync head, not only from the highest block that emitted logs
  - legacy metadata without timestamps is still accepted on disk
- The sync loop is more resilient against bad serving peers now:
  - empty or partial bodies/receipts responses are no longer treated as successful requests
  - peers that return incomplete block data are penalized and disconnected instead of being retried forever
  - the default bodies/receipts fetch batch is now more conservative to reduce size-limit related mismatches
- Receipt validation now follows Reth's historical/mainnet consensus rules:
  - gas-used checks are always enforced
  - receipt-root and bloom checks are skipped pre-Byzantium, which fixes ancient-mainnet stalls like block `46147`
  - post-Byzantium receipt-root and bloom checks are still enforced
- The eth/70 receipt path now handles partial last-block responses correctly:
  - `last_block_incomplete` and `first_block_receipt_index` are honored
  - multi-round receipt fetches are stitched back together before ingestion
  - zero-progress / malformed continuation responses are rejected as bad responses
- The sync engine now also rejects per-block transaction/receipt count mismatches before ingestion.
- Historical receipt decoding on the wire is now fixed for ancient mainnet blocks:
  - LogEx uses a custom Reth-compatible network receipt type that preserves `status_or_post_state`
  - pre-Byzantium receipts with legacy post-state no longer blow up the session decoder
  - this cleared the real mainnet stall around block `46147`
- Shutdown is now bounded:
  - LogEx no longer waits indefinitely for the Reth network task to stop
  - node/server/background tasks are aborted after a timeout if graceful shutdown stalls
- Peer refill/bootstrap behavior is now materially better:
  - peer refill no longer exits after a single quiet wait interval; it uses its full refill budget
  - stale failed/disconnected discovery candidates are no longer left forever in LogEx's local pending-peer view
  - Reth peer-manager dial concurrency and non-fatal backoff durations are tuned for faster blank-dir bootstrap instead of slowly recycling saturated peers
- Old direct dependencies from the previous custom networking path were removed from `logex-sync`.
- Dead scaffolding and stale dependency drift are reduced:
  - unused workspace-level Reth deps from earlier experiments were removed
  - unused `SyncConfig` checkpoint / `FetchedBlock` scaffolding was removed
  - unused `logex-types` column/error shells were removed
  - crate-local unused dependencies were pruned from the manifests
- Live-network validation has now gone further:
  - release-mode sync resumed from persisted head `46146`
  - crossed the old failure point and advanced past block `50,000`
  - persisted sync metadata advanced to block `54946`
  - restart resumed from `54946` instead of starting over
  - `known-peers.json` remained populated with serving peers across restart
  - fresh-dir bootstrap on April 8, 2026 reached a serving peer and advanced persisted sync state to block `2079`
  - restarting that same data dir resumed quickly and advanced onward to block `3103`
- Warm-restart reconnect and operator status were tightened again:
  - productive peers loaded from `known-peers.json` are now rehydrated into the in-memory productive queue on startup instead of being treated like anonymous peers until they re-serve data
  - restart ordering now prefers those previously serving peers immediately, and shutdown no longer risks rewriting the on-disk productive peer cache to `[]` just because no peer re-served data during the current process lifetime
  - live sync now refills peers with the same small active-peer floor used during historical sync, instead of coasting at one or two sessions after restart
  - `syncing` status stays true while blocks are advancing even when no peer has advertised a credible target head yet
- Serving-peer persistence and request routing are now stricter:
  - `known-peers.json` is no longer rewritten just because an already-known productive peer was moved to the front of the in-memory recency queue
  - peers are only promoted/persisted as serving after the fetched body/receipt batch passes block-level validation, instead of immediately after a length match
  - body/receipt requests now prefer peers that either already served valid sync data or advertise a tip high enough for the requested block range
- P2P shutdown handling is now closer to intentional node behavior:
  - LogEx asks Reth to disconnect peers gracefully, drains close events briefly, then aborts the long-lived Reth network/request-handler tasks explicitly instead of waiting on tasks that are not expected to resolve on their own
- Current validation on this refactor:
  - `cargo fmt --all`
  - `cargo test -p logex-sync -p logex-node`
  - `cargo clippy -p logex-sync -p logex-node -- -D warnings`
  - `cargo build --release --bin logex`

## Next TODO

1. Continue comparing the sync/request path against Reth/geth and decide whether to keep the current lightweight scheduler or adopt more of Reth’s downloader pipeline for headers/bodies/receipts.
2. Replace the custom REST/gRPC "simple SELECT only" gate with a DataFusion-backed query path over the existing storage engine.
   - Add a dedicated adapter crate or module that exposes LogEx storage as a DataFusion `TableProvider`.
   - Define an Arrow schema for the current log row model (`block_number`, `block_hash`, `timestamp`, `tx_hash`, `tx_index`, `log_index`, `address`, `topic0..topic3`, `data`, `data_len`, `source`).
   - Convert partition reads into Arrow `RecordBatch` output without changing the on-disk storage format.
3. Preserve LogEx storage/index advantages inside the DataFusion path instead of falling back to naive full scans.
   - Push partition pruning from `block_number` ranges into the adapter.
   - Reuse the existing address/topic/block indexes when a SQL filter can be mapped onto the current planner/index readers.
   - Keep canonical-row filtering and hot-partition correctness guarantees.
4. Implement the first real SQL expansion on top of DataFusion while keeping the current storage layout.
   - Support `COUNT(*)`, `COUNT(column)`, `MIN`, `MAX`, and `SUM` on numeric columns.
   - Support `GROUP BY`, aggregate `ORDER BY`, `DESC`, aliases, and `LIMIT` for aggregate queries.
   - Keep existing row-query behavior working for `SELECT *`, projected columns, `WHERE`, and non-aggregate `ORDER BY`.
5. Preserve LogEx-specific query ergonomics when moving to DataFusion.
   - Rewrite or expose `event'...'`, `address'...'`, and `latest` as SQL-compatible expressions or UDFs.
   - Decide whether `decode(...)` should become a DataFusion UDF in v1 or stay explicitly deferred.
6. Add API compatibility and regression coverage for the new query layer.
   - REST `/query`, gRPC query, and web UI must all use the same DataFusion execution path.
   - Add tests for aggregate correctness, alias ordering, mixed filters, and hot-partition visibility.
   - Benchmark simple indexed queries against the current path so we do not regress the common case badly.
7. Persist a recent canonical header window so restart-boundary reorg recovery is durable.
8. Add end-to-end regression coverage for bootstrap, restart, shutdown, resume, and ancient-block receipt decoding.
9. Reconcile the public README with what the code now actually implements for v1 versus future work.
10. Revisit batch-sizing/fallback strategy for huge bodies/receipt responses so honest peers are not penalized when soft response limits are hit on large blocks.
11. Keep validating blank-dir bootstrap quality on unrestricted networks; cold-start serving-peer conversion is improved, but it is still the key real-world metric to keep watching.

## Deferred

- Historical ETH transfer backfill is out of scope for v1 and stays deferred to a possible v2.
- Running a separate PostgreSQL-compatible storage engine is out of scope for v1; the preferred direction is embedded DataFusion over LogEx's own storage.
