# LogEx Roadmap

## Direction

- Stay close to a real Ethereum execution node on the networking side, while keeping LogEx log-only on storage/execution.
- Use Reth crates where they remove networking risk or missing protocol behavior.
- Do not spend v1 time re-implementing peer discovery, session management, or wire handling that mature clients already ship and test.
- Revisit internalizing selected networking pieces only after bootstrap, sync stability, and operator UX are solid.

## Done

- Standalone log-only node exists: no external RPC, no EVM execution, logs stored locally and queryable.
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
  - inbound `eth` requests are now wired through Reth's request-handler path so peers get prompt empty responses instead of timing out on silent drops
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
- Old direct dependencies from the previous custom networking path were removed from `logex-sync`.
- Live-network validation has now gone further:
  - release-mode sync resumed from persisted head `46146`
  - crossed the old failure point and advanced past block `50,000`
  - persisted sync metadata advanced to block `54946`
  - restart resumed from `54946` instead of starting over
  - `known-peers.json` remained populated with serving peers across restart
- Current validation on this refactor:
  - `cargo fmt --all`
  - `cargo test -p logex-sync`
  - `cargo clippy -p logex-sync -- -D warnings`
  - `cargo build --release --bin logex`

## Next TODO

1. Improve warm-restart reconnect quality so persisted serving peers more reliably convert back into fast sessions right after restart.
2. Fix the remaining p2p shutdown rough edge: the node exits, but the network task still sometimes needs to be aborted after the timeout instead of stopping cleanly on its own.
3. Continue comparing the sync/request path against Reth/geth and decide whether to keep the current lightweight scheduler or adopt more of Reth’s downloader pipeline for headers/bodies/receipts.
4. Persist a recent canonical header window so restart-boundary reorg recovery is durable.
5. Add end-to-end regression coverage for bootstrap, restart, shutdown, resume, and ancient-block receipt decoding.
6. Reconcile the public README with what the code now actually implements for v1 versus future work.
7. Revisit batch-sizing/fallback strategy for huge bodies/receipt responses so honest peers are not penalized when soft response limits are hit on large blocks.

## Deferred

- Historical ETH transfer backfill is out of scope for v1 and stays deferred to a possible v2.
