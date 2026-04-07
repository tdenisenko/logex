# LogEx Roadmap

## Direction

- Keep the current Reth networking crates for now.
- Expand the Reth networking surface selectively where it materially improves bootstrap or protocol correctness.
- Rewriting DevP2P, ECIES, `eth` wire, and discovery from scratch before sync reliability is fully hardened would slow the project down and likely reintroduce bugs that mature clients have already solved.
- The better near-term path is:
  - keep using the narrow Reth networking surface already in `logex-sync`
  - stabilize cold-start discovery, sync correctness, restart behavior, and operator UX first
  - only then decide whether specific pieces should be internalized or replaced one by one
- Full removal of direct Reth crates is still possible later, but it is not the best next task for v1.

## Done

- Standalone P2P log-sync node exists and runs without external RPC.
- Logs are stored locally in columnar partitions and can be queried via SQL-like interfaces.
- REST, JSON-RPC, gRPC, WebSocket, terminal status, and web UI are in place.
- Sync head persistence is correct even for empty-log blocks.
- Query correctness no longer depends on indexes being present.
- Hot-partition index rebuilds survive partition rotation.
- Bootnodes are discovery-only, not fake sync peers.
- Peer identity and known peers persist across restart.
- Only productive serving peers are now written to `known-peers.json`.
- Sync state reporting is more honest:
  - no false `Synced` state before real progress
  - no fake "historical sync complete" on empty responses without a credible target
  - UI/terminal distinguish connecting, reconnecting, syncing, and awaiting a serving peer
- Warm restart behavior is materially better and resumes from persisted head.
- Cold-start discovery has been made more aggressive:
  - discv4 now uses tighter startup-oriented lookup/ping timing
  - active lookup now includes self lookups plus random lookups
  - persisted productive peers are also seeded into discv4, not just the TCP dial queue
- Reth DNS discovery is now part of bootstrap:
  - DNS ENR candidates are merged into the same dial queue as discv4 candidates
  - DNS candidates are prioritized ahead of generic discv4 candidates
  - startup now eagerly waits for an initial DNS batch before falling back to background-only discovery
- Recently failed dial targets are now cooled down briefly so discovery can move on to fresh candidates instead of redialing the same weak peers immediately.
- Latest live validation:
  - warm restart on `/tmp/logex-live-persist-test5` quickly reached `connected_peers=2`, `serving_peers=1`, resumed from block `14847`, and advanced the persisted sync head to block `18943`
  - fresh-dir cold start on `/tmp/logex-dns-cold2` reached a non-empty pending queue faster (`pending_peers=136` after ~15s, `269` after ~45s) while staying honest about not yet being synced
  - fresh-dir cold start is still the main open gap: candidate discovery is better, but this environment still does not consistently turn that into a serving peer quickly

## Next TODO

1. Improve blank-dir candidate-to-serving-peer conversion until fresh data dirs reach a useful peer more consistently.
2. Add end-to-end network integration tests for:
   - cold start
   - warm restart
   - graceful shutdown
   - resume from persisted head
3. Persist a recent canonical header window so restart-boundary reorg recovery is durable.
4. Keep tightening peer scoring and request routing so weak peers are deprioritized faster.
5. Decide whether to add discv5 if discv4 + DNS cold starts still remain behind geth/reth.
6. Reconcile the public README with what the code actually implements.
7. Decide later whether to internalize selected Reth-derived networking pieces after v1 sync behavior is stable.

## Deferred

- Historical ETH transfer backfill is out of scope for v1 and stays deferred to a possible v2.
