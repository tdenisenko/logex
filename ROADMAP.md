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
- Bootnodes are discovery-only, not fake sync peers.
- DNS discovery and stricter peer honesty/status reporting are in place.
- Custom outbound-only TCP/discovery code has now been removed.
- LogEx now runs on top of Reth’s actual network manager/session stack:
  - real listener socket
  - real peer/session lifecycle management
  - discv4 + DNS bootstrap under Reth’s manager
  - persisted productive peers reseeded into the live network stack on restart
- Current validation on this refactor:
  - `cargo check -p logex-node`
  - `cargo fmt --all`
  - `cargo test --workspace --all-targets`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - release-mode smoke run on `http://127.0.0.1:18444/status`
  - in this environment the release binary stayed honest and quickly accumulated a large pending candidate set (`pending_peers=139` after a few seconds, `640` shortly after), but still could not prove a real serving peer because outbound network access here is limited

## Next TODO

1. Validate the new Reth-backed network stack against real mainnet peers outside the sandbox:
   - fresh data dir
   - warm restart with persisted peers
   - sustained historical sync
   - graceful shutdown and resume
2. Improve candidate-to-serving-peer conversion further if real-world cold starts are still slower than geth/reth.
3. Decide whether LogEx should accept inbound sessions as a non-serving node exactly as-is, or advertise a more conservative served range/status.
4. Persist a recent canonical header window so restart-boundary reorg recovery is durable.
5. Add end-to-end network regression coverage for bootstrap, restart, shutdown, and resume.
6. Reconcile the public README with what the code now actually implements.

## Deferred

- Historical ETH transfer backfill is out of scope for v1 and stays deferred to a possible v2.
