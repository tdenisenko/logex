# LogEx Roadmap

## Goal

LogEx should become a canonical Ethereum event-log node that:

- uses Ethereum P2P only
- does not execute the EVM
- does not store the full state trie
- stores logs locally and makes them queryable
- covers the full Ethereum history rather than only the post-Merge era
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
- Receipt-trie verification and canonicality verification are different jobs.
  - Rebuilding the receipt trie proves that a set of receipts matches a specific block header.
  - It does not prove that the header itself is canonical.
  - Canonicality must come from consensus data:
    - post-Merge from the beacon light client
    - pre-Merge from verified PoW header ancestry and total difficulty
- Therefore the canonicality path should be:
  1. verify beacon light-client updates from a weak subjectivity checkpoint
  2. verify the SSZ Merkle branch that binds the `execution_payload_header` into the light-client header, the same way Helios validates `execution_branch` against the beacon header `body_root`
  3. treat the `receipts_root` inside that proven execution payload header as the canonical CL-verified receipt commitment for the corresponding execution block hash
  4. fetch raw receipts from EL peers for that execution block hash
  5. locally encode those receipts exactly as Ethereum does and rebuild the execution-layer receipt trie (MPT) from them
  6. require the computed EL receipt-trie root to match the CL-verified `receipts_root` before accepting any logs derived from those receipts

## Full-Chain Trust Model

- Weak subjectivity means a fresh client needs one recent trusted beacon checkpoint on first startup.
  - In practice that checkpoint is a recent beacon block root, optionally paired with a slot.
  - Development note recorded on April 16, 2026: an operator-provided example finalized beacon block root for future bootstrap testing is `0xfc5b0de0b6d9f78f6528ef455a8efddfdf61de272a9abe86441d62a8f63006f9`.
  - Development note recorded on April 17, 2026: a newer operator-provided example checkpoint for native mainnet smoke tests is `14132160@0x6181b33b475e9cf71a01033ad948aeb163f50f5cfa3c11bf56cbc3dc35fa3ed4`.
  - Treat that recorded root as an example `--checkpoint` input, not as a hardcoded protocol constant; it should be refreshed once it ages out of the weak-subjectivity window.
  - LogEx persists the resulting consensus state locally, so restart should not need the operator to re-enter it.
- CL-backed EL canonicality starts only once execution payloads exist in the beacon chain.
  - That means post-Merge execution blocks are proved by the beacon light-client path.
  - Pre-Merge EL blocks are not proved by the CL and need their own PoW canonicality path.
- The end-state trustless architecture must cover every block, but it does not need to sync from genesis upward.
  - LogEx should sync outward from the checkpoint block in both directions.
  - Start from a recent weak-subjectivity checkpoint near the current head.
  - Continuously sync with the live head from that checkpoint while also proving older history toward genesis.
  - Verify post-Merge canonicality from that checkpoint toward the live head.
  - At the same time, walk authenticated beacon ancestry downward from that checkpoint to the first execution payload and identify the canonical terminal PoW block.
  - From that terminal PoW block, verify the pre-Merge EL header chain downward to genesis using parent links, PoW rules, and total difficulty.
  - For every block on both sides of the Merge, rebuild receipts locally and require the computed trie root to match the canonical header's `receipts_root`.
  - Once the genesis-side verification is complete, LogEx should continue as a forward-only live-sync client from then on.
- Verification depth and stored log depth should be separate operator controls.
  - LogEx should verify canonicality and receipt roots all the way to genesis even when the operator does not want to keep logs for the entire history.
  - Add a client flag that sets the lowest block whose logs should be saved and indexed while the downward side of checkpoint-centered history proving moves toward genesis.
  - Example: if the operator sets the flag to `1_000_000`, LogEx should save and index logs only for blocks above that floor, but it must still continue verifying headers and receipt roots from block `1_000_000` down to genesis.
  - If the flag is omitted, it should default to genesis so the full log history is saved.
- This checkpoint-centered design is intentional.
  - Starting from genesis does not remove the weak-subjectivity assumption for PoS Ethereum.
  - Starting from a recent trusted checkpoint and then proving both toward the head and toward genesis gives full-chain coverage without pretending PoS can bootstrap from arbitrary ancient history with zero trust.

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
- The web UI and status surface now reflect the CL-aware roadmap direction:
  - the dashboard now strips out the old `mode` / `coverage` framing and focuses on checkpoint-centered sync progress only
  - the main sync bar now treats the weak-subjectivity checkpoint as the center marker and shows expansion toward the latest block and toward genesis separately
  - genesis-side progress is intentionally left empty until real downward verification exists, instead of implying that the right side is already being proved
- The native consensus-network foundation is now partially live:
  - LogEx now starts an embedded mainnet discv5 discovery service whenever checkpointed consensus mode is active
  - the node persists a CL discovery identity and a CL dialable-peer cache under the data directory
  - `/status` and the dashboard now expose native CL discovery state such as the local node id, local libp2p peer id, active discovery sessions, dialable peers, live libp2p peer sessions, and first light-client RPC responder counts
  - native libp2p peer-session management now runs on top of discovered ENRs
  - the first outbound single-response CL req/resp transport is wired for `Status`, `GetLightClientBootstrap`, `GetLightClientFinalityUpdate`, and `GetLightClientOptimisticUpdate`
  - outbound CL req/resp is now split by protocol family instead of trying to multiplex every request type through one shared libp2p request/response behaviour
  - `Status v2` and `MetaData v3` are now encoded and decoded correctly for current post-Fulu peers, while still keeping `Status v1`, `MetaData v2`, and `MetaData v1` fallback support
  - mainnet consensus chain constants are now explicit inside LogEx for handshake and future verification work, including the real beacon genesis block root and genesis validators root
  - ENR-to-libp2p peer-id derivation now follows the same secp256k1 conversion path used by Lighthouse, removing a concrete identity-mismatch risk from CL peer dialing
  - ENR-derived CL dial targets now prioritize QUIC ahead of TCP, matching Lighthouse's peer-dial ordering instead of treating TCP as the first choice
  - `Goodbye v1` is now implemented so LogEx can rotate bad peers using the consensus RPC instead of only dropping TCP sessions
  - the CL request scheduler now keeps peer-state counters honest by clearing them on disconnect, treats `Status` as the first handshake to finish before light-client fetches, and counts inbound `Status` as a completed handshake instead of waiting for a redundant round-trip
  - the CL request scheduler now respects a per-protocol in-flight request cap instead of blasting every connected peer at once
  - native CL status surfaces now expose identified-peer counts, protocol-capability counts, per-kind in-flight requests, and per-kind request-failure counters so live mainnet interop failures are inspectable instead of guesswork
  - `/status` now also records the most recent identify summary, most recent transport event, and most recent RPC failure so live mainnet interop debugging can distinguish peer-advertisement problems from raw transport churn
  - peers that do not advertise the full light-client req/resp set are now disconnected as soon as identify proves they are not useful for the light-client path, and peers that repeatedly fail `Status` are rotated out instead of being retained indefinitely
  - LogEx now advertises light-client history-fetch protocols honestly:
    - `LightClientBootstrap`, `LightClientUpdatesByRange`, `LightClientFinalityUpdate`, `LightClientOptimisticUpdate`, `BeaconBlocksByRange`, and `BeaconBlocksByRoot` are now outbound-only until LogEx can actually serve verified history to peers
    - `BeaconBlocksByRange` and `BeaconBlocksByRoot` now negotiate both v1 and v2 protocol ids instead of assuming only the latest variant
  - the CL handshake is now identify-gated:
    - new peer sessions are allowed to identify first
    - `Status` is now sent only to peers that actually advertise the `Status` RPC in identify
    - peers that identify without `Status` are treated as irrelevant and disconnected instead of being probed blindly
  - mainnet smoke runs from the recorded example checkpoint now go beyond discovery-only behavior:
    - live native CL discovery and libp2p peer sessions are established
    - a verified bootstrap store is persisted locally from native peers
    - `/status` now surfaces the checkpoint-derived finalized/optimistic execution anchor from that verified bootstrap store
  - fork-aware typed SSZ decoding is now implemented for current post-Merge light-client payloads:
    - `LightClientBootstrap` for Capella, Deneb, and Electra
    - `LightClientFinalityUpdate` for Capella, Deneb, and Electra
    - `LightClientOptimisticUpdate` for Capella and Deneb-or-later payload layouts
  - verified bootstrap/finality/optimistic summaries are now persisted in the CL state file and surfaced through CLI `info`, `/status`, and the dashboard so live consensus progress is inspectable from trusted light-client state rather than from decode-only payload snapshots
  - the local consensus ENR now advertises zeroed `attnets` and `syncnets` bitfields in addition to the mainnet `eth2` fork id, so LogEx presents a more standards-conformant CL identity on discovery
  - the CL swarm now supports QUIC transport in addition to TCP and keeps QUIC multiaddrs learned from peer ENRs instead of discarding them
  - the TCP side of the CL swarm now matches the production-client transport surface more closely:
    - TCP is now built explicitly with `noise` plus `yamux|mplex` negotiation instead of relying on the default builder path
    - the TCP handshake now has an explicit timeout
    - the combined TCP/QUIC transport is now DNS-wrapped the same way production beacon clients typically build their libp2p stack
  - when the operator uses the same UDP port for CL discovery and CL p2p, LogEx now skips the inbound QUIC listener instead of crashing on a port bind conflict; outbound QUIC dialing still remains available in that configuration
  - LogEx now subscribes to the `light_client_finality_update` and `light_client_optimistic_update` gossip topics for the current fork digest and exposes gossip subscription / decode counters through `/status`
  - consensus gossip payloads are now decompressed with the spec's snappy-block rule and decoded for observability, but LogEx still intentionally does not forward them as validated canonical data until the gossip path is wired into the verified light-client store
  - restart behavior for checkpoints is stricter and more honest:
    - rerunning with the same checkpoint root plus a known slot enriches persisted consensus state
    - rerunning with a conflicting checkpoint root or slot now fails instead of silently mixing trust bases
- The CL-driven storage and runtime boundary is now materially in place:
  - `WeakSubjectivityCheckpoint`, `ExecutionAnchor`, and `ChainAnchors` are shared types used across runtime, sync, storage, and status surfaces
  - `crates/logex-cl` exists and persists checkpoint state plus ordered execution anchors under the data directory
  - the node now accepts `--checkpoint`, persists consensus state on first startup, reloads it on restart, and exposes checkpoint/finality/optimistic/indexed-head status through CLI `info` and `/status`
  - fresh data directories now require a checkpoint before they can start canonical sync
  - storage now persists execution-facing chain anchors separately from log rows
  - finalized-head-aware compaction is live whenever finalized CL anchors exist
  - the anchored EL sync path now fetches headers by block hash, validates the fetched header against the CL-derived execution anchor, validates body/receipts locally, and only then ingests logs
  - anchored sync now rewinds indexed canonical state on optimistic anchor replacement within the persisted recent-header window instead of silently continuing on stale canonical data
  - the verified light-client store is now real instead of decode-only:
    - weak-subjectivity bootstrap is checked against the trusted beacon block root
    - bootstrap current-sync-committee proofs are verified against the beacon state root
    - optimistic and finality singleton updates are signature-checked before they mutate trusted state
    - full `LightClientUpdate` payloads from `LightClientUpdatesByRange` are now decoded, signature-checked, next-sync-committee-checked, and applied to the trusted store
    - the store now tracks `best_valid_update`, next-sync-committee learning, and spec-style forced progression after `UPDATE_TIMEOUT`
    - checkpoint-derived finalized and optimistic execution anchors now come only from that verified store
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
  - native CL discovery is implemented
  - native CL libp2p peer sessions are implemented
  - the first raw single-response CL req/resp transport is implemented
  - that transport now uses per-method protocol families instead of one shared outbound req/resp family
  - `Status v2` and `MetaData v3` are implemented with backward compatibility for older peers, and `MetaData v1` fallback support is now in place as well
  - `Goodbye v1` is implemented so peer eviction can use the consensus RPC instead of only raw disconnects
  - the CL request scheduler now prioritizes `Status` as the connection handshake, counts inbound `Status` as success, and no longer treats disconnected responders as healthy current peers
  - the CL scheduler now keeps per-protocol request concurrency within the consensus req/resp limit instead of fanning out unbounded requests
  - `/status` now surfaces identified peers, protocol-capability counts, per-kind in-flight requests, and per-kind request-failure counters for native CL debugging
  - peers that do not advertise the full light-client req/resp set are now disconnected immediately after identify, and peers that repeatedly fail `Status` are rotated out instead of being held forever
  - typed SSZ decoding and persisted status summaries for light-client bootstrap/finality/optimistic payloads are implemented for current post-Merge fork layouts
  - light-client gossip subscriptions are implemented for finality and optimistic updates, and `/status` now shows gossip subscription / decode counters
  - outbound QUIC transport is implemented and peer ENRs are now harvested for both TCP and QUIC dial addresses
  - consensus dialing is now address-aware instead of spraying every ENR address at once:
    - each peer attempt now chooses a small ranked transport set instead of blindly dialing all TCP/QUIC and IPv4/IPv6 paths together
    - per-peer dial outcomes now remember which address families and transport types have succeeded or failed, and that memory survives restarts through the known-peer cache
    - bootstrap-phase dialing now prefers the most stable paths first and naturally rotates to alternate transports only after the first choice fails
  - the discovery loop now periodically rehydrates dial candidates from the live discv5 routing table, so newly learned ENRs can graduate into the CL dial pool even if they were not surfaced as explicit discovery events
  - the current scheduler once again behaves like a light client should:
    - identify is now the first admission gate on new peer sessions
    - `Status` is sent only after identify confirms that the peer actually advertises the `Status` RPC
    - while LogEx is not yet serving verified light-client history to peers, it now uses a genesis-style `Status` message that follows the spec more closely: the head side is anchored to the real mainnet beacon genesis block root while the finalized checkpoint remains the canonical zero root for epoch 0, instead of advertising checkpoint-rooted availability it cannot serve
    - bootstrap is prioritized before finality / optimistic requests, instead of blasting every light-client method at once
  - verified weak-subjectivity bootstrap is implemented and already lands in fixed-port short mainnet smokes from the recorded real checkpoint
  - cryptographic verification is now implemented for the trusted light-client store path:
    - bootstrap proof verification
    - `execution_branch` verification for trusted light-client headers
    - sync-committee signature verification for optimistic / finality / full updates
    - next-sync-committee proof verification for full updates
  - multi-chunk CL req/resp now splits into two states:
    - `LightClientUpdatesByRange` is upgraded into verified light-client store updates
    - beacon-block fetches still need the authenticated beacon-history segment walker before they can produce contiguous execution anchors
  - current gossip subscriptions decode payloads for observability, but they still do not perform sync-committee validation or feed canonical anchor production yet
  - recent mainnet smokes are healthier than before the latest interop fixes:
    - pre-bootstrap `Status` now uses the genesis head / zero finalized-checkpoint form expected by the spec
    - ENR-derived peer IDs now follow the same secp256k1 conversion path used by Lighthouse
    - the dialer now records concrete address-level failures instead of opaque peer-level churn, which makes `/status` useful for transport debugging
    - bootnodes are once again usable as low-priority fallback libp2p dial targets instead of being discovery-only seeds
    - the reusable smoke cache is now kept cleaner: peers that previously identified without beacon `Status` support are no longer persisted, and ENRs carrying the `opstack` marker are filtered out before they can poison future CL dial attempts
    - with checkpoint `14132160@0x6181b33b475e9cf71a01033ad948aeb163f50f5cfa3c11bf56cbc3dc35fa3ed4`, short fixed-port smokes now persist a verified bootstrap store and surface the checkpoint execution anchor through `/status`
    - the repaired scheduler no longer treats successful post-bootstrap dynamic requests as permanently satisfied, so finality / optimistic / updates-by-range work can be polled continuously instead of freezing after the first success
    - the remaining short-run blocker is now narrower and more honest: in the latest smoke, bootstrap-capable peers landed quickly, but the observed peers exposed `updates_by_range=false`, and the singleton finality / optimistic requests still did not complete in that short run
- The current branch also does not yet implement the pre-Merge PoW canonicality path.
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
   - Done so far:
     - native libp2p peer dialing and stream management now run on top of discovered CL ENRs
     - outbound single-response req/resp transport is wired for `Status`, `GetLightClientBootstrap`, `GetLightClientFinalityUpdate`, and `GetLightClientOptimisticUpdate`
     - outbound req/resp is now split into per-method protocol families so `Status`, `GetLightClientBootstrap`, `GetLightClientFinalityUpdate`, and `GetLightClientOptimisticUpdate` no longer negotiate against the wrong protocol id on the wire
     - `Status v2` and `MetaData v3` are now implemented for current post-Fulu peers, while preserving `Status v1`, `MetaData v2`, and `MetaData v1` fallback compatibility
     - `Goodbye v1` is now implemented so peer rotation can use the consensus RPC instead of only dropping TCP sessions
     - the CL scheduler now treats identify as the first admission gate and only sends `Status` after a peer explicitly advertises the `Status` RPC
     - inbound peer `Status` requests are now tracked separately so they no longer get mistaken for a successful outbound `Status` round-trip
     - peer success counters are now cleared on disconnect so `/status` reflects live CL session health rather than stale historical responders
     - the CL scheduler now caps in-flight requests per protocol kind to the consensus req/resp concurrency limit instead of blasting every connected peer at once
     - the CL scheduler now keeps bootstrap ahead of finality / optimistic requests until a real bootstrap payload has landed, which is closer to the light-client sync process than firing every request type in parallel
     - post-bootstrap dynamic CL requests are no longer treated as permanently satisfied after the first success, so finality / optimistic / updates-by-range work can keep polling instead of freezing at the checkpoint
     - `/status` now exposes identified peers, protocol-capability counts, per-kind in-flight requests, per-kind request-failure counters, and the latest identify/transport/RPC breadcrumbs for native CL debugging
     - `/status` now also exposes light-client gossip subscription counts plus finality / optimistic gossip decode counters
     - peers that do not advertise the full light-client req/resp set are now disconnected as soon as identify proves they are not useful for the light-client path, and peers that repeatedly fail `Status` are rotated out instead of being kept forever
     - the CL scheduler now keeps peer lifecycle memory across disconnects, backs off churny peers with exponential cooldowns, ignores peers that identify as irrelevant for the current phase, and prioritizes peers that have previously returned useful CL responses
     - cached discovery peers are now persisted in usefulness order instead of raw ENR order so restarts retry the best-known CL candidates first
     - the durable known-peer cache is no longer truncated to the live peer limit; LogEx now retains a much larger ranked pool of useful CL candidates across restarts instead of relearning from only a dozen peers every run
     - discv5 bootnodes are now treated as discovery seeds first instead of automatic libp2p dial targets, so early CL dialing is less likely to waste budget on nodes that only help discovery
     - the singleton light-client req/resp methods that LogEx can answer honestly today now negotiate both inbound and outbound directions:
       - `GetLightClientBootstrap`
       - `GetLightClientFinalityUpdate`
       - `GetLightClientOptimisticUpdate`
     - `LightClientUpdatesByRange`, `BeaconBlocksByRange`, and `BeaconBlocksByRoot` remain outbound-only until LogEx can serve those methods with correct local data instead of generic placeholders
     - live mainnet smoke runs from the recorded example checkpoint now go past discovery-only behavior and land a verified bootstrap store from native peers
     - fork-aware typed SSZ decoding now exists for current post-Merge `LightClientBootstrap`, `LightClientFinalityUpdate`, and `LightClientOptimisticUpdate` payloads
     - verified bootstrap/finality/optimistic summaries are now persisted in the CL state file and surfaced through CLI `info`, `/status`, and the dashboard
     - raw bootstrap/finality/optimistic RPC payloads are now persisted alongside the decoded summaries, and LogEx now serves those exact cached payloads back to inbound peers when they are available locally
     - the local consensus ENR now includes `attnets` / `syncnets`, and LogEx now subscribes to the light-client finality / optimistic gossip topics for the current fork digest
     - the consensus transport now includes outbound QUIC dialing, peer ENRs are harvested for QUIC addresses as well as TCP, and shared discovery/p2p UDP ports no longer crash the node when QUIC is enabled
     - restarting with the same checkpoint root plus a newly supplied slot now enriches the persisted checkpoint instead of forcing a fresh data directory, while conflicting checkpoint roots still fail loudly
     - mainnet consensus chain constants are now recorded explicitly inside LogEx, including the genesis time plus the stable fork schedule through Fulu
     - LogEx now derives its own current mainnet `fork_digest` and ENR `eth2` fork id from the consensus-spec fork schedule instead of copying whichever bootnode `eth2` field happened to be encountered first
     - the pre-bootstrap `Status` handshake now uses the real beacon genesis block root plus the canonical zero finalized checkpoint rather than a placeholder-style root or an unserved checkpoint root, and ENR-derived peer IDs now follow the same secp256k1 conversion used by Lighthouse
     - native multi-chunk CL req/resp is now implemented for `LightClientUpdatesByRange`, `BeaconBlocksByRange`, and `BeaconBlocksByRoot`, including chunked stream parsing, per-method protocol advertisement, peer capability tracking, and response/error accounting
     - `LightClientUpdatesByRange` payloads are now decoded, verified, and applied into the trusted light-client store instead of being treated as raw opaque chunks only
     - the verified store now tracks next-sync-committee learning, `best_valid_update`, and spec-style forced progression after `UPDATE_TIMEOUT`
     - the CL scheduler now treats checkpoint-history fetches as first-class work: it will try to fetch the checkpoint beacon block by root, fall back to slot-based block range fetches when the checkpoint slot is known, and request one sync-committee-period update window once the slot context exists
     - the bootstrap scheduler now keeps checkpoint-history peers alive during bootstrap instead of disconnecting them merely because they do not advertise `LightClientBootstrap`; peers that can serve beacon blocks by root/range are now considered useful before bootstrap lands
     - discovery now fans out across multiple random `FINDNODE` targets per cycle instead of a single random lookup, so fresh runs can accumulate dialable CL ENRs faster
     - optional `MetaData` probing has been moved off the bootstrap-critical path so it no longer burns otherwise useful peers before the first successful light-client bootstrap
     - outbound `Status` for the limited-data light-client phase now follows the Altair light-client networking guidance instead of advertising a private checkpoint-backed history surface:
       - `head_root` stays anchored to the real mainnet beacon genesis block root
       - the finalized checkpoint remains the canonical zero-root epoch-0 form
       - `earliest_available_slot` stays at genesis until LogEx can honestly serve more than limited local light-client data
     - outbound req/resp now prefers the most widely interoperable protocol versions first: `Status v1` ahead of `Status v2`, and `MetaData v2` ahead of `MetaData v3`
     - remembered peer support and usefulness scores are now persisted with cached ENRs, survive restarts, and feed the next run's CL peer prioritization instead of being relearned from scratch
     - outbound consensus dialing is now address-aware:
       - each dial attempt picks a ranked transport/address candidate set instead of trying every ENR address at once
       - per-peer TCP/QUIC and IPv4/IPv6 success/failure history is persisted with the known-peer cache and reused on restart
       - bootstrap-phase dialing now prefers the most stable address families first and only falls back to alternate transports after failures
     - the discovery loop now repopulates CL dial candidates from the live discv5 routing table on every query cycle, so the dial pool can grow beyond static bootnodes and cached peers
     - ENR fork filtering now matches the current 4-byte fork digest instead of requiring the entire 16-byte `ENRForkID` tuple to match exactly, which aligns with the consensus spec and admits peers that share the current fork but advertise a different next-fork schedule
     - cached ENRs that previously identified without beacon `Status` support are now dropped on load and no longer persisted back to disk, and ENRs carrying the `opstack` marker are filtered out before they can enter the reusable CL peer cache
     - bootnodes are now reintroduced as low-priority fallback dial targets, and the TCP transport now follows the same explicit `noise` + `yamux|mplex` + timeout shape used by production beacon clients like Lighthouse more closely than the earlier builder-default stack
   - TODO:
     - finish the remaining post-bootstrap mainnet interop gap: the light-client store now bootstraps natively, but short smokes still have not landed verified finality / optimistic / updates-by-range progression beyond the checkpoint
     - keep narrowing the remaining transport churn beyond address selection itself, especially peers that still reset or EOF during the libp2p/noise handshake before identify or beacon `Status`
     - keep proving that `Status` is really stable across Nimbus/Teku/Lighthouse-class peers now that `Status v1` is preferred and the TCP transport matches the production-client muxer surface more closely, and fix any remaining edge cases where peers accept the connection long enough to be dialed but still die before identify, `Status`, or post-bootstrap light-client requests can complete
     - finish separating actual beacon peers from discovery noise earlier in the pipeline so the dial budget is spent on peers that can really speak the beacon req/resp surface, not just nodes that happen to share a compatible ENR fork digest
     - keep hardening `GetLightClientFinalityUpdate` and `GetLightClientOptimisticUpdate` until responses are flowing steadily enough to advance the live store instead of staying pending in short smokes
     - keep the root-only checkpoint path honest: recover the slot from native bootstrap once bootstrap lands, and require `slot@root` only if live root-only bootstrapping remains provably unreliable
     - keep proving that `LightClientUpdatesByRange` lands on real mainnet peers often enough to rotate committees and drive the store forward in practice, not only in unit tests
     - upgrade beacon-block transports from raw chunk streams into decoded, verified block objects that feed backward beacon-history proving
     - upgrade the current gossip path from parse-only observability to real light-client validation and head tracking after the verified req/resp bootstrap path exists
     - persist and serve correct checkpoint-related `LightClientUpdatesByRange` responses once the local cache/model can answer them honestly
   - Why this is still blocking:
     - the node can now discover and dial native CL peers over TCP and QUIC, maintain libp2p sessions, speak version-aware per-method req/resp for both single-response and multi-chunk methods, advertise a more standards-conformant ENR, subscribe to light-client gossip, and persist a verified bootstrap store from native peers, but short mainnet smokes still are not advancing the verified store beyond the checkpoint
     - peer-selection noise has been reduced enough that `/status` now points at the next meaningful blocker: bootstrap-capable peers land quickly, but recent identified peers have exposed `updates_by_range=false`, and singleton finality / optimistic requests still remained pending in the latest fixed-port smoke
     - until finality / optimistic / updates-by-range progression lands reliably on live peers, LogEx cannot yet claim that the native CL path is producing live forward movement beyond the checkpoint
   - Done when:
     - a fresh mainnet sync can start from a weak-subjectivity checkpoint, discover peers natively, and produce verified optimistic/finalized execution anchors without any external consensus RPC

2. Beacon Block Segment Authentication
   - TODO:
     - fetch full beacon blocks between trusted light-client headers
     - verify each full block body against its signed beacon header by recomputing `body_root`
     - walk parent roots backward from the weak-subjectivity checkpoint toward the Merge so every emitted execution anchor is authenticated, ordered, and contiguous
     - derive execution payload headers from the verified block bodies rather than trusting imported execution anchor files forever
   - Why this is still blocking:
     - the current anchor store can hold per-block execution anchors, but LogEx still needs the beacon-side segment walker that creates those anchors from verified CL data
   - Done when:
     - every execution anchor used by EL ingestion can be traced back to verified beacon blocks bounded by verified light-client headers

3. Pre-Merge PoW Canonicality
   - TODO:
     - identify the canonical terminal PoW block from verified post-Merge beacon ancestry
     - verify pre-Merge EL headers downward from that terminal PoW block to genesis
     - enforce parent-link validity, difficulty rules, and cumulative total difficulty for the PoW era
     - use each verified pre-Merge canonical header's `receipts_root` as the commitment that receipt-trie reconstruction must match
   - Why this is still blocking:
     - the beacon light client only proves post-Merge execution payloads
     - without a separate PoW canonicality path, LogEx cannot honestly claim trustless coverage for the full pre-Merge history
   - Done when:
     - LogEx can explain the canonicality source for every block:
       - post-Merge from CL verification
       - pre-Merge from PoW header verification

4. Remove Transitional Consensus Shortcuts
   - TODO:
     - delete the remaining operator-facing language that treats CL networking ports as merely reserved
     - delete the remaining legacy EL-only startup path once live CL anchor production is in place
     - stop relying on imported checkpoint descriptor files as the normal way to feed execution anchors into the node
     - tighten storage/runtime assumptions so checkpointed canonical sync is the only steady-state ingestion path
   - Why this is still blocking:
     - the final architecture should not leave a half-canonical fallback path around after the real CL path exists
   - Done when:
     - canonical sync always means CL-driven sync, not a mix of CL mode and legacy EL-only mode

5. Selective Log Retention While Verifying Full History
   - TODO:
     - add a client flag that sets the lowest block whose logs should be saved and indexed while checkpoint-centered history proving expands toward genesis
     - default that flag to genesis when omitted
     - continue verifying canonical headers and receipt roots below that floor all the way to genesis without persisting those older log rows
     - make status and docs explicit that verification depth and retained log depth are different
   - Why this is still blocking:
     - some operators need trustless canonical verification for all history without paying storage costs for all historical logs
   - Done when:
     - LogEx can verify the entire chain to genesis while storing only the operator-selected suffix of historical logs

6. Mainnet Proving Runs And Failure Handling
   - TODO:
     - run long-lived mainnet sync tests from real weak-subjectivity checkpoints
     - run full-history proving runs that start from a recent checkpoint and expand both toward the live head and downward across the Merge into pre-Merge history
     - verify restart, shutdown, and resume across optimistic updates, finality advances, and anchor replacements
     - test malicious or incomplete EL responses against CL-driven anchors on real network conditions
     - improve recovery behavior for optimistic reorgs deeper than the persisted recent-header window
   - Why this is still blocking:
      - the current code now rewinds correctly within the stored recent-header window, but a deeper optimistic reorg still requires stronger recovery logic or an explicit resync path
   - Done when:
      - LogEx can survive realistic mainnet churn and either recover safely or fail loudly with a precise operator action

7. Release Gate
   - TODO:
     - keep the current receipt-root, restart, corruption-detection, and cross-surface consistency coverage green as the native CL path lands
     - add end-to-end fixtures for checkpoint bootstrap, light-client updates, beacon blocks, execution anchors, EL headers, bodies, and receipts
     - add end-to-end fixtures that cross the Merge boundary and cover the pre-Merge PoW verification path
     - run the benchmark harness on realistic ERC-20 / ERC-721-heavy datasets and record target throughput / latency numbers
     - align README and operator docs with the actual guarantees once the native CL path is complete
   - Done when:
      - the node can sync, restart, query, and serve logs from the final CL-driven architecture with measured performance and truthful documentation

## Sequencing Decision

Do the CL implementation first.

Why:

- Without verified CL anchors, EL work improves performance but not canonicality.
- The CL output defines the exact EL data LogEx should trust and verify.
- Once the CL anchor format is fixed, the EL side becomes a much clearer adaptation of the current receipt pipeline, and the Merge boundary into pre-Merge PoW verification becomes well-defined.
- This keeps LogEx aligned with its real goal: canonical logs without EVM execution.
- The other TODOs stay in scope; this only sets the order of work.

## Validation Target

LogEx should eventually be able to say all of the following:

- it bootstrapped from a weak subjectivity checkpoint
- it verified beacon light-client updates locally
- it verified the execution payload header against the beacon light-client header with an SSZ Merkle proof
- it learned the canonical execution block hash and receipts root from that proven CL data
- it expanded verified coverage outward from the checkpoint, including downward across the Merge to the canonical terminal PoW block
- it verified the pre-Merge PoW header chain from that terminal PoW block down to genesis
- it fetched receipts from EL peers over devp2p
- it re-encoded those receipts and recomputed the execution-layer receipt trie locally
- it only accepted logs whose receipts root matches the canonical header proved by the appropriate consensus system for that era

## Later Expansion

- Historical ETH transfer backfill remains deferred.
- LogEx can later expand the same proof-based model beyond event logs by verifying and exposing additional execution payload roots:
  - `state_root` for account balances, nonces, and code hashes
  - `receipts_root` for logs, gas used, and transaction success or revert status
  - `transactions_root` for transaction inclusion proofs
  - `withdrawals_root` for validator withdrawals after Shanghai
