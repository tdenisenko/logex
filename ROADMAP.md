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
  - LogEx can now fetch the current finalized checkpoint from a trusted Beacon API/checkpoint-sync URL with `--checkpoint-sync-url`, using `/eth/v1/beacon/headers/finalized` as a startup bootstrap aid until LogEx has its own checkpoint distribution endpoint.
  - A slotted checkpoint or persisted verified consensus store is rejected on startup if its trusted slot is outside LogEx's conservative weak-subjectivity freshness window; root-only checkpoints can only be freshness-checked after a checkpoint-sync endpoint resolves their header slot or after bootstrap persists the slot.
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

- EL foundation is solid:
  - standalone EL P2P sync works without external RPC
  - Reth-backed discovery/session/fetch paths are in place
  - restart/resume, recent canonical-header persistence, receipt-root verification, and log ingestion/query surfaces are already working
- Native CL foundation is materially in place:
  - embedded mainnet discv5, libp2p peer sessions, TCP+QUIC transport, per-method req/resp, gossip subscriptions, typed SSZ decoding, and native status/debug surfaces are implemented
  - verified weak-subjectivity bootstrap, verified singleton/full light-client updates, sync-committee checks, `execution_branch` checks, and verified head progression beyond the checkpoint are implemented
  - verified `LightClientUpdatesByRange` payloads are now persisted by sync-committee period and served back over the native CL req/resp surface when the local cache can answer honestly
  - full beacon-block decoding, truthful cached beacon-history serving, backward checkpoint-to-older-history materialization, forward root-chasing, and reorg-safe forward anchor pruning are implemented
- CL-driven runtime/storage boundaries are in place:
  - `WeakSubjectivityCheckpoint`, `ExecutionAnchor`, and `ChainAnchors` are shared across runtime, sync, storage, and status surfaces
  - `crates/logex-cl` persists checkpoint state and ordered execution anchors under the data directory
  - checkpointed startup, restart persistence, finalized-head-aware compaction, anchored EL header/body/receipt validation, and optimistic reorg rewinds are implemented
- Query/storage work from prior slices remains complete and intentionally summarized here:
  - native segment storage and compaction are in place
  - REST/gRPC/UI share the same canonical query core
  - SQL uses DataFusion over native storage, `eth_getLogs` uses the native filter path, and cross-surface consistency coverage exists

## Current Status

- LogEx already has the correct CL-owned persistence model and EL verification boundary:
  - EL peers are data providers only once consensus state exists
  - ordered execution anchors decide which EL blocks are eligible for ingestion
  - optimistic, finalized, and indexed execution heads are persisted explicitly, and optimistic reorg rewinds are implemented
- The native CL path is real rather than placeholder:
  - embedded discv5 discovery, libp2p peer sessions, TCP+QUIC transport, per-method req/resp, gossip subscriptions, typed SSZ decoding, verified bootstrap, verified light-client updates, and verified head progression beyond the checkpoint are implemented
  - full beacon-block decoding, truthful cached history serving, backward checkpoint-to-older-history materialization, forward root-chasing, and reorg-safe forward anchor pruning are implemented
  - fixed-port mainnet smokes from `14132160@0x6181b33b475e9cf71a01033ad948aeb163f50f5cfa3c11bf56cbc3dc35fa3ed4` now persist a verified bootstrap store, expose checkpoint/finality/optimistic execution anchors through `/status`, and show verified head movement beyond the checkpoint
  - a fresh mainnet smoke on May 5, 2026 from `14263616@0xad227f7642484a8c957306069387c2555beab50898ab5a3d5342cd4fcb078267` bootstrapped successfully, verified optimistic/finality updates, and materialized 213 CL-authenticated execution anchors spanning slots `14263488..14263701`
- The native CL P2P milestone from `cl-canonical-verification` has been merged through PR #67:
  - a fresh fixed-port mainnet smoke on May 5, 2026 resolved a current finalized checkpoint via `--checkpoint-sync-url`, bootstrapped from scratch, reached the verified optimistic head, and materialized 343 CL-authenticated execution anchors spanning beacon slots `14263514..14263857`
  - peer churn is still present on public mainnet, but cooldown-aware slot rotation, stricter ENR filtering, wider status/history request concurrency, and invalid/empty history-response accounting are now enough for fresh short smokes to keep the materialized ceiling at the live optimistic head
- The merged CL P2P milestone also does not yet implement the pre-Merge PoW canonicality path, so the project is still not the full end-to-end canonical system.
- The clippy cleanup branch `fix/clippy-cleanup` has been merged through PR #68, keeping lint-only changes separate from UI work.
- The dashboard UI work is merged, and the CL historical-sync fix is merged through PR #70.
- The CL history performance branch is validating native beacon-history throughput with a fixed-port release smoke on `18683`.
  - Latest observed fixed-port smoke state at roadmap update: materialized floor block `24996057`, materialized ceiling block `25035461`, `39405` anchors, and `0` detected anchor-continuity gaps.

## Explicitly Not Needed

- A full state trie
- EVM execution
- A local full consensus client as a mandatory dependency
- An external execution RPC URL
- An external consensus RPC URL in the final design
- Engine API compatibility with a standard beacon node as the primary canonicality path

## Design Decisions

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
- A checkpoint-sync URL is acceptable as a temporary startup bootstrap/freshness helper, but not as a steady-state consensus dependency.
  - This matches Lighthouse-style checkpoint sync ergonomics while preserving LogEx's native CL P2P direction.
  - The endpoint is used to obtain or validate a recent weak-subjectivity checkpoint; canonical head progression and beacon-history materialization still come from native CL P2P and verified light-client data.
- Use a conservative fixed mainnet weak-subjectivity freshness window until LogEx persists enough beacon state to compute the full state-derived spec value locally.
  - This favors refusing stale startups over accepting questionable checkpoints.
  - The remaining tradeoff is that the fixed bound should later be replaced with the exact consensus-spec weak-subjectivity-period computation.
- Keep forward and backward beacon-history work scheduled independently.
  - Public mainnet peers frequently close range streams, so root-based parent recovery must be allowed to run alongside range backfill.
  - Persisting parent beacon roots with execution anchors keeps restarts from losing the authenticated backward walk; legacy anchors without that field are recovered by refetching the oldest known beacon root once.
- Report CL materialized-anchor coverage from the consensus store rather than deriving floor, ceiling, and count separately in each status surface.
  - The status API now includes an anchor-continuity gap count based on contiguous execution block numbers and matching beacon parent roots, so live smokes can prove that backward expansion is not silently creating holes.
- Treat the CL history target for execution-payload verification as the first PoS execution block, `15537394`, not genesis.
  - Bellatrix beacon blocks carry execution payloads only after the Merge transition; pre-Merge log canonicality belongs to the separate EL PoW verification path.
- Show CL history ETA from a rolling browser-side floor-movement sample.
  - The ETA is sampled every 30 seconds over a 10-minute window to avoid the per-poll jitter that made earlier UI counters misleading.
- Ignore peers for the current run when libp2p proves the dialed endpoint has the wrong peer identity.
  - These stale discovery records are not useful transient failures, and retrying them wastes CL dial slots during history sync.

## Completed Since Last Run

- Removed the main CL history materialization bottleneck: backward sync now persists only the newly extended floor range instead of rewriting the full checkpoint-to-floor anchor chain on every batch.
- Raised CL history range concurrency from `4` to `8` after smoke runs showed range-capable peers were available but underused.
- Hardened peer retention by ignoring wrong-peer-ID dial targets for the current run instead of repeatedly backoff-retrying stale discovery records.
- Updated the dashboard history target to first PoS execution block `15537394` and added a remaining-time estimate for long CL history sync.
- Validation run: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, release build, browser UI check, and the fixed-port CL P2P smoke.

## Remaining TODOs

1. Checkpoint Distribution And Weak Subjectivity Precision
   - TODO:
     - replace the temporary checkpoint-sync URL dependency with a LogEx-owned recent-checkpoint distribution endpoint or documented multi-source checkpoint verification flow
     - replace the temporary fixed weak-subjectivity freshness window with the exact consensus-spec weak-subjectivity-period calculation once sufficient state and validator-set churn data are locally available
     - keep the root-only checkpoint path honest, and require `slot@root` only if live root-only bootstrapping remains unreliable in longer proving runs
   - Why this matters:
     - the current branch can use a Lighthouse-style trusted startup endpoint, but the final operator experience should not rely on a third-party checkpoint source or a conservative fixed freshness bound forever
   - Done when:
     - a fresh mainnet sync can obtain or validate a recent checkpoint through LogEx-owned or independently cross-checked sources, and stale checkpoint rejection uses the exact spec-derived weak-subjectivity period

2. Long-Lived Beacon History Proving
   - TODO:
     - keep the current fixed-port release smoke running so the backward parent-root walk can continue from the recent checkpoint toward the first PoS execution block
     - record a longer checkpoint-to-Merge soak result once the run has covered enough history to be meaningful
     - continue tuning CL peer retention only if the soak shows request slots are underfilled or useful peers churn faster than discovery can replace them
   - Why this is still blocking:
     - restart/resume and short sustained smokes now show both directions moving with continuous anchors, but the full checkpoint-to-Merge expansion is still a long-lived network soak
   - Done when:
     - a fresh checkpoint-centered run keeps live head coverage current while steadily expanding historical coverage toward the Merge across restarts

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
     - create current operator docs that align with the actual guarantees once the native CL path is complete
   - Done when:
      - the node can sync, restart, query, and serve logs from the final CL-driven architecture with measured performance and truthful documentation

## Challenges and Resolutions

- Challenge: The previously recorded April checkpoint no longer bootstrapped reliably on live peers.
  - Resolution: Added startup checkpoint freshness enforcement and `--checkpoint-sync-url` so fresh runs can use a current finalized checkpoint from a trusted checkpoint-sync endpoint.
  - Remaining: LogEx still needs its own checkpoint distribution story for operators who do not want to trust an external startup endpoint.
- Challenge: History peers could be rewarded for responses that did not advance verified beacon-history materialization.
  - Resolution: Range requests now retain their requested slot windows, out-of-range blocks are rejected as peer faults, and empty/undecodable history responses are counted as failures.
- Challenge: Public mainnet discovery still returns many peers that either lack beacon req/resp support or close during useful history/light-client RPCs.
  - Resolution: Required `eth2` ENR fork metadata for discovery relevance, widened status/history concurrency, and disconnected idle peers once useful RPC failures put them into cooldown so new candidates can use the slot budget.
  - Remaining: Long-lived soak runs are still needed before release, but fresh fixed-port smokes now bootstrap from scratch and keep materialized history at the verified optimistic head.
- Challenge: Older anchor stores did not persist the parent beacon root for the oldest materialized anchor, which could pin the backward walk at that point.
  - Resolution: Persist parent beacon roots for new anchors and refetch the oldest legacy root when the cached parent is unknown, allowing the authentic parent link to be recovered.
- Challenge: A moving floor/ceiling alone could hide continuity bugs in the materialized anchor range.
  - Resolution: Added consensus-store anchor coverage and a status-visible gap count that treats skipped execution block numbers or broken beacon-parent links as continuity failures.
- Challenge: Historical CL sync slowed as the verified backward chain grew.
  - Resolution: Persist only the new floor extension during backward materialization and widen history request concurrency to use more range-capable peers when available.
- Challenge: Public discovery returned endpoints whose actual libp2p identity did not match the ENR-derived peer.
  - Resolution: Treat `WrongPeerId` and local-peer dial errors as unusable for the current run so the dialer can rotate to better candidates.

## Dead Code and Obsolescence Cleanup

- Inspected the touched CL materialization path, scheduler concurrency constants, UI history labels, and stale history-target wording.
- No obsolete runtime modules were removed; the removed UI wording was only superseded display text.
- The remaining transitional consensus paths are still tracked in the TODO list.

## Git Workflow

- Current branch: `perf/cl-history-sync-eta`.
- Task branch created: `perf/cl-history-sync-eta`.
- Commits made during this run: pending.
- Pull request status: pending local commit/push after validation.
- Merge status: pending PR checks.
- Git/GitHub blockers: `gh` CLI authentication is invalid, so GitHub connector APIs are being used for PR operations.

## Known Issues or Risks

- The current weak-subjectivity freshness guard uses a conservative fixed mainnet window rather than computing the exact state-derived consensus-spec weak-subjectivity period.
- `--checkpoint-sync-url` is a temporary startup bootstrap aid and introduces trust in the selected endpoint for initial checkpoint selection until LogEx provides its own checkpoint source.
- Fresh fixed-port smokes now show both forward and backward CL materialization moving with continuous anchors, but the checkpoint-to-Merge soak should keep running in the background until it reaches the Merge boundary or exposes a new peer-retention failure.
- Keep the HTTP port constant for comparable smoke tests; stop any stale process before rerunning instead of incrementing the test port.
- Pre-Merge PoW canonicality remains unimplemented, so LogEx cannot yet claim full-chain canonicality.

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
