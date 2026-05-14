# Roadmap

## Current Status

LogEx boots from a recent weak-subjectivity checkpoint, follows Consensus Layer head/finality over native P2P, and uses authenticated execution anchors as the pivot for Execution Layer validation. Execution Layer P2P can follow head, walk historical execution data backward from the pivot, verify headers/bodies/receipt roots without executing the EVM, and index queryable logs while the stored range expands toward genesis.

The active task branch is `feature/el-reverse-sync` / draft PR #76. The dashboard cleanup from PR #77 has been merged into this branch. The remote performance run is using one active data directory with older segment directories relocated onto mounted extra volumes through symlinks.

Current remote testing is on the upgraded 8-vCPU/16GB host. The current run retained roughly 100 connected peers and 80+ serving peers, so peer retention is no longer the active limiter. The latest slowdown correlated with dense historical log writes plus background compaction and blocking status storage scans; active-sync compaction now runs in smaller raw-only passes and dashboard storage metrics refresh asynchronously with segment-size caching.

## Completed Since Last Run

- Formatted and mounted the new 100GB remote volume at `/mnt/logex-extra/extra2`, then moved older sealed segment directories onto it and replaced them with symlinks so the active data directory could continue running.
- Rebalanced about 8 GiB of relocated segments from `/mnt/logex-extra/extra2` to `/mnt/logex-extra` so both extra filesystems stay above the 10 GiB safety floor.
- Added a runtime low-disk guard that polls the data-dir filesystem plus relocated segment symlink target filesystems and triggers the existing graceful shutdown path when any monitored filesystem drops below 10 GiB free.
- Deployed the low-disk guard to the remote host and restarted the performance run on the fixed HTTP port `18683`.
- Investigated the CPU/block-rate spike during the remote run and traced the drop to local write stalls, not peer loss.
- Reduced active-sync background compaction pressure by delaying profile rewrites while raw sealed segments still need first-time compaction.
- Smoothed active-sync maintenance further by limiting raw compaction to smaller per-pass batches and reserving catch-up bursts for larger raw backlogs.
- Changed dashboard storage metrics to return cached values immediately and refresh the expensive filesystem scan in a single background task at a lower cadence, preventing `/status` polling from blocking the UI or repeatedly walking the large catalog.
- Added segment-aware storage-size caching so old sealed segments are not recursively rescanned unless their manifest changes; the active hot segment and new segments are still measured exactly.
- Fixed shutdown polling so SIGINT/SIGTERM remains visible after the first async watcher observes it, preventing historical sync from continuing after graceful shutdown starts.

## Remaining TODOs

1. Reduce Execution Layer reverse-sync ETA below the production target.
   - Reason: The latest warmed remote sample reached the sub-6-hour target, but it still needs longer-run and fresh-run confirmation.
   - Completion criteria: A fresh mainnet-like run sustains sub-6-hour ETA on adequate hardware without low-memory fallback, storage exhaustion, or peer-pool collapse, or a documented architecture decision replaces full P2P receipt backfill with another trustless strategy.

2. Stabilize Execution Layer peer ramp and body/receipt throughput.
   - Reason: The downloader needs enough serving peers to hide request latency and keep wide fetch windows active.
   - Completion criteria: Long remote runs retain a large serving pool, keep lookahead filled, and do not regress peer retention compared with the best observed run.

3. Replace the temporary checkpoint source and stale-checkpoint policy.
   - Reason: `--checkpoint-sync-url` still depends on an external checkpoint provider, and weak-subjectivity safety depends on starting from a recent, correct checkpoint.
   - Completion criteria: LogEx has its own recent-checkpoint source or a documented multi-source verification flow, rejects stale checkpoints according to consensus weak-subjectivity rules, and clearly reports when a fresh checkpointed resync is required.

4. Complete adversarial release validation.
   - Reason: LogEx's trustless log-validity claim depends on correct verification and rejection behavior across Consensus Layer, Execution Layer, storage, query, and UI surfaces.
   - Completion criteria: End-to-end tests or smokes cover checkpoint bootstrap, live anchors, reverse Execution Layer headers/bodies/receipts, invalid headers/bodies/receipts/receipt roots from peers, peer starvation/eclipsing liveness behavior, restart/resume, reorgs, Merge boundary and pre-Merge ancestry from a CL-authenticated pivot, query coverage, limits, pagination, and dashboard auth behavior.

5. Harden exposed query and admin surfaces before public exposure.
   - Reason: HTTP Basic auth protects dashboard/status/query/JSON-RPC/WebSocket routes but does not encrypt traffic, and gRPC remains a separate unauthenticated listener.
   - Completion criteria: Public deployments bind sensitive listeners to trusted interfaces or require firewalling/TLS/SSH tunneling; gRPC is disabled, trusted-network-only, or authenticated equivalently; deployment docs make the exposure model explicit.

6. Complete verification-critical security review.
   - Reason: A trustless log-validity claim depends on correct implementation of CL anchor handling, EL ancestry validation, body/receipt verification, storage canonicality, reorg handling, and query bounds.
   - Completion criteria: A documented security review or audit covers verification-critical code paths and threat model assumptions, and all critical/high findings are fixed or explicitly documented before a production-ready release.

## Design Decisions

- Consensus Layer sync is forward-only from a recent checkpoint; Execution Layer historical sync is responsible for walking execution data back toward genesis.
- Logs are valid only inside the verified contiguous stored range. Unsynced historical gaps remain outside query coverage.
- The dashboard keeps the query tool on the main page because querying verified logs is a primary product workflow.
- Performance charts use Chart.js rather than custom SVG path generation.
- Dashboard authentication uses HTTP Basic auth as a lightweight local/server operator control. It should be paired with localhost binding, firewalling, SSH tunneling, or TLS termination when exposed outside a trusted machine.
- Query responses keep a hard `10,000` row cap and default to `50` row pages.
- Dashboard query pagination is client-side over the loaded capped result set, so Next/Previous does not issue additional query requests.
- Storage usage metrics follow relocated segment-directory symlinks because the active deployment may span more than one mounted filesystem.
- Medium-peer historical reverse sync keeps four 2048-block body/receipt fetches queued once at least 8 serving peers are available. High-memory runs with at least 12 connected/serving peers use 4096-block batches at depth three, which keeps a similar in-flight block footprint while reducing scheduling overhead. Available-memory guards reduce both depth and window size before the process risks OOM. An eight-deep trial was rejected because it raised RSS to about 10 GiB without a meaningful throughput gain.
- Peer dialing prefers known productive peers but reserves roughly one third of each refill for fresh discovery candidates, because persisted peer caches can become stale after restarts or host replacement.
- Startup storage integrity verification remains full verification, but segment checks run across a bounded worker pool so large catalogs do not block HTTP readiness on one thread.
- Startup integrity checks verify canonical bitmap length from the bitmap header and file size instead of rereading every canonical row bit. Full canonical bitmap reads remain available for query/reorg paths.
- Cached Consensus Layer beacon blocks maintain a parent-child index because the forward-only CL path repeatedly walks checkpoint-to-head lineage.
- Receipt-root validation keeps the existing trust model but uses the assembly Keccak backend where supported, because hashing is on the critical path for every verified receipt trie.
- Consensus history range progress tracks the highest cached forward slot directly instead of constructing a temporary chain vector.
- Active-sync compaction is treated as best-effort under memory pressure. Verified ingestion remains the priority, and compaction catches up when available memory recovers.
- During active historical sync, background compaction favors raw segment compaction and defers profile-only rewrites while raw backlog exists. Raw compaction protects disk usage; profile migration is an optimization that can catch up after ingestion pressure drops.
- Dashboard storage metrics are eventually consistent: `/status` returns the last cached storage sample and starts one background refresh when the one-minute sample expires. Segment-size accounting caches sealed segment directory sizes by filesystem identity and manifest metadata, while always rescanning the active hot segment and newly discovered segment directories.
- Shutdown checks read the current watch value instead of the unread-change flag, because several async wait paths can consume the change notification before later engine loops poll for shutdown.
- Historical `block_number` columns use signed delta encoding because reverse sync can naturally produce descending or mixed block-number deltas before rows are normalized for storage. Active compaction can rewrite only the legacy block-number column while preserving the rest of the segment, which keeps the migration crash-safe and much cheaper than full segment rewrites.
- The node treats low data-dir or relocated-segment free space as a controlled shutdown condition instead of allowing storage writes to retry into `ENOSPC`. The guard uses the same engine/network shutdown path as SIGINT/SIGTERM so verified in-flight writes can drain before process exit.
- LogEx does not re-run historical PoW fork choice from genesis. A CL-authenticated post-Merge execution header commits to one historical ancestry through parent hashes, so historical validation verifies header linkage, body commitments, and receipt roots against that ancestry.

## Challenges and Resolutions

- Challenge: The dashboard had too many competing metrics and made Execution Layer/log coverage hard to interpret.
  - Resolution: The main view now shows one Execution Layer progress bar, Consensus Layer status, log range, storage, performance chart, and the query panel.

- Challenge: The query engine could be abused if the HTTP server URL is reachable by untrusted users.
  - Resolution: Added optional HTTP Basic auth for HTTP dashboard, status, query, JSON-RPC, and WebSocket endpoints while keeping `/health` public for liveness checks.

- Challenge: The remote root filesystem was close to full while the active data directory still needed to be preserved for performance testing.
  - Resolution: Mounted the additional volume, moved older sealed segments onto it, fixed symlink-aware catalog repair and storage metrics, and reduced compacted selected-row reads so startup does not scan entire column files unnecessarily.

- Challenge: Historical sync still had visible wait time between body/receipt fetches and local processing.
  - Resolution: Raised medium-peer lookahead to four queued fetches after comparing remote batch logs; the run remains CPU-bound rather than peer- or IO-bound.

- Challenge: Restarting with billions of stored rows spent too long rereading canonical bitmaps during integrity checks.
  - Resolution: Integrity checks now verify canonical bitmap length without materializing the full bitmap, reducing remote HTTP-ready time from about 100 seconds to 58 seconds on the active data directory.

- Challenge: Perf samples still showed Consensus Layer lineage selection scanning the full cached block set.
  - Resolution: Added a child index keyed by parent root so forward-chain walking only inspects direct children.

- Challenge: After storage and lineage optimizations, remote perf samples showed receipt-root Keccak hashing as the dominant CPU cost.
  - Resolution: Enabled Alloy's `asm-keccak` feature and validated the sync crate plus full clippy before remote measurement.

- Challenge: Perf samples still showed Consensus Layer range readiness spending CPU on temporary lineage vectors.
  - Resolution: Reworked forward progress selection to walk cached children without allocating a chain.

- Challenge: Warm remote runs still remain above the sub-6-hour target after peer retention and pipeline overlap improvements.
  - Resolution: Profiling now points to receipt-trie hashing, log extraction/storage, and 8GB memory pressure as the remaining limit on the current host; IO delay is not material.

- Challenge: A later remote run dropped from hundreds of blocks/sec to about 43 blocks/sec despite 50 connected peers and 39 serving peers.
  - Resolution: Bounded status, log, and `pidstat` samples showed the stalled batches were dominated by storage/write waits and memory pressure, not peer retention. The remote client was stopped cleanly for a CPU/RAM upgrade.

- Challenge: The upgraded host had enough CPU/RAM headroom but still showed ETA swings when lookahead expanded too early or peer service was thin.
  - Resolution: Historical lookahead now depends on total memory, available memory, and serving-peer count. The branch keeps conservative windows for low-peer or low-memory runs and only enables deeper queues on the upgraded host class.

- Challenge: A trial dial retry cooldown added complexity without a clear peer-retention gain.
  - Resolution: The cooldown was removed before committing; the branch keeps the previously proven peer connection policy.

- Challenge: Body/receipt batches were over-counting safe parallelism because each scheduled range can issue both a body request and a receipt request.
  - Resolution: The paired pipeline now halves the chunk window derived from request concurrency, reducing timeout pressure while retaining overlap.

- Challenge: The 2048-block/depth-6 high-memory path stabilized near 500-550 historical blocks/sec, still above the six-hour target.
  - Resolution: Switched the high-memory path to 4096-block batches at depth three and kept 32-block body/receipt chunks so a full 2048-block prefix can fit within the paired request cap. The warmed remote run reached about 718 historical blocks/sec with stable memory headroom.

- Challenge: Dense historical segments were spending several MiB per compacted `block_number` column because reverse-order rows defeated the unsigned delta codec.
  - Resolution: Historical rows are now extracted in ascending block order, `block_number` compaction uses signed deltas, and active compaction can migrate old compacted block-number columns without rewriting every log column.

- Challenge: Restart warm-up repeatedly stalled below the high-throughput peer threshold, leaving the downloader in 1024-block mode despite enough peers for more work.
  - Resolution: Lowered medium/high historical window thresholds for high-memory hosts, kept low-memory guards intact, and reserved part of each dial refill for fresh discovery candidates. The current remote sample reached about 809 historical blocks/sec and a 3.8-hour ETA with 13 serving peers.

- Challenge: Large data directories made startup availability sensitive to a single-threaded segment integrity scan.
  - Resolution: Segment integrity verification now runs in a bounded worker pool while preserving the same row-count, canonical bitmap, and block-boundary checks.

- Challenge: The remote root filesystem reached 100% usage and the client could not continue writing safely.
  - Resolution: Mounted the new volume, relocated old sealed segment directories onto it, rebalanced the extra volumes above the safety floor, deployed a symlink-aware low-disk shutdown guard, and restarted the client after confirming free space had recovered.

- Challenge: A later run briefly dropped block/sec despite 100+ connected peers and 80+ serving peers.
  - Resolution: `pidstat`, `iostat`, status, and batch logs showed root-volume write stalls during dense historical writes and background compaction. Active-sync compaction now skips profile rewrites until raw compaction backlog clears and waits longer before using the raw catch-up limit.

- Challenge: Block/sec ramped up, then dropped sharply while CPU spiked and the dashboard became intermittently unresponsive.
  - Resolution: Remote logs showed the drops lining up with local maintenance, especially raw compaction overlapping dense writes, and `/status` could block on full data-dir storage scans. Active-sync raw compaction now uses smaller smoothing passes, and storage metrics refresh is nonblocking, single-flight, lower-cadence, and segment-cache aware.

- Challenge: A remote SIGINT logged graceful shutdown but historical backfill kept completing batches.
  - Resolution: Shutdown polling no longer depends on `watch::Receiver::has_changed()`, which becomes false after another wait path observes the signal. Historical prefetch cancellation also checks the sticky shutdown value before falling back to sequential fetch.

## Dead Code and Obsolescence Cleanup

- Inspected the EL peer manager, historical lookahead scheduler, node shutdown path, background compaction loop, storage metrics, and dashboard sync display.
- Removed the experimental peer dial cooldown after remote testing did not show a clear benefit.
- Reverted the eight-deep high-memory lookahead trial after remote RSS and throughput samples showed the extra memory was not justified.
- Kept the memory-aware lookahead, compaction guards, paired request accounting, and bounded chunk hedging because they directly address observed slowdown/OOM or timeout risks.
- Reverted an outbound-heavy peer split trial because it under-filled the peer pool compared with the established split.
- Removed the 16-block wide-peer body/receipt chunk cap after it forced extra request waves under the paired in-flight cap.
- Replaced the old unsigned `block_number` compaction profile for new compactions with signed delta encoding; existing compacted segments remain readable through their manifest-declared codec.
- Added a targeted legacy block-number profile rewrite path instead of using the existing full-row recompact path for this migration.
- Inspected the latest dial selection changes and added tests for productive-peer ordering, fresh-candidate reservation, and full-budget use when all candidates are productive.
- No new obsolete EL sync paths were found in the changed areas; the current write coalescing and threshold tuning replaced runtime constants rather than leaving alternate code paths.
- Local `/private/tmp/geth-src` and `/private/tmp/nethermind-src` currently contain directory skeletons without source files, so peer-policy comparison used the vendored Reth networking source available in Cargo checkouts.
- No obsolete low-disk loop or retry code was found in the node runtime; the new guard was added at the top-level runtime select so existing shutdown code remains the single stop path.
- Extended the low-disk scan to discover relocated segment symlink targets instead of leaving the emergency multi-volume layout unmonitored.
- Inspected the background compaction loop after the write-stall investigation; removed the active-sync profile rewrite catch-up path because it competes with historical ingest and can safely run after raw compaction pressure clears.
- Inspected the status storage-metrics path and active-sync compaction loop after the spike/drop report; no experimental code paths remained, but the blocking status refresh and oversized active raw compaction pass were replaced. The follow-up storage metric cache avoids recursively reading unchanged sealed segment directories.
- Inspected the shutdown and historical prefetch paths after the remote stop hang; no duplicate shutdown path was added, and the existing graceful stop path now uses a sticky shutdown signal.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: none; continuing the existing Execution Layer reverse-sync branch.
- Commits made during this run: `perf: tune historical body receipt windows`; `perf: compress historical block numbers`; `perf: migrate legacy columns during catchup`; `perf: improve historical warmup throughput`; `docs: record latest historical sync run`; `fix: stop sync gracefully on low disk`; `fix: monitor relocated segment disk space`; `docs: clarify production security todos`; `perf: throttle active sync compaction`; `perf: smooth active sync maintenance`; `perf: cache sealed segment sizes`; `fix: keep shutdown signal sticky`.
- Pull request status: draft PR #76 remains open for the Execution Layer production-readiness work.
- Merge status: not ready to merge; Execution Layer throughput and full-history validation remain incomplete.
- Git/GitHub blockers: none known.

## Known Issues or Risks

- HTTP Basic auth does not encrypt traffic. Use it behind localhost, a firewall, an SSH tunnel, or a TLS-terminating reverse proxy.
- gRPC remains unauthenticated and should not be exposed to untrusted networks until it is separately hardened or disabled.
- The parent Execution Layer performance branch is still above the long-term sync ETA target on the current observed runs.
- The 8-vCPU/16GB remote host has headroom, but throughput is still sensitive to useful body/receipt peer supply and dense-log local processing.
- Symlinked segment directories are a deployment compatibility path, not a replacement for a first-class multi-volume storage allocator.
- Relocated segment symlinks keep the current deployment running but still require manual rebalancing until a first-class multi-volume storage allocator exists.
- The current remote run remains below the six-hour target overall, but dense-log ranges still need observation after the maintenance smoothing change to confirm ETA no longer swings sharply.
- Restart startup still scans all segment manifests and compacted block-number page indexes; this is improved but not yet a first-class large-catalog index.
- Verification-critical security review is still required before calling the implementation production-secure.
