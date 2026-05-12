# Roadmap

## Current Status

LogEx bootstraps from a recent weak-subjectivity checkpoint, follows CL head/finality over native CL P2P, and uses CL-authenticated execution anchors as the pivot for EL validation. EL P2P can follow head, fetch historical headers/bodies/receipts backward from the pivot, verify receipt roots without executing the EVM, and index queryable logs while the stored range expands toward genesis.

The current branch is focused on EL reverse-sync throughput and peer behavior. The latest remote runs on May 12, 2026 use `/root/logex-data-remote` and fixed HTTP port `18683`. Historical backfill now overlaps body/receipt fetching, validation, extraction, and storage writes, starts earlier on fresh peer pools, and keeps decoded receipt/log memory bounded. Dense recent ranges still remain below the sub-6-hour full-history target; after peer retention improved, the current bottleneck is memory-safe body/receipt batch throughput and local processing overlap.

## Completed Since Last Run

- Overlapped historical fetch, validation/extraction, and storage writes with bounded ordered lookahead.
- Changed historical body/receipt fetches to ingest verified contiguous prefixes and reset stale lookahead when a peer only returns part of the planned prefix.
- Fused historical validation and log extraction for the pipelined path so decoded bodies/receipts are dropped sooner; remote RSS fell from roughly 5.4 GB to roughly 3.4 GB in comparable dense-range samples.
- Lowered the historical start gate from 16 to 8 connected EL peers so fresh runs can warm peers by serving real requests sooner.
- Increased body/receipt chunk scheduling from 2 to 4 in-flight chunks per peer while retaining the global 128-chunk cap.
- Coalesced fused validation/extraction output back into 1024-block storage write chunks so worker-sized validation chunks do not create excessive sealed segments.
- Increased body/receipt hedge capacity for prefix-blocking chunks so slow dense-range gaps can be retried across more rotated peers without increasing retained batch size.
- Removed stale-dial quarantine/demotion experiments that hurt fresh peer ramp, retested the earlier Geth-style 33/67 outbound/inbound split, and kept aggressive dial submission because exact pending-dial slot accounting slowed warmup on the fresh remote.
- Rejected 2048-block dense body/receipt batches on the 8 GB remote after an OOM kill; dense batches are capped around 1024 blocks again while sparse windows can still widen.
- Confirmed the current remote only has `/root/logex-data-remote`; no old remote data directories remain to delete.

## Remaining TODOs

1. Reduce EL reverse-sync ETA below the production target.
   - Reason: The current downloader still does not sustain the roughly `1,000+ blocks/sec` needed for sub-6-hour full-history backfill.
   - Completion criteria: Mainnet-like reverse sync sustains sub-6-hour ETA on adequate hardware, or a documented architecture decision replaces full P2P receipt backfill with another trustless strategy.

2. Improve EL P2P fetch throughput and peer ramp.
   - Reason: Dense-range batches are now mostly limited by body/receipt P2P latency, bounded memory, and the ability to keep validation/storage overlapped with fetch.
   - Completion criteria: Long remote runs retain a large serving peer pool, keep memory stable, and keep body/receipt fetch latency from draining the lookahead queue.

3. Complete pre-Merge PoW canonicality validation.
   - Reason: A CL pivot authenticates the recent execution anchor, but pre-Merge headers still need execution-layer canonicality checks down to genesis.
   - Completion criteria: Parent links, difficulty rules, and terminal total difficulty are validated before pre-Merge logs are treated as fully canonical.

4. Harden restart, reorg, and long-run peer behavior.
   - Reason: Production sync depends on stable resume, honest serving-range advertisement, and safe handling of normal mainnet churn.
   - Completion criteria: Long smokes show stable resume, persisted productive peers, no misleading advertised range, and explicit fail-fast behavior for unsupported deep reorgs.

5. Replace the temporary checkpoint source.
   - Reason: `--checkpoint-sync-url` still depends on an external checkpoint provider.
   - Completion criteria: LogEx has its own recent-checkpoint source or a documented multi-source verification flow, with stale checkpoint rejection aligned to consensus weak-subjectivity rules.

6. Release validation.
   - Reason: CL, EL, storage, query, and UI surfaces need shared evidence for what is verified and queryable.
   - Completion criteria: End-to-end fixtures cover checkpoint bootstrap, light-client updates, forward anchors, EL headers, receipts/logs, restart/resume, the Merge boundary, query coverage, limits, and pagination.

## Design Decisions

- EL historical validation targets genesis because the CL checkpoint only proves a recent execution pivot.
- Historical log queries are valid for the verified stored range, not for unsynced gaps below the historical floor.
- Historical floor advancement only uses contiguous verified blocks. A partial body/receipt window may be ingested once the contiguous prefix reaches 1024 blocks; dense body/receipt tail data is not retained across batches.
- Reverse-sync requested windows scale with serving peers and gas density. Dense ranges return about 1024 blocks to control memory on the current architecture, while sparse ranges can return larger batches.
- The peer pool uses a Geth-style 1/3 outbound and 2/3 inbound capacity split, but pending dial submissions are not counted as filled peer slots because that exact behavior slowed fresh warmup in remote testing.
- Historical backfill may begin at 8 connected peers so the client can classify serving peers through real requests instead of idling during fresh peer warmup.
- Outbound dial capacity is intentionally higher than a general-purpose full node because LogEx is a sync-focused reader and needs to rebuild a serving peer pool quickly after restart.
- Query limits remain capped at `10,000` rows with `50` row default pages; storage keeps dictionary/topic compression and periodic compaction.

## Challenges and Resolutions

- Challenge: Dense recent blocks caused memory pressure when decoded receipts/logs were retained across lookahead batches.
  - Resolution: Removed tail retention, bounded fetches to contiguous prefixes, and fused validation with extraction so raw decoded data is dropped sooner.

- Challenge: Fresh remote runs idled because historical backfill waited for too many connected peers before issuing requests.
  - Resolution: Lowered the start gate to 8 connected peers while continuing to refill toward the larger active pool.

- Challenge: Some peer-ramp experiments reduced connections on the fresh remote.
  - Resolution: Removed stale-dial quarantine/demotion and kept only the sync-oriented outbound capacity that showed better ramp behavior on this runner.

- Challenge: 2048-block dense body/receipt batches exceeded the 8 GB remote memory ceiling.
  - Resolution: Restored the dense 1024-block gas cap and kept larger windows only for sparse ranges until historical ingestion can stream chunks without retaining full decoded batches.

- Challenge: Body/receipt fetch latency still drains lookahead in dense ranges.
  - Resolution: Increased per-peer chunk scheduling and bounded hedge retries under the existing global cap; longer remote sampling is still needed.

## Dead Code and Obsolescence Cleanup

- Inspected the historical downloader, peer lifecycle/state, request scheduler, storage compression path, and roadmap notes for obsolete experimental code.
- Removed stale-dial quarantine/demotion code and tests.
- Removed the unsafe dense 2048+ block admission path from the current build.
- Kept the hedge retry change because remote samples showed body/receipt fetch remains the dominant dense-range stage.
- Removed obsolete roadmap notes for rejected tail-cache, deferred-compaction, and stale-dial experiments.
- Confirmed query limits, inline compressed historical writes, and topic dictionary/page compression remain active.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: no
- Pull request status: draft PR #76 (`https://github.com/tdenisenko/logex/pull/76`)
- Merge status: not ready; throughput target and pre-Merge validation are still incomplete.
- Git/GitHub blockers: none for committing and pushing; PR remains draft because the task is still incomplete.

## Known Issues or Risks

- Reverse sync remains too slow for the target ETA.
- Full-history receipt/log acquisition over public EL P2P may not realistically match snap-sync full-node timings unless the downloader is redesigned around deeper task queues or another trustless data source.
- Current remote ETA is still above target; the latest stable build is running again and needs a longer uninterrupted run after the dense-batch OOM fix.
- Larger dense batches require a streaming/chunked ingestion redesign or more memory; simply increasing the batch size is not safe on the current 8 GB runner.
- Pre-Merge PoW validation is still incomplete.
