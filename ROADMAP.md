# Roadmap

## Current Status

LogEx bootstraps from a recent weak-subjectivity checkpoint, follows CL head/finality over native CL P2P, and uses CL-authenticated execution anchors as the pivot for EL validation. EL P2P can follow head, fetch historical headers/bodies/receipts backward from the pivot, verify receipt roots without executing the EVM, and index queryable logs while the stored range expands toward genesis.

The current branch is focused on EL reverse-sync throughput and peer behavior. The latest remote runs on May 8, 2026 use `/root/logex-data-remote` and fixed HTTP port `18683`. The best bounded sample so far reached about `144` historical blocks/sec after distributing receipt requests across rotated peers instead of concentrating them on the header peer. This is better than the earlier `57-122` blocks/sec runs, but still far from the sub-6-hour target. A corrupt remote data dir from an overlapping restart was cleared and the current remote run is fresh.

## Completed Since Last Run

- Split historical reverse sync into fetch, validation, and chunked write stages so network fetch can overlap local validation/storage work.
- Added bounded multi-window historical fetch lookahead and ordered floor advancement.
- Changed body/receipt batches to ingest a verified contiguous prefix once it reaches 1024 blocks, then resume from the new floor instead of waiting for slow tail chunks.
- Bounded body/receipt chunk fanout around the contiguous prefix so partial-window ingest does not waste most of the peer pool on cancelable tail requests.
- Aligned historical fetch windows to the 1024-block accepted prefix so lookahead work does not target ranges that become stale after ordered floor advancement.
- Reduced historical write memory pressure by extracting and writing validated logs in 512-block chunks.
- Lowered storage zstd level for faster continuous log compaction while keeping the existing topic dictionary encoding and query limits.
- Improved EL peer ramp behavior with a larger sync peer target, more outbound dial capacity, and temporary demotion/backoff for unresponsive dial candidates instead of deleting persisted productive peers.
- Increased the remote test dial ceiling again after the 1024-prefix run showed network headroom but only 17 serving peers.
- Reused successful receipt responses across body-peer retries inside a body/receipt chunk so a failed body peer does not force duplicate receipt downloads for the same hashes.
- Added a historical tail result cache so completed out-of-order body/receipt chunks below the accepted prefix can be queued for validation instead of being refetched after floor advancement.
- Increased body/receipt request units to 64 blocks and moved historical row extraction into the blocking storage worker so network prefetch can start without being blocked by synchronous log extraction.
- Measured and rejected 4096-block reverse windows and deferred historical compaction because they increased memory or reduced throughput in remote runs.
- Removed per-chunk receipt preference for the header peer so combined body/receipt chunks distribute first receipt requests across the rotated serving-peer set.
- Hardened historical sealed-segment writes against abandoned segment directories left behind by interrupted writes.

## Remaining TODOs

1. Reduce EL reverse-sync ETA below the production target.
   - Reason: The current downloader still does not sustain the roughly `1,000+ blocks/sec` needed for sub-6-hour full-history backfill.
   - Completion criteria: Mainnet-like reverse sync sustains sub-6-hour ETA on adequate hardware, or a documented architecture decision replaces full P2P receipt backfill with another trustless strategy.

2. Implement deeper EL historical scheduling.
   - Reason: The current lookahead overlaps whole windows, but slow body/receipt chunks can still dominate batch time.
   - Completion criteria: Header lookahead, body/receipt chunk scheduling, validation, and ordered historical writes are separated enough to keep many independent chunks in flight without advancing past gaps.

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
- Historical floor advancement only uses contiguous verified blocks. A partial body/receipt window may be ingested once the contiguous prefix reaches 1024 blocks; already completed tail chunks are queued for ordered validation instead of discarded.
- Reverse-sync windows are capped at 2048 blocks for now because 4096-block windows caused excessive memory pressure for only a small throughput gain during dense log ranges.
- Unresponsive dial candidates receive temporary in-memory backoff and productive-queue demotion, not deletion from the persisted known-peer set.
- Outbound dial capacity is intentionally higher than a general-purpose full node because LogEx is a sync-focused reader and needs to rebuild a large serving peer pool quickly after restart.
- Query limits remain capped at `10,000` rows with `50` row default pages; storage keeps dictionary/topic compression and periodic compaction.

## Challenges and Resolutions

- Challenge: A 4096-block reverse window improved request amortization but caused remote memory pressure.
  - Resolution: Capped windows at 2048 blocks and moved log extraction/writes into 512-block chunks.

- Challenge: The earlier stale-dial cleanup hurt peer ramp by deleting peers from the persisted productive set.
  - Resolution: Replaced deletion with temporary backoff plus queue demotion and added a regression test.

- Challenge: Peer count can be high while ETA remains multi-day.
  - Resolution: Storage and validation are now overlapped with fetches, and contiguous-prefix ingest reduces slow-tail stalls.
  - Remaining: The downloader still needs longer-run validation and deeper task queues to reach the target ETA.

- Challenge: Larger receipt chunks looked attractive compared with Geth/Nethermind limits but regressed the remote run.
  - Resolution: Reverted the larger receipt/gas chunk tuning and kept the smaller dense-block chunks.

- Challenge: The chunk pipeline could discard a successful receipt response when the paired body peer failed.
  - Resolution: Cached that receipt response for the next body retry and still validates it before ingestion.

- Challenge: Re-enabling larger reverse windows previously wasted tail work after the first accepted prefix.
  - Resolution: Added tail-batch reuse before restoring 2048-block windows.

- Challenge: Historical log extraction ran synchronously inside the async write future before network prefetch could make progress.
  - Resolution: Moved extraction into the blocking storage worker; the best remote sample improved to about `122` historical blocks/sec.

- Challenge: Deferring historical compaction to the background hot-segment path looked useful for sync-path CPU, but it regressed throughput by competing with sync work.
  - Resolution: Reverted that experiment and kept inline compacted historical writes for now.

- Challenge: Receipt requests were still concentrated on the header peer after per-chunk rotation, limiting the value of a larger peer pool.
  - Resolution: Removed the header-peer preference from combined chunk receipt selection; the best bounded sample improved to about `144` historical blocks/sec.

- Challenge: An overlapping remote restart exposed a storage durability issue where an abandoned segment directory could be reused with stale column files.
  - Resolution: Historical sealed-segment writes now remove any abandoned directory before writing a newly allocated segment. The corrupt remote data dir was deleted after preserving peer/discovery files.

## Dead Code and Obsolescence Cleanup

- Inspected the historical downloader, peer lifecycle/state, storage compression, sealed-segment writes, and roadmap notes for obsolete experimental code.
- Removed the obsolete single-prefetch path in favor of the bounded fetch/prepare pipeline.
- Reverted the 1024-block historical write chunk experiment after it caused excessive memory pressure on the remote runner.
- Confirmed no debug prints, early-return roadmap behavior, or known-peer deletion experiment remains in the Rust code.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: no
- Pull request status: draft PR #76 (`https://github.com/tdenisenko/logex/pull/76`)
- Merge status: not ready; throughput target and pre-Merge validation are still incomplete.
- Git/GitHub blockers: none for committing and pushing; PR remains draft because the task is still incomplete.

## Known Issues or Risks

- Reverse sync remains too slow for the target ETA.
- Full-history receipt/log acquisition over public EL P2P may not realistically match snap-sync full-node timings unless the downloader is redesigned around deeper task queues or another trustless data source.
- The current remote run needs time to rebuild a healthy persisted known-peer set after the previous bad build reduced it.
- Pre-Merge PoW validation is still incomplete.
