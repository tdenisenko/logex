# Roadmap

## Current Status

LogEx bootstraps from a recent weak-subjectivity checkpoint, follows CL head/finality over native CL P2P, and uses CL-authenticated execution anchors as the pivot for EL validation. EL P2P can follow head, fetch historical headers/bodies/receipts backward from the pivot, verify receipt roots without executing the EVM, and index queryable logs while the stored range expands toward genesis.

The current branch is focused on EL reverse-sync throughput and peer behavior. The latest remote run on May 8, 2026 uses `/root/logex-data-remote` and fixed HTTP port `18683`. After correcting an over-aggressive stale-dial cleanup that had reduced persisted known peers, the remote restarted with `30` known peers, rebuilt to `17` connected / `14` serving peers within about 90 seconds, and continued syncing. This early sample is not enough to judge final peer retention because the prior bad run had already damaged the persisted known-peer set.

## Completed Since Last Run

- Split historical reverse sync into fetch, validation, and chunked write stages so network fetch can overlap local validation/storage work.
- Added bounded multi-window historical fetch lookahead and ordered floor advancement.
- Changed body/receipt batches to ingest a verified contiguous prefix once it reaches 512 blocks, then resume from the new floor instead of waiting for slow tail chunks.
- Bounded body/receipt chunk fanout around the contiguous prefix so partial-window ingest does not waste most of the peer pool on cancelable tail requests.
- Reduced historical write memory pressure by extracting and writing validated logs in 512-block chunks.
- Lowered storage zstd level for faster continuous log compaction while keeping the existing topic dictionary encoding and query limits.
- Improved EL peer ramp behavior with a larger sync peer target, more outbound dial capacity, and temporary demotion/backoff for unresponsive dial candidates instead of deleting persisted productive peers.

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
- Historical floor advancement only uses contiguous verified blocks. A partial body/receipt window may be ingested once the contiguous prefix reaches 512 blocks; the scheduler keeps extra chunk headroom for gaps but does not eagerly launch the full tail.
- Reverse-sync windows are capped at 2048 blocks for now because 4096-block windows caused excessive memory pressure during dense log ranges.
- Unresponsive dial candidates receive temporary in-memory backoff and productive-queue demotion, not deletion from the persisted known-peer set.
- Query limits remain capped at `10,000` rows with `50` row default pages; storage keeps dictionary/topic compression and periodic compaction.

## Challenges and Resolutions

- Challenge: A 4096-block reverse window improved request amortization but caused remote memory pressure.
  - Resolution: Capped windows at 2048 blocks and moved log extraction/writes into 512-block chunks.

- Challenge: The earlier stale-dial cleanup hurt peer ramp by deleting peers from the persisted productive set.
  - Resolution: Replaced deletion with temporary backoff plus queue demotion and added a regression test.

- Challenge: Peer count can be high while ETA remains multi-day.
  - Resolution: Storage and validation are now overlapped with fetches, and contiguous-prefix ingest reduces slow-tail stalls.
  - Remaining: The downloader still needs longer-run validation and deeper task queues to reach the target ETA.

## Dead Code and Obsolescence Cleanup

- Inspected the historical downloader, peer lifecycle/state, storage compression, and roadmap notes for obsolete experimental code.
- Removed the obsolete single-prefetch path in favor of the bounded fetch/prepare pipeline.
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
