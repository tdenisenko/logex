# Roadmap

## Current Status

LogEx bootstraps from a recent weak-subjectivity checkpoint, follows CL head/finality over native CL P2P, and uses CL-authenticated execution anchors as the pivot for EL validation. EL P2P can follow head, fetch historical headers/bodies/receipts backward from the pivot, verify receipt roots without executing the EVM, and index queryable logs while the stored range expands toward genesis.

Latest fixed-port remote smoke on May 8, 2026 used `/root/logex-data-remote` and HTTP port `18683`. After restarting with the current branch, `/status` reported historical target `0`, historical floor `24,045,692`, `54` connected EL peers, `51` serving EL peers, and restart-wide historical sync at `64.11 blocks/sec`. Warmed batch logs showed 768-1024 verified blocks per batch, usually around 5-12 seconds once peers were serving, with the historical prefetch queue occasionally reaching depth 2. Peer count is usable; the main bottleneck is still body/receipt window scheduling plus receipt validation/storage.

## Completed Since Last Run

- Added a bounded historical prefetch queue so batch `N+1` and sometimes `N+2` can be fetched while batch `N` is validated and written.
- Lowered the combined body/receipt early-return floor from 1024 to 768 contiguous blocks to avoid waiting on slow tail chunks when the lower part of the reverse window is already usable.
- Rechecked the hot downloader path against Geth's downloader model: the relevant pattern is capacity-scored task assignment with timeout reassignment; LogEx now has peer scoring and request backoff, but still needs deeper multi-window scheduling.

## Remaining TODOs

1. Improve EL reverse-sync throughput.
   - Reason: Current warmed batches can exceed the restart-wide average, but total throughput remains far below the roughly `1,160 blocks/sec` needed for a sub-6-hour genesis backfill.
   - Completion criteria: Mainnet-like reverse sync sustains sub-6-hour full-history ETA, or an explicit architecture decision replaces full P2P receipt backfill with a faster trustless strategy.

2. Implement deeper EL historical scheduling.
   - Reason: The downloader still fetches only one historical body/receipt window at a time; bounded prefetch can only overlap network work with local validation/storage, not keep many independent body/receipt windows in flight.
   - Completion criteria: Header lookahead, body/receipt task assignment, validation, and ordered historical writes are separated enough to keep multiple verified windows active without advancing the historical floor past gaps.

3. Complete pre-Merge PoW canonicality validation.
   - Reason: A CL pivot authenticates the recent execution anchor, but pre-Merge headers still need execution-layer canonicality checks down to genesis.
   - Completion criteria: Parent links, difficulty rules, and terminal total difficulty are validated before pre-Merge logs are treated as fully canonical.

4. Harden restart, reorg, and peer behavior.
   - Reason: Long-running correctness depends on stable resume, honest serving-range advertisement, and safe handling of normal mainnet churn.
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
- Productive peers are prioritized in LogEx's own queue instead of being promoted to trusted/static peers; this matches how Geth and Nethermind treat operator-configured trusted peers.
- Historical prefetch is retained only after the current batch validates and writes, preserving ordered floor advancement.
- Combined body/receipt early return favors contiguous lower-window progress over waiting for every slow tail chunk.

## Challenges and Resolutions

- Challenge: Earlier peer-retention work improved connected and serving peers, but ETA stayed multi-day.
  - Resolution: Remote samples showed body/receipt fetch latency, validation, and storage are the real limit after peers warm up.
  - Remaining: Deeper multi-window scheduling is still needed.

- Challenge: Current-batch local work previously blocked the next network fetch.
  - Resolution: Added bounded prefetch overlap and stale-prefetch invalidation.
  - Remaining: The peer manager still serializes historical body/receipt windows at the engine level.

- Challenge: Large reverse windows can stall on a few slow chunks.
  - Resolution: Allow usable contiguous progress once at least 768 lower-window blocks are available.
  - Remaining: The threshold needs longer-run validation across different block-density regions.

## Dead Code and Obsolescence Cleanup

- Inspected the historical prefetch path, combined body/receipt downloader, peer request scoring, and roadmap notes.
- Replaced the obsolete single-prefetch `Option` state with a bounded queue.
- No new debug-only code or temporary peer-retention experiments were left in the branch.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: no
- Pull request status: draft PR #76 (`https://github.com/tdenisenko/logex/pull/76`)
- Merge status: not ready; throughput target and pre-Merge validation are still incomplete.
- Git/GitHub blockers: local `gh` auth is unavailable, but the GitHub connector can update the existing draft PR.

## Known Issues or Risks

- Reverse sync remains too slow for the target ETA.
- Full-history receipt/log acquisition over public EL P2P may not realistically match snap-sync full-node timings unless the downloader is redesigned around deeper task queues or another trustless data source.
- Pre-Merge PoW validation is still incomplete.
- Local shell access to `127.0.0.1:18683` can require elevated local-network permission in this environment; the client itself should keep using the fixed HTTP port.
