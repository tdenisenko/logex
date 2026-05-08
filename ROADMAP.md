# Roadmap

## Current Status

LogEx now bootstraps from a recent weak-subjectivity checkpoint, tracks CL head/finality forward over native CL P2P, and uses CL-authenticated execution anchors as the pivot for EL P2P validation. The EL path can fetch headers, bodies, and receipts from public execution peers, verify receipt roots without executing the EVM, index recent logs quickly, and expand the verified stored log range backward from the pivot while continuing to follow head.

Fresh fixed-port remote smoke on May 8, 2026:

- Remote data directory: `/root/logex-data-remote`
- HTTP port: `18683`
- `/status` reports `historical_target_block: 0`
- Last sampled status: historical floor `24,185,358`, 49 connected EL peers, 47 serving EL peers, and historical reverse sync at `65.91 blocks/sec` since the latest restart
- Recent warmed batches complete in roughly 5.1-19.1 seconds per 1024 blocks with dynamic body/receipt fanout. Peer retention is no longer the primary blocker; the remaining gap is single-window scheduling plus receipt-root validation/storage time.

## Completed Since Last Run

- Improved historical storage writes with append-oriented column commits, compact WAL payloads, larger hot segments, and deferred background indexing while historical sync is incomplete.
- Added batched historical ingestion and a pipelined body/receipt downloader for reverse sync.
- Tuned EL peer selection with request-rate scoring, productive-peer persistence, Geth-style inbound/outbound capacity, bounded dialing, and `eth/68-69` capability alignment.
- Added a reverse-sync prefetch path so the next historical body/receipt batch can be fetched while the current batch is validated, extracted, and written.
- Capped combined body/receipt chunks to keep each 1024-block window spread across more peers instead of letting adaptive request limits collapse the window into a handful of large requests.
- Reduced historical ingest CPU by extracting log rows directly during validation instead of cloning receipt logs into an intermediate transaction list.
- Removed duplicate non-Osaka pre-execution validation work, chunked historical validation tasks by CPU capacity, and initialized new peer request limits from the warmed pool.
- Added dynamic combined body/receipt chunk sizing: 32-block chunks while the serving pool is small, 16-block chunks once enough peers are available for wider fanout.
- Added a dictionary fast path for repeated fixed-width storage pages so compression can skip unnecessary Zstd candidates when dictionary encoding already wins clearly.
- Kept the effective 1024-block historical window after live testing showed larger header requests are capped by peers.
- Reverted peer-retention and batch-size experiments that did not improve live samples.
- Added storage compression, query limits, pagination support, and UI status corrections for EL historical coverage.
- Fixed clippy issues introduced by the EL work.

## Remaining TODOs

1. Improve EL reverse-sync throughput
   - Reason: Peer retention can reach useful levels, but the latest clean sample is still only `65.91 blocks/sec` against a roughly `1,160 blocks/sec` sub-6-hour target.
   - Completion criteria: Reverse validation sustains roughly `1,160 blocks/sec` or better on mainnet-like data for a sub-6-hour full-history ETA, or a documented architecture decision replaces full P2P receipt backfill with a faster trustless strategy.

2. Complete pre-Merge PoW canonicality validation
   - Reason: A CL pivot proves the recent execution anchor, but pre-Merge headers still need execution-layer canonicality checks down to genesis.
   - Completion criteria: LogEx validates parent links, difficulty rules, and terminal total difficulty across the PoW/PoS boundary before treating pre-Merge logs as fully canonical.

3. Harden restart, reorg, and peer-retention behavior
   - Reason: Long-running correctness depends on recovering cleanly and retaining useful peers across normal mainnet churn.
   - Completion criteria: Mainnet smokes show stable resume, explicit fail-fast behavior for unsupported deep reorgs, persisted productive peers, and no misleading advertised serving range.

4. Checkpoint distribution and weak-subjectivity precision
   - Reason: `--checkpoint-sync-url` is still a temporary startup aid.
   - Completion criteria: LogEx has its own recent-checkpoint source or documented multi-source verification flow, and stale checkpoint rejection uses the exact consensus-spec weak-subjectivity calculation when enough state is available.

5. Release validation
   - Reason: Final guarantees depend on CL, EL, storage, query, and UI surfaces agreeing about what is verified and queryable.
   - Completion criteria: End-to-end fixtures cover checkpoint bootstrap, light-client updates, forward anchors, EL headers, receipts, logs, restart/resume, the Merge boundary, and query coverage.

## Design Decisions

- EL historical validation targets genesis.
  - Why: CL provides a recent trusted execution pivot; EL can validate execution history backward from that pivot.
  - Alternatives considered: Stop at the Merge block. That would leave the UI and verifier with an incorrect full-history target.
  - Tradeoff: Full completion now requires addressing pre-Merge PoW validation and much higher reverse-sync throughput.

- Do not promote arbitrary public EL peers to trusted Reth peers.
  - Why: Geth and Nethermind treat trusted/static peers as configured operator intent, not as a reward for one useful response.
  - Alternatives considered: Promote productive peers into Reth's trusted set. That was removed because it is not protocol-aligned and did not materially improve retention.
  - Tradeoff: LogEx keeps productive-peer preference in its own queue instead of bypassing normal peer behavior.

- Decode eth/69 receipts using the network no-bloom tuple shape.
  - Why: Geth serves receipts as `[tx_type, status_or_post_state, cumulative_gas_used, logs]` for eth/69+ receipt responses.
  - Alternatives considered: Continue treating typed receipts as EIP-2718 byte strings in no-bloom responses. That caused RLP decode failures and peer churn.
  - Tradeoff: Consensus receipt encoding remains separate from network receipt decoding.

- Keep only evidenced peer-retention changes.
  - Why: A live test that retained zero-response peers reduced useful serving peer count and did not improve throughput.
  - Alternatives considered: Keep idle/zero-response peers to avoid churn. That was rejected because it occupied slots without improving historical service.
  - Tradeoff: Peers that return no requested body/receipt data are still dropped from sync rotation, while protocol-compatible productive peers are persisted and prioritized.

- Follow the common-client peer shape before adding custom retention rules.
  - Why: Geth and Nethermind dominate the reachable EL peer set and both reserve substantial inbound capacity.
  - Alternatives considered: Keep most slots outbound. That reached fewer useful steady-state peers.
  - Tradeoff: More inbound capacity improves retention, but throughput still depends on how many body/receipt windows the downloader can keep active.

- Batch historical commits before indexing.
  - Why: Per-block historical writes and immediate index refreshes made storage overhead visible during reverse sync.
  - Alternatives considered: Keep every block as an independent write. That was simpler but made the P2P pipeline wait on storage too often.
  - Tradeoff: Recent queryability remains available, while historical secondary indexes may lag until the backfill catches up or sync is idle.

- Keep reverse historical windows at 1024 headers for now.
  - Why: Live peers cap reverse header responses at 1024 even when the local request limit is raised.
  - Alternatives considered: Request 4096 headers per window. That did not increase returned batch size.
  - Tradeoff: Further throughput needs multiple overlapped windows or a different trustless data acquisition strategy.

- Validate post-Merge execution bodies without a second sealed-block pass except for Osaka.
  - Why: Header validation and body/header validation already cover the current mainnet fork-field and root checks; the second pass recalculated the same roots on the hot path.
  - Alternatives considered: Keep the full Reth pre-execution block validation for every historical block. That was simpler but spent CPU on duplicate work.
  - Tradeoff: The shortcut must stay aligned with Reth consensus changes; Osaka still falls back to the sealed-block path for block-size validation.

- Inherit warmed request limits for newly connected peers and use dynamic combined body/receipt chunks.
  - Why: Geth seeds new downloader peers from the existing peer-set capacity, and live LogEx samples showed larger serving pools were underused by 32-block chunks.
  - Alternatives considered: Keep cold peer limits at 4/8 and static 32-block combined chunks, or switch all runs to static 16-block chunks. The first underused large peer pools; the second hurt cold starts with small peer pools.
  - Tradeoff: Dynamic chunking adds one heuristic that needs continued live validation against peer churn and rate limits.

- Retain speculative historical prefetches only after the current batch is accepted.
  - Why: A next-window fetch is valid only if the current contiguous batch validates and writes successfully.
  - Alternatives considered: Store the prefetched batch immediately after the network request returns. That could leave stale speculative data queued after a validation or storage failure.
  - Tradeoff: Failed current batches discard any concurrent prefetch work, preserving correctness over marginal reuse.

- Prefer compression shortcuts that preserve queryable compact pages over raw uncompressed history.
  - Why: Storage pressure is real, but profiling showed some compression work was wasted on pages where dictionary encoding already beat raw data.
  - Alternatives considered: Disable compression during sync or lower all Zstd levels. That would reduce CPU but risk much faster disk growth.
  - Tradeoff: Repeated values avoid extra Zstd CPU, while high-cardinality pages still use the stronger adaptive candidates.

## Challenges and Resolutions

- Challenge: Serving peers disconnected during receipt fetches with RLP decode errors.
  - Resolution: Matched the eth/69 no-bloom receipt response format used by major clients.
  - Remaining: Longer smokes should confirm this remains stable across Geth, Nethermind, Besu, and Reth peers.

- Challenge: Empty startup cache could make LogEx advertise an execution range it could not serve.
  - Resolution: Startup status now falls back to the genesis range, and the serve cache can answer genesis hash/header/receipt lookups.
  - Remaining: Continue validating advertised ranges as the local cache expands and after reorgs.

- Challenge: Reverse sync produced noisy per-block debug output.
  - Resolution: Demoted per-block and partial-response diagnostics to trace while preserving periodic progress logs.

- Challenge: Historical batches were too small after partial P2P chunk failures.
  - Resolution: Increased the pipelined downloader's early-return floor to keep useful contiguous progress while reducing batch overhead.
  - Remaining: Common peers still cap reverse header windows at 1024 blocks.

- Challenge: Peer retention improved to 90+ connected peers, but ETA stayed multiple days.
  - Resolution: Identified body/receipt fetch latency and serialized historical windows as the current bottleneck, then added one-window prefetch during historical storage writes.
  - Remaining: The downloader still needs deeper multi-window scheduling to approach a sub-6-hour target.

- Challenge: Adaptive request limits made combined body/receipt windows too coarse for the current single-window downloader.
  - Resolution: Use 32-block chunks during small-peer cold starts and 16-block chunks after the body/receipt serving pool is large enough.
  - Remaining: The latest clean sample reached `65.91 blocks/sec`, still multi-day. The next meaningful change is deeper multi-window scheduling.

- Challenge: Current-batch validation and storage were serialized ahead of the next network fetch.
  - Resolution: Overlapped validation/extraction/write for batch N with header/body/receipt fetch for batch N+1 and guarded against retaining speculative prefetches after failed current batches.
  - Remaining: The latest restart-wide sample improved to `75.75 blocks/sec`, with warmed tail batches roughly `120-150 blocks/sec`, but the sub-6-hour target still requires deeper scheduling or lower local ingest cost.

- Challenge: CPU profiling showed Keccak, duplicate root checks, Zstd compression, and allocation on the historical hot path.
  - Resolution: Removed redundant extraction, added a dictionary compression fast path, skipped duplicate non-Osaka pre-execution validation, and grouped validation into CPU-sized blocking tasks.
  - Remaining: Receipt-root validation and single-window fetch scheduling remain the main throughput risks.

## Dead Code and Obsolescence Cleanup

- Inspected EL peer management, historical ingestion, storage append/WAL paths, and background indexing.
- Removed or reverted ineffective peer-retention experiments that did not improve live samples.
- Removed the ineffective 4096-header historical window experiment after peers continued returning 1024 headers.
- Replaced an oversized P2P constructor argument list with a config struct while fixing clippy.
- Kept combined body/receipt chunk capping, ingest/fetch overlap, direct historical row materialization, and dictionary compression fast path because profiling or remote samples showed improvement.
- Confirmed obsolete single-block historical ingestion is no longer referenced; batched historical ingestion is the active path.
- Removed the redundant second historical extraction task phase from the reverse-sync write path; remaining request paths are still used as fallback or validation paths.
- Removed duplicate normal-path sealed-block pre-execution validation; the Osaka-only fallback remains intentionally.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: no
- Commits made during this run: `0984522` (`feat: improve execution sync pipeline`), `603c03f` (`docs: update execution sync roadmap`), `73f78e8` (`perf: increase historical receipt fanout`), `acd7586` (`perf: overlap historical ingest and prefetch`), `54e35fa` (`docs: update execution sync roadmap`), `aca3288` (`perf: reduce historical ingest CPU cost`), `b63488c` (`docs: update execution sync roadmap`), plus latest commit `perf: reduce validation and peer warmup cost`
- Pull request status: draft PR #76 (`https://github.com/tdenisenko/logex/pull/76`)
- Merge status: not applicable yet
- Git/GitHub blockers: local `gh` auth token is invalid, but the GitHub connector created the draft PR successfully; the PR should remain draft because the sub-6-hour sync target is not met yet

## Known Issues or Risks

- Reverse sync works and retains enough peers, but current measured throughput is still too slow for the full genesis target.
- Body/receipt acquisition remains the dominant bottleneck after peer retention improved.
- Pre-Merge PoW canonicality validation remains incomplete.
- Local shell access to `127.0.0.1:18683` requires elevated local-network permission in this environment; the client itself is listening on the fixed HTTP port.
- `--checkpoint-sync-url` remains a temporary startup aid until LogEx has its own checkpoint source.
