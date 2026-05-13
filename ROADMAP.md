# Roadmap

## Current Status

LogEx boots from a recent weak-subjectivity checkpoint, follows CL head/finality over native CL P2P, and uses CL-authenticated execution anchors as the pivot for EL validation. EL P2P can follow head, walk historical execution data backward from the pivot, verify headers/bodies/receipt roots without executing the EVM, and index queryable logs while the stored range expands toward genesis.

The active branch is `feature/el-reverse-sync` and the active draft PR is #76. The current work is focused on EL reverse-sync throughput, peer warmup, and storage pressure. Remote testing is on `root@164.92.232.250` with fixed HTTP port `18683` and data dir `/root/logex-data-remote`.

The latest meaningful bottleneck is no longer raw log storage. Historical batches write raw sealed segments quickly, compress them in the background, and now coalesce historical writes into larger 1024-block chunks to reduce segment churn. On the current 4-vCPU/8 GB remote, the best comparable sample after this change reached roughly 318 historical blocks/sec with a zero raw-segment backlog. The remaining bottleneck is local receipt-root validation/log extraction plus body/receipt P2P latency while the serving peer pool warms up.

## Completed Since Last Run

- Increased historical write coalescing from 512 to 1024 blocks after remote testing showed lower write churn and raw backlog without a throughput regression.
- Added explicit mainnet Merge/terminal PoW block constants to the status surface and tested them against the Reth mainnet chainspec.
- Documented the pre-Merge validation model: finalized post-Merge ancestry plus per-block body and receipt-root validation, without independent Ethash replay.
- Tested a 48-task validation/extraction fanout and reverted it because the remote sample did not clearly beat the existing 32-task configuration.

## Remaining TODOs

1. Reduce EL reverse-sync ETA below the production target.
   - Reason: Current remote runs are still far above the sub-6-hour full-history target.
   - Completion criteria: A fresh mainnet-like run sustains sub-6-hour ETA on adequate hardware, or a documented architecture decision replaces full P2P receipt backfill with another trustless strategy.

2. Stabilize EL peer ramp and body/receipt throughput.
   - Reason: The downloader needs enough serving peers to hide request latency and keep wide fetch windows active.
   - Completion criteria: Long remote runs retain a large serving pool, keep lookahead filled, and do not regress peer retention compared with the best observed run.

3. Replace the temporary checkpoint source.
   - Reason: `--checkpoint-sync-url` still depends on an external checkpoint provider.
   - Completion criteria: LogEx has its own recent-checkpoint source or a documented multi-source verification flow, with stale checkpoint rejection aligned to consensus weak-subjectivity rules.

4. Complete release validation.
   - Reason: CL, EL, storage, query, and UI surfaces need shared evidence for what is verified and queryable.
   - Completion criteria: End-to-end tests or smokes cover checkpoint bootstrap, live anchors, reverse EL headers/bodies/receipts, restart/resume, Merge boundary behavior, query coverage, limits, and pagination.

## Design Decisions

- CL sync is forward-only from a recent checkpoint; EL historical sync is responsible for walking execution data back toward genesis.
- Logs are valid only inside the verified contiguous stored range. Unsynced historical gaps remain outside query coverage.
- Historical storage writes raw sealed segments on the sync hot path and compresses them through periodic background compaction.
- Historical fetch width is peer-count and memory aware. Small 8 GB hosts stay conservative; larger hosts can use wider windows when the serving peer pool is strong.
- Query responses keep a hard `10,000` row cap and default to `50` row pages.
- Pre-Merge log validity is derived from the CL-finalized post-Merge execution header's recursive parent-hash ancestry. LogEx walks that canonical header chain to genesis and validates each block's bodies and receipts against the committed header roots; it does not independently replay Ethash or rederive total difficulty from genesis.

## Challenges and Resolutions

- Challenge: Inline compression made dense historical batches spend too much time in storage writes.
  - Resolution: Raw sealed writes are now used on the hot path, with background compaction handling compression continuously.

- Challenge: Wider fetch windows improved latency hiding but could push the 8 GB remote into unsafe memory pressure.
  - Resolution: Fetch window and lookahead depth now require enough total memory before widening beyond the 2048-block tier.

- Challenge: Post-Osaka block validation duplicated body/root work and cloned full bodies.
  - Resolution: Validation now keeps the required checks while replacing the duplicate Reth block validator call with the specific Osaka max-RLP-size check.

- Challenge: Recent peer behavior regressed from the best observed branch state.
  - Resolution: Peer-warmup constants were pulled back toward commit `8f97cef`; longer remote sampling is still needed to confirm recovery.

- Challenge: More validation workers could have reduced tail latency, but the 48-task fanout produced flat-to-worse remote results.
  - Resolution: The fanout experiment was reverted and the stable 32-task configuration was kept.

## Dead Code and Obsolescence Cleanup

- Inspected historical sync, storage compaction, validation, and peer-manager changes for experimental code.
- Removed stale-dial quarantine/demotion behavior from this branch before the current run.
- Removed obsolete prepared decoded-row lookahead; current lookahead stays at the fetched body/receipt layer.
- Reverted the experimental 48-task validation/extraction fanout after it failed to produce a meaningful throughput gain.
- Kept the raw-segment reader/compactor paths because they are required for immediate queryability plus deferred compression.
- Local `/private/tmp/geth-src` and `/private/tmp/nethermind-src` directories are present but empty, so they could not be used for this pass.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: no
- Commits made during this run: `perf: reduce historical write segment churn`
- Pull request status: draft PR #76 (`https://github.com/tdenisenko/logex/pull/76`)
- Merge status: not ready; throughput target and longer release validation remain incomplete.
- Git/GitHub blockers: none known.

## Known Issues or Risks

- Current EL reverse-sync ETA remains above target.
- The current remote is small; larger RAM can safely activate wider fetch windows, but CPU and receipt validation still need profiling.
- Full public-P2P receipt backfill may not match snap-sync full-node timings without deeper architectural changes or another trustless data source.
- LogEx does not independently replay pre-Merge PoW fork choice; it relies on finalized post-Merge ancestry plus receipt-root validation for light-client log validity.
- The latest remote peer ramp is still under observation after restoring `8f97cef`-style warmup constants.
- The latest comparable 8 GB remote sample is still far above target at roughly 18 hours ETA.
