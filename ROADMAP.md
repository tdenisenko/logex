# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Current branch: `fix/auto-refresh-stale-checkpoint`.

The Mac mini is running this branch against the saved full-sync data directory. Public dashboard forwarding through `157.245.195.72:18683` is active, and the client can bridge a stale restart gap from the saved execution head to a refreshed consensus checkpoint.

## Completed Since Last Run

- Added automatic startup recovery for stale persisted consensus state and stale local execution progress.
- Added a checkpoint-gap bridge that validates the EL parent chain up to a fresh CL checkpoint anchor before ingesting the missing logs.
- Pipelined checkpoint-gap body/receipt fetching with bounded lookahead so long-offline forward catch-up no longer waits for one chunk to fully ingest before requesting the next.
- Fixed checkpoint-gap progress accounting so dashboard logs/sec is sampled once per ingested batch instead of once per block after a batch write.
- Kept the existing EL/log storage and known peer cache intact during checkpoint refresh.
- Verified the remote dashboard path through the VPS and restarted the native Mac mini client with the fix.

## Remaining TODOs

No remaining TODOs for the stale-checkpoint restart recovery task.

## Design Decisions

- Archive stale `cl/consensus_state.json` instead of deleting it.
  - Why: The client can recover automatically while preserving a diagnostic copy of the old trusted state.
  - Alternatives considered: fail startup and require manual data-dir surgery. That was operationally fragile and caused the dashboard outage.
  - Tradeoff: The data directory may retain a small archived consensus snapshot after recovery.

- Bridge long-offline gaps with EL parent-chain validation ending at a fresh CL checkpoint anchor.
  - Why: CL historical sync is intentionally not required; the fresh checkpoint execution hash is enough to validate the canonical EL ancestor chain back to the saved head.
  - Alternatives considered: require a fresh data directory, or reintroduce CL historical sync. Both add unnecessary operational cost for this restart case.
  - Tradeoff: The bridge is correctness-first and less optimized than the normal historical pipeline.

- Account checkpoint-gap progress per contiguous batch.
  - Why: The gap bridge ingests rows in batches; per-block progress updates after a batch write distort live logs/sec because each block update can be separated by only microseconds.
  - Alternatives considered: keep per-block progress updates. That made the dashboard report impossible rates during catch-up.
  - Tradeoff: The live forward metric is chunk-granular during restart recovery, which matches the actual batch-oriented work.

## Challenges and Resolutions

- Challenge: A full synced data directory failed to restart after being offline because the persisted consensus state and local execution head were older than the recent checkpoint window.
  - Resolution: Startup now resolves a fresh checkpoint, archives stale CL state, and resumes using the existing EL/log storage.
  - Remaining: None known for correctness.

- Challenge: The first recovery attempt hit the consensus reorg guard because the fresh CL anchor was ahead of the persisted recent-header window.
  - Resolution: Future-only anchors are classified as restart gaps, then bridged by fetching and validating the EL header chain to the CL anchor.
  - Remaining: None known.

- Challenge: The dashboard briefly reported impossible multi-billion logs/sec during checkpoint-gap catch-up.
  - Resolution: Progress tracking now supports batched forward updates, and the gap bridge records one live rate sample per ingested chunk.
  - Remaining: None known.

## Dead Code and Obsolescence Cleanup

- Inspected the stale startup guards, consensus reorg classifier, checkpoint-gap bridge, and progress tracker.
- No obsolete code was safely removable in this pass; the new bridge reuses existing validation, peer, and storage primitives.
- Removed no files.

## Git Workflow

- Current branch: `fix/auto-refresh-stale-checkpoint`.
- New branch created from `master`.
- Commits made during this run:
  - `7dc7c6fd fix: recover stale checkpoint restarts`
  - `af3a59a3 fix: pipeline stale checkpoint catchup`
- Pull request status: draft PR #102 opened for `fix/auto-refresh-stale-checkpoint` into `master`.
- Merge status: not merged yet.
- Validation run:
  - `cargo test -p logex-node archived_consensus_state`
  - `cargo test -p logex-node restart_guard`
  - `cargo test -p logex-sync locate_consensus_reorg`
  - `cargo test -p logex-sync checkpoint_gap_pipeline_depth`
  - `cargo test -p logex-sync forward_batch_progress_records_one_live_rate_sample`
  - `cargo clippy -p logex-node -p logex-sync --all-targets -- -D warnings`

## Known Issues or Risks

- The checkpoint-gap bridge is only for long-offline restart recovery. Normal historical reverse sync and live tip-following paths are unchanged.
- The local Codex sandbox cannot directly curl the public VPS dashboard, but the VPS can reach `10.66.0.2:18683` and packet capture showed public TCP/18683 traffic being forwarded and answered.
