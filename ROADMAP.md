# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Current branch: `fix/auto-refresh-stale-checkpoint`.

The Mac mini is running this branch against the saved full-sync data directory. Public dashboard forwarding through `157.245.195.72:18683` is active, and the client is bridging the stale restart gap from the saved execution head to the refreshed consensus checkpoint.

## Completed Since Last Run

- Added automatic startup recovery for stale persisted consensus state and stale local execution progress.
- Added a checkpoint-gap bridge that validates the EL parent chain up to a fresh CL checkpoint anchor before ingesting the missing logs.
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

## Challenges and Resolutions

- Challenge: A full synced data directory failed to restart after being offline because the persisted consensus state and local execution head were older than the recent checkpoint window.
  - Resolution: Startup now resolves a fresh checkpoint, archives stale CL state, and resumes using the existing EL/log storage.
  - Remaining: None known for correctness.

- Challenge: The first recovery attempt hit the consensus reorg guard because the fresh CL anchor was ahead of the persisted recent-header window.
  - Resolution: Future-only anchors are classified as restart gaps, then bridged by fetching and validating the EL header chain to the CL anchor.
  - Remaining: Gap-bridge throughput can be optimized later if this path becomes common.

## Dead Code and Obsolescence Cleanup

- Inspected the stale startup guards and consensus reorg classifier.
- No obsolete code was safely removable in this pass; the new bridge reuses existing validation, peer, and storage primitives.
- Removed no files.

## Git Workflow

- Current branch: `fix/auto-refresh-stale-checkpoint`.
- New branch created from `master`.
- Commits made during this run: pending.
- Pull request status: pending after commit/push.
- Merge status: not merged yet.
- Validation run:
  - `cargo test -p logex-node archived_consensus_state`
  - `cargo test -p logex-node restart_guard`
  - `cargo test -p logex-sync locate_consensus_reorg`
  - `cargo clippy -p logex-node -p logex-sync --all-targets -- -D warnings`

## Known Issues or Risks

- The checkpoint-gap bridge prioritizes trustless recovery over throughput. Normal historical sync performance is unchanged.
- The local Codex sandbox cannot directly curl the public VPS dashboard, but the VPS can reach `10.66.0.2:18683` and packet capture showed public TCP/18683 traffic being forwarded and answered.
