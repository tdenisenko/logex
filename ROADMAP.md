# Roadmap

## Current Status

LogEx is converging on a checkpoint-anchored architecture:

- CL bootstraps from a recent weak-subjectivity checkpoint, verifies light-client updates, tracks finalized and optimistic heads, and materializes forward execution anchors from the checkpoint toward head.
- CL no longer syncs backward below the checkpoint. Historical block/log verification below the checkpoint is EL work.
- EL P2P remains the next major implementation area. It must use CL-verified execution anchors as trust roots, then walk headers and receipts backward without executing the EVM.
- The dashboard now presents CL forward progress, EL validation, indexing, and queryable log coverage without a separate CL history lane.

Fresh fixed-port smoke on May 6, 2026:

- data directory: `/tmp/logex-cl-forward-only-20260506`
- HTTP port: `18683`
- CL checkpoint bootstrap succeeded
- forward CL anchors reached optimistic head with `127` materialized anchors and `0` anchor gaps at the last sampled `/status`
- the client is intentionally still running for observation

## Completed Since Last Run

- Killed the stale client process and cleared its old `/tmp/logex-cl-stability-gate-20260506-a` data directory.
- Removed backward CL historical scheduling, root recovery, materialization, status counters, and tests.
- Removed CL history ETA/rate/floor UI surfaces and replaced them with forward CL anchor wording.
- Kept forward beacon-block range/root fetching because the current CL-to-EL handoff still needs contiguous execution anchors above the checkpoint.
- Validated the branch with:
  - `cargo fmt`
  - `cargo clippy --all-targets --all-features -- -D warnings`
  - `cargo test`
  - `cargo build --release`
  - a fresh fixed-port mainnet smoke on `18683`

## Remaining TODOs

1. EL backward verification from a CL anchor
   - Reason: CL can prove the recent/post-Merge anchor, but receipts and historical headers still come from EL peers.
   - Completion criteria: EL can start from a CL-verified execution block, walk backward through parent hashes, fetch receipts, rebuild receipt tries, and accept logs only when the computed receipts root matches the verified header.

2. Pre-Merge PoW canonicality
   - Reason: Beacon light-client data does not prove pre-Merge execution blocks.
   - Completion criteria: LogEx identifies the terminal PoW boundary and verifies pre-Merge headers by parent links, difficulty rules, and total difficulty down to genesis.

3. Reorg and restart hardening
   - Reason: Forward CL tracking and EL validation must remain correct across optimistic reorgs, shutdowns, and process restarts.
   - Completion criteria: Mainnet smokes show stable recovery or explicit fail-fast behavior for reorgs deeper than the retained recent-header window.

4. Checkpoint distribution and weak-subjectivity precision
   - Reason: `--checkpoint-sync-url` is a temporary startup aid, and stale checkpoint rejection currently uses a conservative fixed freshness window.
   - Completion criteria: LogEx has its own recent-checkpoint distribution or documented multi-source verification flow, and stale checkpoint rejection uses the exact consensus-spec weak-subjectivity calculation when enough state is available.

5. Release validation
   - Reason: The final guarantees depend on CL, EL, storage, query, and UI surfaces agreeing about what is verified and queryable.
   - Completion criteria: End-to-end fixtures cover checkpoint bootstrap, light-client updates, forward anchors, EL headers, receipts, logs, restart/resume, and the Merge boundary; benchmarks are recorded on realistic log-heavy datasets.

## Design Decisions

- CL sync is forward-only from the weak-subjectivity checkpoint.
  - Why: A recent finalized checkpoint gives an execution anchor. EL can verify historical headers and receipts backward from that anchor; CL backward sync would duplicate work and gate EL unnecessarily.
  - Alternative considered: continue proving beacon ancestry backward to the Merge before EL history work. That was rejected because it made CL the bottleneck for data that EL can verify itself.
  - Tradeoff: CL no longer provides a user-visible historical floor. EL must implement the historical trust path correctly before full-chain guarantees are complete.

- Keep using native CL P2P for steady-state head/finality tracking.
  - Why: LogEx should not depend on an external CL RPC after startup.
  - Alternative considered: keep relying on a Beacon API for CL data. That weakens the project goal.
  - Tradeoff: P2P peer churn remains a real operational concern and must stay visible in metrics.

- Keep EL log verification EVM-free.
  - Why: LogEx only needs canonical headers and receipt roots to verify logs.
  - Alternative considered: reuse a full execution-client sync path. That would add state trie and EVM work the project explicitly avoids.
  - Tradeoff: LogEx must implement its own high-throughput receipt/header pipeline instead of inheriting a full client database.

## Challenges and Resolutions

- Challenge: CL backward sync made the sync ETA unrealistic and conceptually assigned historical EL work to CL.
  - Resolution: Removed backward CL sync and made CL status/UI forward-only.
  - Remaining: EL backward verification must now become the next implementation focus.

- Challenge: Public CL peers churn and return mixed-quality history responses.
  - Resolution: Existing peer scoring, cooldown, range/root validation, and status metrics remain in place for the forward anchor path.
  - Remaining: Longer mainnet smokes are still needed before treating peer retention as production-ready.

- Challenge: The UI used CL-history terms that were no longer meaningful.
  - Resolution: Removed CL history floor/ETA/rate displays and kept only metrics that describe current CL, EL, indexing, and query progress.

## Dead Code and Obsolescence Cleanup

- Inspected CL network scheduling/materialization, status serialization, REST test fixtures, and dashboard code.
- Removed obsolete backward CL request scheduling, backward root recovery, backward materialization helpers, related tests, and UI/status references.
- Kept forward beacon-block range/root support because it is still required for contiguous CL-authenticated execution anchors above the checkpoint.

## Git Workflow

- Current branch: `fix/cl-forward-sync-only`.
- New branch created this run: yes.
- Commits made during this run: pending final commit.
- Pull request status: ready to publish after final commit.
- Merge status: pending.
- Git/GitHub blockers: none known locally.

## Known Issues or Risks

- EL backward sync and pre-Merge PoW verification are not implemented yet, so LogEx still cannot claim full-chain canonical log coverage.
- The fresh smoke shows CL forward sync working, but EL peers are not yet part of this branch’s success criteria.
- `--checkpoint-sync-url` remains a temporary trusted startup aid until LogEx has its own checkpoint source.
- Keep fixed-port smokes on HTTP `18683`; stop stale processes instead of incrementing the port.
