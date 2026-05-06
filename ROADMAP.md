# Roadmap

## Current Status

LogEx now bootstraps from a recent weak-subjectivity checkpoint, tracks CL head/finality forward over native CL P2P, and uses CL-authenticated execution anchors as the pivot for EL P2P validation. The EL path can fetch headers, bodies, and receipts from public execution peers, verify receipt roots without executing the EVM, index recent logs quickly, and expand the verified stored log range backward from the pivot while continuing to follow head.

Fresh fixed-port smoke on May 6, 2026:

- Data directory: `/tmp/logex-el-genesis-target-20260506`
- HTTP port: `18683`
- `/status` reports `historical_target_block: 0`
- Last sampled status: live head `25,038,616`, historical floor `25,038,015`, 7 serving EL peers, 504,414 stored rows, verified range `25,038,015-25,038,616`
- The client is intentionally still running for observation

## Completed Since Last Run

- Fixed the EL reverse-validation target to genesis instead of the Merge block.
- Fixed eth/69 no-bloom receipt decoding to match the network response shape used by Geth and other peers.
- Made empty-cache status and serving behavior honest by advertising and serving only the genesis range until real canonical data is cached.
- Removed ineffective public-peer trusted promotion while keeping productive peers prioritized in LogEx's own peer queue.
- Updated the dashboard so EL validation, pivot-based indexing, and stored log ranges use consistent genesis/pivot/head wording.

## Remaining TODOs

1. Improve EL reverse-sync throughput
   - Reason: The current smoke proves correctness but reverse verification throughput is still far below production-client expectations.
   - Completion criteria: Header/body/receipt fetching and verification are pipelined enough to sustain materially higher reverse-sync throughput with stable serving peers, and benchmark results are recorded.

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

## Challenges and Resolutions

- Challenge: Serving peers disconnected during receipt fetches with RLP decode errors.
  - Resolution: Matched the eth/69 no-bloom receipt response format used by major clients.
  - Remaining: Longer smokes should confirm this remains stable across Geth, Nethermind, Besu, and Reth peers.

- Challenge: Empty startup cache could make LogEx advertise an execution range it could not serve.
  - Resolution: Startup status now falls back to the genesis range, and the serve cache can answer genesis hash/header/receipt lookups.
  - Remaining: Continue validating advertised ranges as the local cache expands and after reorgs.

- Challenge: Reverse sync produced noisy per-block debug output.
  - Resolution: Demoted per-block and partial-response diagnostics to trace while preserving periodic progress logs.

## Dead Code and Obsolescence Cleanup

- Inspected EL peer management, serve-cache behavior, receipt primitives, progress status, and dashboard wording.
- Removed ineffective trusted-peer promotion and stale Merge-target references.
- Demoted investigation-only debug logs that obscured useful runtime progress.
- No additional obsolete EL pipeline code was removed because the remaining pieces are still part of the active reverse-sync path.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: no
- Commits made during this run: pending
- Pull request status: not created yet
- Merge status: not applicable yet
- Git/GitHub blockers: none known locally

## Known Issues or Risks

- Reverse sync works but is still too slow for a full genesis target without further pipeline/performance work.
- Pre-Merge PoW canonicality validation remains incomplete.
- Local shell access to `127.0.0.1:18683` requires elevated local-network permission in this environment; the client itself is listening on the fixed HTTP port.
- `--checkpoint-sync-url` remains a temporary startup aid until LogEx has its own checkpoint source.
