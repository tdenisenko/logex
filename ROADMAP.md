# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput-v3`.

The sparse historical segment coalescing work was merged. The Mac mini is running LogEx from `/Users/gremlinmaster/logex-src` against `/Volumes/SSD 4TB/LogEx` through the VPS full tunnel with public egress/NAT `157.245.195.72`. Tailscale remains stopped on the Mac during full-tunnel testing; manage the host through `ssh pi-remote` and then `ssh gremlinmaster@192.168.50.44`. The client is running in a detached tmux session using `/Users/gremlinmaster/.local/bin/tmux`, not `screen`.

Historical sync is progressing, but the remaining bottleneck is still body/receipt fetch tail latency. Local validation and storage writes are much smaller than fetch time in the dense ranges sampled so far. Prefix redundancy now activates at smaller serving-peer counts, which improves low-peer dense-range throughput but does not remove the long-tail fetch bottleneck.

## Completed Since Last Run

- Merged the sparse historical segment coalescing PR and deleted its remote branch.
- Created `perf/historical-sync-throughput-v3` from updated `master`.
- Reproduced the historical-sync burst/plunge pattern on the Mac mini.
- Added a scheduler refill fix so completed historical prepare results also refill the body/receipt fetch pipeline with the next child header.
- Verified the refill fix with `cargo fmt --all -- --check` and `cargo test -p logex-sync`.
- Deployed the refill fix to the Mac mini and confirmed historical sync resumes on port `18683` in tmux.
- Tested a denser active-fetch experiment and reverted it:
  - Raising dense fetch depth from 6 to 10 increased RSS and CPU but worsened request-plan tail latency and floor stalls.
  - The running client was restored to the stable refill-fix build.
- Kept a targeted prefix-redundancy change for dense body/receipt plans:
  - Redundant prefix requests now activate at 8 serving peers instead of 16.
  - Live testing on the Mac mini improved low-peer throughput relative to the restored baseline, while focused, full `logex-sync`, and clippy validation passed.

## Remaining TODOs

1. Continue live benchmarking of prefix redundancy and scheduler refill behavior over a longer warm run.
   - Reason: Early low-peer samples improved, but higher serving-peer samples are still required before treating the change as production-proven.
   - Completion criteria: Active fetches remain populated during dense sync, peer count warms normally, and sustained throughput improves without higher memory churn or excessive duplicate fetch work.

2. Reduce body/receipt fetch tail latency.
   - Reason: Request plans still regularly take 10-40 seconds, causing visible floor stalls even when CPU, RAM, and disk are not saturated.
   - Completion criteria: Implement and benchmark a peer-selection or scheduling change that materially reduces long plan tails and improves sustained logs/sec; revert any experiment that does not help.

3. Match production-client P2P behavior more closely.
   - Reason: The target is Geth/Nethermind-class peer retention and sync performance.
   - Completion criteria: Audit relevant Geth/Nethermind peer scoring, request scheduling, and timeout behavior; apply only compatible improvements with live benchmarks.

4. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a live release-candidate run from a clean data directory.

## Design Decisions

- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.
- Checkpoints are accepted only when recent relative to a checkpoint-sync endpoint. Persisted state that is too stale requires a fresh recent checkpoint.
- Sparse historical writes are coalesced in durable storage so verified rows remain crash-safe while segment count stays bounded.
- During the current Mac mini benchmark, full-VPS routing is preferred over direct Tailscale management; use the Raspberry Pi SSH path until the network mode changes.
- Dense historical fetch window size remains capped at 512 blocks. Increasing active dense fetch depth to 10 was rejected because it raised memory/CPU pressure and worsened request tail latency.
- Dense body/receipt prefix redundancy starts at 8 serving peers so the earliest contiguous chunks are less likely to block on a single slow peer. This spends limited spare request capacity on the prefix instead of broadening the whole fetch pipeline.

## Challenges and Resolutions

- Challenge: Dense-depth increase looked like an obvious way to hide slow peers.
  - Resolution: Live testing showed worse stalls and higher RSS/CPU, so the change was reverted.

- Challenge: Completed historical prepare results could consume buffered batches without scheduling the next fetch window.
  - Resolution: Carried the next child header through prepared/written batches and refilled the fetch pipeline after completed prepare ingestion.

- Challenge: Low-peer dense sync could stall behind a slow leading body/receipt chunk.
  - Resolution: Lowered the prefix-redundancy serving-peer threshold from 16 to 8 and validated it with live low-peer samples plus `logex-sync` tests and clippy.

- Challenge: Tailscale and host-wide WireGuard full-tunnel routing conflict on the Mac mini.
  - Resolution: Keep Tailscale stopped during full-tunnel performance runs and manage through the Raspberry Pi route.

## Dead Code and Obsolescence Cleanup

- Inspected historical scheduler and request-layer changes made during this pass.
- Removed the ineffective dense-depth and high-pipeline-threshold experiments before committing.
- Inspected body/receipt request scheduling helpers and tests touched by prefix redundancy; no dead production code was identified.

## Git Workflow

- Current branch: `perf/historical-sync-throughput-v3`
- New branch created this run: yes
- Commits made during this run: pending
- Pull request status: not created for the current branch
- Merge status: previous storage PR merged; current performance branch not merged
- Blockers: none, but longer live benchmarking is required before opening a PR for performance work.

## Known Issues or Risks

- Historical sync still has burst/plunge behavior in dense ranges because body/receipt request plans are dominated by slow peer tails.
- The Mac mini is temporarily managed through the Raspberry Pi while full-VPS WireGuard mode is active.
- Future performance experiments must be benchmarked on live sync before committing; changes that only increase concurrency without reducing tail latency can make throughput worse.
