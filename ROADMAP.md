# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput-v3`.

The Mac mini WireGuard/VPS path is healthy again after replacing the stale-interface wrapper with a health-checking LaunchDaemon script. The wrapper now treats a tunnel as healthy only if the VPS tunnel IP responds or the WireGuard handshake is recent; otherwise it restarts the tunnel and restores the full-tunnel routes while preserving LAN access.

Historical sync performance work has revalidated the earlier high-throughput commits and isolated the main regression to the storage coalescing path introduced after `b7129fe`. The active candidate now keeps sparse historical coalescing for disk efficiency but writes dense historical batches directly as compacted sealed segments. A Mac mini benchmark from `/Users/gremlinmaster/logex-peerload-test` against `/Volumes/SSD 4TB/LogExBench/adaptive-storage-20260619-120422` recovered 900k+ logs/sec peaks and materially higher sustained throughput, while still showing periodic peer-tail dips that need a separate downloader/scheduler fix.

## Completed Since Last Run

- Diagnosed the WireGuard outage as a stale utun/routes state: the interface existed, but the UDP path/handshake was stale, so the old wrapper did not force a restart.
- Confirmed the repaired tunnel has working VPS egress, tunnel ping, DNAT/forwarding counters, and dashboard access through the VPS.
- Built a clean `master` binary on the Mac mini after fixing the SSH PATH for Homebrew `protoc`.
- Restarted LogEx gracefully in tmux without resetting the data directory and benchmarked `master` against the same data/tunnel path.
- Reverted the earlier v3 scheduler experiments that regressed throughput versus `master`.
- Added a narrower peer-load scheduler change for body/receipt attempts and benchmarked it on the Mac mini.
- Measured the peer-load experiment at roughly 212k average logs/sec versus roughly 195k for the refreshed baseline, with plan p95 improving from about 21.3s to about 13.6s and timeout failures per plan dropping from about 2.0 to about 0.7.
- Validated the restored baseline with `cargo fmt --all -- --check`, `cargo test -p logex-sync`, `cargo clippy -p logex-sync --all-targets -- -D warnings`, and `cargo check --workspace`.
- Benchmarked earlier commit states on the Mac mini:
  - `6e6cbd9`: recovered 830k logs/sec max with roughly 385k last-24-sample average, but with high timeout churn.
  - `8136e0a`: recovered 953k logs/sec max with roughly 382k last-24-sample average after warm-up.
  - `812b308`: best earlier balanced baseline, roughly 405k all-window average and 851k max.
  - `6268e82` and `b7129fe`: retained high peaks but had weaker p10/tail behavior than `812b308`.
  - `0e55883` and current master-line storage: dropped back near the 160k-235k range, identifying storage coalescing as the main regression.
- Added adaptive historical storage writes: dense historical batches now bypass raw staging and are written as compacted sealed segments immediately, while sparse batches still coalesce into an active historical segment.
- Added storage tests covering dense sub-target compacted writes and sparse historical coalescing.
- Rebuilt and deployed the adaptive storage branch on the Mac mini; the live benchmark recovered roughly 531k last-24-sample average and 992k max in the first parsed window, then continued showing 300k-890k dashboard samples as peers warmed up.

## Remaining TODOs

1. Confirm the adaptive dense/sparse storage candidate over a longer run.
   - Reason: The first benchmark recovered high throughput while preserving sparse coalescing, but a longer window is needed to estimate total sync-time impact.
   - Completion criteria: Parse at least a 20+ minute dense-range run and confirm sustained logs/sec, p10, p95, and batch tail latency beat `812b308` and current master-line storage without excessive disk growth.

2. Reduce peer-tail sawtooth in dense historical sync.
   - Reason: The goal is to reduce full historical sync toward the 4-hour target without relying on short spikes.
   - Completion criteria: Use body/receipt plan latency, active fetch count, buffer depth, serving-peer mix, CPU, memory, disk, and network observations to prove whether bounded denser lookahead, better hedging, or peer scoring reduces low-throughput minutes without increasing failure churn or memory risk.

3. Match production-client P2P behavior more closely.
   - Reason: The target is Geth/Nethermind-class peer retention and sync performance.
   - Completion criteria: Audit relevant Geth/Nethermind peer scoring, request scheduling, timeout, and peer-retention behavior; implement compatible changes only when benchmarks show they beat the restored baseline.

4. Complete release hardening.
   - Reason: Production readiness depends on verification safety, restart safety, and predictable operations.
   - Completion criteria: Smokes or tests cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a clean full-sync release-candidate run.

## Design Decisions

- Use sustained logs/sec, p95 plan/body-receipt latency, and timeout rate as the main benchmark criteria; peak logs/sec alone is not enough to keep a change.
- Keep the `master` body/receipt scheduler as the performance baseline until a live benchmark proves a replacement is better, then compare that result with earlier high-throughput commit states.
- Preserve useful stabilization changes when they improve tail latency without sacrificing sustained throughput.
- Historical storage should be density-aware: dense batches are already large enough to amortize compacted segment overhead, while sparse ranges need raw staging/coalescing to avoid tiny segment and disk-usage growth.
- WireGuard health must be based on actual tunnel liveness, not just whether a utun interface exists.
- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.

## Challenges and Resolutions

- Challenge: WireGuard suddenly stopped working without a recent config edit.
  - Resolution: Identified the stale-interface failure mode and installed a health-checking wrapper that restarts the tunnel when ping/handshake checks fail.

- Challenge: The v3 branch had accumulated many plausible scheduler changes but live throughput was worse than prior baselines.
  - Resolution: Built and ran clean `master` on the same Mac/data/tunnel path, confirmed it was materially better, and reverted the broad v3 scheduler changes.

- Challenge: The latest peer-load scheduler change improved request tail latency but not enough to meet the throughput target.
  - Resolution: Kept it as a stabilization candidate and moved the next benchmark step to explicit earlier-commit testing.

- Challenge: Current master-line storage was much slower than the high-throughput earlier commits.
  - Resolution: Benchmarked the relevant commit sequence and found the drop at the storage coalescing change. Implemented adaptive dense/sparse storage so dense batches use the fast compacted write path and sparse batches retain coalescing.

- Challenge: Adaptive storage recovered high peaks but still shows low-throughput minutes with many serving peers.
  - Resolution: Treat the remaining bottleneck as peer-tail/downloader scheduling rather than storage; the next experiment should be bounded dense lookahead or peer-tail mitigation measured over a longer run.

- Challenge: Clean remote master build failed because `protoc` was not on the non-interactive SSH PATH.
  - Resolution: Confirmed Homebrew protobuf was already installed and rebuilt with `/usr/local/bin` on PATH.

## Dead Code and Obsolescence Cleanup

- Inspected the historical scheduler changes in `crates/logex-sync/src/engine/anchored.rs`, `crates/logex-sync/src/engine/mod.rs`, and `crates/logex-sync/src/p2p/peer_manager/requests.rs`.
- Inspected the storage regression in `crates/logex-storage/src/native/storage.rs`, `crates/logex-storage/src/native/segment.rs`, and `crates/logex-storage/src/partition.rs`.
- Removed the obsolete broad v3 scheduler delta from the branch by restoring the proven master implementation before adding the narrower peer-load change.
- Removed no unrelated production code; the storage change reuses the prior compacted segment writer and retains the existing sparse staging path.

## Git Workflow

- Current branch: `perf/historical-sync-throughput-v3`
- New branch created this run: no
- Commits made during this run: `fa7b806 perf: stabilize body receipt peer scheduling`; pending commit for adaptive dense/sparse historical storage.
- Pull request status: not created
- Merge status: not merged
- Blockers: none for local code validation; performance target still requires longer remote benchmarking and peer-tail mitigation.

## Known Issues or Risks

- Adaptive storage recovered the 800k-900k+ peak range, but total sync-time improvement depends on reducing low-throughput peer-tail minutes.
- Earlier commit tests must use isolated test data dirs or carefully verified compatibility so old storage code does not mutate the current storage-fix data.
- The branch is not ready for PR/merge until validation passes and a longer remote benchmark confirms a measured improvement over the selected baseline.
