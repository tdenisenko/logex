# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput-v3`.

The Mac mini WireGuard/VPS path is healthy again after replacing the stale-interface wrapper with a health-checking LaunchDaemon script. The wrapper now treats a tunnel as healthy only if the VPS tunnel IP responds or the WireGuard handshake is recent; otherwise it restarts the tunnel and restores the full-tunnel routes while preserving LAN access.

Historical sync performance work is benchmarking from the proven `master` scheduler/storage baseline. A fresh same-data peer-load experiment on the Mac mini reduced historical body/receipt plan tail latency and timeout churn, but did not yet recover the earlier high sustained throughput. The remote client is currently running that peer-load experiment from `/Users/gremlinmaster/logex-peerload-test` against `/Volumes/SSD 4TB/LogEx` for continued observation.

## Completed Since Last Run

- Diagnosed the WireGuard outage as a stale utun/routes state: the interface existed, but the UDP path/handshake was stale, so the old wrapper did not force a restart.
- Confirmed the repaired tunnel has working VPS egress, tunnel ping, DNAT/forwarding counters, and dashboard access through the VPS.
- Built a clean `master` binary on the Mac mini after fixing the SSH PATH for Homebrew `protoc`.
- Restarted LogEx gracefully in tmux without resetting the data directory and benchmarked `master` against the same data/tunnel path.
- Reverted the earlier v3 scheduler experiments that regressed throughput versus `master`.
- Added a narrower peer-load scheduler change for body/receipt attempts and benchmarked it on the Mac mini.
- Measured the peer-load experiment at roughly 212k average logs/sec versus roughly 195k for the refreshed baseline, with plan p95 improving from about 21.3s to about 13.6s and timeout failures per plan dropping from about 2.0 to about 0.7.
- Validated the restored baseline with `cargo fmt --all -- --check`, `cargo test -p logex-sync`, `cargo clippy -p logex-sync --all-targets -- -D warnings`, and `cargo check --workspace`.

## Remaining TODOs

1. Identify the best earlier historical-sync commit baseline.
   - Reason: Retained June 15 runs show materially higher sustained throughput and 800k-970k logs/sec peaks than the current master-line run.
   - Completion criteria: Benchmark `6e6cbd9`, `8136e0a`/`812b308`, `b7129fe`, and current master/storage-fix states under comparable Mac mini/VPS conditions, then identify which code changes improve sustained logs/sec and plan tail latency.

2. Recombine useful stability/storage fixes with the fastest proven baseline.
   - Reason: The storage coalescing fix prevents excessive disk usage, and the peer-load experiment improves plan tail latency, but neither should be kept at the cost of major throughput loss.
   - Completion criteria: Carry forward storage coalescing, restart safety, and proven peer scheduling improvements only after same-machine benchmarks show sustained throughput remains above the selected baseline.

3. Identify the remaining bottleneck if throughput stays below target after peer warm-up.
   - Reason: The goal is to reduce full historical sync toward the 4-hour target without relying on short spikes.
   - Completion criteria: Use body/receipt plan latency, peer count, serving-peer mix, CPU, memory, disk, and network observations to prove whether the bottleneck is peer tail latency, local processing, or infrastructure before changing code.

4. Match production-client P2P behavior more closely.
   - Reason: The target is Geth/Nethermind-class peer retention and sync performance.
   - Completion criteria: Audit relevant Geth/Nethermind peer scoring, request scheduling, timeout, and peer-retention behavior; implement compatible changes only when benchmarks show they beat the restored baseline.

5. Complete release hardening.
   - Reason: Production readiness depends on verification safety, restart safety, and predictable operations.
   - Completion criteria: Smokes or tests cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a clean full-sync release-candidate run.

## Design Decisions

- Use sustained logs/sec, p95 plan/body-receipt latency, and timeout rate as the main benchmark criteria; peak logs/sec alone is not enough to keep a change.
- Keep the `master` body/receipt scheduler as the performance baseline until a live benchmark proves a replacement is better, then compare that result with earlier high-throughput commit states.
- Preserve useful stabilization changes when they improve tail latency without sacrificing sustained throughput.
- WireGuard health must be based on actual tunnel liveness, not just whether a utun interface exists.
- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.

## Challenges and Resolutions

- Challenge: WireGuard suddenly stopped working without a recent config edit.
  - Resolution: Identified the stale-interface failure mode and installed a health-checking wrapper that restarts the tunnel when ping/handshake checks fail.

- Challenge: The v3 branch had accumulated many plausible scheduler changes but live throughput was worse than prior baselines.
  - Resolution: Built and ran clean `master` on the same Mac/data/tunnel path, confirmed it was materially better, and reverted the broad v3 scheduler changes.

- Challenge: The latest peer-load scheduler change improved request tail latency but not enough to meet the throughput target.
  - Resolution: Kept it as a stabilization candidate and moved the next benchmark step to explicit earlier-commit testing.

- Challenge: Clean remote master build failed because `protoc` was not on the non-interactive SSH PATH.
  - Resolution: Confirmed Homebrew protobuf was already installed and rebuilt with `/usr/local/bin` on PATH.

## Dead Code and Obsolescence Cleanup

- Inspected the historical scheduler changes in `crates/logex-sync/src/engine/anchored.rs`, `crates/logex-sync/src/engine/mod.rs`, and `crates/logex-sync/src/p2p/peer_manager/requests.rs`.
- Removed the obsolete broad v3 scheduler delta from the branch by restoring the proven master implementation before adding the narrower peer-load change.
- No unrelated production code was removed.

## Git Workflow

- Current branch: `perf/historical-sync-throughput-v3`
- New branch created this run: no
- Commits made during this run: pending commit for peer-load scheduler stabilization and roadmap update
- Pull request status: not created
- Merge status: not merged
- Blockers: none for local code validation; performance target still requires longer remote warm-up and benchmarking.

## Known Issues or Risks

- Current remote run is healthier than the regressed v3 branch, but has not recovered the retained June 15 sustained throughput.
- The 800k+ logs/sec peak target and higher sustained-throughput baseline have not yet been revalidated on the repaired tunnel.
- Earlier commit tests must use isolated test data dirs or carefully verified compatibility so old storage code does not mutate the current storage-fix data.
- The branch is not ready for PR/merge until validation passes and a longer remote benchmark confirms a measured improvement over the selected baseline.
