# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput-v3`.

The Mac mini WireGuard/VPS path is healthy again after replacing the stale-interface wrapper with a health-checking LaunchDaemon script. The wrapper now treats a tunnel as healthy only if the VPS tunnel IP responds or the WireGuard handshake is recent; otherwise it restarts the tunnel and restores the full-tunnel routes while preserving LAN access.

Historical sync performance work has been reset to the proven `master` scheduler baseline. A same-data A/B test on the Mac mini showed the v3 scheduler branch regressed to roughly 15k-42k logs/sec with 11-20 serving peers, while a clean `master` build reached roughly 60k-99k logs/sec with only 4-6 serving peers and shorter body/receipt plans. The remote client is currently running the clean `master` build from `/Users/gremlinmaster/logex-master-test` against `/Volumes/SSD 4TB/LogEx` for continued warm-up observation.

## Completed Since Last Run

- Diagnosed the WireGuard outage as a stale utun/routes state: the interface existed, but the UDP path/handshake was stale, so the old wrapper did not force a restart.
- Confirmed the repaired tunnel has working VPS egress, tunnel ping, DNAT/forwarding counters, and dashboard access through the VPS.
- Built a clean `master` binary on the Mac mini after fixing the SSH PATH for Homebrew `protoc`.
- Restarted LogEx gracefully in tmux without resetting the data directory and benchmarked `master` against the same data/tunnel path.
- Reverted the v3 scheduler code changes back to the `master` scheduler/storage baseline because live A/B evidence showed the newer branch was a throughput regression.
- Validated the restored baseline with `cargo fmt --all -- --check`, `cargo test -p logex-sync`, `cargo clippy -p logex-sync --all-targets -- -D warnings`, and `cargo check --workspace`.

## Remaining TODOs

1. Re-establish high-throughput historical sync from the master scheduler baseline.
   - Reason: Earlier retained runs reached 800k-970k logs/sec bursts; the fresh master A/B is healthier than v3 but still needs warm peers and further verification.
   - Completion criteria: With the repaired VPS tunnel, run long enough to reach normal peer counts, confirm whether dense ranges return to 800k+ logs/sec peaks, and keep only changes that improve sustained logs/sec and ETA versus the master baseline.

2. Identify the remaining bottleneck if throughput stays below target after peer warm-up.
   - Reason: The goal is to reduce full historical sync toward the 4-hour target without relying on short spikes.
   - Completion criteria: Use body/receipt plan latency, peer count, serving-peer mix, CPU, memory, disk, and network observations to prove whether the bottleneck is peer tail latency, local processing, or infrastructure before changing code.

3. Match production-client P2P behavior more closely.
   - Reason: The target is Geth/Nethermind-class peer retention and sync performance.
   - Completion criteria: Audit relevant Geth/Nethermind peer scoring, request scheduling, timeout, and peer-retention behavior; implement compatible changes only when benchmarks show they beat the restored baseline.

4. Complete release hardening.
   - Reason: Production readiness depends on verification safety, restart safety, and predictable operations.
   - Completion criteria: Smokes or tests cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a clean full-sync release-candidate run.

## Design Decisions

- Keep the `master` body/receipt scheduler as the performance baseline until a live benchmark proves a replacement is better.
- Do not keep v3 scheduler experiments that regress throughput, even if they appear theoretically cleaner.
- WireGuard health must be based on actual tunnel liveness, not just whether a utun interface exists.
- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.

## Challenges and Resolutions

- Challenge: WireGuard suddenly stopped working without a recent config edit.
  - Resolution: Identified the stale-interface failure mode and installed a health-checking wrapper that restarts the tunnel when ping/handshake checks fail.

- Challenge: The v3 branch had accumulated many plausible scheduler changes but live throughput was worse than prior baselines.
  - Resolution: Built and ran clean `master` on the same Mac/data/tunnel path, confirmed it was materially better, and reverted the branch code to the `master` scheduler baseline.

- Challenge: Clean remote master build failed because `protoc` was not on the non-interactive SSH PATH.
  - Resolution: Confirmed Homebrew protobuf was already installed and rebuilt with `/usr/local/bin` on PATH.

## Dead Code and Obsolescence Cleanup

- Inspected the historical scheduler changes in `crates/logex-sync/src/engine/anchored.rs`, `crates/logex-sync/src/engine/mod.rs`, and `crates/logex-sync/src/p2p/peer_manager/requests.rs`.
- Removed the obsolete v3 scheduler delta from the branch by restoring the proven master implementation.
- No unrelated production code was removed.

## Git Workflow

- Current branch: `perf/historical-sync-throughput-v3`
- New branch created this run: no
- Commits made during this run: pending commit for restored scheduler baseline and roadmap update
- Pull request status: not created
- Merge status: not merged
- Blockers: none for local code validation; performance target still requires longer remote warm-up and benchmarking.

## Known Issues or Risks

- Current remote run is healthier but has not yet warmed to the 80-90 peer profile seen in earlier successful runs.
- The 800k+ logs/sec target has not yet been revalidated on the repaired tunnel.
- The branch is not ready for PR/merge until validation passes and a longer remote benchmark confirms the restored baseline or a measured improvement.
