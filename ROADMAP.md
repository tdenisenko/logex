# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `fix/historical-segment-coalescing`.

The Mac mini is now intentionally managed through the Raspberry Pi path (`ssh pi-remote` then `ssh gremlinmaster@192.168.50.44`) while the host-wide WireGuard full tunnel is enabled for LogEx testing. Tailscale is stopped on the Mac because the full tunnel conflicts with reliable Tailscale management on this macOS host. The VPS WireGuard inbound forwarding remains configured for dashboard and P2P ports, and Mac public egress has been verified as `157.245.195.72` in this fallback mode.

The remote LogEx client did not crash. It stopped cleanly on the low-disk safety guard after `/Volumes/SSD 4TB/LogEx` dropped below the 10 GiB free-space threshold. The active data directory contained about 6.98B rows, but it expanded far beyond the expected footprint because sparse historical ranges were persisted as many small segment directories. The oversized data directory has been reset while preserving peer files and node discovery secrets, the rebuilt binary is running in a detached `screen` session, and `/status` responds through the VPS on port `18683`.

## Completed Since Last Run

- Restored direct Tailscale access to the Mac mini and verified `ssh gremlinmaster@100.64.47.113` works.
- Diagnosed WireGuard/Tailscale coexistence:
  - Host-wide WireGuard `0.0.0.0/1` and `128.0.0.0/1` routes make the Mac egress as the VPS, but they also break Tailscale health and direct management access.
  - Routing `100.64.0.0/10` to Tailscale is necessary but not sufficient, because Tailscale's own relay/control traffic uses normal internet destinations.
  - The fallback mode disables Tailscale on the Mac and uses the Raspberry Pi route for management while full WireGuard routing is active.
- Diagnosed the LogEx stop as a graceful low-disk shutdown, not data corruption.
- Started a fresh remote reset that preserves EL/CL known peers and discovery secrets while deleting stale chain/log/index data.
- Rebuilt the remote Mac binary with the sparse historical segment coalescing fix.
- Started a fresh Mac run in full-VPS mode using the rebuilt binary:
  - Data dir: `/Volumes/SSD 4TB/LogEx`
  - Dashboard: `0.0.0.0:18683` with HTTP basic auth.
  - NAT advertisement: `extip:157.245.195.72`.
  - Initial public `/status` check succeeded through the VPS.
- Identified the storage-footprint bug: sparse historical batches created thousands of tiny sealed segments, causing high filesystem allocation overhead on the external disk.
- Added durable sparse historical segment coalescing:
  - Historical writes now append sparse batches into an active historical staging segment.
  - Full or wide staging segments are finalized and compacted.
  - Background compaction and sealed query indexing skip the active historical staging segment.
  - Historical sync finalizes the staging segment once genesis is reached.
- Added tests for sparse historical coalescing and active-staging index exclusion.

## Remaining TODOs

1. Monitor the fresh Mac benchmark.
   - Reason: The reset and restart are complete, but peer retention and historical throughput need a longer run before judging the storage fix and network mode.
   - Completion criteria: Long-run peer count warms toward the expected range, historical sync remains stable, disk usage grows according to the new coalesced segment layout, and no low-disk or networking regressions appear.

2. Recover remote runtime safely.
   - Reason: The current remote data directory is too full to restart LogEx reliably.
   - Completion criteria: Keep the fresh run online, verify it follows live head, and collect enough storage growth data to confirm sparse segment coalescing prevents the previous bloat.

3. Resume historical-sync performance work after networking/storage recovery.
   - Reason: The last full sync still took close to 24 hours; target is 4 hours.
   - Completion criteria: Run a fresh benchmark with healthy peer routing and the segment-coalescing fix, identify the current bottleneck, and only keep changes that materially improve sustained wall-clock sync time.

4. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a live release-candidate run from a clean data directory.

## Design Decisions

- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.
- Checkpoints are accepted only when recent relative to a checkpoint-sync endpoint. Persisted state that is too stale requires a fresh recent checkpoint.
- Sparse historical writes are now coalesced in durable storage instead of only in memory, so verified rows remain crash-safe while segment count stays bounded.
- The active historical staging segment is query-visible but excluded from background compaction and sealed query indexing until finalized.
- WireGuard full-tunnel routing must preserve Tailscale’s `100.64.0.0/10` route through the Tailscale interface, not through the VPS tunnel.
- Host-wide WireGuard full tunneling is not safe on the Mac mini when Tailscale is the required management path; Tailscale also needs non-`100.64.0.0/10` underlay reachability for control and DERP traffic.
- For the current performance run, prefer deterministic VPS egress over direct Tailscale management and use the Raspberry Pi path for SSH.

## Challenges and Resolutions

- Challenge: Direct Tailscale SSH was unavailable after full-tunnel route changes.
  - Resolution: Reached the Mac through the Raspberry Pi, removed the host-wide WireGuard half-default routes, restored `100.64.0.0/10` through Tailscale, disabled the route-maintenance daemon, and verified direct Tailscale SSH recovered.

- Challenge: The VPS full tunnel appeared to work for public egress but broke Tailscale.
  - Resolution: Confirmed Mac egress changed to `157.245.195.72` only while Tailscale health degraded. User approved disabling Tailscale if needed, so the Mac is now in full-tunnel mode and managed through the Raspberry Pi.

- Challenge: LogEx was down on the remote Mac mini.
  - Resolution: Confirmed it exited via the low-disk guard: free space fell just below the 10 GiB threshold.

- Challenge: The data directory used far more disk than expected.
  - Resolution: Found the active catalog has plausible total rows but too many tiny historical segment directories. Implemented durable sparse historical segment coalescing to prevent this on future runs and started a fresh reset to validate the fix.

- Challenge: `tmux` is unavailable on the Mac mini.
  - Resolution: Started LogEx in a detached `screen` session with logs under `/Users/gremlinmaster/logex-src/run/`. This is an operational fallback for the current host; install `tmux` later if persistent tmux workflow is still preferred.

## Dead Code and Obsolescence Cleanup

- Inspected the storage, background indexer, and historical ingest paths touched by the segment-coalescing change.
- No dead production code was removed in this pass; the change replaces the old one-sealed-segment-per-historical-write behavior.
- Existing oversized remote data likely needs reset or an explicit migration/compaction strategy; this was not deleted without approval.

## Git Workflow

- Current branch: `fix/historical-segment-coalescing`
- New branch created this run: yes
- Commits made during this run: `0e55883 fix: coalesce sparse historical segments`, `2d40157 docs: update remote network recovery status`
- Pull request status: not created
- Merge status: not merged
- Blockers: no active blocker for the fresh run; longer observation is needed before concluding the storage fix and network mode.

## Known Issues or Risks

- Peer count and throughput still need longer observation on the fresh run.
- Direct Tailscale management is unavailable by design during full-tunnel testing; use the Raspberry Pi path until the run is over or the network mode changes.
- Historical sync performance work should resume only after the networking and storage baseline is healthy.
