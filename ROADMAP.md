# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `fix/historical-segment-coalescing`.

The Mac mini is directly reachable again through Tailscale at `ssh gremlinmaster@100.64.47.113`. The VPS WireGuard inbound forwarding remains configured for dashboard and P2P ports, but a system-wide WireGuard full tunnel conflicts with reliable Tailscale management on this macOS host because Tailscale's control/DERP underlay traffic is also captured by the full-tunnel default routes. The safe current operating mode is Tailscale for management plus VPS inbound forwarding; making LogEx egress exclusively through the VPS needs a per-service routing design or a VM/container boundary rather than host-wide default-route replacement.

The remote LogEx client did not crash. It stopped cleanly on the low-disk safety guard after `/Volumes/SSD 4TB/LogEx` dropped below the 10 GiB free-space threshold. The active data directory contains about 6.98B rows, but it expanded to roughly 1.5 TiB because sparse historical ranges were persisted as many small segment directories.

## Completed Since Last Run

- Restored direct Tailscale access to the Mac mini and verified `ssh gremlinmaster@100.64.47.113` works.
- Diagnosed WireGuard/Tailscale coexistence:
  - Host-wide WireGuard `0.0.0.0/1` and `128.0.0.0/1` routes make the Mac egress as the VPS, but they also break Tailscale health and direct management access.
  - Routing `100.64.0.0/10` to Tailscale is necessary but not sufficient, because Tailscale's own relay/control traffic uses normal internet destinations.
  - The route-maintenance daemon that kept re-adding the full-tunnel routes is disabled.
- Diagnosed the LogEx stop as a graceful low-disk shutdown, not data corruption.
- Identified the storage-footprint bug: sparse historical batches created thousands of tiny sealed segments, causing high filesystem allocation overhead on the external disk.
- Added durable sparse historical segment coalescing:
  - Historical writes now append sparse batches into an active historical staging segment.
  - Full or wide staging segments are finalized and compacted.
  - Background compaction and sealed query indexing skip the active historical staging segment.
  - Historical sync finalizes the staging segment once genesis is reached.
- Added tests for sparse historical coalescing and active-staging index exclusion.

## Remaining TODOs

1. Choose and implement safe VPS egress for LogEx, if full VPS identity is still required.
   - Reason: Host-wide WireGuard full tunneling breaks Tailscale management. LogEx can currently use VPS inbound forwarding, but outbound traffic remains local unless a per-service routing boundary is added.
   - Completion criteria: Either accept VPS inbound forwarding plus local outbound as the operating model, or implement and validate a per-service/VM routing design where LogEx egresses through the VPS while Tailscale remains healthy.

2. Recover remote runtime safely.
   - Reason: The current remote data directory is too full to restart LogEx reliably.
   - Completion criteria: Either reclaim/reset the oversized data directory after approval or start from a fresh directory with the coalescing fix, then verify LogEx stays up on port `18683` and follows live head.

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

## Challenges and Resolutions

- Challenge: Direct Tailscale SSH was unavailable after full-tunnel route changes.
  - Resolution: Reached the Mac through the Raspberry Pi, removed the host-wide WireGuard half-default routes, restored `100.64.0.0/10` through Tailscale, disabled the route-maintenance daemon, and verified direct Tailscale SSH recovered.

- Challenge: The VPS full tunnel appeared to work for public egress but broke Tailscale.
  - Resolution: Confirmed Mac egress changed to `157.245.195.72` only while Tailscale health degraded. Recovered management access and left the system in the stable split state.

- Challenge: LogEx was down on the remote Mac mini.
  - Resolution: Confirmed it exited via the low-disk guard: free space fell just below the 10 GiB threshold.

- Challenge: The data directory used far more disk than expected.
  - Resolution: Found the active catalog has plausible total rows but too many tiny historical segment directories. Implemented durable sparse historical segment coalescing to prevent this on future runs.

## Dead Code and Obsolescence Cleanup

- Inspected the storage, background indexer, and historical ingest paths touched by the segment-coalescing change.
- No dead production code was removed in this pass; the change replaces the old one-sealed-segment-per-historical-write behavior.
- Existing oversized remote data likely needs reset or an explicit migration/compaction strategy; this was not deleted without approval.

## Git Workflow

- Current branch: `fix/historical-segment-coalescing`
- New branch created this run: yes
- Commits made during this run: `0e55883 fix: coalesce sparse historical segments`
- Pull request status: not created
- Merge status: not merged
- Blockers: remote LogEx cannot restart safely until disk space is reclaimed or data is reset; full VPS egress needs a per-service/VM routing decision because host-wide full tunneling conflicts with Tailscale.

## Known Issues or Risks

- Existing remote data remains oversized; the coalescing fix prevents recurrence on a fresh or future run but does not rewrite the existing 1.5 TiB data directory.
- Peer count cannot be re-evaluated until LogEx is restarted with enough free disk.
- Historical sync performance work should resume only after the networking and storage baseline is healthy.
