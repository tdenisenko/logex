# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `fix/historical-segment-coalescing`.

The Mac mini is reachable through the Raspberry Pi at `ssh pi-remote` then `ssh gremlinmaster@192.168.50.44`, but direct Tailscale access is still down because the Mac is logged out of Tailscale and the Tailscale control-plane dial is failing over IPv6 despite the host having no IPv6 router. A repair script has been copied to `~/logex-gateway/repair-tailscale-wireguard-coexistence.sh` on the Mac; it requires an interactive sudo run.

The remote LogEx client did not crash. It stopped cleanly on the low-disk safety guard after `/Volumes/SSD 4TB/LogEx` dropped below the 10 GiB free-space threshold. The active data directory contains about 6.98B rows, but it expanded to roughly 1.5 TiB because sparse historical ranges were persisted as many small segment directories.

## Completed Since Last Run

- Diagnosed direct Tailscale failure:
  - Mac Tailscale state is `NeedsLogin`.
  - WireGuard was routing `100.64.0.0/10` through the VPS tunnel, which breaks Tailscale peer routing.
  - Tailscale control-plane login is also blocked by IPv6 dialing with no IPv6 route.
- Diagnosed the LogEx stop as a graceful low-disk shutdown, not data corruption.
- Identified the storage-footprint bug: sparse historical batches created thousands of tiny sealed segments, causing high filesystem allocation overhead on the external disk.
- Added durable sparse historical segment coalescing:
  - Historical writes now append sparse batches into an active historical staging segment.
  - Full or wide staging segments are finalized and compacted.
  - Background compaction and sealed query indexing skip the active historical staging segment.
  - Historical sync finalizes the staging segment once genesis is reached.
- Added tests for sparse historical coalescing and active-staging index exclusion.

## Remaining TODOs

1. Restore Mac mini Tailscale and WireGuard coexistence.
   - Reason: Direct remote control and high-peer EL networking need Tailscale management plus the VPS WireGuard full tunnel without route conflicts.
   - Completion criteria: `ssh gremlinmaster@100.64.47.113` works directly, public traffic uses the VPS, `100.64.0.0/10` routes through Tailscale, and LogEx can again warm toward the expected long-run peer count.

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

## Challenges and Resolutions

- Challenge: Direct Tailscale SSH was unavailable.
  - Resolution: Reached the Mac through the Raspberry Pi, confirmed Tailscale is logged out, and prepared a sudo repair script for IPv6/Tailscale/WireGuard route recovery.

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
- Commits made during this run: none yet
- Pull request status: not created
- Merge status: not merged
- Blockers: Tailscale repair requires an interactive sudo run on the Mac mini; remote LogEx cannot restart safely until disk space is reclaimed or data is reset.

## Known Issues or Risks

- Existing remote data remains oversized; the coalescing fix prevents recurrence on a fresh or future run but does not rewrite the existing 1.5 TiB data directory.
- The Mac mini still needs interactive Tailscale re-authentication.
- Peer count cannot be re-evaluated until Tailscale/WireGuard routing is corrected and LogEx is restarted with enough free disk.
- Historical sync performance work should resume only after the networking and storage baseline is healthy.
