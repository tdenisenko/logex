# Roadmap

## Current Status

LogEx boots from a recent weak-subjectivity checkpoint, follows Consensus Layer head/finality over native P2P, and uses authenticated execution anchors as the pivot for Execution Layer validation. Execution Layer P2P can follow head, walk historical execution data backward from the pivot, verify headers/bodies/receipt roots without executing the EVM, and index queryable logs while the stored range expands toward genesis.

The active task branch is `feature/el-reverse-sync` / draft PR #76. The dashboard cleanup from PR #77 has been merged into this branch. The remote performance run is using one active data directory with older segment directories relocated onto the mounted `/mnt/logex-extra` volume through symlinks.

## Completed Since Last Run

- Mounted the remote 100GB volume at `/mnt/logex-extra` and moved older sealed segment directories there to keep the active run alive without keeping multiple data directories.
- Fixed storage catalog repair so symlinked segment directories remain discoverable after restart.
- Fixed dashboard storage accounting so `storage_used_bytes` follows symlinked segment directories and avoids double-counting repeated links.
- Reduced startup/query disk pressure for compacted segments by reading only selected page payloads instead of loading full column files for sparse row reads.

## Remaining TODOs

1. Reduce Execution Layer reverse-sync ETA below the production target.
   - Reason: Current remote runs are still above the sub-6-hour full-history target.
   - Completion criteria: A fresh mainnet-like run sustains sub-6-hour ETA on adequate hardware, or a documented architecture decision replaces full P2P receipt backfill with another trustless strategy.

2. Stabilize Execution Layer peer ramp and body/receipt throughput.
   - Reason: The downloader needs enough serving peers to hide request latency and keep wide fetch windows active.
   - Completion criteria: Long remote runs retain a large serving pool, keep lookahead filled, and do not regress peer retention compared with the best observed run.

3. Replace the temporary checkpoint source.
   - Reason: `--checkpoint-sync-url` still depends on an external checkpoint provider.
   - Completion criteria: LogEx has its own recent-checkpoint source or a documented multi-source verification flow, with stale checkpoint rejection aligned to consensus weak-subjectivity rules.

4. Complete release validation.
   - Reason: Consensus Layer, Execution Layer, storage, query, and UI surfaces need shared evidence for what is verified and queryable.
   - Completion criteria: End-to-end tests or smokes cover checkpoint bootstrap, live anchors, reverse Execution Layer headers/bodies/receipts, restart/resume, Merge boundary behavior, query coverage, limits, pagination, and dashboard auth behavior.

5. Harden non-HTTP query surfaces before public exposure.
   - Reason: The new dashboard password protects HTTP dashboard/status/query/JSON-RPC/WebSocket routes, but gRPC is still a separate unauthenticated listener.
   - Completion criteria: Either gRPC is bound/firewalled to trusted networks by default, gains equivalent authentication, or is explicitly disabled in deployment profiles that expose the HTTP dashboard.

## Design Decisions

- Consensus Layer sync is forward-only from a recent checkpoint; Execution Layer historical sync is responsible for walking execution data back toward genesis.
- Logs are valid only inside the verified contiguous stored range. Unsynced historical gaps remain outside query coverage.
- The dashboard keeps the query tool on the main page because querying verified logs is a primary product workflow.
- Performance charts use Chart.js rather than custom SVG path generation.
- Dashboard authentication uses HTTP Basic auth as a lightweight local/server operator control. It should be paired with localhost binding, firewalling, SSH tunneling, or TLS termination when exposed outside a trusted machine.
- Query responses keep a hard `10,000` row cap and default to `50` row pages.
- Dashboard query pagination is client-side over the loaded capped result set, so Next/Previous does not issue additional query requests.
- Storage usage metrics follow relocated segment-directory symlinks because the active deployment may span more than one mounted filesystem.

## Challenges and Resolutions

- Challenge: The dashboard had too many competing metrics and made Execution Layer/log coverage hard to interpret.
  - Resolution: The main view now shows one Execution Layer progress bar, Consensus Layer status, log range, storage, performance chart, and the query panel.

- Challenge: The query engine could be abused if the HTTP server URL is reachable by untrusted users.
  - Resolution: Added optional HTTP Basic auth for HTTP dashboard, status, query, JSON-RPC, and WebSocket endpoints while keeping `/health` public for liveness checks.

- Challenge: The remote root filesystem was close to full while the active data directory still needed to be preserved for performance testing.
  - Resolution: Mounted the additional volume, moved older sealed segments onto it, fixed symlink-aware catalog repair and storage metrics, and reduced compacted selected-row reads so startup does not scan entire column files unnecessarily.

## Dead Code and Obsolescence Cleanup

- Inspected storage startup, segment-reader, and server metric code paths affected by the remote volume split.
- Kept the symlink-based segment relocation support because it is required by the active remote run.
- No experimental Execution Layer peer-retention code was added or retained in this storage pass.

## Git Workflow

- Current branch: `feature/el-reverse-sync`
- New branch created this run: none; continuing the existing Execution Layer reverse-sync branch.
- Commits made during this run: storage-volume compatibility commit on this branch.
- Pull request status: draft PR #76 remains open for the Execution Layer production-readiness work.
- Merge status: not ready to merge; Execution Layer throughput and full-history validation remain incomplete.
- Git/GitHub blockers: none known.

## Known Issues or Risks

- HTTP Basic auth does not encrypt traffic. Use it behind localhost, a firewall, an SSH tunnel, or a TLS-terminating reverse proxy.
- gRPC remains unauthenticated and should not be exposed to untrusted networks until it is separately hardened or disabled.
- The parent Execution Layer performance branch is still above the long-term sync ETA target.
- Symlinked segment directories are a deployment compatibility path, not a replacement for a first-class multi-volume storage allocator.
