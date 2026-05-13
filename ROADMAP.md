# Roadmap

## Current Status

LogEx boots from a recent weak-subjectivity checkpoint, follows Consensus Layer head/finality over native P2P, and uses authenticated execution anchors as the pivot for Execution Layer validation. Execution Layer P2P can follow head, walk historical execution data backward from the pivot, verify headers/bodies/receipt roots without executing the EVM, and index queryable logs while the stored range expands toward genesis.

The active task branch is `ui/minimal-sync-dashboard`, branched from `feature/el-reverse-sync` / draft PR #76. This branch focuses on the dashboard surface and HTTP access controls while the parent branch remains focused on Execution Layer reverse-sync throughput.

## Completed Since Last Run

- Reworked the dashboard around the primary user-facing signals: Execution Layer sync progress, Consensus Layer status, verified log block range, storage usage, and the query tool.
- Added a page-wide sync performance chart for peers and historical blocks/sec, with 1-hour, 6-hour, and 12-hour windows.
- Moved secondary operational details into a collapsed advanced section.
- Added HTTP dashboard controls: dashboard enabled by default, `--disable-dashboard`, config-level `dashboard_enabled`, and `--dashboard-password` / `dashboard_password` for HTTP Basic authentication.

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
- Dashboard authentication uses HTTP Basic auth as a lightweight local/server operator control. It should be paired with localhost binding, firewalling, SSH tunneling, or TLS termination when exposed outside a trusted machine.
- Query responses keep a hard `10,000` row cap and default to `50` row pages.

## Challenges and Resolutions

- Challenge: The dashboard had too many competing metrics and made Execution Layer/log coverage hard to interpret.
  - Resolution: The main view now shows one Execution Layer progress bar, Consensus Layer status, log range, storage, performance chart, and the query panel.

- Challenge: The query engine could be abused if the HTTP server URL is reachable by untrusted users.
  - Resolution: Added optional HTTP Basic auth for HTTP dashboard, status, query, JSON-RPC, and WebSocket endpoints while keeping `/health` public for liveness checks.

## Dead Code and Obsolescence Cleanup

- Removed the old dense dashboard sections that duplicated sync range information or exposed low-level metrics by default.
- Kept the HTTP `/query`, JSON-RPC, WebSocket, gRPC, and storage query code paths because they remain active APIs.
- No experimental Execution Layer peer-retention or sync-performance code was changed in this UI branch.

## Git Workflow

- Current branch: `ui/minimal-sync-dashboard`
- New branch created this run: yes
- Commits made during this run: `feat: simplify dashboard and protect query routes`
- Pull request status: to be opened as a draft against `feature/el-reverse-sync` after commit/push.
- Merge status: not merged; UI approval is still required before merging this dashboard branch.
- Git/GitHub blockers: none known.

## Known Issues or Risks

- HTTP Basic auth does not encrypt traffic. Use it behind localhost, a firewall, an SSH tunnel, or a TLS-terminating reverse proxy.
- gRPC remains unauthenticated and should not be exposed to untrusted networks until it is separately hardened or disabled.
- The parent Execution Layer performance branch is still above the long-term sync ETA target.
