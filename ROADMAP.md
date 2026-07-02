# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Current branch: `feature/dashboard-p2p-bandwidth`.

This branch updates the dashboard bandwidth tile and `/status` payloads so P2P bandwidth includes download and upload rates across execution sync, historical execution sync, and consensus sync.

## Completed Since Last Run

- Renamed the dashboard metric from `P2P download` to `P2P bandwidth`.
- Added aggregate dashboard display for download and upload throughput in Mbps.
- Added consensus-layer P2P payload download/upload rates and cumulative totals to `/status`.
- Added execution-layer upload accounting for outbound requests and data served from the local serve cache.
- Included execution header responses in the existing execution download metric.
- Added server/status and serve-cache test coverage for the new fields.

## Remaining TODOs

- Merge the bandwidth dashboard task after review/CI.
  - Reason: The code is implemented and locally validated, but the branch still needs the normal PR merge workflow.
  - Completion criteria: PR is created, checks pass, and the branch is merged into `master`.

## Design Decisions

- Track payload-level P2P bandwidth instead of OS network-interface throughput.
  - Why: The dashboard should reflect data LogEx processes, not unrelated host traffic or encrypted transport overhead.
  - Alternatives considered: polling system network counters. That would include non-LogEx traffic and vary by OS.
  - Tradeoff: Mbps is application payload throughput, not exact TCP wire bytes.

- Aggregate EL and CL bandwidth in the dashboard instead of replacing individual network-layer fields.
  - Why: Existing API consumers can still inspect layer-specific data, while the main UI shows the user-facing total.
  - Alternatives considered: a single top-level bandwidth field. That would hide useful debugging detail.
  - Tradeoff: UI aggregation must handle missing per-layer fields as zero.

- Use short rolling windows for CL and EL upload rates.
  - Why: Upload events are bursty, especially when serving peers or sending small RPC requests.
  - Alternatives considered: cumulative average since startup. That would be too stale for a live dashboard.
  - Tradeoff: The displayed upload rate drops to zero when no recent upload payloads were observed.

## Challenges and Resolutions

- Challenge: The prior dashboard metric only showed execution-layer download payloads.
  - Resolution: Added upload fields, CL bandwidth fields, and aggregate UI formatting.
  - Remaining: None known.

- Challenge: Execution upload is served partly through Reth provider callbacks.
  - Resolution: Instrumented `ServeCacheProvider` return paths and outbound request helpers to account for local EL P2P upload payloads.
  - Remaining: Payload sizes are estimates of decoded protocol payloads, not encrypted TCP bytes.

## Dead Code and Obsolescence Cleanup

- Inspected the old `P2P download` UI labels and status fields.
- Replaced obsolete dashboard copy with `P2P bandwidth`.
- Kept existing download fields for compatibility and added upload fields rather than renaming API keys.
- No files were removed.

## Git Workflow

- Current branch: `feature/dashboard-p2p-bandwidth`.
- New branch created from latest `master`.
- Commits made during this run: pending.
- Pull request status: pending.
- Merge status: pending.
- Validation run:
  - `cargo fmt --check`
  - `cargo check -p logex-types -p logex-cl -p logex-sync -p logex-server`
  - `cargo test -p logex-server`
  - `cargo test -p logex-sync p2p::serve_cache`
  - `cargo test -p logex-cl network::tests`
  - `cargo clippy -p logex-types -p logex-cl -p logex-sync -p logex-server -- -D warnings`

## Known Issues or Risks

- Bandwidth metrics report decoded application payload bytes, not full encrypted TCP wire bytes or host network-interface counters.
- The execution upload metric estimates served receipt payloads from encoded receipts and computed blooms; it is intended for dashboard throughput visibility, not byte-perfect packet accounting.
