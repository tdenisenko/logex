# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Current branch: `fix/p2p-bandwidth-accounting`.

The dashboard bandwidth tile and `/status` payloads show P2P download and upload rates across execution sync, historical execution sync, and consensus sync. The current branch fixes the execution bandwidth estimator so the dashboard tracks VPS-observed network traffic closely during high-throughput historical sync.

## Completed Since Last Run

- Verified the dashboard bandwidth metric against fresh Mac Mini runs through the VPS tunnel.
- Replaced per-request EL download EWMA accounting with a rolling aggregate byte window so concurrent peer downloads are summed correctly.
- Replaced decoded/in-memory EL body and receipt sizing with RLPx Snappy wire-equivalent estimates.
- Added execution upload visibility for TCP ACK-side traffic based on measured download throughput.
- Calibrated the estimator against VPS tunnel counters; the final verification sample averaged 268.9 Mbps reported vs 271.9 Mbps observed downstream.
- Removed the temporary Mac Mini bandwidth test data directories and restarted the original full-sync run from `/Volumes/SSD 4TB/LogEx-full-sync-20260702-0837`.

## Remaining TODOs

No remaining code TODOs for the dashboard P2P bandwidth task. PR #105 is open for review, CI, and merge.

## Design Decisions

- Track estimated wire-equivalent P2P bandwidth instead of decoded payload throughput.
  - Why: The dashboard is used to compare LogEx sync traffic with VPS/router charts, so decoded payload bytes underreport and memory-size estimates overreport.
  - Alternatives considered: OS network counters and decoded payload counters. OS counters include unrelated host traffic and vary by platform; decoded payload counters do not match real network charts.
  - Tradeoff: The estimator is calibrated to RLPx/Snappy/TCP/WireGuard behavior and should be close for sync traffic, but it is still an estimate rather than packet-perfect accounting.

- Aggregate EL and CL bandwidth in the dashboard instead of replacing individual network-layer fields.
  - Why: Existing API consumers can still inspect layer-specific data, while the main UI shows the user-facing total.
  - Alternatives considered: a single top-level bandwidth field. That would hide useful debugging detail.
  - Tradeoff: UI aggregation must handle missing per-layer fields as zero.

- Include an ACK-side upload estimate for execution downloads.
  - Why: The client sends very small request payloads while TCP/WireGuard ACK traffic is visible on network charts; without this, upload appeared near zero during heavy downloads.
  - Alternatives considered: reporting request payloads only. That was technically payload-accurate but misleading for user-facing bandwidth.
  - Tradeoff: Upload is estimated from download traffic unless LogEx is serving larger payloads to peers.

## Challenges and Resolutions

- Challenge: The prior dashboard metric underreported a fresh run by an order of magnitude because concurrent request completions were smoothed as one per-request EWMA.
  - Resolution: Replaced it with a rolling aggregate byte window.
  - Remaining: None known.

- Challenge: Raw decoded/in-memory payload sizes did not match VPS traffic counters.
  - Resolution: Account EL response sizes as Snappy-compressed RLPx wire-equivalent bytes and include measured lower-layer overhead factors.
  - Remaining: The value is an estimate, not a packet capture.

## Dead Code and Obsolescence Cleanup

- Inspected the previous payload-only bandwidth accounting path in `logex-sync`.
- Removed obsolete EL download EWMA fields and constants.
- Kept the public `/status` field names stable while updating their documented semantics to estimated wire bytes.
- No files were removed.

## Git Workflow

- Current branch: `fix/p2p-bandwidth-accounting`.
- Task branch `fix/p2p-bandwidth-accounting` was created from latest `master`.
- Commits made during this run:
  - `9a2fb483 fix: calibrate p2p bandwidth accounting`
- Pull request status: PR #105 is open: `https://github.com/tdenisenko/logex/pull/105`.
- Merge status: pending CI/review.
- Validation run:
  - `cargo fmt --check`
  - `cargo test -p logex-sync p2p::peer_manager::tests::payload_bandwidth_window`
  - `cargo check -p logex-types -p logex-sync -p logex-server`
  - `cargo clippy -p logex-types -p logex-sync -p logex-server -- -D warnings`

## Known Issues or Risks

- Bandwidth metrics are calibrated wire-equivalent estimates, not packet captures. They should track normal sync traffic closely, but exact values can differ during peer churn, retransmits, or unrelated host traffic on the same VPS tunnel.
