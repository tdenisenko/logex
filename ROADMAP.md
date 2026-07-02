# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Current branch: `docs/finalize-p2p-download-roadmap`.

The main dashboard now reports Execution Layer P2P download throughput instead of an ETA tile. The metric is measured from successful decoded block-body and receipt payloads returned by peers, then shown as Mbps in the UI.

## Completed Since Last Run

- Added execution P2P decoded payload byte-rate tracking for successful body and receipt responses.
- Exposed `p2p_download_bytes_per_sec` and `p2p_downloaded_payload_bytes` under `execution_network` in `/status`.
- Replaced the main dashboard ETA tile with a `P2P download` Mbps tile.
- Added the same throughput and cumulative decoded payload metric to advanced dashboard details.
- Removed obsolete dashboard ETA formatting helpers that no longer had call sites.

## Remaining TODOs

No remaining TODOs for the dashboard P2P download-rate task.

## Design Decisions

- Use decoded P2P payload bytes rather than raw socket bytes.
  - Why: Reth's current network handle does not expose per-peer transport byte counters, while decoded body/receipt responses are available at the request accounting layer.
  - Alternatives considered: estimate throughput from block/log progress or add invasive transport hooks. Progress-derived rates are less direct, and transport hooks would be much higher risk for this UI change.
  - Tradeoff: The metric represents useful Ethereum payload throughput, not encrypted TCP overhead or protocol framing bytes.

- Expose bytes/sec in the API and format Mbps in the browser.
  - Why: Integer bytes/sec keeps the Rust status type simple and precise while preserving the dashboard wording the user requested.
  - Alternatives considered: expose floating-point Mbps directly. That would require weakening the existing `ExecutionNetworkStatus` equality semantics.
  - Tradeoff: API consumers convert to their preferred network unit.

- Decay stale P2P download rate to zero after a short freshness window.
  - Why: The dashboard should not show an old high throughput number when no body or receipt payloads have arrived recently.
  - Alternatives considered: keep the last EWMA indefinitely. That would be misleading during stalls or idle periods.
  - Tradeoff: Very bursty request windows may briefly show zero between payload samples.

## Challenges and Resolutions

- Challenge: The previous ETA field was computed from sync progress and did not reflect actual P2P download activity.
  - Resolution: Added request-layer byte accounting for body and receipt responses and wired it into the dashboard.
  - Remaining: No known issue for this task.

## Dead Code and Obsolescence Cleanup

- Inspected the dashboard formatting helpers after replacing the ETA tile.
- Removed unused ETA and completion-time formatting functions from the dashboard script.
- Checked request accounting tuple usage and converted it to typed structs so payload bytes are carried consistently.
- No additional safe removal was identified.

## Git Workflow

- Current branch: `docs/finalize-p2p-download-roadmap`.
- New branch created: yes, to remove stale post-merge roadmap state.
- Commits made during this run:
  - `9dc8b5e5 feat: show p2p download throughput`
- Pull request status: PR #99 was created, checks passed, and it was merged.
- Merge status: merged into `master` as `e1180489`.
- Blockers: none known.

## Known Issues or Risks

- `p2p_download_bytes_per_sec` measures decoded Ethereum body/receipt payloads from successful responses, not raw encrypted network interface throughput.
