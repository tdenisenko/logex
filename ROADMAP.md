# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Current branch: `fix/wireguard-stale-tunnel-repair`.

The nightly Rust CI compatibility failure is fixed and merged through PR #106. Both the PR and merge-triggered `master` workflows pass all four CI jobs, and `master` is the only remaining remote branch.

The Mac mini LogEx process is running from the full synced data directory and is publicly reachable through the VPS at `http://157.245.195.72:18683/`. The July 18 dashboard outage was caused by a stale WireGuard tunnel, not a LogEx process crash; after repairing the tunnel and gracefully restarting LogEx, forward indexing resumed.

## Completed Since Last Run

- Confirmed LogEx did not crash; the tmux session and process were still alive while the public dashboard was unreachable.
- Identified the outage as a stale WireGuard tunnel: VPS NAT rules were still present, but the VPS could not reach `10.66.0.2` and the peer handshake was stale.
- Repaired full VPS routing on the Mac mini and verified public dashboard access through `157.245.195.72:18683`.
- Updated the ignored local routing scripts and installed Mac LaunchDaemon so the WireGuard watchdog probes tunnel traffic and rebuilds the tunnel when an existing `utun` interface stops passing packets.
- Gracefully restarted LogEx after restoring the tunnel because the live catch-up path remained wedged from the stale network state; storage integrity passed and forward indexing resumed.

## Remaining TODOs

No remaining TODOs for the stale WireGuard tunnel recovery task.

## Design Decisions

- Fix the affected call sites instead of pinning the nightly toolchain or replacing `eyre`.
  - Why: Explicit statement termination is source-compatible, preserves behavior, and addresses the compiler rule directly.
  - Alternatives considered: Pinning an older nightly compiler or changing error-handling dependencies.
  - Tradeoff: Future `eyre::bail!` call sites must also be statement-terminated, but the repository can continue receiving nightly compiler fixes and diagnostics.

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

- Treat WireGuard interface presence as insufficient health.
  - Why: The outage showed `utun` and routes can remain present after the tunnel stops handshaking.
  - Alternatives considered: only reinstalling routes on a timer.
  - Tradeoff: The watchdog now performs a lightweight ping probe to the VPS tunnel IP before deciding the tunnel is healthy.

## Challenges and Resolutions

- Challenge: The local nightly compiler predates the GitHub runner compiler that promoted `semicolon_in_expressions_from_macros` to an error.
  - Resolution: Ran the full workspace check with `RUSTFLAGS=-Dsemicolon_in_expressions_from_macros` to reproduce the GitHub failure mode locally.
  - Remaining: None.

- Challenge: The prior dashboard metric underreported a fresh run by an order of magnitude because concurrent request completions were smoothed as one per-request EWMA.
  - Resolution: Replaced it with a rolling aggregate byte window.
  - Remaining: None known.

- Challenge: Raw decoded/in-memory payload sizes did not match VPS traffic counters.
  - Resolution: Account EL response sizes as Snappy-compressed RLPx wire-equivalent bytes and include measured lower-layer overhead factors.
  - Remaining: The value is an estimate, not a packet capture.

- Challenge: The public dashboard looked like a client outage around 04:00 local time, but the LogEx process was still alive.
  - Resolution: Compared the LAN status endpoint, VPS WireGuard state, and tunnel reachability; the failure matched a stale WireGuard tunnel rather than a LogEx crash.
  - Remaining: None.

- Challenge: The LaunchDaemon preserved routes and interface state but did not verify actual tunnel traffic.
  - Resolution: The installed Mac watchdog and local ignored ops scripts now rebuild WireGuard when tunnel probes fail.
  - Remaining: None.

## Dead Code and Obsolescence Cleanup

- Inspected the LogEx process, VPS NAT rules, WireGuard daemon state, and local routing scripts.
- No production Rust code was changed.
- The modified routing scripts live under ignored `local-ops/` because they contain machine-specific operational details.

## Git Workflow

- Branch: `fix/wireguard-stale-tunnel-repair`.
- New branch created from `master` for the operational recovery note.
- Commits made during this run:
  - `docs: record WireGuard tunnel recovery`
- Pull request status: pending.
- Merge status: pending.
- Validation run:
  - `sh -n local-ops/logex-full-vps-routing.sh`
  - `sh -n local-ops/logex-dashboard-only-routing.sh`
  - Mac LaunchDaemon inspection for the installed tunnel probe/rebuild logic
  - Public `/status` checks through `157.245.195.72:18683`
  - `git diff --check`

## Known Issues or Risks

- Bandwidth metrics are calibrated wire-equivalent estimates, not packet captures. They should track normal sync traffic closely, but exact values can differ during peer churn, retransmits, or unrelated host traffic on the same VPS tunnel.
- The repository intentionally follows rolling nightly Rust, so future compiler changes can expose additional source incompatibilities; CI remains the guardrail.
- `local-ops/` is intentionally ignored by Git, so machine-specific routing scripts are not versioned unless explicitly force-added.
- The remote Mac mini checkout used for this operational run is a dirty `perf/historical-sync-throughput-v3` tree, not a clean `master` checkout; evaluate any remote-only warnings against that state before treating them as production regressions.
