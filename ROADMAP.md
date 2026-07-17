# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Current task branch: `fix/nightly-bail-ci`.

The nightly Rust CI compatibility failure is fixed in PR #106. The fix is validated locally and the task is complete after PR #106 is merged and all non-`master` remote branches are removed.

## Completed Since Last Run

- Inspected the latest failed GitHub Actions run on `master`.
- Confirmed that Check, Clippy, and Test all failed during compilation from the same newly denied nightly Rust lint; Format passed.
- Terminated seven expression-position `eyre::bail!` invocations as statements without changing their early-return behavior.
- Validated the full workspace against the newly enforced lint and all four CI commands.
- Searched for additional expression-position `bail!` invocations and found none requiring changes.
- Opened PR #106 for the CI fix.
- Removed every non-`master` remote branch after merging PR #106.

## Remaining TODOs

No remaining TODOs for the nightly Rust CI compatibility and remote branch cleanup task.

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

## Dead Code and Obsolescence Cleanup

- Inspected every `bail!` invocation in the Rust workspace for the newly invalid expression-position pattern.
- Confirmed the remaining invocations are already statement-terminated.
- No dead files, imports, exports, dependencies, or superseded code were introduced or found in the affected request path.

## Git Workflow

- Current task branch: `fix/nightly-bail-ci`.
- Task branch `fix/nightly-bail-ci` was created from the latest `master`.
- Commits made during this run:
  - `8d02f434 fix: restore nightly CI compatibility`
- Pull request status: PR #106 was created and merged into `master`: `https://github.com/tdenisenko/logex/pull/106`.
- Remote branch cleanup: every remote branch except `master` was deleted after the merge.
- GitHub CLI authentication was expired; the connected GitHub app supplied workflow logs, PR creation, and merge operations, while authenticated SSH handled Git fetch/push operations.
- Validation run:
  - `cargo fmt --all -- --check`
  - `RUSTFLAGS=-Dsemicolon_in_expressions_from_macros cargo check --workspace`
  - `cargo check --workspace`
  - `cargo clippy --workspace -- -D warnings`
  - `cargo test --workspace`
  - `git diff --check`

## Known Issues or Risks

- Bandwidth metrics are calibrated wire-equivalent estimates, not packet captures. They should track normal sync traffic closely, but exact values can differ during peer churn, retransmits, or unrelated host traffic on the same VPS tunnel.
- The repository intentionally follows rolling nightly Rust, so future compiler changes can expose additional source incompatibilities; CI remains the guardrail.
