# WebSocket delivery continuity and snapshot lifetime

Base: PR #230 merge `a4eafd463205eeefe38d47abd7a3326c6d2cd071`.
Branch: `audit/websocket-delivery`. Implementation and eleven local gates are complete; PR #231 merged as `334ec9ef` after all six CI jobs.

## Findings and scope

- **B8-15 (P2): silent continuation after a broadcast gap.** Both raw and
  retained WebSocket loops log `Lagged` and continue. Exact-base controls use a
  channel of capacity one and two small batches: the actual loops send a later
  data frame instead of terminating the incomplete stream.
- **B8-16 (ownership optimization): acknowledgement snapshot lifetime.** The
  retained handler keeps the cloned snapshot inside its attachment for the
  socket lifetime. Serialize the acknowledgement, release the snapshot, then
  await transmission. Retained server history and detached-session identity
  remain intentional. No heap-byte or throughput measurement is claimed.

A detected gap ends the affected stream. Attempt a close frame with code `1013`
and a reconnect/reconcile diagnostic for at most one second, then release the
socket and run the existing same-instance detach guard. The deadline governs
only this terminal diagnostic; healthy data, acknowledgements and pongs retain
their existing backpressure. An ordinary pending send is not itself classified
as a failure. Broader send deadlines and shared resource limits are separate
policy concerns.

The close reason is best-effort: a stalled or broken transport may expose only
an abnormal disconnection. Clients must reconcile stored logs after losing
continuity. `Lagged(n)` counts broadcast batches, not individual log rows, and
retained history can evict rows beyond its existing 10,000-notification limit.
Reconnection does not promise a complete replay. No unbounded replay or new
global quota is introduced.

The dashboard's existing close callback overwrites diagnostic text with a
generic disconnection message. A narrow companion change displays code `1013` guidance
through the existing plain-text status helper, preserving manual disconnect and
stale-socket ownership behavior. It adds no automatic retry or list-order change.

## Validation and boundaries

Preserve exact original source and reached loopback regressions. Add finite
controls for terminal timeout/cancellation, identity-safe detach and ordinary
chronological delivery. The initial retained snapshot remains newest-first.
Five Node callback controls evaluate the actual embedded connection/status
functions: two diagnostic cases fail before the browser change, three existing
behaviors pass, and all five pass afterward. This isolated verification adds no
browser framework or application dependency; it is not a full browser audit.

Canonical reorg/removal notifications, remaining ERC20 input/event classification,
dashboard ordering/reconnect/layout, shared query memory/admission, verified
offline repair and integrated acceptance remain separate. No live chain,
production dataset, remote host or benchmark is involved.

## Final local validation

All eleven final-source local gates pass: 2,007 workspace tests, zero failures, 24 existing ignores across 37 targets, documentation checks and the release node build.

Source is `71ef3cc0c8b2669355dde499cde7bcd1b1040049`. The
[validation record](baselines/2026-09-17-websocket-delivery.json) retains exact
original/final source hashes, both original wire failures, all focused checks,
independent review and full gate logs. All 149 focused server tests pass with two
existing benchmark ignores. Five browser callback controls pass under Node
`v25.2.1`; the evidence archive includes `browser-close.test.mjs`, which can be
run with `node --test` after extraction. `LOGEX_WS_HTML` selects an alternate
HTML source, including the archived original for reproducing the two failures.
This does not add a Node requirement to Cargo or CI. Strict server Clippy and
formatting pass. Exact-head CI and merge passed; verified closure follows.

All six CI jobs passed on `863fa017` and ten Linux volume/template controls passed with verified cleanup. [PR #231](https://github.com/tdenisenko/logex/pull/231) merged as `334ec9ef`. The merge tree is identical to the tested head. B8-15–16 are closed within detected-gap termination and acknowledgement ownership scope. Reorg notifications, other ERC20 parsing/event review, shared query budgets, dashboard, verified repair and integrated acceptance remain open.
