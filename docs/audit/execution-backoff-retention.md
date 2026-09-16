# Execution retry history and dial capacity

## Findings

**B4-43, moderate — disconnected retry history has no entry bound.** Saturation
and receipt-quarantine maps expired entries by time alone. A finite sequence of
4,097 distinct peer IDs retained 4,097 entries in each map; session/dial limits
are not a bound on distinct IDs remembered over time. This demonstrates missing
retention limits, not a measured production memory-exhaustion event.

**B4-44, moderate — a disconnected peer's cooldown consumes a dial slot.**
`requeue_disconnected_peer` inserted a `SubmittedDial` despite submitting no
connection. The same map determines open slots and the 96-dial ceiling. With
two connected peers, one disconnected peer cooling down and a fresh candidate,
a target of three submitted no connection instead of one. Churn could prevent
otherwise eligible replacements from being tried until the cooldown expired.

**B4-45, low — a later transport failure shortens receipt quarantine.** Separate
request completions could replace an existing five-minute incomplete-service
quarantine with the thirty-second transport-failure quarantine. As with role
pauses in B4-40, a new failure should preserve the later existing deadline.
Useful successful progress can still clear quarantine immediately.

## Correction and retained behavior

Each disconnected history has a 4,096-entry cap. Expired records are removed
before pressure eviction; if still full, the soonest-expiring record is replaced.
Refreshing an existing ID preserves the later deadline. Under pressure, forgotten
disconnected IDs can be tried sooner. These are temporary scheduling hints,
not permanent rejection records or substitutes for final block validation.

Active receipt restrictions live in `ActivePeer`, outside that history budget.
Disconnect transfers a future deadline into bounded history; session admission
moves it back, preserving any later restriction on an already present session.
History pressure cannot clear an active restriction. Status counts unexpired
history and active restrictions once each, and can exceed 4,096 by the number of
active restricted peers. Success and explicit forgetting clear the applicable
state through the existing paths.

Disconnected retry delays now have a separate bounded map retaining the
advertised address. Only actual submitted connections consume dial capacity.
The fifteen-second delay is unchanged. Cooldown expiry re-admits a retained hint
if the pending inventory no longer has it; connection admission and stronger
saturation backoff remove the delay. Expiration metrics count actual submitted
dials, not cooldowns. Newer pending hints still precede older submitted/retry
hints, and an observed remote socket remains distinct from an advertised address.

A short transport quarantine does **not** erase a previously productive restart
hint. Restart hints are optional reconnection candidates, not permission to issue
receipt requests during an active restriction. New reachable hints remain
excluded while restricted. Incomplete-service quarantine explicitly removes a
productive hint, as before. The earlier rehabilitation document now states this
distinction precisely.

No timer, per-block work, disk write, peer-file format change or new public setting
is introduced. Normal updates use map lookups; full-history insertion scans at
most the configured entry cap, and status scans bounded history. This is an entry
bound, not a claim about allocator capacity or global process memory. No ingestion
throughput benchmark or speedup claim is made.

## Reproduction and validation

Five corrected controls ran against unchanged production at `840338b3`: the
restart-hint preservation control passed; both history limits, dial-slot
availability and quarantine deadline preservation failed. The first draft had
incorrectly expected transport quarantine to remove productive restart hints.
Source/history review rejected that oracle and reverted the proposed filter before
commit. Both the original and corrected evidence remain recorded; the rejected
oracle is not counted as a bug. An earlier capacity reproduction also ran with
unchanged dial/requeue functions but other candidate changes present; the clean
baseline run supersedes it as the main evidence.

Twelve final controls cover those paths plus pressure versus active restrictions,
expiry/refresh ordering, disconnect/reconnect ownership, replacement sessions,
actual temporary peer-cache reload, status expiry, forgetting, bounded cooldowns,
and preservation of advertised hints after pending eviction. Strengthening the
last expiry control exposed a candidate-only failure to requeue an evicted pending
hint; the common expiration-admission path now preserves it. That failed run is
retained separately from original-code findings.

All **367 focused P2P tests pass** (one ignored component workload). Fixtures use
dormant localhost listeners, synthetic IDs and temporary files; no peer connections
or service tasks are started. Deadlines use explicit `std::Instant` values with no
sleep or claim that paused Tokio time controls the production clock.

Implementer review traced all history insertions/removals, session ownership,
pending/submitted/cooldown hint priority, capacity accounting, persistence and
success paths. The old combined suppression name, duplicated active quarantine
map entries and obsolete dual-state fixture were replaced. Existing request
selection, validation, backoff durations and hint policies remain necessary.
No independent review, full-node connectivity test, Mac mini work or external
volume access is claimed. Outgoing response retention and other audit batches
remain open.

All eight local gates pass on `38ee2c73`: vendor integrity, workspace and
patched-vendor formatting, all-target check, strict Clippy, 1,636 workspace tests
(24 ignored), documentation tests and release build. PR/CI/merge remain pending.

[Validation record](baselines/2026-09-16-execution-backoff-retention.json).
