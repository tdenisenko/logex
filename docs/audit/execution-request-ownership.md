# Execution request ownership

This milestone corrects execution request accounting across overlapping plans,
retirement, historical pipeline reset and replacement sessions. It does not alter
block validation or the contents of a response. Wire response correlation and
local accounting ownership are separate concerns.

## Findings

- **B4-09: reservation release could subtract another plan's capacity.** Starting
  a request consumed a shared peer/role reservation. The historical attempt kept
  the original reservation descriptor and subtracted its full count again when
  it finished. With two plans each reserving one slot, starting and finishing the
  first could leave zero reserved while the second had not started.
- **B4-10: late accounting could affect replacement work.** Historical reset
  aborted tasks, cleared counters and drained currently queued accounting.
  Aborting does not synchronously drop a task. Its later guard cleanup, success
  or failure events used only a peer ID and could modify new work. Existing
  generation checks protected returned historical outcomes, not those accounting
  events. Individual attempt retirement had the same missing ownership boundary.
- **B4-11: old session accounting could mutate a reconnected peer.** Plans retain
  a sender for the session that handled their request, but returned accounting
  used only the peer ID. After the same identity reconnected, old success/failure
  events could adjust the new session's capacity, pause, serving status or limits.
  Pinned Reth already correlates wire responses through an active session's
  request-ID map and per-request oneshot; this finding does not establish that
  an old wire response is delivered to a different new request.

## Ownership contract

Each executing plan has a unique owner and an explicitly registered ledger.
Remaining reservations and active counts belong to that owner and captured peer
session. Starting consumes only its own reservation; finishing releases only its
own active slot. Retirement removes only that owner's remaining charges and is
idempotent. Late events from retired owners have no effect on replacement work.

Use the captured Tokio session sender's channel identity to match the current
peer session, without introducing an unrelated session counter. Apply the same
session guard to deferred success/failure accounting. Valid response bytes still
proceed through the existing independent content validation path.

Register plans at execution, after refreshing peer snapshots and selecting initial
reservations. Queued, unexecuted plans must not retain registered owners. Both
historical attempts and the forward-gap path use streamed accounting; direct
returned outcomes need session matching too. Owner IDs must not wrap and be reused.

Normal completion must preserve queued terminal success/failure events. Ordered
retirement follows those events on the accounting channel. Explicit cancellation
or reset invalidates its owner immediately, since accounting and outcome messages
use separate channels and a single earlier drain cannot prove no later arrivals.
Delete retired ledger entries rather than retaining permanent tombstones. Preserve
batched success/failure application and its existing ordering. Streamed outcomes
avoid a redundant session map; direct outcomes retain captured senders until their
session-local accounting is applied.

## Existing behavior retained

Role futures are owned by their FuturesUnordered collection. Polled request guards
emit start and finish; dropping the collection drops its guards. The local guard
lifetime is sound, but did not supply ownership to the shared counters. Local
future cancellation does not prove an already submitted wire request is cancelled.

Historical outcome generation/attempt checks and the distinction between streamed
and returned accounting remain necessary. Preserve existing scheduling limits,
retry policies, response validation and ingestion publication semantics.

## Validation

Two original-code controls extract the original scalar delta helper and combine it
with its original release/reset operations. Both fail their ownership expectations:
one remaining reservation or replacement active request is incorrectly reported
as zero. These are controlled accounting sequences, not full PeerManager or wire
integration tests. Evidence is retained separately from candidate regression tests.

Eight new tests cover owner/role isolation, actual shared peer totals, duplicate
retirement, extra active attempts, reset, same-ID reconnect, streamed/direct/header
session filtering, unpolled execution drop, polled task abort and FIFO terminal
accounting. Three obsolete scalar-only tests are removed. The complete sync library
passes: 341 tests, one ignored. Final independent review approves the implementation within B4-09 through B4-11
with no remaining actionable finding. Source hashes and controls are bound in
[the validation record](baselines/2026-09-16-execution-request-ownership.json).
Source `36db2d3d` passes all seven local gates: vendor verification, formatting, workspace check, strict Clippy, 1,445 workspace tests (23 ignored), documentation tests and release node linking. All six CI jobs passed on head `49b1c56e` (run `35006614456`); PR #176 merged as `99128441` after exact head/base verification. No benchmark, throughput
percentage, additional disk write or format change is proposed.

## Follow-up dispositions

- **Receipt-count attribution:** a separately recorded finding shows an expected
  count derived from an unvalidated body can cause a different receipt peer to be
  blamed for disagreement. Resolve with authenticated source validation or
  nonfault handling of cross-source disagreement in a subsequent complete fix.
- **Send deadline:** the response timer starts after awaiting the bounded session
  command queue. Trace enclosing deadlines and a controlled full-channel fixture
  before assigning liveness severity or changing timeout behavior.
- Remaining request matching, partial responses, peer rehabilitation and resource
  limits remain in the broader EL audit. This milestone does not establish a
  global network memory bound or live-sync readiness.
