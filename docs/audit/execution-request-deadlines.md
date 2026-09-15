# Execution request deadlines

This milestone makes each execution request's existing timeout cover both local
queue admission and response waiting. It shares the implementation across direct
requests, paired body/receipt plans and reverse header pages.

## Finding B4-13

**Severity: medium, request liveness.** All three previous helpers awaited a send
to the bounded session queue before starting their response timer. A full live
queue without consumer progress could therefore keep a request waiting beyond
its configured duration. When space eventually became available, it received a
fresh response timeout instead of the remainder of its original budget.

Two new tests ran against the actual original header helper before production
edits. Both fail: a full queue remains pending beyond the timeout, and delayed
admission restarts the timeout. These use a one-entry local channel and Tokio's
paused clock, with no socket or live peer. This proves the local waiting defect;
it does not establish that a remote peer alone can stop the session worker.

The main paired scheduler already polls with a 45-second deadline. Direct calls,
reverse header candidate waves and the later salvage stage still depend on the
individual helper for a queue-admission bound. Shutdown, task cancellation and
channel closure remain separate ways to stop waiting.

## Implementation

One private generic helper wraps admission and response waiting in one timeout.
Existing wrappers supply their selected duration: adaptive paired request limits,
explicit caller limits or the existing ten-second default. They retain the
captured session sender and move the same decoded response values to callers.
Closed queues and response senders remain disconnected errors; response errors
remain unchanged. Deadline expiry uses the existing request-timeout outcome. This is session/request-
path availability handling (smaller batches, pause/demotion and eventual rotation),
not proof that the remote peer received the request or supplied invalid data.

This replaces three duplicate implementations and removes two redundant sender
clones. The production helper is an unboxed future with the same single oneshot
and timeout. It adds no disk write, dependency version, protocol change, content
validation pass or benchmark. The `test-util` Tokio feature is enabled only as a
dev dependency for deterministic clock control.

Cancellation before queue admission drops the unsent request. After admission,
dropping the local response receiver does **not** retract the queued or in-flight
Reth request. The session retains its own response and timeout handling. The tests
assert receiver closure and usable subsequent requests, not wire cancellation.

## Validation and scope

All 350 sync tests pass (one ignored), including six added tests covering both
original failures, custom plan deadlines, successful values, response errors,
closed channels, cancellation and reverse-header rotation past three full queues.
Final independent review passes. Full local gates and CI/merge remain pending.

Per-request timeouts do not establish a whole-batch bound. Positive partial
responses and ETH70 continuations can issue multiple exchanges. The salvage
12-second checks currently occur between awaited calls; aggregate salvage and
continuation budgets remain review items. These unchanged limits are not claimed
fixed by the admission deadline.

A separate confirmed finding, **B4-14**, concerns body and receipt bulk parallel
paths running before explicit timeout and peer-attempt limits are applied. Normal
anchored forward batches are capped at 32 (four during historical work), below the
parallel threshold. A larger checkpoint-gap fallback can reach the bypass when
its paired probe forms only one range but the full remainder forms multiple bulk
ranges. The follow-up must preserve intentional concurrency and define peer-attempt
semantics; this PR makes no parallel scheduling change.

See [the validation record](baselines/2026-09-16-execution-request-deadlines.json)
for source identities, original controls and final results. Mac-mini work and
owned temporary-file cleanup remain complete; no remote work was needed here.
