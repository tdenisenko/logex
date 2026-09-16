# Execution peer backoff and rehabilitation

## Findings

**B4-40, low — a later failure can shorten an active request pause.** Repeated
timeouts scale the existing eight-second role pause, capped at sixty seconds.
A separate in-flight request completing with a transport failure unconditionally
replaced that deadline with a fresh eight-second pause. Severity coalescing only
applies within one feedback batch, so it does not protect separate completions.
The result can be premature retry eligibility after repeated timeouts.

**B4-41, low — expired receipt quarantine can exclude a reconnected retry hint.**
Session admission copied a retained quarantine deadline and tested its presence,
while request selection and persisted-peer enumeration checked whether it was
still active. If reconnection preceded periodic pruning, an otherwise eligible
hint absent from the learned inventory was not added or persisted. This is a
retry-cache omission, not rejection of the connection or invalid-data acceptance.

## Corrections and preserved policy

A failure preserves the later of the current and requested body/receipt pause
deadlines. Useful progress still clears its role's pause, receipt success clears
receipt quarantine, and header errors do not pause payload roles. The existing
durations, timeout-count cap, request-limit adjustment, session ownership,
failure severity and fallback policy remain unchanged. Each new failure can
extend a pause; it cannot shorten one. Successful progress can rehabilitate a
peer immediately without waiting for the old deadline.

Session admission inherits only a quarantine deadline that remains in the future.
An active quarantine still excludes the peer from restart seeds. An expired one
no longer depends on which event-drain/pruning path ran first. Existing periodic
map cleanup remains responsible for removing stale entries; no whole-map scan
is added to session admission.

These are constant-time deadline comparisons on failure/session paths, with no
new per-block work, timer, allocation, persistence format or background task.
Restored seed admission uses the existing change-sensitive peer-cache write.
No benchmark or throughput claim is made.

## Reproduction, review and validation

Three controls ran against unchanged production at `2e547fc9`: one active-quarantine
control passed, while deadline preservation and expired-quarantine admission
failed. The first pause reproduction reaches the body role before its assertion
fails; final controls exercise both body and receipt roles through the shared
failure handler. They represent separate completion batches, not new wire traffic.

Seven final controls cover both role deadlines, extending a short pause, success
recovery, a newly failed request after expiry, header-role isolation, expired
reconnect admission with actual temporary-cache reload and active-quarantine
exclusion. All **301 peer-manager tests pass**. The fixture uses a dormant local
listener and temporary files, with no peer connections or service tasks. Deadline
fixtures use production `std::Instant`; Tokio's paused clock is not presented as
controlling that clock. There are no sleeps or wall-clock expiry waits.

Implementer review traced sequential and batched failure feedback, per-role pause
clearing, reconnect admission, restart-seed filtering and existing stale-map
cleanup. No independent review or full-node connectivity test is claimed.
The overwritten-deadline and presence-only checks were replaced; the existing
cleanup and recovery helpers remain used. Session fixture constructors are reused
rather than duplicated. No Mac mini or external storage was used.

Retained network state and outgoing/transient response allocation still need
separate dispositions. Time-based pruning alone is not a strict global entry or
resident-memory bound. This milestone does not close the whole execution batch.

All eight local gates pass on `46078c53`: vendor integrity, workspace and
patched-vendor formatting, all-target check, strict Clippy, 1,617 workspace tests
(24 ignored), documentation tests and release build. PR/CI/merge remain pending.

[Validation record](baselines/2026-09-16-execution-peer-rehabilitation.json).
