# Historical fetch worker supervision

## Finding B5-01

**Moderate, missing internal task-failure handling.** Historical header and
body/receipt fetch workers sent outcomes through channels, but the engine never
observed their task completion. If an owned worker stopped before sending a
result, its handle remained counted as active. The engine retains channel
senders, so receiver closure could not reveal the stopped worker. Existing
lookahead retries do not cover a sole stopped worker with no completed lookahead.

Four local controls against unchanged production at `cc1bb889` reproduce the
engine continuing to wait after a header or body/receipt task has stopped, both
when canceled and when returning without an outcome. Four successful-result
controls pass on that same production source. This establishes a missing failure
boundary, not an observed live outage or a demonstrated peer-triggered panic.
The existing worker bodies normally always send an outcome; normal successful
execution is not claimed to stall.

## Change and invariants

The engine checks existing fetch task handles during its normal result drains.
Completion is sampled **before** draining the channel: a successful worker sends
its result before finishing, so a result already published by a completed worker
must be considered before reporting its absence. A result from a successful
competing body/receipt attempt also retires the completed attempt. Results may
arrive before the worker itself finishes, as before.

A still-owned completed task without a matching result is joined, and its error
includes the worker kind, sequence, attempt and join failure, or an explicit
missing-outcome diagnostic for a normal return. The pipeline resets and releases
all request owners before the error propagates. This is a local task failure;
peers receive no invalid-data or timeout penalty from this diagnosis.

Obsolete work is retired before the body/receipt completion check. Header identity
includes the generation because result materialization can reset the pipeline.
Intentional reset/replacement removes handles before aborting them, so their later
completion is not mistaken for a current failure. Pending workers remain active.
All result-drain callers now propagate failures through the existing engine error
path, including the readiness probe, refill, fetch and local-work loops.

Successful request limits, retries, scheduling, ancestry checks, receipt validation,
extraction, persisted formats and coverage publication are unchanged. The normal
path scans existing active handles without introducing another collection, timer,
queue or disk operation. Joining is limited to tasks already observed finished.
There is no new per-block processing. No benchmark or percentage performance
claim is made; the broader benchmark campaign remains discontinued.

## Validation and cleanup

Sixteen lifecycle controls cover stopped and normally returned workers, results
queued before and after worker completion, pending workers, intentional reset,
old-generation outcomes, obsolete sequences, stale attempt results and successful
competing attempts. Two controls use the actual header/body request-plan spawning
paths and verify that failure releases all reservations without changing peer
readiness; late retirement messages remain harmless. The first reservation fixture
used one block and therefore did not qualify for the existing parallel planner.
It was corrected to 64 small headers; no production threshold changed.

The existing dormant peer fixture is reused through test-only helpers. Its Reth
handle requires a localhost ephemeral listener, but discovery, DNS, network
services and peer connections are disabled. Storage is a new temporary directory.
Tests do not access existing user data, mac-mini or the physical external volume.
All **552 sync crate tests pass**, with two existing ignored workloads.

Implementer review traced every task spawn, replacement, reset and result-drain
caller. The pinned Tokio 1.51.0 source confirms the completion state uses an
Acquire load and channel `try_recv` waits for an in-progress send instead of
reporting that queue state as empty. No independent review is claimed. Removed redundant stale-work scans from
the candidate; no existing runtime helper became obsolete. All eight local gates pass on `5514fe1b`: vendor integrity, workspace and
patched-vendor formatting, all-target check, strict Clippy, 1,657 workspace tests
(24 ignored), documentation tests and release build. All six CI jobs passed on `fbe3b0f4`;
[PR #197](https://github.com/tdenisenko/logex/pull/197) merged as `f6f05acc`.

## Remaining boundaries

The error uses the existing node shutdown path. Generic engine errors currently
have a separate CLI exit-status audit item; this change does not claim nonzero
process exit or completed runtime supervision. Prepare/write task ownership,
shutdown while local work is in flight, queue admission, reorg and coverage
interactions remain in the broader sync/runtime review. Offline tests do not
replace a later live sync test or staging soak.

[Validation record](baselines/2026-09-17-historical-fetch-supervision.json).
