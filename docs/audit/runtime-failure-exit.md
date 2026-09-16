# Runtime failure exit and cleanup

## Findings

**B10-01 — moderate, unsuccessful shutdown reported as success.** The runtime
logged and discarded a generic sync engine error. Low-disk shutdown returned the
engine's eventual result, usually success, and engine shutdown timeout branches
also returned success. Only the separately latched consensus-storage and
execution-network failures forced a nonzero exit. A service relying on failure
exit status could therefore miss a failed sync or low-disk stop.

Four controls reproduce success for engine error, low disk, engine error while
honoring a shutdown signal and an expired shutdown grace period. Two normal exit
controls pass. To test these without live startup, the selector and discarded
result handling were first extracted without changing their behavior. This is
an equivalent production control-flow reproduction, not a byte-identical full
node run. The original source, extraction patch/script and test snapshots are
recorded separately at base `f6f05acc`.

**B10-02 — low, failure reporting could unwind.** The runtime failure status
marker expected an unpoisoned telemetry mutex. An interrupted update could cause
failure reporting to panic instead of finishing cleanup. The unchanged original
marker was verified against the base source and fails a local poisoned-mutex
control. Recovery is limited to updating failure telemetry; the poison indication
remains. No persisted data is repaired or trusted by this recovery.

## Implementation and invariants

A private supervisor retains the existing event priority and drives one shared
engine shutdown grace period. It retains a failure exit status for engine errors,
low disk and grace expiry. Normal engine completion and cooperative signal-driven
shutdown remain successful. Existing consensus/execution failure latches are still
checked after shared cleanup, including failures arriving during another stop path.

The first failure arms the existing independent watchdog mechanism before supervisor logging,
status locks and cleanup. The added watchdog starts only on failure and its guard
lives in `run_sync` until final exit. Subsequent errors cannot restart its deadline.
The existing 120-second engine grace and 180-second failure-cleanup budget remain;
for a signal that first becomes a failure at grace expiry, the earlier signal
grace has already elapsed before the failure-cleanup budget begins.

Fatal completion uses the existing `process::exit(1)` pattern after shared cleanup.
Returning through runtime destruction could wait indefinitely on blocking tasks
while dropping the watchdog. The original consensus/execution watchdogs remain,
including their startup coverage. Normal ingestion starts no additional watchdog
thread and receives no new timer, I/O or per-block data transformation.

Engine unwinds are contained at the outer future boundary solely to stop the node.
The future is never polled again after a panic. `AssertUnwindSafe` is limited to
that terminal boundary: continued ingestion never relies on partially updated
engine state; the caller retires owned work, closes services and exits. Panic
payload text remains in the diagnostic. This does not recover aborting panics,
resume the engine or establish integrity of data affected by an unrelated bug.
There is one unwind boundary around polling the engine future; no benchmark or
percentage performance claim is made.

All failure paths notify other workers and clear active-sync/ETA status. Consensus
freshness is invalidated for consensus failures only. A local telemetry poison
cannot unwind this marker. The marker runs again after the engine's shutdown
phase, because final progress can otherwise restore a healthy-looking status.
The poison flag is not cleared; other readers still observe an interrupted update.

## Validation and cleanup

All **124 node tests pass**, including 16 new controls for normal exits, the four
failure exits, late and already-latched runtime failures, event priority, first
failure ordering, an unchanged cleanup deadline, engine unwind, telemetry poison
and final-progress overwrite. A real watchdog observes an initially failed latch
even after its sender is dropped; its test callback reports through a local channel.

Candidate review separately reproduced the uncontained engine unwind and a final
engine progress update overwriting the failure marker. Both are fixed and covered.
The initial timeout test requested Tokio's optional paused clock, unavailable in
the node-only feature set; the final control uses a zero-grace pending future
without changing dependency features. These tests use local futures, synthetic
signals/disk readings and in-memory status; they do not fill a disk, signal another
process, start a node or use existing data. No mac-mini work occurred.

Removed four repeated engine-grace blocks and the duplicate low-disk status marker.
The selector, grace handling and failure recording now have one private implementation.
The parent's pin import is test-only. No public CLI, API, dependency or storage
format changed. Implementer review covered all selector branches, watchdog lifetime,
terminal exit mapping and unchanged late-latch checks; no independent review is
claimed. All eight local gates pass on `36caff48`: vendor integrity, workspace and
patched-vendor formatting, all-target check, strict Clippy, 1,673 workspace tests
(24 ignored), documentation tests and release build. All six CI jobs passed on `6f524028`;
[PR #198](https://github.com/tdenisenko/logex/pull/198) merged as `a47ecbd8`.

## Remaining boundaries

These controls do not run a full node or test live sync. The final process-exit
wiring has source review; watchdog tests use callbacks instead of terminating the
test process. HTTP/gRPC/indexer completion supervision, normal cleanup task failures,
free-space probe I/O errors, volume identity/loss protection, query health and
verified offline repair remain open audit work. No complete runtime or release
readiness claim is made.

[Validation record](baselines/2026-09-17-runtime-failure-exit.json).
