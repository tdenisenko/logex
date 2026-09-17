# Blocking maintenance worker supervision

## Finding

**B10-23 — moderate: failed blocking maintenance workers bypassed node supervision.**
At `65844f06`, compaction-backlog refresh, compaction, sealed indexing and hot
indexing each logged a blocking task's `JoinError` and continued. The outer
background indexer therefore remained alive, and its node-worker monitor did
not receive that failure. A panic is not an ordinary retryable operation error:
it can leave task-owned state incomplete. This is a supervision gap, not a
claim of demonstrated data corruption or an input-triggered panic.

Two controls fail under a behavior-preserving extraction of the original join
failure policy: the worker failure returns no error, and the node monitor gets
no failure notification while the loop remains pending. The original production
loop was unchanged during this experiment; the extracted branch, original file,
tests, patch and failing output are retained. This is an extracted control-flow
reproduction, not a full-node or actual compactor failure-injection run.

## Correction

Every blocking operation in the indexer now joins through one small helper.
A join failure returns an error naming the worker. Each caller propagates that
outer error out of the indexer; the existing `TaskMonitor::spawn_result` retains
it and the existing supervisor closes admission, requests shutdown and applies
its bounded failure-cleanup policy. The checkpoint worker already did this and
now shares the helper with unchanged error text and operation-error handling.

The helper leaves each operation's own result intact. Optional compaction and
index I/O errors retain their existing contextual logging/retry behavior;
checkpoint operation errors remain fatal. Successful values, source-change
`WouldBlock` results and the hot-index transient-error check are unchanged.
No worker is resumed after an unwind. The owner does not abort these handles
while running, so an observed unexpected cancellation is also a failure. Outer
indexer cancellation and runtime teardown retain their existing lifecycle.

The patch removes the four error-and-continue join branches. It keeps the actual
compaction/index closures, worker count, timers, plan bounds, filesystem writes,
locks and scheduling policy. No dependencies, persisted formats, API semantics,
configuration defaults or ingestion work change. There is no benchmark or
quantitative performance claim; the successful path adds no allocation or I/O.

## Validation and review boundary

Four new controls pass: worker unwind, retained node-monitor notification,
successful values/unchanged operation errors, and unexpected cancellation.
The full node suite passes 214 tests. Fixtures use owned in-process workers and
channels; the focused controls require no database, external peer or OS signal.
The existing node suite includes owned temporary data and ephemeral loopback
listeners. No production data, mac-mini work or live sync is involved.

Implementer review traced all five blocking-worker joins, normal and error
branches, the outer node monitor, supervisor and bounded cleanup. No independent
review is claimed. Configuration merging, typed options, listener checks and
offline-command ownership were inspected; their broader validation/disposition
remains separate. This milestone does not close the complete runtime batch,
offline repair, integrated acceptance or release readiness.

All nine local gates pass on `d10d2683`: vendor integrity, workspace/Reth/ENR formatting, all-target compilation, strict Clippy, 1,848 workspace tests (24 ignored), documentation tests and release build. Exact-head Linux/macOS CI and merge remain pending.
Machine-readable evidence: [baseline](baselines/2026-09-17-maintenance-worker-supervision.json).
