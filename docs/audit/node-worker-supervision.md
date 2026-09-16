# Node worker failure supervision

## Findings

**B10-03 — moderate, query service failures did not stop sync.** The HTTP and
gRPC spawned closures logged their server error and returned unit. The parent
only examined their task handles at final cleanup. An unavailable listener could
therefore leave ingestion running indefinitely. The parent also announced that
query endpoints were ready before the asynchronous binds completed; gRPC logged
that it was listening before its transport attempted the bind.

Both listener controls fail against a behavior-preserving extraction of the
original spawned closures at `a47ecbd8`. They hold an ephemeral loopback port,
await the actual service failure, then show the existing engine supervisor does
not stop. This is an extracted startup/control-flow reproduction, not a full-node
or live-sync run. The original closures and extraction patch are retained.

**B10-04 — moderate, idle checkpoint errors were only logged.** The background
indexer continued its loop after either an error from `checkpoint_if_due` or a
failed blocking checkpoint worker. In particular, storage requiring recovery
could remain in this state indefinitely while ingestion was idle. This is a
primary persistence failure, distinct from rebuilding a derived index.

The regression creates a one-row temporary store, preserves its catalog under a
fixture-only name, and occupies the catalog publication destination with a
directory. A real checkpoint fails, and the public storage API confirms that
further writes require recovery. The unchanged original indexer still does not
exit; the fixed indexer returns the actionable error. No existing data or mount
is used. The pre-fix production loop was verified byte-identical to the base.

**B10-05 — low, worker unwind during requested shutdown was suppressed.** The
existing execution-worker monitor treated every exit after `begin_shutdown` as
intentional, including a task panic. Its unchanged production implementation
fails the new shutdown-unwind control. Expected return/cancellation must be
distinguished from an explicit error or unwind.

## Implementation and invariants

Move the existing monitor to `logex_sync::tasks`, retaining a single implementation
for the execution workers and the node. This adds no crate or dependency. Its
atomic lifecycle still arbitrates ordinary completion against requested shutdown
and retains the first failure before or after subscription. The new fallible
spawn method retains component/error text. An explicit error or unwind remains a
failure after shutdown begins; normal return and owned cancellation do not.
Tokio still receives the original unwind as a task join error. No failed worker
is resumed and no storage state is treated as repaired.

The node monitors HTTP, gRPC and the background indexer. Checkpoint I/O and join
errors return from the indexer; existing optional index and compaction handling is
unchanged. The indexer prioritizes an already requested shutdown over its timer.
The engine supervisor checks the retained worker-failure channel before polling
the engine, marks shutdown intentional before sending the stop notification, and
uses the existing fatal stop/status path. The final cleanup checks the permanent
latch again so errors arising during another shutdown path produce exit status 1.

One additional independent watchdog starts before these services are spawned.
It waits on the failure notification without polling and enforces the existing
180-second failure-cleanup budget even if execution-network initialization or
application runtime workers are stuck. The guard remains alive through cleanup
and fatal process exit. The ordinary supervisor retains its 120-second engine
grace; subsequent failures cannot postpone the already armed worker deadline.
The tests use a reporting callback, never process exit, to exercise this watchdog.

Startup logs now say the endpoints are starting. HTTP's existing post-bind
listening message remains accurate. This change does not add a combined readiness
handshake or pre-bind both listeners; an early failure is retained and bounds
startup, and the engine cannot proceed past an already recorded worker failure.

## Validation and cleanup

All 131 node tests and 558 sync tests pass (two existing ignored sync workloads).
Thirteen new controls cover the four original failures, fallible normal return,
cancellation before first poll, first-error retention, explicit failure during
shutdown, successful HTTP/gRPC/indexer shutdown, an independent watchdog before
the supervisor is polled, node stop/status propagation, and errors arriving during
engine cleanup. Actual listener controls retain their component-specific error.
The five original execution-worker monitor tests remain in the shared module.

The candidate initially used `AtomicU8::fetch_update`; the pinned nightly reports
that spelling as deprecated. It now uses the recommended `try_update`, without
changing the pinned toolchain. Implementer review inspected monitor lifetime,
shutdown ordering, late error checks, worker ownership and obsolete call sites.
No independent review is claimed. All eight local gates pass on `1103832c`: vendor integrity, workspace and
patched-vendor formatting, all-target check, strict Clippy, 1,686 workspace tests
(24 ignored), documentation tests and release build. PR/CI/merge remain pending.

Removed the old private monitor module, duplicate service error-discard blocks,
checkpoint error-and-continue paths and premature readiness wording. The small
service spawn helpers are shared by production and the actual bind regressions.
No SQL, persisted format, protocol, configuration or dependency changed.

## Performance and remaining boundary

No ingestion storage operation, per-block transformation, request path or recurring
timer was added. The extra runtime resource is one sleeping watchdog thread plus
one retained failure channel; lifecycle atomics run at task exit/shutdown. No
benchmark or percentage throughput claim is made.

This milestone does not close the runtime batch. The outer consensus network
supervisor, general cleanup join timeouts and ignored cleanup errors, compaction
worker failures, volume identity/loss protection, query health and offline repair
remain separate work. In particular, `log_task_exit` still has its old post-abort
join behavior; ordinary shutdown with no latched failure needs further bounded
cleanup work. There was no full-node process test, live sync, deployment or new
mac-mini work. Existing real data and unrelated files remain untouched.

Machine-readable evidence: [baseline](baselines/2026-09-17-node-worker-supervision.json).
