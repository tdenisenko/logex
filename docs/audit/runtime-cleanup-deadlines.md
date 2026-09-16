# Runtime cleanup deadlines and results

## Findings

**B10-06 — moderate, ordinary shutdown was not bounded end to end.** The node's
service join helper waited 60 seconds, aborted the task, then awaited the same
handle again without a deadline. Abort cannot stop already running blocking work.
The existing fatal watchdogs only covered recorded failures; ordinary signal
shutdown had no equivalent protection through engine cleanup and runtime teardown.

A behavior-preserving extraction of the original helper at `4915ad57` makes its
existing timeout a parameter. The production wrapper retains 60 seconds. The
control starts one owned blocking worker, uses zero grace, and fails because the
post-abort await exceeds an outer 200-millisecond observation window. The release
channel is dropped before asserting, so the control always unblocks its own
worker. No other process, data directory, mount or OS signal is involved.

The pinned Tokio 1.51.0 source confirms the second lifetime boundary: dropping
`Runtime` drops its blocking pool, which calls `shutdown(None)` and waits for
started blocking work. A watchdog disarmed when `run_sync` returns cannot cover
that subsequent destruction. Calling `shutdown_timeout` alone would return while
work could continue; it would not establish successful cleanup.

**B10-07 — moderate, cleanup errors could be reported as successful exit.** The
node helper logged join failures without returning them. Execution cleanup also
logged/discarded acknowledgement errors/timeouts and worker abort/join failures;
`SyncEngine::shutdown` returned unit. Previously monitored worker faults still
had their independent latches, so this finding does not claim those were lost.
Missing shutdown acknowledgements, expired cleanup waits and failures outside
those monitors remained unrepresented in the final exit decision.

## Implementation and invariants

Every observed stop trigger arms an independent 180-second whole-shutdown guard
before stop-path logging, status locks or engine grace. This covers normal engine
completion and signals as well as faults. The existing independent consensus,
execution and node-worker failure watchdogs remain and retain their earlier first
failure deadlines. Later errors do not restart any existing budget.

Successful completion of the new guard is explicit. `run_sync` returns it to
`main`, which calls the shared teardown helper with ownership of the Tokio runtime.
Only after runtime destruction finishes does the helper send its completion time.
Dropping the guard without completion, including during unwinding, reports failure;
it cannot silently disarm the watchdog. Expiry and lost completion both use the
failure callback. Production exits with status 1; tests report through a channel.

The completion protocol checks the timestamp against the original deadline, even
if a notification is already queued when the watchdog runs. The main teardown
helper also returns an error on late completion or an expired receiver, preventing
an expired teardown from returning success if the watchdog thread was delayed.
If the independent thread cannot be started, the node exits immediately, before
potentially blocking reporting or cleanup. The expiry callback likewise avoids
application locks and logging. These are process deadlines after a stop is observed;
they do not promise real-time OS scheduling or immediate signal observation when
application workers cannot poll the signal future.

The engine keeps its existing 120-second run grace. Its subsequent cleanup has a
60-second async timeout. HTTP, gRPC, indexer and optional consensus-network handles
are joined concurrently, each with the existing 60-second allowance. The absolute
180-second deadline remains authoritative; inner allowances do not extend it.
A timed-out node join requests abort and returns an error without a second await.
Unexpected cancellation and join panic also return errors. All these errors retain
failure exit status and stopped/failed telemetry through final cleanup.

Execution cleanup attempts every remaining owned worker even after an earlier
failure. It preserves acknowledgement errors/timeouts and join panic/abort timeout
in one bounded list of diagnostics. A requested abort cancellation remains normal.
`PeerManager::shutdown` and `SyncEngine::shutdown` now return results to their owner.
Existing dormant-network controls still verify that owned handles are retired,
and now also check the missing acknowledgement instead of ignoring it.

## Validation and cleanup

All 142 node tests and 24 focused sync supervision tests pass. Thirteen new
controls cover the original unbounded join, normal/failed/canceled joins, requested
abort versus panic and blocking timeout, all stop-trigger hooks, an actual runtime
blocked on owned work, explicit successful completion, guard abandonment, cleanup
unwind, failure status and delayed/expired completion. Test workers are released
before assertions; no test callback exits the test process. All eight local gates pass on `9fb27264`: vendor integrity, workspace and
patched-vendor formatting, all-target check, strict Clippy, 1,700 workspace tests
(24 ignored), documentation tests and release build. PR/CI/merge remain pending.

Implementer review traced all shutdown callers, normal/error event ordering,
watchdog ownership across `block_on`/runtime destruction, source/result lifetimes
and obsolete helper references. No independent review is claimed. Removed the
unit-returning logging helper, unbounded post-abort join, discarded execution
cleanup outcomes and the separate error-only engine watchdog. Existing fatal
component watchdogs remain in use and were not deleted.

No normal-ingestion I/O, recurring timer, storage format, SQL, protocol, dependency
or configuration change is introduced. The new watchdog thread starts only when
stopping; it replaces the engine's previous failure-only cleanup guard. Concurrent
cleanup changes shutdown latency, not ingestion batch processing. No benchmark or
percentage throughput claim is made.

## Remaining boundary

This does not complete the runtime or sync-state audit. Normal peer-hint persistence
errors retain their advisory behavior. Low-disk probe errors, the outer consensus
supervisor's operational liveness, in-flight preparation/write ownership across
reset/reorg, volume supervision and offline repair remain separate items. This
change preserves the accepted storage recovery contract; it does not add a final
checkpoint or claim all recent ingestion is durable at normal exit. Full-node,
live-sync, deployment and staging acceptance remain pending. No mac-mini work or
user-data changes occurred.

Machine-readable evidence: [baseline](baselines/2026-09-17-runtime-cleanup-deadlines.json).
