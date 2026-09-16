# Historical work cancellation and shutdown

## Findings

**B5-04 — moderate: historical jobs could outlive their owners.** Header/body
fetch and prepare task fields held bare join handles. The primary prepare wait
moved one into a local variable, and the older extraction pipeline stored bare
blocking handles in a queue. Dropping those owners detached the jobs. The
historical write helper likewise detached its blocking job when canceled.

At `fd57def9`, three controls against unchanged production reproduce the effects:
a dropped queued write later advances an owned temporary database's historical
floor; dropping the active prepare waiter leaves its worker alive; and dropping
the engine before polling its three test workers leaves header/body/prepare jobs
alive. Each control releases or cancels its owned blockers/workers before
asserting. These prove ownership failures, not reorg corruption or invalid data
publication. The already-existing validation `JoinSet` owners were appropriate
and remain in place.

**B5-05 — moderate: ordinary shutdown discarded a pending write result.** The
prepared-batch selector returned `Ok(false)` immediately on shutdown, dropping
its write future. Two controls exercise the actual engine method while an owned
storage reader delays its write. The original returns before successful commit,
and it also reports normal stop when storage subsequently rejects the batch.
The latter uses a tiny local row/floor mismatch to exercise error propagation;
it does not simulate a disk failure or claim a peer-validation bypass.

**B5-06 — moderate: shutdown-channel semantics differed across engine paths.**
`cancelable` ended work on any notification, including `false`, and could miss a
`true` value already marked seen. Conversely, four direct selectors ignored
channel closure, leaving an immediately ready stop branch without an exit. The
synchronous stop predicate also ignored closure. Three original helper controls
fail for observed `true`, a false notification and a closed owner; the existing
closed-channel cancellation behavior of `cancelable` passes. Normal CLI control
currently sends `true`; no claim is made that it ordinarily sends false updates.

## Implementation

Historical header/body/prepare fields and extraction/write handles now use
`AbortOnDropHandle`. Ownership begins when the handle is stored, including before
the caller's first poll, and remains attached when a handle moves into an active
wait. The wrapper cancels asynchronous jobs and queued blocking jobs when dropped;
completed results and existing peer-owner retirement/generation checks remain.
This uses the existing locked tokio-util 0.7.18 package with its `rt` feature. The
only lockfile changes are the sync-to-tokio-util and tokio-util-to-futures-util
dependency edges. All package identities, versions and checksums are unchanged.
No custom future wrapper, new unsafe code or new runtime worker is introduced.

A shared stop waiter checks the current value, then uses the pinned watch
receiver's predicate wait. True or channel closure stops the engine; false updates
keep work pending. The synchronous predicate recognizes closure too. The borrowed
watch result is dropped before returning, so no watch lock crosses a caller's
await. The existing cancellation helper and all four direct engine selectors
use this policy.

An already-stopped engine does not begin a new prepared batch. Once a verified
batch is selected, ordinary shutdown finishes that batch and observes its result
before reporting stop; it does not continue the refill loop. Write errors still
propagate to the node's failure supervisor. Ingest-in-progress telemetry is cleared
before handling that result. The existing 120-second engine grace and 180-second
whole-runtime watchdog remain authoritative; no second timeout scheme is added.

A started blocking job cannot be forcibly canceled. This includes a job already
waiting for the storage lock: if its caller is forcibly dropped, it may still
finish its verified chunk. A separate actual-storage control confirms that
behavior. The fix cancels jobs that remain queued and retains current-batch
results during ordinary shutdown; it does not promise to roll back a started
transaction. Storage's atomic row/floor publication and restart contract remain.
Standalone library callers retain responsibility for bounding their runtime.

## Validation and cleanup

Original production is unchanged in the saved regression source/patch, apart from
test additions and a `cfg(test)` module declaration. Across four original runs,
eight controls fail and one passes. The final implementation adds sixteen tests
covering those failures, closed-owner prepare termination, pending false-to-true
notification, an already-stopped batch, normal successful/rejected writes,
started-versus-queued blocking work, and empty-block extraction across a chunk
boundary. All 586 sync tests pass; two existing workloads are ignored.

The first extended suite had one fixture timeout: its successful batch at block
100 initiated the normal follow-up fetch, but the dormant peer fixture has no
scripted response. That success fixture now finishes at the history target
(genesis), which tests normal completion without an unintended network wait.
Both its initial failure and the corrected passing runs are retained. The
shutdown regressions keep their original block-100 fixture. Final cleanup also
passes all 586 tests. Workspace gates are pending.

Implementer review traced constructor-to-owner moves, resets, removal on success,
late/stale outcome checks, queued versus started blocking semantics, ordinary
shutdown error propagation, empty-block progress and every direct shutdown
selector. No independent review or full live-node shutdown run is claimed.
Removed redundant explicit abort-before-drop calls and the prepare-map drain loop;
owned-handle destruction now supplies that behavior. Preserved explicit peer
reservation retirement, generation advancement and existing validation owners.

No batch sizes, checkpoints, compression, query paths, wire formats or allocation
budgets change. There are no additional steady-state storage writes. No benchmark,
throughput percentage, physical-volume or mac-mini result is claimed. Global
memory budgets, reorg/coverage state review, volume protection and offline repair
remain separate audit work.

## References

- [Tokio-util 0.7.18 owned task handle](https://docs.rs/tokio-util/0.7.18/tokio_util/task/struct.AbortOnDropHandle.html).
- Installed pinned Tokio 1.51.0 `src/sync/watch.rs` documents current-value checks,
  false-value waiting, closure and temporary `Ref` lifetime; its `src/task/blocking.rs`
  documents the limitation on canceling already-started blocking jobs. The local
  source was inspected; web retrieval of those two versioned documentation pages
  was unavailable.
- [Runtime cleanup deadlines](runtime-cleanup-deadlines.md) document the existing
  engine/runtime bounds used during shutdown.
