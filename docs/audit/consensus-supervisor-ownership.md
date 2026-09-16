# Consensus supervisor ownership and failure reporting

## Findings

- **B3-64 — moderate: canceling the restart supervisor detached its worker.** At
  `0e94d900`, the supervisor awaited a bare spawned `JoinHandle`. Dropping that
  handle on outer cancellation leaves the child running. Node cleanup could
  retire the outer handle while consensus work continued until runtime teardown.
- **B3-65 — moderate: the outer consensus supervisor was not monitored during
  operation.** Node startup retained its handle only for final cleanup. The inner
  worker's failures triggered restart, but an unwind in the restart supervisor
  itself could leave consensus networking stopped without triggering node shutdown.
  Reconstruction and status publication are examples of code outside the child.
- **B3-66 — low: worker failures during shutdown were discarded.** The supervisor
  checked the shutdown flag before inspecting the completed child's result. A
  worker error or join panic could become a successful supervisor completion.
  Independently latched consensus-storage errors were already fatal; this finding
  does not claim those were lost.

Three local controls fail on a behavior-preserving extraction of the original
restart loop: child retirement after outer cancellation, worker error after a stop
request, and worker unwind after a stop request. The extraction substitutes a
prepared worker future and reconstruction closure for the concrete network;
production retains its one-second delay and existing constructor. It gives the
loop an explicit successful result without changing its original error-discarding
behavior. This is control-flow reproduction, not a full-node or live-network run.
The original cancellation control always releases its owned worker before asserting.

## Changes and invariants

`prepare_consensus_network` replaces the implicitly spawning public function.
Initial construction remains synchronous and fallible. Its returned future owns
the restart loop, and the node spawns that future through its existing `TaskMonitor`.
The existing node-worker watchdog is initialized before this spawn, so an early
outer failure is retained even while execution-network initialization is pending.
No new watchdog thread or dependency is added, and no migration is needed.

A `JoinSet` owns exactly one active consensus worker. Dropping the supervisor
requests child cancellation instead of detaching it. As with other Tokio aborts,
this is cooperative retirement; it does not forcibly interrupt synchronous work.
The whole-runtime shutdown deadline from PR #200 remains the final bound.
The one-child completion invariant is asserted where its join result is read.

Normal-operation worker return, error and unwind retain the existing restart
policy and one-second delay. Reconstruction failures remain retryable. Stop
notification takes precedence during that wait. A stop already observed before
first poll or during reconstruction prevents another worker from starting. A
closed shutdown sender is treated as loss of its owner, including when deciding
whether to return the just-completed worker's error.

During shutdown, explicit worker errors and join failures reach the node monitor;
ordinary successful completion remains successful. The node's existing shutdown
ordering first marks the monitor as stopping, then signals workers. Unexpected
outer completion, cancellation or unwind during operation triggers the same
failure latch and bounded node cleanup as other monitored services. Error and
unwind reporting remains active during cleanup.

Consensus status is invalidated before a normal-operation restart, preserving the
existing status policy. No telemetry poison recovery or new status-state model is
introduced here. A supervisor failure stops the node; it does not resume from a
partially unwound supervisor. Inspection of pinned discv5 0.10.4 confirms its
`Drop` calls `shutdown`; this does not claim synchronous joining of all transport
or discovery-internal work.

## Validation and cleanup

Ten deterministic local controls cover child cancellation before/after polling,
error/unwind preservation, closed shutdown ownership, recovery after all three
worker outcomes and a construction error, stop before startup/during reconstruction,
outer reconstruction unwind, and stop during the retry delay. They use owned
futures, channels and local status, without network sockets, chain fixtures or
user data. All 353 consensus tests (one existing ignored test) and 142 node
tests pass. The sandbox denied loopback binding in fourteen existing node controls;
the permitted rerun passed all 142. The final cancellation observation allowance
is five seconds to accommodate CI scheduling; the original reproduction used
200 milliseconds and still released its worker before assertion. All eight local
gates pass on `b8c770e6`: vendor integrity, workspace and patched-vendor
formatting, all-target check, strict Clippy, 1,710 workspace tests (24 ignored),
documentation tests and release build. PR/CI/merge remain pending.

Implementer review traces the initial constructor, restart ownership, all API
callers, shutdown-before-notification ordering and the existing permanent monitor
latch. No independent review or end-to-end node run is claimed. Removed the old
implicitly spawning API and its discarded result path; repository search finds
no remaining caller. The restart helper is private to the consensus network.

This changes startup/restart/cleanup ownership, not per-block validation, storage,
SQL, discovery limits or normal network event processing. There is no new recurring
timer or per-message accounting. No benchmark or numerical throughput claim is
made. Broader CL networking, status accuracy, volume supervision, offline repair,
full-node live sync and staging acceptance remain separate audit items.

Machine-readable evidence: [baseline](baselines/2026-09-17-consensus-supervisor-ownership.json).
