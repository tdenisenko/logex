# Execution worker lifetime and shutdown

## Findings

- **B4-34, medium — repeated polls after event-stream completion.** The network
  and discovery branches of `wait_for_activity` returned immediately on a closed
  stream without retiring it. `fill_peers` could repeatedly retry that ready branch
  during its two-second refill budget. Immediate event draining also discarded the
  distinction between a temporarily pending stream and a completed stream.
- **B4-35, medium — detached workers after owner or constructor exit.** Dropping
  `PeerManager` dropped its three optional `JoinHandle`s without cancelling the
  workers. The family-aware DNS poller was also spawned before the fallible Reth
  network constructor; startup failure could leave it detached.
- **B4-36, high — unobserved execution worker failure.** The network, request
  handler and optional DNS worker handles were only inspected during explicit
  shutdown. An unexpected exit could leave sync waiting for progress. A network
  event stream is not a reliable failure signal: the retained Reth handle can keep
  its sender alive after a worker exits.

These are lifecycle/liveness findings, not evidence of accepted invalid chain
input or persisted data loss. The exact pinned Reth DNS stream never yields `None`
while healthy; its request handler and network manager are also long-lived workers.
Normal worker completion is therefore unexpected while the owner is running.

## Correction

Completed network/discovery streams are replaced with pending streams once; the
optional completed DNS stream is removed. Both immediate draining and awaited
selection preserve completion. Normal events and timer behavior are unchanged.

The DNS service remains owned by the constructor until fallible network setup
succeeds. `PeerManager::drop` marks intentional shutdown and aborts remaining
workers. Explicit async shutdown still uses the existing acknowledgement, drain
and abort/join deadlines; drop is the cancellation safety net, not a blocking join.

A small shared task monitor wraps the three existing workers. A drop guard reports
normal return, unwind or cancellation, including cancellation before the first
poll. One atomic transition arbitrates the first unexpected exit against intentional
shutdown. A permanent watch latch retains that failure before any subscriber is
created and throughout cleanup. Tokio still reports the original join error.
There is no new recurring poll or per-block monitor operation.

The node subscribes before starting the sync engine and routes a failure through
its existing graceful shutdown sequence, with a 120-second engine grace and
nonzero exit. It rechecks the permanent latch after shared cleanup so a failure
during a different shutdown path is not missed. The existing independent watchdog
helper is shared with this path: one additional normally sleeping OS thread enforces
the 180-second total failure-cleanup limit even if application runtime work blocks.
Execution failure does not itself invalidate authenticated consensus freshness.

## Evidence and validation

Three controls were first run against unchanged production code at `e17bd1bf`:
both closed-stream controls and owner-drop cleanup failed. The first attempt did
not execute tests because the local Xcode license blocked linking; after the user
accepted it, all three failures reproduced. That environment failure is retained
separately and is not counted as a regression result.

The corrected suite passes **280 peer-manager tests** and **49 node runtime tests**.
Twelve new controls cover the original failures, immediate draining of all three
streams, normal worker return, isolated unwind, pre-poll cancellation, intentional
shutdown, first-failure preservation, bounded cleanup with a dormant network,
failure notification without a network event and consensus freshness preservation.
Existing watchdog controls verify expiry without an application runtime and cleanup
disarming. Worker-unwind tests are isolated Tokio tasks; no process is terminated.
The peer fixture binds a dormant loopback listener with discovery disabled and
makes no external connections. No Mac mini work or benchmark is involved.

Source review traced all three spawn sites, the constructor's last fallible await,
explicit shutdown, drop, event polling, runtime selection and the final failure
latch. Review was performed by the implementing agent: delegated reviewers were
unavailable due their usage limit, so this record does not claim an independent
review. Startup-failure prevention is established by ownership and spawn ordering;
it was not exercised through a live DNS lookup or an actual node process failure.

The previous detached spawn sites, completion-losing drain loops and store-specific
watchdog helper names are removed. Request queues, worker count, peer scoring,
cache policy, ingestion/storage formats and successful-query behavior are unchanged.
The monitor adds constant ownership state and one idle watchdog thread; no measured
throughput claim is made. All eight initial local gates pass on `c7e3e112`, with 1,596 workspace tests
(24 ignored). Review then strengthened the owner-drop control to await explicit
completion with a deadline instead of relying on a single scheduler yield, and
covered both activity-selection branches with DNS present/absent. Production
code is unchanged. All eight final gates pass on `5c89efae`: vendor integrity,
workspace and patched-vendor formatting, all-target check, strict Clippy,
1,596 workspace tests (24 ignored), documentation tests and release build.
All six CI jobs passed on `f70047fc`;
[PR #190](https://github.com/tdenisenko/logex/pull/190) merged as `eb326cfb`.

## Remaining scope

This closes only the named lifecycle findings after validation and merge. Remaining
execution address/rehabilitation/resource paths still need their dispositions.
Other runtime workers and generic engine-error/low-space exit policies remain in
batch 10. General non-cooperative async work is not forcibly stopped by `abort`;
the fatal-path independent process watchdog provides the outer shutdown bound.
Live sync and the staging soak remain later acceptance gates.

[Validation record](baselines/2026-09-16-execution-task-lifecycle.json).
