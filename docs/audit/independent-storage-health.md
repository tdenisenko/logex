# Independent ordinary-storage health monitoring

## Finding

**B10-19 — moderate: engine filesystem work could postpone ordinary storage
health detection indefinitely.** The engine and health future were siblings in
one biased selector. Moving the filesystem probe to a blocking worker in PR #202
kept the probe itself off that selector, but did not let the health future start
or observe its deadline while another sibling held the poll. PR #207 corrected
this for configured expected volumes; ordinary storage still used the async loop.

The regression runs unchanged production at `43b4f182`, with one added test and
an owned missing child path. On a current-thread runtime, an engine-shaped future
waits at most 100 milliseconds for health notification without yielding. The
sibling invokes the actual ordinary health function. The control fails because
notification never arrives. This models one bounded synchronous engine call; it
is not a physical-device stall or full-node ingestion test. Real synchronous
storage calls occur in anchored ingestion, historical finalization, empty-range
progress and reorg persistence.

## Change

The existing independent volume monitor becomes `StorageMonitor`. Expected-volume
preflight, identity/namespace protections and probe timing remain unchanged.
Ordinary sync starts the same owned monitor immediately after `PartitionManager`
has initialized its writable roots. Main owns it through runtime destruction and
monitor teardown. An already-running expected-volume monitor is reused, retaining
its stronger checks; there is no second monitor for the same node.

Ordinary monitoring probes immediately, then waits 10 seconds after each completed
probe. Each probe has an independent 10-second deadline. The existing 10 GiB
threshold, equality boundary, configured data/segments roots, canonical path
selection and error details remain. Ordinary probes only read filesystem state;
they do not create directories or substitute an existing ancestor when a path
is unavailable. Existing ordinary alias behavior is preserved.

An observed failure arms an independent 180-second terminal deadline before
notifying the runtime and closing storage query admission. This remains effective
while the engine holds its poll. Before the application callback is installed,
failure ends startup with a diagnostic and nonzero exit. The callback continues
to hold only a weak application-state reference, avoiding extended storage-lock
ownership during normal teardown. Monitor drop joins owned work and retains a
latched failure; shutdown cannot turn that failure into successful process exit.

The async health future now only waits for the monitor notification. The old
interval, blocking-pool queue, one-job JoinSet and duplicate completion/deadline
path are removed. Superseded queue-specific tests are removed; independent probe,
deadline, stopped-worker and callback controls remain in the shared monitor suite.

## Validation and limits

The original control fails once on unchanged production. All 12 focused health
controls and all 203 node tests pass. Controls cover immediate observation without
an async runtime, unchanged existing-monitor ownership, initialized writable roots,
missing-path startup failure, retained notification/channel closure, exact path
and free-space diagnostics, and failure notification during a blocked engine poll.
The latter runs in an owned child, with a parent deadline and a marker proving the
assertions completed before the expected nonzero teardown. Signal responsiveness
during an in-flight independent probe remains covered. Existing shared monitor
controls cover timeout, unwind, blocked callback, failure retention and ordinary
stop; cleanup controls cover ownership through runtime destruction.

Implementer review traced startup order, both monitor modes, callback lifetime,
first failure retention and main-owned teardown. No independent review is claimed.
All eight local gates pass on `2ffd2de4`: vendor verification, workspace and patched-vendor formatting, workspace check, strict Clippy, 1,828 workspace tests (24 ignored), documentation tests and release build. All six CI jobs passed on `0c6c8923`, including Linux/macOS tests and ten Linux volume/template cases with verified cleanup. [PR #208](https://github.com/tdenisenko/logex/pull/208) merged as `17b044f9`.

Ordinary monitoring adds an owned sleeping thread and uses the existing short-lived
deadline observer for each periodic probe. It removes blocking-pool scheduling
from this path. It adds no per-block writes, storage formats, query evaluation or
ingestion changes. No benchmark or throughput percentage is claimed.

Ordinary monitoring does not establish volume identity or anchor pathnames. Use
expected-volume configuration for those guarantees. Ordinary startup before
`PartitionManager::open` completes remains outside this monitor's lifetime; this
change does not claim to bound arbitrary startup I/O. A single filesystem syscall
cannot be interrupted; terminal process shutdown bounds a failed/timed-out probe.
The broader runtime audit, verified offline repair and integrated acceptance are
still incomplete. No live sync, service installation or remote machine work ran.

Machine-readable evidence: [baseline](baselines/2026-09-17-independent-storage-health.json).
