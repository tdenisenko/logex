# Bounded storage health probes

## Findings

**B10-08 — moderate: unavailable storage probes were ignored.** The runtime's
free-space guard logged every probe error and continued indefinitely. A missing
configured data path or failed filesystem query could therefore leave the guard
running without producing a shutdown trigger. A regression against unchanged
production at `a3c57ac2` creates one temporary directory, queries a nonexistent
child path, confirms `NotFound`, then observes that the guard does not stop within
100 milliseconds. It fails before the fix. No data is removed, mount changed or
disk filled by this control.

**B10-09 — moderate: filesystem probes ran on the shutdown supervisor's worker.**
Source inspection shows that canonicalization and `statvfs` executed synchronously
inside a future polled by the engine/signal/failure selector. A delayed filesystem
call could prevent that selector from observing other ready events. This is a
blocking-call ownership finding, not a measured throughput regression or a claim
that a real filesystem stalled in the test environment.

## Implementation

The private storage-health module owns the probe loop, path selection and error
reporting. The existing 10-second polling interval, 10 GiB free-space threshold,
strict below-threshold comparison and writable-root selection remain. It checks
the configured data directory and segments root, resolving/deduplicating their
paths while excluding individual sealed-segment link targets. A failed canonical
resolution still probes the configured path; it never substitutes an ancestor.

Each iteration runs canonicalization and free-space queries in one blocking job
with a 10-second deadline, including queue time. A one-job `JoinSet` retains
ownership. Healthy completion permits the next interval; low space, I/O errors,
worker unwind and timeout return an actionable failure to the node supervisor.
Diagnostics preserve the affected path and error; low-space messages preserve
the observed bytes and threshold. The existing supervisor marks failure, notifies
workers and invokes the bounded shutdown path, including runtime destruction.

There is no overlapping retry after failure. Dropping the owner cancels queued
work. A started filesystem call cannot be forcibly interrupted; an expired job
is not awaited again, and the existing independent whole-shutdown watchdog bounds
runtime teardown. Ordinary signal handling remains responsive while a started
probe is waiting. There is no new watchdog thread and no filesystem write in the
probe. Unsupported-platform errors now stop the node rather than indefinitely
warning without a working guard; macOS and Linux are the required platforms.

The moved `statvfs` FFI documents pointer lifetime, alignment, writable output and
successful initialization before reading it. Existing wide multiplication and
saturation remain unchanged. Node descriptor-limit FFI was inspected separately;
its existing initialization/safety comments and bounded soft-limit update remain
appropriate, so no change was made there.

## Validation and cleanup

All 154 node tests pass, including twelve new controls and two relocated existing
controls. These cover the original missing path, exact per-path I/O error,
low-space measurement and equality boundary, actual healthy temporary roots,
worker unwind, a started probe timeout, cancellation while queued, invalid path
conversion, failure status/diagnostics and signal responsiveness during a held
probe. Blocking controls own and release their workers before assertions; the
queued-work control drops its runtime before checking that canceled work never ran.
No physical-volume, live-node, external service or mac-mini test is claimed.
The initial source passed all eight local gates (1,720 tests, 24 ignored) and all
six CI jobs, but final review found a deadline-completion ordering gap before
merge. Pinned Tokio 1.51 polls a ready join before checking its timeout. A
ready-join/expired-deadline control fails on a behavior-preserving extraction of
the candidate's completion path. The corrected worker records its completion
time; the original deadline is established before spawning, and late successful
completion is rejected even if already queued. Timely completion remains valid
when observed late. Both cases have deterministic controls. The final workspace
gates and fresh-head CI/merge are pending.

Implementer review traced both probe roots, error and cancellation paths, exactly
one active job, unchanged interval/threshold, supervisor outcome handling and
runtime deadline ownership. No independent review is claimed. Removed the old
warning-and-continue loop, narrow low-space-only result type and redundant path
insertion helper. The previous fixed-PID temporary test directory is replaced by
an owned `TempDir`; no unrelated directory cleanup is performed.

The same periodic filesystem reads now run on a blocking worker. No ingestion
batch, storage encoding, checkpoint write, query path or dependency changes.
No benchmark or percentage performance claim is made.

## Remaining boundary

This is not expected-volume supervision. It does not prove stable volume identity,
writability, prevent an unmount/path substitution race or implement deployment
preflight. Those protections, offline repair and live/staging acceptance remain
required. Existing storage write-error handling is separate from these read-only
health checks.

Machine-readable evidence: [baseline](baselines/2026-09-17-storage-health-probes.json).
