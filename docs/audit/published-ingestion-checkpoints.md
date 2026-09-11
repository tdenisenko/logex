# Bounded sync progress publication

This is an **unmerged implementation in validation**, following `683bcdf8` in
PR #130. The user authorizes bounded verified re-ingestion, engineering judgment
and fresh incompatible directories, while retaining the 10% ingestion ceiling.
No production directory or service uses this implementation.

## Contract and recovery boundary

Generic row-only writes retain their WAL-backed durable contract. Combined sync
`checkpoint()` orders data and catalog publication without promising that the
latest progress has reached stable media. `checkpoint_durable()` explicitly
persists the latest rows **and** progress. WAL transitions, reorgs and standalone
metadata/maintenance mutations require that stronger boundary.

Every pending and published sync epoch since the last full flush shares one
window: 32 MiB of encoded payload, 64 caller batches or five seconds. Publishing a
catalog does not reset its counters, original segment boundary or deadline. Route
changes harden the preceding window before switching live/historical writers.
A valid oversized caller batch remains an exception to the byte bound and hardens
before return; time bounds are checked at batch boundaries and by the background
idle checker, not during a blocked I/O call. This is not a strict wall-clock
shutdown deadline. Power loss may require repeating some or all of this window.
It is a recovery allowance, not a deliberate rewind on every restart.

The catalog remains the sole authority for rows, canonical state and coverage.
Each preceding catalog retains a complete valid dataset: bundle extents are
immutable and raw replacements preserve their earlier committed prefixes. Startup
never adopts a newer manifest or payload to guess progress. Missing/corrupt
committed bytes still stop recovery. Re-ingestion uses the existing verified fetch
path. This lifecycle adds no recovery-row copy or further on-disk version.

## Publication sequence

1. Flush each affected segment file, directory and its parent. Prepare and flush
   the complete checksummed catalog in an exclusively created temporary file.
2. Order these writes before catalog publication on the catalog's device. Fully
   synchronize other devices: their data can otherwise persist independently of
   the catalog. On Apple, use the existing `F_BARRIERFSYNC` path and stronger full
   sync fallback when barriers are unsupported. Linux continues to use fsync.
3. Rename the catalog, then order its directory before subsequent writes. Plain
   directory fsync is insufficient for this ordering on Apple. This second barrier
   places namespace changes ahead of later reuse of the old catalog's blocks.
4. Preserve the earliest origin, counts and deadline since the last full sync.
   Exclude all affected segments from new detached compaction plans until a full
   checkpoint. An intermediate published catalog can still be the last stable
   state; raw-prefix protections must not be relaxed merely because the earliest
   origin predates that catalog.
5. At a window bound, route change, WAL transition, reorg or destructive
   metadata/maintenance boundary, perform a durable checkpoint. A pending epoch
   uses the existing ordered-publication/full-directory-sync path directly. An
   already published window needs only the final full directory sync; external
   devices were fully persisted before publication. Idle work uses the same API.
6. After process restart, validate catalog/WAL evidence and fully synchronize the
   observed catalog directory before rollback or maintenance. A process restart
   alone does not empty the kernel/device cache. Failed publication or hardening
   poisons the storage handle until reopen.

Apple explicitly documents that a barrier orders previously fsync'd writes on the
same device ahead of later I/O, without promising which writes have reached
stable media when it returns. That supports the ordering primitive; the LogEx
file dependencies, recovery window and maintenance guards still require their own
tests. Filesystems/devices must honor their advertised ordering/flush operations.
[Apple XNU fcntl manual](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fcntl.2).

## Superseded full-data-before-rename experiment

The first implementation fully persisted data and the temporary catalog before
rename, then issued ordinary directory fsync. It allowed only the latest completed
epoch plus its unfinished successor to rewind. Its explicit durable API, startup
hardening and WAL/reorg/maintenance/compaction guards remain useful in the current
implementation, but it still paid a full device flush per publication.

The isolated diagnostic was +8.27% versus original tiny history, but the actual
complete implementation failed: [63 samples per version](baselines/2026-09-11-published-ingestion-original-comparison.jsonl)
gave 6.031583 → 7.657709 ms (+26.96%); large history was +1.09%.
A controlled [four-way comparison](baselines/2026-09-11-published-ingestion-four-way.jsonl)
with 60 samples per variant gave original 6.586146 ms, durable `683bcdf8`
7.825354 ms, diagnostic 7.195896 ms and complete implementation 7.366875 ms.
The complete lifecycle improved over `683bcdf8` but remained +11.85% versus
original. Removing a temporary-name directory flush [did not help](baselines/2026-09-11-rejected-temp-name-flush.jsonl):
+1.45% versus that control, +21.34% versus original. It is not retained as an
optimization. The current ordered-publication design replaces that experiment.

The superseded lifecycle passed [all six local gates](baselines/2026-09-11-published-ingestion-gates.jsonl)
(883 tests/eight ignored). WAL/compaction guard omissions have
[failing-before](baselines/2026-09-11-published-ingestion-wal-guard-before.log)
[regressions](baselines/2026-09-11-published-ingestion-compaction-guard-before.log),
with [all five tests passing](baselines/2026-09-11-published-ingestion-guards-after.log)
when restored. These are historical evidence, not current platform acceptance.

## Current validation and remaining work

Seven focused lifecycle tests pass, including both routes, empty blocks, empty
initial data, rotation, canonical flags, repeated reopen/retry, all preceding
catalog choices, compaction protection across multiple publications, frequent
checkpoint accounting, byte/age/route boundaries, idle handling and poisoned
hardening failures. The new cross-mount matrix adds 96 predecessor cases to the
existing 32 cases; it has not yet run for this implementation.

Broader release comparisons, Linux/macOS CI gates, isolated
ExFAT recovery/performance, explicit durable-checkpoint costs, mixed idle and
maintenance workloads, and startup/space/query measurements remain required.
Clean-reopen timing fixtures do not prove power-failure durability. These are
storage-call timings, not end-to-end peer throughput. No failed profile is waived.


The first current [63-sample original-baseline comparison](baselines/2026-09-11-ordered-ingestion-original-comparison.jsonl)
on ARM APFS gives tiny historical 5.992875 → 4.208916 ms (-29.77%) and large
historical 61.897250 → 64.076583 ms (+3.52%). Both measured medians meet the ceiling;
this does not establish acceptance of other workloads or platforms. Respective p95
values are 8.379291 → 6.409542 ms and 77.476083 → 75.309458 ms. The measured
checkpoint contract now permits bounded re-ingestion; the fixture includes
publication/finalization but does not request `checkpoint_durable()`.


The [call-count regression](baselines/2026-09-11-ordered-ingestion-count-before.log)
and [byte/deadline regression](baselines/2026-09-11-ordered-ingestion-boundaries-before.log)
fail when published work is omitted from the window. [All seven current tests](baselines/2026-09-11-ordered-ingestion-bounds-after.log)
pass with the accounting restored. These are intentional guard-removal tests;
the initial fixture-construction errors were corrected separately.

All six current [local workspace gates](baselines/2026-09-11-ordered-ingestion-gates.jsonl)
pass: 885 tests/nine ignored, formatting, locked all-target check, strict Clippy,
doctests and release build. Later changes only clarify benchmark comments and
audit documentation; formatting was rechecked.
