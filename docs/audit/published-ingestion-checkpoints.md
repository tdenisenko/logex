# Bounded sync progress publication

This is an **unmerged implementation in validation**, following `683bcdf8` in
PR #130. The user authorizes bounded verified re-ingestion, engineering judgment
and fresh incompatible directories, while retaining the 10% ingestion ceiling.
No production directory or service uses this implementation.

The [live-bundle successor](live-segment-bundles.md) added catalog v7 / segment v5
support for hot bundles. The [grouped-table follow-up](bundle-table-groups.md)
uses catalog v8 / segment v6 / bundle v4; the current [table-record candidate](bundle-table-codecs.md)
uses v9 / v7 / v5. These retain the publication contract below.
The measurements here describe the preceding implementation unless explicitly
identified otherwise; its Intel ExFAT live failure remains recorded.

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
existing 32 cases; it passes on the local isolated APFS/ExFAT image in both mount directions.

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

Current [isolated ExFAT validation](baselines/2026-09-11-ordered-ingestion-exfat.jsonl)
passes 177 storage tests/four ignored and all 128 cross-mount cases. The runner
verified and detached its exact disposable image. All six Linux/macOS CI jobs
also pass at c63fb9ed in run 34570017540.


## Broader local comparisons and explicit durability cost

The [five-pair/three-sample broad run](baselines/2026-09-11-ordered-ingestion-broad-comparison.jsonl)
uses the same default ordered publication and exact v3 fixtures with the added
benchmark-only durable-checkpoint switch disabled. Current changes versus original
are tiny history -39.11%, tiny chunks -68.98%, sparse history -67.92%, short history
-58.41%, grouped live -81.87%, sustained history -80.69%, large history +8.12% and
per-block live publication -46.87%. All measured medians are within 10%; the large
profile's difference from the earlier +3.52% result was investigated with a
controlled original/current/harness-switch comparison below. No result is discarded.

`LOGEX_PUBLICATION_DURABLE_CHECKPOINT=1` requests the strong API for the final and
any per-block checkpoints. A separate [same-binary comparison](baselines/2026-09-11-checkpoint-modes.jsonl)
uses five alternating process pairs × three samples, with identical data/oracles:

| Workload | Ordered median | Durable median | Extra elapsed time |
| --- | ---: | ---: | ---: |
| Tiny historical batch | 4.623667 ms | 6.382750 ms | 1.759083 ms |
| 17 grouped live blocks | 38.028000 ms | 42.134375 ms | 4.106375 ms |
| 32 live blocks, checkpoint after each | 349.912000 ms | 515.867958 ms | 165.955958 ms |

These measure the additional promise of latest-progress durability, not a different
compression format or acceptance exemption. Default bounded sync does not request
a hard checkpoint after every block. The new benchmark switch is explicit in JSON
output; inputs, expected rows, progress and clean-reopen checks are unchanged.
Its default correctness test, both measured modes, formatting and strict workspace
all-target Clippy pass. Production code remains c63fb9ed.


A [nine-triplet/seven-sample large-history control](baselines/2026-09-11-ordered-large-control.jsonl)
gives original 63.105042 ms, saved ordered binary 65.962792 ms (+4.53%) and the
optional-switch harness with that switch disabled 64.148750 ms (+1.65%). Both
candidate p95 values improve; the optional-switch binary differs by -2.75% from
the saved binary. The broad +8.12% regression did not repeat at that magnitude.
Retain all measurements and keep examining large-history variability in later
integrated runs; the fixture switch is not a claimed production optimization.
[CI and focused fixture validation](baselines/2026-09-11-checkpoint-modes-validation.jsonl).

Current Intel comparisons confirm APFS tiny history -73.43%, short history -72.08%
and per-block live publication -74.72%. On isolated ExFAT, tiny history is -38.91%
and short history -76.88%, but per-block live still **fails at +26.31%**
(13,259.555959 → 16,748.521002 ms). The ordering change does not solve that profile.
Raw [Intel APFS](baselines/2026-09-11-ordered-ingestion-intel-apfs-performance.jsonl)
and [Intel ExFAT](baselines/2026-09-11-ordered-ingestion-intel-exfat-performance.jsonl)
records include five alternating pairs × three samples, binary identities, host
metadata, exact oracles and runner source. The disposable image detached after
timing. Full current Intel ExFAT correctness subsequently passed 177 tests/four ignored
and all 128 cross-mount cases; the exact disposable image detached again.
[Current Intel build and validation record](baselines/2026-09-11-ordered-ingestion-intel-exfat.jsonl).
Acceptance remains blocked by performance, without changing the user's ceiling.


## ExFAT live I/O investigation

An [instrumented release profile](baselines/2026-09-11-live-file-flush-profile.jsonl)
retains the complete v3 row/progress/reopen oracles. For 128 live blocks and a
publication after each, APFS has 3,720 file flushes and ExFAT has 6,120; both have
492 ordering calls. Scope times overlap and include instrumentation, so they are
not an acceptance comparison. Column append, manifest publication and checkpoint
I/O dominate; cached-header encoding is about 190–220 ms for the whole workload.
No unsupported-barrier fallback was observed in these local image runs.

A separate [two-block inventory](baselines/2026-09-11-live-file-inventory.jsonl)
confirms 20 primary segment files on APFS and the same files plus 20 regular
`._` companions on ExFAT. Apple documents this companion convention in its
[extended-attribute implementation](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/vfs/vfs_xattr.c);
the [AppleDouble MIME specification](https://www.rfc-editor.org/rfc/rfc1740.html)
separates the file data from resource/attribute information.

A [three-triplet/three-sample diagnostic](baselines/2026-09-11-rejected-sidecar-flush.jsonl)
skipping regular `._` leaves in the tree flush does not improve enough to retain:
original/current/diagnostic medians are 7,423.141791 / 6,295.023791 / 6,251.017208 ms.
The diagnostic is only -0.70% versus current, and its p95 worsens. All temporary
source is restored and both profiling/timing images detached. ARM ExFAT current
is faster than original in this run; the repeated Intel +26.31% remains a failure.
This diagnostic did not establish a safe general companion-name exclusion policy.
At that stage, `collect_indexes` listed arbitrary files as Custom indexes,
including companions. The later [index publication cleanup](index-checkpoints.md)
removes this unused manifest list entirely; index readers use the source-bound
checkpoint and known file names.

The [live bundle probe](live-segment-bundles.md) improves the formerly failing
Intel profile by 37.50% against the original baseline. Integration and expanded
recovery tests are underway, with separate performance/platform acceptance still
required. It reuses the immutable representation to reduce column files and
replacements, including first-bundle rollback and generic WAL transitions.


## Deterministic count-bound validation

The Linux Test job at e8e8e43e (run 34572705829) failed because the real five-second
deadline fired while the count-bound test expected all 64 publications to remain
in one window. It observed one batch in a new window when it expected 24. The
other five jobs passed; all six jobs subsequently passed at 6ce06c13, but the
wall-clock assumption remained fragile.

Checkpoint tests now have a thread-local controlled clock. The count-bound case
freezes time while preserving its origin/deadline/counter assertions; production
still uses the real monotonic clock and the same five-second threshold. A past
virtual instant makes the regression fail immediately if elapsed checks bypass
the controlled clock. The before failure, seven passing checkpoint tests, strict
Clippy/check/format results and six-job CI snapshot are retained in the
[clock validation record](baselines/2026-09-11-checkpoint-clock.jsonl).
