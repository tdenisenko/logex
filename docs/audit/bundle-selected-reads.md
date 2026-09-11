# Bundle page-selection investigation

This document retains the investigation chronology and revision-specific results.
Use [the PR #130 acceptance record](pr130-acceptance.md) for the final source,
validation status, compatibility decision and performance tradeoffs.

The grouped bundle at `a0559acd` meets the original-baseline ingestion ceiling
in the recorded sync profiles, but sparse reads and retained storage remain
above the original implementation. This investigation isolates read costs;
it does not relax recovery, checksum or ingestion requirements.

## Caller coverage and reproduction

The original publication benchmark reads every column in one call. Native and
SQL query callers also use `SegmentReader::read_log_rows(Some(ids))`, which
selects and reads individual pages. Those calls repeatedly scan the bundle's
extent prefix to locate a logical range. With many tiny appended extents, a
whole-column benchmark misses that repeated work.

`LOGEX_PUBLICATION_READ_MODE=selected` now validates all rows through explicit
row IDs. The default `full` mode is unchanged. Both modes use the same input
digest, canonical/progress oracle and exact post-reopen comparison. The
`full_row_validation_ms` output includes row-ID construction in selected mode,
materialization, sorting and oracle comparison. It is not end-to-end SQL or
network latency. See [benchmark controls](benchmarks.md#sync-storage-publication).

With the same release binary on both sides, selected validation costs 26.03%
more for mixed live data (9.594 → 12.092 ms) and 82.03% more for sparse historical
data (8.526 → 15.521 ms). This is a caller-shape diagnostic, not a code regression
or optimization. The raw runner had a copied description of a different probe;
an appended correction explicitly identifies the same-binary comparison. Every
configuration's mode and matching binary identity were checked.
[Caller comparison and correction](baselines/2026-09-11-bundle-selection-cost.jsonl).

## Rejected alternatives

These experiments remain in isolated temporary source trees. None of the
following production changes is retained. Each comparison alternates paired
release processes on internal Apple Silicon APFS, uses fresh directories, keeps
OS caches warm/uncontrolled, and runs without concurrent local builds or tests.
Exact row/progress/reopen checks pass. Raw artifacts include source, binary
identities, workload parameters, individual samples and tails. No outlier is
removed. OS process-write counters are not physical NAND amplification.

- A cache of the entire immutable file up to 4 MiB reduces sparse full-row read
  time 27.52%, but sparse peak RSS rises 12.45% (21.58 → 24.26 MB). Mixed-live RSS
  rises 3.89%. It also only helps eligible full-stream calls. This memory cost
  and limited caller coverage do not justify retention.
  [Full-cache probe](baselines/2026-09-11-bundle-bulk-cache-probe.jsonl).
- Borrowing checked extent bytes instead of allocating a temporary copy has
  inconsistent effects: sparse full-row time falls 5.02%, while mixed-history
  and mixed-live times rise 5.00% and 4.48%. Under explicit selection, gains are
  only 0.15% mixed live and 0.33% sparse history. This does not demonstrate a
  worthwhile optimization.
  [Full-read probe](baselines/2026-09-11-bundle-borrowed-buffer-probe.jsonl),
  [selected-read probe](baselines/2026-09-11-bundle-selected-borrowing.jsonl).
- Storing a derived logical offset inside every in-memory extent reduces sparse
  selected-read median 30.92% (15.420 → 10.651 ms, 45 samples); p95 falls from
  16.446 to 11.328 ms. It leaves serialized fields unchanged, but enlarges each
  extent from 16 to 24 bytes. Sparse full-row reads rise 6.11%, selected-run
  reopen rises 4.19%, and selected-run peak RSS rises 4.76%. Noisy full-mode
  tails, including a candidate 87.615 ms sparse-read p95, are preserved. This
  variant is superseded by a lazy lookup experiment rather than accepted.
  [Full-mode comparison](baselines/2026-09-11-bundle-inline-offset-full.jsonl),
  [selected comparison](baselines/2026-09-11-bundle-inline-offset-selected.jsonl).

The earlier [logical-lookup probe](bundle-read-costs.md) measured full reads only
and therefore did not establish whether lookup helps selected pages. Its result
remains valid for that caller; the new benchmark closes the coverage gap.

## Lazy candidate and remaining validation

A lazy per-stream cumulative-offset table is saved as `1e27d838`. It is built only
for nonzero-offset reads of streams with at least 32 extents, lives beside the
existing physical read window, and is bound to the same immutable reader.
Full scans and startup keep the compact extent layout and allocate no lookup.
Binary search skips the earlier extents; every returned extent still passes its
complete checksum. No persisted format, write path or recovery contract changes.

Against `a0559acd` with the same benchmark selector, sparse selected-read median
falls 30.86% (15.472 → 10.697 ms, 45 samples), with p95 16.882 → 11.653 ms.
Mixed-live selection falls 2.56% (15 samples). Sparse ingestion median is -0.01%,
reopen +2.06%, and process peak RSS +0.73%. Selected-run ingestion p95 rises
23.58% because three candidate samples exceed 1,050 ms; those samples also have
larger allocated-file footprints despite identical logical bytes. That is an
observed association, not a proven filesystem cause; the confirmation below
retains and investigates these samples.
[Selected comparison](baselines/2026-09-11-bundle-lazy-offset-selected.jsonl).

Full-mode ingestion changes +0.81% mixed history, -0.62% mixed live and +1.37%
sparse history. Full-row read changes are +1.06%, +1.17% and +4.55%. Sparse warm
reopen rises 5.61% in this 15-sample run; it is +2.06% in the selected run, where
reopen invokes the same code. Full-mode sparse peak RSS rises 6.01%, although
those full reads do not allocate the new lookup. RSS includes the entire
fixture, storage and oracle process; it does not attribute allocations to the
lookup. The confirmation below investigates these differences and ingestion tails.
Logical bytes and median OS-attributed writes are unchanged.
[Full-mode comparison](baselines/2026-09-11-bundle-lazy-offset-full.jsonl).

The candidate adds a variable-length, multi-stream selection test with cloned
readers used concurrently across old and new grouped snapshots. Existing tests
also cover partial/checksummed reads, failed refill/retry, malformed tables and
immutable bounds. All 19 bundle tests and all six workspace gates pass: 900
passed/nine ignored. The integrated release fixture is byte-identical to the
measured prototype. [Validation record](baselines/2026-09-11-bundle-lazy-offset-validation.json).
The exact `1e27d838` confirmation uses another 45 samples per mode. Sparse
selected validation improves 31.20% (15.091 → 10.383 ms), p95 16.663 → 11.545 ms;
full-row validation rises 4.93% (8.373 → 8.786 ms), p95 8.953 → 9.385 ms. That
small full-read cost is repeatable and retained explicitly alongside the selected
read gain; it does not resolve the larger original-baseline full-read gap.
Reopen medians are +2.35%/+3.13% and process RSS -0.07%/+0.30% in full/selected
mode. The earlier >5% reopen/RSS increase does not repeat at that magnitude.
Ingestion medians are -0.85%/-0.73%, p95 -5.69%/+8.59%. The initial +23.58%
ingestion p95 is not repeated at that magnitude; tail variation remains visible
and is not attributed to a proven cause. Logical bytes and median process writes
are unchanged in both modes. All exact oracles pass.
[Full confirmation](baselines/2026-09-11-bundle-lazy-confirm-full.jsonl),
[selected confirmation](baselines/2026-09-11-bundle-lazy-confirm-selected.jsonl).
All six Linux/macOS CI jobs pass in run `34591575477`. Exact current release
binaries pass the targeted ExFAT checks on both Apple Silicon and Intel: 19 bundle
tests and five query audit tests/one ignored. This reader change adds no format
or filesystem primitive; the preceding `a0559acd` full storage/128-case cross-mount
suites are recorded separately. Both exact disposable images were detached.
[Current platform evidence](baselines/2026-09-11-bundle-lazy-platform-validation.jsonl).

Sparse page fragmentation, retained obsolete payloads and safe compaction/file
lifetime remain separate unresolved costs. PR #130 remains draft and unmerged.
