# Sparse bundle read costs

Follow-up within draft PR #130. The integrated ingestion comparisons pass the
original-baseline ceiling, but 1,024 tiny historical calls leave 9.35 MB for 960
logs and full-row validation takes about 15 ms versus about 1 ms originally.
This document separates read costs from the remaining table-growth problem.

## Metadata attribution

One retained disposable fixture contains a 9,344,517-byte bundle. Its 960 tables
occupy 8,969,280 bytes (96%); full table snapshots account for 7,830,960 bytes.
LZ4 and Zstd level 1 reduce those table bytes to 2,926,197 and 1,667,535 bytes in
an isolated codec diagnostic, with exact decode round trips. Compression alone
would leave substantial overhead and does not remove repeated snapshots.

This diagnostic uses a standalone release-default program and fixed codec order;
its CPU timings are not integrated performance acceptance. No codec or stored
format change is retained. [Input identity, program and raw diagnostic](baselines/2026-09-11-sparse-bundle-table-profile.jsonl).

## Read-ahead policy

A bundle reader now retains one bounded physical read window. It reads ahead
only when the next extent of the same column starts within 4 KiB of the current
extent. Large or isolated extents keep the original direct-read path. The window
is aligned to 64 KiB and may extend less than 4 KiB to finish a crossing extent;
it never extends beyond the reader's immutable snapshot. This avoids seeking
separately for many nearby small headers/payloads without fetching large amounts
of unrelated column data.

Every returned extent still passes its complete checksum, including partial row
selections. A failed refill invalidates the old window before changing its offset;
otherwise retry could associate old bytes with the new position. Explicit full
integrity checks clear the cache and reread the backing file. No file encoding,
write ordering, recovery boundary or checksum requirement changes.

The added regression crosses windows and extent boundaries, repeats warm reads,
truncates an appended suffix to the pinned snapshot, forces a short refill,
restores the file and retries, and detects later physical corruption. Disabling
refill invalidation makes the retry fail with a stale-cache checksum mismatch;
restoring it passes. [Guard regression](baselines/2026-09-11-bundle-read-refill-before.log).

## Measurements and rejected variants

All comparisons alternate identical release fixtures on internal ARM APFS with
no concurrent local build/test workload and no cache eviction. All exact row,
progress and reopen oracles pass. Ingestion includes the final checkpoint.
Full-row validation includes reading, sorting and oracle comparison; it is not
SQL latency or P2P throughput. The baseline here is saved `d5115868` behavior,
not the original pre-audit baseline.

The initial indiscriminate window improves sparse reads about 42%, but increases
mixed-live full-row cost 15% and mixed-history warm reopen 14%. It is superseded.
[Initial comparison](baselines/2026-09-11-bundle-read-window-initial.jsonl).

An independent logical-offset/binary-search experiment does not materially
improve the sparse workload: full-row cost changes +0.20%, ingestion -0.11%.
It is removed; there is no retained offset vector/field or lookup abstraction.
[Lookup experiment](baselines/2026-09-11-bundle-logical-lookup-probe.jsonl).

The narrowed policy produces these median changes against saved behavior:

| Workload | Samples each | Ingestion | Full-row validation | Warm reopen |
| --- | ---: | ---: | ---: | ---: |
| Mixed history | 15 | +0.64% | -0.02% | +0.70% |
| Mixed live | 15 | -0.14% | +1.47% | -0.58% |
| Sparse history | 15 | -1.23% | -41.00% | -40.50% |
| Sparse live | 3 | +1.12% | -43.45% | -27.30% |

File sizes and OS-attributed writes are unchanged. Process peak RSS medians are
within +1% for mixed workloads and lower for sparse workloads, but no independent
memory improvement is claimed. One sparse-history sample (pair 2, iteration 2)
stalls in both ingestion and reading: 1,016.986 / 19.780 ms. It makes initial p95
worse despite the median gain; this sample is retained and the focused repeat below resolves
the tail concern. Sparse live has only three samples and is not tail-latency acceptance.
[Narrowed comparison](baselines/2026-09-11-bundle-read-ahead.jsonl).

All six workspace gates pass with the strengthened refill test: 896 passed,
nine ignored. [Validation record](baselines/2026-09-11-bundle-read-validation.json).
The focused repeat uses nine alternating pairs × five samples per revision.
Sparse read medians improve 40.80% (14.504 → 8.586 ms), with p95 improving 39.91%
(15.227 → 9.150 ms). Reopen improves 41.11% (14.517 → 8.549 ms). Ingestion median
is -0.75% (826.297 → 820.111 ms), p95 +1.57% (880.057 → 893.907 ms), with unchanged
bytes and exact oracles. The initial simultaneous stall does not repeat as a tail
regression. No original sample was excluded.
[Focused confirmation](baselines/2026-09-11-bundle-read-confirmation.jsonl).

Exact `9215da2c` release binaries pass on disposable ExFAT images on both Apple
Silicon and Intel: 185 storage tests (four ignored), five query tests (one ignored),
and all 128 cases in three cross-mount recovery tests. Both images were detached
after validation. All six Linux/macOS CI jobs pass in run `34582923672`.
[Platform evidence and binary identities](baselines/2026-09-11-bundle-reader-platform-validation.jsonl).

Excessive table growth and the gap against original sparse reads remain
unresolved; this read optimization does not finish PR #130.
