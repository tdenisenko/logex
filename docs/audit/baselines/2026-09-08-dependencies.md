# Dependency release comparison — 2026-09-08

Base runtime: `afe5939c` (identical benchmark/runtime source to `0438627c`).
Candidate runtime: `50fe56b91b1572cf7c256a90b8e82ff53425f25c`.
Only dependency resolution changes runtime behavior in this comparison.

## Method

Same Mac14,15, 8 logical CPUs, 16 GiB RAM, aarch64 macOS 26.6.2 (25G83),
internal APFS SSD and pinned Rust/Cargo versions as the
[initial baseline](2026-09-07.md). Approximately 161 GiB free during this run.
Both executables were built before measurement; no compiler ran concurrently.
The baseline executable was copied before rebuilding the candidate.

For each dense/sparse profile, run five base/candidate pairs, alternating AB/BA.
Investigate differences above 5% with ten additional alternating pairs; retain
all 15 pairs in the combined tables below. Each executable invocation uses one
fresh-directory iteration, 200,000 rows, 50,000 rows/segment, 8,192 rows/write,
and four query threads. Fixtures, release profile and harness are identical.
All 60 invocations passed the exact query/storage assertions and both fixture
digests matched the original baseline logs. All file byte counts and compacted
segment counts also matched the original baseline for each profile.

Example invocation for either already-built executable:

```sh
LOGEX_BENCH_PROFILE=dense LOGEX_BENCH_ROWS=200000 LOGEX_BENCH_REPEATS=1 \
LOGEX_BENCH_SEGMENT_ROWS=50000 LOGEX_BENCH_BATCH_ROWS=8192 LOGEX_BENCH_WORKERS=4 \
/usr/bin/time -l /path/to/audit_harness benchmark_storage_indexes_and_queries \
  --ignored --nocapture --test-threads=1
```

OS caches were not evicted, queries were warmed once, and this was an ordinary
desktop session without CPU isolation. With 15 samples, nearest-rank p95 is the
maximum. These are local fixture timings, not production tail-latency estimates.
Process RSS includes fixture/oracle/result allocations. It does not measure
per-query memory. Physical write amplification was not measured.

Candidate lockfile SHA-256: `6dcb648496174b61ba135432d0f1eec51a9d1ae9dbcbd9f8a5e6483906e7e50c`.

Harness SHA-256: `0df8e9a28642760b1a7b609a0c4c4c0f3aaef3f4484150f8367435bc55a0bc12`.

Executable SHA-256 values and raw configuration, timing, storage and RSS records
are preserved in [initial five pairs](2026-09-08-dependency-comparison.jsonl)
and [ten follow-up pairs](2026-09-08-dependency-comparison-followup.jsonl).

## Dense timing results

| Operation | Base median ms | Candidate median ms | Change | Base p95 ms | Candidate p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| compaction | 72.859 | 77.014 | +5.70% | 78.120 | 131.405 |
| concurrent_native_queries | 121.024 | 120.529 | -0.41% | 144.056 | 164.406 |
| historical_storage_ingest | 213.453 | 225.924 | +5.84% | 301.159 | 283.908 |
| index_build | 225.294 | 230.750 | +2.42% | 245.615 | 420.592 |
| live_storage_ingest | 408.694 | 403.709 | -1.22% | 474.668 | 457.102 |
| native_filter | 90.718 | 90.701 | -0.02% | 118.310 | 129.236 |
| reopen | 1.244 | 1.258 | +1.16% | 1.375 | 2.207 |
| sql_count | 4.672 | 4.722 | +1.07% | 7.005 | 5.332 |
| sql_ordered | 9.951 | 10.463 | +5.15% | 12.000 | 25.822 |

Base process RSS: median 524,091,392 bytes; maximum 534,904,832 bytes.

Candidate process RSS: median 521,764,864 bytes; maximum 649,773,056 bytes.

## Sparse timing results

| Operation | Base median ms | Candidate median ms | Change | Base p95 ms | Candidate p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| compaction | 78.859 | 80.045 | +1.50% | 91.778 | 86.470 |
| concurrent_native_queries | 117.939 | 120.485 | +2.16% | 145.874 | 131.537 |
| historical_storage_ingest | 228.472 | 232.500 | +1.76% | 273.497 | 310.806 |
| index_build | 437.998 | 448.044 | +2.29% | 471.302 | 502.681 |
| live_storage_ingest | 408.309 | 412.519 | +1.03% | 468.844 | 475.206 |
| native_filter | 88.822 | 88.041 | -0.88% | 123.538 | 96.893 |
| reopen | 1.206 | 1.259 | +4.38% | 2.417 | 1.337 |
| sql_count | 4.680 | 4.652 | -0.61% | 5.910 | 47.078 |
| sql_ordered | 9.962 | 10.190 | +2.29% | 19.659 | 21.361 |

Base process RSS: median 627,769,344 bytes; maximum 640,548,864 bytes.

Candidate process RSS: median 628,424,704 bytes; maximum 640,303,104 bytes.

## Investigation and accepted tradeoff

The initial five dense pairs showed +11.36% compaction, +21.46% historical
writes and +5.30% ordered SQL median time. The ten additional pairs showed
+0.39%, +5.19% and +2.53% respectively. Initial sparse concurrent-query and
live-write differences above 5% reduced to +0.71% and effectively zero in the
follow-up. This variability prevents attributing every initial difference to
changed dependencies. Combined results, including outliers, are retained above.

Dense historical writes remain slower in both groups: +5.84% combined median
(about 12.5 ms per 200,000-row fixture). Inspection found no changed storage
algorithm or direct RNG use in that path. The query/storage dependency graph
activates updated ruint/rand/fastrand; the network and certificate updates are
outside this harness. This does not establish which dependency, code generation,
allocation behavior or filesystem timing caused the difference. Isolating that
cost belongs in the storage batch's write-path profiling; it is not dismissed
as noise. Dense compaction and ordered SQL also exceed 5% in the combined
medians, although those thresholds did not repeat in the ten-pair follow-up.
Candidate dense maximum RSS has an outlier (650 MB versus 535 MB base), while
median RSS differs by less than 1%; retain this observation for allocation
profiling rather than claiming a memory improvement.

Retain the compatible security and arithmetic fixes with these disclosed local
performance costs. This is a correctness/security update, not an optimization
claim. Reverting to affected versions to improve a benchmark is not the chosen
tradeoff. Later storage/query profiling must use these results as its baseline.

## Validation

Formatting, locked all-target workspace checking, strict all-target Clippy,
720 workspace tests, explicit doc tests, release node linking and the release
benchmark build all passed locally. One full-size benchmark is intentionally
ignored by ordinary test runs and was executed explicitly 60 times here.
The advisory scan still exits nonzero for documented remaining matches; see
[dependency remediation](../dependency-remediation.md). Cross-platform CI and
merge status are recorded in the PR. No production deployment or soak occurred.
