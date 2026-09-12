# SQL predicate and inclusive-index performance acceptance

Baseline `f19f47c4651ca9b4ef412030a32240ee48ecb4de` versus implementation `0329745c0f26753bb37a3e354e40b65dd25ff625`. All validated source hashes, including the dashboard HTML, match the implementation commit. The baseline uses the identical corrected fixture: both revisions query canonical lowercase address literals and verify the same intended rows. Refreshed archive mtimes, fresh Cargo artifacts and separately copied executables prevent shared-target reuse. Distinct executable hashes and complete fixture/source/runner metadata are retained in [primary samples](2026-09-12-sql-predicates.jsonl).

[Confirmation samples](2026-09-12-sql-predicates-confirmation.jsonl), [all summaries](2026-09-12-sql-predicates-summary.jsonl) and [local gate records](2026-09-12-sql-predicates-gates.jsonl) are retained. No failed or inconvenient benchmark observations were discarded. The earlier fixture smoke failure and corrected smoke are linked from the audit record.

## Conditions

Mac14,15, eight logical CPUs, 16 GiB RAM, macOS 26.6.2, internal APFS, pinned nightly-2026-08-24 (rustc fb6531d55). Each dense/sparse fixture has 200,000 rows, 50,000-row segments, 8,192-row write batches and four concurrent query workers. Standard runs use three fresh datasets per process. Five primary alternating process pairs plus ten fixed confirmation pairs yield 45 samples per metric/profile/revision and 15 whole-process peak RSS samples. DataFusion uses five pairs and 20 warm iterations per shape: 100 samples per shape/profile/revision, five RSS samples. All exact row/order/value/count oracles pass.

Caches were not forcibly evicted; no concurrent builds/tests ran, and other applications were not stopped. Peak RSS includes fixture buffers, result materialization and allocator retention; it is not steady-state node RAM. The standard write fixture covers generic durable WAL storage, including final checkpoints, not P2P or the production combined-sync pipeline. The implementation changes no storage/ingestion write path. p95 uses nearest rank. No production data, external volume or services were accessed.

## Primary observations and fixed confirmation

The primary 15-sample dense run showed historical-write p95 +10.19% and reopen p95 +13.80%; with 15 samples that percentile is the maximum. Sparse concurrent-query p95 was +8.22%, and sparse whole-process RSS median/p95 +5.30%/+11.67%. These observations triggered a predeclared ten additional alternating pairs per standard profile. Confirmation used unchanged binaries, fixtures and parameters. Both the primary and separate confirmation summaries remain available, together with every raw sample. DataFusion did not show a positive change above 5%, so it was not repeated. The sparse confirmation alone also showed COUNT/ordered p95 +11.66%/+13.59%; the original sparse samples had different tails, and the combined values are +0.09%/+3.03%. These separate observations are retained rather than suppressed by the combined summary.

## Combined standard results

| Profile / metric | Baseline median | Candidate median | Median change | p95 change |
| --- | ---: | ---: | ---: | ---: |
| dense / compaction | 77.385 ms | 79.172 ms | +2.31% | +1.83% |
| dense / concurrent_native_queries | 110.550 ms | 109.769 ms | -0.71% | -0.50% |
| dense / historical_storage_ingest | 296.200 ms | 295.972 ms | -0.08% | +7.52% |
| dense / index_build | 251.180 ms | 251.115 ms | -0.03% | +0.33% |
| dense / live_storage_ingest | 485.000 ms | 479.369 ms | -1.16% | -0.92% |
| dense / native_filter | 87.657 ms | 87.516 ms | -0.16% | -4.97% |
| dense / reopen | 6.698 ms | 6.814 ms | +1.72% | +7.63% |
| dense / sql_count | 4.456 ms | 4.489 ms | +0.73% | +7.00% |
| dense / sql_ordered | 9.384 ms | 9.443 ms | +0.64% | -2.05% |
| dense / peak_rss | 765.422 MiB | 761.422 MiB | -0.52% | +0.45% |
| sparse / compaction | 76.480 ms | 76.183 ms | -0.39% | -3.58% |
| sparse / concurrent_native_queries | 109.467 ms | 108.377 ms | -1.00% | -0.95% |
| sparse / historical_storage_ingest | 288.216 ms | 288.046 ms | -0.06% | -0.33% |
| sparse / index_build | 462.396 ms | 462.137 ms | -0.06% | +1.22% |
| sparse / live_storage_ingest | 475.962 ms | 475.257 ms | -0.15% | +0.38% |
| sparse / native_filter | 86.637 ms | 86.690 ms | +0.06% | +0.33% |
| sparse / reopen | 6.797 ms | 6.702 ms | -1.40% | -8.15% |
| sparse / sql_count | 4.464 ms | 4.457 ms | -0.15% | +0.09% |
| sparse / sql_ordered | 9.433 ms | 9.447 ms | +0.14% | +3.03% |
| sparse / peak_rss | 687.891 MiB | 687.328 MiB | -0.08% | +0.01% |

Logical file sizes match exactly in every primary standard run: dense raw/indexed/compacted bytes are 68,497,907 / 80,557,319 / 23,655,901; sparse values are 68,497,907 / 120,554,991 / 72,607,756. Four segments compact in each layout. These are logical file sizes, not device write-amplification counters.

## Primary DataFusion results

| Profile / metric | Baseline median | Candidate median | Median change | p95 change |
| --- | ---: | ---: | ---: | ---: |
| dense / datafusion_aggregate | 92.951 ms | 92.730 ms | -0.24% | +0.20% |
| dense / datafusion_narrow | 107.591 ms | 107.337 ms | -0.24% | +0.87% |
| dense / datafusion_wide | 309.788 ms | 309.142 ms | -0.21% | -2.73% |
| dense / peak_rss | 357.438 MiB | 357.250 MiB | -0.05% | -0.02% |
| sparse / datafusion_aggregate | 92.121 ms | 91.786 ms | -0.36% | -1.60% |
| sparse / datafusion_narrow | 109.572 ms | 109.273 ms | -0.27% | +0.17% |
| sparse / datafusion_wide | 308.732 ms | 308.796 ms | +0.02% | -0.71% |
| sparse / peak_rss | 350.250 MiB | 345.938 MiB | -1.23% | -26.32% |

## Phase-isolation investigation

The remaining combined dense historical-write/reopen/COUNT p95 observations (+7.52%/+7.63%/+7.00%) triggered a separate fixed five alternating pairs, with 20 iterations per process: 100 samples per metric/revision. The [diagnostic fixture](2026-09-12-sql-predicates-isolation.rs) is appended identically to the validated harness in isolated revision archives; production source is unchanged. [All source, build, parameter and measurement records](2026-09-12-sql-predicates-isolation.jsonl) are retained. Historical writes run first on fresh directories, without preceding concurrent-query phases. Reopen and warmed queries repeatedly use one indexed/compacted dataset, with all original exact oracles. These results remain separate from the mixed-workload samples.

| Metric | Baseline median | Candidate median | Median change | p95 change |
| --- | ---: | ---: | ---: | ---: |
| historical_storage_ingest | 292.213 ms | 291.911 ms | -0.10% | +0.07% |
| native_filter | 85.897 ms | 85.800 ms | -0.11% | -1.46% |
| reopen | 5.740 ms | 5.621 ms | -2.07% | -8.96% |
| sql_count | 4.446 ms | 4.431 ms | -0.36% | -29.39% |
| sql_ordered | 9.356 ms | 9.352 ms | -0.04% | -21.01% |
| peak_rss | 1805.547 MiB | 1799.469 MiB | -0.34% | +0.19% |

No positive median/tail difference in this diagnostic exceeds 1%. Its longer-lived processes retain more allocator/fixture memory than the three-iteration mixed runs; RSS is compared only between equivalent isolation runs. The differing tails across mixed and isolated runs support workload/environment variability rather than a stable query or ingestion cost increase. This is an inference, not proof that every scheduling/IO interaction is absent. The baseline also has larger SQL tail observations in the isolation run; no speedup is claimed from those differences.

Acceptance retains the mixed dense +7–8% tail observations as explicit limits, below the user's 10% ceiling, with unchanged ingestion code and exact file sizes. Combined standard ingestion medians are −1.16% to −0.06%; all DataFusion medians are −0.36% to +0.02%. No performance optimization is claimed. The later integrated offline workload and actual live-sync gates must still measure interactions and node memory; this patch is accepted for correcting demonstrated query results without a demonstrated material isolated-path regression.

All six local gates pass: format, check, Clippy, 992 workspace tests (13 ignored), documentation tests and release node build. Release query/index regressions and the actual dashboard-function generator check pass. This milestone does not complete the wider offline audit or establish live-sync/release readiness.
