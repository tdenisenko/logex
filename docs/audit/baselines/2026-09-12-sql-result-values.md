# SQL result value performance acceptance

Baseline `4be5250c` versus candidate `2ca975df`. All source hashes captured by the
validation runner were checked against the committed candidate. The identical
tracked benchmark fixture was copied to the isolated baseline source before
building; source mtimes were refreshed and fresh artifacts/distinct executable
hashes were required. Raw metadata contains the complete fixture, build/runner
scripts, source hashes/diff, hardware, filesystem and pinned toolchain.

[Primary samples](2026-09-12-sql-result-values.jsonl),
[confirmation samples](2026-09-12-sql-result-values-confirmation.jsonl),
[combined summaries](2026-09-12-sql-result-values-summary.jsonl), and
[local gates](2026-09-12-sql-result-values-gates.jsonl) are retained.

The [initial candidate measurements](2026-09-12-sql-result-values-initial.jsonl)
and [interrupted sparse run](2026-09-12-sql-result-values-interrupted.log) remain
separate evidence. Candidate `9e9aefd3` completed 500 dense samples per shape
before a hidden-temporal-value regression was found. Those samples did not
substitute for the final-source comparison below.

## Conditions and limits

Mac14,15, eight logical CPUs, 16 GiB RAM, internal APFS, macOS 26.6.2 and pinned
nightly-2026-08-24. No concurrent builds/tests; other applications were not stopped.
Caches were not forcibly evicted. No external volume or production data access.
Each fixture has 200,000 coherent rows, 50,000-row segments, 8,192-row ingestion
batches and four native query workers. Dense and sparse profiles are separate.

Each DataFusion process creates/indexes/compacts/reopens a fresh fixture, warms
three queries, then executes 100 iterations per shape in rotating order. Five
alternating process pairs yield **500 samples per shape/profile/revision**.
Narrow and ten-column wide queries order by block arithmetic to force DataFusion,
return at most 1,000 rows, and check every value against an independent oracle.
MAX and COUNT of arithmetic provide an aggregate control. All these baseline
results were correct; previously broken decimal/nested results are excluded from
speed comparisons. These measurements cover the general engine, including its
column reads, rather than only the converter.

Standard storage/index/query controls use five initial and ten confirmation
alternating process pairs, with three fresh datasets per process:
**45 samples per metric/profile/revision**. The fixed confirmation set investigates
initial dense native-filter p95 +7.76%, sparse compaction p95 +7.45% and sparse
concurrent-query median +5.55%. All original samples remain in the aggregate.
Their ingestion paths are generic WAL writes. Production combined sync and its
storage implementation are unchanged by this query-only fix; these numbers do
not claim P2P throughput or replace the previous combined-sync acceptance.
Peak RSS has five DataFusion and fifteen standard process samples, includes fixture/result buffers and allocator
retention, and does not represent steady-state node memory. p95 is nearest-rank.

## Measurements

| Family/profile | Metric | Baseline median | Candidate median | Median change | p95 change |
| --- | --- | ---: | ---: | ---: | ---: |
| datafusion/dense | datafusion_aggregate | 93.045 ms | 92.898 ms | -0.16% | +0.31% |
| datafusion/dense | datafusion_narrow | 107.693 ms | 107.553 ms | -0.13% | -0.21% |
| datafusion/dense | datafusion_wide | 310.176 ms | 309.776 ms | -0.13% | -1.34% |
| datafusion/dense | peak_rss | 355.672 MiB | 355.797 MiB | +0.04% | +0.81% |
| datafusion/sparse | datafusion_aggregate | 92.145 ms | 92.006 ms | -0.15% | -0.09% |
| datafusion/sparse | datafusion_narrow | 109.757 ms | 109.624 ms | -0.12% | +0.16% |
| datafusion/sparse | datafusion_wide | 309.420 ms | 309.499 ms | +0.03% | -2.36% |
| datafusion/sparse | peak_rss | 348.203 MiB | 347.703 MiB | -0.14% | +0.13% |
| standard/dense | compaction | 86.407 ms | 85.885 ms | -0.60% | +0.38% |
| standard/dense | concurrent_native_queries | 113.626 ms | 116.147 ms | +2.22% | +2.27% |
| standard/dense | historical_storage_ingest | 294.938 ms | 297.804 ms | +0.97% | +1.69% |
| standard/dense | index_build | 252.212 ms | 253.202 ms | +0.39% | -1.06% |
| standard/dense | live_storage_ingest | 483.212 ms | 483.970 ms | +0.16% | +5.91% |
| standard/dense | native_filter | 88.629 ms | 88.105 ms | -0.59% | -1.95% |
| standard/dense | reopen | 6.834 ms | 6.832 ms | -0.04% | -0.32% |
| standard/dense | sql_count | 4.507 ms | 4.563 ms | +1.25% | -12.80% |
| standard/dense | sql_ordered | 9.372 ms | 9.450 ms | +0.83% | +1.57% |
| standard/dense | peak_rss | 747.047 MiB | 757.328 MiB | +1.38% | -1.67% |
| standard/sparse | compaction | 75.563 ms | 74.921 ms | -0.85% | +3.19% |
| standard/sparse | concurrent_native_queries | 108.887 ms | 115.461 ms | +6.04% | +3.47% |
| standard/sparse | historical_storage_ingest | 289.576 ms | 286.900 ms | -0.92% | -0.78% |
| standard/sparse | index_build | 461.215 ms | 460.958 ms | -0.06% | -5.73% |
| standard/sparse | live_storage_ingest | 471.148 ms | 463.015 ms | -1.73% | +4.12% |
| standard/sparse | native_filter | 87.448 ms | 87.022 ms | -0.49% | +0.78% |
| standard/sparse | reopen | 6.603 ms | 6.599 ms | -0.06% | +7.19% |
| standard/sparse | sql_count | 4.520 ms | 4.484 ms | -0.79% | -7.07% |
| standard/sparse | sql_ordered | 9.457 ms | 9.468 ms | +0.12% | +1.23% |
| standard/sparse | peak_rss | 679.734 MiB | 675.438 MiB | -0.63% | +1.35% |

## Isolated concurrent-query and reopen investigation

The mixed sparse concurrent-query median remained +6.04% after confirmation.
Mixed sparse reopen p95 was +7.19% (about 0.55 ms); dense generic live-ingestion
p95 was +5.91% (30.35 ms). These observations remain in the table and raw data.
The generic live-ingestion primary and confirmation p95 changes were +1.64% and
+0.23% separately; the different combined quantile is retained rather than hidden.

An [isolated investigation](2026-09-12-sql-result-values-warm.jsonl) uses the same
commits and 200,000-row fixtures with an identical additional test in both source
archives. After ingestion/indexing/compaction/reopen, it repeats four-client native
queries against the unchanged dataset, checking every returned row outside timing,
and then measures repeated opens. Five alternating process pairs provide 150
query groups and 150 opens per profile/revision. Source mtimes, fresh artifacts,
executable hashes, fixture, build script and runner are retained in its metadata.
No production source changed for this experiment. It isolates repeated operations;
it does not erase or prove a cause for the mixed-workload observations.

| Profile | Metric | Baseline median (ms) | Candidate median (ms) | Median change | p95 change |
| --- | --- | ---: | ---: | ---: | ---: |
| dense | warm_concurrent_native_queries | 111.707 | 110.273 | -1.28% | -12.04% |
| dense | warm_reopen | 5.762 | 5.690 | -1.25% | -3.54% |
| sparse | warm_concurrent_native_queries | 112.318 | 113.300 | +0.87% | +4.75% |
| sparse | warm_reopen | 5.872 | 5.827 | -0.77% | -8.88% |

## Disposition

Accept the correctness fixes with the mixed-workload costs explicitly recorded.
The complete final-source DataFusion comparison has median changes −0.16% to
+0.03%, p95 changes −2.36% to +0.31%, and peak-memory medians −0.14% to +0.04%.
Native SQL control medians change −0.79% to +1.25%. Generic ingestion medians change
−1.73% to +0.97%, with all measured ingestion p95 increases below 10%. There is no
storage/production-ingestion implementation change and no P2P throughput claim.

Sparse mixed concurrent queries retain the +6.04% median observation; isolated
repeated-query median is +0.87%, p95 +4.75%. Repeated-open medians improve on both
profiles. This supports proceeding within the user's 10% performance ceiling,
while preserving the remaining mixed latency differences as measured limits.
Do not call them a proven noise source, claim an optimization from these small
changes, or substitute these fixtures for the integrated audit and live-sync soak.
All exact correctness oracles and all six local gates pass (978 tests, 11 ignored).
Linux/macOS CI and merge remain the final workflow gates for this milestone.
