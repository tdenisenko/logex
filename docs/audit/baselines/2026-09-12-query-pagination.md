# Query pagination release comparison

Source `7ebc3795` (production `85950b57`) versus `fb858187`. Only native/SQL query
execution changes; storage, indexes, ingestion code, Cargo files and the standard
benchmark fixture are unchanged. [Initial](2026-09-12-query-pagination-initial.jsonl)
and [confirmation](2026-09-12-query-pagination-confirmation.jsonl) records contain
source snapshots/diffs, executable/fixture hashes, runners and raw measurements.

Mac14,15, 16 GiB RAM, eight CPUs, macOS 26.6.2, internal APFS; pinned
nightly-2026-08-24 (rustc 1.100.0-nightly fb6531d55). Temporary datasets contain
200,000 rows with a 50,000-row segment target, 8,192-row batches and four concurrent
native-query workers. Queries are warmed once; OS caches are not evicted. Other
desktop applications remain active. Timed runs have no concurrent builds/tests
and do not access production data.

## Standard fixture

Fifteen samples/revision/profile from five alternating process pairs initially.
The sparse concurrent-query median was +6.17%; an additional ten alternating
pairs (30 samples) gave +0.61%, so that initial difference did not repeat. The
table retains **all 15 dense and 45 sparse samples** per revision. Every exact
row/order/count/reopen oracle passed. p95 uses nearest rank; sample counts and
synthetic workloads limit production tail-latency conclusions.

| Profile | Operation | Base median ms | Candidate median ms | Change | Base p95 ms | Candidate p95 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| dense | compaction | 80.404 | 82.908 | +3.11% | 88.086 | 87.330 |
| dense | concurrent_native_queries | 115.798 | 110.700 | -4.40% | 127.078 | 121.871 |
| dense | historical_storage_ingest | 294.366 | 297.269 | +0.99% | 320.714 | 335.922 |
| dense | index_build | 250.327 | 251.761 | +0.57% | 268.828 | 273.429 |
| dense | live_storage_ingest | 485.055 | 499.791 | +3.04% | 497.817 | 584.613 |
| dense | native_filter | 87.779 | 87.985 | +0.23% | 100.636 | 91.228 |
| dense | reopen | 6.886 | 6.885 | -0.01% | 7.382 | 12.548 |
| dense | sql_count | 4.546 | 4.484 | -1.36% | 9.423 | 5.430 |
| dense | sql_ordered | 9.528 | 9.407 | -1.27% | 20.112 | 10.655 |
| sparse | compaction | 84.452 | 85.145 | +0.82% | 88.003 | 89.938 |
| sparse | concurrent_native_queries | 114.217 | 115.898 | +1.47% | 124.345 | 121.209 |
| sparse | historical_storage_ingest | 287.967 | 287.110 | -0.30% | 302.730 | 313.967 |
| sparse | index_build | 461.242 | 460.522 | -0.16% | 472.566 | 476.983 |
| sparse | live_storage_ingest | 492.596 | 494.956 | +0.48% | 519.742 | 525.127 |
| sparse | native_filter | 86.758 | 86.938 | +0.21% | 87.344 | 92.086 |
| sparse | reopen | 6.899 | 7.039 | +2.02% | 8.501 | 8.556 |
| sparse | sql_count | 4.448 | 4.431 | -0.38% | 4.718 | 5.482 |
| sparse | sql_ordered | 9.408 | 9.424 | +0.16% | 9.996 | 9.762 |

No optimization gain is claimed. Generic storage API timings are included as
workload controls; they are not combined-sync or P2P measurements. Ingestion
code and its durability policy are unchanged.

## Limited native pages

A [targeted identical fixture](2026-09-12-query-pagination-pages.jsonl) is appended
to both archived source revisions without editing production code. It builds and
compacts the same disjoint chronological dataset, then measures ascending and
descending native pages with offset 500/limit 1,000. Both revisions must return
the exact correct page; an incorrect overlapping baseline is never used to claim
a speed comparison. Each of five alternating process pairs performs 30 queries
per order after a warmup, yielding 150 samples/revision/profile/order.

| Profile | Order | Base median ms | Candidate median ms | Change | Base p95 ms | Candidate p95 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| dense | ascending | 21.366 | 21.421 | +0.26% | 23.265 | 22.080 |
| dense | descending | 21.703 | 21.547 | -0.72% | 23.124 | 22.290 |
| sparse | ascending | 21.074 | 21.154 | +0.38% | 21.933 | 22.402 |
| sparse | descending | 21.402 | 21.300 | -0.48% | 26.810 | 22.872 |

No repeated median regression above 5% is established. All exact page assertions
pass. Archived sources have refreshed mtimes before each sequential build to
avoid shared Cargo target reuse; distinct executable hashes and full diagnostic
source/runner are recorded. Compilation is separate from timing.

## Memory, storage and gates

Process peak RSS includes fixture/result buffers and allocator retention, rather
than query-only or steady-state node memory.

| Profile | Base median peak RSS MiB | Candidate median peak RSS MiB |
| --- | ---: | ---: |
| dense | 747.6 | 752.6 |
| sparse | 632.5 | 632.2 |

Logical file sizes are unchanged for corresponding fixtures. All six
[local gates](2026-09-12-query-pagination-gates.jsonl) pass with 943 tests and ten
intentional ignored entries, plus documentation tests. Required Linux/macOS CI
is recorded in the implementation PR. Query snapshot lifetime and overall memory
accounting remain pending audit work; live-sync and release acceptance are later
gates after offline completion.
