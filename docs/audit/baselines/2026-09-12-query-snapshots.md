# Query snapshot release comparison

Implementation: `e2106241`, against `6ffc1201` (merged PR #133). Baseline query
binary is preserved from `fb858187`; the combined publication binary is preserved
from `85950b57` (PR #132), whose storage source is unchanged by PR #133. Both
fixtures are byte-identical across the compared builds. Source hashes, executable
hashes, commands, parameters and raw results are retained in the
[initial round](2026-09-12-query-snapshots-initial.jsonl) and
[confirmation round](2026-09-12-query-snapshots-confirmation.jsonl).

Host: Apple Silicon Mac14,15, eight logical CPUs, 16 GiB RAM, internal APFS,
macOS 26.6.2 (25G83). Pinned nightly-2026-08-24: rustc 1.100.0-nightly
(fb6531d55, LLVM 23.1.0). Release builds, alternating revision order, no concurrent
builds/tests. Other applications remain active. Fresh directories per write
fixture; OS caches are not evicted. Queries are warmed once. Every exact row,
order, count, progress and reopen oracle must pass. No production data or external
volume is used.

## Combined ingestion

Five process pairs per workload, three fresh datasets per process: 15 samples
per revision. Live: 128 blocks/15,360 logs, 128 logs per nonempty block, checkpoint
each block. Historical: 2,048 blocks/245,760 logs, 64-block chunks, cached historical
head. Both use mixed payloads, empty every 16th block and 8,192 warm headers.
Timings include combined rows/head/floor/anchor publication and final checkpoint.
They exclude consensus/EL network fetching and cryptographic validation.

| Workload | Median base → candidate (ms) | Change | p95 base → candidate (ms) | Change |
| --- | ---: | ---: | ---: | ---: |
| mixed-live | 885.416 → 869.970 | -1.74% | 918.898 → 955.924 | +4.03% |
| cached-mixed-history | 84.278 → 84.434 | +0.18% | 93.044 → 88.955 | -4.39% |

Both routes meet the 10% ingestion ceiling. Allocated bytes and process-written
bytes are unchanged; logical file sizes differ by less than 0.0001%. No additional
durability operation, persisted field, format or writer-loop change is introduced.

## Queries

Initial 15 plus 30 confirmation samples per profile/revision: all 45 retained.
Each fresh fixture has 200,000 rows, 50,000-row segment targets, 8,192-row batches
and four concurrent native-query workers. Positive differences mean slower.

| Profile / query | Median base → candidate (ms) | Change | p95 base → candidate (ms) | Change |
| --- | ---: | ---: | ---: | ---: |
| dense / concurrent_native_queries | 116.087 → 110.593 | -4.73% | 123.674 → 120.237 | -2.78% |
| dense / native_filter | 87.561 → 87.790 | +0.26% | 93.755 → 90.359 | -3.62% |
| dense / sql_count | 4.495 → 4.478 | -0.37% | 4.754 → 4.747 | -0.14% |
| dense / sql_ordered | 9.427 → 9.451 | +0.25% | 9.767 → 9.958 | +1.96% |
| sparse / concurrent_native_queries | 114.702 → 116.236 | +1.34% | 117.804 → 123.953 | +5.22% |
| sparse / native_filter | 86.845 → 87.229 | +0.44% | 89.066 → 89.521 | +0.51% |
| sparse / sql_count | 4.491 → 4.505 | +0.32% | 5.084 → 4.894 | -3.75% |
| sparse / sql_ordered | 9.434 → 9.505 | +0.75% | 9.899 → 10.679 | +7.88% |

The first 15-sample set produced p95 outliers (the maximum at that sample size),
including dense SQL count 4.812 → 8.104 ms and sparse ordered SQL 9.878 →
12.979 ms. No samples were removed. The additional 30 dense samples do not
reproduce those query tails. Across all 45 samples, sparse ordered-query p95
still changes +7.88% (0.780 ms) and concurrent-query p95 +5.22% (6.149 ms).
These retained differences prompted a separate warm-query investigation.

The [warm-query investigation](2026-09-12-query-snapshots-latency.jsonl) uses
identical appended fixtures built against both archived commits; their source
mtimes are refreshed before each sequential build and distinct executable hashes
are required. Five alternating process pairs each ingest/index/compact/reopen a
fresh sparse fixture, then issue 300 warm queries per SQL shape and 30 concurrent
native-query groups. All 1,500 SQL samples and 150 groups per revision are retained.

| Warm query | Median change | p95 base → candidate (ms) | p95 change |
| --- | ---: | ---: | ---: |
| concurrent_native_queries | +0.78% | 123.857 → 125.271 | +1.14% |
| sql_count | +0.22% | 4.670 → 4.763 | +2.01% |
| sql_ordered | +0.71% | 9.893 → 10.140 | +2.50% |

The larger mixed-workload query tails do not repeat at the same magnitude in
this isolated warm workload. This narrows the investigation but does not identify
a definitive cause or erase the original mixed-workload costs. Retain the
+7.88% ordered-query/+5.22% concurrent-query mixed p95 differences explicitly;
the correctness fix meets the measured 10% ceiling without weakening checks.

## Other costs and limits

Generic row-only ingestion controls are separate from combined sync publication.
Their dense medians change −0.68% live/+0.09% historical, with retained p95
differences +5.98%/+9.43%; sparse medians change −1.38%/−0.02%. The additional
30-sample dense run measured p95 +5.97% live/+3.62% historical. Those tail costs
remain visible; unchanged writer loops/disk counts and stable medians do not
prove a cause for individual delays. Neither generic nor production ingestion
medians show a substantial regression.

Index-build medians change −0.20% dense/+0.45% sparse, compaction −0.41%/+1.28%,
and reopen +1.29%/−0.05%. Query-fixture raw/indexed/compacted logical sizes match.
Process RSS includes fixtures, result vectors and allocator retention, not node
steady-state memory:

| Workload | Median peak RSS base → candidate (MiB) |
| --- | ---: |
| dense | 755.4 → 758.3 |
| sparse | 686.3 → 683.9 |
| mixed-live | 68.2 → 67.8 |
| cached-mixed-history | 784.0 → 784.4 |

[All combined statistics](2026-09-12-query-snapshots.jsonl) and
[six workspace gates plus both benchmark builds](2026-09-12-query-snapshots-gates.jsonl)
are retained. All 955 tests pass (ten intentional ignored tests), as do doc tests,
Clippy, checking, formatting and release linking. This is a correctness fix with
measured costs, not a claimed optimization. Mixed external-service workloads,
actual live sync and the 24-hour staging soak remain later acceptance gates.
