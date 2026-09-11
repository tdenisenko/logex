# Journaled commit release comparison — 2026-09-10

This initial implementation fixes duplicate replay and missing durability barriers.
Its ingestion slowdown was rejected by the user; these measurements are historical
evidence, not acceptance of the cost. PR #130 remains open for a performance fix.
It is a correctness change with substantial measured I/O cost, not a throughput
improvement. A profiled concurrent-flush experiment was rejected because it did
not demonstrate useful gains. No production directory was opened.

## Environment and reproducibility

- Baseline: `09a63f555db19a03850ce5eba6f149957febaf3b` (merged PR #129).
- Final implementation: `c6f156a0e4a1aaea0093b31fd7bd560c890ed568`. Initial and experiment binaries were built
  from the working implementation before its final mechanical cleanup: the final
  tree also handles empty relative WAL parent paths, removes an inlined forwarding
  helper, completes zero-prefix recovery, and adds subprocess tests. Fixture paths always have absolute parents.
  Final-code confirmation runs are linked below; exact executable hashes are in
  each raw file.
- Toolchain: pinned `nightly-2026-08-24`; rustc `1.100.0-nightly`, commit
  `fb6531d550e0075b9eb9a51464f404805eec87d9`, LLVM 23.1.0;
  cargo `1.100.0-nightly (e8cb624d5 2026-08-22)`.
- Host: Mac14,15, 8 logical cores, 16 GiB RAM, macOS 26.6.2 (25G83).
  Internal APFS SSD; approximately 147 GiB free at the beginning. OS-default
  temporary directories on that volume; OS caches were not evicted.
- Unchanged `audit_harness`, fixture version 1; dense and sparse profiles,
  200,000 rows, 50,000-row segments, 8,192-row batches, four query workers.
  Each process performs an untimed warmup and three measured fresh-directory
  iterations. Every query/lifecycle path checks exact results independently.
- Each comparison has five alternating process pairs per profile: 20 processes,
  60 measured iterations. No compilation or tests ran concurrently with timing.
  A separate five-second `sample` profile is excluded from all timing summaries.
- Existing dependency artifacts were shared, but workspace crates were rebuilt
  for each checkout and benchmark artifacts reported `fresh: false`. Executables
  were copied and SHA-256 hashed before the next build. An initial baseline build
  in a separate target directory was stopped and restarted using the existing
  cache; no measurements came from that abandoned build.

Lockfile SHA-256: `6dcb648496174b61ba135432d0f1eec51a9d1ae9dbcbd9f8a5e6483906e7e50c`.
Harness SHA-256: `0df8e9a28642760b1a7b609a0c4c4c0f3aaef3f4484150f8367435bc55a0bc12`.

## Baseline versus initial sequential durability

All times are milliseconds across 15 measured iterations per cell. P95 is
nearest-rank over this small sample, not a production tail-latency estimate.

| Profile | Operation | Baseline median / p95 | Durable median / p95 | Median change |
| --- | --- | ---: | ---: | ---: |
| dense | live_storage_ingest | 432.041 / 567.122 | 4037.965 / 4178.976 | +834.62% |
| dense | historical_storage_ingest | 251.480 / 349.700 | 4080.023 / 6051.796 | +1522.41% |
| dense | index_build | 235.972 / 251.414 | 323.246 / 409.233 | +36.99% |
| dense | compaction | 79.329 / 84.727 | 183.621 / 202.301 | +131.47% |
| dense | reopen | 1.315 / 1.646 | 6.936 / 7.761 | +427.45% |
| dense | native_filter | 96.349 / 111.873 | 94.422 / 100.989 | -2.00% |
| dense | sql_count | 5.403 / 6.873 | 5.457 / 7.591 | +1.01% |
| dense | sql_ordered | 10.602 / 13.225 | 11.015 / 15.481 | +3.89% |
| dense | concurrent_native_queries | 130.720 / 156.096 | 138.377 / 261.620 | +5.86% |
| sparse | live_storage_ingest | 441.603 / 543.054 | 4054.523 / 4229.820 | +818.14% |
| sparse | historical_storage_ingest | 264.055 / 306.216 | 4906.324 / 5212.192 | +1758.07% |
| sparse | index_build | 462.422 / 490.756 | 555.517 / 788.321 | +20.13% |
| sparse | compaction | 84.607 / 90.413 | 192.170 / 214.120 | +127.13% |
| sparse | reopen | 1.334 / 1.552 | 7.119 / 9.549 | +433.60% |
| sparse | native_filter | 93.463 / 100.550 | 94.193 / 98.519 | +0.78% |
| sparse | sql_count | 5.240 / 6.483 | 5.080 / 6.260 | -3.05% |
| sparse | sql_ordered | 10.701 / 13.161 | 10.466 / 11.701 | -2.19% |
| sparse | concurrent_native_queries | 131.130 / 150.355 | 145.804 / 226.283 | +11.19% |

These initial write/index/compaction/reopen costs are unacceptable for merging.
The follow-up must preserve durable publication while reducing repeated flushes. Live ingestion in
this fixture is approximately 49,000 rows/s with durability versus 453,000–463,000
rows/s before it. Historical ingestion is approximately 41,000–49,000 rows/s
versus 757,000–795,000 rows/s. They are local storage figures, not verified node
or P2P throughput.

Concurrent-query medians also rose 5.86% (dense) and 11.19% (sparse) in the first
comparison. All five dense paired medians increased; sparse paired results were
mixed. The query implementation is unchanged and single-query results are within
5%, but that does not establish the cause of the concurrent timing difference.
The lifecycle profile attributes substantial time to synchronization, not the
query tail. Retain this observed workload tradeoff explicitly and investigate
isolated query/allocator/scheduling behavior in batches 7 and 12; no query
performance improvement is claimed by this milestone.

## Rejected concurrent-flush experiment

The separate profile showed many `File::sync_all` → `fcntl` → `__fcntl` stacks
under atomic replacement, directory synchronization and `sync_tree` before
manifest publication. The pinned Rust source confirms both `sync_all` and
`sync_data` use `F_FULLFSYNC` on Apple, so substituting `sync_data` is not a
flush-cost solution here.

The experiment buffered at most eight file paths and used scoped workers to
synchronize them concurrently. Every worker was joined before directory sync
and manifest publication; errors/panics propagated. It did not remove barriers.
The following comparison reran the exact sequential executable against that
experiment under alternating order. No useful ingestion gain exceeded noise,
so the threads and buffering were removed from the final implementation.

| Profile | Operation | Sequential median | Parallel median | Change | Five paired median changes |
| --- | --- | ---: | ---: | ---: | --- |
| dense | live_storage_ingest | 4204.777 | 4092.322 | -2.67% | +2.61%, +0.89%, +1.11%, -5.01%, -0.81% |
| dense | historical_storage_ingest | 4114.914 | 4132.782 | +0.43% | +2.94%, -0.64%, +0.41%, -1.67%, -1.17% |
| dense | index_build | 317.826 | 314.221 | -1.13% | -3.74%, -0.47%, +26.01%, -2.66%, -3.11% |
| dense | compaction | 169.011 | 164.731 | -2.53% | -4.16%, -1.04%, +10.64%, -10.25%, -2.62% |
| sparse | live_storage_ingest | 4215.053 | 4211.438 | -0.09% | -2.12%, +0.20%, -0.62%, -0.40%, -3.19% |
| sparse | historical_storage_ingest | 5176.809 | 5126.256 | -0.98% | -1.15%, +0.97%, +1.80%, +8.69%, -19.91% |
| sparse | index_build | 570.594 | 562.744 | -1.38% | -2.58%, -0.96%, +0.72%, -0.57%, -4.23% |
| sparse | compaction | 180.861 | 174.613 | -3.45% | -0.32%, -1.76%, -3.45%, -8.66%, -6.92% |

## Memory, disk use and limits

Peak RSS covers the whole process, including fixtures, independent oracles,
query results and allocator retention. It is not per-query memory.

| Profile | Baseline RSS median / maximum | Durable RSS median / maximum |
| --- | ---: | ---: |
| dense | 695,386,112 / 755,302,400 B | 607,567,872 / 614,514,688 B |
| sparse | 641,531,904 / 717,406,208 B | 606,846,976 / 651,296,768 B |

Logical file bytes were identical in every initial comparison: 68,497,860 after
live writes for both profiles; after indexing, 80,561,156 dense / 120,558,828
sparse; after compaction, 23,659,738 dense / 72,611,593 sparse. Every iteration
compacted four segments. The journal is removed after successful batches.
Temporary replacement writes and metadata flushes can increase physical write
amplification even when these final sizes match. `/usr/bin/time -l` reported zero
block-I/O counters despite actual writes, so those counters are not usable
physical-I/O evidence. Device write amplification remains unmeasured.

Smaller RSS is observed but not claimed as an allocation optimization: workflow
duration, allocator reuse and OS scheduling differ. No cache eviction, external
volume, network, validation pipeline, failure injection or production soak was
part of the timing run. The older dependency-remediation historical-write
regression is a separate baseline and remains unattributed.

## Final-code confirmation

Four additional unpaired confirmation processes ran after the alternating
comparisons: two at `9d9fa9af` and two at final commit `c6f156a0`. Each used three
measured iterations and passed every exact-result check. They are separate from
the five-pair estimates above and do not support a new speedup/regression claim.
The final binary SHA-256 is
`ab5d717fdd6861b62d39b09a2d2b2d70cf340882ba1efd8065a686c26343a494`.

| Operation | Final dense median / p95 (ms) | Final sparse median / p95 (ms) |
| --- | ---: | ---: |
| live_storage_ingest | 4400.930 / 5132.671 | 4531.924 / 4560.396 |
| historical_storage_ingest | 4388.060 / 4574.061 | 5616.824 / 5628.055 |
| index_build | 313.805 / 336.060 | 529.895 / 545.898 |
| compaction | 170.473 / 174.620 | 181.039 / 189.279 |
| reopen | 5.958 / 7.030 | 5.819 / 7.770 |
| native_filter | 90.170 / 90.254 | 93.036 / 94.165 |
| sql_count | 5.320 / 13.208 | 5.201 / 5.638 |
| sql_ordered | 10.183 / 13.541 | 10.191 / 10.573 |
| concurrent_native_queries | 131.586 / 142.390 | 119.323 / 130.131 |

The later write timings are higher than the initial comparison, while concurrent
queries are faster. The earlier confirmation at `9d9fa9af` also varied (live
medians 4,516/4,153 ms and historical 4,715/5,122 ms for dense/sparse). This limits
cross-round attribution; retain all samples rather than present either direction
as a new implementation gain. The measured durable write cost remains material.
Final process RSS was 612,466,688 B dense and 714,276,864 B sparse; final logical
file sizes and four-segment compaction counts match the initial fixtures.

## Raw evidence

- [Initial baseline / durability comparison](2026-09-10-commit-replay-initial.jsonl).
- [Rejected concurrent-flush comparison](2026-09-10-commit-replay-parallel-experiment.jsonl).
- [Earlier code confirmation](2026-09-10-commit-replay-pre-final-confirm.jsonl).
- [Final-code confirmation](2026-09-10-commit-replay.jsonl).

Use the [standard benchmark commands](../benchmarks.md), setting
`LOGEX_BENCH_REPEATS=3`, then execute the copied test binary with
`--ignored --nocapture --test-threads=1` under `/usr/bin/time -l`.
All raw records retain fixture digests, parameters, throughput and per-iteration
values; RSS and executable hashes are added by the comparison runner.
