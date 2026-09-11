# Page row-count release comparison

Source: `9a3318b7` (production source `4408e070`) versus `d0863842`. Only the
segment-reader source differs among the 88 previously validated Rust/Cargo/
toolchain files. Same release harness and fixture hashes; executable hashes,
hardware/toolchain/filesystem, exact commands, source diff, runner and all samples
are recorded in the [initial run](2026-09-12-page-row-count-initial.jsonl) and
[confirmation](2026-09-12-page-row-count-confirmation.jsonl).

200,000 rows; 50,000-row segment target; 8,192-row batches; four concurrent query
workers. Fresh temporary APFS directories, warm queries, OS caches not evicted.
No concurrent local builds/tests or production data access. Unrelated desktop
applications remained active. Fifteen alternating base/candidate samples per
profile initially; thirty additional dense samples to investigate initial 5–7%
differences and large outliers. The repeat did not reproduce those differences.

The table pools **all** 45 dense samples and 15 sparse samples per revision.
p95 uses nearest rank; these small samples are not a production latency estimate.

| Profile | Operation | Base median ms | Candidate median ms | Change | Base p95 ms | Candidate p95 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| dense | compaction | 81.058 | 80.576 | -0.59% | 146.833 | 108.085 |
| dense | concurrent_native_queries | 125.595 | 126.217 | +0.49% | 219.311 | 214.273 |
| dense | historical_storage_ingest | 276.973 | 276.639 | -0.12% | 391.954 | 370.395 |
| dense | index_build | 257.752 | 258.985 | +0.48% | 335.565 | 353.712 |
| dense | live_storage_ingest | 448.838 | 446.988 | -0.41% | 657.992 | 674.975 |
| dense | native_filter | 92.049 | 92.455 | +0.44% | 100.255 | 109.255 |
| dense | reopen | 6.830 | 7.057 | +3.31% | 9.035 | 8.023 |
| dense | sql_count | 4.757 | 4.711 | -0.96% | 7.334 | 7.443 |
| dense | sql_ordered | 10.417 | 10.704 | +2.76% | 14.073 | 14.143 |
| sparse | compaction | 83.879 | 83.554 | -0.39% | 99.861 | 130.924 |
| sparse | concurrent_native_queries | 120.559 | 118.088 | -2.05% | 211.562 | 161.885 |
| sparse | historical_storage_ingest | 280.857 | 285.990 | +1.83% | 402.158 | 306.625 |
| sparse | index_build | 492.903 | 469.770 | -4.69% | 1135.022 | 676.778 |
| sparse | live_storage_ingest | 479.937 | 469.097 | -2.26% | 614.793 | 590.284 |
| sparse | native_filter | 91.197 | 90.470 | -0.80% | 135.599 | 185.465 |
| sparse | reopen | 7.133 | 7.030 | -1.45% | 9.186 | 8.452 |
| sparse | sql_count | 4.748 | 4.647 | -2.13% | 10.226 | 10.607 |
| sparse | sql_ordered | 10.099 | 10.362 | +2.60% | 15.528 | 20.025 |

The separate 30-sample dense confirmation gives native +0.66%, concurrent native
+0.08%, SQL count −0.19%, ordered SQL +1.01%; live storage −0.67%, historical
storage +0.26%, compaction −0.11%, index build +0.40%, warm reopen +3.36%. No
repeatable median regression above 5% was established. Tails remain noisy; both
initial and repeat results are retained instead of choosing only favorable runs.

No ingestion writer, durability or format changed. These standalone storage
timings do not measure the production combined-sync pipeline or P2P throughput;
they do not replace the prior production-sync acceptance or later live testing.
No optimization or throughput gain is claimed. All exact row/count/order/reopen
oracles passed. Successful query result semantics remain unchanged.

Logical file sizes are unchanged for corresponding fixtures. Process RSS includes
fixture/result buffers and allocator retention, not per-query or steady-state
node memory:

| Profile | Base median peak RSS MiB | Candidate median peak RSS MiB |
| --- | ---: | ---: |
| dense | 713.8 | 720.0 |
| sparse | 658.8 | 661.8 |

[Local gates](2026-09-12-page-row-count-gates.jsonl): all six pass. The first
sandboxed full-suite attempt failed only on denied loopback binds; the permitted
rerun passed 923 tests with 10 intentional ignored entries. Documentation tests
also pass. Required Linux/macOS CI is recorded in the implementation PR.
