# Segment-reader integrity release comparison

Source: `003df476` (query baseline, production source `d0863842`) versus
`85950b57`. The production combined-sync fixture uses the preserved `4408e070`
PR #130 baseline; that comparison also includes PR #131's page row-count guard,
which changed no writer. Fixture source hashes are unchanged.

[Final-source samples](2026-09-12-segment-integrity-final.jsonl) and
[confirmation](2026-09-12-segment-integrity-confirmation.jsonl) include executable
hashes, the source snapshot/diff, toolchain, hardware, filesystem, runner and raw
results. Preliminary measurements use an earlier error-classification variant
and are retained separately; they are **not** pooled with final-source results.

Mac14,15, 16 GiB RAM, eight CPUs; macOS 26.6.2, internal APFS; pinned
`nightly-2026-08-24` (rustc 1.100.0-nightly fb6531d55). Fresh temporary directories,
warmed queries, OS caches not evicted. Other desktop applications remained
active; no concurrent builds/tests or production data access during timing.

## Query/storage fixture

200,000 dense or sparse rows, 50,000-row segments, 8,192-row ingestion batches,
four concurrent native-query workers. Five alternating process pairs with three
datasets each initially, then ten more pairs to investigate timing variation.
All exact row/count/order/reopen assertions passed. The table includes **all 45
samples per revision/profile**. p95 is nearest rank; these small samples and
synthetic workloads are not production tail-latency estimates.

| Profile | Operation | Base median ms | Candidate median ms | Change | Base p95 ms | Candidate p95 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| dense | compaction | 81.952 | 81.945 | -0.01% | 85.965 | 84.999 |
| dense | concurrent_native_queries | 111.517 | 115.296 | +3.39% | 126.068 | 119.399 |
| dense | historical_storage_ingest | 300.116 | 295.240 | -1.62% | 312.147 | 308.875 |
| dense | index_build | 252.236 | 251.189 | -0.42% | 264.341 | 256.829 |
| dense | live_storage_ingest | 492.653 | 494.828 | +0.44% | 524.812 | 513.802 |
| dense | native_filter | 87.837 | 87.703 | -0.15% | 92.135 | 89.960 |
| dense | reopen | 7.538 | 8.028 | +6.50% | 9.321 | 10.515 |
| dense | sql_count | 4.492 | 4.485 | -0.15% | 4.686 | 6.546 |
| dense | sql_ordered | 9.461 | 9.383 | -0.83% | 9.788 | 10.751 |
| sparse | compaction | 83.176 | 83.133 | -0.05% | 87.765 | 89.715 |
| sparse | concurrent_native_queries | 109.103 | 110.844 | +1.60% | 120.840 | 123.065 |
| sparse | historical_storage_ingest | 292.182 | 290.593 | -0.54% | 311.144 | 300.094 |
| sparse | index_build | 462.677 | 461.500 | -0.25% | 486.863 | 478.217 |
| sparse | live_storage_ingest | 495.673 | 491.099 | -0.92% | 536.022 | 509.629 |
| sparse | native_filter | 86.978 | 86.478 | -0.57% | 91.247 | 88.570 |
| sparse | reopen | 7.443 | 8.319 | +11.77% | 9.631 | 11.844 |
| sparse | sql_count | 4.479 | 4.442 | -0.83% | 5.651 | 4.738 |
| sparse | sql_ordered | 9.482 | 9.422 | -0.63% | 10.860 | 9.826 |

The initial sparse concurrent-query median of +6.85% did not repeat (−0.90%
in the additional 30 samples). Warm reopen remains the investigated exception:
initial dense +17.77% and sparse +0.86%; confirmation dense +2.53% and sparse
+16.95%. All samples are retained, including less favorable results.

An [isolated repeated-open experiment](2026-09-12-segment-integrity-reopen.jsonl)
uses the same fixture creation/index/compaction logic, excludes setup and a first
open, and then measures 100 opens per process in five alternating pairs. Both
source trees contain the identical diagnostic fixture. Across 500 repeated opens
per revision/profile, dense is 0.850 → 0.865 ms (+1.79%), sparse 0.835 → 0.833 ms
(−0.23%). p95 is 4.885 → 5.602 ms dense and 0.878 → 0.986 ms sparse. The large
dense outliers occur on both versions; individual process medians are retained.

This separates repeated metadata access from the original first reopen after
fixture writes/compaction. It does not erase the first-open measurements or prove
the cause of their variation. The measured first-open increases of 0.490 ms
dense (+6.50%) and 0.876 ms sparse (+11.77%) remain an explicit accepted cost of
this correctness change; repeated opens and combined-sync ingestion do not show
a corresponding regression. No payload scan was added. There is no speculative
performance rewrite or relaxation of checks to improve these small timings.

The first attempt to build the isolated experiment exposed shared Cargo target
reuse across archived trees with old mtimes: both executables had identical
hashes. No timings were taken from that pair. Refreshing source mtimes before
each sequential build forced recompilation; final executable hashes differ and
are recorded. Production source was not edited for this experiment.

## Combined sync publication

Fifteen samples per revision/workload, five alternating process pairs of three
fresh datasets each. Mixed payloads, 128 rows/block, 8,192 warm cached headers,
1,000,000-row segment target and complete post-reopen row validation. Live uses
128 blocks and per-block checkpoint calls; cached historical uses 2,048 blocks
in batches of 64. Timings include combined row/head/floor/anchor publication and
the final checkpoint. They exclude peer networking and cryptographic validation.

| Workload | Operation | Base median ms | Candidate median ms | Change | Base p95 ms | Candidate p95 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| mixed-live | ingest_ms | 921.475 | 925.091 | +0.39% | 954.250 | 961.278 |
| mixed-live | warm_reopen_ms | 16.738 | 17.039 | +1.80% | 18.010 | 18.409 |
| mixed-live | full_row_validation_ms | 9.796 | 9.908 | +1.14% | 10.072 | 10.840 |
| cached-mixed-history | ingest_ms | 85.596 | 84.137 | -1.70% | 97.190 | 89.904 |
| cached-mixed-history | warm_reopen_ms | 19.145 | 17.412 | -9.06% | 20.842 | 20.794 |
| cached-mixed-history | full_row_validation_ms | 134.078 | 133.211 | -0.65% | 211.732 | 187.383 |

Live +0.39% and cached historical −1.70% satisfy the user's 10% ingestion
ceiling. No throughput gain is claimed. Allocated storage is unchanged:
7,233,536 bytes live and 50,573,312 historical. Process-written bytes are
536,940,544 → 536,944,640 live and unchanged at 59,338,752 historical.
Serialized logical sizes differ by a few bytes; they are retained in raw records.
No log columns, payload encoding or durability policy changes.

## Memory and validation

Median process peak RSS includes fixture/result buffers and allocator retention;
it does not measure query-only or steady-state node memory.

| Workload | Base MiB | Candidate MiB |
| --- | ---: | ---: |
| query dense | 708.5 | 711.0 |
| query sparse | 633.2 | 631.9 |
| publication mixed-live | 67.6 | 67.8 |
| publication cached-mixed-history | 777.3 | 784.4 |

All six [final-source local gates](2026-09-12-segment-integrity-gates.jsonl)
pass: 934 tests, ten intentional ignored entries, plus documentation tests.
Required Linux/macOS CI is recorded in the implementation PR. Actual live-sync
and staging acceptance follow completion of the remaining offline audit.
