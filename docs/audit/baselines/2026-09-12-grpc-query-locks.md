# gRPC query lock performance acceptance

Baseline `a52d79e9` versus implementation `47b75dee`. Every validation source
hash was checked against the committed implementation. The identical tracked
fixture was installed in the baseline archive; source mtimes were refreshed,
fresh compiler artifacts were required and distinct executable hashes verified.
[All raw samples and environment/build metadata](2026-09-12-grpc-query-locks.jsonl),
[local gates](2026-09-12-grpc-query-locks-gates.jsonl) and the
[before-fix lock failure](2026-09-12-grpc-query-locks-before.log) are retained.

## Conditions

Mac14,15, eight logical CPUs, 16 GiB RAM, macOS 26.6.2, internal APFS,
pinned nightly-2026-08-24. Five alternating process pairs; each builds, indexes,
compacts and reopens a fresh 20,000-row fixture in 8,192-row segments with 4,096-row
write batches. Warm paths once then rotate 50 queries per shape/process, yielding
250 samples per shape/revision. Verify every response value against an independent
fixture oracle after timing. No concurrent builds/tests, caches not forcibly
evicted, other applications not stopped. No production/external-volume access.

Native COUNT, native full projection of 1,000 ordered rows, and a DataFusion
MAX/COUNT aggregate measure direct gRPC handler execution and JSON serialization.
Network transport and live peers are excluded. Peak RSS includes fixture and
response buffers; five process samples per revision do not estimate steady-state
node memory. p95 uses nearest rank. The controlled future-poll regressions prove
writer availability during pending SQL independently of these latency samples.

## Results

| Metric | Baseline median | Candidate median | Median change | p95 change |
| --- | ---: | ---: | ---: | ---: |
| native_count | 0.549 ms | 0.539 ms | -1.94% | -5.10% |
| native_wide_page | 5.274 ms | 5.213 ms | -1.15% | -3.22% |
| datafusion_aggregate | 1.410 ms | 1.390 ms | -1.43% | -9.84% |
| peak_rss | 63.453 MiB | 62.375 MiB | -1.70% | +1.35% |

All measured median and tail changes remain within the user's 10% ceiling. No
positive latency change exceeds 5%. These measurements support absence of a
handler-latency regression; modest improvements are not claimed as an established
speedup. Ingestion/storage code is unchanged, and these results do not claim
live-peer throughput. Removing the held read guard resolves a directly reproduced
ingestion stall rather than adding per-batch persistence work.

All six local gates passed: formatting, all-target workspace check and Clippy,
981 tests with 12 intentionally ignored benchmarks, documentation tests, and the
release node build. All 12 gRPC unit tests also pass in release mode. The broader
offline audit, actual live-sync acceptance and staging soak remain separate.
