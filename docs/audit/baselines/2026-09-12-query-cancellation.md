# REST query cancellation performance acceptance

Baseline `73443007` versus implementation `d08c69e8`. Every source hash from
the local validation runner matches the committed implementation. Identical
tracked benchmark fixtures were used in the isolated baseline and candidate;
archive mtimes were refreshed, fresh compiler artifacts required and distinct
executable hashes checked. [Raw metadata and all samples](2026-09-12-query-cancellation.jsonl) retain
the fixture, source/diff, runner, toolchain, hardware and filesystem.
[All local gates](2026-09-12-query-cancellation-gates.jsonl),
[initial race failure](2026-09-12-query-cancellation-before.log) and
[expanded race failure](2026-09-12-query-cancellation-before-expanded.log) are retained.

## Conditions and limits

Mac14,15, eight logical CPUs, 16 GiB RAM, macOS 26.6.2, internal APFS,
pinned nightly-2026-08-24. Five alternating process pairs use fresh coherent
20,000-row fixtures, 8,192-row segments, 4,096-row writes, index construction,
compaction and reopen before warming the three query paths. Each process rotates
50 requests per shape, yielding 250 samples per shape/revision. All output fields
and row counts are independently checked outside timing. No concurrent builds or
tests; caches not forcibly evicted, other applications not stopped. No production,
external-volume or service access.

Direct REST calls include request admission, SQL execution, JSON serialization
and guard drop. They exclude network transport/live peers. Native COUNT, a native
full projection of 1,000 ordered rows and a DataFusion MAX/COUNT aggregate cover
fast and materializing paths. Peak RSS has five samples per revision and includes
fixture/result buffers; it does not describe steady-state node memory. p95 is
nearest-rank. Concurrency regressions independently verify cancellation ownership;
latency samples alone do not prove race freedom.

## Results

| Metric | Baseline median | Candidate median | Median change | p95 change |
| --- | ---: | ---: | ---: | ---: |
| native_count | 0.542 ms | 0.539 ms | -0.46% | -2.16% |
| native_wide_page | 5.290 ms | 5.291 ms | +0.02% | -1.47% |
| datafusion_aggregate | 1.401 ms | 1.385 ms | -1.15% | -4.89% |
| peak_rss | 62.391 MiB | 62.484 MiB | +0.15% | -1.35% |

All measured latency median/tail changes are below 5% and within the user's 10%
ceiling. The largest positive latency change is +0.02%. This supports absence of
a material regression; small differences are not claimed as an established
speedup. The per-request token allocation is included. Storage/ingestion code and
persisted formats are unchanged; this is not a live-peer throughput measurement.

All six local gates pass: format, workspace all-target check/Clippy, 987 tests
(13 intentionally ignored), documentation tests and release node build. All 89
server unit tests also pass in release mode. Wider offline auditing and later
live-sync/staging acceptance remain open.
