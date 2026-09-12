# Read-only SQL performance acceptance

Baseline `13bd33cc168cad3786373ae7911eaffa67807f9b` versus implementation `f73b316be833671e3e4715c5215f8ba08a35a5eb`. Every validated source hash matches the committed implementation. The same tracked handler benchmark runs in both revisions, with refreshed archive mtimes, fresh compiler artifacts and distinct copied executable hashes. [All metadata and samples](2026-09-12-sql-read-only.jsonl) retain source/diff, fixture hash, build/run script, toolchain, hardware and filesystem. [Local gate results](2026-09-12-sql-read-only-gates.jsonl) and the [before-fix reproduction](2026-09-12-sql-read-only-before.log) are retained.

## Conditions

Mac14,15, eight logical CPUs, 16 GiB RAM, macOS 26.6.2, internal APFS and pinned nightly-2026-08-24. Five alternating process pairs create fresh coherent 20,000-row fixtures, 8,192-row segments and 4,096-row writes; indexes, compaction and reopen precede measurement. After one warmup, each process rotates 50 requests per shape: 250 samples per shape/revision. All row counts and JSON fields are checked exactly outside timing. No concurrent builds/tests; caches not forcibly evicted, other applications not stopped.

Direct REST calls include admission, SQL execution, response serialization and guard drop, excluding transport/live peers. Native COUNT and a 1,000-row native full projection measure the AST check; the DataFusion MAX/COUNT aggregate includes the plan policy as well. Peak RSS has five samples per revision and includes fixture/materialization buffers, not steady-state node memory. p95 uses nearest rank. No production data, external volume or service access.

## Results

| Metric | Baseline median | Candidate median | Median change | p95 change |
| --- | ---: | ---: | ---: | ---: |
| native_count | 0.540 ms | 0.543 ms | +0.47% | -0.47% |
| native_wide_page | 5.285 ms | 5.271 ms | -0.26% | -0.65% |
| datafusion_aggregate | 1.402 ms | 1.396 ms | -0.43% | +0.36% |
| peak_rss | 62.203 MiB | 62.859 MiB | +1.06% | -0.67% |

All measured latency median/tail changes are below 1%, and all memory changes are below 2%, within the user's 10% ceiling. No established speedup is claimed from these small differences. The production ingestion code and storage format are unchanged; this is not an end-to-end live-sync throughput measurement.

All six local gates pass: format, workspace all-target check/Clippy, 995 tests (13 ignored), documentation tests and release node build. All 66 query unit tests and both protocol consistency tests also pass in release mode. The wider offline audit and subsequent live-sync/staging gates remain open.
