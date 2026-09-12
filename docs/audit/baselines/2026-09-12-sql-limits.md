# SQL expression limit performance acceptance

Baseline `452d0874bebcf399ad855414aadb89f5d0e0d95b` versus implementation `af88a02beb9c74ea140844f4fa7b6a68665ee435`. Every validated source hash matches the committed implementation. The identical tracked handler benchmark runs in both revisions, with refreshed archive mtimes, fresh compiler artifacts and distinct copied executable hashes. [All metadata and samples](2026-09-12-sql-limits.jsonl) retain source/diff, fixture hash, runner, toolchain, hardware and filesystem. [All local gate results](2026-09-12-sql-limits-gates.jsonl) and the [finding/prototype records](../sql-expression-limits.md) are retained.

## Conditions

Mac14,15, eight logical CPUs, 16 GiB RAM, macOS 26.6.2, internal APFS and pinned nightly-2026-08-24. Five alternating process pairs create fresh coherent 20,000-row fixtures, 8,192-row segments and 4,096-row writes; indexing, compaction and reopen precede measurement. After one warmup, each process rotates 50 requests per shape: 250 samples per shape/revision. All row counts and JSON fields are checked exactly outside timing. No concurrent builds/tests; caches not forcibly evicted, other applications not stopped. All original samples are retained; no exclusions or confirmation runs.

Direct REST calls include admission, SQL execution, response serialization and guard drop, excluding transport/live peers. Native COUNT, a 1,000-row native full projection and the DataFusion MAX/COUNT aggregate provide ordinary-query controls. A 10,000-literal IN list and a 126-term addition chain exercise the new validation with equivalent successful results in both revisions. Rejected-input timings are not used as a substitute for successful-query performance. Peak RSS has five samples per revision and includes fixture, parser, planner and materialization buffers, not steady-state node memory. p95 uses nearest rank. All data is disposable and local; no production, external-volume or service access.

## Results

| Metric | Baseline median | Candidate median | Median change | p95 change |
| --- | ---: | ---: | ---: | ---: |
| native_count | 0.668 ms | 0.632 ms | -5.41% | +3.66% |
| native_wide_page | 5.694 ms | 5.508 ms | -3.26% | -4.76% |
| datafusion_aggregate | 1.740 ms | 1.593 ms | -8.42% | -21.86% |
| literal_in_list | 52.252 ms | 50.184 ms | -3.96% | -5.21% |
| expression_chain | 12.939 ms | 12.621 ms | -2.45% | -5.19% |
| peak_rss | 269.016 MiB | 271.406 MiB | +0.89% | +1.36% |

All positive median/tail differences are below 5%, within the user's 10% ceiling. These runs establish no measured regression beyond that threshold for the tested workloads; they do not establish a speedup from the new checks. The mixed five-shape workload and process/environment variation can affect the apparent negative changes. Production ingestion and storage write code are unchanged. This is not an end-to-end live-sync throughput measurement or a general query resource bound.

All six required local gates pass: format, workspace all-target check/Clippy, 996 tests (14 ignored), documentation tests and release node build. Additional release validation passes all 66 query unit tests, the subprocess regression parent (which explicitly runs its ignored child entry) and both protocol consistency tests. Workspace gates execute the same child cases in debug mode. CI and merge are recorded in the PR; the wider offline audit and subsequent live-sync/staging gates remain open.
