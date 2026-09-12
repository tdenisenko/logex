# Storage decoding bounds

Review branch: `audit/storage-decode-bounds`, based on PR #146 merge
`afc7c7dce261981ff17a26bd4d24e5b9c28ca17e`. Local correctness and measured
performance acceptance are complete on
`8a23555a6decbf462ac7ae9c03ceb5a5c0258f66`. Exact-head CI and PR merge remain
pending; this milestone does not complete the offline audit.

## Findings and scope

These findings describe the baseline behavior corrected by this milestone.

- **B2-20 (P2): variable-page decoding precedes its metadata bounds.**
  Compacted query reads use unbounded byte decompression, then validate the
  page index's row count. Full log-row materialization compares payload lengths
  even later; direct payload projections do not compare the companion lengths.
  Independently stored per-row lengths already provide the exact expected page
  shape. Reuse that information before decoding, then check the complete page's
  row lengths. Reject inconsistencies through normal storage errors.
- **B2-21 (P2): dictionary indexes are not checked before indexing entries.**
  A row that selects a missing dictionary entry reaches a slice panic. The
  retained source column and adaptive fixed-column profiles use this decoder.
  Check the index against the dictionary entry count before slicing and return
  an explicit invalid-data error. A small fixture suffices to demonstrate it.

Four final public-reader/dictionary regressions fail as expected on a fresh
archive of the exact baseline. They cover the dictionary panic, direct raw and
compacted per-row length disagreement, and a byte page exceeding its stored
lengths. Both streams, copied final fixtures and the exact source are retained
under the reproduction record. All required local gates now pass. This review
uses only disposable local fixtures; it does not modify production directories,
mounts, peers or running services.

## Compatibility and implementation constraints

Keep all retained raw, Zstd, LZ4 and adaptive byte-page encodings, including both
32-bit and 64-bit offset tables. The stored index already limits a page to
16,384 rows; the segment reader enforces the u32 row-addressing domain. Neither
limit is a payload byte limit. Existing tests explicitly support an individual
payload larger than the joint checkpoint's 32 MiB threshold.

The exact encoded shape is the row-count field, its offset table and the sum of
that page's companion data lengths. Validate the decoded row count before
allocating offset/result vectors. Validate each row length, including an
inconsistency where the total length remains unchanged. A selected-row read must
validate its complete decoded page, preserve ordering and duplicates, and match
lengths by row range even when column page boundaries differ.

Projected data reads must retain the companion length artifacts from the same
captured generation. Do not reopen them by pathname after a rename or compaction.
Full log materialization should reuse lengths obtained for data validation;
selected reads should load only overlapping length pages and avoid repeated
index scans and decompression. Maintenance retains its separate explicit total
payload budget.

The change adds no ingestion writes, schema migration or format version change.
The private unbounded page decoder and adaptive wrapper are removed; all three
retained readers use the validated decoder. Public compression helpers remain public compatibility APIs.

This bound checks consistency with persisted metadata. It does not establish a
query-wide memory budget when both metadata and data describe a coherently large
valid result. Whole raw-column reads, large index/file allocations and aggregate
query memory remain separate audit items.

## Validation and performance protocol

First retain the baseline failures and final successes with source and fixture
identities. Cover full, selected, projected and maintenance callers, different
column page boundaries, empty values, ordinary larger values and both dictionary
and adaptive fixed-page callers. Run the six workspace gates, release query and
protocol checks, and exact-head Linux/macOS CI before merge.

Compare separately built release archives of the recorded base and final source,
with identical fixtures and a fixed balanced process schedule. Reuse the existing
dense/sparse mixed storage/index/query fixture, dense/sparse general-query fixture
and joint live/historical publication fixture. Add a finite direct storage-read
fixture to expose costs of selected data reads and dictionary decoding. Retain
all latency observations, process memory, file sizes, failed attempts, source
hashes, toolchain/hardware/filesystem and cache conditions. Investigate repeatable
changes above 5%; source-induced regressions above 10% cannot be accepted.

The final local validation record contains all nine check logs and the source
inventory. Workspace tests pass 1,044/0 failed/19 ignored; release query tests pass
139/0 failed/7 ignored; release protocol consistency passes both tests. Format,
workspace compilation, Clippy, documentation tests, vendor verification and the
release node build also pass. The seven documentation test groups contain no
runnable examples.

Final measurements below establish acceptance for these synthetic workloads.
They do not establish live-sync throughput or release readiness.

## Retained implementation investigation

The first implementation, `72f0cb0f`, passed 232 storage tests and a two-repeat
benchmark smoke check. Six balanced release pairs retained 2,400 latency samples
and 12 memory observations. The additional per-entry dictionary check costs
+9.79% median/+13.33% p95, with all six process-pair medians increasing. That
implementation is not accepted as the final performance result.

`0539d96f` validates the maximum decoded dictionary index before the copy loop.
All entries remain checked, including an empty dictionary with nonempty rows;
no check is disabled. The unchanged six-pair protocol on separately built sources
measures dictionary median +3.61%/p95 +3.81%, selected data median +0.80%/+0.82%
with slightly lower tails, and full data median -6.06%/p95 -6.93%. Peak process
memory is approximately unchanged. Every earlier sample remains retained.
The selected path's coverage lists have no demonstrated end-to-end cost requiring
a further refactor in this direct fixture. The interim broader comparison on that
source was interrupted after review identified the allocation error path below. Completed and partial original logs
remain retained; it does not supply final performance acceptance.

Review also identified a remaining allocation-error path in the shared bounded
Zstd wrapper: the library's convenience method uses an infallible output
reservation. A tiny address-space-limit regression fails before the correction
and passes on `b3d433b1`, which uses fallible reservation and the same buffer
decoder. The boundary fixture fails before any large allocation; it tests the
helper error contract rather than simulating system memory exhaustion. This does
not replace the separate query memory budget review or impose a new valid-payload
limit.


## Dependency feature validation

The second required-gate attempt caught an incorrect assumption in the new
allocation test. Reth's existing `reth-zstd-compressors` dependency enables Zstd's
`experimental` feature across a workspace build. Its decoded-size estimate can
reduce the output allocation for valid input even if the supplied maximum is
very large, so successful decoding is correct in that configuration.

`8a23555a` changes only the fixture to a tiny incomplete frame with no size
estimate. It exercises the same capacity-error path with and without that
feature, without attempting a large allocation. The exact baseline fails and
the candidate passes in both configurations. Valid small/empty controls remain.
This is a test correction; no production check was relaxed.

The final release protocol selects the workspace for each benchmark target and
records/asserts the resolved Zstd feature set in every build. This matches the
node dependency feature union. Earlier package-only comparisons remain retained
as interim measurements, not substitutes for final node-feature acceptance.


## Final release measurements and disposition

The complete first protocol uses ten balanced process pairs for six workloads,
retaining 14,000 latency observations and 120 process RSS counters. Each target
is freshly built from exact source archives with workspace dependency features;
its artifact identity and Zstd feature set are verified. The baseline receives
only the identical new direct-read benchmark; the existing mixed/query and
publication fixtures are unchanged. No performance samples are excluded.

Host: Mac14,15, 8 CPUs, 16 GiB RAM, aarch64 macOS 26.6.2 (25G83), pinned
`rustc 1.100.0-nightly (fb6531d55 2026-08-23)` and Cargo 1.100.0-nightly.
The disposable fixtures reside on `/System/Volumes/Data` through `/private/tmp`;
the initial filesystem report records 48 GiB available. Caches are warmed by each
fixture; the OS cache is not evicted. Source/binary/fixture/lockfile hashes, exact
commands, timings, process counters, file sizes and all cache parameters are in
the linked reports. No builds or other tests run concurrently with measurements.

The initial dense workload shows nearly unchanged medians but simultaneous
lifecycle tail spikes: live ingestion +69.89%, index construction +55.16%,
compaction +65.57%, reopen +80.93%, concurrent queries +31.42% and historical
ingestion +99.94% at p95. Initial live publication p95 rises +19.66%; its warm
reopen auxiliary measure rises +18.11%. These initial observations are retained
and do not pass acceptance by themselves.

Before further timing, a fixed investigation specifies twenty additional source
pairs each for dense mixed work and publication, plus ten identical-candidate
executable pairs for each. It retains another 6,600 latency observations and 120
RSS counters. The source confirmation shows positive median changes at most
+1.37% and positive p95 changes at most +1.75%. Publication live/historical
medians are +0.20%/+0.38%, with p95 +1.64%/-0.10%. No source or fixture changes
occur between the original and confirmation runs.

The identical-executable controls demonstrate material variation without any
source difference: concurrent-query p95 differs +15.46% between scheduling
labels, and historical-publication p95 differs -21.48%. Controls are reported
separately and never pooled with baseline-versus-candidate comparisons. They do
not prove the precise cause of every earlier spike. Together with the fixed
confirmation, they establish that the initial large tail differences are not a
repeatable demonstrated regression from this source change.

The table includes every source sample: thirty process pairs for the two
investigated workloads, ten pairs for each other workload. Negative changes mean
less time. Full original, confirmation and control statistics remain separate
in the evidence. The 18,400 source-comparison observations plus 2,200 control
observations total 20,600, with 240 process memory observations.

| Workload | Operation | Median change | p95 change |
| --- | --- | ---: | ---: |
| direct | data_one_row | +2.03% | -4.72% |
| direct | data_shuffled_duplicates | +2.08% | +0.08% |
| direct | data_full | -5.29% | -7.47% |
| direct | source_dictionary_full | +0.86% | -4.23% |
| engine_dense | datafusion_narrow | -0.73% | -3.31% |
| engine_dense | datafusion_wide | -0.12% | -3.34% |
| engine_dense | datafusion_aggregate | -1.08% | -3.31% |
| engine_sparse | datafusion_narrow | -0.54% | -1.63% |
| engine_sparse | datafusion_wide | +0.10% | -0.83% |
| engine_sparse | datafusion_aggregate | -0.66% | -2.69% |
| integrated_dense | live_storage_ingest | -0.13% | +0.20% |
| integrated_dense | index_build | +0.17% | +4.10% |
| integrated_dense | compaction | +0.46% | +6.02% |
| integrated_dense | reopen | +1.18% | +2.33% |
| integrated_dense | native_filter | -0.39% | -0.42% |
| integrated_dense | sql_count | -0.24% | +0.92% |
| integrated_dense | sql_ordered | -1.21% | -1.65% |
| integrated_dense | concurrent_native_queries | -1.05% | +5.79% |
| integrated_dense | historical_storage_ingest | -0.85% | +1.87% |
| integrated_sparse | live_storage_ingest | -0.07% | +0.02% |
| integrated_sparse | index_build | -0.16% | +1.62% |
| integrated_sparse | compaction | -0.63% | -0.79% |
| integrated_sparse | reopen | -2.61% | +1.80% |
| integrated_sparse | native_filter | -0.69% | -3.77% |
| integrated_sparse | sql_count | -0.54% | -0.33% |
| integrated_sparse | sql_ordered | -1.64% | -3.25% |
| integrated_sparse | concurrent_native_queries | -1.46% | -17.14% |
| integrated_sparse | historical_storage_ingest | +0.19% | -2.55% |
| publication | live_storage_publication | +0.19% | +1.97% |
| publication | historical_storage_publication | +0.38% | +0.93% |

All measured source medians and combined p95 changes stay below the 10% limit.
The remaining combined dense compaction p95 +6.02% and concurrent-query p95
+5.79% are explicit measurement limitations: confirmation is -0.24% and -1.04%,
respectively, and the latter's identical-executable control differs more. Keep
these observations in batch 12's controlled integrated/staging review. Do not
claim uniform tails below 5% or attribute every change to decoding. No known
repeatable source-induced regression above 10% is accepted.

Direct payload selections cost +2.03%/+2.08% at the median. Full payload decoding
improves 5.29% median/7.47% p95, consistent with the earlier direct investigation;
dictionary decoding costs +0.86% median. This supports retaining the checked
implementation and length reuse. It does not establish a node-wide speedup.

Peak RSS median changes range from -1.10% to +1.28% across the final source
comparisons; the largest positive p95 change is +2.66%. Counters include complete
fixture/oracle/result buffers and allocator retention, not per-query memory.
Dense and sparse mixed fixtures produce identical logical file sizes on both
sources. Combined publication allocation, file and segment counts are unchanged;
logical-byte differences are below 0.001%. Median process-attributed writes are
+0.03% live and unchanged historical, with unchanged p95. These OS counters are
not device write amplification. Combined full-row validation medians improve
2.54% live and 2.80% historical; warm reopen remains approximately unchanged.

## Evidence and reproduction

- [Final local validation](baselines/2026-09-13-storage-decode-validation.json):
  exact source inventory, all nine logs and independently checked test totals.
- [Initial complete release report](baselines/2026-09-13-storage-decode-release.json)
  and [all raw release evidence](baselines/2026-09-13-storage-decode-release-raw.json.gz).
- [Fixed tail investigation](baselines/2026-09-13-storage-decode-tail.json) and
  [all raw confirmation/control evidence](baselines/2026-09-13-storage-decode-tail-raw.json.gz).
- [Earlier investigation evidence](baselines/2026-09-13-storage-decode-investigation.json.gz):
  rejected dictionary cost, both interim direct comparisons, interrupted broader
  run, exact baseline reproductions, focused tests and first gate failure.
- [Dependency-feature investigation](baselines/2026-09-13-storage-decode-feature-checks.json.gz):
  second gate failure, feature graph, exact before/after fixtures under both
  feature configurations, and prospective final protocol versions.

Compressed records are JSON with original file text, lengths and SHA-256 hashes.
Their embedded runners, fixed plans and verification scripts reproduce the
process schedule and recompute every reported statistic. Final packaging checks
all source archive files against their commits, copied benchmark exceptions,
compiled artifact identities/features, logs, raw observations, counts, ordering
and summary correspondence. Binary hashes and compiler logs are retained;
executables can be rebuilt from the recorded sources rather than committed.

The original worker's first three logs lack preserved source snapshots; they
remain historical observations with that limitation. Later root reproductions
retain exact sources and final fixtures and establish the before-fix behavior.
No missing or interrupted run is represented as a completed measurement.

[Benchmark instructions](benchmarks.md) describe each fixture's API, cache and
throughput semantics and the workspace-feature build procedure. Broader query
memory, whole-file/index allocations, remaining integrity paths, offline repair
and volume supervision stay open in their own audit batches.
