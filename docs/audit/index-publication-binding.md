# Index publication binding

Status: artifact binding committed as `61a2deec`, short-path fixture as `271564d4`,
checkpoint parsing optimization as `64598064`, and the steady concurrency fixture
as `f996c990` on `audit/index-source-binding`, based on
PR #148 merge `e317a5295856da23d0078b621099e46e44195908`. This is an offline
correctness milestone, not completion of batch 6 or release approval.

## Confirmed findings

The V5 derived-file container validates its own header and pages, including a
random 128-bit file identity. Before this milestone, the publication checkpoint
did not record the expected identities. A complete file copied from another
index retained valid internal checks and could silently exclude matching rows.

These are high-priority data-integrity findings because the failure is a silent
missing result, rather than a visible read error. They require incorrect local
artifact placement or source replacement; this does not change the consensus
trust model.

Three finite, two-row fixtures reproduced the failures on `26a8818c`, with only
regression tests added. Each fixture compares indexed row IDs with an independent
full scan of generated source rows. All three returned an empty indexed result
where the scan returned one matching row:

| Case | Affected behavior | Disposition |
| --- | --- | --- |
| A complete block-hash index copied from another source | A block-hash lookup misses row 1 | Fix in this milestone |
| A timestamp index copied to the block-number index path | A block range misses row 0; both key widths are eight bytes | Fix in this milestone |
| A complete checkpoint and index directory copied between equal-row legacy sources | An address lookup misses row 0 | Separate source-publication milestone; remains open |

The reproducer command was `cargo test -p logex-query --lib --locked
native::tests::complete_`. It exited 101 with three expected assertion failures;
compilation succeeded. The complete patch, output and source hash are retained in the
[before-fix evidence](baselines/2026-09-13-index-binding-reproduction-1.json).
The inputs use disposable local directories only.

## Implementation boundary

Record a bounded mapping of canonical artifact names to their existing random
file identities in the checkpoint. Published query paths must compare an
artifact's expected identity against the header from the same opened file before
using either a matching bitmap or an absence result. This includes B-tree point
and range reads, composite lookups, and native and SQL bloom prechecks.

Builders must validate existing identities before reusing files, including when
expanding an index profile. A missing, unregistered or replaced artifact must be
rebuilt, scanned around safely, or reported as an explicit error. Earlier
checkpoint versions require rebuilding derived indexes. Keep source columns,
V5 index encodings and ingestion publication unchanged. Use already-read headers
on query paths; do not add per-query directory or full-file scans.

The implementation uses checkpoint V4 with at most 12 unique names, names of at
most 64 ASCII bytes and a 4 KiB total checkpoint limit. Registration and decoding
both validate names/counts; duplicate registration preserves the original ID.
Header-only builder probes preserve unrelated I/O errors instead of treating all
failures as permission to replace an artifact. Current bound range reads share
the same initialized full-file path as unbound reads.

Focused validation passes: nine checkpoint tests, 80 index library tests, ten
native query tests, formatting and strict Clippy across all targets in the three
touched crates. Cases include complete primary/composite/bloom substitution, the
shared SQL bloom precheck, missing/replaced/unregistered artifact rebuilding,
profile expansion, read-side duplicate/invalid/oversized registration lists and
legacy marker rebuilds. The parsing optimization also exercises an explicitly
unsorted valid checkpoint with the earlier decimal-array ID representation.
All nine local gates pass at `64598064`: 1,105 workspace tests (22 ignored),
143 release query tests (eight ignored), both release protocol checks, zero doc
test examples, formatting, checking, strict Clippy, vendor verification and the
release node build. Complete [validation logs and hashes](baselines/2026-09-13-index-binding-validation.json)
are retained. The later diagnostic fixture passes formatting, a bounded
four-worker smoke run and strict Clippy; its release comparison is complete.

## Release comparisons and initial investigations

The [fixed mixed comparison](baselines/2026-09-13-index-binding-mixed-1-release.json)
uses exact source archives at `26a8818c` and `61a2deec`, fresh release binaries,
the pinned nightly, identical existing fixtures, the workspace dependency feature
union and ten alternating process pairs per workload. It retains 10,000 measured
timings and 100 process memory observations. The fixture warms query paths once
before measurement; those existing warm-up calls do not emit separate timings.
All emitted samples, outliers and counters are retained.

Dense and sparse engine-query medians differ by at most 0.5%. Actual live and
historical storage-publication medians change -0.34% and -0.31%; the corresponding
p95 changes are -5.44% and -19.89%. Ingestion code is unchanged, so large tail
improvements are observations rather than claimed algorithmic gains.

Initial concurrent-query p95 increased 14.74% for dense data and 15.23% for sparse
data despite median changes of only +0.10%. A
[fixed confirmation and control run](baselines/2026-09-13-index-binding-tail-1.json)
therefore repeats both complete integrated workloads with 20 additional source
pairs and ten identical-candidate-binary control pairs each. It retains another
10,800 measured timings and 120 memory observations. Controls remain separate
from comparisons between source revisions.

| Metric | Dense combined median / p95 | Sparse combined median / p95 |
| --- | --- | --- |
| Live storage ingestion | -1.46% / -35.75% | +0.99% / +0.79% |
| Historical storage ingestion | +0.28% / -34.38% | +0.42% / -1.26% |
| Index construction | +0.38% / -3.65% | +0.94% / +0.74% |
| Native filter | -0.21% / -6.64% | +0.33% / +0.19% |
| Concurrent native queries | -0.06% / +5.52% | -0.11% / +8.32% |

These combined values include every initial and confirmation source sample.
Confirmation-only concurrent-query p95 changes are +2.16% and +5.47%; identical
candidate controls vary -9.72% and +1.37%. This does not identify the cause of
every initial spike or prove every tail is below the budget. The retained
combined +5.52%/+8.32% observations remain explicit inputs to the broader
integrated audit. No repeatable source-induced regression above 10% has been
established by these mixed-workload comparisons.

The timed mixed queries use bound composite point lookups on all three segments.
They open common bloom files but do not probe bloom bits, because the selected
filter does not constrain topic positions 1 or 2. The [short-path comparison](baselines/2026-09-13-index-binding-paths-1-release.json)
uses an identical compatible fixture on both revisions, with independent row-field
oracles outside timers. Ten alternating pairs retain 10,000 measured timings,
100 explicitly timed warm-ups and 20 memory observations. Primary point, block
range, timestamp range and positive bloom/composite medians change +2.80%, 0.00%,
+0.40% and +0.64%; their p95 changes range from -0.98% to -0.12%. Fast bloom
exclusions change +6.53% at the median (0.285687 to 0.304333 ms) and +2.69% at p95.

That exclusion median exceeds the 5% investigation threshold. Commit `64598064`
therefore replaces the two read-side tree collections with one validated, sorted
vector and binary search across at most 12 entries. IDs serialize as compact hex
instead of decimal arrays; both encodings decode through the existing fixed-byte
serde implementation. Source columns, artifact framing, durability barriers and
all duplicate/name/count checks remain unchanged. The [fixed before/after comparison](baselines/2026-09-13-index-binding-optimization-2-release.json)
retains another 10,000 measured timings, 100 timed warm-ups and 20 memory
observations. All five medians improve: primary point -1.51%, block range -0.70%,
timestamp range -0.90%, positive bloom/composite -0.87%, and fast bloom exclusion
-2.61%. Exclusion p95 improves 5.75%; nine of ten paired process medians improve.
The final comparison against the merged baseline follows below. An initial
metadata probe was denied by the local sandbox before any build or measurement;
its output is retained beside the successful authorized run. It is an environment
failure, not a product test failure.

The ingestion fixtures exercise storage publication, not a complete running node.
The background indexer also calls freshness checks while selecting work under a
storage read guard, currently using the ERC20 profile. Its scheduling, lock hold
times and worker behavior remain part of the runtime/integrated audit. These
finite measurements cannot establish live node throughput or uniform tail bounds.

## Final-source comparison and follow-up

The [final-source release comparison](baselines/2026-09-13-index-binding-final-1-release.json)
compares `26a8818c` with `64598064`, using fresh workspace-selected artifacts and
identical compatible fixtures across six workloads. It retains 20,000 measured
timings, 100 explicitly timed short-path warm-ups, 120 process memory observations
and all storage/lifecycle counters. All independent result oracles pass.

All five short-path median increases are below 3.2%: primary point +2.46%, block
range +1.30%, timestamp range +0.87%, positive bloom/composite +0.57%, and fast
bloom exclusion +3.15%. Dense and sparse mixed storage-ingestion medians remain
within 1.2% of baseline. Actual live/historical publication medians change -1.30%
and +0.47%; their p95 changes are -16.14% and -14.31%. Large negative tails are
observations, not attributed algorithmic gains.

Dense engine-query medians increase 5.13–6.08%, with coordinated variation across
query shapes and process pairs. Sparse concurrent-query p95 increases 16.61%,
and sparse process-memory p95 increases 42.12%. Every observation remains in the
initial report. A predefined follow-up repeated the entire dense engine and sparse
integrated workloads with 20 balanced source pairs and 10 identical-candidate
control pairs each. It uses the exact saved artifacts and unchanged parameters;
controls remain separate from source comparisons. The [fixed follow-up](baselines/2026-09-13-index-binding-final-tail-1.json)
retains 14,400 measured timings and 120 memory observations. Combined dense engine
medians are +0.12% to +0.39%; their confirmation medians are within 0.1%. Combined
sparse live/historical ingestion medians are +0.90%/+0.22%, with p95 -0.01%/+4.34%.
Combined sparse memory p95 is unchanged; the earlier +42.12% observation remains
retained.

Sparse concurrent-query confirmation median/p95 are -0.37%/+7.55%; combining every
initial and confirmation source sample gives -0.69%/+11.47% (p95 15.695250 to
17.495958 ms). Identical-candidate concurrent p95 changes -3.96%, while native
filter and historical-ingestion controls vary +13.31% and +11.81%. Controls show
tail variability without proving the cause of every source-comparison spike.
Combined sparse ordered-query p95 also remains +5.93%.

The call-path review finds no new locks, extended guard lifetime, whole-file reads
or added artifact metadata syscalls. The extra work is bounded checkpoint
artifact decoding and small allocations across twelve checkpoint reads per
four-worker batch. This is a plausible scaling cost and does not establish the
cause of the observed tail. A separate steady concurrent-query fixture was
prepared to measure batch and individual-worker timing against one fixed
snapshot, alongside the retained mixed results. The completed diagnostic and
performance disposition follow below. All nine workspace/release gates pass at
the production-change commit;
the added diagnostic fixture is committed as `f996c990` after focused checks.
Its twenty-pair release comparison and ten-pair identical-candidate control used
predefined schedules. These record wall-clock durations, not per-thread CPU time.

## Performance disposition

The [steady concurrent-query comparison](baselines/2026-09-13-index-binding-concurrent-1-release.json)
uses twenty balanced source pairs, with one compacted/reopened snapshot and 100
four-worker batches per process. It retains 20,000 batch/worker timings, 200
warm-up timings and 40 memory observations. Batch median/p95 changes are
-2.19%/-4.95%; worker medians improve 2.09–2.38% and worker p95 improves
4.33–5.86%. All results match the independent row-field oracle. Each batch timing
is checked against the longest worker duration; these are wall-clock observations,
not CPU-time measurements.

The predeclared [identical-binary control](baselines/2026-09-13-index-binding-concurrent-control-1.json)
retains another 10,000 batch/worker timings, 100 warm-up timings and 20 memory
observations. Its batch median/p95 varies +0.46%/+5.56%, and worker p95 varies
+2.94% to +3.46%. Controls are never pooled with source comparisons.

Retain the artifact-binding fix and the measured checkpoint parsing optimization.
The production publication and ingestion medians remain within the required
budget, all final short-query median increases are below 3.2%, and the initial
concurrent tail increase does not repeat above 10% in the fixed confirmation or
steady diagnostic. The evidence does not establish a repeatable source-induced
regression above 10%. It also does not identify every timing fluctuation or
certify every tail below that threshold: the combined mixed sparse concurrent
p95 +11.47% and ordered-query p95 +5.93% remain explicit, unresolved observations
for the broader integrated audit. This is not acceptance of an established
above-budget implementation tradeoff, and does not approve live sync or release.

Across this milestone's eight fixed comparisons/control runs, the archives retain
105,200 measured timing observations, 600 explicitly timed warm-ups and 560
process memory observations. Batch/worker timings and repeated samples within a
process are correlated, not independent trials. Existing mixed/engine warm-ups
that do not emit timings are disclosed above. Every emitted sample, counter,
outlier and failed attempt is retained; no source/control observations are removed.

## Reproducing the added fixtures

Run the short paths with `cargo test --workspace --test audit_harness --release
--locked benchmark_index_publication_binding_query_paths -- --exact --ignored
--nocapture --test-threads=1`. Its `LOGEX_BINDING_ROWS`,
`LOGEX_BINDING_SEGMENT_ROWS` and `LOGEX_BINDING_REPEATS` defaults are 20,000, 8,192
and 100. It generates temporary data and records one explicit warm-up per path.

Run the steady diagnostic with `cargo test --workspace --test audit_harness
--release --locked benchmark_steady_concurrent_native_queries -- --exact --ignored
--nocapture --test-threads=1`. Its `LOGEX_CONCURRENT_PROFILE`,
`LOGEX_CONCURRENT_ROWS`, `LOGEX_CONCURRENT_SEGMENT_ROWS`,
`LOGEX_CONCURRENT_REPEATS` and `LOGEX_CONCURRENT_WORKERS` defaults are sparse,
20,000, 8,192, 100 and four. Bounds are documented in the fixture; it creates a
fresh temporary dataset and does not use an operator's data directory.

For revision comparisons, use the exact plans, runners, source/fixture hashes,
workspace feature union and balanced schedules embedded in each linked report.
The compatible fixture is copied identically to both Git archives. Verify the
saved artifacts before executing controls, and run builds, tests and measurements
sequentially. Preserve all warm-ups and observations rather than selecting runs.

## Source identity remains open

The current source identity contains row count, generation and an optional bundle
reference. Manifest-less sources use generation zero and no bundle reference.
Equal-row sources can therefore have identical checkpoint identities. Unbundled
manifests also lack an independent source namespace, and segment numbers are
local to a database. Bundle references describe positions, lengths and checksums;
they are not an independent source namespace.

A copied checkpoint carries any recorded artifact identities with it. The
artifact mapping alone therefore does not fix the third reproducer. Likewise,
replacing legacy columns with different data of the same row count can leave
the old source identity unchanged.

The next milestone must establish identity from source-owned publication state,
retain that identity in captured readers, and invalidate it on replacement.
A stable sidecar alone is insufficient: it survives equal-row replacement.
Updating a replacement token only before or only after publishing individual
columns also leaves a capture race. Trace committed/in-progress source state,
legacy migration, native append/replacement, recovery and restored datasets before
selecting the final mechanism. Avoid new per-batch durability barriers or source
scans during queries. Preserve the whole-set reproducer for that milestone.

## Cleanup and merge closure

Caller searches and strict Clippy confirm every published primary, composite,
range and bloom open uses the expected identity; the shared SQL precheck receives
the caller-held guard. Direct-file APIs remain intentionally public for standalone
readers and tests. Shared helpers preserve one decoding implementation and the
initialized full-read path. The read-side tree collections and unused set import
are removed; no unrelated code was deleted speculatively.

All nine final local gates pass at `f996c990f3908ddee3240105be4725f2f8aad490`,
including the added steady fixture: 1,105 workspace tests (23 ignored), 143
release query tests (nine ignored), both release protocol-consistency tests,
zero doc-test examples, formatting, workspace checking, strict Clippy, vendor
verification and the release node build. Complete [final validation evidence](baselines/2026-09-13-index-binding-validation-final.json)
includes every focused attempt and source/log hashes. PR #149 merged as
`9c0a58fc64fac3da7f5a50b3a20b7ab729f029e9` on 2026-09-13 after all six CI jobs
passed at `ce2da301c1100a2c8dfffea3c13a69c630eaae51`, run `34742994503`,
attempt 1. The [CI and merge record](baselines/2026-09-13-index-binding-ci-final.json)
retains job results and verifies that the merged tree equals the tested head.
The separate source-publication issue and broader audit work stay open.
