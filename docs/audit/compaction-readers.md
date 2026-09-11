# Compaction publication and reader lifetime

Two deterministic fixtures expose a correctness bug at `1e27d838`: a reader
captured before successful compaction fails with `NotFound` afterward. A newly
opened reader returns the correct rows. Raw-to-page compaction unlinks the raw
columns; the legacy block-number profile rewrite unlinks its previous pages.
Readers remember paths and reopen those paths on demand, so their source can
disappear between query planning and materialization.

The raw fixture compares all 50 rows after compaction. The profile fixture
compares all 8,192 block numbers after the existing Delta-to-DeltaZigZag rewrite.
[Regression patch](baselines/2026-09-11-compaction-reader-before.patch),
[raw failure](baselines/2026-09-11-compaction-raw-reader-before.log), and
[profile failure](baselines/2026-09-11-compaction-profile-reader-before.log).

## Isolated captured-file experiment

The first prototype keeps handles to the declared physical column artifacts.
Clones share those handles. Bundled segments already retain one file and an
immutable table; their transport remains unchanged. Raw fixed columns use the
captured manifest's visible row prefix, including after a later append updates
the physical header. Existing raw validators are reused. The known schema bounds
an unbundled reader to at most 33 file handles.

Both failing fixtures pass with captured handles. All 190 storage library tests
pass (four ignored), strict storage Clippy passes, and release publication/query
fixtures pass. This is an isolated prototype, not an integrated acceptance result.
It does not yet fix directory reuse before manifest publication, opening during
publication, or stale maintenance ownership.

Five alternating release process pairs, three fresh datasets each, compare
`1e27d838` with the prototype on internal Apple Silicon APFS. Query workloads use
200,000 rows, 50,000-row segment targets, 8,192-row batches and four query workers.
Fixtures retain exact native/SQL/reopen oracles. No cache eviction or concurrent
local build/test workload is used. Complete source, binary hashes, machine and
runner parameters are included in the records. No sample is discarded.

| Query lifecycle metric | Dense median change | Sparse median change |
| --- | ---: | ---: |
| Generic live WAL ingestion | +3.29% | +1.06% |
| Generic historical WAL ingestion | +1.93% | +0.71% |
| Compaction | +1.01% | +1.55% |
| Index construction | +3.11% | +2.20% |
| Native filter | -2.26% | -1.30% |
| Concurrent native queries | -2.19% | -4.95% |
| SQL count | +12.38% | +13.95% |
| Ordered SQL | +9.39% | +8.14% |
| Warm reopen after compaction | +7.06% | +10.14% |

The SQL count regression is unacceptable for adoption. These queries capture
columns they never read; a projection-aware capture is being investigated.
Tail costs are also unresolved: sparse index-build p95 increases from 524.114
to 912.876 ms, historical ingestion from 303.788 to 343.370 ms, live ingestion
from 461.866 to 593.883 ms, and reopen from 15.016 to 35.574 ms. These observations
do not establish a cause or justify attributing the differences to noise.
[Full query comparison](baselines/2026-09-11-compaction-pinned-query.jsonl).

The separate combined-sync comparison has 15 samples per profile. Ingestion
medians change +0.18% mixed history, +0.48% mixed live and +0.28% sparse history.
Logical bytes, median allocated bytes and median OS-attributed writes are
unchanged. All exact rows/progress/reopen checks pass. Sparse full-read p95 rises
from 11.422 to 38.399 ms despite a +2.06% median change; this remains visible in
the record. These are storage fixtures, not network throughput or a staging soak.
[Full sync comparison](baselines/2026-09-11-compaction-pinned-sync.jsonl).

## Remaining publication work

Rewrite paths currently remove a fixed destination directory before publishing
its replacement manifest. Already-open handles address retirement of old files,
but new captures also need immutable generation names and a bounded retry when
publication retires their source. Replacement artifacts must be complete and
flushed before publication; cleanup must target only obsolete referenced files,
preserve unrelated artifacts, and report failures. Interruption tests and stale
plan review remain required before adopting the complete fix.

The interruption fixture now confirms incorrect decoded block numbers when a
legacy block-number column already occupies the rewrite destination. An injected
failure at the first publication flush leaves the old Delta descriptor pointing
to replacement DeltaZigZag pages. For example, an expected block number of 1,999
is read as 2,001. A second fixture confirms cleanup deletes unrelated files in
directories whose names merely start with `columns`.
[Interruption failure](baselines/2026-09-11-compaction-interruption-before.log),
[cleanup failure](baselines/2026-09-11-compaction-cleanup-before.log),
[test patch](baselines/2026-09-11-compaction-interruption-before.patch), and
[exact before source and focused validation](baselines/2026-09-11-compaction-interruption-validation.json).

The next isolated implementation exclusively creates a distinct directory for
each rewrite, publishes the complete referenced tree, then removes only old
manifest-referenced files and empty parent directories. Existing canonicality is
preserved. Both block-only and full rewrite fault sweeps pass locally, including
reopening and retrying maintenance after each interruption. Captured readers and
raw projected prefixes survive append/retirement; a deterministic open race
retries a retired manifest while same-generation missing files remain errors.
Workspace/platform acceptance and stale maintenance ownership remain open.

## Projection experiment

The first follow-up captures predicate columns (including index fallbacks), SQL
ordering/grouping columns and requested output columns. It uses the same paired
query parameters and passes all exact fixture oracles. SQL count medians change
-1.66%/-2.05% dense/sparse; ordered SQL changes -4.89%/-2.63%. Native filter and
concurrent-query medians also improve. Generic ingestion remains within 1%.
RSS medians decrease 3.0%/3.2%, but this includes all fixture buffers, not just
reader state.

Warm reopen remains +13.93%/+12.30%; this variant still captures every column in
writer-side inspection and index-identity checks. The next experiment limits
capture to reader ownership and requests only identity metadata for index
publication. That follow-up has not yet been measured. Tails remain visible:
sparse concurrent-query p95 is 184.822 versus 133.731 ms, and ordered SQL p95 is
13.770 versus 11.839 ms. Median gains alone do not establish final acceptance.
[Projection source and complete samples](baselines/2026-09-11-compaction-projected-query.jsonl).

## Ownership and publication follow-up

The next paired comparison includes immutable rewrite directories, precise
cleanup, writer inspection without reader capture, and index identity projection.
All 194 storage library tests (four ignored), 45 query unit tests and five query
integration tests (one ignored) pass, as do strict storage/query Clippy and
release fixtures. It uses the same five process pairs/15 samples per profile.

| Query metric | Dense median change | Sparse median change |
| --- | ---: | ---: |
| Generic live WAL ingestion | +1.38% | +2.27% |
| Generic historical WAL ingestion | -0.36% | -2.45% |
| Compaction | -1.55% | -2.06% |
| Index construction | +2.74% | -0.21% |
| Native filter | -3.32% | -3.71% |
| Concurrent native queries | -6.09% | -9.51% |
| SQL count | -11.84% | +0.18% |
| Ordered SQL | -1.08% | -6.98% |
| Warm reopen | +10.94% | +5.28% |

Dense count timings vary across the experiments; the apparent 11.84% improvement
is not claimed as a stable gain. Sparse live-ingestion p95 is 702.456 versus
465.819 ms, and compaction p95 is 121.035 versus 85.305 ms. Earlier query-tail
spikes do not repeat at their previous magnitude, but these new observations
remain in the record. Warm reopen still incurs unnecessary capture in the
boundary-block integrity check. The integrated follow-up projects that reader
to block number plus canonicality; its measurement is pending.
[Complete query comparison](baselines/2026-09-11-compaction-owned-query.jsonl).

Combined-sync ingestion medians change +0.68% mixed history, +1.02% mixed live
and -0.78% sparse history. Reopen medians remain within 1.1%; full-row validation
medians remain within 1.9%. Sparse full-read p95 increases from 9.406 to 12.479 ms.
Logical sizes and median OS-attributed writes are unchanged. All exact oracles
pass. The original-baseline sparse storage/read gap is not resolved by this fix.
[Complete sync comparison](baselines/2026-09-11-compaction-owned-sync.jsonl).

An initial Intel validation attempt reused the `1e27d838` binaries: the generated
archive had changed source contents but retained the base archive's timestamps,
allowing Cargo's shared target cache to treat them as unchanged. Matching binary
hashes exposed the error. That attempt was stopped and its disposable image was
detached; it is excluded from candidate validation. The corrected archive stamps
changed sources freshly, and the build checks for the new regression tests before
running ExFAT validation. No production volume contents were accessed.

## Integrated confirmation at ee502b56

All six required workspace gates pass: 905 tests, nine ignored, plus documentation
tests and the release node build. All six Linux/macOS CI jobs pass in run
34598239258. Source hashes bind the generated validation archive to the eight
changed source files in this commit.
[Local gates](baselines/2026-09-11-compaction-capture-local-validation.json),
[CI results](baselines/2026-09-11-compaction-capture-ci.json).

The final query comparison uses 15 alternating process pairs, three fresh datasets
per process: **45 samples per revision/profile**. Startup integrity validation now
captures only block numbers plus canonicality, retaining the preceding complete
raw-file/prefix checks. This removes the earlier startup penalty.

| Query metric | Dense median change | Sparse median change |
| --- | ---: | ---: |
| Generic live WAL ingestion | -1.96% | +1.88% |
| Generic historical WAL ingestion | -2.05% | -0.91% |
| Compaction | -0.03% | +0.65% |
| Index construction | +2.28% | +1.89% |
| Native filter | -3.42% | -3.21% |
| Concurrent native queries | -4.24% | -4.30% |
| SQL count | -3.72% | +0.90% |
| Ordered SQL | -6.78% | -4.16% |
| Warm reopen | -3.79% | -0.77% |

Every measured query/lifecycle median and p95 regression is below 5% in this
confirmation. Earlier larger tail spikes do not repeat at their previous magnitude.
RSS medians decrease 2.52%/0.54%; they include the full fixture and query buffers.
This supports retaining the compaction fix and projected capture without accepting
the first prototype's count/startup penalties. It does not resolve the older
original-baseline generic-write, index, startup or sparse-bundle costs.
[Exact source, parameters and all samples](baselines/2026-09-11-compaction-capture-query.jsonl).

The exact combined-sync comparison has 15 samples per profile. Ingestion medians
change -1.78% mixed history, +0.37% mixed live and +0.08% sparse history; median
reopen/read changes stay below 1.9%. Logical sizes and median OS-attributed writes
are unchanged. Sparse ingestion p95 rises 6.84%, and sparse full-row validation
p95 rises 7.43%, both within the user's 10% ceiling and still recorded for the
broader performance review. No samples are excluded. The preceding 15-sample
comparison had sparse ingestion p95 improve; these runs do not establish a stable
tail gain. All rows, progress, fixture digests and reopen oracles pass.
[Complete sync comparison](baselines/2026-09-11-compaction-capture-sync.jsonl).

Exact ExFAT validation on both Apple Silicon and Intel passes all 194 storage
tests/four ignored and five query tests/one ignored. Both matched disposable
images are detached. The block/full rewrite sweeps exercise 88/146 observed
main-thread publication boundaries on Apple Silicon and 48/77 on Intel; these
counts are discovered from each platform's actual successful operation trace.
They do not exhaust every worker syscall or prove physical power-loss behavior.
[Build hashes, runner sources and complete results](baselines/2026-09-11-compaction-capture-platform-validation.jsonl).
The rejected cached-binary attempt remains explicitly excluded in its
[validation record](baselines/2026-09-11-compaction-rejected-intel-validation.jsonl).

Ownership review confirms completed historical segments are not selected again
for ingestion: a new historical owner gets a newly allocated ID. This narrows the
stale-plan concern but does not prove exclusion between overlapping compaction
and standalone manifest refresh. A controlled refresh/publication interleaving is
the next regression to test; future bundle reclamation also remains open.

## Standalone manifest refresh race

The controlled interleaving reproduces another publication bug at `ee502b56`:
standalone refresh captures the raw column descriptors, compaction publishes pages
and retires the raw files, then refresh successfully republishes its old raw
descriptors. Both operations report success, but a new reader fails `NotFound`.
The test pauses refresh with channels after descriptor capture; no sleeps or
clock assumptions choose the interleaving.
[Before proof](baselines/2026-09-11-compaction-refresh-race-before.log),
[test-only patch](baselines/2026-09-11-compaction-refresh-race-before.patch), and
[exact source/focused validation](baselines/2026-09-11-compaction-refresh-race-validation.json).

The follow-up gives nonempty sealed-segment compaction and standalone manifest
refresh exclusive access to the same directory inode. Conflict returns
`WouldBlock` before mutation, so callers can retry. Ownership spans descriptor
capture, publication and cleanup, with explicit unlock on drop. Query readers
continue using their captured files. Active hot/historical ingestion ownership
retains its existing rules; this is not a new per-batch ingestion lock.

The regression passes with the guard, then retries compaction and checks exact
rows. All 11 focused compaction tests and strict storage Clippy pass, followed
by all six workspace gates (906 tests/nine ignored, documentation tests and the
release node build). All six Linux/macOS CI jobs pass at `a0c3aa64` in run
34600712890. Both ARM/Intel disposable ExFAT runs pass all 11 compaction tests
and five query integration tests/one ignored, then detach their matched images.
These targeted runs supplement the preceding full `ee502b56` platform suites.
[Local validation](baselines/2026-09-11-compaction-owner-local-validation.json),
[CI](baselines/2026-09-11-compaction-owner-ci.json), and
[platform source, binaries and results](baselines/2026-09-11-compaction-owner-platform-validation.jsonl).

Five alternating process pairs, three fresh datasets each, compare the exact
`a0c3aa64` release binaries against `ee502b56` with the same parameters above.
Compaction medians change -0.06% dense/+0.16% sparse. Query/lifecycle medians
stay within +4.6%; dense index p95 rises 8.54%. Sparse warm startup p95 rises
20.51% (14.064 to 16.948 ms) despite a +0.32% median (12.625 to 12.665 ms).
That observation prompted a larger confirmation; it is retained in the record.
[Initial query comparison](baselines/2026-09-11-compaction-owner-query.jsonl).

The independent sparse confirmation uses 15 alternating process pairs with three
fresh datasets each, **45 samples per revision**, without concurrent local builds
or tests. Warm startup median changes -0.71% (12.693 to 12.603 ms); p95 changes
+3.08% (13.935 to 14.364 ms). The earlier larger startup tail does not repeat.
Compaction median changes -0.96%, generic live/history -0.87%/-0.13%, and index
construction +0.10%. All query/lifecycle median and p95 regressions are below 5%.
Every exact native/SQL/reopen oracle passes, and no sample is discarded.
[Complete confirmation](baselines/2026-09-11-compaction-owner-sparse-confirmation.jsonl).

The separate combined-sync comparison has 15 samples per profile: ingestion
medians change +0.70% mixed history, -0.76% mixed live and +0.16% sparse history.
Read/reopen medians stay within +2.2%; mixed-live reopen p95 rises 5.23% and
remains recorded. Logical bytes and median OS-attributed writes are unchanged.
All exact rows/progress/reopen oracles pass. This supports retaining the ownership
guard without a material ingestion cost; it does not resolve the older costs
against the original baseline or establish P2P/staging acceptance.
[Complete sync comparison](baselines/2026-09-11-compaction-owner-sync.jsonl).

The confirmed compaction/refresh publication and captured-reader bugs are fixed.
Safe bundle reclamation and broader query/reorg snapshot behavior remain open;
PR #130 stays draft until its remaining correctness and performance work passes.
