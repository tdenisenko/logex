# Derived index file integrity

This batch is in progress on `audit/index-file-integrity`, based on PR #147
merge `ae4c01c913f89e8448a2c98d5905733525c51047`. Final performance comparisons and all nine local validation gates are complete;
PR CI and merge remain pending. It changes derived indexes, not log columns or ingestion
transactions. Whole-file source identity and the rest of batch 6 remain open.

**Measurement correction:** screens 1–13 timed the row-ID correctness oracle
inside `point_present` and `open_range`. For the large-payload fixture this adds
a full scan of 262,144 returned rows, diluting the relative cost of the actual
lookup. Those screens remain retained investigation history and are not raw
lookup performance acceptance. The corrected fixture stops the clock before
assertions and oracle iteration, keeps full-reader destruction timed, and checks
every returned result afterwards. It is copied identically into both revisions
for a new comparison. No previous sample is removed or relabeled as raw lookup
latency.

Earlier nine local gates passed at exact head `3b0a52b7`, including 65 index
unit tests, workspace/release checks and both protocol consistency tests. Those
results predate the later reader/writer optimizations. Current production source
`7c99c9f4` passes all 79 focused index tests. Final exact-source validation at
`b6076c00` passes all nine gates, including 1,096 workspace tests, 139 release
query tests and both release protocol-consistency tests. Complete logs, source
hashes and independently checked totals are in
[final local validation](baselines/2026-09-13-index-integrity-validation.json). Earlier logs
remain in `/private/tmp/logex-index-integrity-gates-1`.

## Findings and reproduction

- **B6-01, P1: successful absence from incomplete B-tree files.** Unknown
  versions enter the v1 reader; v1 can skip beyond EOF; v2 can compare a key and
  return absence without requiring its descriptor/payload to exist. Full readers
  accept unsorted/duplicate keys, aliased payloads and trailing bytes, although
  lookup/range execution assumes a complete, strictly ordered index. Declared
  counts and lengths also reach allocations before checking available bytes.
  Native callers can use a successful absence to omit matching logs.
- **B6-02, P1: bloom damage can silently omit matching logs.** Both bloom formats
  lack persisted bit integrity. Clearing the first tested bit for a known-present
  key returns `false`. Readers also accept non-writer bit/round geometry and
  incomplete or extended files. The fixture uses ordinary 2 MiB local files.
- **B6-03, P1: inconsistent source-column row counts.** Builders
  zip separately read columns. Manifest-less complete columns can have different
  lengths, so a shorter column can omit entries before publishing the address
  column's row identity. A finite three-row fixture tests all index profiles with
  a complete two-row or four-row replacement topic column.
- **B6-04, P2: legacy bitmap metadata.** The pinned Roaring
  reader does not use stored offsets or check each run container's declared
  cardinality. Small valid controls and inconsistent descriptors are retained;
  all three inconsistent fixtures reproduced successful acceptance and are now rejected.

Before production changes, `/private/tmp/logex-index-integrity-before-1` captured
the complete source inventory and run: **30 passed, eight failed**. The failures
cover the incomplete layouts, ordering/alias/trailing cases and both bloom
problems. Tests looping versions can stop at the first failing assertion;
post-fix coverage must exercise every case. No large declared-size allocation
fixture ran against the unbounded reader.

The first implementation snapshot in
`/private/tmp/logex-index-integrity-focused-1` passes **47 index unit tests**.
Additional before-fix run `logex-index-integrity-additional-before-1` reproduced
all six mismatched source-column/profile combinations and three bitmap metadata
cases (**48 passed, four failed**). The final focused snapshot
`logex-index-integrity-focused-3` passes **55 unit tests** and compiles the ignored
performance fixture. All five publication tests pass, including legacy rebuild,
version-digest mutation, locks and interrupted publication. Focused Clippy passed
on its second run after fixing two constant-chunk lints and a test tuple type;
the first failure log is retained. Workspace/release/performance gates remain
pending.

## Implementation under validation

New derived files wrap their existing logical B-tree/bloom encoding in a page
integrity container. Its 48-byte header fixes magic, version, 4096-byte page size,
logical length, a per-file 128-bit random identity and reserved fields, protected by CRC32. The original logical
bytes remain contiguous. The current experiment uses container version 5 and
one eight-byte metadata-seeded XXH3 fingerprint per page. Screens 1–16 used
version 1 and four-byte CRC32 page checks; screen 17 used version 2 and XXH64.
Each page fingerprint
includes a domain tag, format version, logical length, file identity, page position and actual
page length. Version 3 hashes that complete metadata into a seed and uses the
standard seeded hash on the page bytes. Versions 4, 5 and 6 retain this construction with
2, 4 and 8 KiB pages respectively and their own version domains. This is a distinct encoding from hashing
the concatenated metadata and page. Exact physical extent is checked using the
opened handle.

Point lookups validate only bytes from pages they inspect, keeping logarithmic
table search. Bloom exclusions verify the page containing the tested bit. Range
readers retain contiguous whole-file reads and validate every page. A reader
holds a 4 KiB data-page cache and a checksum cache capped at 16 KiB (smaller files
allocate only their footer size); no global validation cache or
per-query full-file scrub is introduced. Writer fingerprint storage costs eight
bytes per logical page, and B-tree payloads stream once instead of retaining all
serialized payload buffers. Integrity overhead on disk is 48 bytes plus eight
bytes per page; actual runtime cost still needs measurement.

Every primary/composite/bloom builder checks the length of every participating
column against the captured row count before iteration or u32 row-ID conversion.
A mismatch leaves the set unpublished. Bitmap preflight checks container order,
canonical offsets, exact extents and each run container's declared cardinality
and ordered disjoint ranges; the dependency validates array and bitmap contents.
Each bitmap payload is decoded once.

Full B-tree decoding validates version, key width, counts/extents, strictly
increasing keys, contiguous payload descriptors and exact bitmap/EOF consumption.
Legacy raw B-trees remain structurally readable through a full validation path.
Raw blooms return a rebuild diagnostic because structural validation cannot
detect a cleared presence bit.

Index publication advances from `LXICP002` to `LXICP003`. A valid old marker
causes source-column fallback and rebuild; it does not authorize old index
pruning. The new marker's digest binds both format magic and payload, so changing
an old marker's version cannot promote its unchecked files. Existing locks,
source identity and durable publication ordering remain in use. Normal builds
recreate known files when the previous marker is unavailable/old. Log storage
formats and ingestion writes are unchanged. Downgrading after rebuilding derived
indexes is not supported by the older reader; operators must rebuild derived
files using their selected version. No production directory is modified here.

## Boundaries and outstanding validation

The page fingerprints detect accidental persisted-byte changes; they do not authenticate data,
prove writer correctness or detect a complete valid file copied from a different
segment. Publication/source binding of individual artifacts is a separate open
batch-6 requirement. The current publication lock must be held throughout query
reads; page checks do not replace generation/lifetime coordination.

Required before this milestone is accepted: finish focused regressions and
independent reference coverage, inspect obsolete paths, run every workspace and
release gate, measure equivalent release index construction/point/range/bloom
workloads and the existing mixed ingestion/query fixtures, retain all samples
and investigated regressions, document exact source/CI evidence, then merge.
No source-induced repeatable regression over 10% will be accepted.

## Corrected read-cost investigation

The [supplemental evidence](baselines/2026-09-13-index-integrity-investigation-supplement.json)
retains screens 13–16: 25,296 timings and 136 RSS observations, independently
rechecked against sources, binaries, logs and the fixed schedules. Together with
the first archive this preserves 96,720 timings and 520 RSS observations.
It also retains all nine passing gates at `3b0a52b7` (1,082 workspace tests,
139 release query tests and two release consistency tests), subsequent focused
checks, benchmark correction failures and diagnostic profiles. Final source
validation is still required after later implementation changes.

Corrected screen 14 (`18204b14`) exposes large-bitmap point/full-open median
regressions of 21.52%/35.07%. Screen 15 (`21f96a04`) removes unnecessary full-read
caches, overlapping prefetch and explicit zero-fill, while retaining one opened
handle and shared framing checks. All 65 focused tests and Clippy pass, but
large point/full-open medians remain 19.65%/29.77% above baseline. Screen 16
(`b1af037d`) adds nonconsecutive row IDs with the same returned cardinality;
its point/full-open medians are 37.79%/43.84% above baseline. None is accepted.

Finite candidate-14 profiles isolate reads from oracle iteration and confirm
that page checks contribute significant CPU cost. A separate 108-sample cost
experiment compares the same contextual byte stream using pinned libraries.
For 1 KiB pages, CRC32 and streaming XXH64 medians are 180.88 and 119.13 ns;
short 64-byte input favors CRC32. These are algorithm-cost diagnostics, not
node throughput or index acceptance results.

The next isolated candidate uses the already resolved `twox-hash` 2.1.2 XXH64
feature. It preserves every page, identity, length, position and error check.
The footer grows from four to eight bytes per page; the header stays CRC32.
The 16 KiB footer cache now spans 2 MiB of logical bytes, so a maximum-sized
bloom with its extra 20-byte logical header crosses a cache-window boundary.
This memory/I/O tradeoff must be measured. The old unmerged container prototype
is rejected by the explicit version check; released legacy indexes still follow
the documented rebuild path. All 66 focused tests pass, including an independent
one-shot check of complete contextual fingerprint bytes.

XXH64 is a noncryptographic fingerprint, with different error-detection
properties from CRC32; it does not inherit CRC burst-error guarantees or provide
authentication. This use is limited to accidental persisted-byte changes.
Complete artifact/source identity remains separate work.
See the [algorithm's official documentation](https://github.com/Cyan4973/xxHash).

Screen 17 (`540ad32c`) retains 7,440 timings and 40 RSS observations. XXH64
improves the large contiguous point/full-open medians to +13.66%/+22.65%, but the
gapped case remains +27.42%/+31.77%. The candidate is insufficient and unaccepted.
Further finite diagnostics compare full copy/initialization costs with standard
metadata-seeded XXH3. The latter measures 54.24 ns per 1 KiB page versus 102.24 ns
for streaming XXH64 in the same rotated-order experiment. The earlier
prepacked-only and slower scalar/copy variants remain retained. Across four
diagnostic matrices, 612 original timings are preserved; 384 deterministic
same-mode reference cases match between scalar and accelerated builds.

The next candidate uses exactly that measured seeded construction, with flat
44-byte file context and 60-byte per-page metadata. The footer geometry and all
structural checks are unchanged from version 2. No page-copy buffer, per-page
heap allocation or project `unsafe` block is introduced. Both discarded
prototype versions are explicitly rejected. All 66 index tests pass.

The dependency remains pinned at 2.1.2; its verified archive digest matches
`Cargo.lock`. Enabling only `std` and `xxhash3_64` adds the `alloc` feature closure,
but the selected one-shot APIs use fixed local buffers. Random/default features
remain disabled. Inspection confirms runtime CPU guards before NEON/AVX2/SSE2
dispatch and scalar fallback. Internal slice chunking uses bounded array views;
the selected one-shot path uses the library's fixed valid 192-byte internal
constant. This review and scalar/vector equivalence do not replace final
workspace-feature measurements and macOS/Linux CI.

## First performance screening (not accepted)

Exact candidate `9f14ceb4` versus baseline `ae4c01c9`, with identical copied
release fixture and workspace feature union, retained 5,952 timings and 32 RSS
observations in `/private/tmp/logex-index-integrity-release-1`. Four balanced
pairs covered 16/8,192/65,536 keys and a one-key large-bitmap case. Each process
checks 30 point/range/bloom samples per metric and three write/build samples.

The candidate exceeds the budget: typical/many-key full-open range medians are
+16.25%/+16.37%, bloom medians generally +13–17%, and B-tree writes +12–54%.
Small-workload tails also vary substantially. These are index microbenchmarks,
not live-sync measurements. No result is discarded or accepted as node throughput.

The next candidate removes redundant first-page reads and general-loop overhead
for the common single-container bitmap while retaining every validation check.
Independent review also identified a prototype gap: copying a page together with
its checksum from another same-sized file passed the original framing. A retained
finite fixture reproduces it; per-file random checksum context corrects it. The
new header is 48 bytes. A complete valid-file substitution remains outside this
check and still needs source binding. All 57 focused index tests pass after these
changes; their equivalent release measurements are pending. Writer buffering and
page-granularity costs will be investigated separately if they remain excessive.

Second screening (`0d77f36b`, `/private/tmp/logex-index-integrity-release-2`)
retains another 5,952 timings/32 RSS. It still exceeds the budget: typical/many-key
range medians +17.90%/+17.56%, B-tree writes +14–59%, and several bloom paths
remain excessive. No performance acceptance is inferred. Five-second profiles of
both retained binaries are in `logex-index-integrity-profiles-1`; profiled runs
are separate from acceptance measurements. They identify allocation/free work,
parser helpers and checksum work on the candidate range path.

The third candidate changes only writer buffering: accumulate small serialized
writes per page, retain contiguous bulk writes, handle explicit flush prefixes,
and make a failed writer unusable. Finite mixed/flush/write-error fixtures bring
the focused total to 60 passing tests; focused Clippy passes. Its fixed comparison
(`bed0ee6e`, `logex-index-integrity-release-3`) retains 5,952 timings and 32 RSS
observations. Typical B-tree write median falls from +59.14% to +22.30% relative
to baseline; the many-key write median is -6.77%. Small and large-payload writes
remain +30.70%/+43.79%, typical/many-key ranges +17.25%/+15.89%, and several bloom
paths still exceed 10%. This candidate is not accepted.

The fourth candidate (`854a68c7`, `logex-index-integrity-release-4`) passes the
same 60 focused tests. Complete bitmap preflight already establishes exact
consumption, so the dependency now decodes the borrowed slice directly, avoiding
the redundant mutable-slice remainder path. Review checked preflight and decoder
extent agreement for empty, array, dense and run containers. Another 5,952
timings/32 RSS show typical/many-key range medians +9.84%/+7.26%, while small
ranges remain +14.95%. Typical/large-payload writes remain +27.66%/+37.02%; bloom
lookup medians remain +11.78–23.67%. All observations, including variable small
workload tails, are retained. This candidate is also not accepted.

The next isolated experiment increases the physical output buffer from 8 KiB to
64 KiB. Inspection of pinned Roaring serialization shows that dense containers
emit 1,024 scalar eight-byte writes; the page writer combines those into 4 KiB
writes. The larger sink can reduce physical flushes. It does not remove scalar
dispatch costs, which will require a separate measured change if still relevant.
All ten file-container tests pass with the larger sink. Screening five
(`ba98f78d`, `logex-index-integrity-release-5`) retains 5,952 timings/32 RSS.
Small/typical/many-key/large-payload write medians are +9.88%/+20.70%/-17.03%/
+14.17%; bloom medians remain +10.34–15.83%. It is insufficient by itself.

Screening six (`a61cd867`, `logex-index-integrity-release-6`) adds a concrete
64 KiB buffer before the checked writer. This lets the pinned bitmap serializer
inline scalar writes and batches them before checksum processing, without
retaining every serialized payload. Explicit final flush propagates errors.
All 60 focused tests pass; another 5,952 timings/32 RSS show write medians
-12.02%/-62.50%/-63.25%/-15.92% for small/typical/many-key/large payloads.
Some write tails remain variable and require more final samples. Bloom medians
remain excessive; this is progress on writes, not complete acceptance.

Screening seven (`c6ed7e99`, `logex-index-integrity-release-7`) tests reusing the
per-file checksum prefix. It retains 5,952 timings/32 RSS but does not establish
a clear gain beyond noise; the change is dropped. A useful ordinary roundtrip
test is retained: nine dense bitmap containers cross the 64 KiB serialization
buffer, with exact row IDs checked through full and point readers. All 61 focused
tests pass. Borrowed multi-bitmap union was also reviewed as a range-cost lead,
but unconditional use can expand many sparse containers, so it is not introduced.

The next experiment returns to screening six's production code and prefetches
the header and first logical page in one bounded read. A complete single-page
file also supplies its checksum footer. Prefetched data remains unverified until
the existing checksum check succeeds; invalidation precedes fallible loads.
Large full-file reads retain contiguous I/O. All 61 focused tests pass before
its separate release comparison.

## Subsequent read and build investigation

The first twelve complete screens are retained in
[`2026-09-13-index-integrity-investigation.json`](baselines/2026-09-13-index-integrity-investigation.json)
and its linked compressed raw archive: **71,424 timings and 384 RSS observations**.
The independent packager checks Git source/archive agreement, copied fixture
identity, fresh workspace feature-union artifacts, every process and log, balanced
ordering, sample counts and recomputed statistics. The archive also preserves
reproductions, focused failures/successes and diagnostic profiles. No screening
candidate is claimed as final acceptance.

- Screen eight (`1b573d21`) adds the bounded prefetch. Typical/many-key bloom
  presence medians remain +12.66%/+12.74%; initial small processes vary strongly.
- Screen nine (`b64f0979`) uses 1 KiB data pages and up to 16 KiB of footer cache.
  All 62 focused tests pass, including exact reads across footer-cache windows.
  Reused bloom presence medians improve by 32.92–34.54%; absent-open medians
  are +2.70–7.06%. Small builds and ranges remain excessive.
- Screen ten (`ff36d8f9`) uses borrowed multi-bitmap union for reader ranges only
  when represented high-16-bit row containers span at most eight values. This
  bounds temporarily promoted bitmap payload to 64 KiB, plus small descriptors
  and the result; wider spans retain pairwise union. Zero/one entries return
  directly. Independent row-pair/BTreeSet controls cover overlapping, sparse,
  dense and distant rows, boundary selections and inclusive/exclusive endpoints.
  All 63 focused tests pass. Typical/many-key range medians are -45.80%/-47.07%.
- Screen eleven (`168b9e6d`) reads full logical data and its footer together,
  verifies every page before returning bytes, and shares the verifier with random
  reads. Allocation remains fallible and bounded by actual opened-file geometry;
  small/raw paths reset their cursor. Typical/many-key range medians are
  -48.35%/-49.46%; later small-process medians are about 16.1–16.2 microseconds
  versus 14.7, while the all-process aggregate retains larger startup variation.
- Screen twelve (`5032073c`) revisits checksum metadata cost with four times as
  many pages: it reuses the per-file prefix and batches page index/length into one
  update. Review verifies identical checksum bytes. Reused bloom medians improve
  to roughly -37% on the larger configurations. Small bloom builds remain about
  2.45 ms in later processes versus 2.20–2.24 ms; the aggregate is +22.12%.

The second short profiling attempt retains both successful fixture/sampler exits,
but the baseline process ended before stack collection. Candidate stacks identify
checksum work and file operations alongside the existing key hashing; this is
diagnostic evidence, not a paired CPU comparison.

The next candidate sizes bloom payloads to captured rows within 256 KiB–2 MiB,
retaining four hash probes and the existing maximum. Smaller segments currently
pay to write and verify a fixed 2 MiB even when much of its capacity is unnecessary.
The proposed allocation rounds 32 bytes per source row up to a permitted power of
two, with clamping before multiplication. Up to 65,536 rows this supplies at least
128 bits per possible inserted topic key (at most two per row); above that the
previous 2 MiB geometry remains. The usual uniform-hash false-positive estimate
is below one part per million through that threshold, but this is an estimate,
not a guarantee. Smaller filters can increase false positives versus the old
oversized file. Exact no-false-negative checks and measured query/build cost are
required before accepting this tradeoff. Existing protected 2 MiB files remain
supported under the current page format.


## Remaining read overhead after seeded fingerprints

Screen 18 (`7d714253`) retains all 7,440 timings and 40 RSS observations. Large
contiguous point/full-read medians are +2.63%/+9.37%; nonconsecutive-row point/
full-read medians are +8.87%/+12.37%. Typical and many-key full reads improve
about 49%, and their writes improve 68–70%. This candidate remains unaccepted.
The small full-read aggregate is +26.06%; its four candidate process medians are
40.35, 20.69, 14.63 and 14.60 microseconds versus 24.13, 15.96, 13.79 and 14.08.
All startup observations remain included. More substantial final sampling and
explicit cold/warm conditions are required; later samples alone are not acceptance.
The actual workspace feature union for twox-hash 2.1.2 is `alloc`, `std`,
`xxhash32`, `xxhash3_64`, `xxhash64`.

Inspection of the pinned standard library confirms that `Take<File>` forwards
uninitialized-buffer reads, but its default `read_to_end` begins without the
known file-size hint. A finite local diagnostic compares this path, explicit
zero-initialization plus `read_exact`, and `fs::read` at four sizes, retaining
14,400 timings in `/private/tmp/logex-index-full-read-micro-1`. Every byte is
checked outside the timer. Zero-initialization is slower at every measured size;
at 64 KiB the medians are 10.67/11.29/10.63 microseconds respectively. The bounded
reader is retained, and the proposed read-strategy change is rejected.

The next candidate increases page size from 1 to 2 KiB. Prior checksum diagnostics
show substantial per-page metadata/seed work, so halving the number of pages
reduces this work and footer size without dropping any check. This trades larger
point-read/cache granularity for fewer fingerprints and a footer-cache span of
4 MiB. The 2 MiB bloom plus its logical header now fits in one footer window.
The independent persisted-byte oracle fixes the new geometry explicitly, and
all previous prototype versions are rejected. All 66 index unit tests and focused Clippy pass; logs and source hashes are
retained in `/private/tmp/logex-index-integrity-focused-7`. Equivalent release
measurements remain outstanding, so this is not yet accepted.


Screen 19 (`32aea52c`, 2 KiB pages) retains another 7,440 timings/40 RSS. Large
contiguous full-read median is +8.77%, and the nonconsecutive-row median remains
+11.20%; their p95 changes are +1.00%/+5.52%. Point medians are -0.57%/+6.14%.
The small full-read aggregate remains +21.84%, with substantial initial-process
tail variation retained. This experiment remains insufficient for acceptance.
The next source uses 4 KiB pages (container version 5), reducing per-page seed
work again and expanding footer-cache coverage to 8 MiB. Its larger point-read
and page-cache granularity remains an explicit performance tradeoff to measure.
All 66 index unit tests, formatting and focused Clippy pass for this source;
`/private/tmp/logex-index-integrity-focused-8` retains the parent revision, exact
diff and logs. No validation path or old-format fallback was removed in either
page-size experiment.


## First larger comparison and writer follow-up

The fixed protocol at `c794fda8` completes in
`/private/tmp/logex-index-integrity-final-1`: 66,000 measurement timings/100 RSS,
plus 3,720 separately labeled warm-up timings/20 RSS. Every original process and
sample remains retained. The small full-read median/p95 is +3.97%/+3.23%, while
typical/many-key full-read medians improve about52%. The large contiguous full
read is +9.86%/+8.94%, close to the budget and retained as a sensitivity limit.
The nonconsecutive-row full read is +11.86%/+11.90%, and its point-read p95 is
+15.48%. Small B-tree writes are +17.87% median/+26.98% p95. This candidate fails
acceptance. Many-key write p95 is +26.30% despite a large median gain and remains
subject to investigation. Typical write p95 improves 46.75%. The earlier prose
and archive-report limitation incorrectly grouped both tails as regressions;
this corrects that description without changing any raw measurement.

Source review identifies an avoidable flush in B-tree serialization: calling
`BufWriter::flush` forwards the flush through the container before its footer is
appended. The follow-up drains the serializer with `into_inner`, propagating
buffer-write errors and leaving the final page/footer/file flush to the container.
Pinned standard-library source confirms that `into_inner` uses `flush_buf`, not
the inner writer's `flush`. Existing container poisoning, length checks and final
error propagation remain in effect. This changes no persisted encoding or reader.
All 66 index unit tests, formatting and focused Clippy pass after draining the
serializer buffer; parent revision, exact diff and logs are retained in
`/private/tmp/logex-index-integrity-focused-9`. The remaining full-read regression
requires further investigation before another acceptance comparison.


## Dense bitmap decoding and retained evidence

Implementation `02898c44` uses the pinned library's public `from_lsb0_bytes`
constructor after complete structural preflight when every container is dense
and non-run. Each call receives exactly 8 KiB and a checked u16 container key;
even the highest key has an inclusive end within u32. Its returned cardinality
must match that container's descriptor, including when inconsistent descriptors
would preserve the overall total. Mixed and run encodings retain the ordinary
decoder. No format change or new project `unsafe` is involved.

For multiple containers, owned `MultiOps<Result<RoaringBitmap, io::Error>>::union`
moves their payloads. The pinned implementation collects at most 50 initial
items (10 when the upper bound exceeds 50), then appends the remaining ascending
keys with binary searches and amortized vector growth. It avoids the quadratic
repeated length calculation of successive owned `|=` operations. Normalization
uses cached cardinalities for these validated dense containers. Errors propagate
while partial results are dropped. A single container returns directly.

Two independent public-API diagnostics retain 27,600 timings. The second has
1/4/8/9/64-container configurations and compares ordinary decoding, the bounded
eight-container prototype, and the scalable owned-union path. The scalable path
improves their medians by about 33%/10%/20%/16%/22%, respectively. This is decoder
cost evidence only; index and ingestion performance acceptance remains required.
The discarded whole-file-fingerprint idea would add writer work and another
format, so it is not implemented.

All 71 index tests, formatting and focused Clippy pass. Independent generated-row
oracles cover nonconsecutive keys, container65535, cardinality boundaries and
1/4/8/9/49/50/51/64 containers. Finite inconsistent-count controls check both
initial and later iterator failures; mixed and sparse controls remain covered.
The tests were delegated as requested, reviewed and run locally, and committed
with the implementation. No SQL code was authored during this pass.

Screens17–20 are independently verified in
[`2026-09-13-index-integrity-fingerprint-investigation.json`](baselines/2026-09-13-index-integrity-fingerprint-investigation.json)
and its compressed raw archive (29,760 timings/160 RSS). The original human-readable
purpose field in plans19/20 was inadvertently copied from18; source/build hashes
were correct. Both immutable originals and an explicit description correction
are retained. The first larger comparison and following diagnostics are in
[`2026-09-13-index-integrity-expanded-comparison-1.json`](baselines/2026-09-13-index-integrity-expanded-comparison-1.json)
and its raw archive (69,720 timings/120 RSS). These remain failed/investigation
results, not acceptance. The next fixed comparison adds 9- and 32-container file
layouts to the existing five configurations, with the same separately retained
warm-up and measurement protocol.

### Expanded comparison 2 and bounded-buffer investigation

Exact source `f2885fa9` completed seven layouts, including nine and 32 dense
containers: 92,400 measured timings / 140 RSS observations and separately retained
5,208 warm-up timings / 28 RSS observations. It is not accepted. Small writes are
+12.72% median / +18.14% p95; eight-container full reads +10.51% / +18.98%; the
32-container point/full-read medians +12.31% / +10.65%. Typical and many-key full
reads improve about 51%, and their write medians improve 67–71%; those gains do
not cancel the failing cases. All raw results remain in the isolated comparison
output and will be packaged before this milestone concludes.

A finite paired small-write profile used only generated disposable files. Each
revision ran 20,000 writes and checked the exact reopened row set. Cargo builds,
fixture exits and sampling exits passed. Most samples were filesystem open/close
calls; allocation was a small share, so this does not prove allocation explains
the slowdown. Initial standalone linking failed and is retained as a diagnostic
failure, followed by separate successful Cargo-built profiles.

The next narrow candidate bounds the serializer buffer by logical file length
and the physical buffer by checked physical length, both capped at 64 KiB. The
checked size is capped before conversion to `usize`. Existing roundtrip,
page-boundary and write-error checks remain unchanged; all 71 index unit tests,
formatting and focused Clippy pass. This is a performance hypothesis until the
fixed comparison against the preceding candidate completes.

### Eight KiB page experiment

Source `211cd7ad` uses version 6 with 8 KiB pages. The independent persisted
framing oracle uses the exact new constants; all previous prototype versions,
including version 5 with a matching recomputed header CRC, remain rejected.
Existing cache-window, page-boundary, partial flush, error poisoning, exact-length
and complete-file tests remain enabled. All 71 unit tests, formatting and focused
Clippy pass. Larger pages halve the number of fingerprints for large files but
increase point-read verification and page-cache size; the seven-layout original-
baseline comparison must measure that tradeoff before acceptance.

The bounded-buffer comparison does not establish a small-write speed gain:
median -1.30%, p95 +6.24% against `f2885fa9`. It reduces actual transient buffer
capacity for files below 64 KiB; no speed claim is made. Above-capacity gapped
writes have unchanged capacities but vary +9.11% median / -11.68% p95, showing
material host/process variability. A separate 4,800-iteration rotating I/O
control retains every open/write/close timing. Single writes are faster than
splitting the same bytes at 8 KiB, and the two generated file sizes have similar
costs. This control excludes entropy and serialization and cannot replace the
application comparison.

### Expanded comparison 3: rejected larger pages

The seven-layout comparison at `984e65af` retains all 97,608 timings / 168 RSS
observations. Version 6 / 8 KiB pages are rejected: gapped full reads remain
+10.54% median / +10.49% p95; 32-container point median is +13.89%; bloom absent
medians range from +11.16% to +16.80%, with several larger tail regressions.
Small writes (-13.65% / -29.16%), typical/many-key full reads (about -51%) and
write gains do not offset failed layouts. The implementation returns to version
5 / 4 KiB pages and explicitly tests rejection of the discarded version 6.

A finite 288-timing checksum diagnostic compared current seeded XXH3 with nested
XXH3 and streaming XXH3 over the concatenated metadata and page. All independently
assembled composition controls passed using the same pinned implementation.
Nested hashing improves only about 3.7% at 4 KiB and 1.6–2.4% at 8 KiB and changes
collision composition; streaming is slower. Neither alternative is adopted.
The full comparison, diagnostic source, controls and raw results are retained in
[expanded comparison 3](baselines/2026-09-13-index-integrity-expanded-comparison-3.json).

A separate initialized-read API diagnostic retains 240 batch timings across
4/32/64/256 KiB and 1 MiB generated files. Every result is checked after its timer;
each batch retains 8 MiB of outputs until the oracle and destruction. The pinned
nightly borrowed-buffer API reduces median API cost by about 14.6% at 32 KiB and
3.2–3.8% at 64/256 KiB. This is an API/allocator diagnostic, not application
acceptance. A subsequent production comparison and initialization-contract review
are recorded below.

The next bounded point-read candidate uses an unverified prefix solely to choose
whole-file verification for a matching single-entry v2 file larger than one page
and at most 1 MiB. Width, key, count and canonical descriptor extent must match the
hint. The same opened handle then verifies the whole body/footer and fully checks
B-tree and bitmap structure before a bitmap is moved out. Absent, wrong-width,
large and multi-key lookups retain the ordinary path. This can save a separate
footer read and partial-page copy, but also rereads the prefetched page; all 75 index tests, formatting and focused Clippy pass at `e3b809e4`.
An isolated comparison against `3d723cb9` (same 4 KiB geometry and bounded write
buffers) retains 51,072 timings / 96 RSS observations. Present-point medians
improve 3.92% / 4.00% / 6.08% for four/eight/32 dense containers; p95 improves
2.89% / 11.90% / 3.03%. The unchanged small-point median is equal, with p95 +0.36%.
Absent-point and full-read controls remain retained, including 32-container
full-read p95 +7.97%. This is isolated evidence, not original-baseline acceptance.

### Initialized full reads

Implementation `7c99c9f4` replaces the path-based full reader's adaptive
`Take::read_to_end` with a fallibly allocated exact spare-capacity read using the
already pinned nightly's `BorrowedBuf` and `read_buf_exact` APIs. It uses one
opened file's metadata and constrains the writable spare slice to that extent.
An explicit filled-length check is mandatory even after success: a safe custom
reader could otherwise report success without filling the buffer. The vector's
length becomes visible only after that check; every error drops a zero-length
vector. The consuming same-handle fallback still uses its existing zero-filled
read. Header, extent, page, footer and structural checks are unchanged.

The single new `unsafe` operation is documented at its call site. Review against
nightly-2026-08-24 verifies `core/src/io/borrowed_buf.rs` (filled-length tracking,
checked advance and append), `alloc/src/io/read.rs` (exact-read retry and EOF
behavior), and the Unix File/read-buffer forwarding path. Safe cursor operations
cannot claim initialized bytes without initializing them; operations which can
bypass that property require their own unsafe contract. These unstable APIs must
be revalidated when changing the pinned toolchain. No toolchain upgrade or new
dependency is introduced.

All 79 index tests, formatting and focused Clippy pass. Finite controls cover
empty/exact/short input, bytes beyond the requested extent, interruption, partial
I/O failure, and a reader that reports success without filling. The isolated
comparison against `8ddb9b8c` retains 63,840 timings / 120 RSS observations. Full-
read medians improve 2.94% / 2.83% / 2.47% / 2.54% / 3.23% for small/four/eight/
nine/32-container layouts; p95 improves 3.76% / 0.25% / 3.63% / 3.78% / 7.29%.
Unchanged point medians range from -0.41% to +1.81%; 32-container point p95 +5.67%
remains in the evidence.

Both isolated comparisons, focused validation and the initialized-read API
diagnostic are independently verified in
[read comparison evidence](baselines/2026-09-13-index-integrity-read-comparisons.json)
and its compressed raw archive (114,912 application timings / 216 RSS).
They use the predefined two warm-up and ten measured balanced process pairs,
with 100 measured reads but only one incidental write/build sample per process.
They therefore do not establish write-tail acceptance. The seven-layout original-
baseline comparison retains 30 write/build samples per measured process. That
comparison, mixed ingestion/query workloads, final workspace gates and CI remain
required before accepting or merging this milestone.


### Expanded comparison 4 and remaining attribution

Exact source `9269254c` completes the original seven-layout protocol, retaining
97,608 timings / 168 RSS observations in
[expanded comparison 4](baselines/2026-09-13-index-integrity-expanded-comparison-4.json).
Every generated-row result check passes. Typical/many-key full reads improve
51.37%/51.92% at the median and 49.49%/50.46% at p95; their write medians improve
67.30%/71.22%. Small/four/eight/nine-container full-read medians are
-1.74%/+3.03%/+6.76%/+7.15%. Bloom absent-open medians range from +3.73% to +8.16%;
these are retained costs of checking the selected page.

This run does not establish acceptance. Small writes are +7.42% median/+10.60%
p95, eight-container writes +27.38%/+8.88%, and nine-container writes
+10.94%/-1.57%. Thirty-two-container point reads are +9.12%/+20.33%, absent reads
+5.12%/+20.87%, and full reads +9.64%/+14.63%. Similar point/absent tail movement
and the large variation of unchanged write code across prior runs are reasons
to investigate, not grounds to discard these values.

For example, small-write per-process median changes range from -26.14% to
+56.43% in the ten balanced pairs; the pooled median is +7.42%. The recorded APFS
filesystem has roughly 33 GiB available and reports 97% capacity. Neither the
benchmark nor the API diagnostic controls OS writeback or cache eviction. These
conditions constrain attribution; no causal claim is made from them alone.

The next predefined write-only comparison uses identical copied bounded fixtures
and the same public writer API, with generated-row bitmap equality after every
timed write. It isolates writer timing from bloom construction and query work;
its oracle can still warm filesystem and allocator state. The unchanged mixed
storage/index/query/publication fixtures are also required to measure real sync
impact. All earlier samples and candidates remain retained.


### Mixed workloads and fixed confirmations

The unchanged mixed fixtures at `9269254c` retain 10,000 timings / 100 RSS in
[mixed comparison 2](baselines/2026-09-13-index-integrity-mixed-2-release.json).
Both dense and sparse query-engine result workloads stay near the baseline
(median changes -0.38% to +0.28%, largest p95 +2.14%). Integrated index build
medians improve 7.45% dense / 13.58% sparse. Live/historical publication medians
change +0.03% / +1.33%, p95 -3.99% / +0.67%. Sparse ingestion/query/compaction/
reopen checks stay within 5% at median and p95, with faster concurrent-query p95.
All exact result and reopen checks pass.

Dense ingestion still needs attribution: live p95 +9.21%, historical p95
+116.41% (57.874 to 125.243 ms), despite median changes +0.18% / -1.36%.
Seven large historical samples occur in candidate process pairs 2, 6 and 7;
these processes also contain live-write spikes. They remain in the evidence.
The predefined follow-up keeps every original sample and uses twenty more
balanced source pairs plus ten identical-candidate-binary control pairs. The
control labels are scheduling groups, never different source revisions.

The [write-only confirmation](baselines/2026-09-13-index-integrity-write-confirmation-1.json)
retains 10,600 timings / 120 RSS, with exact bitmap equality after every timed
write. Small/four/eight/nine/32-container write median changes are
-9.54%/-9.20%/-0.26%/+5.04%/+0.00%; p95 changes are
-6.62%/-7.00%/-12.34%/+17.06%/+6.67%. It does not reproduce the larger small/eight/
nine median increases, but the nine-container tail remains a recorded observation.
Reopening each result warms state differently from the original surrounding
workload; these measurements are not pooled with the original workload or used
to erase its results. No speculative writer rewrite is adopted from these tails.

The [read-tail confirmation](baselines/2026-09-13-index-integrity-read-tail-1.json)
retains 25,536 timings / 48 RSS, including all incidental write/build samples.
For 32 containers, point/absent/full-read median changes are +6.75%/+2.73%/+5.97%
and p95 changes -1.48%/-11.26%/-6.74%. The four-container control has point median
-2.65%, full median +3.38%, with corresponding p95 -3.10%/+1.59%. This does not
repeat the original read-tail increases. The 32-container point/full median
costs remain explicit, including the original +9.12%/+9.64% values. One incidental
write sample per process cannot establish writer-tail acceptance.


The [dense confirmation and identical-binary control](baselines/2026-09-13-index-integrity-dense-tail-1.json)
are complete, retaining 5,400 timings / 60 RSS. Confirmation historical median/
p95 changes are -1.28%/+0.24%; live changes -0.36%/+3.10%. Combining all original
and confirmation source samples gives historical -1.30%/+3.20% and live
-0.17%/+9.10%. The identical-binary controls remain separate; their live and
historical median/p95 scheduling-group differences are -0.45%/-2.09% and
-0.33%/-3.08%. These controls are stable and do not identify the cause of the
initial spikes. The initial seven large historical observations remain included
in the combined results, not removed as outliers.

The final source comparisons retain 149,144 timings / 496 RSS across expanded
comparison4, mixed2, write-only, read-tail and dense-tail runs. The fixed
confirmations do not establish a repeatable source-induced regression above10%.
Retained costs and limits remain explicit: 32-container full/point medians near
6–10%, bloom absence medians up to8.16%, combined dense live p95+9.10%, and
non-repeated direct write/read tail increases. The different surrounding
workloads are reported separately. This supports proceeding to correctness and
CI gates for this scoped milestone; it does not prove every individual tail is
within10%, explain every filesystem delay, or establish live-sync/release
readiness. Broader mixed/staging performance work must retain these observations.

The final static production review finds no new concrete correctness concern in
the combined framing, full/point reads, writer finalization and builder/checkpoint
paths. This is a bounded review, with runtime validation supplied separately.


### Final local validation

All nine commands pass at `b6076c00718c9e8f4d90e132e3c93f7624782aff`:
vendor verification; formatting; locked workspace/all-target check, Clippy and
tests; locked workspace documentation tests; locked release node build; release
query tests; and release protocol-consistency tests. Workspace totals are
1,096 passed / 0 failed / 21 ignored; release query totals139 / 0 / 7; both
protocol checks pass. Ignored opt-in benchmarks were executed separately in the
recorded comparisons. Documentation tests contain no executable examples.

The validation archive independently checks the exact Git source tree, every
command/exit/log hash, test totals and compressed-evidence roundtrip. Subsequent
validation-document changes do not alter those production/configuration sources.
Existing dependency future-compatibility warnings remain recorded in the audit
ledger. CI on the PR head and merge remain required; the broader audit is open.
