# Derived index file integrity

This batch is in progress on `audit/index-file-integrity`, based on PR #147
merge `ae4c01c913f89e8448a2c98d5905733525c51047`. No performance acceptance or
merge is claimed yet. It changes derived indexes, not log columns or ingestion
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

All nine local gates passed at exact head `3b0a52b7`, including 65 index unit
tests, the required workspace checks, release query tests and both release
protocol consistency tests. Logs and source hashes are retained in
`/private/tmp/logex-index-integrity-gates-1`. The subsequent timing correction
affects only the opt-in benchmark, so its compilation and focused execution
still require separate validation.

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
integrity container. Its 48-byte header fixes magic, version, 2048-byte page size,
logical length, a per-file 128-bit random identity and reserved fields, protected by CRC32. The original logical
bytes remain contiguous. The current experiment uses container version 4 and
one eight-byte metadata-seeded XXH3 fingerprint per page. Screens 1–16 used
version 1 and four-byte CRC32 page checks; screen 17 used version 2 and XXH64.
Each page fingerprint
includes a domain tag, format version, logical length, file identity, page position and actual
page length. Version 3 hashes that complete metadata into a seed and uses the
standard seeded hash on the page bytes. Version 4 retains this construction with
2 KiB pages and its own version domain. This is a distinct encoding from hashing
the concatenated metadata and page. Exact physical extent is checked using the
opened handle.

Point lookups validate only bytes from pages they inspect, keeping logarithmic
table search. Bloom exclusions verify the page containing the tested bit. Range
readers retain contiguous whole-file reads and validate every page. A reader
holds a 2 KiB data-page cache and a checksum cache capped at 16 KiB (smaller files
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
Only one bitmap deserialization is performed.

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
