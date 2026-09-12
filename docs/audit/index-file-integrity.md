# Derived index file integrity

This batch is in progress on `audit/index-file-integrity`, based on PR #147
merge `ae4c01c913f89e8448a2c98d5905733525c51047`. No performance acceptance or
merge is claimed yet. It changes derived indexes, not log columns or ingestion
transactions. Whole-file source identity and the rest of batch 6 remain open.

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
bytes remain contiguous, followed by one CRC32 per page. Each page checksum
includes a domain tag, format version, logical length, file identity, page position and actual
page length. Exact physical extent is checked using the opened handle.

Point lookups validate only bytes from pages they inspect, keeping logarithmic
table search. Bloom exclusions verify the page containing the tested bit. Range
readers retain contiguous whole-file reads and validate every page. A reader
holds bounded page/checksum caches of bytes; no global validation cache or
per-query full-file scrub is introduced. Writer checksum storage costs four
bytes per logical page, and B-tree payloads stream once instead of retaining all
serialized payload buffers. Integrity overhead on disk is 48 bytes plus four
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

CRC detects accidental persisted-byte changes; it does not authenticate data,
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
the focused total to 60 passing tests; focused Clippy passes. Its separate fixed
comparison is pending.
