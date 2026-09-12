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
integrity container. Its 32-byte header fixes magic, version, 4096-byte page size,
logical length and reserved fields, protected by CRC32. The original logical
bytes remain contiguous, followed by one CRC32 per page. Each page checksum
includes a domain tag, format version, logical length, page position and actual
page length. Exact physical extent is checked using the opened handle.

Point lookups validate only bytes from pages they inspect, keeping logarithmic
table search. Bloom exclusions verify the page containing the tested bit. Range
readers retain contiguous whole-file reads and validate every page. A reader
holds bounded page/checksum caches of bytes; no global validation cache or
per-query full-file scrub is introduced. Writer checksum storage costs four
bytes per logical page, and B-tree payloads stream once instead of retaining all
serialized payload buffers. Integrity overhead on disk is 32 bytes plus four
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
