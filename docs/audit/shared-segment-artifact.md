# Shared segment artifact prototype

Status: integrated, unaccepted candidate within unmerged PR #130. The user permits a fresh
sync and breaking storage changes to meet the ingestion performance constraint.
Existing production directories and the protected external volume remain outside
this work. The equal-bytes probe in `sync-ingestion-checkpoints.md` supports
investigation, not acceptance of the new format.

## Scope

Reuse the current page encoders, logical column schema, catalog checkpoint model
and segment coalescing. Store compressed column payloads, page indexes and null
bitmaps and canonical flags in one appendable artifact. Reorgs append a new
canonical view and commit its reference through the catalog. Raw live columns
remain supported. This removes
many file creations/replacements without weakening the durable checkpoint.

## Publication and recovery invariants

- Each appended compressed page occupies one or more bounded immutable
  extents with checked offset, length and CRC32. A bounded, checksummed table maps the fixed set of logical
  column artifacts onto those extents; it cannot name arbitrary filesystem paths.
- A manifest references one immutable table by offset, length and checksum.
  Readers use that captured reference, never whichever table happens to be last
  in the file. The existing per-column page indexes retain logical row/byte
  positions; the artifact reader resolves those to physical extents.
- The catalog records the table reference for each active bundled segment along
  with its committed row count. Unfinished appends cannot change that reference.
  Each durable table and every extent it references must precede catalog
  publication under the existing per-device ordering rules.
- Startup validates the catalog-pinned table and committed extents before any
  mutation. Reconstruct the derived manifest from the committed reference before trimming only
  the uncommitted file suffix. Interrupted rollback must repeat safely. Missing
  or corrupt committed bytes remain an explicit repair error.
- Bound table size, extent counts, allocations and checked arithmetic. Rotate
  before append metadata exceeds its configured bound; do not stall forever on
  a valid tiny batch. Measure resulting segment counts and disk/write overhead.
- Bump the incompatible catalog/segment versions and reject older directories
  unchanged. A new directory is the default migration; original directories
  remain available for rollback. No automatic production deletion is implied.

## Implementation and acceptance sequence

1. Implement the bounded artifact reader/writer with malformed-input, checksum,
   extent-overlap, snapshot-prefix and interrupted-append tests.
2. Connect it to historical page encoding and SegmentReader, preserving the
   per-column path for raw compaction/profile handling where still needed.
   Make manifest and catalog references explicit in typed publication results.
3. Adapt the crash matrices to assert data rather than obsolete physical paths.
   Exercise partial payload/table/manifest/catalog publication, canonical flags,
   repeated rollback/retry, cross-device storage and generic WAL replay.
4. Repeat short, large, sustained and sparse original-baseline workloads with
   finalization/checkpoints, exact row/progress/reopen oracles, peak memory and
   logical/physical disk footprint. Measure query effects and stale-table growth.
5. Retain the format only if representative ingestion meets the user's 10%
   ceiling and correctness/platform gates pass. PR #130 remains draft meanwhile.

The prototype file used by the isolated layout probe is not this storage format
and must not be treated as an implementation or migration artifact.


## Inline-index milestone (`5df13474`)

`bundle.rs` is now connected to historical ingestion, SegmentReader and catalog
recovery. Catalog v5 (`LXCAT005`) and segment v3 intentionally reject older
formats without rewriting them. New historical segments use
`columns/segment.bundle`; raw live columns and their existing compaction path
remain supported. This candidate is not accepted for deployment.

The candidate uses `LXBND002` file magic and immutable `LXBT0002` tables. A
reference stores row count, table offset, table length, CRC32, ancestor depth and
total table-chain bytes. Tables encode numeric stream IDs, logical lengths and
physical offset/length/CRC32 extents. IDs 0–13 hold append-only column payloads;
IDs 14–27 hold append-only page-index entries inline in the checksummed tables,
without a repeated header or separate fragments. The reader reconstructs the
existing logical index header from the captured count.
IDs 28–31 hold replaceable null bitmaps, tagged as plain or LZ4-compressed bytes.
Compression is used only when smaller, and decoding allocates from the captured
row count, never a size supplied by compressed input. Bundled row count is bounded
by 4,096 fixed-column pages × 16,384 rows (about 8 MiB per decoded bitmap).

Most appends write only changed stream extents. A delta records its parent
reference; after at most 32 tables, or before 4 MiB of cumulative table reads, the
writer emits a complete table. This bounds startup/query traversal while reducing
repeated metadata writes. It does not reclaim older full tables. No table field
supplies a filesystem path. Individual extents are at most 1 MiB and each stream
at most 4,096 extents. Counts, reserved fields, logical lengths, offsets, physical
ordering, overlap and trailing bytes are checked before use. Parent snapshots must precede
their children physically, with nondecreasing rows and strictly increasing depth
and cumulative read budget. Delta
extents must follow their parent's complete snapshot. CRC32 detects accidental
corruption; it is not authentication.

Readers retain a file handle and the table they opened. Selected ranges verify
each intersecting fragment before returning bytes. Writers append fragments and
then the immutable table, and return a reference after userspace buffers flush.
**The caller must still order and persist the file before catalog publication.**
An I/O failure poisons that writer; old references remain readable. The append
constructor rejects an unpublished tail until storage recovery trims it.

Regression coverage includes: exact snapshots across append
and metadata replacement, bounded-fragment selections and concurrent writers,
every truncation/one-bit mutation of a small artifact, valid-checksum malformed
tables, interrupted fragment/table writes and retry, four deterministic seeds of
mixed operations compared with an independent model, reordered-fragment rejection,
open-file lifetime after replacement, and capacity/invalid-operation behavior.

The catalog and manifest hold the same typed `BundleReference`, pinned alongside
their row count. The fixed logical schema resolves 14 data streams, 14 page indexes
and four null bitmaps; incomplete/duplicated schemas, unknown paths and incompatible
codecs fail explicitly. No per-column bundle sidecars are needed. Encoders run on
the existing scoped workers; the shared writer serializes only compressed writes.
The canonical bitmap remains separate for existing reorg behavior.

Startup verifies the catalog-pinned table, every committed extent checksum, all
page-index prefixes and bitmap lengths before changing that segment. An actual
rollback durably restores the canonical prefix and manifest before truncating the
uncommitted suffix. Healthy reopen preserves the artifact byte for byte. CRC does
not authenticate chain data, and startup verification cost must be measured.

Before an append, fixed-column page counts and zstd's worst-case variable-data
bound reserve fragment capacity. A full artifact is finalized and a new segment
allocated; the previous target-row and block-span coalescing limits still apply.
Old tables and replaced metadata remain inside committed artifacts, so physical
size and write amplification remain acceptance measurements.

Integration regressions cover old readers across page boundaries and metadata
updates, capacity-driven rotation, committed corruption preserved before rollback,
seven interrupted payload/table/manifest/catalog phases, exact surviving committed
bytes, repeated reopen/retry and both per-column/bundled malformed append metadata.
The existing commit and rollback failure matrices also exercise bundled segments.
Whole-query snapshot lifetime and reorg isolation remain separate batch-7 work.

Still required for acceptance: complete workspace/platform gates, original-baseline
ingestion comparisons, query/startup effects, physical disk/write-amplification
measurements and isolated ExFAT/cross-device validation. The 10% performance ceiling
is unchanged, and PR #130 remains draft.


## Metadata growth finding (2026-09-11, `0223055d`)

The diagnostic confirms excessive stale metadata, not just a theoretical bound.
Every case remains one bundle and passes exact rows after finalization/reopen:

| Chunk rows × calls | Rows | Artifact bytes | Current reachable bytes | Stale bytes |
| --- | ---: | ---: | ---: | ---: |
| 1 × 1,024 | 1,024 | 295,440,392 | 814,600 | 294,625,792 |
| 120 × 128 | 15,360 | 5,386,120 | 216,712 | 5,169,408 |
| 15,360 × 64 | 983,040 | 24,534,152 | 7,857,800 | 16,676,352 |

[Probe source and records](baselines/2026-09-11-bundle-metadata-growth.jsonl).
Allocated file blocks were also recorded; these are not physical-device write
counters. The one-row case grows from 1.25 MB at 64 calls to 4.79 MB at 128,
18.76 MB at 256, 74.25 MB at 512, and 295.44 MB at 1,024. The current immutable
full-table/full-index snapshot format therefore cannot be accepted as-is.

The next prototype now keeps immutable committed references but appends only
new page-index entries, publishes bounded table deltas with periodic full tables,
and compresses nullable bitmaps with an explicit decoded-size bound. Retain one
artifact and the current coalescing behavior. Bound ancestor depth and total
metadata bytes before allocation, validate parent ordering and stream prefixes,
and flatten before the bound. A successor incompatible format must reject older
directories unchanged. These changes are implemented locally; performance and platform acceptance remain open.

Validate this against the existing malformed-input and mixed-snapshot model tests,
add ancestor/flattening/checksum/crash regressions, repeat growth/query/startup and
original-ingestion comparisons, then platform checks. Avoid solving metadata
growth by proliferating tiny segments without measuring query/index consequences.

A new regression reproduces the growth failure before the change using 1,024
small appends and exact stream reads. Added tests exercise table flattening,
reopening old snapshots, corrupt ancestors, valid-checksum invalid parent/delta
bounds, and malformed/oversized compressed bitmaps. The mixed-operation model
now crosses two flattening boundaries. Final validation results follow below.


## Incremental metadata and inline indexes (2026-09-11)

The first incremental prototype reduced the 1,024-row artifact from 295.44 MB to
8.86 MB, but one separate fragment per index append caused excessive small reads:
ingestion took 9,005.58 ms and reopen 31.46 ms in the diagnostic, versus 4,124.96 ms
and 12.51 ms in the previous diagnostic. It is not retained in that form.
[Intermediate growth evidence](baselines/2026-09-11-incremental-metadata-growth.jsonl)
records its exact source. A separate original-baseline comparison still failed
one tiny historical batch (+31.68%); other tested profiles were faster or within
10%. [Original comparison](baselines/2026-09-11-incremental-original-comparison.jsonl).

The current refinement keeps index bytes inside the bounded, checksummed table
chain. They are available after table validation and require no per-append fragment
reads. Raw index data is bounded to 4,096 × 24 bytes per column before allocation
or concatenation. Malformed flags/counts and cumulative chain overflow have
explicit tests. Existing payload extents and nullable bitmap checksums remain.

| Chunk rows × calls | Current artifact bytes | Reachable bytes | Stale bytes |
| --- | ---: | ---: | ---: |
| 1 × 1,024 | 10,411,036 | 833,320 | 9,577,716 |
| 120 × 128 | 406,128 | 228,292 | 177,836 |
| 15,360 × 64 | 7,490,422 | 7,387,441 | 102,981 |

All three retain one segment and exact post-reopen rows. These are 96.48%, 92.46%
and 69.47% smaller than the initial full-metadata prototype. Periodic full tables
still retain stale metadata; this is bounded by segment capacity, not a claim of
linear file growth or complete space reclamation. The diagnostic takes 4,444.86 /
260.22 / 275.53 ms for ingestion and 12.03 / 2.44 / 3.51 ms for reopen. Single-run
timings are diagnostic only; repeated original-baseline acceptance remains open.
Reachable bytes include the complete table chain and do not double-count inline
index bytes. File allocation is not a physical-device write counter.

[Inline-index probe and records](baselines/2026-09-11-inline-index-growth.jsonl),
[growth regression before the fix](baselines/2026-09-11-incremental-metadata-before.log).
All six local workspace gates pass: formatting, locked workspace/all-target
check and strict Clippy, 871 tests/eight ignored, doctests and release node build.
A stale format-number diagnostic was corrected during validation; formatting,
check, strict Clippy and catalog tests pass again after that correction.
[Validation records](baselines/2026-09-11-inline-index-gates.jsonl). Current platform
and final performance acceptance remain pending.


## Current candidate: immutable canonical metadata

A [benchmark-only diagnostic](baselines/2026-09-11-immutable-canonical-diagnostic.jsonl)
estimated the cost removable by eliminating per-append mutable metadata ordering.
It temporarily deferred bitmap/manifest publication and flushed all affected
bundle trees at checkpoint. **That temporary rule is not crash safe in the present
format**; it was compiled only into the isolated fixture and restored before use.
No node executable or production data used the rule. Five alternating pairs ×
three release samples passed exact clean-reopen oracles: one-row history was
2,179.70 → 989.07 ms (-54.62%), and 128-call history 348.01 → 121.04 ms (-65.22%).
These are feasibility measurements, not acceptance or claims about a real format.

The chosen follow-up, using the user's fresh-sync/breaking-format authorization,
is to store canonical bits as another checksummed immutable bundle stream. The
catalog then pins both rows and canonical state; an unfinished epoch can append
without overwriting either committed prefix. Bundled manifests become derived
metadata reconstructible from the trusted catalog and fixed format schema.
Checkpoint must flush every affected bundle tree before catalog publication,
including a previously committed active historical segment. Reorg changes must
append a new bitmap/table and publish its catalog reference durably; older bundle
snapshots retain their original canonical view. Raw/per-column representations
retain their separate bitmap and existing publication protocol.

The format change is implemented locally and remains in validation. Version
rejection, canonical CRC/length bounds, interruption, reordered/torn metadata,
non-canonical preservation, reorg publication, captured readers, generic WAL
replay and cross-device ordering are covered by regression tests. It uses catalog v6
(`LXCAT006`), segment manifest v4, bundle magic `LXBND003` and table magic
`LXBT0003`. Stream 32 holds canonical bits; streams 0–31 retain their prior roles.
The obsolete grouped canonical-file publication machinery is removed. Current full workspace validation passes; performance acceptance and current
Linux/macOS CI remain required before merge.


The actual implementation's [many-appends comparison](baselines/2026-09-11-immutable-canonical-original-comparison.jsonl)
uses five alternating pairs × three release samples with all exact oracles passing.
One-row history improves 2,156.22 → 803.80 ms (-62.72%); 128-call history improves
349.36 → 94.92 ms (-72.83%). These replace the diagnostic with recovery-capable
implementation evidence for these two workloads only. The broader profiles below include the remaining acceptance failure.


[Broader original comparison](baselines/2026-09-11-immutable-canonical-broad-original-comparison.jsonl):
five alternating pairs × three release samples, exact oracles passing, final
checkpoint/finalization included. Few logs remain 6.039792 → 7.067042 ms
(+17.01%, fails); tiny chunks -60.41%, sparse history -44.80%, short history
-35.62%, grouped live -81.98%, sustained history -77.58%, large history +6.32%,
and per-block live checkpoints -21.38%. These storage timings do not establish
peer throughput. The tiny finalized case still blocks acceptance; the repeatable
large-history cost also exceeds the 5% investigation threshold. No merge.


The initial format's isolated ExFAT suite passed 167 tests/three ignored and all
32 cross-mount combinations; the image was detached afterward. Final ExFAT
validation for the later guard and bitmap fixes is running. The guard prevents
a valid stale manifest from seeding a second canonical update and restoring flags
that an earlier reorg had cleared. The new regression fails with that guard removed
and passes with it restored; reopen reconstructs the manifest from the current
catalog before retry. Temporary benchmark/growth instrumentation is removed.


Unused bitmap padding could become a present row after `push(false)`, either from
decoded padding or a preceding out-of-range `set`. Both regression tests fail
[before the fix](baselines/2026-09-11-bitmap-padding-before.log) and pass
[after it](baselines/2026-09-11-bitmap-padding-after.log). Decoding now clears only
bits outside the declared row count and checks address/length conversions before
allocation. Out-of-range updates are ignored; declared values and the fast append
path remain unchanged.

All six [local gates](baselines/2026-09-11-immutable-canonical-gates.jsonl) pass
for the final guard and padding changes: 878 tests/eight ignored, formatting,
locked all-target check, strict Clippy, doctests and release build.
The [growth probe](baselines/2026-09-11-immutable-canonical-growth.jsonl) retains
exact row/reopen oracles and records reachable/stale artifact bytes. Its single-run
timings are diagnostic, and allocated bytes do not measure physical writes.
