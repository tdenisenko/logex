# Shared segment artifact prototype

Status: design/prototype work within unmerged PR #130. The user permits a fresh
sync and breaking storage changes to meet the ingestion performance constraint.
Existing production directories and the protected external volume remain outside
this work. The equal-bytes probe in `sync-ingestion-checkpoints.md` supports
investigation, not acceptance of the new format.

## Scope

Reuse the current page encoders, logical column schema, catalog checkpoint model
and segment coalescing. Store compressed column payloads, page indexes and null
bitmaps in one appendable artifact. Keep the canonical bitmap separate because
reorgs change its existing bits. Raw live columns remain supported. This removes
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
  mutation. Restore the committed manifest/canonical prefix before trimming only
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


## Current low-level implementation

`bundle.rs` is currently compiled only for tests. It is not used for ingestion,
querying or startup, and the node's format versions have not changed yet.

The prototype uses `LXBND001` file magic and immutable `LXBT0001` tables. A
reference stores row count, table offset, table length and CRC32. Tables encode
numeric stream IDs, logical lengths and physical offset/length/CRC32 extents.
IDs 0–13 are append-only data streams; IDs 14–31 are replaceable index/nullable
metadata streams. No table field supplies a filesystem path. Individual extents
are at most 1 MiB, each stream at most 4,096 extents, and table reads at most
4 MiB. Counts, reserved fields, logical lengths, offsets, physical ordering,
overlap and trailing bytes are checked before use. CRC32 detects accidental
corruption; it is not authentication.

Readers retain a file handle and the table they opened. Selected ranges verify
each intersecting fragment before returning bytes. Writers append fragments and
then the immutable table, and return a reference after userspace buffers flush.
**The caller must still order and persist the file before catalog publication.**
An I/O failure poisons that writer; old references remain readable. The append
constructor rejects an unpublished tail until storage recovery trims it.

Nine focused tests and strict storage Clippy pass: exact snapshots across append
and metadata replacement, bounded-fragment selections and concurrent writers,
every truncation/one-bit mutation of a small artifact, valid-checksum malformed
tables, interrupted fragment/table writes and retry, four deterministic seeds of
mixed operations compared with an independent model, reordered-fragment rejection,
open-file lifetime after replacement, and capacity/invalid-operation behavior.

Still required before activation: typed manifest/catalog references and version
rejection, complete schema checks for all expected column streams, proactive
rotation before the fragment bound, storage-level rollback publication, all
existing ingestion/query/compaction paths, physical disk/write-amplification
measurements, and original-baseline performance/platform acceptance. Low-level
snapshot tests do not establish whole-query snapshot isolation.
