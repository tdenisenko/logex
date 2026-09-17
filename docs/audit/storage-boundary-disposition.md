# Storage boundary disposition

This pass starts from PR #224 merge `2b86b8a6` on
`audit/storage-boundary-disposition`. It completes the remaining structural
page/compression/bundle, raw-reader, WAL/journal and catalog review. Source `a7eae4b9` is committed after focused checks and independent review.
All ten final-source gates pass; exact-head CI and merge remain. Shared query memory policy and automatic
verified offline repair remain separate work.

## Plain scalar allocation ordering

**B2-28 — moderate: plain u32/u8 pages copy invalid input before checking its
exact length.** This is the remaining scalar counterpart of B2-24 in
[storage allocation review](storage-resource-bounds.md). Retained unbundled page
readers accept the plain codec. A physically present payload can pass the page
index's extent checks yet disagree with its expected scalar row count. Both
decoders previously copied that payload before reporting `InvalidData`.

A finite test constructs a 64 KiB input before enabling a thread-local allocation
counter, then decodes it as a one-row plain u32/u8 page. The original code returns
the expected errors but makes one >=64 KiB allocation per call; the same check
after the correction records zero for each. No exhaustion test or latency
benchmark is involved. Original source, additive test-only instrumentation,
before/after output and the fixed instrumented source are retained as evidence.
The allocator wrapper is removed from the final test binary; ordinary permanent
shape/codec controls cover empty, one-row and maximum-page inputs, overflowed
u32 row counts, and shorter/longer payloads. These ordinary error controls are
not represented as distinguishing the old allocation order.

Plain u32 decoding now validates and collects directly from the borrowed bytes,
removing the intermediate byte copy for valid pages too. Plain u8 uses the
existing exact-length fixed-width validator before copying. Successful values,
codecs, format and limits remain unchanged. The plain-u8 length error now uses
the existing helper's wording; its `InvalidData` kind is unchanged. The current bundled profile uses
compressed u32/source columns, so this is not a demonstrated normal bundled
query failure. There was no wrong-result or process-failure reproduction.

## Remaining parser and recovery disposition

The review found no further demonstrated defect in the following scoped paths.
Keep the useful current implementations.

- Fixed and variable decoded shapes use checked arithmetic and bounded
  decompression; offset tables require zero origin, monotonic bounds, exact
  terminal length and agreement with per-row lengths. Packed widths/padding,
  dictionary entries, bitmap framing and page-index geometry are validated
  before dependent slicing or output allocation.
- Bundle references constrain chain depth, decoded table bytes, extent counts,
  stream geometry and physical ranges. Captured reads retain their snapshot
  boundary and verify selected extents. Full verification explicitly rereads
  payloads; a cached read is not a fresh whole-file integrity scrub.
- Raw fixed/variable readers validate same-file physical layout. Selected-prefix
  APIs deliberately permit an incomplete unrelated append tail, while complete
  reads require exact length. Current compaction and append still use these
  validators; full-row assembly already delegates to captured `SegmentReader`.
  The corrected legacy recovery ownership spans capture through publication.
- A shorter unbundled append guard was investigated and left unchanged. Actual
  detached maintenance capture excludes active historical sources and pending
  transactions; finalized segment IDs are never reactivated. Native catalog
  mutation is serialized by mutable storage ownership and the data-directory
  lock. Index publication cannot rewrite the source manifest. No supported
  conflicting publisher was found in that interval.
- WAL decoding validates the whole recoverable sequence and preserves complete
  corruption/ambiguous truncation as errors. Partial final headers and valid
  payloads with incomplete matching CRC prefixes are the documented incomplete
  tails. Actual read errors and growth are not silently accepted. Binary row
  shapes and cursor bounds are checked; legacy JSON remains a supported public
  WAL read path with row-count/data-length validation.
- Startup verifies journal/WAL evidence before rollback or predecessor
  retirement, then restores the catalog checkpoint and resumes only a verified
  transaction suffix. Transaction origin, source commitment, canonical bits and
  row content are checked independently of the journal checksum. The <=16 KiB
  journal distinguishes active/completed checkpoints and intentionally identical
  writes. Generic WAL writes preserve their durable contract.
- Catalog format 13 bounds the complete frame to 64 MiB, its cached headers to
  8192 and each header to 16 KiB. Fixed-width conversion and cached-header slices
  follow validated bounds, including the pinned RLP decoder's remaining-length
  check. Segment IDs, paths, row addressing and active ownership are validated.
  Missing catalogs with existing artifacts are explicit errors. The small
  active-hot hint remains only an untrusted status-cache hint.
- Existing ordered/deferred/durable publication and cross-device sequencing
  remain. Apple FFI borrows valid live descriptors, retries interruptions and
  falls back to full sync where ordering barriers are unsupported. No new
  persistence operation, lock or ingestion strategy is introduced.

The retained reports include precise source references and existing finite tests.
Final workspace validation executes those tests, including WAL truncation,
repeated replay, I/O interruption, bundle/bitmap mutation, dictionary and offset
bounds, and source/maintenance ownership controls. It is not an exhaustive proof
of every schedule or hardware failure.

## Cleanup and limits

`ColumnData` is a public enum with no producer, consumer, test or documentation
reference in this repository beyond its declaration, implementation and re-export.
Removing that unused result abstraction does not remove any reader. This is an
API cleanup under the explicitly waived compatibility requirement; external
consumers would need to stop importing it. Useful raw readers, public codecs and
recovery paths remain because their removal has no demonstrated benefit.

Whole-file raw buffers, valid large payloads, query candidate sets and result
materialization still need shared resource accounting. Format limits are not a
process-wide RSS budget. Checksums do not authenticate independently rewritten
source data. Same-length external file changes still depend on ownership and
integrity checks. No 32-bit platform certification is claimed.

No performance campaign, live sync, deployment, remote-host work or production
file change was performed. This correction changes two decoder branches and
removes an unused enum; ingestion writes, fsync frequency, format and dependencies
are unchanged. The broader audit, verified repair and integrated offline tests
remain open; live sync and staging acceptance follow offline completion.

## Final local validation

All ten final-source local gates pass: 1,962 workspace tests, zero failures, 24 existing ignores across 35 targets, documentation tests and release build.

The [machine-readable record](baselines/2026-09-17-storage-boundary-disposition.json)
links hash-verified archives of original/fixed evidence, finite test output,
source reviews and complete final gate logs. Existing future-compatibility and
linker warnings remain in the logs; all gates pass. Source is `a7eae4b957520fe642832ac5bd2b52b79a229c79`.
Exact-head CI and merge remain before batch-2 offline closure.
