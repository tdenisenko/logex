# Segment-reader integrity

Base: `003df476` (merged PR #131). Scope: bitmap row boundaries, manifest reads,
segment addressing and the metadata recovery paths that share those reads.

## Findings and resolutions

| ID | Severity | Confirmed behavior and resolution |
| --- | --- | --- |
| B2-16 | P1, incorrect query results on damaged storage | A valid but short compacted topic bitmap returned null for missing rows. A short canonical bitmap made queries silently omit rows. Require coverage of the reader's captured row boundary before interpreting bits. Longer unbundled bitmaps remain valid append prefixes; preserve both true and false bits. A native-query regression requires an explicit error and unchanged damaged bytes. |
| B2-17 | P2, unbounded metadata reads | Five manifest-loading paths read the whole file before parsing. A valid manifest padded beyond 1 MiB was accepted. Use one bounded loader with an initial size check, fallible reservation, a bounded read that also handles growth, and path-bearing parse/size errors. Enforce the same 1 MiB limit on writes. Generated manifests normally contain fourteen descriptors and a few KiB of metadata; no payload is added or re-encoded. |
| B2-18 | P1, unavailable metadata treated as absent | A dangling manifest alias was treated as no manifest, allowing readers to fall back to raw files and restart recovery to replace the alias. Only genuinely absent paths return `None`; unavailable aliases and other I/O errors propagate. Repeated startup failures preserve catalog, alias and segment artifacts for raw and bundled data. Restoring the alias target allows normal reopen with exact rows. |
| B2-19 | P2, unvalidated row/allocation bounds | Unbundled manifests could raise the page-row limit or exceed the u32 row-ID domain. Raw row-count reads accepted unsupported headers and oversized counts. Enforce the existing 16,384-row page ceiling and u32 segment addressing before use. Raw count reads validate version/compression and physical address-column length, reading only the fixed header rather than copying the whole column. |

The seven initial reader regressions all [failed before the fix](baselines/2026-09-12-segment-integrity-before.log).
They use small malformed files, including a 1 MiB whitespace fixture; no OOM or
production dataset is used as a test mechanism. Controls include exact-limit
manifests, valid empty raw columns, longer bitmap prefixes and preserved flags.

## Recovery and compatibility

Manifest JSON is still derived metadata for an immutable bundle. If it is
malformed or oversized, catalog-directed recovery still verifies every retained
bundle payload before rebuilding it. A dedicated regression repeats reopening
with an oversized manifest: valid payloads recover exact rows; corrupt payloads
leave both the original manifest and bundle unchanged. Appending through an
oversized manifest also fails without changing any column or bundle bytes.

An unavailable alias is an I/O failure, not malformed JSON eligible for local
reconstruction. A typed load error distinguishes malformed metadata from I/O,
even when an I/O failure itself has `ErrorKind::InvalidData`; error codes alone
must not authorize repair. A classification regression covers malformed JSON,
a real directory-read failure, and I/O error conversions including `InvalidData`. Empty active-segment recovery and genuinely absent manifest
handling retain their existing catalog-directed rules.

No storage version, payload codec or durability contract changes. The manifest
limit and page/row checks reject inputs outside the supported metadata bounds;
files produced by the existing writer fit those bounds. No production files,
mounts or services are modified. This is a prerequisite improvement, not the
planned external-volume supervisor or verified corrupt-segment repair feature.

## Validation and remaining scope

Fifteen reader tests, the native-query omission regression, append preservation,
and raw/bundled alias and oversized-metadata recovery tests pass. All six
final-source local gates pass with 934 tests (ten intentionally ignored).
[Release acceptance](baselines/2026-09-12-segment-integrity.md) retains all 45
query samples per revision/profile and 15 combined-sync samples per workload.
Live ingestion is +0.39%, cached history −1.70%; query medians range −0.83% to
+3.39%. Initial first-reopen costs and the isolated 500-open investigation are
reported explicitly. Preliminary measurements are kept separately from final
source `85950b57`. All six CI jobs passed on evidence tip `eb8d0983`;
[PR #132](https://github.com/tdenisenko/logex/pull/132) merged as `7ebc3795`.

Removed the five duplicated manifest read/parse implementations and unified the
page-row constant used by writers and bundled/unbundled readers. The manifest
loader intentionally separates parsing from read-bound validation so that
catalog-directed recovery can inspect mismatched derived metadata safely.

Variable-page decompression, large column/index inputs and total-query resource
accounting remain separate offline work. In particular, a representable u32 row
count is not itself a memory budget. SQL/native ordering across overlapping ranges
and query-wide snapshot lifetime are still pending. No complete-storage-audit or
release-readiness claim is made by these fixes.
