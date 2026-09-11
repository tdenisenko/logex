# Startup validation and recovery evidence

PR #130's bundle recovery initially verified every payload extent on every open,
even with an unchanged catalog/manifest and no uncommitted suffix. At a0ed88c1,
the 2,211,840-row / three-segment growth fixture reopened in 93.540 ms versus
1.469 ms on the original implementation. These are warm-cache measurements, but
the unconditional scan also required reading all stored payload bytes on a cold
restart. That work scales with the entire database and is unnecessary for a
clean metadata open.

Clean startup now validates the catalog, complete bundle table chains, schema,
page indexes, stream lengths, null/canonical bitmaps and row/block boundaries.
Every payload extent actually read still requires its CRC to match before bytes
are returned. Clean startup is **not a full payload scrub**: damage in an unread
column can be reported by its first read. The startup success log names metadata
and row-boundary checks explicitly. This does not silently repair corruption,
recanonicalize rows or return invalid payload bytes.

Recovery that rewrites a derived manifest or trims an unpublished suffix still
verifies every retained payload extent before its first mutation. Orphan and
predecessor bundle retirement separately verifies the current bundle before the
first existing regular artifact is deleted. Missing old artifacts and unrelated
files do not force a full scan. The catalog is still hardened before recovery;
WAL evidence checks, per-read CRCs, page decoding limits and exclusive ownership
are unchanged. Corrupt retained payloads prevent recovery or retirement and
preserve the relevant files for later verified offline repair.

## Regression evidence

- The clean-open test fails before the change with `bundle extent checksum
  mismatch`, demonstrating that startup reads an unrelated payload column.
- Afterward, a clean reopen succeeds and the full-row read returns InvalidData.
  Repeating the open/read preserves exact bundle, manifest and catalog bytes.
- The same corrupt payload with a trailing unpublished suffix, missing manifest,
  or orphan generation makes startup fail before recovery changes those files.
- A real sparse repack followed by restoration of its predecessor artifact and
  corruption of the current payload preserves both generations across repeated
  failed reopen. Live and historical routes are both exercised.
- Existing rollback corruption cases now include a non-metadata payload, alongside
  canonical bitmap damage, table damage and truncation. Seven targeted bundle
  tests pass. An intermediate test helper searched an identical appended stream
  as well as the committed prefix; its unique-location assertion caught this.
  The helper was corrected to select only the authoritative reference's prefix.
  The initial failure is retained; it was not an implementation checksum failure.

The final paired growth measurement improves warm startup 87.042 → 7.528 ms
(-91.35%), with exact rows/progress and no ingestion regression. Sparse growth
and mixed history/live startup also improve.

Final-source platform checks, workspace gates and measurements are recorded in
[the PR acceptance record](pr130-acceptance.md). This check scope must be preserved
when the later offline repair coordinator and health/status behavior are added.
