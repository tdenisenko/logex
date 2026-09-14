# Legacy column-reader consolidation

This follow-up to merged PR #151 removes weaker duplicate readers on
`audit/legacy-column-readers`, based on `29376426`. Final gates and merge are
pending. The exported methods have only test callers in the current repository;
production query reads already use `SegmentReader`.

## Findings

- **B2-25 (P2): full-row assembly assumes equal column counts and matching data
  lengths.** A valid one-row block-number column beside a two-row address column
  caused an index panic. Two four-byte payloads with declared lengths three and
  five were returned as successful rows. Both cases are reproduced with two-row
  temporary fixtures. Delegate full-row reads to the existing captured segment
  reader, which validates column counts and per-row payload lengths. Remove the
  duplicated assembly loop and its unchecked indexing.
- **B2-26 (P2): raw row counts trust an unvalidated header and read the whole
  column.** Unsupported formats, oversized counts and body-length mismatches
  could be returned as successful counts. Read a fixed-size header and metadata
  from the same open file. Share the existing segment reader's raw address
  validation, including the u32 row-ID bound and exact physical length. A short
  header returns `InvalidData`; other I/O errors remain actionable errors.

## Behavior and cost

Full-row reads now use one captured publication and honor manifest errors,
including dangling aliases. Empty, descending and duplicate selections retain
row order. An authoritative manifest can retain its prior published prefix while
an append advances other files. A manifestless directory with an unfinished
raw layout cannot establish that complete boundary and returns an explicit error.
This is an intentional change to the legacy wrapper under the audit's waived
compatibility requirement; production SQL/native readers already use this policy.

Standalone selected-column readers retain their existing prefix behavior. The
five-stage unfinished-append fixture now exercises each affected column reader
directly and also checks the stricter full-row behavior. A captured-manifest
fixture verifies full-row prefix reads with descending and duplicate IDs.

No ingestion or compaction writer call path changes. The production raw count
fallback calls the same factored validation logic with captured header and length.
The legacy count wrapper reads only the header, removing its whole-column copy.
The wrapper delegates full-row I/O to the existing captured reader; no timing
improvement is claimed and no benchmark campaign is required for this cleanup.
Individual-column helpers remain in use by compaction or focused tests.

## Validation

Before the implementation change,
`cargo test -p logex-storage --lib reader::tests::legacy_ --locked --offline -j 2`
returned 101: all three new tests failed (vector-index panic, invalid payload
lengths accepted, unsupported header accepted), while two matching existing
segment tests passed. Later cases in each loop were not reached after the first
failure. Production code at reproduction matched `29376426`.

After consolidation, all 56 reader/segment-reader tests pass. Controls retain
empty/all-null/replaced sources, arbitrary selected order, incomplete append
prefixes, captured manifest boundaries, per-row length validation, and unchanged
dangling aliases. Independent review found no additional concrete issue. Final
workspace and release results will be recorded before the PR.
