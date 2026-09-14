# Storage allocation and aggregate batches

This follow-up to PR #150 reviews stored lengths, decoder allocations and
selected payload reads. Work is on `audit/storage-resource-bounds`, based on
`8595e040`. All nine local gates pass on production source `0cf9e944`.
[PR #151](https://github.com/tdenisenko/logex/pull/151) merged as `29376426`
after all six final-head CI jobs passed on `0ac17fa7` in run `34904979423`.
The [final CI and merge record](baselines/2026-09-15-storage-resource-bounds-ci.json)
retains those outcomes. The broader storage and query
resource audit remains open.

## Findings and corrections

- **B2-22 (P1): raw append accepts an invalid existing layout.** Fixed columns
  checked row counts but not version, compression or physical body length.
  Variable appends copied offsets without checking their origin, monotonicity,
  payload bounds or final sentinel. Two-row fixtures demonstrated append success
  with a truncated address column and with a nonzero initial data offset.
  Fixed/nullable columns now check format and exact physical length before
  changing that file. Variable appends reuse the existing raw-column validator
  before constructing a replacement. Errors prevent canonical publication;
  other parallel workers can still leave uncommitted tails, handled by the
  existing recovery lifecycle. This is not a whole-directory rollback guarantee.
- **B2-23 (P2): bounded LZ4 output uses infallible allocation.** The size limit
  was checked, but the convenience decoder allocated its output infallibly.
  Reserve fallibly and use the same bounded slice decoder. Allocation failure
  now propagates as an I/O error. Small controls cover ordinary output, advertised
  capacity, incomplete input and byte limits; no memory-exhaustion experiment was
  run. This finding is established by the active dependency implementation.
- **B2-24 (P2): plain fixed-page length is checked after copying.** Validate the
  exact expected size before copying raw or adaptive-raw fixed-width payloads.
  Existing empty/exact/short/long fixtures cover the same successful output and
  explicit invalid-length errors.
- **B7-26 (P2): data-only aggregates retain a segment's selected payloads and
  miss cancellation during that read/fold.** The first bounded batching change
  restored cancellation between batches, but review found it would reread whole
  raw files per batch and could decode a compacted page twice. A storage-owned
  iterator now prepares captured artifacts once and follows physical page
  boundaries. It caches the current companion length page, including when data
  and length page boundaries differ. Initialization is lazy so cancellation can
  be checked before the first payload read; any storage error ends iteration.
  Existing result semantics and arbitrary-order reader APIs remain required
  invariants; the new monotonic scan API serves bitmap IDs.

## Implementation costs and limits

The new scan API explicitly requires ascending unique row IDs and checks the
captured row boundary. SQL bitmap selections satisfy that contract. Existing
arbitrary-order and duplicate-preserving read APIs are unchanged. Preparation
validates complete captured page indexes; raw files must cover every captured
row even if only an earlier row was selected. File access stays on the reader's
captured artifacts, including after replacement or rename.

Raw fixed/nullable append adds one metadata lookup per column, with no new fsync
or full-column read. Variable append already reads the old file; reusing its
validator removes separate old/new offset arrays and writes the existing encoded
offsets directly. Only new payload lengths are summed, using checked arithmetic.

The pinned `lz4_flex` 0.11.6 default `safe-decode` path already zero-initializes its
output. Fallible reservation preserves that initialization and decoder, rather
than adding another output pass. Plain fixed-page validation happens before an
allocation that invalid input does not require. No unsafe code was introduced.

These changes do not impose a total query byte budget. Raw readers retain whole
file buffers, candidate row IDs remain materialized, and a valid variable page
can contain large payloads. DataFusion's default pool is unbounded; its managed
pool also excludes some scan buffers and final collected/JSON results. gRPC and
REST do not yet share all admission/cancellation behavior. Those are explicit
follow-up items in the local roadmap and the batch ledger. Successful queries
must not be silently truncated to address resource pressure. Exported legacy
`ColumnReader::read_log_rows` and `read_row_count` have weaker validation than
`SegmentReader`; their current repository callers are tests. Their consolidation
is addressed in the subsequent [legacy-reader consolidation](legacy-column-readers.md);
these findings are not claimed as failures in the production query path.

The audit owner ended the extended benchmark campaign. No timing campaign is
required for these necessary fixes. Further measurement is reserved for concrete
implementation opportunities likely to yield substantial gains. Allocation and
I/O changes above are source-level properties, not measured latency claims.

## Adjacent paths reviewed without changes

Bundle reference validation runs before parent-depth arithmetic and table reads.
It bounds chain depth and decoded table bytes (4 MiB), validates decreasing
references, and checks stored checksums. Table parsing bounds streams (33),
extents (4,096 per stream) and extent sizes (1 MiB), checks counts against input,
and rejects invalid logical lengths, physical overlap and trailing bytes. Larger
read buffers use fallible reservation. No additional defect was established in
this pass; existing bundle regressions remain part of the workspace gate.

Production page-index validation enforces the 16,384-row ceiling. Fixed-page
size arithmetic is checked; dictionary/packed integer decoders validate their
input/index bounds, and variable pages use companion lengths and exact decoded
row lengths. The earlier PR #147 corrections remain in place. Valid payload byte
size and whole-query accounting are separate limits, described above.

## Regression evidence and validation

The new append regression tests were run before the production append fix:
`cargo test -p logex-storage --lib append_rejects_invalid_ --locked --offline -j 2`
returned 101, with both new fixtures failing because append returned success.
The existing canonical preflight control passed. The append implementation at
that point matched `8595e040`; other independent files had candidate changes,
so this is not an exact whole-baseline build. Later cases in each test loop were
not reached before the first failure.

The first full storage run after the fix passed 317 tests (five ignored).
A further focused column run passed all 22 tests, including repeated appends
starting from an empty column with mixed empty/nonempty payloads and selected
reads. These checks use temporary directories and small synthetic records.

The aggregate cancellation fixture was also run with only the old single-read
body restored, retaining the new test. It failed as expected; restoring the
candidate passed both focused controls and all 79 query library tests (one
ignored). That is regression evidence for cancellation, not final validation of
the subsequently refined iterator. The final iterator passes all 37 segment
reader tests and a focused repeat after strengthening a fused-error assertion.
The final SQL integration passes both aggregate controls. Coverage includes raw
and compacted storage, mixed column layouts, exact once-only data/length page
reads across differing boundaries, snapshot replacement and rename, raw buffers
retained across batches, and invalid stored lengths. The complete workspace and release results are
recorded below.

## Final local acceptance

All nine local gates pass on `0cf9e944`: vendor verification, formatting, workspace
check, strict Clippy, workspace tests, documentation tests, release query tests,
release API consistency and release node build. The [command/result record](baselines/2026-09-15-storage-resource-bounds-gates.json)
retains commands, compiler identity, log hashes and the initial failed attempt.

Workspace tests pass 1,198 (23 ignored); release query tests pass 151 (nine
ignored); release API consistency passes both tests. The seven documentation
test groups contain no examples. The initial workspace run stopped when the
sandbox denied localhost listeners in existing checkpoint fixtures. An approved
rerun of the same source passed; no test or protection was disabled. No benchmark
ran, and no timing improvement is claimed.

The temporary page-size export and count-based query batching were removed after
the captured storage iterator replaced them. Variable appends no longer allocate
two redundant offset arrays. Remaining resource/legacy-reader work stays in the
batch ledger and local roadmap. This milestone does not complete the offline
audit or authorize live-sync deployment.
