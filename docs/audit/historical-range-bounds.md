# Historical segment bounds

This storage follow-up is on `audit/historical-range-bounds`, based on PR #220
merge `4635cbda`. It addresses B2-27 without sorting stored rows or changing the
file format. Source is committed as `75a73e4d`; all ten local gates pass.
CI passed; the verified merge is recorded below.

## Finding and scope

**B2-27 (P2): historical range metadata can exclude accepted rows.** The public
`PartitionManager::write_historical_batch` has no input-order restriction.
Both dense segments and sparse staging accept nonmonotonic block numbers and
timestamps, but their descriptor helper takes extrema from only the first and
last row. The catalog and manifest therefore advertise bounds narrower than the
data they contain. Opening the resulting storage succeeds.

Native queries use those bounds to skip partitions and to stop pagination once
the remaining partitions cannot improve the result. Exact-original controls
demonstrate missing block/time point results in both historical routes, and
incorrect ascending and descending one-row pages across overlapping segments.
Unrestricted queries first confirm that the fixture rows are actually present.

The production historical collector sorts authenticated blocks by number before
extracting rows. This finding demonstrates incorrect behavior for other orders
accepted by the public storage API; it is not evidence that ordinary production
historical sync produces those orders. Compaction eligibility and legacy replay
also consult descriptor bounds, but this pass does not claim a reproduced failure
in those consumers.

The correction calculates block and timestamp extrema independently over every
accepted row, merges them with existing segment bounds, and preserves physical row
order and logical source commitments. It covers dense bundles, new and existing
staging, splits/rotation and reopen. Historical and live descriptor updates now
share the same range semantics.

## Original regression evidence

With production source at `4635cbda`, two storage tests use four rows with block
numbers `[100, 5, 900, 101]` and timestamps `[1000, 50, 9900, 1001]`. They verify
physical row preservation and successful reopen before failing the expected
full-extrema assertion. Both actual descriptors report blocks `100..101` and
timestamps `1000..1001`; correct values are `5..900` and `50..9900`.

Six separate public native-query tests fail on the same original production
source: block and timestamp point filters for dense/staged storage, ascending
first-row selection and descending last-row selection. The pagination fixture
returns block 30 instead of 1 and block 500 instead of 900. Each result is an
assertion failure after successful fixture setup and execution. Later assertions
in a failing test are not claimed as independently reproduced original failures.

Evidence uses small synthetic rows and owned temporary directories. No production
data, external volume, remote host or live sync is involved.

## Implementation cost and remaining boundaries

The shared `RowBounds` accumulator is ephemeral. Dense/new bundles already have a
per-row encoding preflight; existing staging already scans candidate block ranges
before deciding whether to rotate. Reusing those passes avoids an additional full
historical pass. Live bounds replace four independent extrema scans with one.
These are source-level cost properties, not measured throughput results. No broad
benchmark campaign is planned.

This fix does not rewrite previously published incorrect descriptors. The audit
owner permits fresh storage and does not require migration. Existing user data
remains protected. No reorg range pruning is introduced; the earlier full scan
continues to avoid relying on old descriptor bounds. Automatic offline repair
and other storage/parser review items remain separate audit work.

## Validation and cleanup

The four storage controls and six public native-query controls pass after the
correction. Storage controls cover dense/new staging, existing staging, row-count
and block-span rotation, dense prefixes with staged remainders, repeated reopen
with and without checkpointing, raw/bundled live writes, supported raw historical
prefixes, empty input and single rows. Separate fixtures have block-ordered rows
with interior timestamp extrema. Tests compare catalog, manifest and public
metadata to full physical reads and verify that rows/payloads retain caller order.
Native controls exercise filters before/after index construction and reopen, plus
both directions of pagination over overlapping ranges.

The complete storage library suite passes 336 tests (five existing ignores);
six native range controls and nine existing pagination tests pass. Scoped strict
Clippy and formatting pass. The first format check found only line wrapping in
the newly added native test; it was corrected without changing behavior.
Independent review found no omission in the scoped candidate. All ten final-source
local gates pass: 1,940 workspace tests, zero failures, 24 existing ignores across
35 targets, documentation tests and release.
The [machine-readable record](baselines/2026-09-17-historical-range-bounds.json)
links hashed archives of original controls, fixed checks, independent review, source
inventories and full gate output. Inherited dependency/debug-linker warnings remain
recorded; all final local gates passed. CI passed; the verified merge is recorded below.

Removed the endpoint-only descriptor helper, duplicate descriptor-update logic and separate
block-only range helper; no reorg pruning or serialized metadata was added.
This milestone does not complete the offline audit or establish live-sync/release
readiness.

All six CI jobs passed on `d7a907e0`, including ten Linux volume/template cases with verified cleanup. [PR #221](https://github.com/tdenisenko/logex/pull/221) merged as `e54e9303`. The merge tree is identical to the tested head. B2-27 is closed for newly written data; remaining storage and broader audit work stays open.
