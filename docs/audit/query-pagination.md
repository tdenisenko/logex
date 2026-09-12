# Native and SQL pagination

Base: `7ebc3795` (PR #132). This pass fixes ordering and pagination on valid
storage. Concurrent snapshot replacement and general SQL/resource semantics
remain separate audit work.

## Confirmed findings

| ID | Severity | Evidence and resolution |
| --- | --- | --- |
| B7-01 | P1, wrong query rows | Native filtering truncated physical rows before global ordering, and both native/native-SQL paths stopped after the first partitions supplied enough rows. Reverse ingestion `[30,40]` then `[10,20]` with ascending LIMIT 2 returned `[30,40]`. Overlapping ranges `[10,40,70]`, `[20,50,80]`, `[30,60,90]` returned `[10,40]` instead of `[10,20]`. Retain the globally ordered prefix and inspect every remaining range that can improve it. |
| B7-02 | P1, incorrect API pagination | Native SQL combined API offset with result limit and then added it again for scanning, without subtracting it from SQL LIMIT. SQL LIMIT 3 with API limit 2/offset 2 returned blocks 30,40,50 instead of only 30. Apply the page to the SQL result: remaining SQL rows are `sql_limit.saturating_sub(page.offset)`, output limit is the minimum of that and API limit, and scan includes the offset once. |

All four initial reproducers [failed before the fix](baselines/2026-09-12-query-pagination-before.log).

## Ordering and pruning

Ascending partitions are ordered by minimum block, descending by maximum block.
Every bounded prefix is sorted before truncation. A query can stop only when the
next partition's bound is strictly beyond the last retained block in the selected
direction; equal boundaries remain eligible because transaction/log keys can
still change the page. Empty limits return empty results without storage reads.
Native SQL merges every result in a scanned window before pruning later windows.

The shared row-order helper replaces duplicate sorting logic. SQL partition
materialization already follows sorted row IDs, so its redundant second sort was
removed. Unlimited queries retain a final global ordering pass. No writer, index,
file format, durability policy or dependency changes.

## Validation

Nine integration regressions cover raw/indexed/compacted storage, historical
writes, equal block boundaries, offsets, zero/unbounded limits and offsets up to
`usize::MAX`. An independent input-vector reference and equivalent DataFusion
execution validate SQL LIMIT/API composition. Twelve deterministic shuffled
fixtures exercise raw multi-segment and coalesced historical insertion.

A 65-partition fixture requires merging equal-block keys across SQL scan windows.
A separate 65-partition fixture makes an irrelevant middle range unreadable:
both ascending/descending limited queries prune it; unbounded queries return an
explicit error. This guards against replacing pagination with an unconditional
full-database scan. No production files or services are involved.

Focused query tests pass (60 tests, one intentional ignored benchmark). All six
workspace gates pass (943 tests/ten ignored, plus documentation tests). Release
performance comparisons and all six CI jobs remain before merge.

## Remaining scope

Captured partition metadata alone does not establish a query-wide file snapshot;
selection and materialization still need their own concurrency audit. Candidate
row vectors remain segment-sized, and unlimited materialized results can be large.
This task does not claim complete query-memory accounting, full SQL validation,
live-sync readiness or release acceptance.
