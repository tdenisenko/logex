# Index lifecycle integration

This batch starts at PR #223 merge `1a34273c` on
`audit/index-lifecycle-integration`. Source `8c47fa8e` corrects the public prefix
boundary below. All ten local gates, independent review and six CI jobs pass; PR #224
merged as `2b86b8a6`. Batch-6 offline code review is complete.

The review covers builder inputs, profile expansion, read-side fallback, physical
row identity, rotation, compaction and interrupted publication. Earlier page,
artifact and source-binding fixes remain documented in
[index file integrity](index-file-integrity.md),
[index publication binding](index-publication-binding.md) and
[source publication identity](source-publication-identity.md).

## Confirmed public prefix boundary

**B6-05 — low: public composite prefix helpers omit the maximum prefix.**
`CompositeQuery::scan_by_address` and `scan_by_topic0` computed an exclusive
upper bound by incrementing the prefix. An all-`FF` prefix wraps to zero, so the
B-tree range is empty even when matching keys exist. Current production query
paths use other exact/inclusive helpers; this is a public library correctness
finding, not a demonstrated application-query failure.

Two actual regressions run against unchanged original production plus an additive
test patch. Each constructs a 15-row B-tree and independently filters the input
row prefixes to obtain the expected bitmap. Zero, ordinary, carry and immediate
successor prefixes pass before the maximum prefix returns an empty result instead
of row IDs 12, 13 and 14. Both tests fail separately after successful setup.
Suffixes cover minimum, interior and maximum values. An absent-prefix control
also passes in the corrected run; it comes after the original failing assertion.
The preserved source and patch were independently verified against the exact
base, including isolated patch application and reversal.

The correction uses the existing inclusive B-tree range with the same fixed
prefix and minimum/maximum suffix. Both identical regression entry points now
pass. The unused increment helper and its obsolete wrap-behavior test are removed.
Two oracle tests replace one old helper test, giving one net additional test.
Public APIs and file formats remain unchanged.

## Reader and builder disposition

No additional defect requiring a change was established in these scoped paths.
Keep their current implementation.

- Primary and composite indexes retain physical row ordinals before nullable
  filtering. Participating source columns must match the captured row count
  before indexing. All physical rows are indexed; canonical selection remains
  a query filter, so reorgs do not renumber key indexes.
- A published set binds namespace, logical-prefix commitment, row count,
  generation, bundle reference and each artifact's file identity. Missing,
  mismatched, old-version or busy publications fall back to captured columns.
  Malformed checkpoints and registered damaged/missing artifacts return errors.
  Unregistered artifacts cannot establish a successful absence.
- Builders retain exclusive index-directory ownership, durably withdraw the old
  checkpoint before mutation, validate any reused artifacts, and compare source
  identity again before final publication. Abandonment leaves no partial published
  set. Profile expansion preserves only matching artifacts; a changed source
  invalidates other profiles too. The eleven current artifact names plus the
  optional legacy bloom fit the twelve-entry registry limit.
- Native OR alternatives union candidates; conjunctions intersect and remaining
  predicates refine against captured columns. Block/timestamp/composite block
  endpoints already use inclusive ranges. Bloom exclusions require the matching
  event/address/topic-position context and absence of all alternatives for a
  constrained position. Legacy Transfer bloom never excludes Approval queries.
  Read-side artifact guards stay alive through all such checks.

Existing regression coverage includes independent indexed/scan predicates,
maximum numeric endpoints, malformed and substituted artifacts, profile expansion,
source-column mismatches, adaptive bloom geometry and reopened inserted-key
presence. Full validation below executes the retained workspace tests; it does
not replace source review with a passing test count.

## Storage publication and snapshot disposition

No additional storage-lifetime defect was established in this scope.

Captured readers own their column/bundle handles and row boundary. Identified raw
capture validates its canonical envelope and logical-prefix identity, with bounded
retry if publication changes; legacy capture compares source markers. Compaction
publishes complete replacement columns/manifest before retiring old files. Open
handles preserve captured content, and representation-only compaction preserves
logical row ordinals. Query snapshots validate their read-view epoch before and
after execution and exclude appended suffixes/new segments. Reorg or storage
close invalidates old views rather than returning successful mixed-canonical data.

Captured compaction tasks retain data-directory ownership. Sealed compaction and
bundled canonical changes share a maintenance owner; stale source plans are
rejected before publication. Raw canonical/source writes retain source ownership.
Background index I/O runs on blocking workers after bounded metadata capture
releases ingestion access; active historical segments are excluded from sealed
indexing. CLI maintenance retains its storage owner through worker completion.

Existing tests cover interrupted index publication, raw/compacted/bundled captures,
rotation, sparse repacking, reorg invalidation, stale maintenance tasks, concurrent
builder exclusion and independent pagination across indexed/unindexed storage
layouts. Detailed source references and exact regression names are retained with
the reader, builder and storage-lifecycle reports.

## Validation, cost and limits

Focused validation passes both new regressions, all 82 index library tests, strict
all-target index Clippy and formatting. Two existing performance tests stay
ignored and no documentation test examples are present in the index crate. An
initial format check identified one blank line left after deleting the obsolete
test; the corrected formatting check passes. Final source and independent review
hashes match.

This correction adds no I/O, locks, allocation strategy, dependencies, storage
metadata or ingestion work. It reuses the existing inclusive range algorithm.
No benchmark is justified for helpers unused by current production consumers.

Retain the existing lazy integrity checks: freshness does not scrub every payload
page, and an accessed damaged artifact reports an error. Construction memory still
scales with source rows/cardinality; this review does not claim a process-wide
memory budget or justify a speculative rewrite. Previous performance samples and
limits remain in their original records. Integrated acceptance, automatic repair
and other audit areas remain separate. No broad benchmark, remote-host use, live
sync, production-data change or deployment occurred.

## Final gates and evidence

All ten final-source local gates pass: 1,961 workspace tests, zero failures, 24 existing ignores across 35 targets, documentation tests and release build.

The [machine-readable record](baselines/2026-09-17-index-lifecycle-integration.json)
links hash-verified archives of the original source and test patch, original/fixed
logs, source reviews, lifecycle dispositions and all final gate logs. Existing
vendor/future-compatibility and debug-linker warnings are retained; final gates
pass. Exact-head CI and merge passed; verified closure follows.

All six CI jobs passed on `c0f7a35a`, including ten Linux volume/template cases with verified cleanup. [PR #224](https://github.com/tdenisenko/logex/pull/224) merged as `2b86b8a6`. The merge tree is identical to the tested head. B6-05 and the batch-6 offline code review are closed; integrated workloads, shared resource policy and automatic repair remain in their separate scopes.
