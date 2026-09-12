# Native SQL filters and exact predicate pushdown

Base: `f19f47c4` (merged PR #137). This milestone audits predicates translated
into native filters, the DataFusion provider's exact-pushdown declaration and
numeric index range boundaries. General SQL binding, the custom SUM(data)
residual/CASE evaluator, casts and query resource limits remain separate audit
areas. This record does not claim completion of those paths or the offline audit.

## Findings and correction

- **B7-07 — P1, unsupported operators change results or disappear.** Native
  numeric `!=`/`<>` became equality, while DataFusion was told those filters were
  exact even though the provider did not apply them. `WHERE block_number + 1`
  was also accepted as equality in the native path instead of rejected as a
  non-boolean predicate. Only represented numeric comparison operators are now
  accepted; the general engine retains all other predicates and validates them.
- **B7-08 — P1, conjunctions can lose constraints.** Disjoint address lists
  became the empty vector meaning unrestricted, and another predicate could
  repopulate them. Block-hash and data-length equalities overwrote earlier
  equalities. Preserve an empty intersection as an inverted inclusive block
  interval, which later intersections cannot revive; reject it before segment
  reads. Equalities intersect consistently in native AST and provider paths.
  Data lengths use checked u32 conversion instead of clamping a different value.
- **B7-09 — P1, byte conversion violates ordinary SQL string equality.** Hex
  decoding ignored case/prefix differences and provider scalar parsing trimmed
  whitespace. The stored columns are canonical lowercase 0x-prefixed strings;
  conversion to bytes is exact only for that representation. Other string
  literals keep their comparison in DataFusion. Typed legacy `address'…'` and
  `event'…'` rewriting remains unchanged. This corrects selection/count/provider
  filter paths; the custom big-integer residual evaluator remains to be audited.
- **B7-10 — P2, strict numeric endpoints include impossible boundary values.**
  Saturating `< 0` or `> u64::MAX` manufactured an inclusive endpoint instead of
  the empty set. Checked successor/predecessor conversion preserves emptiness,
  including reversed comparisons and subsequent conjunctions.
- **B6-01 — P1/P2, numeric range indexes panic or omit endpoints.** An inverted
  SQL range caused an invalid slice in the reader; the in-memory B-tree range
  had the same reversed-bound precondition. Empty/reversed half-open ranges now
  return empty sets. Numeric scans use an inclusive reader operation instead of
  adding one to the upper bound, preserving u64::MAX. Composite address/topic/
  block scans use the same inclusive operation with their original prefix.
  Existing public half-open APIs remain available; key construction is shared.

## Reproduction and oracle

The [raw-storage baseline failures](baselines/2026-09-12-sql-predicates-before-raw.log)
show exact result differences. The [indexed baseline](baselines/2026-09-12-sql-predicates-before-indexed.log)
reproduces the reversed-range panic. After the initial operator/intersection
correction, an additional [boundary run](baselines/2026-09-12-sql-predicates-before-boundaries.log)
found the upper-inclusive index omission before that separate correction.

An independent DataFusion MemTable is built directly from fixture values. It
uses no LogEx provider, stored columns, index code or native filter translation.
159 predicates (31 fixed and 128 seeded AND/OR combinations) compare native
selection, forced-DataFusion selection and COUNT to that reference, including
invalid WHERE expressions. Run against raw hot storage, indexed hot storage and
indexed/compacted/reopened storage. Ordinary null/OR and noncanonical string
literal cases are included; no source row is mutated by query testing.

A separate integer oracle checks zero/u64::MAX equality, strict/inclusive bounds,
standalone block/timestamp indexes and the composite index in all three layouts.
An index-level oracle checks 133 values including both endpoints and seeded
values, with equal/reversed/unbounded-edge ranges: 532 comparisons each for the
in-memory and file-backed half-open APIs and the inclusive file reader.
These are integer set comparisons independent of binary search implementation.

All four integration tests, 64 existing query unit tests and 28 index unit tests
pass. The range oracle assumes a valid sorted index; malformed on-disk key tables,
allocation bounds and point-lookup validation remain in the wider index audit.

The existing benchmark/pagination SQL used Rust's checksummed address display,
which depended on the old coercion. A [smoke failure](baselines/2026-09-12-sql-predicates-bench-smoke.log)
caught this dependency before any performance comparison. Those fixtures now
emit canonical lowercase literals, copied identically to both measured revisions;
the [corrected smoke](baselines/2026-09-12-sql-predicates-bench-smoke-canonical.log)
checks the same intended rows. The dashboard also canonicalizes the selected
token when generating SQL, preserving matches for checksummed custom-token input.
Checksum validation/display and explicit typed wallet literals stay unchanged.
A [manual Node check](baselines/2026-09-12-sql-predicates-builder.mjs) loads the
actual embedded functions, checks whole-script syntax and four token cases with
isolated UI-input stubs; [results and source hashes](baselines/2026-09-12-sql-predicates-builder.jsonl)
are retained. Run it from the repository root. This checks generated SQL,
not a browser layout or the whole dashboard.

All six local workspace gates pass: format, check, Clippy, 992 tests
(13 ignored), documentation tests and the release node build. Release query/index
regressions and the generated-SQL check also pass. Implementation `0329745c`
passes [repeated equivalent release comparisons](baselines/2026-09-12-sql-predicates.md):
45 standard samples per metric/profile/revision and 100 DataFusion samples per
shape/profile/revision. All initial and confirmation observations are retained.
Dense mixed historical-write/reopen/COUNT tails remain +7.52%/+7.63%/+7.00%,
explicitly below the 10% ceiling. A separate 100-sample phase-isolation check
shows no positive median/tail difference above 1%; no speedup is claimed.
PR/CI/merge remain. Performance comparisons use baseline
queries with already-correct outputs; timing a wrong equality result against a
correct inequality is not a meaningful regression measurement. Common valid
range/address/topic predicates retain their native/indexed paths.

## Cleanup and limits

Remove the default-to-equality coercion, clamped data length, overwriting
conjunctions and saturated numeric index endpoints. Keep generic hex parsers for
explicit legacy literals and the separately scoped custom aggregate evaluator;
keep public half-open index APIs for compatibility. No new persisted format,
index encoding, ingestion write, dependency, unsafe block or toolchain change.

All fixtures use temporary directories. Production data, protected external
volumes and services remain untouched. Actual live sync and staging acceptance
follow completion of the full offline audit.
