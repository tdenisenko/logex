# Parsed query syntax eligibility

Base: `c78e519c` (merged PR #144). This pass checks whether parsed options are
implemented, rejected, or silently ignored by the optimized and engine paths.
Implementation: `1b43f00b`, followed by stack correction `fe400c7f041e76556d1e7343c76e9edd4c2269fd`.
Direct review, focused checks, all six required local gates and additional
release checks pass. The first workspace run exposed an existing query-boundary
regression, corrected before repeating all gates. Paired release performance
acceptance passes after the retained tail-latency investigation. Final-head CI
and merge remain pending.

## Confirmed findings

- **B7-21 — P2, interpolation syntax can disappear in optimized execution.**
  The native row/count/exact-sum order matchers and fixed metadata sorter inspect
  the ordering expressions but omit the enclosing interpolation option. The
  pinned engine explicitly rejects that option. The small baseline fixture
  demonstrates acceptance before the correction.
- **B7-22 — P2, metadata table aliases can ignore column definitions.**
  Plain table aliases are supported, but a column list attached to an alias is
  ignored. A full-width permutation of the four fixed table-metadata names
  demonstrates a different value under the selected name in the independent
  reference. The old metadata path returns the original value instead.
- **B7-23 — P2, falling back to the engine does not ensure parsed options run.**
  Direct review and runtime checks show that the pinned engine drops several
  parsed fields. On a two-row fixture, a one-row fetch and a selective early
  filter both still return two rows; zero-percent sampling also returns two.
  A grouping modifier omits the required summary row. Unsupported execution,
  table and function options also need explicit dispositions instead of assuming
  that the engine rejects everything it does not implement.

These are query acceptance/result defects. They are not evidence of altered
ingestion writes, trusted state, storage formats or persisted data.

## Implementation and review boundaries

The correction reuses the existing first parsed AST and its visitor. Read-only
violations remain distinct from unsupported-feature errors. Features that the
pinned engine would silently discard now fail explicitly; supported engine paths
are preserved. The existing
iterative scan covers set-operation arms; normal visitor recursion reaches CTEs
and nested queries. This does not add another parse or a new planner.

Metadata table column aliases require a local explicit unsupported error because
the fixed metadata path does not implement renaming. Plain aliases remain valid.
Changing the ownership and naming model of the fixed metadata schema is not
needed to stop incorrect results.

## Field review

| Boundary | Disposition |
| --- | --- |
| Query body, CTEs, ordering kind, limits/offsets and pipes | Keep supported planning and existing native eligibility. The engine explicitly rejects unsupported limit-by syntax. |
| Fetch, row locks, result-format clauses and settings | The pinned planner does not consume these parsed query fields; explicit errors are required. |
| Select filtering/hierarchy/value modes | Reject dropped early-filter, hierarchy and value-mode options. Preserve implemented window filtering and ordinary projections. |
| Select-level exclusion | Not emitted by the pinned default GenericDialect; do not label this as a reproduced reachable bug. The known unsupported field can be guarded explicitly. Wildcard exclusions already have separate eligibility checks. |
| Grouping | Reject ignored trailing grouping modifiers. Preserve ordinary grouping expressions and grouping-all where implemented. |
| Table modifiers | The engine consumes table names, arguments and aliases but drops several physical-table options. Sampling has an independent cardinality counterexample. Hints and other unimplemented options require explicit errors, without claiming all change row count. Table-function settings are also dropped: two-row default-series calls reproduce acceptance inside CTEs and derived queries. Reject settings while preserving ordinary arguments. |
| Function calls | Reject ignored parametric argument lists; preserve ordinary arguments, filters, null handling, windows and implemented ordering clauses. |
| Syntax markers | Source positions, clause-position flags, FROM-first forms and ODBC wrappers must not be rejected merely for having a different parsed representation. Reference controls verify actual behavior; a FROM-only projection is not assumed to be a wildcard. |

The value-table mode, select-level exclusion, table version, table JSON-path and
index-hint fields have source-level defensive dispositions; this pass does not
claim runtime reproductions for them through the default dialect. Alternate
values of demonstrated fields are handled consistently, without claiming a
separate fixture for every spelling or modifier.

The reference must check known semantics independently when the pinned engine is
itself the source of the defect. Comparing two equally incorrect results is not
acceptance. Other cases use a small independent in-memory schema and full planning
and collection. All tests remain bounded and use disposable local data.

## Validation and retained attempts

The six focused groups pass, as do preceding aggregate, identifier, metadata and
scope regressions, strict focused Clippy and formatting. Supported controls cover
plain aliases, ordinary rollup expressions, grouping-all, wildcard exclusion,
FROM-first projections, ODBC wrappers and bounded default table functions.
The hierarchy fixture follows a strictly forward two-node relation.

The first required workspace run failed the unchanged arithmetic boundary child
in `sql_limits`: its stack was exhausted. Formatting, checking and strict Clippy
passed before that failure. The entire failed gate output is retained in the
[validation record](baselines/2026-09-12-sql-syntax-eligibility-validation.json).
Focused reproduction confirmed
the failure, and a layout probe measured the initial tagged error payload at
24 bytes, compared with 16 bytes for the previous borrowed message. A payload-free
typed violation is one byte. Passing that compact kind through the generated
recursive visitor, then constructing the message after it returns, restores the
unchanged boundary test. No query limit, expected result or test stack is changed.
The corrected source passes all repeated required gates. The final validation
binds 102 source/configuration hashes to `fe400c7f`: formatting, workspace
checking, strict Clippy, 1,021 workspace tests with 17 unchanged exclusions,
documentation tests and the release node build. Additional release checks pass
126 query tests with six exclusions and both protocol-consistency tests. The
workspace exclusions include explicit benchmarks, four isolated-mount checks
and a child entry invoked by its normal parent test; the six query exclusions
are five benchmarks and that child entry. This SQL-only correction does not
repeat the separately recorded cross-mount storage acceptance.

The [retained attempts](baselines/2026-09-12-sql-syntax-eligibility-before.json.gz)
include the interpreted investigation inventory, source/fixture/configuration
snapshots, original logs, standalone layout probe and binary hashes. Initial
reference expectations were corrected before final acceptance; their failed
outputs remain. A preliminary snapshot with no executed test is labeled as such.
The original prepared acceptance runner was never executed before the stack
correction. Baseline source snapshots were checked against `c78e519c`; final
source and fixture snapshots match `fe400c7f`.

The initial argument-settings review considered only direct outer tables. That
was insufficient: the established shallow table check delegates CTE definitions
and derived queries to the engine. Reproduction through both paths confirmed the
missing guard before the implementation was committed. This is now covered by
the recursive visitor and an ordinary-function compatibility control.

## Release measurements and investigation

The [initial release record](baselines/2026-09-12-sql-syntax-eligibility-release.json)
contains the complete runner, final fixtures, exact source/binary hashes,
environment and correctness outputs. Exact-commit archives receive identical
fixtures; refreshed source timestamps, fresh-artifact checks and distinct copied
binary hashes prevent shared-target reuse from invalidating the comparison.
Fresh release reference checks pass with four failing defect groups and two
passing compatibility groups on the baseline, then all six passing on the
candidate. Both revisions also pass all six previous identifier regressions.
Groups that stop at the first failed assertion do not prove later cases ran.

Five alternating metadata pairs use three exact-result shapes with 1,000 rotating
samples per shape/process. Five alternating REST pairs use five shapes and 50
rotating samples per shape/process. Each REST process creates, indexes, compacts
and reopens 20,000 synthetic rows. Each shape is warmed once; cache eviction is
not forced. No builds/tests run concurrently with timing. Environment: Mac14,15,
16 GiB memory, eight CPUs, macOS 26.6.2, internal APFS temporary directories and
pinned `nightly-2026-08-24`; full toolchain/filesystem details are retained.

The first unchanged metadata/REST comparison retains all 32,500 latency samples
and 20 memory observations. Metadata medians change by +0.16% to +0.32%; REST
medians change by -0.11% to +1.21%. Native-count p95 increases +5.39%, requiring
investigation. Its five individual process-pair p95 changes
range from -5.03% to +28.13%; every original observation remains retained.
A predefined five-pair [confirmation](baselines/2026-09-12-sql-syntax-eligibility-confirmation.json)
reuses the exact same binaries, starts candidate-first and retains initial,
confirmation and combined results separately. No source, fixture, workload-size
or toolchain change occurs. All 2,500 additional latency samples and ten memory
observations remain, including original full process logs and counters.

The native-count confirmation changes median/p95 by +0.41%/-1.11%; the initial
tail increase does not repeat. Combining all ten pairs gives +0.76%/+2.39%
(p95 0.809042 ms to 0.828375 ms). This does not establish a particular operating
system cause, and the original +5.39% is not replaced or excluded.

| Workload | Median change | p95 change |
| --- | ---: | ---: |
| Table metadata, five pairs | +0.29% | -12.06% |
| Filtered column metadata, five pairs | +0.16% | -7.60% |
| Aliased column metadata, five pairs | +0.32% | -7.78% |
| Metadata process peak RSS | -0.22% | -0.34% |
| REST engine aggregate, all ten pairs | +1.08% | +2.40% |
| REST scalar expression, all ten pairs | -0.22% | -0.67% |
| REST flat literal list, all ten pairs | 0.00% | -0.13% |
| REST native count, all ten pairs | +0.76% | +2.39% |
| REST native wide page, all ten pairs | +0.14% | +0.36% |
| REST process peak RSS, all ten pairs | +0.61% | +0.66% |

All positive combined changes remain below 5% and the 10% rejection ceiling.
The small median changes and variable tails do not establish a general speedup.
The initial investigation threshold was respected, and no failed timing run or
unreported confirmation precedes this disposition. All 35,000 latency samples
and 30 process-memory observations are retained across the original and
confirmation datasets. The [original stream](baselines/2026-09-12-sql-syntax-eligibility-final.jsonl.gz)
and [original process logs](baselines/2026-09-12-sql-syntax-eligibility-run-logs.json.gz)
remain unchanged; the filename containing `final` predates the confirmation and
denotes that original stream, not pooled acceptance. The confirmation record
links separate initial/confirmation/combined streams and all additional logs.
Compressed artifacts round-trip exactly, summaries were independently
recalculated, and combined records were checked against every original record.

No ingestion write, storage format, dependency or toolchain changes are included.
These offline query measurements exclude transport and live peers; they do not
establish whole-node ingestion throughput. Final-head Linux/macOS CI and merge
follow this local acceptance.

## Cleanup and remaining work

The expanded validator replaces the old private visitor name and reuses its
existing traversal. Temporary duplicate interpolation checks were removed when
the central guard superseded them. Error messages are constructed only after
traversal from compact typed kinds. Existing local eligibility helpers remain
used; no second parser, planner, unused dependency or unsafe block is introduced.

Broader aggregate contracts, legacy rewriting, resource/cancellation ownership
and the remaining subsystem batches stay open. Actual live sync and staging
follow completion of the offline audit.

One retained grouping probe combines grouping-all with a count and reaches an
explicit physical-planning error. It was unsuitable as an already-supported
compatibility control, but it is valid follow-up audit evidence. Investigate the
engine/provider boundary during the broader aggregate-contract pass; this
milestone's ordinary grouping-all control does not certify that combination.
