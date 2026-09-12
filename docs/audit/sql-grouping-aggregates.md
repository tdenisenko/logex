# Grouping and membership planning

Base: `c24d56b6` (merged PR #145). Implementation: `8b7812bb`. Independent
fixtures confirm two engine defects. The attributed backports pass local
correctness gates and equivalent-query performance checks. The complete release
investigation and its measurement limits are recorded below. PR/CI/merge remain.

## Starting evidence

A retained two-row probe from the syntax audit combines the source grouping key
with a count, infers grouping from the selected expressions and orders by source.
It reaches an explicit physical-planning error concerning an aggregate expression.
Ordinary grouping-all without an aggregate and ordinary rollup grouping with a
count succeed. The original source, fixture and failed output remain in the
[syntax investigation](baselines/2026-09-12-sql-syntax-eligibility-before.json.gz).

That error made the combination unsuitable as an already-supported compatibility
control. It remains an audit lead: do not mistake the simpler passing control
for proof that the aggregate combination works, or assume the local table provider
causes the error without checking the engine's independently planned results.

## Confirmed cause and remedy assessment

**B7-24 — P2, inferred grouping misclassifies nested aggregate expressions.**
An independently populated in-memory table in DataFusion 51 reproduces the same
planning failure. Its inferred grouping includes a count beneath two aliases,
which cannot be compiled as a physical grouping key. The wildcard count planner
preserves the function's original display name with an inner alias; the explicit
output alias adds another. Inferred grouping unwraps only one alias. Arithmetic
around an aggregate exposes the same shallow classification problem. Explicit
grouping provides a working reference with known row counts.

The independent table establishes the engine cause without using LogEx's lazy
storage provider. The initial native-count candidate fixes only the existing
source/global projection forms and did not resolve the broader engine defect.
It was removed without a commit; its source, tests and results remain retained
as rejected investigation evidence.

Upstream [fix #20943](https://github.com/apache/datafusion/pull/20943), merged as
`dfc8bb7dd65ba6a96b2a259dec5776945b361767`, recursively checks each inferred
grouping expression for aggregates. The fix is present in the released
[DataFusion 54.1.0 planner](https://github.com/apache/datafusion/blob/54.1.0/datafusion/sql/src/select.rs).
The released upgrade was evaluated before selecting the remedy. It passed the
new grouping regressions after API adaptation, but failed broader compatibility
checks. The candidate is therefore rejected; no performance acceptance or broad
workspace gate claim applies to it.

The selected remedy is a backport of that upstream grouping correction to a
verified copy of the published DataFusion SQL 51 package. Preserve the existing
engine/parser/dependency versions and apply only the attributed planner change.
The package copy introduces an explicit maintenance obligation; source provenance
and the complete local diff must be reviewable and checked. A second local
inference/coercion planner would introduce more semantic duplication.

## Published-package provenance

The candidate retains the complete published packages, including their original
licenses, notices, manifests, tests and examples. The root lockfile keeps all
existing dependency versions; only the source of the two patched packages changes.
The vendor directories are excluded workspace members. New LogEx regressions
must therefore run through workspace test targets rather than relying on upstream
unit tests that the workspace gates do not execute.

| Published package | Archive SHA-256 | Original files | Intended modified files |
| --- | --- | --- | --- |
| DataFusion SQL 51.0.0 | `3fc195fe60634b2c6ccfd131b487de46dc30eccae8a3c35a13f136e7f440414f` | 47 | Aggregate grouping in the selection planner |
| DataFusion optimizer 51.0.0 | `9f35f9ec5d08b87fd1893a30c2929f2559c2f9806ca072d8fefca5009dc0f06a` | 52 | Expression simplification and filter elimination |

The archive checksums match the original root lockfile. Independent comparison
confirms that the other copied upstream files remain byte-for-byte identical.
Preserve reviewed original inventories and explicit local patch records; CI must
check the complete copied file sets and intended modifications offline. Source
acceptance includes these files, the verifier and the CI configuration in its
unchanged-source hashes. This is a maintained backport, not an engine upgrade.

## Rejected release experiment

DataFusion 54.1 intentionally changes number/text comparison rules, including
membership, ranges and simple conditional comparisons. Both 54.0 and 54.1 contain
upstream change [#20426](https://github.com/apache/datafusion/pull/20426).
Official 53.1 and 52.5 planners still contain the original grouping defect, so
neither supplies the desired released fix while retaining the existing rules.

**B7-25 — P2, membership set simplification erases NULL semantics.**
The upgrade investigation exposed incorrect rows for a finite membership guard
combined with negated membership containing NULL. The behavior occurs in an
independent in-memory table and LogEx's general query path. Each unguarded
control correctly returns no rows. A subsequent same-type numeric control proves
that the retained DataFusion 51 general path has the same defect. Its first
expression-simplification pass subtracts list elements as ordinary sets and
removes the NULL condition, admitting four rows from the six-row fixture.

The earlier statement that the 51 reference preserved the result was too broad:
it described the mixed-type metadata fixture only. That control passes under 51,
while the generic same-type fixture fails. The rejected release's coercion change
and this existing optimizer defect are distinct findings. Do not attribute the
entire difference to an engine upgrade or replace the failing generic control
with the passing metadata case.

The source trace matches upstream [issue #24246](https://github.com/apache/datafusion/issues/24246)
and [correction #24258](https://github.com/apache/datafusion/pull/24258), merged
on 2026-09-07 as `46bbf0db1accffb81822e4a7a374818b36b70def`. The cached web
page initially showed an open proposal; a fresh official API check confirms
the merge. The correction assessment must preserve unknown results in projections and
under negation as well as row filtering, and must account for nullable and
runtime-valued list items. The selected optimizer backport covers the expression simplifier and filter
elimination changes, keeping unrelated function changes separate. Set operations
also require literal list entries whose value equality supports the structural
comparison; runtime-valued expressions must not be treated as distinct constants.
The reviewed implementation preserves the failing control and adds projection,
negation, runtime-value and filter-pruning regressions with known expected results.

One intermediate verbal report also reversed reference/local direction for the
mixed-type NULL case. The retained raw logs resolve that error. Known expected
results remain the authority when both local and reference providers share an
optimizer defect. All available investigation sources and outputs remain
retained, with missing original diagnostic streams identified below.

The rejected experiment selected Arrow/Parquet 58.4.0 and parser 0.62.0. Shared
Tokio, indexmap and libc also change because the new engine requires versions
above the baseline's locks. Existing storage compression and consensus hash
crate versions remain present alongside additional engine versions; a lockfile
that contains multiple versions must not be summarized as replacing all users
with the newest version. The native Zstandard wrapper version changes while its
bundled native library remains 1.5.7. Those dependency changes are not retained in the backport.

The rejected release's new lazy-generator reset contract required fresh per-execution
cursor state. Immutable selected row IDs must be shared across resets instead
of copied in full. Cancellation and captured selections must retain their
original ownership. The parser review includes table-alias and wildcard fields
as well as the top-level selection fields; successful syntax-order equivalents
must remain equivalent rather than acquire unnecessary rejection guards.

The source review distinguishes syntax-only information from semantic options:

| Parser surface | Compatibility disposition under the configured generic dialect |
| --- | --- |
| Selection flavor and explicit alias marker | Record existing syntax ordering/spelling; preserve successful equivalents. |
| Optimizer comment hints | Preserve comment acceptance; the engine does not implement these selection hints. |
| Selection modifiers | Specialized-dialect semantic options; the configured dialect cannot emit them. Native eligibility remains conservative if presented internally. |
| Hierarchical selection | Representation changes from an optional node to a vector; continue rejecting any nonempty form. |
| Table-alias positional binding | Requires the specialized PartiQL dialect; not reachable through the configured parser. |
| Wildcard alias | Requires a specialized dialect and is explicitly unsupported by the engine; native wildcard checks already compare the complete options value. |
| Multiple expression aliases | The configured parser can produce this form; defer to the engine's explicit unsupported error. |
| Array cast marker | Keep it outside exact native integer-cast eligibility. |

These are source dispositions, not claims that every specialized-dialect field
has a runtime reproducer through LogEx. The backport retains parser 0.59, so these migration adaptations are not part
of the accepted source. Existing eligibility and complexity regressions must
pass unchanged.

## Focused implementation review

The reviewed candidate passes thirteen focused tests, including the original
generic NULL failure, native exact-aggregate residuals, outer negation, nullable
constant-true projection/filter behavior and volatile/fallible filter branches.
Relevant aggregate, syntax, metadata, predicate, limit and library suites pass,
as do focused strict Clippy, workspace formatting and the provenance verifier.
Implementation `8b7812bb` passes all six required local gates: 1,034 workspace
tests with 18 expected exclusions, documentation checks and the release node
build. Extra release checks pass 139 query tests with seven expected exclusions
and both protocol checks. The initial release comparison is complete; its
tail-latency differences require the confirmation described below.
Validation hashes cover 213 source, configuration and fixture
files, including both complete backports and the verifier.

Review removed whole-list cloning and set construction before eligibility is
known. Difference and intersection require matching scalar types and safe
literal entries; deterministic union retains all values and therefore has a
separate volatility check. A rejected operand must retain its original
expression. The filter traversal uses the existing recursive-protection feature,
and the adaptation uses the older engine's checked filter constructor.

The filter backport also recognizes a negated membership condition containing
NULL as unable to admit a row, but only when its operands can safely be skipped.
That additional local rule is confined to positive filter contexts; projection
and negation semantics remain unchanged. Both new recursive traversals use the
existing protection feature.

Some original optimizer unit fixtures combined a text column with integer
literals. Their simplification assumptions did not satisfy the new exact-type
requirement. The affected fixtures now use matching types and explicit nullable
expectations, with separate regression coverage for actual type mismatches.
Production eligibility checks remain strict.

The initial isolated upstream test attempt could not resolve an uncached test
dependency offline. After fetching the package's test dependencies in an isolated
copy, its affected simplifier test and all six existing filter-elimination tests
pass using the published lockfile. The complete optimizer library suite also
passes all 571 tests. This supplementary check does not change the application
dependency graph; workspace integration separately exercises the application
versions of the engine dependencies. One earlier compiler
attempt retained stdout but lost its original stderr after context compaction.
Its failure and available source remain recorded, with an explicit missing-log
note; no later rerun is represented as the original diagnostic.

## Acceptance protocol established before measurements

Build the merge baseline and candidate from separate exact source archives,
using identical compatible grouping and established benchmark fixtures. Refresh
source timestamps when sharing the dependency target, require freshly compiled
test artifacts and copy each executable immediately. Require distinct executables
for targets that include the changed query dependencies. The standalone storage
publication target has no changed production dependency and may be byte-identical;
record that identity explicitly instead of requiring an artificial binary change.
Record both lockfiles,
toolchain, hardware, operating system, filesystem, source and executable hashes.
Expected baseline failures and complete candidate results precede measurement.

Use five alternating baseline/candidate process pairs for unchanged metadata,
REST handler and general-engine result workloads. Retain every sample and
process memory observation. Include the existing dense/sparse storage, index,
compaction, reopen and concurrent-query fixture, and the combined canonical/
historical publication fixture with a retained header window. The latter includes
the final checkpoint and independent clean-reopen oracle. These local workloads
do not measure live-peer throughput or establish whole-node staging acceptance.

The original seven-workload schedule has 37,500 latency observations and 70
process memory observations. Before any measurements, the optimizer correction
extended that schedule with four unchanged-result membership/filter-pruning
controls, each repeated 50 times in five alternating process pairs. Three use
small literal lists; the fourth uses two approximately 1,000-entry lists to
measure the new per-literal eligibility checks and allocation behavior. The added
workload contributes 2,000 latency and ten process memory observations; the
combined acceptance schedule therefore retains 39,500 latency and 80 process
memory observations. Queries whose baseline results are incorrect serve as
correctness regressions and cannot establish an equivalent-work performance gain.

General-engine and mixed storage/query workloads
use 20,000 rows, both sparse and dense block distributions, 8,192-row segments
and 4,096-row write batches. The publication workload uses 128 measured blocks,
128 rows per nonempty block, every sixteenth block empty, rich headers, mixed
payloads, an 8,192-header cache and both live and historical routes. It uses the
existing bounded checkpoint policy, with final checkpoint costs included.

Measure equivalent release builds sequentially, without concurrent Cargo jobs.
Keep raw logs, attempt history, source/fixture identities, disk/write counters
where emitted, and both per-process and pooled summaries. Investigate repeatable
changes above 5%; reject repeatable regressions above the user's 10% ceiling.
Any confirmation must keep the initial observations and use a predefined paired
schedule rather than replacing outliers. Run all six repository gates and the
additional release query/protocol checks on the accepted source, followed by
exact-head Linux/macOS CI before merge.

## Initial measurements and confirmation

The exact-archive release fixtures reproduce eight failing baseline regression
groups while preserving five passing controls. All thirteen groups pass on the
candidate, and all six identifier controls pass on both revisions. The complete
initial schedule retains 39,500 latency observations and 80 memory observations.

Initial latency medians remain within 2% across every workload. However, several mixed
storage-operation p95 values rise substantially: dense live/historical ingestion
are +91.23%/+107.39%, and sparse live/historical ingestion are +84.34%/+105.78%.
Index construction, compaction, reopening and concurrent queries also exhibit
large intermittent pauses. These observations are retained and cannot be
described as accepted performance merely because their medians are stable.

The standalone publication control provides independent evidence of measurement
variation: both copied executables have the identical SHA-256
`5b16de9d08916baaa6d8b6ffeae38438e8479fd63604e5d2bd04d261152e3146`, yet its
initial live/historical p95 differences are +30.54%/+26.70%. This target contains
no changed production dependency. Both labels run the same machine code with
the same workload parameters. This establishes substantial run-to-run variation;
it does not establish that all differences in the changed query target are noise.

Membership control medians remain between -0.90% and +0.72%, but the long-list
p95 rises 5.65% and the nullable-pruning p95 rises 10.77%. They also require
confirmation. Metadata, REST and standalone dense/sparse engine query changes
remain below 5% without further measurement.

A fixed confirmation plan was recorded before its measurements: twenty process
pairs each for dense mixed work, sparse mixed work, publication and membership.
Parameters and already-copied executables remain unchanged. Each workload has
ten baseline-first and ten candidate-first pairs in a recorded seeded order,
avoiding a decision about order based on intermediate results. This adds 16,000
latency observations and 160 memory observations. Report both confirmation-only
and all-sample combined statistics; retain every initial observation. Do not
repeat measurements until a threshold passes. Confirmation is complete, and all
55,500 revision-comparison latency observations and 240 memory observations have
been independently verified against their original logs.

The large storage tail differences do not repeat at their initial magnitude.
Combined dense live/historical ingestion p95 becomes -2.18%/+1.94%, and sparse
live becomes -4.05%. Sparse historical ingestion remains +13.70% combined,
versus +6.12% in the confirmation alone; keep both values visible. Combined dense
concurrent-query p95 is +7.13%, versus +11.86% in confirmation alone and +26.08%
initially. These differences require explicit disposition, not removal of
unfavorable observations. The byte-identical publication control's combined live
p95 is +6.87%; its historical p95 is -0.10%.

Read-only call-path review confirms that the mixed fixture's native queries and
preceding count/ordered queries do not invoke either patched optimizer function.
They use unchanged native handlers, and all worker threads are joined. The
concurrent timer excludes spawn calls but starts before the main barrier wait;
worker readiness, scheduling, query execution and joins remain inside the
measurement. Large concurrent pauses sometimes coincide with large delays in
other operations in the same iteration, on both revisions. This supports a
pipeline/environment explanation but does not itself certify performance.

Two bounded diagnostics are fixed before execution. Reuse the committed PR #135
isolated native-query/reopen fragment with its original 200,000-row dense/sparse
fixtures, five alternating process pairs and thirty samples per metric/process.
Separately run twenty balanced pairs of the same candidate executable against
the unchanged mixed sparse fixture to assess variation without a code difference.
The latter uses scheduling labels A/B, not different revisions. Neither
diagnostic replaces or is pooled into the original revision comparison. Both
diagnostics are complete, retaining another 4,800 latency observations and sixty
memory observations. Total retained evidence contains 60,300 latency observations
and 300 process memory observations, explicitly separated by purpose.

## Performance disposition

The new query-planning checks remain within the user's 10% budget on equivalent
queries. Combined membership medians range from -0.13% to +0.94%; their maximum
positive p95 change is +5.01% for the small literal-intersection control. Its
initial/confirmation p95 changes are +3.89%/+7.35%, with combined median +0.06%.
The approximately 1,000-entry control is +0.94% median/+1.47% p95. Preserve the
small-list tail observation as an explicit measured cost/uncertainty; this is a
correctness fix, without a claimed query-speed improvement.

| Workload | Combined median change | Combined p95 change |
| --- | ---: | ---: |
| Dense live storage ingestion | -0.86% | -2.18% |
| Dense historical storage ingestion | +0.07% | +1.94% |
| Sparse live storage ingestion | -1.22% | -4.05% |
| Sparse historical storage ingestion | +0.09% | +13.70% |
| Live publication, byte-identical executables | +0.15% | +6.87% |
| Historical publication, byte-identical executables | -0.29% | -0.10% |
| Dense concurrent native queries | +0.63% | +7.13% |
| Sparse concurrent native queries | -0.33% | -0.51% |

The separate 200,000-row isolated control measures dense/sparse concurrent-query
medians at +0.83%/+0.48% and p95 at -9.29%/+9.01%. Its reopen medians are
+2.41%/+0.31%, while p95 is +15.84%/+5.25%. These are separate workload results,
not replacements for or samples pooled into the table above.

The same-executable mixed sparse calibration has historical ingestion median
+0.07% and p95 -2.22% between its A/B scheduling labels. Its count-query median
is -0.51% but p95 -25.70%, despite identical machine code and inputs. Together
with the byte-identical publication control and the observed simultaneous stalls,
this demonstrates that this host's tail measurements can vary substantially
without a code difference. It does not prove that every measured difference is
noise, or establish all storage tails below 10%.

Review found no changed ingestion, indexing, native-query or reopen operations,
dependency versions or work added along those paths. The substantial initial
storage tails do not reproduce at their original magnitude; the changed planner
paths meet the budget. Proceed with the correctness backports without speculative
changes to unaffected storage operations. This disposition does **not** accept a
known source-induced regression above 10%, or claim uniformly bounded tail
latency. Keep the sparse historical, isolated reopen, small-list and other >5%
observations in integrated acceptance with controlled workloads and staging.
Actual live-peer throughput remains unmeasured here.

## Retained validation evidence

- [Local gates](baselines/2026-09-13-sql-grouping-aggregates-validation.json):
  complete logs, test counts and 213 unchanged source/configuration hashes.
- [Release comparison](baselines/2026-09-13-sql-grouping-aggregates-release.json):
  exact source, fixture, lockfile and executable identities, expected baseline
  failures, all initial measurements and linked compressed original logs.
- [Fixed confirmation](baselines/2026-09-13-sql-grouping-aggregates-confirmation.json):
  prospective ordering, all 16,000 additional samples, phase-specific and combined
  statistics, source-record correspondence checks and the verification script.
- [Separate diagnostics](baselines/2026-09-13-sql-grouping-aggregates-diagnostics.json):
  retained fixture provenance, fresh build records, fixed protocols, every
  isolated/same-executable observation and independent verification.
- [Investigation history](baselines/2026-09-13-sql-grouping-aggregates-before.json.gz):
  rejected implementations, failed attempts, available original source snapshots,
  upstream package test results and explicit missing-original-log limitations.

No timing sample was removed. Every compressed artifact was decoded and checked
against its original data. The unchanged root source passes the required local
gates; the final PR head must also pass all six Linux/macOS CI jobs before merge.

## Review boundaries

Trace native aggregate eligibility, fallback logical grouping/projection,
physical planning and the lazy provider. Compare complete planning and collection
with an independently populated in-memory table and known results. Use tiny
finite fixtures, covering neighboring expression/alias/group combinations where
they explain the cause. Preserve exact-integer data aggregation and existing
supported-query and explicit-error contracts.

Keep the payload-free visitor error kind and existing query bounds established
by PR #145. Preserve every reproduction, failed assumption and source/fixture
identity. A correction must pass focused checks, all required gates, appropriate
release/reference/performance acceptance, exact-head Linux/macOS CI and merge.

Mixed aggregate output contracts, broader query resource/cancellation behavior
and the other subsystem batches remain open. Actual live sync and staging follow
completion of the offline audit.
