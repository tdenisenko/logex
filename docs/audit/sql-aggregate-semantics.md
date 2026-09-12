# SQL aggregate predicates and exact values

Base: `84a7889d` (merged PR #140). This milestone checks custom exact-integer
aggregate predicates, CASE/null/cast behavior and default NULL ordering against
an independent in-memory SQL table. Final source `a4f7032e` passes all local
workspace/release gates and repeated performance acceptance.
[PR #141](https://github.com/tdenisenko/logex/pull/141) merged as `f45f2640` after
all six CI jobs passed on evidence head `80659701`. Shared identifier binding,
mixed-output typing and broader query-resource work remain separate audit items
after this coherent correction.

## Confirmed behavior

**B7-11 — P1, custom aggregates return incorrect totals for ordinary predicates.**
The previous evaluator treats unhandled conditions as false, omits some numeric
columns, applies byte coercion to ordinary SQL strings and implements two-valued
logic where SQL requires NULL propagation. On six rows with amounts 10–60,
`SUM(data) WHERE TRUE` returns NULL instead of 210, and `CASE WHEN TRUE THEN data
ELSE 0 END` sums to zero. NULL tests, NOT, lists containing NULL, column comparisons
and ordinary remainder/LIKE conditions have further differences. The independent
reference uses separate numeric amounts and public SQL string/null columns;
it shares no LogEx provider, index, filter or aggregate evaluation.

The [initial reproduction](baselines/2026-09-12-sql-aggregates-before.json) retains
its base revision, test source, command and every difference. The current
66 predicate/WHERE/CASE comparisons pass after the first correction.

**B7-12 — P1, native aggregation discards cast requirements.** The old recognizer
matches type names by substring and discards precision, scale, integer widths
and fallible-cast behavior. It can return an exact data total where conversion
should fail or produce NULL. It also intercepts ordinary numeric aggregates and
changes their result representation. The correction explicitly recognizes only
the existing unbounded exact decimal conversion forms; standard numeric-only
aggregation and all other casts retain DataFusion semantics.

**B7-13 — P2, missing aggregate values sort in the wrong default position.**
The old comparator treats NULL as low, reversing the pinned engine's default
ascending/descending placement. With a limit, this selects the wrong group.
The independent grouped reference covers both directions and the limited result.

## Implementation

Prepare residual conditions and complete CASE selection trees through the
existing DataFusion expression APIs, against the public log schema. Evaluate
the residual selection first, then evaluate CASE only for the selected rows,
using batches of at most 4,096 rows. The engine selects exact-integer leaf
values; LogEx retains arbitrary-precision accumulation. Separate expression
occurrences are not combined merely because their text matches. Expression
errors propagate as SQL errors instead of silently becoming false. A plain
data-only SUM needs no DataFusion expression setup. No ingestion/storage writes
change. Implementation checkpoints `da965035`, `51e18324` and `a4f7032e` record
the correction, direct-review fixes and measured preparation improvement. Final
source passes all required local and release validation.

This reuses the existing engine's coercion and predicate behavior instead of
extending a second partial evaluator. The old evaluator and its unused helpers
were removed after checking their callers. Existing canonical parsing remains.
The general scan reads referenced predicate columns and then data and optional
address columns. Candidate selection already refines native constraints against
stored columns, with or without indexes, and applies captured/canonical row
boundaries. The union of candidate filters retains the common constraints.
Independent source review verified both checkpoint/index and raw fallback paths
perform that refinement, including canonical and captured row bounds. Removing
the redundant full-row read does not certify the integrity of derived index files.

## Retained investigations

The first correction evaluated every CASE condition in advance. Six ordinary
reference cases exposed this prototype's evaluation-order errors and existing
NULL, signed-value and simple-CASE gaps. A calculation in an unused branch, or
on a row excluded by the selection, must not run. The complete CASE expression
now remains with DataFusion so its branch semantics apply. The [failing diagnostic and corresponding source](baselines/2026-09-12-sql-aggregates-case-prototype.json)
were retained before further edits.

All initial performance evidence remains available:

- [Initial baseline](baselines/2026-09-12-sql-aggregates-initial-base.jsonl).
- [First prototype](baselines/2026-09-12-sql-aggregates-first-prototype.jsonl).
- [Projected-column prototype](baselines/2026-09-12-sql-aggregates-projected-prototype.jsonl).

These are preliminary sequential runs, with 50 measurements per shape on 100
and 20,000 rows. The first prototype's residual median increased 10.93% and
10.34%, respectively, so it was not accepted. Removing redundant full-row
materialization reduced those differences to +5.62% and -6.25%. However, the
20,000-row plain-path median/p95 also increased 9.38%/32.82% in that run.
That observation was unresolved at this prototype stage. Subsequent paired
measurements below resolve it; all prototype samples remain retained.

The [first release validation](baselines/2026-09-12-sql-aggregates-release-first.json)
records all six new regressions failing on the baseline and passing on
implementation checkpoint `da965035`. Its timing run stopped during baseline
warmup: the added nested-CASE workload required a predicate that the old evaluator
does not implement correctly. That workload is unsuitable for an equivalent
performance comparison. It was corrected while retaining its exact oracle.
Direct review also found a parenthesized-CASE compatibility omission and silent
handling of impossible selector IDs. Follow-up `51e18324` restores nested input
compatibility, reports selector inconsistencies explicitly, preserves quoted
integer leaves in exact-data CASE inputs and corrects the benchmark workload.
The first attempt remains retained. The [first equivalent paired comparison](baselines/2026-09-12-sql-aggregates-paired-first.jsonl)
uses `51e18324`, five alternating pairs per size and 250 samples per shape and
revision. Every exact value passes. On 100 rows, medians increase 6.33% for
conditional totals, 7.25% grouped, 8.58% nested and 9.10% residual; plain totals
change +0.15%. On 20,000 rows, the corresponding medians decrease
25.82%/26.13%/27.58%/10.19%, with plain totals -3.91% and median peak RSS -19.26%.
The small-query increases were below 10% but exceeded the 5% investigation
threshold. The full repeated evidence remains retained; the preparation work
and final comparison below resolve them.

The preparation investigation identifies repeated default context construction
as a fixed cost. A private template supplies a fresh query state without
rebuilding the default function registry and runtime for each expression pool.
Filtering and CASE compilation must share that one query state. The general
table-provider execution keeps its own context and captured storage snapshot;
the expression template never registers query tables or user state.

Direct review also reproduced a stable-function evaluation error: DataFusion
requires simplification before executing query-time functions. Its documented
expression pipeline requires type coercion before simplification. The candidate
now follows that pipeline using one query's execution properties. Bounded
regressions retain CASE selection/excluded-row behavior, actual stable
function evaluation and independent volatile occurrences. The [preparation investigation](baselines/2026-09-12-sql-aggregates-preparation.json)
retains each diagnostic, the stable-function failure, command/prototype
provenance and one earlier sample that was available only in tool output.
The final diagnostic measures 59.9 microseconds for context construction,
10.7 microseconds for a template snapshot and 43.5 microseconds for preparing
filtering and CASE together, versus 126.4 microseconds originally. These are
bounded 1,000-iteration averages, distinct from final paired query latency
acceptance. Final source `a4f7032e` passes 69 library tests, six aggregate tests,
focused Clippy and formatting. Its final end-to-end paired comparison passes
as recorded below.

## Final performance acceptance

[Final paired measurements](baselines/2026-09-12-sql-aggregates-final.jsonl) compare
base `84a7889d` with `a4f7032e`, using identical committed benchmark source,
fresh release artifacts with distinct hashes, five alternating pairs per size,
one warmup and 50 rotating calls per shape and process. Each shape has 250
samples per revision and size. Every independent expected value passes.
Hardware, pinned toolchain, internal APFS, dataset parameters, cache conditions,
binary hashes and the complete runner are in the record. No sample is excluded.

| Rows | Shape | Median change | p95 change |
| ---: | --- | ---: | ---: |
| 100 | Conditional total | -3.55% | -7.20% |
| 100 | Grouped conditional total | -2.52% | -4.48% |
| 100 | Nested CASE total | +0.22% | -1.11% |
| 100 | Plain total | -0.05% | -4.20% |
| 100 | Residual filter total | +0.74% | -0.13% |
| 100 | Peak process RSS | +2.64% | +2.78% |
| 20,000 | Conditional total | -28.44% | -29.35% |
| 20,000 | Grouped conditional total | -28.82% | -30.40% |
| 20,000 | Nested CASE total | -32.36% | -34.14% |
| 20,000 | Plain total | -1.36% | -5.65% |
| 20,000 | Residual filter total | -15.88% | -19.42% |
| 20,000 | Peak process RSS | -18.85% | -19.57% |

The largest positive query median is +0.74%; every p95 decreases. The largest
positive process-memory difference is +2.78%. All positive differences are below
the 5% investigation threshold and the 10% rejection ceiling. The small-query
setup regression and the earlier plain-path tail observation are resolved by
the repeated comparison. Conditional/grouped/nested scans at 20,000 rows improve
28–32% at the median, with residual scans improving 16%; these gains exceed the
small-query variations in the same controlled comparison.

These are warm synthetic aggregate queries on indexed storage, with a separate
compacted/reopened correctness suite. Peak RSS includes fixture/reference setup
and allocator retention. The result does not establish live-peer throughput or
whole-node concurrent ingestion performance. No ingestion write path, storage
format, dependency or toolchain changes are included.

## Validation and remaining work

All fixtures are bounded, disposable local data. The ordinary-query release
baseline was captured before changing aggregate execution: 100 and 20,000 rows,
plain/conditional/residual SUM shapes, independent exact values, 50 samples per
shape and process memory. Those original runs remain retained.

Adjacent review also identified preexisting quoted-name/alias normalization and
mixed exact/numeric aggregate output questions. Shared identifier handling stays
in the wider binding review. The current milestone preserves successful exact
aggregate behavior. The existing mixed
exact/numeric native aggregate path emits decimal strings for every aggregate
projection; this milestone preserves that behavior while ordinary numeric-only
queries return the engine's normal types. Further type consistency is explicitly
open, rather than changing working mixed queries into errors.

The focused suite now passes six tests covering predicate and CASE references,
raw/indexed/compacted/reopened storage, invalid fields/types before empty or
zero-limit execution, cast behavior and grouped NULL ordering. Compatibility
controls cover unbounded decimal aliases, nested forms and quoted integer leaves.
[Final local validation](baselines/2026-09-12-sql-aggregates-validation.json)
records all six required workspace gates passing, with 1,005 tests passing and
16 intentionally ignored. The additional release query suite passes 110 tests
with five ignored, and both release protocol-consistency tests pass. Source
hashes bind all 98 relevant source/configuration files to `a4f7032e`. The six new
integration regressions fail on the baseline and pass on the final release
candidate; the retained stable-function diagnostic separately records its
pre-correction failure.

The obsolete partial predicate evaluator, generic comparison/hex helpers,
substring cast matching, redundant row materialization and repeated expression
context construction were removed after checking callers. Existing native
candidate validation, canonical parsing, snapshot and cancellation paths remain
used. No new unsafe block or dependency was introduced.

Implementation, local acceptance, exact-head Linux/macOS CI and merge are
complete. [CI run 34698630663](https://github.com/tdenisenko/logex/actions/runs/34698630663)
passed all six jobs on `80659701` without retries. The
[retained CI record](baselines/2026-09-12-sql-aggregates-ci.json) records the exact
head and job results. Merge `f45f2640` has identical source. The wider audit stays
open; actual live sync and staging follow offline completion.
