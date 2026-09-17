# Aggregate result contracts

Base: `657c99f4` (merged PR #214). This pass reviews the previously deferred
mixed exact/numeric result contracts and corrects aggregate name binding.
Source `c95ed325` passes 28 focused application tests and all 500 isolated planner
tests. All nine local workspace gates pass; exact-head CI/merge remain.

## Finding B7-31

**P2: distinct typed aggregates can collide despite distinct output aliases.**
On three rows, an ordinary integer sum alongside an explicitly decimal sum
should produce integer three and an exact scaled decimal three. The retained
planner instead rejects the aggregate schema as a duplicate unqualified field.
The native exact-data/literal combination passes its separate control.

The pinned expression display omits cast types from schema names. SQL planning
collects structurally distinct aggregate expressions without their outer aliases,
then builds a schema that requires unique names. This failure occurs before the
optimizer can preserve the projected aliases. The existing upstream
[typed aggregate naming issue](https://github.com/apache/datafusion/issues/3353)
documents the same structural problem. The local reproduction, rather than that
historical issue's current status, establishes the defect in this dependency.

The correction keeps aggregate expression identity distinct from its internal
output name, within the already-vendored planner. Direct ordering aggregates,
including those absent from the output, participate in the same binding for
queries that already aggregate. Noncolliding names and public aliases stay stable;
projection, aggregate predicates, windows and ordering resolve the matching
expression. No engine upgrade or public type change is included.

## Existing execution boundary

The native exact path evaluates a supported family of sums over log data,
integer literals and conditional choices, plus addition/subtraction between
those sums. It preserves arbitrary-precision signed totals and returns exact
base-ten strings. All aggregate projections in an eligible mixed exact/literal
sum query use that convention. This behavior was explicitly retained in PR #141.
A difference from a standalone ordinary numeric sum is not, by itself, a bug.

Ordinary numeric aggregation uses the retained SQL engine and its normal numeric
or decimal output types. The exact extension does not implement every aggregate
or query shape; unsupported shapes fall back to the general engine, where the
public data column is hexadecimal text. The controls confirm explicit errors for
unsupported exact-data mixtures, rather than silently coercing the text into
correct-looking numeric results. Existing text COUNT/MIN/MAX behavior is retained.

Native ordering and aggregate filtering operate on arbitrary-precision values
before string serialization. Empty/all-null behavior, negative results and
values wider than machine integers pass finite independent expected results.

## Consumers

REST forwards the query result JSON values directly. gRPC serializes each of
those values into its row JSON string without numeric coercion. Ethereum JSON-RPC
and WebSocket log subscriptions do not expose SQL aggregate execution. Generic
dashboard scalar formatting and CSV conversion preserve decimal strings.
Ordinary JSON-number precision in browsers remains part of the dashboard review.

README now documents the established exact-sum convention alongside normal
engine integer/decimal encoding. No protocol-layer coercion is added.

## Validation and scope

The current candidate passes 28 focused tests with two existing ignores: nine
new contract controls, six existing aggregate tests and thirteen grouping tests.
These include ordinary alias/ordinal sorting. Queries whose only aggregate
appears in ordering are explicit rejection controls: an exact baseline check
confirms those forms already failed, so no new capability is claimed.

The value-changing grouped control rejected an earlier candidate: fractional
and integer-cast sums correctly produced different output values, but direct
ordering still used the wrong aggregate. The final mapping covers ordering and
hidden aggregates as well as projection, filtering and windows. Equal-valued
ordering controls alone had failed to reveal this prototype defect. Original
attempts and exact source snapshots remain retained. The full planner suite also
caught an ordinary ordering/unparser roundtrip change in the prototype; restricting
ordering rebinding to actual collisions preserves that existing route. No snapshot
expectation was relaxed. Final isolated package validation passes 62 library and
438 integration tests using the published package lock. All 47 original package
files match the retained vendor copy; application tests separately use LogEx's
unchanged dependency graph. Vendor formatting and reversible patch checks pass.
The isolated build target was removed, reclaiming 2,840,580,096 allocated bytes.
All nine final-source workspace gates pass: 1,870 tests with 24 existing ignores
across 30 targets, documentation tests, strict Clippy, compilation, formatting,
vendor checks and release linking. There is no new benchmark campaign,
ingestion operation, persisted format, live sync or
Mac mini access. Broader query memory/admission and protocol lifecycle work remain
in their separate roadmap items.

## Follow-up B7-32: ordinary integer sum overflow

**P1: the retained ordinary aggregate kernel wraps fixed-width integer sums.**
A bounded two-row signed-64-bit control returns negative two for a mathematical
total of 18,446,744,073,709,551,614. The pinned aggregate kernel explicitly uses
wrapping addition; the current planner correction does not modify that kernel.
The first failing mathematical assertion ran against the naming candidate with
the unchanged upstream sum kernel. A later exact-baseline diagnostic also prints
the wrapped result; its diagnostic output is not mislabeled as an assertion.
Exact sources, the pinned kernel and both outputs are retained separately from
this PR's successful controls. They are not a passing regression or evidence that
ordinary overflow is safe.

Fix this in the next scoped task after the name-binding PR merges. Review scalar
and grouped accumulation, partial-state merging and result-type/error behavior
before changing the kernel. Exact native data sums already use arbitrary precision.
This remaining issue prevents closing the numeric correctness audit.

[Validation record](baselines/2026-09-17-query-aggregate-contracts.json),
[raw gate logs](baselines/2026-09-17-query-aggregate-contracts-validation.json.gz)
and [investigation evidence](baselines/2026-09-17-query-aggregate-contracts-evidence.json.gz)
retain the commands, source identities, attempts and mathematical expectations.
All original failures are classified separately from candidate compilation errors,
fixture corrections and diagnostic-only baseline output. Exact-head CI and merge
remain pending.
