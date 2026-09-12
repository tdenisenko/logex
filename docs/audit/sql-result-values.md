# SQL result values and projections

Base: `4be5250c` (merged PR #134). This pass covers JSON output values and
projection completeness in native SQL, DataFusion and the fixed metadata tables.
It does not close the wider SQL binding/filter/resource audit.

## Confirmed findings

| ID | Severity and behavior | Correction |
| --- | --- | --- |
| B7-04 | P1, incorrect successful SQL values. Decimal arithmetic and narrow numeric, temporal and binary expressions returned datatype labels. Numeric arrays returned null; structs returned their debug type. | Convert supported Arrow values, preserving exact decimal strings and nested nulls; unsupported values return an explicit error. |
| B7-05 | P1, silent projection loss. Repeated native aliases overwrote fields, wildcard expansion dropped later projections, and unknown native columns became null. Metadata projections had the same duplicate/wildcard loss and validated unknown fields only while materializing matching rows. | Validate projections before executing or applying empty-result limits. Expand plain wildcards at their position, reject duplicate output names, and delegate log wildcard modifiers/unknown columns to DataFusion. Metadata modifiers remain explicitly unsupported. |
| B7-06 | P2, valid DataFusion queries failed. Selecting virtual `topics` requested a nonexistent physical column. This also blocked the proper fallback for wildcard modifiers. | Read the four underlying topic columns with deduplicated physical projections and build the existing virtual list. |

The [before-fix log](baselines/2026-09-12-sql-result-values-before.log) retains the
initial failures. The exact decimal fixture casts a string to isolate conversion
from SQL numeric-literal inference. The uppercase constant string and top-level
DataFusion duplicate-alias query are passing controls, not additional bugs.

## Representation and implementation

Common integers, Float64, booleans, strings and UTF8 lists still convert directly
to JSON values. Select the converter once per column, avoiding repeated dynamic
downcasts per cell. Reuse the pinned Arrow 57.3 JSON encoders for other supported
types with one reusable value-sized scratch buffer; do not serialize and parse
an entire batch. No new dependency, toolchain, storage format or ingestion change.

Decimal32/64/128/256 values use exact strings, including decimals nested in lists,
structs or dictionaries. Ordinary integer columns remain JSON numbers. Decimal
formatting follows Arrow, including scale and negative scales. SQL binary values
use hexadecimal without a prefix; core log hash/address/data strings retain their
existing `0x`. Dates/times use Arrow textual formats. Lists and objects retain
explicit nulls. Existing Float64 non-finite values remain JSON null.

Arrow's default dictionary encoder does not test whether a non-null key points
to a null value. The custom dictionary encoder preserves that logical null,
including NullArray and nested decimal values. Arrow's infallible JSON temporal
encoder can produce successful `ERROR: ...` text. Top-level temporal values use
the fallible formatter; nested temporal values are checked with a nonallocating
format sink before encoding. Validation follows the actual output values, so null
parents, unused dictionary entries and values outside slices do not cause false
errors. The same rule applies to duplicate map keys. Invalid dates fail the query. These safeguards were
verified against the pinned dependency source and dedicated regression fixtures.

JSON object field names must be unique: duplicate output fields, nested struct
fields and string map keys return errors. Non-string map keys and unsupported
Arrow encodings also return errors rather than loss or guessed conversions.
Null map rows are skipped when checking their keys. Conversion errors propagate
through the existing SQL error path; a partially converted result is not returned.

Removed the obsolete type-label/NULL fallback, per-cell batch converter,
allocating wildcard column-name helper and metadata `project_all` shortcut.
Metadata columns are planned once rather than reparsed for every result row.

## Validation and performance

Focused regressions pass for scalar/nested SQL expressions, real stored block
arithmetic, duplicate/unknown projections, wildcard additions/modifiers and
virtual topics. Array-level tests cover all four decimal widths, exact large
integers, UTF8/view/large-string layouts, null/empty/sliced list layouts, nested
dictionaries, map key errors, invalid dates, multiple batches and empty schemas.
A seeded 512-row nullable-list fixture is checked against an independent JSON
oracle, including sliced offsets. Tests use temporary storage only.

The initial candidate passed all six gates (976 tests, 11 intentionally ignored)
and the complete dense DataFusion comparison. Subsequent review reproduced false
errors from hidden temporal values. The correction also covers unused duplicate
map keys. All six gates now pass with 978 tests (11 intentionally ignored).
Final-source performance measurements remain; initial samples are retained and
will not substitute for that acceptance. The
tracked `benchmark_datafusion_result_values` fixture forces the general SQL
engine with arithmetic ordering and checks narrow/wide projections and aggregate
results against independently constructed values. Only queries with correct
baseline results are eligible for before/after performance claims. Previously
broken values are correctness tests, not favorable benchmark baselines.

The remaining offline audit, actual live sync and staging soak remain separate
acceptance gates. No production files, external volume contents or services have
been accessed by this pass.
