# Scalar expression correctness

Base: `4b0b1437` (merged PR #212). The remaining DataFusion #24246 leads
reproduce through LogEx's public query path. The scoped correction passes focused application and optimizer/math package
checks; all final-source local gates and exact-head CI pass; PR #213 is merged. This report does not close the
broader query audit.

## Findings

| ID | Severity and demonstrated behavior |
| --- | --- |
| B7-26 | P2: logarithm and power identities replace a nullable runtime argument with a non-NULL constant, changing projections and row selection. |
| B7-27 | P2: repeated XOR operands cancel even when NULL propagation or evaluation of a fallible expression must be retained. An invalid numeric conversion can become a successful zero result. |
| B7-28 | P2: rewriting array membership to SQL membership changes an unmatched array containing a NULL element from false to NULL. Negation and filtering consequently change too; an empty-list rewrite also erases a NULL needle. |
| B7-29 | P2: logarithm/power algebra assumes domains and exact inverse arithmetic that runtime floating-point functions do not guarantee. Nonnullable stored log indices zero and one produce different results when these shortcuts apply. |

These are query result/evaluation defects, not evidence of corrupted stored logs
or invalid chain verification. Tiny finite fixtures use known expected results.
The general query path and native exact payload aggregation both use the engine
simplifier, so both require controls. An independent table helps isolate the
engine cause but is not an independent oracle when it shares the same optimizer.

JSON represents nonfinite floating values as null. Tests therefore inspect
explicit NULL predicates and NaN predicates as well as the displayed result;
a JSON null alone cannot establish SQL NULL propagation.

## Selected correction

Keep the pinned engine and existing package-backport strategy. Add complete
copies of its already-used functions and nested-functions packages, and extend
the existing optimizer patch. Preserve licenses, original inventories, exact
local diffs and unrelated lockfile resolutions. This adds two maintained source
copies without introducing another engine or a second expression evaluator.

The upstream NULL corrections are the starting point:

- [Math identities #24247](https://github.com/apache/datafusion/pull/24247),
  merge `c08832d481cea2dcea98e43393e3dd640d421064`.
- [XOR cancellation #24248](https://github.com/apache/datafusion/pull/24248),
  merge `50cdbde1ed4b7cd9a0eb69443e22f09771cd16fa`.
- [Membership #24258](https://github.com/apache/datafusion/pull/24258),
  merge `46bbf0db1accffb81822e4a7a374818b36b70def`.

Fresh official API checks establish their merged status; a cached page can show
an earlier proposal state. The patch metadata distinguishes these backports from
local changes required by the additional domain/evaluation reproductions.

Nullable guards alone do not prove logarithm domains or inverse rounding safe.
Retain runtime kernel evaluation for the demonstrated unsafe algebra instead of
adding an independent domain solver. Remove the two repeated-operand XOR cancellation rules. A proposed column-only
guard was insufficient: common-subexpression elimination can first move a
fallible expression into a generated column, allowing later cancellation to
erase its evaluation. Retaining the runtime operator avoids a new provenance
analysis. Keep ordinary constant folding and the power exponent-one identity.
Power exponent-zero folding may only discard a non-NULL literal base.

Array membership retains its function when list elements have incompatible NULL
semantics, or an empty list could erase a nullable needle. Safe nonempty-list
rewrites remain. Exact final rule disposition is checked against retained
regressions and the source diff.

## Cost and review boundaries

The affected expressions may do more work because the old shortcuts skipped
required evaluation or returned the wrong result. This is a necessary correctness
cost; no benchmark percentage or whole-query performance guarantee is asserted.
The changes do not add work to live/historical ingestion or storage publication.
No broad benchmark campaign is warranted under the audit owner's updated policy.

Review also traced native SUM residual/CASE preparation and general planning to
the shared simplifier. Repeated lazy physical-plan execution remains a separate,
unconfirmed lead: source cursor ownership alone does not establish a public-query
failure. Broader aggregate contracts, query resource admission, protocol behavior,
and automatic offline repair remain open.

## Reproduction, cleanup and validation

Four original assertion groups fail before the first correction. Broader probes
then expose nonnullable domain behavior, lost conversion errors and empty-list
NULL behavior in intermediate candidates. Those attempts are retained rather
than replaced by the passing result. Exploratory enumeration/probe logs that print
incorrect values are distinguished from failing assertion regressions.

Seven final application tests cover projections, NULL predicates, negated
filters, nullable integer/floating power, nested XOR, literal/runtime/empty/NULL
arrays, a membership alias, known stored-row domains and conversion errors,
native exact SUM residual/CASE evaluation, and volatile expression preservation.
The empty-array contract is also checked by invoking the UDF directly without
an optimizer. Known exact payloads establish the aggregate expectations.

All seven regressions pass. Neighboring aggregate suites pass 19 tests with two
existing ignored benchmarks. The isolated optimizer library passes 571 tests;
44 isolated math tests and seven isolated array-membership tests pass. These excluded vendor packages use their published
locks for supplementary checks; workspace validation exercises the application's
resolved graph. Initial missing cached test dependencies were fetched only for
the isolated checks. No application dependency versions changed.

Removed the obsolete inverse-recognition helpers and repeated-XOR cancellation
helper, and replaced unit expectations that depended on the removed rules.
Temporary diagnostic printing is gone. Retained upstream crypto deprecation
warnings come from unchanged package code, not new unused imports. Parent review
independently compared package inventories to the published archive hashes and
verified that the lockfile changes only the two new package sources.

Source commit: `6d766f98`. Final-source workspace gates and fixture identities are recorded below;
PR #213 is merged as `8c881b94`. The nested-function
package initially lacked a schema in an existing simplifier test; supplying its
actual nullable column schema fixed the fixture without weakening the guard.
The isolated temporary build directory was removed after checks, reclaiming
4,446,824 KiB; logs, source copies and the useful workspace target remain.

All nine local gates pass on `6d766f98`, including 1,855 workspace tests with
24 existing ignored tests across 28 targets, documentation tests and the release
node build. [Validation record](baselines/2026-09-17-sql-scalar-expressions.json)
retains commands, identities and counts; [raw gate logs](baselines/2026-09-17-sql-scalar-expressions-validation.json.gz)
and [investigation evidence](baselines/2026-09-17-sql-scalar-expressions-evidence.json.gz)
retain original output and source-snapshot limitations. All six CI jobs passed on `7d836fef`, including ten Linux volume/template cases with verified cleanup. [PR #213](https://github.com/tdenisenko/logex/pull/213) merged as `8c881b94`.

Not every intermediate diagnostic fixture or failed package-source state was
snapshotted. The evidence manifest names those gaps, distinguishes printed
comparisons from assertions, and retains their original output. Final committed
source and passing package copies were compared byte-for-byte. These finite
controls do not constitute a general audit of every scalar or bitwise rule.
