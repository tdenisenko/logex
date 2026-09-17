# LogEx DataFusion array membership correction

This is the complete published `datafusion-functions-nested` 51.0.0 package.
The workspace patches its source without upgrading any dependency version.

## Provenance

- Archive SHA-256: `ae5c06eed03918dc7fe7a9f082a284050f0e9ecf95d72f57712d1496da03b8c4`
- Published VCS commit: `fd35a09438a2b4841431f5e86ffef378cbbda7c9`
- License: Apache License 2.0; original license, notice and package files retained.
- Original file inventory: `LOGEX-UPSTREAM-SHA256`
- Complete reversible source diff: `LOGEX-PATCH.diff`

## Changes and limits

Backport the array-membership portion of Apache DataFusion #24258, merged as
`46bbf0db1accffb81822e4a7a374818b36b70def`, adapting only its simplifier-context
API to 51.0.0. A NULL element in an array is not an unknown match, whereas the
same NULL in an SQL IN list can make a nonmatch unknown. Keep the original
function when a literal array contains NULL or a constructed array contains an
unsafe nullable element. The local extension also retains the function for an empty list with a nullable
needle: `array_has([], NULL)` is NULL, while `NULL IN ()` is false. This kernel
contract is independently verified by direct UDF invocation.

A matching deterministic needle retains the existing
safe rewrite; syntactic equality of volatile expressions is insufficient.

Workspace regressions in `crates/logex-query/tests/sql_expression_nulls.rs`
exercise actual LogEx projections, NULL predicates, negation, nullable needles,
literal/runtime elements and exact SUM residual/CASE selection. Upstream tests
are retained, but excluded vendor crates are not automatically tested by
workspace gates. Run `python3 tools/verify_datafusion_vendor.py` to verify all
published files and the exact patch offline.

## Removal condition

Remove this override when a maintained DataFusion release contains the fix and
passes LogEx compatibility gates. Keep this patch confined to array-membership
simplification; no other nested-function behavior is changed.
