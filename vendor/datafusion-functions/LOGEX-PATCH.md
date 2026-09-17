# LogEx DataFusion math simplification correction

This is the complete published `datafusion-functions` 51.0.0 package.
The workspace patches its source without upgrading any dependency version.

## Provenance

- Archive SHA-256: `794a9db7f7b96b3346fc007ff25e994f09b8f0511b4cf7dff651fadfe3ebb28f`
- Published VCS commit: `fd35a09438a2b4841431f5e86ffef378cbbda7c9`
- License: Apache License 2.0; original license, notice and package files retained.
- Original file inventory: `LOGEX-UPSTREAM-SHA256`
- Complete reversible source diff: `LOGEX-PATCH.diff`

## Changes and limits

Apache DataFusion #24247, merged as
`c08832d481cea2dcea98e43393e3dd640d421064`, establishes NULL guards for
logarithm and power identities. LogEx reproduces those wrong results in the
pinned engine. It also confirms that the upstream NULL-only remedy leaves
nonnullable zero and unit bases incorrectly simplified, and that dropping a
runtime base can hide an invalid cast.

The local correction therefore retains kernel evaluation for the logarithm
algebraic identities and both log/power inverse identities. It does not implement
a second numerical evaluator or assume exact real-number identities preserve
floating-point domains, overflow and rounding. Ordinary constant evaluation is
unchanged. Power's exponent-one identity remains; its exponent-zero shortcut
requires a non-NULL literal base so no runtime evaluation is discarded. Obsolete
inverse-recognition helpers and imports are removed. Return typing and function
kernels are unchanged. This is a conservative local extension of the upstream
NULL fix, not an unmodified backport and not a general numerical audit.

Workspace regressions in `crates/logex-query/tests/sql_expression_nulls.rs`
cover actual LogEx projections, NULL predicates, filters, negation, exact SUM
residuals/CASE, known non-NULL domain results and invalid casts. Upstream package
tests are retained; excluded vendor crates are not automatically tested by the
workspace gates. Run `python3 tools/verify_datafusion_vendor.py` to verify all
published files and the exact patch offline.

## Removal condition

Remove this override only when a maintained DataFusion release preserves these
NULL, domain and required-evaluation cases and passes LogEx compatibility gates.
No performance improvement or uniform timing bound is claimed for the retained
kernel evaluation.
