# LogEx checked SUM correction

This is the complete published `datafusion-functions-aggregate-common` 51.0.0 crate.
Local changes are a LogEx correction, not an upstream backport claim.

## Provenance

- Archive SHA-256: `62f4a66f3b87300bb70f4124b55434d2ae3fe80455f3574701d0348da040b55d`
- Package VCS commit: `fd35a09438a2b4841431f5e86ffef378cbbda7c9`
- License: Apache-2.0; published LICENSE, NOTICE, manifests, tests and sources retained.
- Original file inventory: `LOGEX-UPSTREAM-SHA256` (unchanged).
- Exact reversible local patch: `LOGEX-PATCH.diff`.
- Published Cargo.lock is provenance; the root lock governs application builds.

## Local behavior

Only `src/aggregate/sum_distinct/numeric.rs` changes. An opt-in checked
constructor rejects arithmetic and declared-precision overflow while evaluating
SUM DISTINCT. Existing `new()` consumers, including AVG DISTINCT and Float64
SUM, retain their original wrapping/floating loop. Hash-set iteration order can
affect which checked subtotal overflows. No public/state type changes.

Checked semantics apply to running states, not only the final mathematical
result. A subtotal, partial merge, DISTINCT hash-set iteration, or window
transition can fail even if later cancellation or retraction would make the
result fit. The window engine adds entering rows before retracting departing
rows, so even individually representable frames can encounter a rejected
transition. Types remain fixed; no general widened/deferred state is introduced.
Native LogEx arbitrary-precision data SUM is unchanged.

Public regressions are in `crates/logex-query/tests/sql_sum_overflow.rs`; direct
accumulator controls also cover merges, decimal storage widths, emitted group
prefixes, NULL/FILTER conversion, failure handling and retained memory estimates.
The corrected source files are checked explicitly by CI rustfmt because these
packages are excluded from the workspace formatter.

## Maintenance

Run `python3 tools/verify_datafusion_vendor.py` to verify complete inventories
and reviewed patches. Remove these overrides only when a maintained engine
release passes the checked SUM and existing SQL compatibility regressions;
validate successful types and the documented running-state error boundary.
