# LogEx checked SUM and AVG corrections

This is the complete published `datafusion-functions-aggregate` 51.0.0 crate.
Local changes are a LogEx correction, not an upstream backport claim.

## Provenance

- Archive SHA-256: `1c25210520a9dcf9c2b2cbbce31ebd4131ef5af7fc60ee92b266dc7d159cb305`
- Package VCS commit: `fd35a09438a2b4841431f5e86ffef378cbbda7c9`
- License: Apache-2.0; published LICENSE, NOTICE, manifests, tests and sources retained.
- Original file inventory: `LOGEX-UPSTREAM-SHA256` (unchanged).
- Exact reversible local patch: `LOGEX-PATCH.diff`.
- Published Cargo.lock is provenance; the root lock governs application builds.

## Local behavior

`src/sum.rs` changes. Fixed-width integer and decimal SUM use checked
running arithmetic, including partial merges and sliding retractions. Decimal
results are validated against declared precision. Grouped SUM retains a vector
layout and existing NULL/FILTER tracking; Float64 uses the original kernels.
Scalar/sliding batches stage sums and counts before commit. Grouped and sliding
DISTINCT accumulators poison partially modified state after errors.

Sliding DISTINCT uses the pinned hashbrown 0.14.5 Entry API for one key lookup
per change, retaining checked mutation and poisoning. It skips null slots,
returns NULL without valid values, and
estimates retained map allocation using peak usable capacity and the existing
bucket estimator. Its list state repeats keys to preserve multiplicity through
merge/retraction, with checked total/list-offset lengths and fallible reservation. A primitive
buffer avoids per-element ScalarValue conversion. This can enlarge
serialized state to the number of valid frame rows; production window execution
does not serialize/merge this accumulator. Non-Int64 sliding DISTINCT stays
unsupported. The state type is unchanged.

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

## AVG correction

`src/average.rs` also uses checked native arithmetic for decimal and duration
sums/counts, including grouped partial merges and sliding retraction. Duration
state/result remain i64 in the original unit. Float64 populated-frame arithmetic
and rounding retain their existing paths. Scalar batches stage changes; checked
groups poison partial state on update, merge, or emission failure. Count-zero
sliding frames return NULL and reset their sum. The same empty-state invariant
is corrected for Float64 and duration AVG: this prevents non-null NaN results
and division-by-zero respectively. Duration populated-frame arithmetic now
reports checked overflow, while Float64 populated-frame arithmetic is unchanged. All-null scalar batches retain
the cheap null-count fast path.

Checks constrain backing storage, not the input decimal precision: a partial
sum such as 180 for two DECIMAL(2,0) values of 90 is valid intermediate state.
The existing internal state type metadata is retained, and the engine consumes
that backing state directly rather than serializing it as final JSON. Final
averages still validate output precision. Successful output types and rounding
are unchanged. The checked running-state boundary above also applies to AVG,
including rejected transient window unions whose individual frames fit.

Grouped AVG size now includes native sum elements, counts, struct, retained
NULL buffers, and error-message allocation; this is an allocation estimate,
not a global query memory limit. Public controls are in
`crates/logex-query/tests/sql_decimal_average.rs`; private controls exercise
state, all decimal widths and duration units, count limits, masks, emission and
failure reuse. Nonempty duration arithmetic was reproduced as both wrapped
scalar/grouped means and a window panic; it now returns an AVG overflow error.
Duration DISTINCT support is unchanged; no new numeric family is introduced.

## Maintenance

Run `python3 tools/verify_datafusion_vendor.py` to verify complete inventories
and reviewed patches. Remove these overrides only when a maintained engine
release passes the checked SUM, decimal AVG and existing SQL compatibility regressions;
validate successful types and the documented running-state error boundary.
