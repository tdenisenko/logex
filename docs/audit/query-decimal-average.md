# Average arithmetic and window state

Review base: PR #216 merge `929a3639`, on `audit/query-decimal-average`.
Corrections are committed in `71652e41`; final source `51fed125` also applies
workspace formatting to the application regression file. All 44 focused
application controls and 108 isolated package tests pass. All ten local gates pass, including 1,886 workspace tests with 24 existing
ignores, documentation tests and the release build. Exact-head CI and merge remain pending.

## B7-36: decimal subtotal wrap (P1)

Three decimal values of 0.9 at precision and scale 38 produce a negative average
in the original scalar and grouped paths. A sliding window also produces a
negative mean for positive values, and distinct values 0.7, 0.8 and 0.9 produce
a negative mean instead of 0.8. These are public application assertions against
small fixtures, not conclusions inferred only from unchecked arithmetic in the
source. Original code, assertions and results are retained separately.

The scoped correction uses checked native running arithmetic and preserves the
existing final result types. It does not promise a wider mathematical subtotal
or recovery after a prior overflow. Output decimal precision remains checked.
Execution order, partial grouping and distinct-set iteration can affect such
boundary errors. Sliding execution adds entering rows before retracting departing
rows, so its temporary subtotal can exceed the backing width even when both
individual frame means fit. This is an explicit fixed-state limitation.
Average partial sums may legitimately exceed the input's declared precision:
two precision-two values of 90 must still average to 90.0000. The existing
internal state convention retains those native subtotals for direct merging;
they are not final values passed to JSON encoding. A SUM-style input-precision
check would incorrectly reject these ordinary averages.

## B7-37: last valid value leaves an average window (P1)

The public sliding-window fixture contains one valid decimal followed by two
nulls. After the valid value leaves the frame, the count reaches zero while the
accumulator retains a present zero subtotal. Evaluation attempts division by
zero instead of returning NULL. Original empty and all-null aggregate controls
already pass; they do not exercise this transition.

Adjacent public probes reach the same state invariant in the unchanged float
and duration implementations. Floating-point evaluation produces NaN, so an
outer SQL null check incorrectly returns false; JSON null encoding alone would
hide the defect. Duration evaluation divides by zero. The correction checks the
valid-value count and clears the subtotal when the last value leaves for all
three implementations. Normal nonempty floating-point arithmetic remains on
the existing path; duration subtotals are addressed separately below.

These two probes ran against a decimal candidate whose float/duration kernels
were still byte-for-byte unchanged from the base; that source snapshot and
provenance distinction are retained. They are not described as a complete
original-revision test run.

## B7-38: negative-scale average factors (P1)

The helper computes absolute powers of ten after casting signed decimal scales
to unsigned integers. A numeric cast into a supported negative-scale decimal
reaches a zero factor and division by zero. The bounded original fixture
averages 100 and 200. An earlier string-cast attempt was rejected before the
average function and is retained only as a fixture/capability observation.

The correction computes a checked relative scale factor and validates
division, without general accumulator widening. The supported result scale is
derived by the retained average type rule; final precision remains enforced.

## B7-39: grouped memory estimate omits native buffers (P2)

The grouped accumulator multiplies sum-vector capacity by the size of a type
marker instead of the stored primitive value. Its estimate also omits fixed
state and the existing null-tracking allocation. A tiny direct assertion on the
exact original implementation reports 32 bytes although the allocated buffers
require at least 160. The correction counts native element capacity, fixed
state, the existing null buffer and retained error text without scanning groups.
The physical aggregate operator
uses this estimate for reservations and peak-memory metrics. LogEx currently
uses an unbounded engine pool, so correcting this estimate does not establish
a complete query memory budget.

## B7-40: duration subtotal overflow (P1)

Once public duration casting was established, a two-value boundary fixture
confirmed a separate arithmetic defect. Scalar and grouped averages of two
maximum signed-64-bit durations return negative one when the final result is
cast back to an integer. The window path overflows during addition. The final
cast isolates the mean from duration display formatting.

The first probe reached a window failure before printing its accumulated scalar
and grouped mismatches. A second probe captures each task outcome and confirms
all three paths separately. These probes use the unchanged nonempty duration
arithmetic in the decimal candidate; complete source snapshots retain that
provenance. No claim of testing a complete original revision is made for them.

The correction retains duration units and signed-64-bit result/state while
checking subtotal arithmetic, count conversion and division. Scalar operations
stage changes, and duration groups use the existing checked vector path. The
same documented running-state limit applies; general widened means are outside
this correction.

## Preserved behavior and validation scope

Ordinary non-decimal numeric averages retain the engine's Float64 coercion and
rounding. Decimal results remain exact JSON strings with the declared output
scale. Native arbitrary-precision data sums and ingestion/storage paths are
separate. No live sync, remote host, production data or benchmark campaign is
part of this review.
Duration arithmetic has its own confirmed finding and controls; the empty-state
reproduction alone is not presented as evidence of subtotal safety.

The controls cover scalar/grouped/distinct/window arithmetic, null and filter
masks, partial-state merge, successful small-precision means, supported negative
scales, all four decimal backing widths and count conversions. Tiny metadata
controls exercise count boundaries without large allocations. Decimal backing
limits in direct tests are internal native-width controls, not claimed valid
public values at every declared precision. Bounded decimal DISTINCT window
retraction remains unsupported; cumulative DISTINCT controls pass. This change
does not add duration DISTINCT support.

## Implementation cost and cleanup

Decimal and duration scalar mutations stage counts and subtotals before
committing them. Grouped checked arithmetic is selected outside the row loop;
partially changed state is rejected after an error, including failed result
emission. Primitive vectors, null/filter handling, prefix emission and zero-copy
state conversion remain in place. Duration scalar operations borrow the typed
value buffer and null mask once per batch, retain the all-null fast path, and
select addition or subtraction outside the loop. No rollback vector copies or
new per-row dynamic array dispatch are introduced.

Checked arithmetic has a cost; no throughput percentage or improvement is
claimed. No ingestion/storage code, dependency version or persistent format
changes. Existing published vendor inventories and original manifests remain
intact, with reversible patches extended for the touched average files. The
verifier checks all 449 original files across nine packages; CI formatting covers
all five modified aggregate Rust files explicitly.

Removed the obsolete native-type import and exploratory probe files after
retaining their evidence. Source review found no new unsafe block or leftover
debug output in the touched code. The completed isolated package target was
removed (3,491,815,424 allocated file bytes); useful workspace artifacts remain.

## Validation evidence

- [Investigation and review bundle](baselines/2026-09-17-query-decimal-average-evidence.json.gz)
  retains 121 source, assertion, log and review files with individual hashes.
- The final application run passes 44 tests with two existing benchmark ignores
  across five targets. Isolated library suites pass 87 aggregate and 21 common
  tests against byte-identical final package sources. Their manifest override
  only points the aggregate package at its patched sibling; resolved versions
  and the root dependency graph are unchanged.
- An early candidate edit overlapped compilation; that run is retained but is
  not used as immutable final-source evidence. Subsequent frozen runs supersede
  it. The first whole-workspace gate stopped at test-file formatting; the
  mechanical correction leaves whitespace-stripped bytes identical, and the
  final focused run and workspace formatting both pass.
- The original single-row-window transition succeeds even though its temporary
  union subtotal exceeds native width. The final explicit error is a documented
  conservative-state tradeoff, not presented as a baseline correctness failure.

Full gate commands, source identities and outcomes are retained in the
[validation record](baselines/2026-09-17-query-decimal-average.json) and
[raw gate logs](baselines/2026-09-17-query-decimal-average-validation.json.gz).
