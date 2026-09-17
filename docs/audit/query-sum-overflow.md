# Ordinary numeric sum correctness

Source `52ce3bfd` on `audit/query-sum-overflow`, based on PR #215 merge
`8fd2e55f`, corrects the findings below. Focused validation passes 33 application
controls (two existing benchmark ignores) and 99 isolated package tests. All ten final-source workspace gates pass (1,875 tests / 24 existing ignores).
Exact-head CI and merge remain pending.

## B7-32: silent fixed-width overflow (P1)

The retained ordinary aggregate kernel returns a wrapped negative value for
some positive integer totals. The bounded two-row reproduction first retained
in the [aggregate contract review](query-aggregate-contracts.md) returns negative
two for a mathematical total of 18,446,744,073,709,551,614. The implementation
uses wrapping addition in more than one execution path; a scalar-only fix would
leave grouped, distinct and sliding-window calculations exposed.

The correction preserves established result and partial-state
types, and replaces unchecked accumulation with fallible arithmetic. The
contract rejects out-of-range running state, including partial sums;
it does not promise to recover a representable final total after an earlier
subtotal overflow. Input order, partitioning and distinct-set iteration order
can therefore affect whether such a calculation succeeds. Sliding windows add
new rows before removing old rows in the retained engine; a temporary transition
subtotal can overflow even when the actual old and new frames both fit. The
correction deliberately reports that condition rather than widening all temporary
accumulator states. This explicit error
contract avoids presenting a wrapped value as a correct result. Decimal declared
precision requires checks in addition to its backing integer width.

The native exact data-sum extension remains separate and preserves arbitrary
integer precision and decimal-string results. Floating-point accumulation and
existing rounding behavior remain on their original kernels.

## B7-33: null-only distinct window totals (P2)

The original sliding distinct accumulator iterates backing values without the
Arrow validity mask. It also returns numeric zero when no valid value remains.
The public baseline reproduces zero for a null-only window and after the last
non-null value leaves a window; both should produce NULL. A truly empty preceding
frame already passes through the window executor's separate empty-frame path.
That successful control is retained and is not mislabeled as an original failure.

## B7-34: decimal result precision (P1)

Decimal backing types can hold values outside the declared SQL precision. The
baseline accepts sums beyond precision 38 or 76 and renders misleading totals.
The fix checks declared precision as well as backing-integer overflow across
ordinary and distinct paths. This is a separate boundary from B7-32 integer wrap.

## B7-35: distinct-window state loses multiplicity (P2, internal API)

A direct accumulator control restores a window containing two copies of five,
then removes one copy. The original serialized state retains only distinct keys,
so the restored accumulator returns zero instead of the remaining five. The
first direct assertion ran on a candidate that already corrected null handling
and returned NULL; an exact original-source assertion separately confirms zero.
Both failures and their different source identities are retained. This is a trait-level
finding: the current physical window executor does not serialize or merge this
accumulator, and no public query failure is claimed for this path.

The correction retains the existing list state type and repeats keys according
to their counts. Serialization checks both the total count and 32-bit list-offset range before
reserving a primitive integer buffer fallibly. The existing list builder consumes
that buffer without an intermediate scalar object per value. This state can be larger when duplicates are present; the current
production window path incurs no serialization cost. Checked updates and
retractions retain multiplicity, and invalid or overflowing map operations
prevent subsequent partial-state emission.

## Existing error boundary

The ordinary query path collects all result batches before JSON materialization.
A calculation error propagates through `SqlQueryError`, before a successful result
is returned. REST maps the error to HTTP 400 and an error object; gRPC maps it to
`InvalidArgument` before row serialization. Request guard cleanup releases
admission on error. These are code-path observations at the base revision;
focused query controls and the final workspace suite pass. No
protocol implementation change was needed.

## Implementation and cost

Scalar and ordinary sliding batches compute into local sums and row counts,
then publish them only after success. Grouped and distinct-window operations
avoid cloning their vectors/maps for rollback; an error marks their partial
state unusable, so evaluation, state publication and later updates also fail.
Group emission, filters and null masks use the existing engine machinery.

Integer and decimal grouped sums retain primitive per-group vectors. Floating
sums retain the original scalar, grouped, distinct and sliding kernels and
rounding. Empty/all-null scalar and sliding batches return immediately. Shared
distinct-average consumers retain their old constructor and arithmetic; only
SUM opts into the checked helper. Decimal averages remain a separate review lead.
The window map now reports an allocation estimate using retained peak capacity
and the engine's bucket estimator; it is not a complete query memory limit.
The final refinement uses one entry lookup per value instead of separate lookup
and insertion/removal operations. Review against the actual hashbrown 0.14.5
dependency confirms that checked arithmetic precedes mutation and allocation
accounting is retained. No standard-library map behavior is assumed.

These are correctness changes with additional arithmetic/precision checks. No
performance percentage is claimed. There is no new ingestion, storage, consensus
or network operation. Concurrent query/sync contention remains part of integrated
validation. No broad benchmark campaign or Mac mini access was performed.

## Dependency provenance and cleanup

The two complete published aggregate packages remain at version 51.0.0. Their
archives match the original application lock checksums, and all 72 original file
paths match the retained upstream inventories. Only the sum implementation and
the shared distinct-sum helper change. Independent forward/reverse patch checks
produce the exact vendor files and restore their originals. Application lock
changes only replace registry locations/checksums with the two same-version
path overrides; package versions and dependency edges are unchanged.

Existing common infallible kernels and unrelated average callers remain in use.
The old fixed-width wrapping paths, null-backing-value iteration and key-only
window state encoding were replaced only where their behavior was incorrect.
CI now checks formatting for the two patched files explicitly because vendor
packages are excluded from workspace formatting. Disposable patch-review
fixtures were removed. Completed isolated build output was removed twice:
2,917,605,376 allocated bytes before the final map refinement, and 1,846,743,040
allocated bytes after rebuilding and validating it. These are separate cleanup
observations, not a measurement of net disk-space change. Evidence and the useful
workspace target remain.

## Validation and limits

The five new application tests contain overflow/error, window-null, explicit
transition-limit, state-restoration and successful-type/filter controls. They
pass alongside nine prior result-contract, six aggregate and thirteen grouping
tests: 33 passed, two existing benchmark ignores. The two isolated published
packages pass 81 aggregate and 18 common tests, including seven added direct
controls for partial merges, failure-state handling, emitted group prefixes,
null/filter conversion, all four decimal storage widths, retraction/counters,
retained memory and early list-offset rejection. The offset control changes
only tiny internal metadata; it does not allocate a large fixture.

Baseline assertions and rejected candidates are retained separately. The
successful original single-row window transition becomes an explicit overflow
error under the documented running-state contract; this deliberate behavior
change is not mislabeled as an original failing query. The exact original
state-restoration failure is separate from the earlier candidate's NULL result.
All ten workspace gates passed on the initial correction `d2d13a65`; those logs
remain classified separately from final-revision validation. Final-source focused
Clippy, formatting and complete-vendor verification pass. All ten final-source workspace gates pass, including 1,875 tests with 24 existing
ignores across 31 targets, documentation tests, strict Clippy, formatting, vendor
checks, compilation and release linking. Exact-head CI and merge remain pending.

[Validation record](baselines/2026-09-17-query-sum-overflow.json),
[raw gate logs](baselines/2026-09-17-query-sum-overflow-validation.json.gz) and
[investigation evidence](baselines/2026-09-17-query-sum-overflow-evidence.json.gz)
retain exact source identities, commands and attempt classifications.
