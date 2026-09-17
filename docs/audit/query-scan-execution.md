# Query scan execution ownership

Base: `8c881b94` (merged PR #213). The remaining lazy-cursor review now has
an actual public-query reproduction. Source `86c7e6f8` passes all nine local gates. PR #214 is merged;
this is a scoped query correction, not completion of the query audit.

## Finding B7-30

**P1: a supported recursive query silently loses later iterations.** A bounded
recursive query starts at one, advances once for a matching stored log and stops
at four. Its independently expected output is one, two, three, four. The original
public LogEx path returns only one and two, both with one storage partition and
with three. Ordinary repeated CTE branches, self/cross joins and repeated scalar
subqueries pass their finite controls. Stored data itself is not lost.

The first fixture incorrectly assumed a tiny write sealed a partition; a second
attempt used a recursive column alias the pinned planner did not resolve. Those
are retained fixture/planning failures, not the cursor reproduction. The third
attempt reaches execution and asserts the wrong result. Exact source snapshots
and original logs distinguish these stages.

The pinned engine's lazy scan stores a mutable generator inside the physical
plan. Executing that plan again shares its exhausted cursor. Recursive execution
resets its input subtree, but the old lazy leaf returns the same object from its
default reset path. The next iteration therefore sees no rows.

Public query calls normally create a fresh context, provider, DataFrame and
physical plan, so a manually repeated internal execution alone was insufficient
to claim this bug. The recursive public-query fixture establishes reachability.

## Correction and invariants

Use the pinned engine's existing `PartitionStream` / `StreamingTableExec` source
adapter. Planning captures the same selected row IDs, projection and pushed
limit. Each execution creates a fresh offset over the immutable selection;
resetting a plan or running independent streams cannot consume a sibling cursor.

Keep the original row-ID vector allocation behind a private `Arc<Vec<u32>>`.
Execution shares that selection; it neither copies all IDs nor replans against
newer storage. No query engine upgrade, vendor patch, stored format or ingestion
operation is needed. Use the already-resolved futures utility package and remove
the old query crate's now-unused direct lock dependency.

The empty source still produces the expected empty batch for each execution.
The existing append/compaction regression is adapted to the new stream API.
Cancellation remains checked between batches using the original captured
closure. Public execution still combines it with the read-view token and checks
view validity after success or error, so resets must not create a fresh snapshot.

The builtin adapter supplies cooperative polling. Its partitioning metadata is
unknown rather than round-robin; neither source advertises sorted rows. With no
local fetch, its internal baseline poll metrics differ from the old leaf. No
LogEx consumer of those internal execution-plan metrics was found; existing
query/status counters remain unchanged. Do not claim identical internal metrics
or a measured speedup. Correct recursive queries now process iterations that
the buggy source skipped; comparing those incomplete old results would not be
an equivalent-work performance comparison. General planning, projection, pagination and concurrent
execution controls must validate the adapter change.

## Validation

All nine local gates pass on final source `86c7e6f8`: vendor integrity,
workspace/vendor formatting, all-target compilation and strict Clippy, 1,861
workspace tests (24 existing ignores across 29 targets), documentation tests and
the release node build. The focused run passes 97 tests with two existing ignores.

Six public controls cover recursive rescans, CTE branches, joins, scalar
subqueries, concurrent public calls and an empty recursive source. The adapted
8,193-row lifecycle control interleaves two streams from the same physical plan
across append/compaction, verifies all three captured batches, repeats and resets
the plan, then checks cancellation of active and freshly constructed streams.
Interleaving is deterministic; it does not claim simultaneous OS-thread execution.

[Validation record](baselines/2026-09-17-query-scan-execution.json),
[raw gate logs](baselines/2026-09-17-query-scan-execution-validation.json.gz) and
[investigation evidence](baselines/2026-09-17-query-scan-execution-evidence.json.gz)
retain commands, source identities, original failures and final controls. The
manifest distinguishes fixture errors from the genuine public reproduction.
Original manifests were recovered from the immutable base commit; the first
successful candidate's intermediate manifests were not separately saved. Exact
final manifests and sources were retained and compared. Final full gates include
the readability-only follow-up made after focused checks. All six CI jobs passed on `2d2867d2`, including ten Linux volume/template cases with verified cleanup. [PR #214](https://github.com/tdenisenko/logex/pull/214) merged as `657c99f4`. The merge tree is identical to the tested head.

The general path's per-scan `total_scanned` overwrite is a separate telemetry
lead, pending a precise counter contract and reproduction. Broader query memory,
aggregate output contracts, file lifetime and protocol review remain open. This
milestone does not establish release readiness or substitute for live sync.
