# Identifier and alias binding

Base: `f45f2640` (merged PR #141). Implementation `b81a9bb2` corrects native
query and introspection name resolution against the pinned DataFusion 51
planner. All six local gates, focused validation, direct review and paired performance
acceptance pass. PR, exact-head CI and merge remain in progress.

## Confirmed findings

**B7-14 — P1, case-insensitive identifier and alias matching changes results.**
Quoted identifiers must preserve spelling, while unquoted identifiers normalize
to ASCII lowercase. The former native helper lowercased both. It accepted
nonexistent quoted fields and qualifiers, including on empty or zero-limit
results. Explicit aliases retained their raw unquoted spelling, producing JSON
keys different from the engine and accepting duplicate normalized names.
Case-insensitive aggregate alias lookup selected the first of two distinct
quoted aliases for filtering or ordering, choosing the wrong aggregate value.

**B7-15 — P1, native ordering and aggregate filtering use the wrong name scope.**
A source-qualified reference cannot bind an output alias. The previous aggregate
recognizer discarded the qualifier and accepted that reference. Native select
and grouped-count ordering also used physical fields when an output alias had
the same name. Limited queries could therefore select the wrong row or group.
The engine prioritizes output fields for ordering and input fields for grouping
and aggregate filtering. Those contexts must remain distinct.

**B7-16 — P2, metadata queries validate and resolve names inconsistently.**
Metadata projections and filters used the log-table qualifier helper; an
unrelated qualifier could be accepted. Case-insensitive lookup accepted unknown
quoted fields. Sort fields were not checked when no rows remained, and predicate
short-circuiting could hide unknown names. Ordering ignored projection aliases,
including aliases that reused a different metadata column's name.

## Implementation and direct review

One identifier normalizer preserves quoted spelling and normalizes unquoted
names. It is reused for projections, filters, group fields, qualifiers and
explicit aliases. Object names retain their component boundaries: a quoted
name containing a dot is distinct from two separately qualified components.
Duplicate output names are checked after normalization.

Native ordering resolves an unqualified output alias before its input field;
source-qualified names retain source scope. The optimized physical order is
used only if that resolved field matches the supported ordering tuple. Grouped
counts similarly fall back to DataFusion when an output alias denotes the count
rather than its grouping field. Existing valid queries still execute. Exact
aggregate aliases bind by exact normalized spelling; invalid qualification and
input-field conflicts fall through to normal planning errors.

Metadata queries validate every expression identifier with the existing AST
visitor before evaluating rows. Projection and sort binding also use the fixed
schema, so empty data and zero limits do not skip validation. Sort aliases
resolve before source columns. Unsupported qualified metadata references return
an explicit error; this change does not add a new qualified metadata syntax.

Unquoted alias keys now follow the engine's lowercase convention. Clients that
need a mixed-case output key can use an explicitly quoted alias, whose spelling
is preserved. Requests with an invalid quoted field or qualified output alias
now return an error instead of an incorrectly successful result. Existing result
pagination and exact aggregate value representation are unchanged.

The first prototype normalized names but still discarded scope information.
Direct review found the qualified-alias issue, output/source collisions,
component-boundary loss and incomplete metadata validation. The final fix uses
context-specific binding and full identifier traversal. The
[retained investigation](baselines/2026-09-12-sql-identifiers-before.json)
distinguishes the five failing test groups on the true baseline from the later
qualification-only failure on that intermediate prototype. The latter stopped
at its first assertion and does not certify other cases. An earlier initial
confirmation was available only in tool output; its captured repeat is retained.

Final test fixtures use coherent increasing block timestamps and ordinary
addresses whose ordering differs from block ordering. Numeric reference amounts
are separate from encoded log data. Invalid reference cases use that independent
amount column so a missing data field cannot cause a false-positive rejection.
Planning and collection both participate in the reference error checks.

## Validation and performance

Six focused test groups pass, covering raw and indexed fixtures, distinct quoted
aliases, normalized duplicates, qualified references, input/output precedence,
metadata aliases and names, and validation on empty or zero-limit results.
The [final local validation record](baselines/2026-09-12-sql-identifiers-validation.json)
captures all six required workspace gates passing: 1,011 workspace tests with
16 intentionally ignored, documentation tests, strict Clippy, formatting,
workspace checking and the release node build. Additional release query checks
pass 116 tests with five ignored; both release protocol-consistency tests pass.
Hashes bind 99 source/configuration files to `b81a9bb2`. The
[final archive correctness record](baselines/2026-09-12-sql-identifiers-release.json)
confirms all six final regressions fail on the true baseline and pass on the
release candidate, using identical final test source and distinct fresh binaries.

Final acceptance uses identical committed reference and REST fixtures in fresh
archives of both exact revisions. Refreshed source mtimes, non-reused compiler
artifacts and separately copied binary hashes prevent shared-target confusion.
Correctness runs precede five alternating release pairs, retaining 250 samples
per shape and revision and process peak RSS. Every ordinary REST shape has an
exact expected result. The [complete final measurements](baselines/2026-09-12-sql-identifiers-final.jsonl)
retain all 2,500 latency samples and ten process-memory observations. The same
20,000-row fixture is created, indexed, compacted and reopened in each process;
one warmup precedes 50 rotating calls per shape. Caches are not forcibly evicted,
and no concurrent build or test runs during timing. The record includes exact
commits, binary/fixture hashes, runner, hardware, OS, toolchain and filesystem.

| Shape | Median change | p95 change |
| --- | ---: | ---: |
| Engine aggregate | +0.72% | -1.70% |
| Bounded scalar expression | +0.14% | -2.80% |
| Flat literal list | -0.06% | -1.43% |
| Native count | +0.50% | -1.76% |
| Native wide page | -0.23% | -1.65% |
| Peak process RSS | +0.01% | +0.13% |

The largest positive query median is +0.72%; every query p95 decreases. Peak
process RSS changes +0.01% at the median and +0.13% at its five-process p95.
All positive differences remain below the 5% investigation threshold and the
10% rejection ceiling. These small variations do not establish a speedup.
No failed timing run or excluded sample precedes this accepted comparison;
initial and intermediate correctness failures remain separately retained.

All fixtures are disposable local data. No production ingestion write, storage
format, dependency or toolchain changes are included. Synthetic warm handler
latency does not establish live-peer or whole-node ingestion throughput.

## Cleanup and remaining work

Removed raw alias output, case-insensitive field/alias lookup, flattened name
dispatch and the log-table qualifier assumption in metadata paths. Traced every
remaining identifier/binary helper and function-name comparison; shared literal,
aggregate, snapshot and cancellation helpers remain used. No unsafe code or
unused dependency was added.

Explicit group-key ordering for exact aggregates was already unsupported and
remains a separate capability question. Mixed aggregate output types, broader
query resource handling, metadata NULL semantics and legacy text rewriting
remain open offline audit items. PR, exact-head CI and merge remain for this
milestone. Actual live sync and staging follow offline completion.
