# Named-subquery scope

Base: `2d443187` (merged PR #143). Implementation `b4522da9` corrects the
immediate table check for declared common table expressions (CTEs). Direct
review, focused checks, all six required local gates and additional release
correctness checks pass. Fresh release reference and paired performance
acceptance also pass; exact-head CI and merge remain required.

## Confirmed finding

- **B7-20 — P2, declared named subqueries can be rejected before planning.**
  The immediate outer-query table check recognizes physical tables but does not
  consider the current query's declared CTE names. An ordinary named subquery
  over the log table therefore fails as an unsupported table before the engine
  can resolve it. A three-row disposable fixture reproduces the failure on the
  exact preceding merge.

This is a query acceptance defect. It does not change persisted logs, validation
of ingested blocks, or coverage publication.

## Implementation and review boundaries

The check now collects normalized CTE names from the current query and accepts
them as one-component relations in the immediate outer body. It reuses the
existing identifier normalization: unquoted names normalize, quoted spelling
remains significant, and object-name components remain structurally distinct.
An ordinary query with no CTE does not allocate a name buffer.

Direct review of the pinned DataFusion 51 planner confirms that it plans WITH
definitions before the outer body. The engine continues to handle definition
order, recursive definitions, inherited/nested visibility and duplicate names.
The existing treatment of derived tables and set operations is preserved; this
fix does not introduce a second recursive name-resolution implementation.

The independent reference exposes only the public numeric block-number field
used by the cases. It has its own Arrow schema and in-memory table and checks
both planning and result collection. Ten successful cases cover ordinary,
mixed-case, quoted, dotted quoted, chained, nested, physical-table-shadowing,
joined, set-operation and finite three-row recursive definitions. Five cases
check missing or out-of-scope references, including forward references and
qualification. A separate case retains the pinned engine's rejection of nested
duplicate names; SQL-standard shadowing support is not assumed.

One explicit compatibility boundary is retained: the engine can stringify a
qualified relation like a quoted one-component alias containing a dot. LogEx
continues to reject that qualified relation at this immediate structural table
check. Deeper references remain subject to the existing engine planning rules.
This case records an intentional existing difference, not engine error equivalence. Ordinary log reads, metadata reads, read-only enforcement
and unknown physical-table errors remain covered by controls.

## Validation and measurements

The [retained initial attempts](baselines/2026-09-12-sql-query-scope-before.json.gz)
contain all fifteen original focused/build logs and saved baseline/final snapshots.
The first baseline run used the exact preceding production source and a retained
intermediate fixture. Two of three test groups fail: the first successful-query
case is rejected, and an invalid-reference case names the outer alias instead
of reaching the missing relation inside its definition. The third control group
passes. A group stopping at its first failed assertion does not establish the
outcome of its later cases.

The initial reference helper had an Arrow downcast compile error. It was fixed
before execution; that output is a test-build failure, not a runtime defect.
All intermediate attempt logs and their provenance are retained. Complete source
and fixture snapshots exist for the initial behavioral baseline and committed
final fixture; intermediate fixture stages are described without claiming that
every stage has a complete saved snapshot. The final focused fixture passes all three groups; focused checking, strict Clippy and formatting
also pass. The [full local validation record](baselines/2026-09-12-sql-query-scope-validation.json)
binds 101 source/configuration hashes to committed source `b4522da9`. All six required gates pass: formatting, checking, strict
Clippy, 1,015 workspace tests with 17 intentionally ignored, documentation tests
and the release node build. Additional release checks pass 120 query tests
with six ignored entries and both protocol-consistency tests. The workspace
exclusions comprise explicit benchmarks, four isolated-mount checks and a child
process entry that its ordinary parent test invokes. The release query exclusions
comprise five benchmarks and that child entry. This query-only change does not
repeat the separately recorded cross-mount storage acceptance.

The [release acceptance record](baselines/2026-09-12-sql-query-scope-release.json)
contains the complete runner, final fixtures, correctness logs, source/binary
identities and environment. Both exact-commit archives receive identical final
fixtures. Refreshed source timestamps force new artifacts, which must report
fresh compilation before being copied separately; corresponding baseline and
candidate binary hashes must differ. The final scope fixture reproduces two
failing groups and one passing control on the baseline, then passes all three
on the candidate. Both revisions pass all six prior identifier regressions.
Correctness finishes before timing; no build or test runs concurrently with the
measured processes.

Five alternating process pairs use the unchanged five-shape REST fixture. Each
process creates, indexes, compacts and reopens 20,000 synthetic rows, warms each
shape once and rotates 50 calls per shape. Every response has an exact expected
result. Cache eviction is not forced. The run uses Mac14,15, 16 GiB memory,
eight CPUs, macOS 26.6.2 and pinned `nightly-2026-08-24`; the full filesystem and
compiler details are in the release record.

| Workload | Median change | p95 change |
| --- | ---: | ---: |
| REST engine aggregate | +0.41% | +0.48% |
| REST scalar expression | +0.29% | +0.82% |
| REST flat literal list | -0.20% | -0.02% |
| REST native count | +0.74% | -0.89% |
| REST native wide page | -0.36% | -0.41% |
| REST process peak RSS | -0.37% | -0.25% |

All positive changes remain below 1%, below both the 5% investigation threshold
and 10% rejection ceiling. These small differences do not establish a general
speedup. No failed timing attempt, confirmation run or excluded sample precedes
this acceptance. Newly accepted CTE queries cannot be timed meaningfully against
a baseline that rejects them. The metadata path returns before the modified
check, so its previous accepted measurements are retained without an unnecessary
additional latency run. Ingestion writes are unchanged; warm synthetic query
measurements do not establish live-peer or whole-node ingestion throughput.

The [complete measurement stream](baselines/2026-09-12-sql-query-scope-final.jsonl.gz)
retains all 2,500 latency samples and ten process-memory observations. The
[original timed process outputs](baselines/2026-09-12-sql-query-scope-run-logs.json.gz)
and [all six fresh-build logs](baselines/2026-09-12-sql-query-scope-build-logs.json.gz)
are also retained. All compressed artifacts round-trip exactly, and summaries
were independently recalculated from raw samples. Compressed/decoded hashes
are in the release record. Exact-head CI and merge remain outstanding.

## Cleanup and remaining work

The original physical-table and metadata checks remain used. The change reuses
normalization and adds no public API, dependency, unsafe block, storage format
or ingestion write. The reference was reduced to its one necessary field;
unused all-column construction and conversion helpers were removed before the
final commit. No speculative recursive validator or replacement planner remains.

Optional syntax fields, mixed aggregate contracts, legacy rewriting and wider
query resources/cancellation remain separate offline review work. Actual live
sync and staging follow completion of the offline audit.
