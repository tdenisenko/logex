# Metadata predicate semantics

Base: `99beaa83` (merged PR #142). Implementation `9cff21c5` corrects filters on
the two fixed metadata tables. Direct review, all six required local gates,
additional release checks and paired performance acceptance pass. All six
exact-head CI jobs passed; PR #143 merged as `2d443187`.

## Confirmed findings

- **B7-17 — P2, NULL predicates can select incorrect metadata rows.** The old
  evaluator collapses SQL unknown values into ordinary booleans, treats two
  NULL values as equal, and negates nonmatches for membership and ranges.
  Unknown must remain distinct until the final filter accepts only true rows.
- **B7-18 — P2, mixed text and numeric comparisons can silently miss rows.**
  A small independent reference reproduces a missing row for numeric metadata
  compared with its equivalent text value. Range and membership operands must
  use one common type, following the pinned engine's supported coercions.
- **B7-19 — P2, metadata expression validation depends on selected rows.**
  Identifier prevalidation from PR #142 remains necessary, but other unsupported
  expressions can be hidden in skipped Boolean branches or after an early list
  match. Validate the complete supported predicate before evaluating rows.

These findings concern metadata discovery results and error consistency. They
are not evidence of changed log persistence, consensus validation or ingestion
behavior.

## Implementation and review boundaries

The implementation keeps the existing metadata path and its simple projections,
filters and ordering. It prepares each supported predicate once, resolving column
positions, parsing its literal values and determining the common comparison
kind. The full identifier walk and its quoted-name/qualification rules remain.
Prepared predicates use three-valued logic and safe short-circuiting.
Unsupported grammar returns an explicit error even when no row would be returned.

The independent reference uses the pinned DataFusion 51 engine and a small
MemTable with separately specified public field values and types. In particular,
the all-NULL default-value column remains nullable text, and selected ordinal
positions include values above nine to distinguish lexical from numeric order.
Reference planning and result collection both participate in error checks.
An explicit three-by-three truth matrix checks both Boolean operators against
independently specified row counts. Separate counted cases cover a NULL range
bound with a true or false other comparison, and membership with a NULL plus a
matching or nonmatching value.

Direct source review checked the pinned engine's comparison coercion and its
common-type calculation across both range bounds and all membership values.
The reference cases and final before/after run validate those assumptions within
the existing supported metadata grammar.

## Validation and measurements

The [retained initial attempts](baselines/2026-09-12-sql-metadata-before.json.gz)
contain all four before-fix snapshot groups and their logs. The initial test
stops at numeric/text equality. A subsequent four-row fixture collects thirteen
incorrect-result or acceptance differences. Both precede the final seven-row
reference, eighteen Boolean truth cases and four independently counted NULL
range/membership cases. Historical filenames containing “final” describe those
phases; they are not final committed-source acceptance.

One detached comparison reused the post-fix shared-target binary and falsely
passed. That output is explicitly invalid correctness evidence and is retained.
The subsequent isolated rebuild was stopped before linking/execution; its log
is an interrupted non-result. Final acceptance uses refreshed source timestamps,
fresh artifact verification, separately copied binaries and expected baseline
failure/candidate success, while preserving all six earlier binding regressions.

The initial evidence is losslessly compressed: decoded SHA-256
`c895a8d8919518dc8d609d67b4ec610c106f61273edd3228d38b1fa09863a2b8`.
It includes exact source/test snapshots and original logs/provenance. The large
git archive is retained locally by hash and reconstructible from the base commit.
The [complete local validation record](baselines/2026-09-12-sql-metadata-validation.json)
binds 100 source/configuration hashes to committed source `9cff21c5`. All six
required gates pass: 1,012 workspace tests, 17 intentionally ignored,
documentation tests, formatting, workspace checking, strict Clippy and the
release node build. Additional release checks pass 117 query tests with six
ignored entries, and both protocol-consistency tests. These six query exclusions
are five explicit benchmarks and a child-process entry invoked by its normal
parent test.

The [final release record](baselines/2026-09-12-sql-metadata-release.json)
contains the complete final fixtures, runner, correctness logs, binary and
fixture hashes, environment and measurement summaries. The final regression
compares 41 supported predicates, plus one independently rejected type error and
five explicit unsupported-expression cases. It reports 17 baseline differences:
twelve incorrect row sets and five incorrectly accepted expressions. The same
fixture passes on the candidate. Both revisions pass all six earlier binding
regressions. Every source archive is based on its exact commit, receives identical
final fixtures and produces separately copied fresh binaries with distinct hashes.

Five alternating process pairs compare the unchanged three-shape metadata
benchmark and five-shape REST benchmark. Each metadata process uses one disposable
empty storage directory and one warmup per shape, then 1,000 rotating samples per
shape. Each REST process creates, indexes, compacts and reopens the same 20,000-row
fixture, warms each shape once and records 50 rotating samples per shape. Every
sample has an exact result oracle. There is no concurrent local build or test
during timing, and cache eviction is not forced. Measurements ran on Mac14,15,
16 GiB memory, eight CPUs, macOS 26.6.2, with the pinned `nightly-2026-08-24`
(Rust compiler commit dated 2026-08-23); full environment details are in the record.

| Workload | Median change | p95 change |
| --- | ---: | ---: |
| Table metadata | -0.71% | -5.09% |
| Filtered column metadata | -4.33% | -8.21% |
| Aliased column metadata | -4.37% | -8.55% |
| Metadata process peak RSS | +0.45% | +0.56% |
| REST engine aggregate | +0.04% | +1.10% |
| REST scalar expression | -0.34% | -2.95% |
| REST flat literal list | -0.32% | -3.69% |
| REST native count | -0.83% | -0.63% |
| REST native wide page | -0.27% | -2.18% |
| REST process peak RSS | -0.02% | +0.38% |

All positive latency/memory changes remain below the 5% investigation threshold
and 10% rejection ceiling. Metadata medians decrease 0.71–4.37% in this fixture;
small ordinary-query variations do not establish a general speedup. No failed
timing attempt, confirmation run or excluded sample precedes this acceptance.
The [complete measurement stream](baselines/2026-09-12-sql-metadata-final.jsonl.gz)
retains all 32,500 latency samples and 20 process-memory observations; the
[original process logs](baselines/2026-09-12-sql-metadata-run-logs.json.gz)
retain all twenty timed process outputs including counters. Both gzip files
round-trip exactly; compressed and decoded hashes are in the release record.
Decompress them with gzip, then read the first as JSON Lines and the second as JSON.

No production ingestion write, storage format, dependency or toolchain change is
included. These synthetic warm query measurements do not establish live-peer or
whole-node ingestion throughput. PR #142's separately investigated pooled metadata
p95 observation remains retained in its historical acceptance record.

The [final CI record](baselines/2026-09-12-sql-metadata-ci-final.json) confirms
all six jobs passed on final head `a3da0858`, run `34703886508`, including Linux
and macOS tests, without retries. [PR #143](https://github.com/tdenisenko/logex/pull/143)
merged as `2d443187` on 2026-09-12. Its merge tree exactly matches the reviewed
final head.

## Cleanup and remaining work

The old Boolean-only evaluator is replaced. Per-row AST traversal, identifier
normalization, field-name lookup and literal parsing are removed from predicate
evaluation. Direct search confirmed the shared unqualified-binary normalizer
still serves the native path, and the scalar sorter still serves metadata
ordering; both remain. No obsolete helper, unused import, dependency or unsafe
block was introduced. Wider query resources, other aggregate contracts, legacy
rewriting and CTE/table scope remain separate offline audit work. Actual live sync and
staging follow completion of the offline audit.
