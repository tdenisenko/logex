# SQL expression growth and stack exhaustion

Base: `452d0874` (merged PR #139). This milestone bounds SQL input before
recursive parsing, AST traversal, cloning and destruction. It does not establish
a complete execution-memory budget or finish the remaining SQL/API audit.

## Finding

**B8-04 — P1, a small SQL request can abort the process.** Isolated child
processes executing `SELECT 1 + ... + 1` returned exact results through 1,250
terms on the tested release grid, then aborted from stack exhaustion at
1,500–5,000 terms. The 1,500-term input is roughly 6 KB, within normal API request
sizes. Both `13bd33cc` and read-only implementation `f73b316b` have the same
observed behavior; PR #139 did not introduce a changed threshold on this grid.
[Initial probes](baselines/2026-09-12-sql-limits-before.jsonl) and
[the fixed refinement](baselines/2026-09-12-sql-limits-before-refinement.jsonl)
retain source revisions, test source, runner, executable hashes and every result.
Those two production sources match their subsequent merge trees.

The pinned parser's recursion counter does not limit a long left-associative
AST chain. Checking an AST after it is built is insufficient: traversal or even
destruction can exhaust the stack. A first [256-syntax-token prototype](baselines/2026-09-12-sql-limits-256-prototype.log)
also failed its admitted boundary in a debug build. After reducing that bound,
generated input exposed a separate [nested-unary failure](baselines/2026-09-12-sql-limits-recursion-prototype.log)
with 52 NOT operators under the parser's default recursion allowance. These
failed prototypes are retained; neither is an accepted implementation.

## Correction and query contract

- Limit incoming SQL text to 256 KiB before legacy rewriting or tokenization.
  Recheck rewritten text because event/address literals can expand.
- Before constructing the first AST, allow at most 128 syntax tokens: identifiers,
  nonliteral keywords, operators, opening grouping delimiters and other syntax.
  Numeric/string/boolean/null literals, whitespace/comments, commas, semicolons
  and closing delimiters do not consume this complexity budget. Count identifiers
  as well as operators so implicit FROM joins and field/function chains cannot
  bypass it. Unknown/future token kinds consume the budget.
- Use a recursion allowance of 16 in that first parser. Subsequent parsing and
  execution are reached only after the same rewritten SQL passes these bounds.
  Some parser alternatives turn recursion exhaustion into an ordinary syntax
  error; preserve the original error with context stating the configured budget.
- Inspect borrowed tokens in the existing DFParserBuilder token stream, without
  advancing its cursor, copying literal payloads, or adding another tokenizer
  pass. Use the pinned parser's public APIs; no dependency or toolchain change.

Excessive input returns an explicit SQL error through the existing REST/gRPC
invalid-query responses. These are deliberate syntax limits, not a result-row
cap: no successful result is silently truncated. Large flat literal IN lists
remain usable within the text bound. Long/deep expressions may need simpler
conditions, IN lists, or separate queries. The new limits apply consistently in
debug and release, including native SELECT/count/custom aggregate paths.

## Validation and acceptance

The subprocess regression contains 21 cases: oversized arithmetic/boolean/cast/
null/set-operation chains, nested unary/subquery/function/grouping expressions,
implicit joins, raw text and legacy-expanded text. Positive controls include
maximum admitted arithmetic/boolean/cast/null shapes, 60-table joins on empty
storage, a 10,000-value literal IN list, operator-heavy strings/comments and text
exactly at the byte limit. A seeded set adds 144 expressions across six forms
and 144 incomplete variants; small valid expressions retain exact values, while
larger forms must either return their exact value or an explicit error. Every
child confirms a subsequent simple query still works; the parent enforces a
bounded timeout and reports aborts instead of losing the whole test suite.

The ignored child entry is invoked explicitly by the non-ignored parent, so the
normal workspace gate executes its cases. Focused debug regressions and all 66
existing query unit tests pass. REST/gRPC consistency tests include excessive
syntax/text, exact storage-tree immutability and successful subsequent queries.
All six required local gates pass: format, workspace all-target check/Clippy,
996 tests (14 ignored), documentation tests and the release node build.
All 66 query unit tests, the subprocess parent and both protocol consistency
tests also pass in release mode. [Equivalent release measurements](baselines/2026-09-12-sql-limits.md)
retain 250 samples per shape/revision: all positive latency/memory median and
tail changes are below 5%. No speedup is claimed. Implementation `af88a02b`
merged in PR #140 as `84a7889d` after all six CI jobs passed on evidence
`9f08dc00` (run `34693416605`, attempt 2). The first macOS dependency setup
failed to resolve its compiler version before tests; the unchanged-source retry
passed. Five ordinary dashboard-generated default/block/time queries with all
fields and combined filters also passed release acceptance in disposable data.

The existing release handler benchmark now also covers a 10,000-value IN list
and a maximum admitted addition chain, with independent exact JSON oracles.
Identical fixtures were used in baseline and candidate; native count/full
projection and DataFusion aggregate remain timing controls. Production ingestion
and storage write paths are unchanged.

## Cleanup and limits

Replace the first unbounded default parse with the existing builder plus bounded
pre-AST inspection. Retain the read-only AST and logical-plan policies, and the
remaining parsers reached only after this validation. No catch-panic workaround,
unchecked stack-size increase, new unsafe code or raw-text keyword scanner.

Execution memory, result materialization, query cancellation/worker ownership,
large scalar evaluation and integrated ingestion contention still need their
remaining audit. These input bounds do not prove a general resource sandbox or
complete SQL semantics. All tests use disposable data; no production, protected
volume or service access. Actual live sync and staging follow offline completion.
