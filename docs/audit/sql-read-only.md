# Read-only SQL execution boundaries

Base: `13bd33cc` (merged PR #138). This milestone checks whether public SQL
requests can execute a write-shaped operation despite the documented read-only
contract. It does not certify the remaining SQL resource limits, expression
semantics, authentication or subscription behavior.

## Finding and correction

**B8-03 — P2, SELECT INTO passes the outer read-only check.** The existing check
accepted any outer `Query` statement. Pinned DataFusion 51 translates a direct
`SELECT block_number INTO scratch FROM logs` into creation/materialization of an
in-memory table: the public query returned success with no result rows, after
scanning the logs. A literal-only SELECT INTO also succeeded. Inside UNION,
DataFusion discarded the INTO clause and returned ordinary query rows instead.
The [baseline reproduction](baselines/2026-09-12-sql-read-only-before.log) records
all four accepted forms. These tests demonstrate a read-only contract violation
and unintended work; they do not demonstrate persistent-file writes or an
external-file disclosure.

The already-parsed statement is now checked with the pinned sqlparser visitor.
Every nested statement must be a query, and SELECT INTO is rejected in all set
operation arms, CTEs, derived tables and expression subqueries. An explicit stack
walks set-operation arms; a simple SELECT needs no stack allocation. The visitor
handles nested queries and expressions. This catches clauses the engine might
otherwise discard instead of relying solely on the final logical plan.

DataFusion execution also uses its supported `SQLOptions` with DDL, DML (including
COPY) and other statements disabled. Its plan visitor includes subqueries and
runs before execution. Native selection/count/custom aggregate paths retain their
existing planners and already reject INTO-bearing SELECTs; they do not acquire a
DataFusion planning step. No new dependency, persisted format or ingestion write
is introduced. The pinned implementation and its `sql_with_options`,
`SQLOptions::verify_plan` and SELECT INTO planning code were inspected locally.

## Validation

- Seven direct/filtered/literal/nested/CTE/scalar-subquery/UNION SELECT INTO cases
  must fail through the public query entry point. The baseline already rejected
  some nested forms; the original accepted forms are explicitly retained.
- Valid aggregate, scalar subquery, CTE, UNION, native COUNT and ordered native
  selection return exact expected fixture values.
- REST and gRPC reject SELECT INTO, UNION/CTE forms, DELETE, CREATE and COPY with
  their existing invalid-query statuses. After each rejection, both protocols
  return the original count; REST ownership is released for the next request.
- The entire disposable storage tree (directory entries and all file bytes)
  remains unchanged after each attempted write and the subsequent reads. COPY
  names an additional path inside that same owned temporary directory.

All six local workspace gates pass: format, all-target check/Clippy, 995 tests
(13 ignored), documentation tests and release node build. All 66 query unit tests
and two protocol integration tests also pass in release mode. Equivalent release
handler measurements and PR/CI/merge remain. Performance acceptance must include native fast paths and a
DataFusion aggregate because the two checks run at different execution stages.

## Cleanup and limits

Replace the outer-only variant match and duplicated error construction with one
AST policy/error helper. Reuse the engine's existing plan restriction API; do not
introduce a second SQL execution engine or scan raw text for keywords. Quoted
strings/comments are interpreted by the parser. No obsolete public API is removed.

The AST visitor traverses parser-produced expressions; this milestone does not
establish a complete SQL length/depth/memory budget. Those limits, cancellation
and blocking-worker ownership remain in the query/API audit. The table whitelist
and custom SUM(data)/introspection evaluators also need their remaining semantic
review. Actual live sync and staging follow the complete offline audit; production
data, external volumes and services remain untouched.
