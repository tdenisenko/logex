# LogEx DataFusion SQL backport

This directory is the complete published `datafusion-sql` 51.0.0 crate from
crates.io. LogEx patches that release because the first maintained DataFusion
release with the required `GROUP BY ALL` fix also changes established SQL
coercion behavior. A guarded `NOT IN (..., NULL)` optimizer bug was found in
both release lines and is tracked independently from that compatibility choice.

## Upstream provenance

- Package: `datafusion-sql` 51.0.0
- crates.io archive SHA-256:
  `3fc195fe60634b2c6ccfd131b487de46dc30eccae8a3c35a13f136e7f440414f`
- Package VCS commit:
  `fd35a09438a2b4841431f5e86ffef378cbbda7c9`
- Upstream license: Apache License 2.0
- Original file hashes: `LOGEX-UPSTREAM-SHA256`
- Exact reversible local diff: `LOGEX-PATCH.diff`

The published `LICENSE.txt`, `NOTICE.txt`, README, manifests, examples, source,
and tests are retained. The package's `Cargo.lock` is provenance from the
published archive; the workspace-level `Cargo.lock` controls LogEx builds.

## Local changes

`src/select.rs` contains the production backport from Apache DataFusion pull
request #20943, merged upstream as commit
`dfc8bb7dd65ba6a96b2a259dec5776945b361767`. The backport makes `GROUP BY ALL`
recursively detect aggregate functions and removes the now-unused `Alias`
import. A nearby modification notice records the local change as required by
the Apache License.

The SQL planner also contains a local correction for distinct aggregate
expressions with identical schema names (notably CAST/TRY_CAST arguments).
The upstream issue is https://github.com/apache/datafusion/issues/3353; this
change is a LogEx correction, not a claimed upstream backport. Only colliding
aggregate outputs receive unique internal aliases, reserved against input and
group output names. Structural expression matching rebinds SELECT, HAVING,
QUALIFY and ORDER BY through the existing planner; public projection names remain
intact. Hidden ORDER BY aggregates are collected when the query already has
aggregation; the nonaggregate planning path remains unchanged.
Noncolliding expressions retain their original names and borrowed rewrite path.
Public regressions live in `crates/logex-query/tests/sql_aggregate_contracts.rs`,
including ordering, rollups, windows, nulls and preserved duplicate-name errors.

The only other additions under this directory are this document, the exact
local diff, and the original-file hash inventory. Run
`python3 tools/verify_datafusion_vendor.py` from the repository root to verify
all complete packages and their reviewed patches offline.

## Removal condition

Remove this patch and vendor directory when LogEx adopts a maintained
DataFusion release containing #20943 and that release passes the full SQL
compatibility suite, including mixed numeric/text coercion and guarded
`NOT IN (..., NULL)` cases. The local aggregate binding correction must likewise
remain until a maintained release fixes #3353 and passes the aggregate contract
regressions.
