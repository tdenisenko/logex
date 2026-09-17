# LogEx DataFusion optimizer backport

This directory is the complete published `datafusion-optimizer` 51.0.0 crate
from crates.io. LogEx patches that release to preserve SQL three-valued logic
when the expression simplifier combines `IN` and `NOT IN` predicates or
cancels repeated XOR operands.

## Upstream provenance

- Package: `datafusion-optimizer` 51.0.0
- crates.io archive SHA-256:
  `9f35f9ec5d08b87fd1893a30c2929f2559c2f9806ca072d8fefca5009dc0f06a`
- Package VCS commit:
  `fd35a09438a2b4841431f5e86ffef378cbbda7c9`
- Upstream license: Apache License 2.0
- Original file hashes: `LOGEX-UPSTREAM-SHA256`
- Exact reversible local diff: `LOGEX-PATCH.diff`

The published `LICENSE.txt`, `NOTICE.txt`, README, manifests, benchmark,
source, and tests are retained. The package's `Cargo.lock` is provenance from
the published archive; the workspace-level `Cargo.lock` controls LogEx builds.

## Local changes

The production changes in `src/simplify_expressions/expr_simplifier.rs` and
`src/eliminate_filter.rs` backport Apache DataFusion pull request #24258,
merged upstream as commit
`46bbf0db1accffb81822e4a7a374818b36b70def`. The expression fix retains NULL
branches during set operations, and the filter fix preserves safe pruning when
FALSE and NULL reject the same rows. LogEx extends that filter backport to
recognize a safe `NOT IN` list containing NULL as unable to accept a row.

LogEx additionally restricts structural set difference and intersection to a
common column or scalar literal tested expression and non-NULL, unannotated,
non-nested scalar literal list entries of one normalized type. The guard runs
before set construction so rejected lists are neither cloned nor hashed. Other
expressions remain unchanged because structurally different runtime expressions
may compare equal, and volatile or fallible expressions may not be duplicated
or removed safely. The union rewrites retain deterministic expressions but do
not merge volatile tested expressions or list entries. The filter extension
uses a deliberately small discard-safety allowlist: columns, literals, IN lists,
nonfallible comparisons and Boolean conjunctions, and unary Boolean/null tests.
It excludes arithmetic, casts, and functions so pruning cannot hide an error or
observable evaluation. This retains empty-scan pruning without changing the
projected three-valued result. Both added recursive traversals use DataFusion's
existing `recursive_protection` feature.

The modified files carry nearby change notices as required by the Apache
License. Run `python3 tools/verify_datafusion_vendor.py` from the repository
root to verify all complete packages and their exact patches offline.

## XOR cancellation

Apache DataFusion #24248, merged as
`50cdbde1ed4b7cd9a0eb69443e22f09771cd16fa`, demonstrates that repeated
nullable operands must not be cancelled. LogEx's actual query fixture also
shows that nullable guards, and even restricting cancellation to integer
columns, cannot preserve required evaluation: common-subexpression elimination
can hoist an invalid cast into a nonnullable generated column, after which
cancellation and projection pruning erase its error.

The local correction therefore removes the two repeated-operand cancellation
rules and their unused helper in `src/simplify_expressions/utils.rs`. Ordinary
constant folding and vectorized XOR evaluation remain. Superseded structural
cancellation tests are replaced by runtime-expression preservation tests.
Workspace regressions cover nullable cancellation, runtime cast errors,
volatile operands and correct nonnullable results. This extends beyond the
upstream NULL-only guard; it is not an unmodified backport or a general audit of
all bitwise simplification rules.

## Removal condition

Remove this patch and vendor directory when LogEx adopts a maintained
DataFusion release containing #24258 and that release passes the full SQL
correctness and performance acceptance suite. Until then, keep the backport
limited to these set-rewrite and XOR families and required filter pruning
support. A successor release must also preserve the required runtime evaluation
covered by the LogEx regressions.
