# Consensus metadata ownership and obsolete forks

Base: PR #236 merge `59f5722ca67806cc23a6efae8854ba0bd6200193`.
Branch: `audit/consensus-metadata-lifetime`.
Source: `ab618bf56c10dfda7d94bcb8a75c517099ab501c`. All eleven local gates pass; exact-head CI and merge remain.

## B3-73: authenticated metadata outlives its owners

Severity: medium, a retained-resource lifetime defect. No invalid trusted data
or lost execution logs were demonstrated by this finding.

Optional unconnected beacon metadata already has a bounded cache. Once a root
became authenticated, it left that cache's eviction bookkeeping permanently.
Replacing its materialized anchors or abandoning its selected target did not
release that protection. Obsolete forks could therefore accumulate during the
process lifetime even after their consumers had moved on.

Two finite tests reproduce the defect against unchanged original production
code. One uses the real durable history materializer to replace an old fork;
the other uses the actual target selector to abandon a completed old target.
Both first verify that required metadata remains available, then fail because
the obsolete root stays permanently protected. These are decoded-metadata
fixtures, not new evidence about SSZ, signatures or live peers.

## Required ownership

Persisted anchor roots, the checkpoint, active and latest selected targets, and
outstanding request/recovery dependencies retain their exact cached ancestry.
An incomplete active target can lag the latest selected head; both matter.
Parent traversal requires the exact root and a strictly smaller slot. Missing
parents remain recoverable through retained child references.

The store accepts the same beacon root at multiple execution heights. A lookup
at the cached block's height proves ownership when it matches; a mismatch does
not prove the root is unowned. Reclamation must also account for owners at
other heights and roots reintroduced before cleanup runs.

The range writer reports unique prior roots that were removed or changed at
their execution height. It captures these candidates within the same serialized
transaction and returns them only after durable publication. Same-root metadata
changes still persist but produce no release candidate. The existing reversed
and out-of-range upsert behavior is preserved. No candidate result escapes a
failed write. A fresh network ownership check is still required.

## Implementation cost and scope

Release is triggered by actual ownership changes or completed authenticated
batches. Positive height/root lookups and retained children stop ordinary
canonical progress. An unresolved release batch may require one borrowed scan
of retained anchors to check ownership at other heights. No reverse root index
is added to every persisted anchor, and retained history is not cloned for this
check. Checking and local metadata reconciliation share one snapshot lock,
without awaiting, filesystem I/O or reentrant store calls.

Released metadata returns to the existing optional FIFO. An old branch selected
again after eviction may need to be fetched again. Required anchor history,
historical period responses and independent raw-body cache ownership remain
separate. This is not a cap on total memory or required trusted history.

The source-cost review does not establish measured throughput or latency gains.
No broad benchmark campaign, live sync, production data or remote host is used.
Eligible gossip cache admission, priority queues, shared query resources,
dashboard review, verified offline repair and integrated acceptance remain open.

## Validation

Original controls: two expected failures. Store controls pass against an
independent map reference, including duplicate roots/keys, moved roots, sparse
and boundary ranges, journal and checkpoint reopen, exact no-op, metadata-only
updates and a failed publication in a disposable directory. Nine network lifetime controls and all 405 consensus tests pass, with one existing
ignore, and strict consensus Clippy passes. Independent store, network and
combined ownership reviews are clear. All eleven local gates pass; exact-head CI and merge remain.

The network controls cover durable replacement, abandoned unmaterialized targets,
root aliases, reownership before cleanup, shared and unequal-depth ancestry,
active/latest ownership, unchanged ticks and advancing heads, request completion,
retry/stale responses, pending recovery, late required-root arrival and detached
parent replacement. Existing candidate-pressure/backward-recovery tests also pass.

One existing range-response fixture previously relied on permanent retention
after its synthetic target was legitimately abandoned for a different checkpoint.
It now persists the synthetic head as an explicit continuing owner. All original
assertions remain, including that the missing parent prevents anchor publication.
Two draft raw-admission promotion defects found during review were fixed and
covered before the final passing run. The evidence retains this review history.

## Full validation record

The frozen source passes all eleven local gates: vendor integrity, workspace and
four patched-package format checks, workspace check, strict Clippy, all-target
tests, documentation tests and release node build. There are 2,072 passing
workspace tests, zero failures and 24 existing ignores across 37 targets.
Documentation checks pass across 8 crate targets (0 examples). No dependency or
lockfile change is included.

The [machine-readable record](baselines/2026-09-18-consensus-metadata-lifetime.json)
contains original/final source inventories, exact commands and hashes. Compressed
[evidence](baselines/2026-09-18-consensus-metadata-lifetime-evidence.json.gz) and
[validation logs](baselines/2026-09-18-consensus-metadata-lifetime-validation.json.gz)
are JSON archives; every embedded file includes its size and SHA-256 digest.
These are correctness records, not performance benchmark results. Linux/macOS
CI and the existing Linux volume controls must pass before merge.
