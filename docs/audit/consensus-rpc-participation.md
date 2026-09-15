# Consensus RPC participation processing

## Scope and invariants

Continue batch 3 from merged PR #161 (`b72c8a01`), with source commit
`2556fad6a3a2fa0bd1319c591403670271bfb95d`. Review the owned singleton
finality and optimistic response paths, their shared light-client processing,
serving caches and durable publication. Range responses, gossip admission,
retention and aggregate network memory are separate review areas.

The [pinned Altair light-client sync protocol](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/altair/light-client/sync-protocol.md)
processes every relevant verified update. Its maximum-participation sample and
best-update selection can change even when neither stored header advances.
The optimistic safety threshold is half the larger current/previous committee
participation maximum, with integer division. Discarding an update solely because
its headers are already known is therefore not equivalent to protocol processing.

Existing request ownership, RPC fork-context validation and cryptographic checks
remain required. Missing local committees, irrelevant updates and future signatures
retain the existing local-error attribution. RPC responses do not inherit gossip
propagation timing or gossip outer-layout admission rules.

## Findings

| ID | Severity | Evidence and disposition |
| --- | --- | --- |
| B3-31 | P2 | Two signed owned-response regressions fail on the existing singleton stale filters. Optimistic participation 1 to 4, and finality participation 342 to 344, must update the participation maximum despite unchanged headers. A later response at exactly half the improved maximum must not advance optimism. Remove the slot-only prechecks while retaining full verification. |
| B3-32 | P2 | Range processing updates global summaries without replacing singleton payloads. Those summaries cannot safely rank the actual cached payload; an intermediate or matching singleton response may improve the cache even when it does not improve the global summary. After removing the stale filters, a signed mixed-response regression still fails on the shared recorder using status as cache identity. Rank actual cached bytes independently of diagnostic summaries. |

The two original participation regressions fail before the stale-filter removal.
The mixed-cache regression then fails after that removal and before correcting
cache ranking. These are separate stages of evidence. The initial post-fix
participation run reached reopen and exposed a fixture outside the current
weak-subjectivity window. Recent same-period fixtures resolve that test setup
issue without weakening the startup check.
The selected implementation shares conditional verified recorders across RPC and
gossip. It decodes the retained bounded singleton payload to determine its actual rank,
keeps the better diagnostic summary independently, and preserves verified-store
changes even if neither summary nor cache advances. Range publication uses the
same summary ranking. Stable ties retain the previous item. If all three are
unchanged, the transaction avoids the historical snapshot clone and write.
A malformed retained payload returns an actionable local error before mutation;
it is not silently treated as an empty cache.
Changed state keeps the complete candidate, durable write, then publication
ordering. Explicit header seeding and history materialization run only when a
verified header changes. Request scheduling still seeds current headers internally
on changed transactions; a true no-op skips scheduling entirely.

Peer success means a response passed verification and context validation; it does
not assert that a header advanced or local storage succeeded. Local save failures
must leave published state unchanged and latch the existing storage-failure signal.

No benchmark, live-peer test or remote-host work is part of this pass. Signature
verification on relevant responses remains necessary. Broader historical snapshot
cost, retention and complete memory accounting remain open in batch 3.


## Validation and cleanup

Focused validation passes all eight owned-response controls plus a retained-cache
error control. The complete consensus suite passes 249 tests (one existing ignored
test). Independent source review found no actionable issue; the reviewed source matches immutable commit `2556fad6`. Strict CL Clippy
passes. All seven required local gates pass: format, workspace check, strict workspace
Clippy, 1,327 workspace tests (23 ignored), documentation tests, release build and
vendor verification. The [validation record](baselines/2026-09-15-consensus-rpc-participation-gates.json)
binds commands, results and review hashes to the source. PR/CI completion remains
pending before merge.

The controls cover equal-slot participation and later threshold behavior, normal
finality advancement, missing local committees, invalid signatures and context,
matching/intermediate range and singleton cache candidates, weaker later ranges,
stable no-write responses, local save failure and reopening. They use constructed
signed data and temporary state without starting discovery or polling a swarm.
The small test-only range helper preserves signed finality fields and uses the
protocol's absent-next-committee representation; it does not alter production
verification. The fixed context fixture follows the currently configured BPO2
schedule, without claiming support for an unscheduled future fork.

Removed both slot-only stale helpers, their unused network imports, duplicate
unconditional singleton recorders and unnecessary response payload clones.
The recorders and priority helpers now have transport-neutral names. Existing
writer locking and failed-save latching remain shared. No storage format or
public successful-query contract changes in this pass.
