# Canonical reorg subscription delivery

Base: PR #232 merge `c85df998f7759d63f6e0701fabceb0ddb765edbb`.
Branch: `audit/reorg-subscription-delivery`. Producer, consumer and storage source reviews are recorded.
The user approved removal events and surviving retained-history snapshots.
Source `eaee66f80f3ad18dc19d9fbe264906a5739e5abc` is committed. Implementation and eleven local gates are complete; exact-head CI and merge remain.

## Baseline findings

- **B8-20 (P2): missing committed reorg event.** The two forward ingestion paths
  notify additions after accepted publication. Canonical reorg finish retired
  stored rows and rewound progress but published no corresponding subscription
  event. Both stream projections hard-coded `removed: false`.
- **B8-21 (P2): retained history cannot reconcile orphaned entries.** Named-session
  upserts preserve history while replacing filters. Retraction cleanup based only
  on the current filter would miss earlier entries; delivery tracking limited to
  retained history would also miss previously delivered, evicted entries.
- The original dashboard prepended chronological batches unchanged and did not
  process removal flags. Actual callback regressions cover the correction. Full
  dashboard acceptance is separate.

The [Geth subscription contract](https://geth.ethereum.org/docs/interacting-with-geth/rpc/pubsub#logs)
provides the reference behavior for removed old-chain logs and replacement logs.
LogEx retains its custom array protocol and ephemeral delivery guarantees.
Historical backfill remains queryable without generating new live alerts.

## Approved contract

The user selected removal events before replacements while keeping connections
open. Retained snapshots represent surviving previously admitted alerts: delete
orphan entries, keep unrelated entries and never insert tombstones. Deletion
must not increment capacity-eviction counts or promise to refill older history.
Use block hash plus log index for identity, preserving distinct replacements
that reuse a transaction hash. Retained-session receivers may receive removals
for unknown identities so a prior filter change/eviction cannot hide a needed
retraction; clients ignore those unknown identities.

The alternative, explicit terminal stream invalidation with query reconciliation,
was presented and not selected. Per-log removal delivery is approved separately
from the standing publication approval.

## Storage and validation requirements

Physical old log payloads survive the canonical bitmap update. Emit no removal
before durable reorg finish succeeds, and do no row scan under the consensus
selection lock. Preserve exclusive storage/file lifetime while materializing
selected old rows. Checkpointing, fresh consensus admission, durable finish and removal delivery
execute together in one owned blocking worker. The engine awaits its result
before restoring its tracker and admitting replacements. Queued work can be
canceled before it starts; started work owns storage and completes even if its
awaiting future is dropped. Normal supervised shutdown awaits the engine;
terminal timeout does not resume ingestion on the old in-memory tracker.
The initial worker proposal was rejected by automatic approval review on
transaction/lifecycle grounds. The user subsequently explicitly approved the
concrete local change and offline cancellation tests; those controls pass.
Capture exact changed row IDs during canonical retirement,
then open and drain one affected segment at a time after commit. Per-segment
source/maintenance ownership and logical identity checks must account for
already-captured background compaction tasks; the outer storage guard alone is
insufficient. Avoid a full-suffix allocation and repeated raw-file reads.
Do not use legacy range metadata as proof a segment cannot contain retired rows.
The prepared iterator reuses scalar metadata and payload preparation per
segment. Delivery is row/page-batched; existing raw-column buffers and selected
metadata remain per-segment costs, not a fixed byte or global RSS cap. No new
disk spool, persistent format or ordinary-ingestion barrier is introduced.

Post-commit materialization failure must end continuity through existing failure
supervision rather than silently continue with incomplete reconciliation.

Required finite tests: actual producer success/rejected/stale decision, no events
before commit, empty-block retirement, removal-before-add order, changed-filter and
evicted-history reconciliation, disconnected-session snapshots, attachment
boundary, identity reuse and the actual browser callback. Keep existing
interruption/reopen, lag and cancellation controls. No live system or benchmark
is needed for this milestone.

## Boundaries

Forward notifications currently follow accepted visible ingestion, which can
require bounded re-fetch after power loss; this work must not claim per-batch
fsync or exactly-once/restart delivery. No persisted schema change is proposed.
Shared query budgets, consensus history persistence, verified offline repair
and integrated/live acceptance remain separate. The new producer fixture uses
the actual selected-storage helper and mirrors the existing forward notification
callers; it does not run peer fetching or a complete logless replacement pipeline.
Those full-pipeline combinations belong to integrated acceptance.

## Regression status

The actual original browser callback fails five finite controls covering ordering,
removal, stale-socket ownership, unknown-removal status and mixed capped batches;
its snapshot control passes. All six pass with the candidate callback. A direct retained-helper probe demonstrates that merely
appending a removed notification would retain the original orphan and add a
tombstone; this is a correction-design witness, not a claim that the original
producer emitted removals. The unchanged original producer now reproduces the reachable failure: after
successful canonical retirement its channel remains empty. That run used the
compatible candidate consumer event type and is not claimed as an entirely
unchanged workspace. Preserve its exact producer snapshot and build inventory.
Initial focused server execution encountered four existing loopback fixtures
blocked by sandbox bind permissions; that log is retained. The escalated server
run passes 162 tests with two existing ignores, and strict server Clippy passes.
No live-system test is involved.

Four prepared-reader controls pass, covering raw/paged/bundled expected-row
equivalence, selected gaps, invalid inputs, retained source lifetime and terminal
read errors. This is a new internal helper; no before-fix executable failure is
claimed for an API that did not previously exist. Seventeen native reorg controls
and nineteen sync reorg controls pass, including five new notification controls,
actual producer success/rejection, stale queued selection, queued-worker abort
and started-worker completion. Mixed raw/bundled sources, prior false rows,
empty blocks, commit-phase failures, compaction and a later segment read failure
after earlier removal delivery are covered. All eleven local gates pass on the
committed source: 2,031 workspace tests, zero failures, 24 existing ignores,
eight documentation-test targets and the release node build. Formatting, patched
vendor checks, workspace check and strict Clippy pass. Exact-head CI and merge remain.

## Cleanup and implementation cost

The existing retirement scan captures row identities only for the notification
variant; normal retirement/recovery callers keep the count-only path. Catalog
indices provide constant-time descriptor lookup during drain, with ID and full
source-identity checks. Each segment is prepared once and drained before the next
reader opens. Ordinary ingestion adds no storage barrier or persisted metadata.

Both notification kinds now share the publication helper; the prior separate
transfer-publication helper is removed. The addition-only projection wrapper is
kept only for existing tests. Browser reconciliation uses one identity map per
batch, avoiding repeated full-history scans. Addition publication avoids empty
removal allocations and allocations for nonmatching sessions. No benchmark or
throughput claim is made. Existing storage readers and count-only reorg APIs
remain used; no unrelated code or tests were removed.

## Reproducible evidence

[The machine-readable record](baselines/2026-09-18-reorg-subscription-delivery.json)
contains the exact source inventory, gate commands, exit statuses and counts.
The accompanying evidence archive contains 55 files, including baseline producer
and consumer snapshots, test-only witnesses, final source reviews and approval
history. The validation archive contains the gate runner, recorder and all logs.
Both archives were decoded and their per-file hashes verified after creation.

Evidence SHA-256: `b238278ffd34f2701a1381e3c8abfcba46e813d66dd6957b559c437cb5b5185a`.
Validation SHA-256: `c9ed428b402824b99402757cb8a6de474f0003784164813c1bf96e505366a4f0`.
Existing dependency deprecation/future-compatibility notices remain outside this
change; no test was ignored or removed to make these gates pass.
