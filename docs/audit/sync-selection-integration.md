# Sync selection and historical integration

This follow-up starts at PR #222 merge `bdee0df8` on
`audit/sync-selection-integration`. Source `b61e6363` passes all ten local gates; CI and merge remain. The checkpoint-gap retention item
is closed by [PR #222](checkpoint-gap-memory.md).

## Forward publication under changing selection

At the original base, the outer sync loop reconciles consensus selection before fetching forward work.
Ordinary anchored ingestion then retains copied anchors through header, body and
receipt requests. Checkpoint-gap ingestion retains its terminal anchor through
the complete header download and multiple payload chunks. Neither publication
path rechecks those captured anchors against current selection. Both update the
in-memory tracker and canonical peer cache before awaiting storage write access.

An authenticated old branch is not automatically invalid input: normal reorg
processing necessarily has some lag. **B5-15 — moderate: obsolete work can make a recoverable fork require a resync.**
On exact `bdee0df8`, a test-only four-header fixture replaces the materialized
consensus fork while the captured consumer future is pending. With tracker depth
three, the original consumer publishes all four old-branch rows, evicts the known
shared ancestor, and fails the required subsequent reconciliation assertion with
a window-exceeded error. Depth eight preserves the same ancestor and successfully
rewinds, distinguishing this failure from ordinary eventual reorg recovery.

That first run has one behavior failure and one passing control. Its single poll
does not identify the pending await: validation also awaits workers. The
strengthened original run holds a storage read lock and polls until the fair
lock refuses another reader, proving the consumer's writer is queued. Replacing
selection at that point produces the same one failing recovery assertion and
one passing larger-window control. Both stages remain separately recorded. The reduced
window is a finite mechanism test. Production uses 8,192 retained headers; the
larger-gap implication is source composition, not a production-sized reproduction
or an observed live-chain failure. An initial test helper compile error is
recorded separately from the successful fixture setup and failing assertion.

The correction acquires execution storage access first, then checks full
captured anchor identity and the existing selected-lineage readiness condition
under the consensus snapshot mutex. Synchronous storage/tracker publication can
then complete before that in-memory consensus selection changes. Preparation,
network waits, validation, peer-cache updates and notifications stay outside that
mutex. Compatible extensions and unrelated consensus metadata do not discard
valid work merely because the store changed. The existing behavior when a selected
head is strictly ahead of incomplete materialization remains: admission does not
prove ancestry under a higher head whose chain is not yet known. The correction
addresses already-visible replacement, missing anchors and the existing known
incompatible pending-lineage condition, not future canonicality or finality.

The inspected lock graph has no reverse execution-storage dependency in consensus
store publication. Its writer lock serializes candidate preparation and persistence;
the inner snapshot mutex is released during consensus-file I/O and reacquired for
publication. A publication callback must not call another consensus accessor or
update, await, or invoke peer/subscriber callbacks. Holding the inner mutex through
storage publication adds contention for that duration; no hard latency bound or
throughput measurement is claimed. It linearizes against the in-memory selection,
not consensus-file durability or future reorgs after the lock is released.

## Rewind publication review

The existing reorg path also computes its decision before awaiting storage.
A selection that returns to the current indexed branch during that wait can make
the copied rewind unnecessary. Applying it still retires valid rows and evicts
cache entries, requiring refetching; checking only the retained common anchor
would miss this because that ancestor remains identical. **B5-16 — moderate: a queued obsolete rewind retires the current selected suffix.**
The exact-original control persists and caches A100–A102, selects conflicting
B101–B102, queues the real rewind behind a storage read lock, then restores A102
before admitting the writer. The original action rewinds persisted, tracked and
reported progress to 100, leaves only block 100 canonical, and evicts A102 from
the serving cache. The preservation assertion fails. All three physical rows
remain; this establishes unnecessary rewind/refetch and availability loss, not
permanent data loss. Fixture setup succeeded without a compile/setup failure in
this run. Source review found no permanent loss-of-ancestor example because the
rewind retains an earlier prefix.

The correction recomputes the bounded decisive snapshot after acquiring storage.
The full reorg operation scans segments, so holding the consensus mutex across
that whole operation would introduce an unacceptable new liveness dependency.
The existing persisted reorg intent provides the admission boundary: checkpoint
pending execution writes under exclusive storage access, then recompute selection
and publish that intent under the consensus mutex. Release the mutex before the
existing row scan and finalization. A pending handle borrowing storage owns that
completion; consuming it finishes the operation, while dropping it leaves storage
requiring recovery. The existing combined API composes both phases. The journal,
format, no-op behavior and interruption event order remain unchanged.

Intent admission still serializes the catalog and performs durability I/O; it is
not constant-cost or hard-latency-bounded. The full scan and pending-row checkpoint
are excluded from the consensus lock. Existing scan cost itself is retained, and
later selection changes still use ordinary reorg reconciliation.

## Historical disposition

No new confirmed defect was found in the scoped historical ancestry, ordered
outcomes, empty/genesis progress and queue-ownership paths. This disposition combines source review and existing regressions. The final
focused run passes all 171 engine tests; complete workspace gates also pass.

- Reverse fetch starts at the persisted floor header's parent hash and validates
  each page against its exact child. Admission compares number and hash. The
  first anchored publication establishes the historical anchor, while forward
  reorgs retain a prefix at or above it; ordinary forward reorgs therefore do not
  invalidate ancestors below that floor. Reorgs beyond retained evidence still
  require an explicit resync.
- Generation, sequence, attempt and expected-child checks reject obsolete
  results. Reset clears inventories and retires owned requests/tasks. The
  completion-before-drain and verified-write shutdown fixes from
  [fetch supervision](historical-fetch-supervision.md) and
  [work cancellation](historical-work-cancellation.md) remain in place.
- Prepare admission counts active and completed work. Header splitting, retry
  policy, lookahead and write backpressure bound work inventory. Bulk splitting
  can add several plans, so a ready-buffer threshold is not an exact instantaneous
  map-size or total-memory guarantee.
- Empty validated blocks advance persisted coverage independently of row count.
  Reverse work stops at genesis before subtraction; missing floors and disabled
  historical mode do not start reverse work. A completed verified shutdown write
  leaves a persisted floor even when no further live progress update is emitted.

The detailed source references and existing control names will be retained with
the final evidence. Other audit batches, automatic repair, and subsequent live
sync/staging acceptance remain open.

## Validation and implementation cost

The exact original production source and test-only patches are retained with
separate failing logs for forward admission and queued rewind. The final
candidate passes 171 engine tests, eight canonical-reorg controls, two CL
publication controls, the existing bounded CL snapshot control and a prospective
tracker snapshot oracle. The 17-test selection filter also passes; it includes
existing endpoint tests and is not a count of new tests. Existing reorg recovery
coverage exercises all 30 publication failure points and repeated reopen.

An initial test helper needed an explicit Rust 2024 opaque-return lifetime.
A later lifecycle test incorrectly treated an empty write as a mutation; the
established empty-write no-op succeeds, so the test now attempts a real nonempty
write after abandoning an admitted reorg. Both intermediate outcomes are retained;
neither is counted as an original production regression.

Independent review verifies the CL/storage lock order, guarded predicate, durable
intent split and active/no-op handle semantics. A separate review verifies forward
publication, the shared helper, tracker retention and successful-cache/notification
ordering. All 11 candidate source hashes match the reviewed manifest.

Ordinary ingestion builds one prospective bounded persistence snapshot and appends
only new headers after successful publication. It does not restore or rehash the
entire 8,192-header window for each block. Early checks stop known obsolete gap
work at existing page/chunk boundaries; payload concurrency is unchanged. No new
metadata stream, dependency or authoritative format is added. The old one-caller
`ingest_block` wrapper and unused gap `Option<Header>` result are removed.

The guarded synchronous ingestion still performs its existing storage work;
contention during publication is a correctness tradeoff without a measured timing
claim. Reorg admission retains catalog persistence cost, but the full row scan and
pending-write checkpoint stay outside the consensus mutex. No broad benchmark or
live sync was run for this correction.

## Final gates and evidence

All ten final-source local gates pass: 1,960 workspace tests, zero failures, 24 existing ignores across 35 targets, documentation tests and release build.

The [machine-readable record](baselines/2026-09-17-sync-selection-integration.json)
links hash-verified archives of original sources and patches, actual failing and
passing logs, exact known command provenance, final reviews, source inventories
and complete final gate logs. Two early invocation/exit details were not preserved
and remain explicitly unknown; their compiler/behavior outputs are retained.
Known vendored dependency and debug-linker warnings remain recorded; all final
gates pass. Exact-head CI and merge are still required for closure.
