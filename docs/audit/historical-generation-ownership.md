# Historical preparation and write generation ownership

The September 27 `6949ca48` live run stopped after historical verification stalled
at block 6,245,027. September 28 checks at 06:42:08 and 06:44:57 UTC retained the
floor and fetch/prepare cursor 3,029 seen in the separately timed 06:13 public
check: three completed preparations, no active historical work and no reported
write backpressure. Live sync, fresh consensus and bounded queries continued.
The monitor was paused; the original client exited cleanly at 06:45:01 UTC and
only its owned dataset was removed at 06:47:17 UTC. Logs, catalog and provenance
remain. This run's data and elapsed time are excluded from future acceptance.

## Reproduced defect

Fetch outcomes carry a generation, and a reset invalidates the mapped workers
and restarts sequence numbering. An awaited preparation and an owned ordered
write temporarily live outside those maps. They could survive a lookahead reset
and later restore their old sequence into the new generation. Newly prepared
batches below that cursor then became unconsumable. Gap recovery only searched
for later work, so it did not recover this state.

A finite local-channel regression reproduces this interleaving with actual header
processing and a queued storage write. An unusable expected fetch resets the
pipeline, a new-generation header result becomes prepared, and the older write
then restores cursor 3,029 instead of zero. The original code fails the cursor
invariant. Two other controls show an obsolete preparation publishing after a
reset and an obsolete primary worker remaining awaited until timeout.

These controls prove a source defect capable of producing stranded work. The
live snapshots did not expose queue keys, pipeline generation or exact reset
cause, so they cannot prove that the deployed instance followed this precise
interleaving. The final historical log preceded the first plateau snapshot;
neither is an exact stall-onset timestamp.

## Correction and ownership boundaries

- Capture the generation of an awaited primary preparation. Stop and abort its
  owned worker if lookahead resets that generation, before retaining or publishing
  it. Existing cancellation and live-yield ownership remain intact.
- Pass the current primary preparation's generation and sequence into gap
  recovery. A worker intentionally outside the maps is still owned; a token from
  another generation cannot hide a real gap. No persistent ownership marker can
  survive cancellation of the wait.
- Check generation again after draining ready work and after pre-write refill.
  Discard invalidated preparations before starting a write.
- Once a write is owned, retain its completion and storage errors. If its pipeline
  reset meanwhile, preserve the committed floor and progress accounting, discard
  lookahead planned against the earlier floor, and return to the normal scheduler.
  Never restore old sequence numbers or continue that batch's residual work in
  the new generation. Normal same-generation coalescing and refill are unchanged.

The existing scheduler, validation and storage publication paths are retained.
There is no timeout watchdog, unconditional retry loop, query-format change or
migration. Recovery may refetch bounded speculative work discarded after a reset;
that is preferable to keeping work planned against a superseded floor.

## Validation and limits

Six new controls cover reset before writing, obsolete-worker cancellation,
write completion plus subsequent ingestion, retained storage failure, current
versus obsolete preparation ownership, and overlapping new-generation work.
The overlap control continues the same engine through a fresh live head at 164
and verified genesis using finite local request channels. No external execution
or consensus service is contacted by these controls. Fixtures use temporary
storage and an unpolled localhost listener required by the network constructor.

All **697 sync unit tests passed**, with **two existing ignores**. The original
failure output and corrected suite are pinned by hashes in the
[regression evidence](baselines/2026-09-28-historical-generation-regressions.json).
An initial fixture compile error and sandbox listener denial were retained as
setup failures; neither is counted as a reproduction of the scheduler defect.
The overlap fixture was refined to deliver an expected fetch failure and capture
a real new-generation header result before asserting the old cursor failure.

Review covered generation changes at each await boundary, map versus local task
ownership, coalescing, residual handling, storage-result retention and normal
refill callers. All changed helper call sites were checked; no superseded helper
or temporary diagnostic remains. Full workspace gates, exact-head CI, merged-tree
verification and final native release tests/build must be recorded before a fresh
live run. This correction does not establish full sync or 48-hour acceptance.
