# Ingestion progress publication and restart boundaries

This document retains the investigation chronology and revision-specific results.
Use [the PR #130 acceptance record](pr130-acceptance.md) for the final source,
validation status, compatibility decision and performance tradeoffs.

**PR #130 remains a draft.** The measurements and reproducer below describe
`09a63f55` and `ff728ea3`. A new [combined sync checkpoint prototype](sync-ingestion-checkpoints.md)
implements bounded row/progress rewind and passes its initial regression tests.
It still needs performance acceptance and platform validation. The prior WAL
checkpoint prototype exceeds the user's 10% performance-regression limit. These findings extend the
current storage milestone into the ingestion commit boundary; they do not
establish completion of audit batch 5.

## B2-10: restart can repeat rows whose progress was not saved

Severity: P1, duplicate queryable logs after restart. This reproduces on both
the original `09a63f55` baseline and checkpoint candidate `ff728ea3`.

At those revisions, the live caller in `logex-sync/src/engine/ingest.rs` writes rows, then records
canonical headers/anchors and historical coverage. Historical chunk ingestion
writes rows, then advances its historical floor. Forward-gap ingestion likewise
writes rows before publishing its canonical updates. The restart path in
`engine/historical.rs` prefers the persisted sync head and restores the retained
header window; historical backfill resumes from the persisted floor header.

A deterministic reproducer uses the same storage calls:

1. Persist a valid preceding canonical header window, or a historical floor
   above the incoming block.
2. Write three rows for that incoming block.
3. Close before the caller publishes the new head/floor; reopen normally.
4. Observe three stored rows but the old progress marker.
5. Resume from that marker, ingest the same block, then publish progress.

Both live and historical cases contain **six rows instead of three**. Both
assertions failed as expected on both revisions. The experiment models the
process interruption between storage calls; it is not a physical power-loss
test. [Captured assertion outcomes](baselines/2026-09-11-publication-gap.json).
Temporary reproducer additions were removed after preserving their output;
the ordinary fixture continues to test successful publication and exact reopen.

This is separate from B2-05, which repeated rows *inside WAL recovery* after
segment rotation. Exact WAL-prefix replay does not make the caller's later
progress write atomic. Equality-based deduplication in the generic row writer is
not a valid repair: intentional repeated batches and distinct ingestion routes
must remain distinguishable.

## Storage-call performance baseline

`crates/logex-storage/tests/ingestion_publication.rs` adds an ignored release
benchmark and a small ordinary CI test. It uses synthetic linked headers,
whole-block rows and every sixteenth block empty, including the final measured
block. The fixture calls the same live and historical storage methods as sync.
It does not perform networking, cryptographic validation, extraction, async
scheduling or subscriber delivery.

The live path starts with an untimed, populated 8,192-header canonical window,
matching `SyncEngine::RECENT_HEADER_WINDOW`. Each measured block publishes rows,
the current header window, its anchor and the historical floor. The historical
path starts at its newest header and publishes descending complete-block chunks
and floor updates. Fixture construction, extraction-equivalent row coalescing
and setup are excluded from timing; finalization/checkpoint costs are included.
Every row and the persisted head, anchor, floor and empty-block progress are
checked after reopen.

[Raw comparison](baselines/2026-09-11-ingestion-publication.jsonl): internal APFS
on the same Mac14,15, pinned compiler and release configuration as the existing
benchmarks. Three alternating process pairs, three iterations per process:
128 measured blocks, 128 rows per nonempty block, 15,360 rows, 2,048 historical
blocks per allowed chunk and a 1,000,000-row segment target. All oracles passed.
This short historical dataset fits in one chunk and needs sparse finalization;
it is not a full historical throughput workload. No local build/test ran during
measurement. Binary and harness hashes are in the raw records. The baseline
receives the same fixture with only the unavailable final `checkpoint()` call
omitted, as its original live writer already retires WAL data per batch.
The final source uses Clippy's equivalent `is_multiple_of(16)` spelling in
untimed fixture construction; the recorded binaries used `% 16 == 0`. Timed
operations and generated inputs are unchanged.

| Storage-call sequence | Baseline median ms | Candidate median ms | Regression |
| --- | ---: | ---: | ---: |
| Live rows and progress publication | 2,819.002 | 4,778.635 | +69.52% |
| Historical rows and floor publication | 14.944 | 50.809 | +240.00% |

Live pair-level regressions are 65.31–72.41%; historical ones are
235.43–245.04%. These results measure more work than the row-only fixture and
must not be combined with its timings or described as P2P throughput. The
earlier row-only results remain available. They understated the impact of
synchronizing the engine's separate progress publications.

## Implications for the next implementation

The user suggested restarting from a safer point and re-fetching the unfinished
tail. That is a promising way to avoid duplicating every historical row in a
WAL, but a correct checkpoint must bind data **and** progress:

- Record a trustworthy durable boundary for segment positions, canonical head,
  header window, chain anchors and historical coverage, including empty blocks.
- Keep the unfinished range bounded and distinguish local persistence from
  consensus finality. A finalized chain block can still be an unfinished local
  write, while an old committed row may share a segment with a new tail.
- Recover by completing a verified transaction or rewinding the matching rows
  and metadata together. Never preserve an advanced coverage marker after
  removing its rows, or retain unpublished rows while resuming from an older
  marker without an explicit replacement protocol.
- Preserve older committed prefixes, canonical flags and complete affected
  blocks across segment boundaries. Re-fetch through the existing authenticated
  validation path; absent anchors or unavailable history must be explicit
  blockers, not guessed data or false completeness.
- Test interruption at every combined commit phase, exact query equivalence,
  retries and empty-block progress before benchmarking. Grouping fsync calls
  alone cannot fix B2-10, and no rollback/refetch implementation is claimed here.

The next performance strategy must be evaluated against both the original
row fixtures and these caller-shaped publication fixtures. No slowdown has
been accepted or deployed.

All six final local workspace gates pass with the new ordinary fixture:
824 tests, zero failures, six ignored tests. The optional distinct-mount test
also passed explicitly. [Final gate results](baselines/2026-09-11-publication-gates.jsonl).
These gates exclude the deliberately temporary pre-fix reproducers above and
therefore do not establish that B2-10 has been resolved.
