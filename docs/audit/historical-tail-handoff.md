# Historical payload handoff at the end of backfill

The September 27 live acceptance run on source `0c8fd848` reached historical
floor 113 at 10:59:39 UTC and remained there through the 11:16 observation.
Live sync, fresh consensus and bounded queries continued. The run was stopped
cleanly and its owned test dataset removed before client changes; it did not
pass full sync or the uninterrupted acceptance window.

## Cause and correction

The asynchronous historical header worker hands nonempty batches to the combined
body/receipt planner. That planner rejected batches below the 64-block parallel
threshold and batches fitting a single request chunk. The caller then discarded
the headers and could repeatedly fetch them without requesting their payloads.
The existing all-empty-header path bypasses the planner, so empty-chain controls
did not expose this failure. A block with an ommer commitment needs a body even
when it contains no transactions or log rows.

The combined planner now accepts any nonempty batch that has eligible peers and
request capacity, including one bounded chunk. It uses the existing scheduler,
peer accounting, retry limits and ordered validation/ingestion path. Empty
input still has no plan. Body-only and receipt-only parallel helpers retain their
sequential fallback thresholds; those helpers do not own this asynchronous
handoff. No block validation or coverage-publication checks were relaxed.

## Regression evidence

The original source reproduced the failure in three new local-channel tests:

- Two planner controls exercise 1, 2, 63, 64, 113 and 128 blocks, with peers that
  permit the entire request in one chunk. The original planner returned no plan.
  The corrected planner completes exactly one body and one receipt request,
  returns the full batch and leaves no residual chunks.
- An engine control starts at floor 63 with an ommer-bearing historical block
  and concurrent live anchors. Before correction it issued 7,967 historical
  header requests in five seconds, fetched no body and left floor 63 unchanged.
  After correction it fetches the validated tail once, actually requests the
  nonempty body, reaches genesis and advances the live head to block 127. Its
  zero-row storage result also checks that verified empty-block coverage advances.

Both planner tests and all nine live/history fairness tests passed after the
correction. The fixture timeout bounds a liveness regression; the request count
is evidence of avoided repeated work, not a throughput benchmark. Full workspace
gates, hosted CI and native release validation are recorded against the final
source in the implementation PR and subsequent acceptance evidence.

The live snapshot did not log which planner rejection branch was taken. The
local controls establish both small-batch rejection paths and the resulting
engine stall, without attributing an unobserved branch to the production run.
A fresh uninterrupted run is still required to establish live acceptance.
