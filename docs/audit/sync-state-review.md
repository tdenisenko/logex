# Sync trust, partial progress and canonical recovery

This batch reviews startup trust, forward progress and whole-reorg publication.
It does not close all of batch 5. The checkpoint-gap memory finding below and
integrated offline workloads remain separate work; no live sync was performed.

## Confirmed findings

**B5-07 — high: an explicit legacy execution-only mode bypassed the required
consensus dependency.** `SyncEngine` accepted an absent consensus store and
selected separate execution-only live/historical loops. Runtime admission also
accepted existing execution progress when consensus state was absent. Ordinary
fresh startup already resolved a checkpoint, so this is not a fresh-start bypass.
The controlled startup reproduction removes consensus state between the old
preliminary probe and final admission; it is sequential, not a forced process
race. Existing rows cannot establish consensus trust.

Commit `3089569c` requires a consensus store in the engine constructor, removes
the legacy loops and makes runtime admission fail with `MissingCheckpoint`
regardless of existing execution rows. The authoritative checkpoint decision
occurs while holding data-directory ownership and after storage monitoring starts.
An available consensus store with no usable anchor waits cooperatively. Existing
checkpoint refresh and supervised storage-failure behavior remain in use.

**B5-08 — moderate: parent validation used stale state after partial progress.**
Anchored ingestion updates the tracked head and durably writes each successful
block. A later unavailable or invalid response can return `Ok(progressed)` before
the old `last_validated_header` field was updated. The next parent lookup could
therefore return `None` even though the current tracked parent was available.
Consensus anchor, body and receipt checks still applied; this finding concerns
the additional parent-relative execution checks.

Commit `807c81a8` uses the tracked tip directly with checked block sequencing and
removes the duplicate cached field and obsolete return plumbing. The original
regression reproduces helper-visible partial-progress state; it does not simulate
a complete peer request loop or establish a consensus-proof bypass.

**B5-09 — high: reorg rows and coverage were published in separate operations.**
The old caller retired each block hash separately, then rewound the persisted
header window, sync head and indexed anchor. An interruption between those calls
could reopen with some canonical rows missing while the old head still described
the range as ingested. Later reconciliation might correct it, but opening storage
and answering queries did not wait for that reconciliation.

The correction records a bounded reorg intent in the existing checksummed catalog
before changing rows. The original recent-header window supplies the reverted
hashes, and the intent identifies its retained prefix and target indexed anchor.
The storage operation scans segments once for all reverted hashes, preserves raw
source ownership and bundle publication rules, then atomically publishes the
rewound metadata and clears the intent. Opening storage completes any pending
intent before returning usable storage. Failed operations keep read views invalid
and writes unavailable until recovery. Complete physical rows remain available;
only canonical selection and progress change, including empty reverted blocks.

**B5-10 — moderate: absent consensus anchors could retire valid gap progress.**
The old reorg search treated any older matching anchor as a reason to rewind the
suffix, even when no overlapping anchor conflicted. A checkpoint-gap payload
pipeline can commit a validated prefix before a later payload request is
unavailable. Its tip need not have a materialized consensus anchor, so the next
iteration could discard correct progress unnecessarily. A missing anchor is not
evidence of a conflicting block. The correction requires positive reorg evidence
before selecting a matching ancestor: an observed overlapping conflict, or an
explicitly selected shorter terminal whose complete ancestry is materialized
(as detailed below).

The decision also reads its anchor evidence and coverage from one consensus
snapshot. The ordinary matching-tip case returns before copying any anchor range;
only sparse or conflicting cases copy a bounded window. This avoids inconsistent
multi-read evidence without adding a full-window allocation to each sync loop.

**B5-12 — low: programmatic zero batch sizes were not rejected at startup.**
`SyncConfig` allows callers to construct zero header or payload batch sizes; a
zero payload size reaches a slice-chunk panic. The engine now reports an
actionable configuration error before restoring or starting sync. Normal defaults
are unchanged, and the node CLI does not expose these two fields. This is a
configuration API boundary, not a network-input finding.

**B5-13 — moderate: a captured bundled maintenance task could republish old
canonical metadata.** The task checked its source while holding the segment
maintenance owner, but bundled canonical mutation did not share that owner.
A controlled pause after the check allows the original reorg to succeed and
rewind to block 100, then the task republishes the old manifest: a direct reader
again sees canonical block 102. The control uses the public captured-task API
with a current-profile bundle; the ordinary background scheduler skips that
candidate. It establishes an inherited API-level publication race, not a
demonstrated failure of normal background scheduling.

Bundled canonical updates now retain the existing segment maintenance owner
through manifest publication. Raw updates retain their existing source owner
without taking the same lock twice. Contention reports `WouldBlock`, retains the
durable reorg intent and invalidates queries until recovery. A task captured before
a completed reorg fails its source-reference check instead of republishing the old
manifest. These controls also verify successful repeated recovery afterward.

## Selected-head review before merge

**B5-14 — moderate: stale materialized anchors could hide a selected shorter execution
head.** Light-client head advancement follows beacon slots; it does not require
execution block height to increase. The verified head and its complete materialized
ancestry are published in separate steps. The old matching-tip shortcut could therefore
use a cached prior-fork tip while the selected ancestry was still being fetched. This
behavior predates this PR. Independently, requiring an overlapping conflict for every
rewind misses an explicitly selected shorter head that matches a retained ancestor;
absent anchors alone still do not justify rewinding a partially ingested checkpoint gap.

Commit `118088ab` inspects the actual verified selected head under the same consensus
lock and waits cooperatively when known shorter/equal selection or a stale terminal
at/above the selected height lacks complete selected ancestry. This also protects
empty-tracker bootstrap from a same-height cached terminal with a different beacon
identity. The complete materialized terminal must match the full selected anchor,
including its beacon identity, before lower cached anchors establish a common ancestor.
Merely overlaying the selected head onto older cached anchors would not prove their
ancestry and is unsuitable. An explicitly selected shorter terminal can then authorize a
rewind even without an overlapping different block hash.

The initial stale-tip control selected an unsupported slot for its fixture and failed
during setup; that failure is not regression evidence. The corrected control on exact
`c6eec210` reaches and fails the stale `MatchingTip` assertion. Both original patches
apply and reverse against their recorded source; the setup failure remains separately
labeled. This is a consumer-state and signed-fixture boundary; no mainnet occurrence is
claimed. When the selected head is ahead of both the tracked tip and materialized
ceiling, the existing materialization-lag behavior is retained. The snapshot does not
lock head selection across asynchronous network requests, and does not claim to
eliminate all in-flight selection changes.

A final call-path review found that the initial selected-head accessor rebuilt its
beacon root using an allocating helper. Commit `9869104d` reads the existing derived
anchor only when a verified store is present. Every production store assignment
refreshes this cache; materialization and restore reapply it before atomically
publishing the snapshot. This removes the extra per-iteration hash/allocation without
adding persisted state or weakening selected authority. A materialized-only fallback
summary is never treated as a verified selected head.

## Review boundaries and retained behavior

- Forward ingestion authenticates headers against selected consensus anchors, or
  against the terminal checkpoint after validating the entire intervening chain.
  Body/receipt validation precedes log publication. Initial bootstrap can start
  at an authenticated nonzero block with an empty tracker.
- Historical work descends from its persisted child header. Generation, sequence,
  attempt and child identity checks reject obsolete outcomes before admission.
  The completed-result and shutdown ownership corrections in
  [PR #197](historical-fetch-supervision.md) and
  [PR #204](historical-work-cancellation.md) remain applicable.
- First anchored publication establishes the historical anchor and floor at P;
  historical ingestion only lowers the floor. Normal forward tracking starts at
  P, so a reorg retaining a common ancestor in that window retains the historical
  ancestry. No reachable counterexample requiring a historical-marker reset was
  found in these paths. A reorg beyond the retained window remains an explicit
  error, not a guessed reset.
- Empty validated historical blocks advance persisted coverage without logs.
  Genesis stops reverse fetching. Disabling historical sync remains respected by
  the synchronization-complete gate.
- Gap payload work limits active plus completed outcomes to the existing pipeline
  depth (at most eight), with at most 128 blocks per chunk. This is a count bound,
  not a total resident-memory guarantee. Local `JoinSet` ownership cancels pending
  workers when that pipeline exits.

## B5-11 follow-up: bounded checkpoint-gap headers

The original gap path accumulates every header through the terminal anchor, then
adds one hash per header and retains both vectors during payload ingestion.
Request page size and payload concurrency do not bound this O(gap) allocation.
This is a source-confirmed retention issue; no RSS, OOM or throughput claim is made.

The [checkpoint-gap follow-up](checkpoint-gap-memory.md), source `b327cf90`,
preserves terminal-anchor authentication using disposable authenticated scratch
and bounded page/chunk memory. Eleven new controls cover framing, cancellation,
cleanup, I/O, terminal trust, fallback and ingestion equivalence. All ten local
gates and CI passed before PR #222 merged as `bdee0df8`. Immediate forward
publication before terminal
authentication is unsuitable. An arbitrary maximum-gap rejection would prevent
otherwise valid restarts and is not an equivalent remedy.

## Subsequent storage-range correction

At this milestone, historical descriptors used endpoints without an enforced
row-order contract, so reorg pruning was deferred. The subsequent
[historical bounds review](historical-range-bounds.md) reproduced native-query
exclusions and corrected newly computed bounds in PR #221 (`e54e9303`). Existing
underestimated descriptors are not retroactively repaired. The full reorg hash
scan remains; no range-pruning improvement is claimed.

## Performance and validation

The trust and parent fixes remove obsolete state and code. The reorg intent adds
publication work only during reorgs; ordinary live and historical ingestion gain
no per-block metadata writes from this change. Retiring a suffix scans segments
once rather than once per reverted hash. No throughput gain or regression is
claimed, and no broad benchmark was run under the user's updated measurement
policy.

Final source is frozen at `9869104d`. Focused checks passed: 50 runtime controls,
155 engine controls, four selected-head/snapshot/signature controls, seven
canonical-reorg controls and eleven storage reorg tests (overlapping sets are
not added into a separate total). All ten final-source local gates pass, including 1,930 workspace tests
with zero failures and 24 existing ignores, documentation tests and release. Earlier
sources `3baecd32` and `118088ab` passed ten local gates (1,925 and 1,930 tests,
respectively; 24 ignores each). The first published head `c6eec210` also passed
six CI jobs and ten Linux controls. These results are retained separately and
do not substitute for final-source validation.
Final-source CI passed; the verified merge is recorded below.

The six effective original test-only patches apply and reverse against their
recorded baseline commits. The superseded fixture-setup patch is retained
and labeled separately. Storage controls include 30 finite interruption points
with repeated reopen, empty-log suffixes, malformed intent rejection and an
interrupted recovery. The maintenance race uses an explicit bounded pause and
owned temporary files. Initial sandbox listener restrictions, an incompatible
baseline fixture setup and a rejected large-enum intermediate candidate are
retained separately from final successful results.

The [machine-readable record](baselines/2026-09-17-sync-state-review.json) links
compressed original/fixed evidence and complete validation logs. Local gates
and publication status are updated there before closure. Batch 5 remains open
for the specific remaining work described above.

All six CI jobs passed on `89af308f`, including ten Linux volume/template cases with verified cleanup. [PR #220](https://github.com/tdenisenko/logex/pull/220) merged as `4635cbda`. The merge tree is identical to the tested head. The remaining batch-5 and storage-range items remain open.

## Subsequent selection integration

The [selection integration follow-up](sync-selection-integration.md) extends the
same-snapshot decisions above through synchronous admission after storage waits.
It corrects reproduced obsolete forward publication and queued-rewind behavior
without holding CL across the complete reorg scan. Source `b61e6363` passes
focused checks, final review and all ten local gates (1,960 tests /
24 ignores); CI and merge remain.
