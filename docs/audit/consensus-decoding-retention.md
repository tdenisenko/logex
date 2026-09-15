# Consensus decoding and retained-anchor work

This milestone removes demonstrated allocation and persistence work while preserving
consensus roots, ordered-anchor semantics and the existing save transaction. It
also records a disposition for typed Beacon allocations after the incoming and
serving payload limits merged. It does not complete the broader offline audit.

## Findings and decisions

| ID | Classification | Evidence and resulting behavior |
| --- | --- | --- |
| B3-49 | Performance opportunity | Execution-payload hashing collected 32 bytes per transaction before building its tree. Stream those roots through the pinned tree hasher, and use a 544-byte stack array for the fixed 17 payload fields. Exact roots and list limits remain unchanged. |
| B3-50 | Performance opportunity | Replacing an identical anchor range cloned, normalized and durably rewrote the entire snapshot. Compare the normalized affected range under the existing serialized writer; identical replacements return before snapshot cloning or I/O. Actual changes still publish only after durable replacement. |
| B3-51 | Performance opportunity | Anchor lookups scanned a strictly increasing list, and normalization rebuilt a tree even when input was already sorted and unique. Use binary lookup/partition points, a constant-time last-anchor check and a no-allocation sorted-input path. |

These are unnecessary-work findings, not evidence of incorrect roots or previously
wrong lookup results. No throughput or latency percentage is claimed. The owner
ended the extended benchmark campaign; the evidence here is eliminated allocations,
file replacement and algorithmic work rather than host timing.

## Root hashing and typed decoding

The new transaction-root helper uses the same incremental MerkleHasher pattern as
pinned `ssz_types` 0.11.0. Each transaction still uses its existing contiguous-byte
root, the outer tree retains its declared maximum, and the actual transaction count
is mixed in exactly once. The fixed payload fields retain their original order.
The checked writes and finish calls follow the bounded list and complete-leaf
invariants used by the dependency's generic implementation.

The removed transaction-root Vec requested 32 bytes per transaction. The protocol
allows 1,048,576 transaction entries, making that scratch allocation up to 32 MiB.
An offset table with empty inner byte lists illustrates the SSZ allocation bound;
it is not an execution-valid block claim and is not generated as a large test.
Small controls cover empty/nonempty lists, powers-of-two boundaries and byte lengths
around 32-byte chunks. They compare against generic SSZ list hashing. The retained
real Fulu fixture independently checks the complete expected Beacon root.

The pinned SSZ decoder checks element counts against an in-bounds offset table and
field limits. It derives fixed-element allocations from actual bytes; it does not
allocate a tree or transaction buffer proportional to the one-GiB declared inner
byte-list maximum. Other generic list roots already stream. The complete typed
Beacon tree remains local to one decode call. Network processing decodes one body
at a time and retains only compact metadata with the already-accounted raw chunks.

Typed Vec descriptors, inner byte allocations, ordinary allocation-failure behavior
and allocator transients remain additional finite overhead outside raw RPC quotas.
A million Vec descriptors alone are approximately 24 MiB on the current 64-bit
target, excluding allocator costs. This is sizing from types, not a total RSS
bound. Work can scale with protocol element counts. No new parser, protocol cap,
process-wide memory ceiling or performance guarantee is introduced. Current
Electra/Fulu body support and trust validation are unchanged.

## Ordered-anchor updates

All public anchor mutators normalize the list; restored snapshots reject duplicate
or decreasing block numbers. These invariants justify binary search and partition
points without an added index or a new persisted field. Queries at zero, gaps and
u64::MAX retain their exact results without incrementing the requested block number.

The usual range replacement normalizes only incoming records and identifies the
existing interval using partition points. Complete record equality includes the
execution roots, finalization flag and parent Beacon root. An equal slice returns
before cloning, serialization, file sync or rename. Failure latching and the writer
mutex still precede that check, so a no-op cannot bypass a prior uncertain save.

For changed contained ranges, replacement splices into the private candidate and
recomputes the existing summaries before the normal durable publication. Incoming
records outside the interval, and reversed intervals, preserve the public method's
previous merge behavior. Duplicate incoming block numbers still use the last
record. Sorted unique input normalization returns its original allocation.

This does not bound historical retention or remove full-snapshot writes for actual
changes. Trusted snapshot integrity envelopes, accumulated history, repeated status
coverage scans and period-payload retention remain separate review work. No
retained anchors are discarded, no storage directory is reset, and no migration
or new on-disk format is part of this milestone.

## Validation and cleanup

Four anchor controls check unchanged-file identity while keeping the original file
open, changed-metadata publication/reopen, the previous-failure latch, replacements
against an independent last-record-wins oracle, scan-equivalent lookup boundaries
and sorted-input allocation reuse. Two isolated controls restore the exact original
range replacement or normalizer in the candidate harness and fail on the expected
file replacement or allocation respectively. They are not full baseline checkouts;
exact source and log hashes are retained.

Independent reviews cover anchor locking/equality/fallback semantics and the final
streaming root implementation. A strict Clippy check identified a fixed-chunk idiom
in the draft stack-buffer loop; the final loop uses fixed-size chunks and all CL
checks were rerun. The initial lint result remains separate from final acceptance.
Removed full-list transaction-root staging, fixed-field heap staging and unnecessary
sorted normalization/replacement work. No unsafe code, dependency change, benchmark,
remote operation or production-data access was added.

Source and gate results are recorded in the
[validation record](baselines/2026-09-15-consensus-decoding-retention.json).
The focused consensus suite passes 305 tests (one ignored) and strict CL Clippy.
Source `331c97fa` passes all seven local gates: vendor integrity, formatting,
workspace check, strict Clippy, 1,383 workspace tests (23 ignored), documentation
tests and the release node build. All six Linux/macOS CI jobs passed in run `34978618567` on head
`fa8a79e5`. PR #169 merged as `2bf55c4a` after exact head/base verification.
