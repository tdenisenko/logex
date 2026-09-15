# Bounded Beacon body retention and serving

Source `2b8dc746` bounds optional Beacon bodies and shares their backing storage
through queued and active responses. It writes one encoded chunk at a time and
stops range responses at missing bodies. Verified ancestry and incoming response
validation remain independent. This is a prerequisite to the still-open aggregate
incoming-response memory budget, not completion of that budget.

## Findings

- **B3-39 — unbounded retained bodies (P2, fixed):** every decoded Beacon body was
  retained in a network-lifetime map with no removal or byte limit. Serving cloned
  each body's Vec. Retention now has explicit byte and resident-object limits;
  serving clones shared ownership instead of body bytes.
- **B3-40 — gaps in served ranges (P2, fixed):** filtering out a missing canonical
  body and continuing could return A and C while omitting their intermediate B.
  The original helper reproduced two returned bodies where a valid prefix has
  one. Stop at the first missing requested canonical body; empty slots remain
  valid gaps between existing blocks. Root requests may still omit unavailable
  roots.
- **B3-41 — accumulated encoded responses (optimization retained):** the writer
  built the complete encoded stream before its first write. Four small chunks
  produced one 120-byte write in the original implementation. The corrected
  writer produces four complete 30-byte writes and identical decoded contents.
  Encoding also writes directly into the header's Vec, eliminating extra
  compressed copies and an uncompressed-size reservation.
- **B3-42 — decoded framing allowance (P2, fixed):** the SSZ ceiling included
  1,024 bytes intended for wire overhead. Enforce the exact **10 MiB** decoded
  protocol maximum. A tiny length-prefix control demonstrates rejection before
  payload allocation; the exact limit remains admissible to the framing scanner.

The pinned [Phase 0 v1.6.0 networking specification](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/phase0/p2p-interface.md)
sets MAX_PAYLOAD_SIZE, permits shorter range responses, requires consecutive
blocks from one chain, and permits unavailable roots to be omitted. A light
client's optional cache does not promise full-node recent-history availability;
peers may choose to disconnect when requested history is unavailable.

## Ownership and availability

The cache admits at most **128 MiB of Vec backing capacity** and **4,096 resident
body objects**. Both limits include objects evicted from the cache but still held
by queued or active sends. Counting capacity prevents spare Vec allocation from
escaping accounting; counting resident objects also bounds zero-capacity bodies.
The map/order structures have bounded entry counts but their allocator overhead
is not included in the 128 MiB body-byte figure.

Each immutable body owns its charge through an Arc. The last owner frees its Vec
before releasing the counters. Admission has one mutable owner; other threads can
only release charges, so no semaphore wait, lock or permit deadlock is introduced.
FIFO eviction removes cache ownership. Duplicate roots reuse the existing body
and queue position. If outstanding sends keep the budget full, omit optional
caching of the new body while still recording its verified metadata and ancestry.
There is no body copy on cache admission or serving selection.

V1 responses lack fork context. Before admission, fill absent context from the
already-verified block slot, allowing the same body to serve V2. Supplied context
still passes the existing decoder verification. Independent review found this
edge in the draft; a real V1-first/V2-later fixture failed before normalization
and now verifies successful V2 serving and duplicate shared-body reuse. This draft
regression is distinguished from the baseline findings above.

Range selection stops at a missing requested canonical body, including a missing
first body. A later cached block is not substituted for it. Root selection keeps
requested order while omitting misses. Neither cache eviction nor unavailable
serving data removes ancestry, resets trusted state or changes query coverage.

## Encoding and failure behavior

Incoming variants and persisted RawRpcResponse serialization retain their shape.
Explicit outgoing cached variants carry shared bodies. Before writing a stream,
validate all chunk counts, context requirements and protocol payload ceilings.
Then encode and write one complete chunk at a time, releasing completed body
references as the iterator advances. Enforce the existing **256 MiB cumulative
wire limit** before writing the next encoded chunk.

A later wire-limit error can leave an already-written prefix of complete chunks;
the writer returns an error and does not perform a successful close. An underlying
I/O failure can interrupt a chunk; its error propagates and remaining ownership is
released. Cancellation likewise drops the current encoded buffer and remaining
body references. These are transport outcomes, not partial trusted-state writes.

## Validation and limits

Four new codec controls cover incremental writes and round-trip bytes, the exact
SSZ ceiling, preflight and cumulative wire limits, and quota release after send
progress, cancellation and write error. Four cache controls cover capacity/entry
charging, held evictions, multiple owners after cache destruction, FIFO/duplicate/
oversize behavior and zero-capacity bodies. Network controls cover V1/V2 reuse,
range holes/empty slots and metadata preservation when body admission is omitted.

The before-controls restore the original writer/encoder/constant and range
selection operation inside candidate test harnesses; they are not full baseline
checkouts. Their exact method, source bindings and logs are retained. The V1 draft
failure and initial compile-only adaptation failure are recorded separately.
All 272 consensus tests pass (one ignored), strict CL Clippy passes, and independent
final review binds all four committed source hashes. All seven local gates pass: vendor integrity, formatting, workspace checking,
strict workspace Clippy, 1,350 workspace tests (23 ignored), doc tests and release
build. The first workspace test attempt encountered sandbox-denied loopback binding
in 11 existing checkpoint fixtures; the authorized retry passed without source
changes, and both logs are retained. See the
[validation record](baselines/2026-09-15-consensus-beacon-body-memory.json).
PR/CI completion remains pending before merge.

This change removes unbounded optional body retention, deep serving copies and
whole-stream output accumulation. It adds one shared allocation per admitted
body, bounded FIFO work and atomic ownership counters. No benchmark, remote test,
production-data change or throughput claim accompanies it.

**Still open:** aggregate/concurrent incoming encoded and decoded responses,
typed Beacon decoding allocations, encoded buffers across simultaneous sends,
ancestry/history retention, and the broader offline audit. The body cache's
limits are not a bound on total process memory. Existing successful log-query
semantics are unaffected; optional P2P serving availability is a separate contract.
