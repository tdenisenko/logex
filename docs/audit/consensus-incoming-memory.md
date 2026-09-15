# Incoming consensus response memory

This milestone bounds owned RPC payload buffers and preserves historical retry
progress under local memory pressure. It does not bound total process memory or
close the remaining consensus audit.

## Findings

| ID | Severity | Evidence and affected behavior | Disposition |
| --- | --- | --- | --- |
| B3-43 | High | The old 256 MiB encoded-stream and 128-chunk limits allowed up to 1.25 GiB of decoded Beacon bodies in one response, with no shared allowance across responses. Compressed bodies and queued events could retain these buffers concurrently. | Incremental chunk reads, shared reservations, a decoded response ceiling and nonwaiting local admission. |
| B3-44 | Medium | Response chunk count was checked against the protocol maximum during decoding and against the actual smaller request only after the response was allocated. | Bind the actual requested count to the per-stream codec and reject a surplus success code before reading its body. |

## Resource policy and ownership

All ten RPC behaviours share one set of pools, including their codec and
connection clones. Beacon responses use a 256 MiB pool; control and light-client
responses use a separate 8 MiB pool. Each response retains at most 64 MiB decoded.
These are internal runtime limits, with no persisted format or user configuration
change. Existing protocol/type and stream-wire limits remain enforced.

Before allocating a chunk, the reader validates its declared size and atomically
reserves its maximum accepted encoded capacity plus declared decoded capacity.
It validates each frame header before allocating or reading the advertised body.
Only the current encoded chunk is retained; its buffer is freed before the
reservation shrinks to the decoded Vec capacity. Completed decoded bodies keep
unique reservations through queued libp2p events and synchronous validation.
Payload fields drop before their reservation fields, including cancellation,
invalid tails and an error response waiting for stream closure.

Admission never waits while retaining partial responses. A rejected response
releases all its private chunks and cannot publish a prefix. A terminal remote
error supersedes the success prefix. The existing trust, root, range and fork
checks still run before bodies are accepted by the application.

The pinned libp2p request-response handler clones a codec per outbound stream,
then uses that same clone to write the request and read its response. This permits
retaining the actual request count without a global codec/request lookup.

## Retry behavior and performance implications

Typed local capacity/allocation failures clear pending ownership and defer
requests for one second without incrementing peer-failure counters. Beacon pool
pressure defers both historical request families; control failures defer only
the affected family. A decoded response ceiling additionally halves the actual
failed batch count, down to one. Builders and previews use the same cap, preserving
request metadata and complete replacement requests. Stale failures cannot change
caps or deadlines after request ownership has been removed.

A completely validated, useful, nonempty response that fills its requested batch
can restore that family's cap gradually. Growth is at most twice the current cap,
up to 128 and the count implied by 64 MiB divided by the largest body in the
successful response. A successful response never lowers the cap. Empty, short,
invalid and duplicate-only results do not justify growth. Heterogeneous later
bodies can still require another reduction; this is not a prediction of future
body sizes. Recovery avoids a permanent small-batch penalty after one outlier.

The implementation removes whole-response encoded accumulation. The decoder uses
fallible exact reservation and a fixed EOF probe to make capacity ownership
explicit. An initial concern that the old EOF read grew the Vec was not reproduced
by the retained original-decoder controls on the pinned toolchain; it is not a
confirmed finding or measured allocation improvement. Costs include atomic
admission/release per chunk, bounded
frame reads, fallible allocation and destination initialization before decoding.
There is no lock wait, new disk write or fsync in this path. The user ended the
extended benchmark campaign: no throughput improvement or regression percentage
is claimed. Large historical responses may require more requests under the
explicit memory policy; ordinary requests retain their previous maximum size
until a local decoded ceiling is encountered. Separate control capacity protects
light-client recovery from historical payload pressure.

## Validation and remaining scope

Offline tests use small configured pools, in-memory streams, controlled time and
existing Beacon SSZ fixtures. They cover queued ownership across clones and pools,
concurrent primitive admission, cancellation during partial chunks and terminal
EOF, invalid tails, surplus requested counts, smaller retries, pending metadata,
no peer blame and gradual cap recovery. A generous-budget two-body positive
control and smaller retries both publish the same two distinct decoded roots.
The second body is a synthetic SSZ field mutation with a recomputed root; this
control does not assert proposer-signature validity or canonical chain membership.

Validation results and immutable source bindings are recorded in the accompanying
[validation record](baselines/2026-09-15-consensus-incoming-memory.json). All seven local
gates passed on source `bb69d1b0`: vendor integrity, formatting, workspace check,
strict workspace Clippy, 1,366 workspace tests (23 ignored), doc tests and release
build. PR/CI and merge remain pending at this writing.

The pool figures cover payload Vec capacity, not allocator rounding, collection
metadata, libp2p transport buffers or total RSS. Snappy's fixed synchronous decoder
scratch (about 142 KiB per decoder in the pinned implementation), typed Beacon
structures, ancestry/history retention and encoded buffers across simultaneous
outgoing writers remain outside these pools. Cached optional bodies have their
separate ownership budget from PR #166. Persistent trusted-state and query memory
remain separate audit items. No remote tests, live sync, production data changes
or performance measurements were performed for this milestone.
