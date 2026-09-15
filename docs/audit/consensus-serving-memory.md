# Consensus serving memory and temporary availability

This milestone extends payload ownership to outgoing response queues and encoded
writers. It also replaces payload-sized diagnostics and distinguishes a remote
rate-limit response from a peer fault. The broader offline audit remains open.

## Findings and scope

| ID | Severity | Evidence / affected behavior | Disposition |
| --- | --- | --- | --- |
| B3-45 | High | Each writer retained only one encoded chunk, but concurrent writers had no shared allowance. Copied light-client payloads could also wait in response channels without a reservation. | Shared serving admission before queuing, held through the writer and failed-send handling. |
| B3-46 | Medium | Failed-send diagnostics unconditionally formatted entire response byte vectors, then retained and copied that string into status. Large LC responses could become still larger diagnostic strings. | Bounded variant/count/byte/error-code summaries. |
| B3-47 | Medium | Remote rate-limit code 139 was counted as an ordinary fault. LC/Beacon responses caused cooldown followed by fault disconnection; repeated Status responses could also disconnect a healthy busy peer. | Brief per-peer availability deferral; repeated Busy for the same request type triggers nonfault rotation and a temporary redial delay. |
| B3-48 | Low | The singleton LC writer applied the general 10 MiB body limit rather than its smaller protocol-specific bound. A direct codec caller could emit a response that the receiving codec rejects. Normal cached payloads already undergo validation. | Apply the singleton protocol limit before encoding/writing; this is an output-boundary correction, not evidence of invalid normal cache contents. |

## Ownership and resource policy

One shared outgoing Beacon pool allows 128 MiB, with a separate 8 MiB serving
control/LC pool. These are independent of the incoming pools. Before placing a
response in a libp2p channel, reserve its owned raw Vec capacities plus the largest
possible single encoded chunk. Optional shared Beacon bodies remain charged to
the existing body cache and are not charged again as owned raw data.

The private response admission state distinguishes pending, ready and busy.
Ready ownership cannot be cloned or charged a second time by the writer. A direct
codec caller still undergoes admission if its response was not prepared by the
network. One reservation covers the entire stream, so writing a later chunk does
not need to reacquire capacity while holding a partial response. Failed channel
sends return the intact budgeted wrapper through bounded diagnostic handling;
only that failure return allocates a small Box.

If admission fails, the original response and reservations are dropped before a
fixed two-byte empty rate-limit response is written. This uses the existing code
139 and zero SSZ length encoding, without a compressor or payload allocation. No
success prefix is written by this fallback. Normal framing, context, type and
stream-wire checks still apply before success bytes are sent.

The encoder writes through a fallible bounded Vec adapter. Its growth is clamped
to the prepaid maximum rather than relying on generic Vec write growth. It retains
the same Snappy wire bytes and propagates underlying I/O/allocation errors,
including final compressor flush. Encoded buffers drop before their reservation
on success, write failure and cancellation.

## Availability and performance implications

A remote Busy response is temporary availability information. It briefly defers
that peer without changing fault counters or penalizing healthy peer inventory.
The first two replies delay requests for one second. The third Busy reply for the
same request type closes the connection as planned availability rotation and
prevents redial for 30 seconds, freeing a slot for another peer. The counters
saturate at three within the existing bounded peer lifecycle and survive short
retry expiry and reconnect. Only a successful response of that same request type
resets its counter; unrelated Status or Ping responses cannot mask unavailable
history or bootstrap work. A still-busy peer gets another attempt after redial,
then rotates again until it successfully serves that request type.

Pending requests are cleared before rotation so later transport failures are
stale. An incoming reconnect during the redial delay is closed before recording
dial success or dispatching work. A planned-close marker survives the ordering
where the transport has closed but its close event is still queued. The configured
connection limit permits only one established connection per peer. Non-rate-limit
error codes keep their existing handling.

The initial one-second-only deferral passed its isolated controls but failed a
later scheduling review: capable peers repeatedly replying Busy could occupy
all connection slots indefinitely. The finite attempt policy corrects that gap;
initial gate results are retained separately from final revised-source acceptance.

Admission adds a bounded metadata scan and atomic reservation per response. There
is no new disk write, fsync or waiting for quota while holding response bodies.
Under pressure, optional serving requests may receive a rate-limit response.
Diagnostic size no longer scales with body bytes. No benchmark or throughput
percentage accompanies the change; the user ended the extended benchmark campaign.

The serving figures are payload-capacity limits, not total process RSS. Fixed
synchronous compressor scratch, allocator/reallocation transients and transport
or collection metadata remain outside them. The current network constructs one
LC response synchronously before admission: existing inbound count validation
caps it at 128, and validated cached updates are at most 26,936 bytes each. This
single temporary clone window is about 3.29 MiB plus metadata; after preparation,
queued copies remain charged. Existing trusted snapshot/history retention and
typed Beacon allocations are separate audit items.

## Validation

Isolated offline controls cover queued ownership, shared pool pressure, control
isolation, one reservation across streamed writes, cancellation, failed sends,
cache ownership, fixed fallback bytes, bounded diagnostics and remote Busy
handling. Encoder output is compared byte-for-byte with the pinned original
FrameEncoder over deterministic data across frame boundaries. No real peers,
production data, remote tests or performance measurements are used.

The revised source `c10d4bef` passes all 300 consensus tests (one ignored) and
independent review. Three original-function controls reproduce missing writer
admission, oversized singleton output and encoded Vec excess. Network controls
reproduce payload-sized diagnostics and rate-limit fault attribution. The late
liveness control restores the exact initial rate-limit arm and fails at the
missing planned rotation after the third Busy reply. These restore isolated
original paths in candidate harnesses, not full baseline checkouts. Tests model
connection events and enqueue an alternative dial without polling a real socket;
they establish the scheduler transition, not live-network availability.

Exact source bindings and results are recorded in the
[validation record](baselines/2026-09-15-consensus-serving-memory.json). The initial
source and its successful gates/CI are retained separately. The revised source
passes all seven local gates: vendor integrity, formatting, workspace check,
strict Clippy, 1,378 workspace tests (23 ignored), documentation tests and the node
release build. Revised-head CI and merge remain pending.
