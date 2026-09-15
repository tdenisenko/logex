# Consensus request framing and ownership

Base: `e79e2d23`, after merged PR #156. This pass covers RPC payload boundaries,
pending request metadata, late failures and response shape. Source `c2b1a315`
contains the completed fixes and passed all seven local gates. This record does
not close the CL networking batch.

## Confirmed findings

| ID | Severity | Evidence and affected behavior |
| --- | --- | --- |
| B3-09 | P2, input correctness | Both Snappy prefix decoders copied `min(read, remaining)` and discarded surplus frame output. A nine-byte frame declared as eight bytes was accepted. The analogous 4,097/4,096-byte buffered-read boundary is covered after the fix and supported by the pinned decoder's buffering; the before run stops at its first failing eight-byte assertion. |
| B3-10 | P2, input correctness | A tenth uint64 varint byte with excess payload bits wrapped during shifting. A bounded ten-byte fixture reproduced acceptance of an overflowing length as eight. |
| B3-11 | P2, interoperability | Metadata V1 included the V2 syncnets byte in both encoder and decoder. A literal sixteen-byte V1 response failed despite passing the old same-implementation roundtrip. |
| B3-12 | P2, allocation/work bounds | Fixed-size responses used the generic ten-MiB payload ceiling before type validation. Single-response partial reads repeatedly allocated and decompressed the entire received prefix. |
| B3-13 | P2, request ownership | Light-client range requests retained kind and peer but lost requested start/count. Cryptographic verification did not establish correspondence to the requested period interval or chunk count. |
| B3-14 | P2, peer recovery | Connection closure cleared request ownership before libp2p delivered queued outbound failures. Failure handlers still penalized the peer after cancellation, including planned closure. |
| B3-15 | P2, history response correctness | A repeated Beacon block was accepted and cached for a single requested root. Existing membership/window checks did not establish chunk cardinality, uniqueness or ordered chain continuity. |
| B3-16 | P2, shutdown scheduling | The scheduler could send status work to a connected peer already queued for closure. A fixture with pending Goodbye and missing status reproduced new work on that peer. A queued inbound Goodbye failure can also reinsert an already disconnected peer into the closing set; the failed disconnect has no later closure event to clear it. |
| B3-17 | P2, response publication | A valid update followed by a malformed or context-invalid chunk could still publish the valid prefix and record success. The response must pass validation as a whole before publishing the staged candidate. |

Proof/signature verification remains required. Response-shape checks supplement
authentication; these findings do not establish acceptance of forged BLS proofs.

The codec now checks full frame output against the SSZ declaration and rejects
uint64 overflow. It uses type-specific allocation limits and correct sixteen-byte
Metadata V1 encoding. During fragmented single-response accumulation, each
completed frame is scanned once without decompression. Final decoding repeats one
linear boundary scan, then delegates one complete decompression and checksum pass
to `snap`. Valid nonminimal prefixes, padding, repeated stream identifiers and
adjacent response chunks remain supported within the compressed-byte limit.

Light-client requests retain start/count with pending ownership. Valid responses
must remain within that interval and have consecutive returned periods; an initial
unavailable period and a valid shorter response are allowed. Beacon ranges must
have increasing slots and unique roots, with direct parent continuity when step
is one. Missing slots are allowed; returned slots need not differ by one. Root
responses require requested membership, uniqueness and request cardinality, without
imposing an unspecified return order.

All candidates remain private until the response passes. A store-relative
irrelevant/unknown-committee response stops without publication or peer blame;
the current scheduler requests one light-client update at a time. Malformed,
context-invalid and other peer-fault responses reject the complete candidate.
Late outbound failures apply only while request ownership remains. Closure,
success and failure release the corresponding range metadata.

The scheduler skips closing peers. An attempted disconnect removes its closing
marker if the swarm reports no remaining connection, because no later closure
event is guaranteed in that case. Owned response payloads move into staging;
the handler no longer clones each payload while retaining the original stream.

## Protocol and dependency evidence

The pinned [Phase 0 networking specification](https://raw.githubusercontent.com/ethereum/consensus-specs/v1.6.0/specs/phase0/p2p-interface.md)
defines Metadata V1, uint64 prefixes, SSZ type bounds, compressed-byte limits and
range/root response rules. It permits nonminimal varints and partial responses.
The [light-client networking specification](https://raw.githubusercontent.com/ethereum/consensus-specs/v1.6.0/specs/altair/light-client/p2p-interface.md)
defines requested period intervals and consecutive returned periods.

Pinned `snap` 1.1.1 buffers whole frame output internally. A second arbitrary
decoder read cannot safely locate an RPC chunk boundary because it can consume
the following response. Framing must establish the boundary while the dependency
continues to perform decompression and checksum verification.

Pinned `libp2p-swarm` 0.47.1 emits connection closure before the queued request
failures from `libp2p-request-response` 0.29.0. The network admits only one
established connection per peer, so this finding does not assume multiple active
connections for one peer. Pending identity already includes RPC family and request
ID; the transport correlates peer/connection/request and unknown responses are
already ignored.

## Resource boundary and remaining work

No new benchmark or remote-host work runs in this pass. Implementation evidence
establishes bounded linear framing passes and no repeated decompression for every
fragment; it does not establish a node throughput percentage.

Aggregate Beacon response memory remains open: 128 chunks at the existing
per-chunk ceiling permit about 1.25 GiB decoded despite the 256 MiB wire bound.
That requires explicit aggregate accounting or bounded consumption, including
concurrent ownership and retry behavior. Smaller type-specific light-client caps
do not dispose this issue. Historical retention and complete snapshot integrity
also remain separate recorded work.

The retention follow-up must also cover inbound-only peers: disconnected-peer
pruning currently iterates the dialable inventory, while lifecycle records can
exist without a dialable entry. This is a concrete code-review lead awaiting a
bounded reproduction and disposition in the next retention pass.

## Validation

Fixtures use temporary data and scripted events; no production peers, data or
services are involved. All 41 focused RPC tests passed, followed by strict CL
Clippy. The network suite passed 76 tests before the final additional controls;
the final 11 lifecycle controls and a fresh strict Clippy run also passed.

Initial before-fix runs reproduced all four codec controls and all four network
controls. The network fixtures construct the real network object, queue requests
and deliver scripted responses/failures without starting discovery, polling the
swarm or contacting peers. Signed light-client controls distinguish a valid
requested single update from duplicate chunks on that same request. These tests
do not assert live transport timing or network throughput.

Separate before runs reproduced valid-prefix publication after an invalid tail
and the disconnected-peer closing marker. The latter tests the actual
`disconnect_now` leaf; dependency source establishes the delayed inbound Goodbye
event path, without constructing private transport IDs or introducing unsafe test
code. Signed wrong-request-period and stale previous-period responses are checked
separately: the former is rejected as an invalid response, the latter does not
blame the peer or mutate trusted state. History controls exercise short, empty,
malformed-tail, duplicate, reversed and skipped-slot cases.

The largest-layout codec fixtures extend existing four-byte execution extra data
to the permitted 32 bytes and adjust SSZ offsets. That changes commitments, so
these controls establish wire layout and size acceptance only, not proof validity.
One initial fixture-size assumption failed and was corrected before final checks.
An initial stale-response fixture remained legitimately relevant while the next
committee was unknown; a signed previous-period fixture establishes the intended
store-relative rejection instead. Both diagnostic outcomes are retained.

Removed repeated prefix decompression, obsolete generic prefix wrappers,
redundant error-size checks, invalid-chunk skip/publication paths and payload
clones. Independent source review checked pinned dependency behavior and the final
production diff; it found the closing-marker issue, verified its correction and
reported no further actionable issue in this scope. This is bounded review,
not a claim of exhaustive network or runtime correctness.

Source `c2b1a315` passed vendor verification, formatting, workspace all-target
check, strict all-target Clippy, 1,280 workspace tests (23 ignored), documentation
tests and the release node build. [Gate records](baselines/2026-09-15-consensus-request-lifecycle-gates.json)
retain exact commands, toolchain, source hashes, logs and focused review evidence.
CI and merge are pending.
