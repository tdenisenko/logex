# Consensus gossip message IDs and bounds

Base: `0d5104e6`, after merged PR #159.
Final source: `d297b77b315012f8e41e87042868bee6e9eaac43`.
All seven local gates passed; PR/CI and merge remain pending. This pass covers modern light-client
gossip IDs, wire/decode limits
and the pinned Ethereum gossip configuration. It does not close the entire
gossip, fork-transition or memory-admission audit.

## Findings and protocol references

| ID | Severity | Before-fix evidence and correction |
| --- | --- | --- |
| B3-23 | P2, interoperability | The default identity transform passes compressed wire data to the message-ID function, which always hashes it with the valid-Snappy domain. Different encodings of the same payload therefore get different IDs, and malformed Snappy uses the wrong domain. Hash decoded content for valid bounded Snappy and raw content under the invalid domain otherwise. |
| B3-24 | P2, protocol bounds | The same 10 MiB + 1024 constant governs the wire frame and decoded payload. The pinned specification separates a 10 MiB decoded cap, Snappy worst-case compressed size and a further 1024-byte frame allowance. Enforce the distinct bounds before decoded allocation and apply supported light-client SSZ limits before application decoding. |
| B3-25 | Protocol configuration | Generic library defaults differ from Ethereum's mesh target/low watermark, heartbeat, full-message cache windows and duplicate-ID lifetime. Set the pinned protocol values explicitly; preserve defaults where the protocol does not specify replacements. |

The [Altair v1.6.0 message-ID specification](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/altair/p2p-interface.md)
requires a 20-byte SHA-256 prefix including domain, little-endian topic-byte
length, topic and decoded payload for valid Snappy; invalid Snappy hashes raw
payload under a separate domain. These are modern light-client topics. The
Phase 0 topic algorithm is distinct and is not claimed supported by this pass.

The [Phase 0 v1.6.0 networking specification](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/phase0/p2p-interface.md)
sets the decoded limit at 10 MiB, compressed limit at Snappy's worst-case size,
and frame allowance at a further 1024 bytes. It also specifies mesh target 8,
low/high watermarks 6/12, lazy gossip target 6, 700 ms heartbeat, 60-second fanout,
six full-message windows, three gossip windows and a duplicate-ID lifetime of
two epochs (768 seconds on mainnet).

## Decode ownership and cost

Preserve identity transformation of admitted wire bytes. A small preflight
transform rejects globally oversized compressed or declared-decoded payloads
before message-ID computation; malformed payloads within those bounds still
reach the invalid-Snappy ID path and application rejection. No decoded-message
side cache or internal payload envelope is introduced.

ID generation performs bounded decompression because valid IDs depend on decoded
content. Application decoding separately checks the supported SSZ type bound
first. A valid light-client update is decoded twice, bounded to at most 2,120
bytes for finality or 1,032 bytes for optimistic updates. This explicit small
correctness cost avoids extra cache ownership and payload-format complexity.
No benchmark or node-throughput claim is made. Unknown-topic application
fallback still uses the global decode cap before Ignore; its unnecessary
second decode remains part of the topic-admission follow-up.

Globally oversized messages rejected by the transform do not reach application
event bandwidth/decode-failure counters. This follows the pinned library's early
rejection path; those application metrics must not be interpreted as complete
wire-traffic counters. The pinned library allocates the bounded RPC frame and
clones the raw message before calling the transform; this is not a limit on all
network allocations. ID decoding also precedes duplicate lookup.
The longer protocol duplicate-ID lifetime increases
retention of unique IDs; it is time-based, not an aggregate memory or entry cap.

## Validation and remaining scope

Two initial independent digest tests reproduce both ID bugs on the base code.
All 96 network tests and strict CL Clippy passed. Independent review matched
all six fixed digest oracles and reconciled the final source files. All seven
local gates passed on this commit, including 1,299 workspace tests (23 ignored),
documentation tests and release build. See the [gate and review record](baselines/2026-09-15-consensus-gossip-conformance-gates.json)
for toolchain, commands, log/specification hashes and immutable source binding.
All six Linux/macOS CI jobs passed in run `34924077952` at head `cccf6190`.
[PR #160](https://github.com/tdenisenko/logex/pull/160) merged as `10d05727`
on September 15 at 03:18:28 UTC; see the [CI and merge record](baselines/2026-09-15-consensus-gossip-conformance-ci.json). All fixtures are bounded and
offline. No live peer, production
data or Mac mini work is part of this change.

Unknown/retired-topic application acceptance, topic-to-fork validation, per-fork
timing rules, supervision and complete resource accounting remain explicit
follow-ups. Required protocol values are not a substitute for those reviews.

## Cleanup

Removed the test that mirrored the old compressed-byte hash and the shared
wire/decoded limit constant. Reused existing RPC SSZ layout bounds via a crate-
visible helper; the RPC behavior itself is unchanged. No additional obsolete
caller or unused compatibility wrapper was found in the touched paths.
