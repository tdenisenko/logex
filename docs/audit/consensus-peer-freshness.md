# Consensus peer record freshness

Base: `c7ef4411`, after merged PR #158. Final functional source: `57c36f760100662fe94eb88464cffee0bb5d5ae7`.
All seven local gates passed; PR/CI and merge remain pending. This pass follows peer records from startup and discovery through
address selection, status, pruning and the saved reconnection cache.

## Confirmed findings

| ID | Severity | Before-fix evidence |
| --- | --- | --- |
| B3-21 | P2, connection liveness | Observing sequence 2 at TCP port 9002 and then sequence 1 at port 9001 replaced the current dial address with port 9001. |
| B3-22 | P2, connection liveness | A newer valid ENR without an RPC endpoint left the prior endpoint in dialable inventory. Saved-cache selection supplied with the old routing-table ENR also retained that withdrawn endpoint. |

Three initial focused tests fail on the base implementation. These are scripted
state transitions with locally signed fixture records, not observations of a live
peer. Equal-sequence conflicts, family/network withdrawal, startup ordering and
pruning require the additional controls recorded below.

## Invariants and implementation direction

[EIP-778](https://eips.ethereum.org/EIPS/eip-778) uses an increasing sequence
number to identify updated signed records; endpoint fields are optional. A valid
new record may therefore withdraw an endpoint. The pinned `discv5` 0.10.4
`service::discovered` emits discovery events before its routing-table sequence
checks. Event arrival order cannot establish record freshness.

Keep the latest canonical ENR in the existing peer lifecycle entry. Replace it
only on a strictly newer sequence; re-evaluate local eligibility from that
canonical content rather than an incoming older or conflicting equal-sequence
record. Remove withdrawn or ineligible RPC endpoints from dialable inventory and
live discovery counts. Re-evaluate retained records on local fork transitions.
Startup boot/cache ordering uses the same authority. Saved-cache selection takes
the newer of its routing-table record and retained record, preferring retained
content on equal sequences. It keeps the existing candidate set and ordering.

The existing inactive-peer pruning policy owns this metadata. No separate
unbounded tombstone collection or persisted format is introduced. Sequence
history expires when a peer is evicted; restart retains only records surviving
in the existing reconnection cache or built-in configuration. This is a bounded
connectivity cache, not a durable global record-history guarantee. Protected
active/pending/bootnode states and periodic pruning retain their existing limits.
Current connections and owned requests do not become invalid merely because a
peer advertises a new endpoint.

## Validation

Initial reproduction is complete. All 91 network tests, eight focused freshness
controls and strict CL Clippy passed. Independent source review found no
actionable issue in this bounded patch. All seven local gates passed on this source, including 1,294 workspace tests
(23 ignored), documentation tests and the release build. The
[gate record](baselines/2026-09-15-consensus-peer-freshness-gates.json)
contains toolchain, commands, log hashes, focused evidence and immutable review
binding. PR/CI and merge remain pending. Fixtures use temporary directories and scripted
objects without starting discovery, polling a swarm or contacting peers.
No timing benchmark or Mac mini work is planned for this change.

Review identified additional integration controls: a routing-table record can be
newer than retained state; known-no-Status peers must stay excluded across
refreshes; canonical eligibility must be recomputed after restoring the winning
cache metadata in both support directions; live discovery-count provenance must
survive a period of fork ineligibility. These were draft-review findings, not
additional failures merged into the repository. Final source reconciliation confirms their resolution.

## Cleanup and cost

Removed address-only ENR observation and centralized canonical eligibility. Keep
canonical metadata in already-pruned lifecycle entries; observation clones the
ENR only when accepting a newer record. Saved selection remains bounded by the existing
candidate set and 256-entry output cap; this pass does not establish a total
network memory budget or measured throughput improvement. Removed the redundant startup retained-record vector. Persistence compares
against actual loaded file contents so an ordinary subsequent save normalizes
obsolete, duplicate or ineligible records.

