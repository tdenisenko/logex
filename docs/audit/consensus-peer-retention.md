# Consensus peer inventory and retention

Base: `aa624447`, after merged PR #157. This pass reviews peer inventory bounds,
address-family eligibility and the work used to select the saved peer cache.
Final implementation: `dc1c07d3af5d94fc46f38b0dd3b8f8fabf3612bc`.

## Findings

| ID | Severity | Evidence and correction |
| --- | --- | --- |
| B3-18 | P2, memory and bookkeeping | Disconnected-peer pruning counted only dialable peers. Inbound peers can leave lifecycle records without a dialable ENR; 501 bounded records remained after pruning despite the existing 500-peer limit. Count the union of both inventories once and clear all associated state for evicted peers. |
| B3-19 | P2, discovery liveness | Startup filtered peer addresses by configured IPv4/IPv6 families, but live ENR observation used an unfiltered helper. An IPv6-only ENR entered an IPv4-only inventory and could count toward the discovery reserve despite having no usable dial address. Saved-cache selection also accepted an IPv4-discoverable peer offering RPC only over IPv6. Use shared configured-family eligibility for live observation and saved-cache selection. |
| B3-20 | Implementation cost | Selecting the saved peer cache serialized each ENR and parsed it again for both sides of every sort comparison. Pinned `enr` 0.13.0 parsing verifies the record signature, repeating expensive validation on already decoded records. Compute priority from the decoded record once and preserve the existing priority/tie-break ordering. |

The existing retention limit, preference ordering and protected active/pending/
bootnode states remain the policy. The fix must deduplicate peers present in both
inventories and preserve cleanup of support, status, failure and rate-limit maps.
Periodic pruning does not impose a strict instantaneous bound between events.
Protected records can exceed the cap by themselves; the existing policy retains
them rather than canceling active requests or dropping configured bootnodes.

The 256-peer persisted cache is a reconnection aid. This pass does not change
consensus trust, stored anchors, history payload retention or cache-file durability.
Peer-cache crash recovery and malformed-file startup handling remain separate
work, as do aggregate response memory and gossip conformance.

This pass establishes address-family eligibility, not ENR freshness. An updated
record without compatible RPC addresses can leave older addresses in memory.
Blindly deleting on any such event is unsafe for liveness: pinned `discv5` 0.10.4
emits `Discovered` before its routing-table sequence checks, so an older event can
arrive after a newer record. A separate recorded follow-up must retain sequence
authority, handle endpoint withdrawal and bound that metadata. The saved-cache
selector evaluates the current ENRs supplied by the routing table.

## Validation and cost

Before-fix tests reproduced the inbound-only retention gap, live address-family
mismatch and incompatible saved cache using temporary data and scripted objects.
The saved-cache control inserts an IPv4-discoverable, IPv6-RPC-only ENR into the
real discovery table, then calls the actual persistence method and reloads the
file. IPv4 excludes it; switching the fixture to IPv6 retains it. The network
fixture never starts discovery, polls its swarm or contacts a peer.

Mixed-inventory, protection, cleanup, ranking and repeated-prune controls pass.
All six focused retention/cache tests and strict CL Clippy pass. Selection controls
check a preferred peer, exclusion of a peer lacking Status, deterministic tie
ordering independent of input order, retained counters and the 256-entry cap.
All seven local gates passed on this source, including 1,286 workspace tests
(23 ignored), documentation tests and the release build. The [gate record](baselines/2026-09-15-consensus-peer-retention-gates.json)
includes toolchain, commands and evidence hashes. Independent source review
found no actionable issue in this bounded patch. CI and merge remain pending.
No timing benchmark, remote host or production data is used.
Removal of repeated ENR parsing is an implementation-level cost reduction, not
a measured throughput percentage or complete event-loop performance claim.

The pinned `enr` 0.13.0 `FromStr` path decodes base64/RLP and verifies the signature.
The old sort called it for each comparison operand. The replacement obtains
the peer ID from each already decoded record and computes its priority once.
Signature validation when records first enter the discovery/cache pipeline stays
in place. Selected metadata borrows the lifecycle entry rather than cloning it.

Removed the unused unfiltered observation helper and sort-time ENR reparsing.
Shared address extraction prevents startup, live observation and saved-cache
selection from implementing different family eligibility rules.
