# Consensus gossip admission and forwarding

## Scope and invariants

Continue batch 3 from merged PR #160 (`10d05727`), with source commit
`d3aeb891875dbb52756f556be381845176b71376` (following `4bb4c30a`). This pass reviews supported
light-client gossip topics, fork context, propagation timing and the distinction
between processing a verified update locally and forwarding it to peers.
All fixtures use isolated temporary state and constructed signed light-client
objects. They do not establish canonical mainnet history or exercise live peers.

An admitted update must match its topic's attested-slot fork and supported outer
SSZ schema. Signature slot determines timing and the existing signature-domain
rules separately. Valid updates may improve local participation state even when
they do not qualify for forwarding. A failed durable save must not publish a
candidate or mark a message as forwarded. Existing RPC verification semantics
remain the default outside the explicit controlled-clock gossip entry points.

## Findings

| ID | Disposition | Evidence and resulting behavior |
| --- | --- | --- |
| B3-26 | Confirmed admission defect | Application decompression precedes topic classification; only the current topic pair is handled, and decoded updates are not bound to the topic digest. Classify canonical supported topics first, preserve benign handling of recognized inactive topics, and validate attested-slot context for admitted topics. |
| B3-27 | Confirmed timing defect | Generic future-slot verification rejects early gossip and does not wait for the required block propagation interval within a slot. Apply a gossip-specific Ignore gate with an injected millisecond clock before shared verification. |
| B3-28 | Confirmed forwarding defect | Successful processing always returns Accept, even if the required store header did not advance. Store-based stale checks also suppress useful participation processing and the matching optimistic-after-finality exception. Separate local processing from bounded forwarding history. |
| B3-29 | Confirmed outer-schema gap | A signed Capella update encoded in an upgraded Deneb outer container passes the generic header verifier. Gossip must select its outer layout from the attested epoch; the valid nested-header upgrade support remains available to existing verification callers. |
| B3-30 | Confirmed local-state attribution defect | Missing next committees and farther catch-up return UnknownSyncCommitteePeriod solely because local verification state is unavailable. Ignore that exact error without publication, forwarding history or failure counts. Retain rejection for detected structural, proof and signature failures. |

The [Altair v1.6.0 light-client networking specification](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/altair/light-client/p2p-interface.md)
defines topic context using the attested header's epoch. It separately defines
forwarding freshness, propagation timing, store advancement and the matching
optimistic/finality exception. The
[Altair topic transition rules](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/altair/p2p-interface.md)
allow overlap across forks and prohibit penalizing a peer merely for delivering
an old-topic message after the transition. The
[Phase 0 topic rules](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/phase0/p2p-interface.md)
require rejecting unknown topics.

The pinned [Altair timing helper](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/altair/fork-choice.md)
and [Phase 0 slot-component arithmetic](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/phase0/fork-choice.md)
use integer division: 12,000 milliseconds times 3,333 basis points divided by
10,000 is 3,999 milliseconds. With the mainnet 500-millisecond clock allowance,
the earliest forwarding threshold is 3,499 milliseconds after signature-slot
start. These values apply through the configured Fulu/BPO schedule; no claim is
made for an unscheduled future fork with changed timing.

## Processing, persistence and cost

Processing ignored valid updates requires preserving the better cached response
independently of changes to the verified store. Gossip-specific recorders retain
a newer optimistic response and rank finality responses by finalized slot,
supermajority, attested slot and participation. Equal priorities retain the
existing response. This is local serving-cache selection, separate from the
protocol's runtime forwarding history.

The writer checks whether either the verified store or selected payload changes
while holding writer ownership. A no-op skips the whole historical snapshot
clone, serialization and durable replacement. Changed state uses the existing
complete candidate, durable write, then publication transaction. The network
seeds and materializes historical headers only when a verified header changes; other changed state may still drive scheduling. RPC recording
retains its existing behavior. Necessary bounded signature/SSZ checks remain;
this pass makes no benchmark or node-throughput claim.

## Validation and remaining scope

Final source passes 240 consensus tests (one existing ignored test) and strict CL
Clippy. Independent source review and immutable commit binding found no
remaining patch finding. Workspace tests passed (1,318 tests, 23 ignored), as
did vendor, format, check, Clippy and documentation tests. Release linking
initially ran out of disk space; its unchanged-command retry passed after
cleanup of only the repository's disposable Cargo debug incremental cache.
All seven required local gates passed on `d3aeb891`. See the [gate and review
record](baselines/2026-09-15-consensus-gossip-admission-gates.json) for commands,
source binding, log hashes and retained attempts. PR/CI remains pending.
The first gate attempt on `4bb4c30a` was deliberately stopped during tests for
the availability correction after vendor, format, check and Clippy passed.
Its interrupted logs remain recorded separately from final validation.
The initial two regression controls fail behaviorally on the base: a valid
Snappy payload on an unknown topic is ignored instead of rejected, and a
one-participant finality update is forwarded despite no finality advancement.

The [pinned light-client sync process](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/altair/light-client/light-client.md)
describes fetching missing committees while catching up. Unavailable local
committee state cannot establish that a peer supplied an invalid signature;
its exact verification error now produces Ignore without publication or forwarding.
Both missing-next-committee and farther-catch-up controls fail on the prior source
and pass after the correction, with the same bytes authenticated by a matching
fixture bootstrap. Available-committee negative controls retain rejection for
malformed data, invalid execution proofs and invalid signatures.

The older-signature-period control protects benign late messages after committee
rotation. The BPO transition control invokes actual subscription maintenance with
a controlled slot: pre-subscribe, rotate, process a last-pre-fork attestation with
a first-post-fork signature, then retire after two epochs. Exact matching tests
include a valid different aggregate at the same attested slot. Cache recorder
tests isolate trusted store publication; manually varied participation counters
in those recorder tests are not claims about a verified chain trajectory.

Early messages are ignored without a replay queue. RPC recovery remains available.
Forwarding history is bounded to the latest finality correspondence and an
optimistic slot, plus one pending acceptance; successful library reporting means
forwarding was invoked, not acknowledged by remote recipients. History is local
to the running process. Unknown/inactive topics skip application decompression;
message-ID decoding before duplicate lookup remains globally bounded as in PR #160.

Whole consensus snapshot integrity/retention, complete network memory accounting,
peer-cache recovery and broader supervision remain batch 3 follow-ups. The
unchanged RPC stale prefilters also need a separate participation-only response
review with an actual reproducer before selecting any policy change. This pass
does not complete the offline audit, establish live readiness, or touch the Mac
mini or existing user data.

## Cleanup

Replaced gossip-only pre-processing stale checks and unconditional forwarding
with explicit admission, processing and forwarding outcomes. RPC stale checks
remain used. Shared writer ownership retains durable publication while permitting
no-op transactions. Removed duplicate gossip-handler success/error branches;
existing raw-Snappy ID handling remains required. Static call-site review and
strict CL Clippy found no additional obsolete caller in the touched paths. The controlled-clock wrappers preserve original
RPC wall-clock entry points.
