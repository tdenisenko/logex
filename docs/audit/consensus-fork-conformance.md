# Consensus fork conformance

Base: `d67f9360`, after PR #155. This batch resolves historical digest and
genesis-finality conformance leads, checks the activated mainnet schedule and
adds independent published SSZ controls. It does not complete the networking,
retention or stored-state integrity audit.

## Findings

| ID | Severity | Behavior and correction |
| --- | --- | --- |
| B1-23 | P2, historical interoperability | The epoch-dependent digest applied Fulu's blob-parameter shift to every epoch. Exact context checks consequently rejected compliant pre-Fulu responses and derived incorrect missing cache contexts. Return the plain digest before the explicit Fulu activation epoch; retain the existing shifted rule afterward. |
| B1-24 | P2, update conformance | Genesis finality and absent finality were confused with ordinary finalized headers. A valid genesis proof uses the zero leaf and a completely default header. A nonzero finality branch identifies presence, including genesis; an absent branch requires the default header and no proof check. Preserve presence in update selection and reject inconsistent headers. |
| B1-25 | P2, update conformance | A nondefault next committee with an all-zero branch could be treated as present if the signed fixture header committed to that constructed proof. The branch now determines presence; an absent branch requires the default committee. Existing valid committee proofs continue to verify. |

Finality status reports the attested update's fork, including when the finalized
header is genesis or from an earlier fork. A before-fix control reproduced a
Deneb update labeled Capella. Shared summary construction now uses the attested
header; the standalone finalized-header metadata override and unused
`MissingFinalizedHeader` error variant are removed.

## Authoritative rules and schedule

The [specification correction](https://github.com/ethereum/consensus-specs/commit/4d623657a12ef326b7a50107589e6b0c33b439ec)
explicitly keeps the original digest for epochs before Fulu. The independently
maintained [Prysm implementation](https://github.com/OffchainLabs/prysm/blob/5c92ffe9173a6da349e83f87c41a3bdbc46554e4/config/params/config.go#L643-L663)
makes the same distinction. The older Fulu helper referenced by PR #155 omitted
this historical dispatch; that ambiguity is now resolved with pinned sources.

The [official mainnet configuration](https://github.com/ethereum/consensus-specs/blob/81ce8fd6f88d1fd04299010f58266dd082110e9c/configs/mainnet.yaml)
matches the repository's activated fork and blob schedules as reviewed on
September 15, 2026. No activation date is invented for disabled future forks.
Fulu starts at epoch 411392; blob transitions remain 412672/15 and 419072/21.
The explicit activation field is checked by independent boundary vectors.
[Source records](baselines/2026-09-15-consensus-fork-sources.json) retain revisions
and local evidence hashes.

The independent calculation hashes `version[4] || zero[28] || genesis_root[32]`
and keeps the first four bytes. From Fulu onward, XOR those bytes with the first
four bytes of SHA256 over the blob activation epoch and maximum blob count,
each encoded as a little-endian `u64`. Before the first BPO, these blob parameters
are Electra's epoch 364032 and count 9.

| Activation epoch | Expected digest |
| --- | --- |
| 0 | `b5303f2a` |
| 74240 | `afcaaba0` |
| 144896 | `4a26c58b` |
| 194048 | `bba4da96` |
| 269568 | `6a95a1a9` |
| 364032 | `ad532ceb` |
| 411392 | `cc2c5cdb` |
| 412672 | `cb0d1acc` |
| 419072 | `8c9f62fe` |

Finality presence and default-header requirements follow the
[Altair light-client protocol](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/altair/light-client/sync-protocol.md#validate_light_client_update),
with fork-specific header validation and generalized indices. A slot-zero
header is not sufficient: every raw header field must be default before the zero
proof leaf is used. Ordinary nonzero finalized headers still prove their beacon
header root. This corrects accepted/rejected structures without treating a local
synthetic fixture as a mainnet authenticated update.

## Scope and cost

The canonical digest helper is shared by bootstrap/range/finality/optimistic RPC
checks, beacon block context checks, cached-context normalization and local
network metadata. Correcting it fixes each historical path together. Current
Fulu/BPO digests remain unchanged. Reverse digest lookup keeps its existing
plain-version recognition for peer discovery; it does not bypass exact payload
epoch checks.

An old cache containing an incorrect supplied historical digest is rejected on
restore. It is not silently relabeled. Legitimately absent context is derived
from the decoded object epoch. This follows the user's compatibility waiver;
no migration or modification of existing user data is performed.

The pre-Fulu branch avoids the blob-parameter hash entirely. The genesis change
adds no durability barrier, network round trip or new signature verification.
No benchmark or percentage performance claim is needed for these correctness
changes.

Committee presence follows the same branch-based rule. Reject inconsistent raw
committee/branch pairs before creating an optional verified committee, and report
the actual attested slot in shape errors. Existing rejection of a default
all-zero committee with a nonzero branch is retained. No wider acceptance of
invalid committee keys is introduced.

## Validation and remaining work

Independent SHA256 vectors cover the epoch before, at and after each mainnet
fork/blob transition. Historical RPC and Electra beacon controls use literal
expected contexts, so an error shared by helper and fixture cannot hide the
defect. All three focused controls failed before the digest correction.

Genesis controls exercise signed bounded Capella, Deneb and Electra updates,
standalone and range forms, cached validation and best-update presence semantics.
Valid absent-finality controls use an attested state root independent of an
all-zero proof reconstruction. Invalid controls retain correctly formed local
signatures while varying header/proof consistency.

The [13 published SSZ fixtures](../../crates/logex-cl/tests/fixtures/consensus-spec-tests/README.md)
exercise all four payload families for Capella, Deneb and Electra, plus Fulu's
inherited range layout. Tests call the production decoders, compare decoded
fields and independently calculated beacon-header roots, reencode the exact
bytes, and check pinned upstream file hashes. Random official SSZ vectors
establish serialization conformance; they do not represent authenticated mainnet
blocks or committee signatures. The fixture README states which fields and roots
are asserted and which are retained only as source evidence.

The completed integrated consensus-crate run passes 179 tests (one ignored),
including the official fixtures and committee controls; focused strict Clippy
also passes. Full workspace gates and CI/merge records are being completed. Live sync and the
staging soak remain later gates.
