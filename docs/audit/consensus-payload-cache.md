# Consensus payload cache validation and serving

Base: `d0a31b2a`, after merged PR #154. This batch reviews cached consensus
responses, their wire context and the amount of data copied to serve a request.
The complete CL networking/retention audit remains open.

## Findings and changes

| ID | Severity | Evidence and correction |
| --- | --- | --- |
| B3-05 | P2, response correctness | Valid finality/optimistic gossip updates were saved with no RPC context. Serving forwarded that absence, omitting four bytes required by light-client response framing. Record verified singleton payloads with context derived from their bootstrap/attested slot; normalize legitimate missing context on restore. |
| B3-06 | P2, restored cache integrity | A structurally valid verified store could contain truncated or inconsistent cached payloads that were served unchecked. Reopen now validates cached SSZ/header constraints, bootstrap checkpoint binding, update slot/participation/proof structure, range-map periods and supplied context. It rejects invalid state before constructing the store, without rewriting the file. |
| B3-07 | Implementation cost | Every singleton/range RPC read cloned the full historical payload map. Store accessors now clone only selected singleton values or the requested consecutive period range. Retained history and successful-response semantics are unchanged. No timing improvement is claimed. |
| B3-08 | P2, encoder invariants | The encoder accepted wrong protocol/response pairs, missing required contexts, surplus context on Beacon V1, and an error variant carrying result code zero. Encoding now enforces the negotiated protocol, requires supplied context where specified, omits it for V1 and rejects success-coded errors. |

Individual cached byte arrays now enforce the existing RPC wire limit during
serde decoding. The streaming visitor ignores size hints, reserves conservatively
and reports allocation failures. This bounds each entry without inventing a total
history cap. Cached update signatures also undergo the same point-encoding and
subgroup validation used by normal verification. These checks do not authenticate
signatures against historical committees.

## Context and historical cache rules

The [light-client networking specification](https://raw.githubusercontent.com/ethereum/consensus-specs/master/specs/altair/light-client/p2p-interface.md)
selects context from the bootstrap header slot, or an update's **attested** header
slot. The signature slot, finalized header slot and current node epoch are not
substitutes. Its range response starts at the earliest available period inside the
requested range and ends at the first gap. The
[Electra schema tables](https://raw.githubusercontent.com/ethereum/consensus-specs/master/specs/electra/light-client/p2p-interface.md)
select the wire layout at that context epoch. The
[Fulu digest rule](https://raw.githubusercontent.com/ethereum/consensus-specs/v1.6.0/specs/fulu/beacon-chain.md#modified-compute_fork_digest)
uses blob parameters at that epoch, so a cached pre-BPO object cannot be tagged
with today's digest.

Already verified singleton writes use the known decoded status slot to check/fill
context before the writer transaction. They do not repeat SSZ/proof verification
on each save. Invalid supplied metadata returns an input error, keeps memory/file
unchanged and does not latch a filesystem failure. Restore validates each cache
and derives missing context in memory; valid input files are not rewritten merely
because that derived field was absent.

Cached updates may be older than selected heads or the current committee. They
are not re-applied to the current store: doing so would reject legitimate history
through relevance, period, time or committee checks. Standalone cache payloads
need not equal saved status summaries, since range updates change summaries
independently. A cache need not contain a contiguous history. Its supplied
bootstrap, if present, must match the retained checkpoint.

The existing pre-Fulu digest policy is retained. Independent compatibility work
on historical plain-versus-shifted digests remains in the roadmap; no policy
change is inferred from a function name or unverified future schedule.

These checks establish decoded structure and local commitments, not authenticity
of arbitrary modifications to a trusted file. No current-committee signature
replay is used to validate historical caches, and no integrity envelope for the
complete consensus snapshot is introduced in this batch.

## Read cost and response boundaries

The previous public whole-cache getter has no remaining repository caller and is
removed under the explicit compatibility waiver. Three crate-private accessors
clone one requested optional payload. The range accessor borrows the map under
the snapshot lock and clones only returned values; the network validates its
128-period request limit before calling it. The redundant network wrapper is
removed and its selection tests move with the helper.

Selection preserves an empty response for zero count/no match, skips unavailable
initial periods only within the requested range, and stops at the first internal
gap. An inclusive last-period calculation also handles the mathematical maximum
key. That maximum is an arithmetic control, not a practical mainnet period bug:
real periods derive from a `u64` slot divided by 8192.

The encoder never substitutes the current node fork for missing context. Empty
streams remain valid. Context-free status/metadata/ping/goodbye/error responses
remain context-free, and Beacon V1 removes a V2 context from shared cached bytes.
All chunks are encoded before the transport writes the buffer, so a missing
context in a later chunk cannot emit a partially encoded success stream.

## Evidence and cleanup

Before-fix codec runs reproduced malformed context roundtrips and protocol/variant
mismatches with bounded framing fixtures; these fixtures do not assert SSZ proof
validity. A separate run reproduced an error variant using success code zero.
After the encoder correction all 27 focused RPC tests passed; the per-entry serde
controls bring the focused RPC suite to 29 passing tests. Small-limit fixtures
exercise empty/exact/over-limit sequences and misleading size hints. A diagnostic
checks production-limit wiring without constructing a large JSON fixture.

Store-level before-fix tests reproduced a gossip cache retaining None context
and a truncated bootstrap cache accepted on reopen. The latter fails on its first
family before the guard; all four cache families are checked afterward. Signed
modern fixtures replace four old persistence tests that used three-byte payloads.
They verify checkpoint enrichment, finality/optimistic persistence without
deadlock, historical period retention and exact reopened responses.

The initial integrated CL suite passed 166 tests, with one explicit benchmark
ignored. Further review reproduced acceptance of invalid cached signature encodings;
all 25 focused light-client tests pass after that correction. A valid encoded
signature from a different key remains accepted by structural cache checks and
rejected by ordinary authentication, explicitly testing that distinction. A 25,600-case small-map comparison checks range selection against an
independent scan; additional controls cover maximum-key arithmetic and owned
returned payloads. Signed BPO controls separate attested context from signature
and finalized slots. Independent review found no change in normal proof-validation
order or historical cache acceptance. An obsolete BTreeMap import found by
focused strict Clippy was removed; the corrected focused check passes.

No benchmark, remote host or external volume was used. The production cache map
and retention policy are unchanged. Removed the whole-map getter, redundant
network selector/wrapper and synthetic persistence payload scaffolding. Shared
proof helpers preserve existing normal verification order and avoid duplicated
commitment-checking code. The completed integrated consensus-crate suite passes
171 tests with one explicit benchmark ignored. Source `7a2734a8` passed all seven
local gates: vendor verification, formatting, workspace check, strict Clippy,
1,249 workspace tests (23 ignored), documentation tests and release build.
[Gate records](baselines/2026-09-15-consensus-payload-cache-gates.json) retain
commands, toolchain, log hashes and focused evidence. All six Linux/macOS CI
jobs passed on head `f157d5f7` in run `34914060937`; [PR #155](https://github.com/tdenisenko/logex/pull/155)
merged as `d67f9360` on September 15, 2026 at 00:44:44 UTC.
[CI and merge records](baselines/2026-09-15-consensus-payload-cache-ci.json) retain
the exact head and final outcomes.
