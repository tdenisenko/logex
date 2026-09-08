# Batch 1e: beacon SSZ shapes, limits and fork context

Base: `f9f2d49e`, after receipt-validation PR #125. This milestone covers the
existing Electra/Fulu beacon-body decoder. It does not add older beacon-body
schemas or change the light client's trust model.

## Invariants and findings

SSZ fixed vectors are encoded inline; lists have a maximum cardinality that also
determines tree-hash depth. Wire decoding and hashing must agree on both. A
response chunk's fork digest is determined by the block slot, including scheduled
blob-parameter-only transitions. A known digest from another epoch is insufficient.

| ID | Severity | Reproducer and disposition |
| --- | --- | --- |
| B1-12 | P2 correctness/liveness | `Deposit.proof` was `Vec<B256>`, causing derived SSZ decoding to expect an offset for a variable field. The protocol requires an inline fixed vector of 33 roots. Independently assembled canonical deposit bytes failed with `OffsetOutOfBounds(1111638594)`. Use `FixedVector<B256, U33>` and verify exact round-trip both alone and inside a complete block. This fixes a schema gap for deposit-bearing blocks; the existing Fulu fixture has no legacy deposits. |
| B1-13 | P2 validation | Lists were unbounded vectors while manual hashing supplied separate maximum lengths; a 33-byte execution extra-data field was accepted. Use the existing `ssz_types::VariableList` for every protocol list, including nested transactions, operations and requests. Their SSZ decoders enforce cardinality before element allocation and tree hashing uses the same limits. No truncating list constructors are used by production code. |
| B1-14 | P2 wire conformance | A BPO2-era block was accepted under an Electra or Fulu-start context because all recognized modern contexts selected the same decoder. Compare the provided digest to the digest at the decoded block's epoch and return a mismatch error. Keep the existing no-context fallback for supported Electra/Fulu slots and fail closed on older/unknown schemas. |

Three focused regressions failed before production changes. Invalid lengths and
contexts now fail before a decoded block can be returned to network caching.
Block commitments still require the existing authenticated ancestry path before
new history is published. These findings do not establish a forged beacon root,
signature bypass, or validation of full beacon state transitions. The decoder's
existing `VerifiedBeaconBlock` name denotes decoded commitments in this layer;
authentication remains in the light-client/ancestry pipeline.

Primary references reviewed on 2026-09-09:

- [Deposit fixed-vector definition](https://github.com/ethereum/consensus-specs/blob/master/specs/phase0/beacon-chain.md#depositproof)
- [Electra operation/request limits](https://github.com/ethereum/consensus-specs/blob/master/presets/mainnet/electra.yaml)
- [Block response context by slot](https://github.com/ethereum/consensus-specs/blob/master/specs/altair/p2p-interface.md#beaconblocksbyrange-v2)
- [Fulu epoch-dependent digests and networking](https://github.com/ethereum/consensus-specs/blob/master/specs/fulu/p2p-interface.md)
- Installed `ssz_types` 0.11.0 fixed-vector/list decoding and tree-hash implementations,
  using the already pinned SSZ/tree-hash dependencies.

## Validation and compatibility

- Canonical inline deposit bytes preserve data and all 33 proof roots. Reject
  proofs with 0, 32 or 34 roots. A raw SSZ offset fixture inserts the deposit
  into a complete signed block and round-trips exactly.
- Zero, maximum and maximum-plus-one counts for all seven operation lists,
  all three execution-request lists, attesting indices, withdrawals and transaction
  count. Extra-data lengths 0, 31, 32 and 33 are covered. The test suite does not
  allocate a 1 GiB transaction to exercise that byte limit; it uses the same
  bounded byte-list implementation as extra data.
- Correct and incorrect digests immediately before/at Electra, Fulu, BPO1 and
  BPO2 transitions; older schemas remain explicitly unsupported. Updated old
  tests that paired a BPO2-era block with the Fulu-start digest.
- The tracked real 205,273-byte Fulu fixture re-encodes exactly and retains its
  known state, body, parent and beacon roots. Independent declared-limit root
  calculations cover composite and packed integer lists. Contiguous-byte roots
  match the generic bounded SSZ implementation around 32-byte chunk boundaries.
- All six local gates pass: formatting, locked all-target check, strict Clippy,
  763 workspace tests, doc tests and release linking. Two explicit benchmarks
  are ignored during ordinary tests. Linux/macOS CI and merge status are in the PR.

No public schema, persisted data, dependencies, mainnet schedule or production
deployment changed. The fixed deposit layout corrects a private wire model;
it requires no data-directory migration. Bounded wire types replace four manual
tree-hash implementations and their duplicate limit/merkle helper code. Execution
payload hashing retains contiguous-byte hashing after measurements showed a
repeatable twofold cost in the generic per-byte path; SSZ bounds remain enforced. Removed
a test-only trusted-root helper/error and its self-test, which did not exercise
production authentication; the real ancestry walkers have regression coverage
from PR #124. No new unsafe code was introduced.

See the [release comparison](baselines/2026-09-09-beacon.md) for measured decode
cost, reproduction instructions and limits. This milestone is not a full CL
performance audit or conformance-suite replacement.

## Remaining work

Continue the trust review with execution-header/fork rules and public extraction
numeric limits. Broader historical conformance fixtures, persisted-state integrity,
framing/decompression bounds, peer attribution and reorg publication remain open
in their respective batches. Automatic repair, external-volume supervision and
the integrated staging soak are not implemented by this milestone.
