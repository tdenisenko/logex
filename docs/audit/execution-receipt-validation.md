# Batch 1d: receipt wire decoding and extraction

Base: `06454a53`, after cached-consensus-state PR #124. This milestone audits
the custom receipt codec and traces count/root validation to extraction. It does
not complete the execution networking, sync-state, or storage audits.

## Invariants and reviewed paths

Live, historical and anchored ingestion validate the downloaded body against its
header, require equal transaction and receipt counts, then verify receipt gas,
trie root and aggregate bloom before extracting rows. Transaction order supplies
the receipt-to-transaction association; trie indexes bind receipt ordering. The
historical direct-append path retains the same preconditions. Empty receipts must
match zero gas, the empty trie root and a zero bloom. Existing pre-Byzantium
support preserves the 32-byte post-state commitment.

The extraction functions themselves are conversions, not verifiers. Their
callers establish provenance and metadata. The row constructor's documentation
now states this explicitly. Topic absence means there is no topic at that
position; a zero hash is a present topic. An anonymous Solidity event may still
have indexed topics, so it does not imply `topic0 = None`. Corrected those field
comments without changing stored columns or query semantics.

Primary sources reviewed on 2026-09-08:

- [EIP-658 status and historical state roots](https://eips.ethereum.org/EIPS/eip-658)
- [EIP-2718 receipt envelopes and transaction association](https://eips.ethereum.org/EIPS/eip-2718)
- [ETH wire receipt formats](https://github.com/ethereum/devp2p/blob/master/caps/eth.md)
- Installed Alloy consensus 1.8.3 `receipt/status.rs` and `receipt/envelope.rs`,
  primitives 1.5.7 `log/mod.rs`, RLP 0.3.15 `header.rs`, and pinned Reth `d6324d63`
  receipt/ETH-wire implementations. The general status decoder coerces arbitrary
  single-byte values; the generic log decoder omits list-kind and topic-count
  validation. Alloy's receipt envelope rejects a typed legacy prefix.

## Findings and fixes

| ID | Severity | Evidence and disposition |
| --- | --- | --- |
| B1-09 | P2, wire conformance | Receipt status decoding accepted noncanonical zero, other single-byte statuses, and list-shaped values; log decoding accepted string-shaped log containers and more than four topics. Decoding/re-encoding could normalize malformed wire bytes before root checking. Decode status as canonical false, true, or a 32-byte post-state string. Decode each log as a bounded list of address, at most four topics, and data, rejecting extra fields. Preserve all valid old/current encodings. |
| B1-10 | P2, parser boundary | An empty declared receipt list followed by receipt-like bytes consumed those following bytes before returning a length error. Nested fields were decoded from the remaining message and checked only afterward. Bound receipt, typed envelope, log-list, log and topic-list payload slices before decoding or allocating their fields; exact consumption replaces post-hoc length arithmetic. |
| B1-11 | P2, envelope conformance | The typed receipt entry point accepted type zero and re-encoded it as an untyped legacy receipt. Reject the legacy identifier in typed envelopes, while retaining ETH/69's explicit legacy type field and canonical legacy RLP receipts. Unknown typed identifiers remain errors. |

These findings establish malformed-input acceptance and excessive parsing beyond
declared boundaries. They do not demonstrate forged authenticated receipt roots
or altered mainnet log data. Root verification still protects log contents after
decoding; rejecting noncanonical wire forms makes that boundary explicit. The
overlong-topic fixture is synthetic and cannot occur in a valid Ethereum receipt.
Outer frame/decompression bounds and peer attribution remain for the EL network
batch. No valid receipt encoding, on-disk format, dependency or API changed.

## Reproduction and regression coverage

Before production changes, five tests failed with:

```sh
cargo test -p logex-sync --lib --locked receipt_decode
```

They reproduced each of the malformed status, string-shaped log, fifth topic,
out-of-list consumption and typed-legacy acceptance cases. All now pass.

Expanded coverage includes:

- Invalid/padded statuses and list-shaped post-state values in both bloom-bearing
  and ETH/69 receipts; preserve actual historical hashes, including zero hashes.
- 275 valid fixtures across all five supported transaction types, both statuses,
  legacy post-state, zero through four topics, data lengths 0/1/55/56/256, and
  `u64::MAX` cumulative gas. Compare bloom-bearing RLP and EIP-2718 decoding with
  independently constructed Alloy receipt envelopes; ETH/69 also round-trips.
- Reject every proper truncation of each valid network/RLP fixture. Preserve
  following sibling receipts and reject extra fields inside declared containers.
- Deterministic byte mutations of legacy and typed receipts with and without
  blooms: every accepted prefix must re-encode byte-for-byte identically. Invalid
  input is not required to leave the caller buffer unchanged; it cannot consume
  fields beyond the declared container.
- Root validation rejects changes to address, topic, data, log count, status,
  transaction type, intermediate cumulative gas and receipt ordering.
- Compare both extraction paths with empty transactions interspersed among logs;
  assert exact transaction/global indexes, hashes, addresses, absent versus zero
  topics, data length and receipt provenance. Appending preserves existing rows.

All six local merge gates passed: formatting, locked all-target checking, strict
Clippy, 757 workspace tests (one ignored full-size benchmark), doc tests and
release linking. CI and merge status are recorded in the PR. Existing dependency
future-incompatibility and macOS debug-linker unwind-size warnings were also
present before this milestone. No speedup is claimed or measured optimization
retained; this milestone adds correctness checks.

## Cleanup and remaining work

Removed permissive status/log decoding calls and superseded post-hoc container
length arithmetic from the custom codec. Both receipt formats share strict
status/log helpers. Corrected misleading topic/provenance documentation. Reviewed
all extraction and receipt-validation callers; no current ingestion path was
found bypassing count/root checks. Preserve public conversion helpers rather than
removing them solely because they are not direct validation boundaries.

Remaining batch 1 work includes full header/fork and beacon-body conformance,
additional independently sourced historical fixtures, extraction numeric limits
at public conversion boundaries, and wider source/trust review. Full snapshot
integrity, request correlation, peer failures, reorg publication, durability,
offline repair, volume supervision and staging are separate remaining batches.
