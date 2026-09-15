# Header-bound receipt resource limits

## Finding

**B4-23 — medium, resource retention:** standalone and paired ETH70 requests can
accumulate positive incomplete fragments for one block. Per-message framing and
elapsed deadlines do not bound the merged receipts. ETH68/69 also lacked a
header-derived resource check before local collection; ETH69 reconstructed log
blooms before higher-level validation.

A small original-code control retains two one-receipt fragments at a proposed
42,000 weight allowance, then accepts a third and retains 63,000. The first
control passed and the third-fragment policy control failed with original
production unchanged. These are typed parser/accounting fixtures, not receipts
claimed to match an executed or committed block. No large input or benchmark was
needed to reproduce absence of the policy.

## Bound and authority

For the pinned supported mainnet gas schedule, a valid block necessarily obeys:

```
W = 21,000 × receipt_count
  +    375 × log_count
  +    375 × topic_count
  +      8 × log_data_bytes
W ≤ 2 × header.gas_used
```

Every receipt-bearing transaction pays at least 21,000 intrinsic gas. Retained
logs are a subset of executed LOG instructions, whose fixed/topic/data charges
supply the other terms. Historical refunds cannot reduce net gas below half of
gross consumption. Later one-fifth refund caps and calldata floors only tighten
this conservative inequality. This is not a receipt-RLP-size divided-by-eight
rule; receipt envelopes, historical post-state fields and bloom bytes differ.

Source review uses pinned [Ethereum execution specifications](https://github.com/ethereum/execution-specs/tree/f7847b76bac6113cce6a94e8d62c734609b7ec32/src/ethereum/forks)
and the locked Revm/Alloy implementations. Relevant rules include
[Frontier refund accounting](https://github.com/ethereum/execution-specs/blob/f7847b76bac6113cce6a94e8d62c734609b7ec32/src/ethereum/forks/frontier/fork.py),
[LOG charging](https://github.com/ethereum/execution-specs/blob/f7847b76bac6113cce6a94e8d62c734609b7ec32/src/ethereum/forks/frontier/vm/instructions/log.py)
and [EIP-3529](https://eips.ethereum.org/EIPS/eip-3529). The source-backed proof
covers the pinned mainnet schedule through Osaka/BPO2, including EIP-7702 refunds
and system operations that do not produce transaction receipts. Future fork
changes to these rules require review. Arithmetic tests do not substitute for
executing every historical fork or block.

`ReceiptRequestContext` binds block hash and gas used by deriving both from one
header. Its fields are private and there is no unchecked production constructor.
All six standalone and three paired engine callers already have corresponding
headers; they now pass mandatory contexts instead of independent hashes and
optional scheduling gas vectors. Plans retain those contexts and slice by the
same block ranges used for body requests. Wire hashes and scheduler gas values
are derived from them. There is no hash-only receipt API fallback.

Construction binds identity; it does not authenticate the header or establish
canonicality. Existing header/ancestry/anchor and final body/receipt-root checks
remain required. Legacy unanchored callers are not relabeled consensus-authenticated.
Resource rejection is neutral local request policy: it neither blames a receipt
peer for an invalid candidate header chosen elsewhere nor advances trusted state.
Unverified body transaction-count hints remain removed.

## Enforcement and error behavior

ETH70 carries the weight of just the unfinished block across fragments. It checks
cursor shape, then all incoming block weights, before reconstructing blooms or
mutating the retained prefix. A failure leaves the prefix and accumulator intact.
Finishing a block discards its partial accumulator; the next block uses its own
allowance. No previous receipt list is rescanned. Checked `u128` arithmetic
accommodates twice `u64::MAX`; receipt-reported cumulative gas never supplies the
allowance.

Both plan and manager ETH69 paths check raw receipt objects before bloom work.
ETH68 already arrives with wire blooms, so it is checked before local collection.
Known excess outer block counts are rejected before resource or bloom work and
mapped back to the collectors' existing `Incomplete` shape classification.
Sequential protocol-fault handling and chunk role disabling remain unchanged.
The initial implementation review caught this classification distinction; its
finding and correction are retained in the evidence record.

A resource violation uses a separate neutral outcome, preserving finite retries,
per-block sources, normal timeouts and final validation. It is not a successful
partial result and cannot mark coverage complete. The previous elapsed-window
correction remains unchanged.

## Cost and limits

The guard visits each new receipt and log once, reading topic counts and byte
lengths without encoding, compression or hashing log data. ETH69/70 bloom work
still follows the complete preflight. Contexts add bounded per-request metadata
and copies alongside existing hash vectors; plans retain one context per requested
block. Each ETH70 role attempt adds one `u128` partial accumulator. No storage
format, ingestion commit ordering, network concurrency or persisted column changes
are made.

Live sync reuses context-derived hashes. Other engine paths still hold previously
computed header hashes and perform another small header hash at context construction;
no unchecked cached-hash injection was added. Two unnecessary ETH70 context clones
were removed, and undersized plan requests return before constructing scheduling
metadata. No throughput claim or new benchmark is made.

This is a per-block logical resource bound, not a fixed global memory ceiling.
Decoded responses exist before these checks; allocator capacity, shared byte
backing, concurrent requests, completed engine batches and outgoing copies remain
separate audit concerns. A very large unanchored candidate gas allowance is not
an absolute memory budget. Final consensus/root validation is still essential.

## Validation

Initial focused checks passed all 11 new controls and all 218 peer-manager tests.
Review identified the outer-shape classification change; the correction and three
additional controls now pass with all 221 peer-manager tests (14 new controls in
total). Those checks verify both manager/plan shape categorization, neutral
resource rejection, actual wire hash binding, mixed-budget partial alignment,
atomic multiblock rejection, accumulator resets and legacy/typed arithmetic.
Independent final source/test review found no remaining actionable defect.

Cleanup removed the hash-only paired prepare wrapper and independent optional
hash/gas API parameters. Still-used scheduling and final verification helpers
remain. Validation metadata records original/final logs, proof and review hashes.
All seven local gates pass on `641d5a2e`, including 1,532 workspace
tests (23 ignored), doc tests and release linking. CI and merge remain pending.
[Validation record](baselines/2026-09-16-execution-receipt-resource-bounds.json).
No Mac mini work was performed.
