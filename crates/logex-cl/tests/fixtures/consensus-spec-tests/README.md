# Official light-client SSZ conformance fixtures

Source: [ethereum/consensus-spec-tests](https://github.com/ethereum/consensus-spec-tests/tree/bc5c1a7fb2a8871aaffd4b16ee4dd9c72bb81908), revision `bc5c1a7fb2a8871aaffd4b16ee4dd9c72bb81908`.

The 13 selected cases are `tests/mainnet/{fork}/ssz_static/{type}/ssz_random/case_0`: bootstrap, finality update, optimistic update, and range update for Capella, Deneb, and Electra; and one Fulu range update. Fulu inherits Electra's light-client layout; Electra optimistic updates inherit the Deneb layout. These cases cover each supported layout without downloading a bulk release.

Each `serialized.ssz_snappy`, `value.yaml`, and `roots.yaml` is unchanged upstream data. `provenance.json` records the exact source path, SHA-256 and length for every upstream file; tests check those digests. The upstream MIT license is included as `LICENSE`, with one trailing space removed from its copyright line; provenance records both original and normalized hashes.

`expected.json` is a local extraction of named scalar fields from the neighboring unchanged `value.yaml`. Integer values retain all 64 bits; byte fields retain their hexadecimal representation. The additional `beacon_root` is derived independently with Python standard-library SHA-256: encode slot and proposer index as little-endian uint64 padded to 32 bytes, append parent/state/body roots, pad the five leaves to eight, and hash pairs until one root remains. It is not derived through the production Rust implementation.

The test invokes production payload decoders and checks selected wire layouts, every beacon-header field, selected execution fields (including extra data and Deneb blob gas fields), signature slot/bits/bytes, independently derived beacon-header roots, and byte-for-byte reencoding. Exact reencoding also covers committees and Merkle branches. The entire upstream payload root in `roots.yaml` is retained as provenance but is **not asserted**: the production code has no whole-payload tree-hash API, and this test does not introduce a duplicate implementation solely to assert it.

These are randomized **mainnet-preset SSZ vectors**, not historical Ethereum-mainnet blocks or authenticated updates. Random slots need not correspond to the fixture fork's mainnet activation interval; random signatures, execution commitments, branch values, and slot ordering need not be valid. No test claims proof/signature verification, cache validity, fork-context correctness, or state transition conformance. Existing independently signed local fixtures cover those separate checks.
