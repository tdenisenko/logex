# Batch 1a: light-client header and checkpoint-slot verification

Base: `bf4b97ab`, after dependency-security PR #121. This is the first scoped
milestone of batch 1, not a disposition for all trust boundaries.

## Invariants and evidence

A bootstrap must prove its beacon root and current sync committee against the
trusted checkpoint. A supplied checkpoint slot must agree with the proven
beacon header. Once a verified store exists, its finalized header establishes
weak-subjectivity freshness; unverified metadata cannot make old state fresh.

Light-client execution proofs must use the schema at the beacon header's slot.
An update's finalized header can precede the update's wire-format fork. Before
Capella, upgraded headers carry no execution data or branch. Before Deneb, blob
gas fields are zero; during Capella, the execution commitment contains 15 fields.
From Deneb onward it contains 17. Adding two zero fields still changes that tree.
Execution extra data is limited to 32 bytes.

Primary specification references reviewed on 2026-09-08:

- [Capella header validity](https://github.com/ethereum/consensus-specs/blob/master/specs/capella/light-client/sync-protocol.md#modified-is_valid_light_client_header)
- [Deneb execution roots and header validity](https://github.com/ethereum/consensus-specs/blob/master/specs/deneb/light-client/sync-protocol.md#modified-get_lc_execution_root)
- [Electra branch normalization](https://github.com/ethereum/consensus-specs/blob/master/specs/electra/light-client/fork.md#normalize_merkle_branch)

The existing Electra normalization correctly pads/removes leading zero siblings;
no change is needed to that algorithm. Mainnet fork configuration, BLS signature
requirements, participation thresholds, committee rotation and force-update
policy are unchanged by this milestone.

## Findings and fixes

| ID | Severity | Reproducer and disposition |
| --- | --- | --- |
| B1-01 | P2 correctness/liveness | A Capella execution header upgraded into a Deneb-format update has a valid 15-field execution proof. The verifier always hashed 17 fields and rejected it as `InvalidExecutionProof`. Hash according to the header slot, retaining the existing Deneb root for modern headers. |
| B1-02 | P2 validation | Self-consistent synthetic proofs allowed more than 32 extra-data bytes and nonzero pre-Deneb blob fields. Pre-Capella empty execution headers were incorrectly rejected. Enforce the fork-specific field constraints before hashing and accept empty pre-Capella execution fields without inventing an execution anchor. Reject Capella-only encoding at Deneb or later slots. These tests establish conformance gaps, not a forged mainnet commitment or signature bypass. |
| B1-03 | P1 trust metadata | A valid bootstrap for slot 10,000,000 accepted a supplied slot 10,000,001 and persisted the latter. Restart freshness used the maximum of verified finalized slot and bootstrap metadata; a large unverified hint could make old verified state appear fresh. Reject a slot/root mismatch with an explicit error, always persist the proven bootstrap slot, and use the verified finalized slot for freshness when a verified store exists. Root-only checkpoints still learn their slot from the proof. |

Malformed updates return errors before producing an updated store. No ingestion,
query, on-disk format, dependency, or production service changes are included.
Existing stores retain their artifacts; this change removes bootstrap metadata's
ability to extend their freshness window. It does not reconstruct or rewrite
historical bootstrap metadata in existing stores.

## Regression coverage

Before production changes, four new header regressions failed, the supplied-slot
regression failed, and the extended restart-freshness regression failed. After
the fixes, all pass. The oversized-data reproducer was extended into a boundary
matrix rather than retaining two redundant tests.

- Capella-start and last-Capella-slot execution proofs in Deneb-format headers.
- Empty pre-Capella execution data, rejecting populated fields or branches.
- Both blob gas fields independently rejected before Deneb, even with a matching
  synthetic Deneb proof.
- Extra-data lengths 0, 1, 31, 32, 33 and 64 for Capella, upgraded Capella and
  Deneb headers; reject obsolete encoding at the Deneb boundary.
- An SSZ-encoded, BLS-signed Deneb finality update carrying a Capella finalized
  header, including the committee-period transition. One participant advances
  only optimism, preserving the existing finality threshold.
- Tampering with every execution/finality proof sibling, and a structurally
  valid signature from the wrong key, returns the expected verification error
  without changing the caller's store.
- Matching, mismatched and absent checkpoint-slot hints; restart freshness stays
  bound to the verified finalized slot even with `u64::MAX` metadata.

All six local workspace gates passed: formatting, locked all-target checking,
strict Clippy, 727 tests (zero failures, one ignored full-size benchmark), doc
tests and release node linking. Focused consensus validation passed 120 tests.
Cross-platform CI and merge status are recorded in this milestone's PR. No speedup
is claimed; this milestone adds correctness checks and does not optimize a
measured workload. It does not substitute for official multi-fork conformance
fixtures or a full-chain staging soak.

## Cleanup and remaining batch 1 work

Removed a stale `allow(dead_code)` on the next-committee generalized-index helper
that has production callers, shared the identical fork-version lookup, and
removed the superseded oversized-data test. Kept existing public error variants
and persisted schemas compatible.

Continue reviewing checkpoint HTTP quorum/fallbacks and descriptor trust, stored
CL-state validation, beacon-body roots and ancestry, receipt codecs and roots,
transaction counts/order, extraction conversions, topic/null handling and
source semantics. Expand independent fork/conformance fixtures and malformed
input coverage as those paths are audited. Later CL/network and storage batches
remain responsible for framing bounds, liveness and persisted-state corruption.
