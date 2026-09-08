# Batch 1c: cached consensus-state boundaries

Base: `bb437026`, after checkpoint-resolution PR #123. This is a scoped batch 1
milestone covering committee participation and cached beacon ancestry. It does
not complete the consensus, storage, or execution validation audits.

## Trust model and invariants

Wire sync committees contain exactly 512 public keys; persisted committees use
a variable-length vector. Every participation bit must correspond to a key in
the committee used for BLS verification, including the next committee during a
period transition. Malformed cardinality must return an error before an update
can produce new trusted state.

Beacon block decoding computes a root and execution commitment. Publication of
new history requires a complete parent-linked path from the checkpoint to a
verified optimistic or finalized light-client root. Self-consistent block bytes
alone do not authenticate an unrelated descendant. A parent must have a strictly
smaller slot than its child; skipped slots remain valid.

The cache also accepts persisted anchor records and explicitly trusted checkpoint
descriptors. Legacy records without parent metadata infer the preceding anchor's
root. These are existing local trust inputs, not newly authenticated peer data.
This milestone retains that contract and does not revalidate previously published
anchors or make arbitrary edits to a trusted store safe.

## Findings

| ID | Severity | Evidence and fix |
| --- | --- | --- |
| B1-07 | P2, malformed local-state validation | A persisted committee truncated to one key accepted an SSZ update signed by that key with all 512 participation bits set. Signature verification iterated only existing keys while participation counted every bit. With 513 keys, even an empty bitfield panicked at byte index 64. Require exactly 512 keys before selecting participants and return `InvalidCommitteeLength` otherwise. Cover both current and next-period committee selection. |
| B1-08 | P2, malformed local-state correctness/liveness | Cached parent links with equal or increasing slots were accepted for history publication; missing-parent discovery continued through invalid links. Three backward walkers had no cycle bound, so cyclic imported/cached links could stall the network task, with unbounded vector growth in publication. Require decreasing slots and matching cache-key/root metadata. Stop invalid discovery paths and return no publishable chain. The existing preferred-root set also stops traversal at previously visited/shared roots. |

These are defensive checks at persisted/imported-state boundaries. A correctly
decoded peer committee already has fixed cardinality, and forging a cyclic chain
of authenticated beacon roots is not demonstrated. Neither finding establishes
a remote mainnet signature forgery. Full stored-state integrity, error/status
reporting for corrupt stores, persistence ordering, and automatic recovery remain
for the storage/runtime batches. A rejected lineage remains unavailable; these
guards do not erase data or silently reconstruct missing history.

## Reproduction and validation

Four regressions were run before production changes and failed:

```sh
cargo test -p logex-cl --lib --locked malformed_committee
cargo test -p logex-cl --lib --locked cached_ancestry
```

The initial committee tests reproduced the panic and acceptance of inflated
participation. The initial ancestry tests reproduced publication and follow-up
fetching through non-decreasing parent slots. Cycle tests were added after the
guards; the old unbounded loops were not run in-process.

Additional coverage includes:

- Committee lengths 0, 1, 511 and 513, exact error classification, and unchanged
  caller state after rejection using the current or next committee.
- Every individual bit in a valid 512-key committee, plus empty/full selection.
- Self-parent and two-node cycles across publication, missing-parent discovery,
  and preferred-lineage collection.
- Root metadata mismatch, missing ancestors, shared finalized/optimistic tails,
  checkpoint-slot mismatch, checkpoint-only paths, skipped slots and slots 0 and
  `u64::MAX` without arithmetic overflow.
- Exhaustive comparison of 500 small parent graphs against an independent
  reference that detects repeated roots and validates the completed path's slot
  order. This includes cyclic, incomplete, valid and non-monotonic graphs.

All six local merge gates passed: formatting, locked all-target workspace check,
strict Clippy, 747 tests (one ignored full-size benchmark), doc tests and release
node linking. CI/merge disposition is recorded in the PR. Existing dependency
future-incompatibility warnings remain unchanged. No performance improvement is
claimed; these guards add constant work per traversed node or committee lookup.

## Cleanup and remaining review

Moved the service-bound preferred-lineage walker to a directly testable helper
and removed the superseded method. Removed repeated map lookups from the
missing-parent walk. Reviewed backward parent traversal and participant selection
callers; no other production paths were removed. The persisted schemas,
descriptor semantics, legacy parent fallback, and signature thresholds remain
compatible.

Continue batch 1 with beacon-body fork/schema constraints, execution header/body
and receipt validation, receipt codecs, transaction/log ordering, extraction
conversions, topic/null behavior and source semantics. Later batches must review
complete CL snapshot integrity and write-failure handling; these guards do not
provide a replacement for either.
