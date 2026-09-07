# Batch 1b: checkpoint source resolution

Base: `5bcf325f`, after consensus-header trust PR #122. Keep the configured
majority threshold (including explicit single-source configurations) and require
agreement on the complete block slot/root pair. HTTP agreement selects the trust
root; subsequent bootstrap still proves the header and committee against it.
This milestone does not replace that trust model or infer independent operators
from endpoint hostnames.

## Findings and fixes

| ID | Severity | Evidence and disposition |
| --- | --- | --- |
| B1-04 | P2 availability | Automatic resolution selected the minimum finalized slot from any responding source. With two recent sources and one stale source, all three could agree on an old historical block and resolution returned that stale checkpoint. Conversely, a single ahead source's maximum slot could cause rejection of a recent explicit checkpoint. Use the configured quorum's reported finality height as the freshness reference, try reported candidate slots below that height in descending order, and retain full slot/root quorum agreement. With default 2-of-3 configuration, one outlier no longer selects either extreme. |
| B1-05 | P2 correctness | The finality-checkpoint fallback multiplied epoch by 32 and reported that as the block slot. If the first epoch slot was skipped, the root identifies an earlier block. Resolve that root through the header/block endpoints and retain its actual slot; reject unavailable roots, mismatched roots and blocks after the epoch boundary instead of guessing. |
| B1-06 | P2 validation/resource bounds | Header responses could contain malformed roots or identify a different requested slot, and JSON bodies were buffered without a size limit. Validate normalized 32-byte roots and requested slot/root identity across both header and block fallbacks. Read JSON incrementally with explicit limits, checking actual bytes even without Content-Length. Bound a source's complete fallback sequence by one deadline. |

For B1-05, the Beacon API returns a finalized **epoch/root** pair, while the
consensus state retains the latest block root through skipped slots. The epoch
boundary is not evidence of a block at that slot. See the official
[finality-checkpoint API](https://github.com/ethereum/beacon-APIs/blob/master/apis/beacon/states/finality_checkpoints.yaml)
and [consensus block-root and slot processing](https://github.com/ethereum/consensus-specs/blob/master/specs/phase0/beacon-chain.md#slot-processing).

## Algorithm and limits

Sort successful finalized headers by descending slot and take the configured
quorum's last member as the reference. Do not shrink the threshold when sources
fail. Automatic resolution tries distinct reported slots at or below that
reference, newest first, and skips candidates outside the existing 256-epoch
relative-freshness window. A false intermediate/skipped slot may fail; try the
next candidate, still requiring the original quorum to agree on slot and root.
Absolute weak-subjectivity age remains enforced by the consensus store.

Each source lookup has a 30-second deadline including all fallback requests;
individual HTTP requests retain their existing timeout. The number of candidate
rounds is bounded by the number of configured distinct reported slots. This is
not one 30-second deadline for the whole multi-round operation.

Metadata responses (header, root, finality) are limited to 64 KiB. Full beacon
block fallback responses are limited independently to 64 MiB because they include
transaction hex and the entire body. The small header endpoint remains preferred.
These are explicit HTTP resource ceilings, not consensus block-size constants.
Oversized responses fail with the URL and limit, rather than being truncated or
used as checkpoints. No dependency or toolchain change was needed.

## Tests and cleanup

Five scripted HTTP regressions failed before production changes, covering the
stale-source choice, ahead-source freshness veto, skipped-slot fallback,
unresolvable fallback and mismatched/malformed response. All pass after fixes.
Further cases cover:

- Unavailable intermediate candidate followed by a valid quorum.
- Three disagreeing roots and one healthy source with two unavailable sources.
- Wrong root and block slot later than the finalized epoch boundary.
- Non-numeric/overflowing epochs and malformed or empty roots.
- Valid JSON at the metadata size ceiling and rejection one byte above it,
  both with and without Content-Length.
- Existing block/root fallback and descriptor/inline parsing behavior.

All six local workspace gates pass, including 738 tests (zero failures, one
ignored full-size benchmark), strict Clippy, formatting, all-target checking, doc
tests and release linking. The focused checkpoint suite passes 28 tests. Linux/
macOS CI and merge status are recorded in the PR. The deadline uses
Tokio's standard timeout around the complete source operation; these tests do
not simulate all network stalls or establish a production latency distribution.

Consolidated repeated status/JSON error handling into the bounded JSON reader,
removed the old unbounded response helper, and separated block lookup from
finality lookup to avoid recursive fallback. Renamed the obsolete test that
claimed an epoch boundary was a block slot. Updated the HTTP fixture server to
own its routes and test missing Content-Length without leaking fixtures.

No performance optimization or production deployment is claimed. Remaining
batch-1 work includes descriptor anchor trust, stored CL-state validation,
beacon ancestry, EL receipt/body validation, extraction and independent protocol
fixtures. Network policy, source URL configuration, persisted corruption and
long-running failure/soak testing continue in their designated audit areas.
