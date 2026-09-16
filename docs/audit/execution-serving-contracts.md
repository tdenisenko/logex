# Execution serving contracts

## Findings and original evidence

- **B4-28 — medium, availability:** an empty body/receipt cache advertised the
  consensus head as a one-block serving range. Header-only retention or removing
  the final cached body produced the same mismatch. The provider can always serve
  mainnet genesis, which is the correct empty-cache fallback.
- **B4-29 — medium, stale availability:** pinned Reth checked only a latest-height
  increase of at least 32 before notifying an established peer. Earliest-only
  changes, a same-height replacement hash, or a lower range could remain
  unannounced indefinitely even though its periodic timer kept running.
- **B4-30 — medium, receipt progress:** an ETH70 receipt above the 2 MiB soft
  response target produced an empty incomplete fragment. Retrying its unchanged
  cursor could never obtain that receipt.
- **B4-31 — medium, receipt completion:** when receipt bytes fitted the target but
  their list framing did not, the partial path returned every receipt and still
  marked the block incomplete. It could also append an unstarted empty fragment
  after earlier complete blocks instead of stopping at that block boundary.
- **B4-32 — medium, range consistency:** range numbers and hash were stored and
  read separately. SessionManager publishes while separately spawned sessions
  read; a snapshot could combine different publications. A bounded concurrent
  control observed new numbers 20..30 with the previous hash. Regressing a range
  can also produce an invalid earliest/latest pair under the source interleaving;
  the retained run did not observe that additional outcome.

Before fixes, five positive controls passed and eleven assertions failed: four
provider-backed advertisement cases, two actual public-handler receipt cases,
four extracted original range-decision cases and one actual-source snapshot case.
The range-decision controls reproduce the exact old predicate; they are not full
session or socket tests. The snapshot race control is probabilistic; its initial
per-round-barrier version saw no inconsistency, then a bounded free-running version
observed the mixed publication. Both logs remain available.

Fixtures use synthetic headers and ordinary typed receipt objects of roughly
2 MiB or less, with the largest encoded receipt 2 MiB plus one byte. These are
serving-contract fixtures, not claimed executed-valid mainnet blocks. No remote
node, socket or network task is needed for these new controls. Existing broader
peer-manager tests retain their dormant loopback handle fixtures.

## Corrections

The advertisement adapter no longer accepts a consensus head as a fallback. It
returns the retained contiguous cache range or the provider's serveable mainnet
(0, 0, genesis hash) tuple. Startup and subsequent updates always apply that range.
The real consensus head still drives fork selection and dialing activation;
`set_head` preserves the existing order of head update followed by served range.
The superseded optional return and fabricated-head fallback tests are removed.

The local patch keeps Reth **1.11.3** at exact upstream commit
`d6324d63e27ef6b7c49cdc9b1977c1b808234c7b`. Its private shared range is one
`RwLock<BlockRangeUpdate>`; updates replace the tuple, and range/message reads take
one snapshot. Scalar accessors preserve their field semantics; separate accessor
calls are not promised to represent one instant.

Active sessions compare the complete snapshot with the last queued tuple at the
existing delayed 384-second interval, retaining its existing missed-tick `Delay`
behavior. Changed earliest bounds, hashes and regressing heights can now be
announced; identical tuples remain suppressed. The exact queued snapshot is
remembered and the task is woken to flush its queue on the next poll. This retains
coalescing and does not promise immediate delivery or delivery through a stalled
transport. [EIP-7642](https://eips.ethereum.org/EIPS/eip-7642) describes serving-range
notifications and permits coalescing at epoch frequency.

ETH70 retains its whole-block fast path. When pagination is needed, it permits
one receipt over the soft target only if no block or receipt progress would
otherwise occur. Earlier empty but completed blocks count as progress. An unstarted
trailing block is omitted; an emitted prefix is marked incomplete only when more
post-cursor receipts remain. The next request can therefore resume at the exact
receipt or block boundary. This follows the cursor/incomplete contract in
[EIP-7975](https://eips.ethereum.org/EIPS/eip-7975), currently a review-stage EIP
implemented by the pinned supported wire version.

The response target is soft and excludes some surrounding framing. This change
does not promise an arbitrary receipt fits a peer's hard message cap; the pinned
incoming codec checks 10 MiB separately. Existing content, receipt-root and consensus
validation remain essential, and transport progress alone never establishes valid
logs or complete coverage.

## Dependency provenance and cost

Vendoring is limited to the network crate: 54 unchanged-or-patched upstream files,
plus the exact upstream root licenses and formatting configuration. The standalone
manifest preserves inherited dependency features/defaults, package metadata and
lints. The lockfile changes only the network crate to its local source plus two
already-locked test dependency edges; no package version or other Reth pin changes.
The reviewed diff modifies that manifest and four existing Rust files, adding one
private range-decision helper. The complete inventory, hashes and diff are enforced
by the existing vendor verifier; CI also formats the changed vendor sources.

No dispatcher, extra request queue, EVM test feature set, wire format or storage
migration is added. Range scalar reads now use the tuple's small read lock instead
of separate atomics. That is an explicit correctness cost; the old hash already
used a lock. Receipt responses retain existing ownership and whole-block cloning
before cursor slicing. No benchmark or ingestion-throughput guarantee is claimed.

Outgoing ownership review found independently owned containers/shared immutable
byte backing, no escaping borrowed guards, a single serial handler, a bounded
256-entry request channel and existing per-session response backpressure. The
2 MiB target can be exceeded by one response item; retained-cache 128 MiB accounting
is not global RSS or a bound on all concurrent outgoing copies. A cursor-aware
serving API and early cancellation checks are optional cost opportunities, not
established ownership leaks. A broader handler/provider rewrite is not justified
by these corrections. Repeated cache-range scans/unchanged status submissions
remain a concrete implementation-cost review item.

## Validation and cleanup

All 266 peer-manager tests pass. Twenty new controls cover provider availability,
actual handler pagination/cursor behavior, actual shared-range source snapshots and
range-change semantics. Four final pagination cases additionally cover prior empty
and nonempty complete blocks, a resumed large receipt, and a large prefix with an
exact remaining suffix. One obsolete fabricated-head unit case is removed, for a
net increase of nineteen tests.

Private range source files are compiled directly in a test-only LogEx harness;
they are not a copied alternate implementation. Initial discovery that the upstream
session module is private led to this harness, without widening production APIs or
enabling heavyweight upstream EVM/provider test features. Original compilation and
control logs are retained separately. No complete upstream network suite or live
transport soak is claimed.

Cleanup removed the independent range atomics/inner wrapper, latest-only remembered
state, ambiguous constant-incomplete flag and head-based advertisement fallback.
Existing wire and cache APIs remain. Independent source review found no behavioral
defect; its stale soft-limit comment was corrected. Vendor verification passes for
all 156 upstream files across three crates. All eight local gates pass on `7d5d826a`: vendor integrity, workspace and
patched-vendor formatting, all-target check, strict Clippy, 1,577 workspace tests
(23 ignored), doc tests and the release node build. All six CI jobs passed on `be102894`;
[PR #188](https://github.com/tdenisenko/logex/pull/188) merged as `e07fb451`.

[Validation record](baselines/2026-09-16-execution-serving-contracts.json).
No Mac mini work or new benchmark was performed. Its earlier cleanup remains complete.
