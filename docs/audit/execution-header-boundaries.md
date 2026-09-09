# Execution header boundaries

Batch 1 follow-up to the extraction milestone, based on merged PR #127
(`f5fdd83c`). The supplemental checks apply to downloaded headers in both forward
and reverse validation. Existing consensus-authenticated anchor and ancestry
requirements are unchanged. These are structural validation gaps; none of the
reproducers forges an authenticated mainnet block.

## Findings

- **B1-17 — pre-London base fee accepted (P3, fixed).** Reth 1.11.3 requires a base
  fee when London is active but does not reject it before London. A standalone
  block 12,964,999 with `base_fee_per_gas: Some(0)` passed. Reject any present base
  fee before London, including zero. The same check applies to reverse parents.
  This agrees with [EIP-1559](https://eips.ethereum.org/EIPS/eip-1559) and
  [Geth's pre-London check](https://github.com/ethereum/go-ethereum/blob/master/consensus/ethash/consensus.go).
- **B1-18 — missing standalone minimum gas limit (P3, fixed).** The upstream
  standalone gas check enforces used-versus-limit and the maximum but leaves the
  minimum to parent validation. A first header with no parent and a gas limit of
  zero passed. Require the existing `MINIMUM_GAS_LIMIT` (5,000) before delegation.
  This is a local invariant at a public validation boundary; ordinary linked
  headers already undergo parent checks. Genesis remains valid at 5,000.
- **B1-19 — missing standalone excess blob gas (P3, fixed).** At Cancun, upstream
  standalone checks require blob gas used and a parent beacon root but omit
  excess blob gas; the parent-dependent path checks it later. Toggling only
  `excess_blob_gas` to `None` at the activation timestamp passed standalone
  validation. Require its presence once Cancun is active, including when its
  value is zero. [EIP-4844](https://eips.ethereum.org/EIPS/eip-4844#header-extension)
  defines both blob gas fields. This reproducer uses an in-memory header shape;
  it does not establish a wire-decoder or authenticated-root bypass.

`validate_execution_header` contains only these supplemental checks and delegates
the rest to the pinned Reth validator. Both existing callers use it. Public
function signatures and error enums are unchanged: use existing standalone
consensus errors, with `ConsensusError::Other` for unexpected pre-London base fee.
No schema, configuration, dependency, toolchain, trust-model or service changes.

## Validation evidence

The pre-London and minimum-gas tests failed before production edits. The expanded
fork-field matrix then failed on missing excess blob gas before its guard was
added. Focused validation now passes 21 tests, including the existing receipt-root
and ancestry cases.

New coverage includes:

- Twelve historical mainnet headers: genesis; blocks before/at/after London;
  before/at Merge, Shanghai, Cancun and Prague. Parse the retained RPC header
  fields, check each complete header hash against the recorded hash, validate
  standalone rules, and validate all adjacent pairs in both directions. Compare
  genesis to the pinned mainnet genesis header. Tamper difficulty, nonce and
  ommers hash at the real first proof-of-stake header and reject all three.
- A synthetic London chain exercises gas-limit elasticity, the initial one-gwei
  base fee, the one-eighth increase after a full block, wrong initial fees, missing
  post-London fee and an unexpected pre-London parent fee. It is a conformance
  fixture, not a canonical-chain claim.
- Eighteen synthetic timestamp contexts immediately before/at/after Shanghai,
  Cancun, Prague, Osaka, BPO1 and BPO2. Each valid field shape passes; toggling
  withdrawal root, either blob gas field, parent beacon root or requests hash
  produces 90 rejected mutations. These test field presence, not payload execution.
- Gas-limit/used and extra-data boundaries, zero/minimum/maximum values, and
  existing upstream protections against parent-number overflow and traversal
  before genesis. No speculative overflow patch was added where upstream already
  checks the arithmetic.

All six local workspace gates passed: formatting, locked all-target check, strict
Clippy, 776 tests (three explicitly ignored benchmarks), doc tests and release
node build. Cross-platform CI is required before merge. No performance
optimization is claimed: the production change adds three inexpensive standalone
predicates before delegating to existing consensus validation. End-to-end header and
sync profiling remains part of the networking/sync/integrated audit.

## Fixture provenance and limits

[`execution_headers.json`](../../crates/logex-sync/tests/fixtures/execution_headers.json)
contains 20,482 bytes of public chain header data fetched using read-only
`eth_getBlockByNumber` calls to `https://rpc.flashbots.net` on 2026-09-09. The
retrieval timestamp and source are stored in the file. Requests had timeouts and a
4 MiB response cap. Two earlier public RPC providers refused or failed the request;
the final fixture is entirely from Flashbots, without credentials, signed
transactions, production node access or service changes. Tests make no network
requests. The fixture SHA-256 is
`b9b2f1f8082b3eb2c81520b3b67ec3fb8e8a443c22213fa41f215841b95da9a5`.

The fixture hashes catch field/encoding changes and parent-link discrepancies;
they are not themselves consensus proofs. Canonical provenance still depends on
the production consensus-anchor chain. This audit does not turn the node into an
EVM reexecutor or independently validate historical proof-of-work difficulty,
state roots, transaction execution or withdrawals by replaying state. Those are
covered by the established authenticated-header trust model, not by these new
synthetic tests. Crash consistency and persisted-header shape validation remain
for storage/sync batches.
