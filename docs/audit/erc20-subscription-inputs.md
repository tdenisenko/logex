# ERC20 subscription inputs and event classification

Base: PR #231 merge `334ec9ef985773341572b00969b7c8d48be47dbe`.
Branch: `audit/erc20-subscription-inputs`. Implementation and eleven local gates are complete; PR #232 merged as `c85df998` after all six CI jobs.

## Findings and scope

- **B8-17 (P2): literal subscription field types.** Address and amount helpers
  deserialize through `serde_json::Value`. With the enabled raw-value feature,
  its private tagged-object representation can reinterpret a wire object as a
  different value. Finite original-source controls reproduce acceptance in six
  field/member positions, including an object that removes an amount bound.
  Decode supported forms directly with typed visitors, including literal string
  members inside address arrays. Invalid input is rejected before session updates.
- **B8-18 (P2): amount missing hexadecimal digits.** Bare `0x` is padded into
  zero. Require at least one hexadecimal digit, while preserving optional blank
  strings as unset, odd-length hex, decimal leading zeros and full uint256 strings.
- **B8-19 (P2): extra topic accepted as standard Transfer.** The classifier
  checks the signature, both indexed addresses and amount data, but overlooks
  `topic3`. Require its absence. This changes only transfer notification
  classification; arbitrary valid chain logs remain stored and raw-queryable.

The [ERC20 event specification](https://eips.ethereum.org/EIPS/eip-20#transfer-1)
and [Solidity ABI](https://docs.soliditylang.org/en/latest/abi-spec.html#events)
require the signature plus two indexed addresses and one nonindexed uint256
word. Existing address-padding and declared/actual 32-byte data checks are
correct and retained. Zero-address and zero-value transfers remain valid.
Event shape alone does not establish complete contract compliance.

## Compatibility and implementation cost

Retain wallet/amount aliases, optional null values, top-level delimited address
strings, arrays of individual address strings, scope/ID aliases and existing
unknown-member behavior. No request-root representation policy is introduced.
Exact unsigned JSON integers through u64 remain accepted; full-width bounds use
strings. Floating-point/exponent tokens remain unsupported, avoiding rounded
amount comparisons. Inclusive bounds and rejection of inverted ranges remain.

Direct visitors remove intermediate generic value trees. Hex amount decoding
uses fixed-size buffers. Decimal syntax is still checked, but leading zeros
need no uint256 arithmetic; at most 78 significant digits reach exact checked
arithmetic. This preserves valid zero-prefixed decimal strings without adding
an input quota. No measured throughput or allocation-size claim is made.

## Validation and boundaries

Preserve exact-base source and test-only regressions. Verify valid forms,
uint256 boundaries, topic presence, padding, actual/declared data lengths,
retained and ephemeral classification, raw-log preservation and invalid-update
atomicity. Independent design and final-source reviews precede full gates.

No ingestion-write, persisted-format, SQL, dashboard or dependency changes.
No live chain, production dataset, remote host or benchmark. Reorg/removal
notifications, aggregate admission budgets, the remaining dashboard review,
verified offline repair and integrated acceptance remain open.

## Final local validation

Source `cdb175430e8e2bc67a4b5638fb6530d9abba1ef1` passes all eleven local gates:
2,016 workspace tests, zero failures, 24 existing ignores across 37 targets,
documentation checks and the release node build. All 158 focused server tests
pass with two existing benchmark ignores. Strict Clippy, formatting and
independent source review pass.

The [validation record](baselines/2026-09-17-erc20-subscription-inputs.json)
retains exact original/final source hashes, the three original failures, focused
checks, design/final reviews and complete gate logs. The source and validation
archives were decoded and checked byte-for-byte against the recorded files.
Exact-head CI and merge passed; verified closure follows.

All six CI jobs passed on `8957bb7d` and ten Linux volume/template controls passed with verified cleanup. [PR #232](https://github.com/tdenisenko/logex/pull/232) merged as `c85df998`. The merge tree is identical to the tested head. B8-17–19 are closed within literal ERC20 input decoding, amount syntax and exact event classification scope. Reorg notifications, shared query budgets, dashboard, verified repair and integrated acceptance remain open.
