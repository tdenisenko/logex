# Dashboard and status audit

Base: PR #237 merge `93c160c33f1ccf702ec6dcef3f54d690df3851a3`.
Branch: `audit/dashboard-observability`.

This milestone reviews the embedded dashboard, its status projection, browser
state, query builder, result display/export, and keyboard/mobile behavior.
Source `d375e95b` is committed. Focused controls and independent review pass;
all twelve local gates pass. Exact-head CI and merge remain.

## Findings and corrections

| ID | Confirmed behavior | Correction and finite control |
| --- | --- | --- |
| B9-01 | A status request captured fresh/synced consensus state before awaiting the storage lock and could deliver that obsolete state after connectivity changed. | Sample volatile state after storage and metrics awaits. A held-writer control changes shared state while the request is pending and checks the delivered state and withheld live fields. No new nested lock order or ingestion work. |
| B9-02 | Stale consensus telemetry suppressed the activity boolean even while historical backfill was active. | Preserve actual activity for enabled, incomplete historical work; continue withholding unavailable live target, progress, ETA and latest coverage. Stopped, completed and disabled history are preservation controls. |
| B9-03 | Unowned browser polls overlapped; delayed responses could overwrite newer status. Hung requests retained apparently current success, and a storage failure was indistinguishable from a lost status connection. | One request owns fetch and body decoding, with a ten-second abort/deadline and identity checks. Late responses/failures cannot affect a successor. Received age uses a monotonic clock. The existing storage-503 response has a distinct state and text diagnostic. These deadlines concern browser telemetry only. |
| B9-04 | The browser inferred Synced from head proximity, overriding an explicit disconnected state. | Display the server state. Runtime failures can leave recent consensus freshness available briefly, so proximity is insufficient. An actual status-renderer control covers this combination. |
| B9-05 | Current editor text could turn an explicit different event into a Transfer amount; builder token/decimal changes could rescale already loaded results. Custom checksummed addresses also missed lowercase lookup. | Require explicit row event/address evidence. Capture matching custom token metadata at dispatch and publish it with the result; use static known-token metadata otherwise. Missing/conflicting context stays raw. Deferred-response and rerender controls retain exact uint256 values. |
| B9-06 | Raw copy and CSV used array joining, losing nested objects, arrays and explicit nulls. | Serialize structured cell values as JSON. Independent CSV decoding verifies nested values, exact strings, quoting and all loaded rows, including rows outside the visible page. Friendly topic display remains separate. |
| B9-07 | Reselecting the active range mode cleared both bounds, broadening the next generated query. | Same-mode selection preserves values; actual unit changes still clear incompatible inputs. Saved-state initialization was traced against the initial markup. |
| B9-08 | The event builder rejected canonical tuple signatures while accepting aliases and unknown types that hashed differently from canonical events. | Validate canonical ABI elementary types, tuples and arrays with an iterative stack; explicitly reject aliases. Existing canonical bytes, whitespace handling, exact amount predicates, ordering and the explicit generated limit remain unchanged. |
| B9-09 | An empty submission cleared backing results but left the previous table and count visible. | Clear both representations coherently, without a request or history success. |
| B9-10 | History details required pointer activation, and action buttons were clipped on narrow screens. | Native disclosure buttons expose expanded state; the table scrolls horizontally and actions wrap. Actual Chrome controls verify reachability at 320/375/1280 pixels and native Enter/Space activation. Pointer, Use and export behavior remains. |
| B9-11 | A wall-clock rollback froze chart sampling behind future saved timestamps; chart legends could also select future data. | Filter invalid, expired and future samples before throttling and rendering. Finite fake-clock controls cover rollback, reload, throttling and future-only legends. Chart range controls announce selection and decorative motion respects reduced-motion preferences. |
| B9-12 | Floating-point conversion accepted nonintegral block text after rounding or underflow; a native invalid empty input could remove a bound. | Check textual decimal/scientific integrality before numeric conversion, retain the existing safe/nonnegative integer check, and distinguish native bad input from an intentionally blank block/time field. No exponent-sized allocations. |
| B9-13 | Ordinary i64/u64 results were exact on the wire but rounded by the browser before display, copying and export; decoding errors could become successful empty results. | The query-only native JSON reviver preserves unsafe integer source tokens as decimal strings, including nested values. Successful-response decoding/schema failures reach the query error path. Modern and simulated older parser controls cover exact boundaries, floats, strings, errors and history state. |

Canonical signature validation follows the [official Solidity ABI type
specification](https://docs.soliditylang.org/en/latest/abi-spec.html#types),
including tuple/array composition and sized integer, byte and fixed-point types.
This changes builder validation, not server event-literal hashing.

## Review dispositions

- DOM text and data attributes use existing escaping/text setters. Native
  transaction links and copy keyboard handling remain intact. Result/export
  fixes preserve raw values; token formatting continues to use BigInt.
- Query responses preserve ordinary large integers as browser-local decimal
  strings using the [ECMAScript JSON source
  context](https://tc39.es/ecma262/multipage/structured-data.html#sec-json.parse).
  The REST JSON-number contract is unchanged. Safe numbers and floating-point
  tokens retain their previous representation. An older browser without source
  context explicitly rejects potentially unsafe integer-valued results; it cannot
  distinguish a large whole-valued float either. No rounded fallback or new
  parsing dependency is used. Nested raw JSON exports represent preserved large
  integers as decimal strings.
- The existing Chart.js 4.5.0 CDN bytes were verified against the published npm
  package, including registry tarball integrity and anonymous CORS. The same URL
  now carries its SHA-384 integrity value. No dependency version changed or
  downloaded code was executed during verification. An unavailable or changed
  asset follows the existing explicit chart-unavailable display.
- Browser preferences and capped query history remain best effort local state.
  Live transfers restore server snapshots; this milestone does not add automatic
  retries across the already explicit stream-gap reconciliation boundary.
  Earlier ordering/removal/socket ownership fixes remain covered by their
  respective audit records.
- The top timestamp says **Status received**. CPU/disk values are cached and
  refresh in the background, normally after a sixty-second TTL; a refresh can
  take longer. Unknown values remain null/`--`, and unknown CPU chart samples
  remain gaps. This is not a sixty-second maximum sample-age guarantee.
- Status reads scan in-memory segment descriptors. Disk/CPU collection remains
  single-flight blocking work outside ingestion locks. No new storage flush,
  metadata journal, network request or benchmark campaign is introduced.
- There is no secondary-index-readiness widget. The existing worker distinguishes
  unsupported/ineligible segments from missing indexes and validates current
  artifact identity before publication. Physical stored-row bounds and ingestion
  anchors are not renamed or claimed to be secondary-index coverage.
- Storage health remains independent of peer readiness. Unavailable consensus
  withholds live coverage while historical activity can continue. Repair status
  will be integrated with the separate repair coordinator in batch 11.
- A suspected synced/incomplete-history combination was rejected after tracing
  all production callers: `try_mark_synced` checks enabled historical completion,
  and network-state updates downgrade incomplete history. No speculative state
  machine change was made.
- Custom address-label decoration still follows explicitly selected browser
  metadata; amount units are bound to the completed result. No cross-tab browser
  persistence guarantee or new query memory/admission policy is implied.

## Validation and reproducibility

The tracked tests use Node built-ins and evaluate the actual embedded functions:

```bash
node --test tools/dashboard-*.test.mjs
```

`DASHBOARD_SOURCE` selects an archived original HTML file for before/after
controls. One shared loader avoids duplicated extraction mechanics. The tests
use finite promises, clocks, result rows and DOM stubs without application
requests. Complete inline-script parsing supplements the focused runtime checks.
Both existing Linux and macOS CI test jobs run these controls.
They select Node 24 with the official `actions/setup-node` v6 action pinned to
commit `249970729cb0ef3589644e2896645e5dc5ba9c38`; package caching is disabled.
The tests need a modern native JSON parser as well as the simulated older-parser
controls. This is test tooling, not a node runtime dependency.

Original-production failures are retained separately from implementation passes.
The REST regressions hold a local storage guard and poll the handler directly;
they require no sleeps or peers. Visual fixtures contain actual component CSS,
markup and functions with application initialization and external resources
disabled. Chrome uses a dedicated temporary profile. Geometry, keyboard input
and reduced-motion computed styles are checked; this is not a full browser
network end-to-end test.

Frozen-source workspace results are recorded below; exact-head CI must pass before merge.
The broader offline audit, verified repair, integrated acceptance, later live
sync and the staging soak remain separate. This milestone is not release
acceptance.

## Frozen-source validation

Source `d375e95b16e8389a3e89cf7f94be9806f357aa88` passes all twelve local gates: 36 dashboard controls, vendor
integrity, workspace and four patched-package format checks, workspace check,
strict Clippy, all-target Rust tests, documentation tests and the release node
build. There are 2,074 passing Rust tests, zero failures and
24 existing ignores across 37 targets. Documentation checks pass
across 8 targets (0 examples). Focused REST tests pass 39/39.

The [machine-readable record](baselines/2026-09-18-dashboard-observability.json)
contains exact commands, final source hashes and browser geometry/keyboard
results. Compressed [review and original-control evidence](baselines/2026-09-18-dashboard-observability-evidence.json.gz)
and [validation logs](baselines/2026-09-18-dashboard-observability-validation.json.gz)
are JSON archives with individual file sizes and SHA-256 digests. Screenshots
were inspected locally; recorded geometry and reproducible fixtures provide
the retained automated controls. The dedicated browser profile was removed.

These are correctness records, not throughput/RSS benchmarks. The source does
not modify ingestion, storage encodings or query execution. Linux/macOS CI and
the existing ten Linux volume/template controls remain required before merge.
