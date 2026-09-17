# Single-request JSON-RPC admission and correlation

Base: PR #228 merge `910a74aea39b7b358d397651450691acb90192cd`.
Branch: `audit/jsonrpc-admission`.
Implementation and eleven local gates are complete; PR #229 merged as `61f2506b` after all six CI jobs.

## Findings

**B8-09 — request envelopes and notifications (P2).** The original HTTP handler dispatches
unvalidated version/ID shapes; the stock typed JSON extractor returns transport
errors for malformed syntax and for a valid notification without an ID. Validate
single-request structure before dispatch, distinguish absent ID from explicit
null, return protocol parse/invalid-request errors and suppress responses to
valid notifications after their ordinary awaited execution. The parser consumes
invalid field shapes and duplicate fields before selecting an invalid-request
response, allowing the extractor to identify a malformed remainder.

**B8-10 — parameter errors wait for storage and become internal errors (P2).**
The original handler acquires query/worker/storage state before recognizing an invalid
log-filter argument. Its generic String error path reports client argument errors
as internal failures. Parse supported method arguments before admission and keep
invalid-parameter errors separate from real storage, worker and snapshot errors.
Preserve the literal parameter root kind; a dependency-internal raw-value object
must not turn named arguments into a positional array.

**B8-11 — numeric correlation IDs are rounded (P2).** An original metadata request
with numeric ID `18446744073709551617` returns `1.8446744073709552e+19`. Preserve
validated ID tokens through response serialization using the pinned library's
raw-value support. This concerns request correlation, not stored/query values.

## Invariants and implementation scope

- One HTTP entry point handles single-request parsing, dispatch and response
  suppression. No compatibility dispatcher added merely to avoid updating tests.
- Only validated string, numeric and null IDs are echoed. Omitted ID denotes a
  notification; malformed envelopes without an ID remain errors. Unknown methods
  stay method-not-found. Missing and explicit-null IDs are distinct.
- Metadata methods need no storage and remain available during storage failure.
  Their arguments must be empty. Log queries keep one positional filter and the
  existing pagination/filter semantics; detailed Ethereum filter fixes follow.
- Notifications await the same owned work as requests, retaining cancellation,
  worker permits and snapshot checks. No detached notification tasks or batch
  fan-out are introduced. Valid notifications return an empty HTTP 204 response.
- Preserve authentication ordering, body limits, content-type and transport errors.
  Raw ID capture does not replace ordinary parameter deserialization or enable
  global arbitrary-precision parsing. No dependency version change is required.

The [JSON-RPC specification](https://www.jsonrpc.org/specification) defines the
version, ID, notification and error distinctions used in this correction.

## Explicit limits

Batch execution remains unsupported and is a separate compatibility/resource
item. Sequential collection can still retain every completed response, so adding
it is not an incidental parsing fix. Empty batches are invalid requests; nonempty
batches retain an explicit HTTP 422 response and are not executed. The shared
query resource decision remains open.

Nested parameter values retain the pinned ordinary Value decoder; its internal
raw-value conversion and early data errors are not claimed fixed globally. That
detailed filter-decoding follow-up remains recorded separately.

Ethereum topic/wildcard/range/tag semantics, WebSocket gaps/reorg notifications,
acknowledgement snapshot cleanup and dashboard history order remain separate
milestones. No live sync, production data, remote host or benchmark is involved.

## Evidence

Exact-original router controls reproduce four admission failures while the
transport/authentication control passes. A separate tiny wire assertion reproduces
the numeric ID change. Original source, test-only changes and logs are retained;
candidate-only wire-type and syntax-precedence regressions are separately
retained. The final document visitor records shape faults while consuming the
input, preserves ordinary nested parameter parsing and retains raw values only
for IDs. Final regression, independent review and workspace evidence are recorded below;
exact-head CI remains required before merge.

The initial workspace test attempt was restricted by the sandbox: existing node
and checkpoint fixtures could not bind their local listeners. The failed log is
retained separately; the unchanged-source test gate passed with local fixture
permissions, followed by documentation checks and the release build. No test is removed or ignored for that failure.

## Final local validation

All eleven final-source local gates pass: 1,987 workspace tests, zero failures, 24 existing ignores across 36 targets, documentation checks and release node build.

Source is `333fcf409b1fe11beeac8a6e858775a11248dd3b`. The [validation record](baselines/2026-09-17-jsonrpc-admission.json)
retains source hashes, original and candidate regressions, independent review and
full gate logs. All 130 final server tests pass with 2 existing benchmark ignores.
Vendor sources are unchanged; CI retains pinned expiry and volume regressions.
Exact-head CI and merge passed; verified closure follows.

All six CI jobs passed on `bd556e0b` and ten Linux volume/template controls passed with verified cleanup. [PR #229](https://github.com/tdenisenko/logex/pull/229) merged as `61f2506b`. The merge tree is identical to the tested head. B8-09–11 are closed within single-request admission and correlation scope. Batch dispatch, detailed filters, subscriptions, shared query budgets and integrated acceptance remain open.
