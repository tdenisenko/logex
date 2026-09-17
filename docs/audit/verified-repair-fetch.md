# Finite authenticated repair fetching

This batch 11 prerequisite adds an explicit, finite repair cursor in `logex-sync`.
It does not run the repair command, write replacements, publish coverage or
complete automatic repair. Primary-data inspection was merged in PR #239.

## Scope and invariants

The caller supplies an intact, currently selected consensus execution anchor and
an inclusive range ending at or below it. An `ExecutionAnchor` value is not itself
proof of provenance. The future coordinator must select the anchor from verified
consensus state and revalidate its selection before publication.

The cursor verifies the exact anchor header and its standalone rules, then checks
reverse parent links through bounded pages. Headers bridging from a higher anchor
are authenticated without requesting their payloads. Every delivered block is
contiguous in descending height order. Nonempty partial header pages can advance;
empty responses mean unavailable history, not a zero-log block.

Body commitments are checked before receipts are requested. Receipt count, root,
gas and bloom are verified through the existing execution validators before the
existing extractor produces rows. Historical PostState receipts remain intact.
Transaction positions include transactions without logs, and block-global log
indices retain their existing semantics. Blocks with no rows still produce a
whole-block result. Only a separate completion result proves the exact requested
block count was delivered. It does not prove that the caller staged those results.

Verified-block and completion constructors are private. The completion carries
the original anchor, range and delivered block count. No normal sync progress,
ingestion, reorg notification, catalog or segment publication is invoked.

## Ownership, limits and failure

The cursor retains one bounded header page, its ancestry cursor and one block's
payload/extraction buffers. It does not collect the entire range. All budgets are
explicit caller inputs, with no new production defaults: page size, total headers
including the bridge, transactions, encoded body size, rows, log-data bytes,
request timeout, selected peer attempts and an absolute operation deadline.

Limits screen validation/extraction inputs after existing network decoding. They
are not a process RSS or wire-allocation guarantee. The absolute deadline includes
time between cursor calls, including staging. CPU validation is synchronous;
cancellation and time are checked between finite steps, not through preemption.
The future maintenance coordinator must own runtime responsiveness and shutdown.

An error or drop of a polled operation makes the cursor terminal. An interrupted
page cannot be resumed accidentally after internal state advanced. Prior results
remain unpublished staging work; a fresh plan/cursor is required to continue.
Errors distinguish invalid input/data, unavailable history, resource limits,
cancellation, deadline, terminal state and local failures without parsing text.

The real PeerManager adapter uses explicit-limit requests. Those calls do not
reserve the ordinary bulk scheduler's ownership entries. Dropping a wait closes
its local response receiver; Reth may still own a queued/inflight wire request
until response, native timeout or session teardown. This module does not promise
wire revocation or transactional rollback of peer accounting. Networking can still
persist its normal optional peer cache. Existing request futures are awaited by
their owning task; this API does not add a cross-thread future guarantee.

## Validation and review

Eleven finite cursor controls cover bridge/partial pages, exact empty/no-log block
coverage and ordering, malformed or unavailable input, resource limits, incomplete
prefixes, cancellation and dropped waits, later-page ancestry, pre-Byzantium
receipts, genesis, maximum-height completion and actionable source-error context. Three existing dormant-network
fixture controls verify the limited reverse wrapper's request parameters,
timeout/peer limits and receiver cancellation. Discovery and external connections
are disabled; no live sync or production data is used.

Independent reviews cover transport lifetime, fixture validity and complete cursor
behavior. Initial compilation exposed two draft assumptions: existing validation
errors implement Display but not Error, and existing peer-request futures do not
meet a Send promise. Explicit diagnostic mapping and the existing owning-task
model resolved those integration issues. Strict Clippy identified the large block
result variant; boxing that owned result keeps the step enum compact. These were
new implementation issues, not claims of previously shipped ingestion bugs.

Source `cbae4942` passes all twelve local gates: 2,117 Rust tests with zero
failures and 24 existing ignores across 37 targets; documentation checks across
eight targets; 36 dashboard controls; formatting, vendor integrity, workspace
checking, strict Clippy and the locked release build. The [baseline](baselines/2026-09-18-verified-repair-fetch.json)
records hashed evidence and validation archives. Exact-head Linux/macOS CI and
merge remain required.

## Cost, cleanup and remaining work

Ordinary live/historical ingestion call paths, encodings, flushes, dependencies
and the pinned toolchain are unchanged. Repair uses one payload block at a time;
this favors bounded retained work over parallel repair throughput. No benchmark
claim is made and no new timing campaign was run. Existing unrestricted reverse
requests remain necessary for normal sync callers; no obsolete caller or code
path was found to remove in this scope.

Remaining batch 11 work includes complete-block replacement planning and overlap
ownership, preserving unrelated/noncanonical rows, derived-index recovery,
resumable staging/quarantine/publication, disk checks, read-only volume preflight,
CLI/status/runtime integration and interruption/equivalence controls. The
coordinator must never treat local commitments, row extrema or a fetched prefix
as proof authorizing replacement. Offline audit and release acceptance remain open.
