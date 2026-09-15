# Execution cache canonicality and range contracts

This milestone keeps the outgoing execution cache consistent when restored
headers or previously evicted bodies are reverted, and corrects provider range
and sealed-header behavior. It changes no persisted data or trust validation.

## Findings

**B4-16 — medium, outgoing canonicality.** Reorg removal required an entry in the
body reverse index before removing either a header or body. Startup restores
recent verified headers without bodies, and body eviction also leaves retained
headers. Both therefore survived invalidation. The actual startup/reorg path
reaches these operations, and Reth's header handler reads the affected maps.
A reverted header could remain available until replacement or eviction. This
finding does not demonstrate acceptance of invalid ingestion data.

**B4-17 — medium, provider consistency.** Inserting a different header at a cached
height retained the previous canonical body and receipts. Sealed-header reads
used the body hash and independently resolved the header, allowing a mismatched
pair; restored headers without bodies could not be sealed at all. Concurrent
replacement between the separate read guards could also mix a hash and header.
The deterministic replacement sequence reproduces the mismatch. Current
production header-only insertion occurs during startup; no normal live
header-only replacement or direct Reth use of the sealed-header API is claimed.

**B4-18 — low, provider range semantics and cost.** Saturating exclusive-bound
arithmetic made `..0` include genesis. Unbounded header ranges stopped at the
body tip and omitted restored headers. Range methods iterated every numeric
height instead of the bounded retained inventory. Current Reth wire handlers
use their own request-count loops, so this is a provider contract and local work
finding, not a demonstrated remotely reachable range amplification.

## Correction and implementation cost

Header and body indexes retain separate availability but share canonical identity
at each height. Reorg removal independently removes each hash's owned mappings.
Conflicting header replacement discards the old body and receipts; reinserting
the same header retains them. Removing an obsolete hash cannot erase its
successor. Header-only insertion reports removed bodies to the peer manager so
its existing advertised-history update runs when needed.

Point reads resolve identity and clone the corresponding value under one read
guard. Sealed headers use that exact stored hash without recomputing it. Range
bounds use checked arithmetic and reject empty/inverted ranges before map access.
Header ranges merge the ordered header/body map slices; body, receipt,
transaction and index ranges visit only body-map slices. Missing heights create
no work proportional to the numeric span. Header/body range APIs preserve their
implicit genesis fallback; canonical hash ranges continue to return only stored
entries. Each call has one coherent snapshot, not a guarantee across separate
provider calls.

The external sealed-header predicate runs after releasing the guard. This needs
an owned snapshot, including later matching headers even if the predicate stops
early. The inventory is bounded by 8,192 header entries plus 4,096 bodies. Body
snapshots likewise hold a read guard while cloning the requested retained
payloads. These are count bounds, not byte or resident-memory bounds; optional
payload admission remains a separate review item.

Insertion reuses the already computed header hash and clones receipts without
copying a discarded bloom. Transaction queries clone only transactions, and body
index queries read only transaction counts. Body-index lookups consequently no
longer count a full block as uploaded. Other range payload estimates are summed
once per result rather than repeatedly locking metrics for each block; sealed
predicate accounting still includes the examined stopping header. Existing
estimates remain approximate and are not wire-byte measurements.

Removed the superseded body-hash/whole-block/receipt lookup paths and integer-span
range scans. No storage write, migration, scheduler change, dependency upgrade,
remote experiment or benchmark is introduced. No quantified throughput claim is
made.

## Validation and remaining scope

The original production code, with only regression tests added, reports ten
passing and eight failing cache controls. Failures cover restored and evicted
header invalidation, replacement consistency, sealed-header availability/hash,
exclusive zero and restored-header range extent. All eighteen controls pass after
the initial implementation. Five additional controls pass for full-domain sparse
ranges, retained-body fallback after header eviction, body-change reporting,
delayed invalidation and callback mutation snapshots (23 cache tests total).
Whole-domain controls use only a handful of cached entries and were not run
against the original numeric-span loops. Final independent review and workspace
gates are recorded in the linked validation record before merge.

See [the validation record](baselines/2026-09-16-execution-cache-canonicality.json).
Cache payload admission, aggregate request budgets and cheaper/honest network
telemetry remain separate items. The wider offline audit, external-volume
supervision, verified repair, live sync and staging acceptance remain incomplete.
Mac-mini testing and obsolete audit-artifact cleanup are complete; no remote
access is needed for this milestone.
