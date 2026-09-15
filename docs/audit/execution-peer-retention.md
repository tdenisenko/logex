# Execution retry-hint retention and admission

This milestone bounds learned execution retry hints and keeps valuable candidates
eligible when the pending discovery queue fills. It preserves active request,
session, submitted-dial and backoff ownership. Saved peer snapshots retain their
existing 512-record format and publication contract.

## Findings

- **B4-06: live retry hints could accumulate indefinitely.** The persisted
  snapshot and productive list were bounded, but accepted reachable sessions and
  productive peers appended new identities to a separate `known_peers` vector.
  DNS/discovery generally feeds pending candidates; not every discovery event
  grows this vector. Reseeding repeatedly copied the entire retained list.
- **B4-07: valuable retries could be excluded before dial selection.** At the
  4,096-entry pending limit, every new identity was rejected, including productive
  or configured peers when the map contained only fresh discovery candidates.
  The later productive-biased dial selector could not select a missing candidate.
- **B4-08: reseeding counted attempts as admissions.** Its counter increased even
  when pending admission rejected a record because of capacity or eligibility.
  It now counts successful new-ID admission, including priority replacement.

Final review also caught a draft-limit integration issue: family filtering alone
would allow zero-TCP or built-in bootstrap entries to consume all retained startup
slots. Apply complete retry eligibility before the unique-record limit. Discovery
seed configuration remains separately available for entries that are useful only
for discovery. This is a corrected draft finding, not claimed as a preexisting
512-record truncation bug.

## Retention policy

Retain at most 512 learned, nonconfigured records. Eligible configured direct
seed identities are recorded explicitly and contribute a separate term bounded
by user configuration. Built-in mainnet bootnode detection is not a substitute
for configured-seed provenance, particularly with IPv6 direct fallback.

Preserve first/best eligible startup records while deduplicating before the limit.
Runtime observation still refreshes an existing identity's endpoint. On pressure,
remove the oldest nonconfigured, nonproductive hints. Productive records already
have a 512-entry lifecycle; when that set is full, an additional unproductive
hint may be omitted until it proves useful. Explicit invalid-peer forgetting,
receipt quarantine and existing disconnect policy still remove records. The
configured ID set does not itself recreate a forgotten peer record.

The policy bounds actual learned records, not merely total length plus every
configured ID ever registered. Forgetting a configured record must not increase
learned capacity. Active and submitted records remain owned separately, and
pressure eviction does not disconnect or cancel them.

## Pending admission and reseeding

Keep the existing 4,096 pending limit. An existing identity can refresh its
endpoint without growth. With free space, normal admission is unchanged. At
capacity, a new record may replace only a strictly lower-priority pending hint:
productive first, then configured, previously reachable known, and fresh.
Fresh discovery cannot replace a full queue, and equal classes do not churn one
another. Victim ranking uses lookup sets for the full-queue case, avoiding a
nested scan of all retry lists for each pending entry.

Remove the complete known-list clone. Node records are copied one at a time from
an indexed scan; pending admission does not mutate that inventory. A cursor moves
to the entry after the last new admission, so the next free slot is not always
assigned from the beginning of a large configured list. Empty lists and removal
normalize the cursor. A full unsuccessful pass does not reset admission progress.
This improves admission opportunity, without asserting deterministic service for
every endpoint under the existing dial selector and continuing peer churn.

The existing selection balance between productive candidates and fresh discovery
remains. This is not a new request retry limit, peer-reputation policy, consensus
trust decision, or global networking memory budget. User-sized configuration and
other bounded/unreviewed networking structures remain explicit.

## Validation and implementation cost

Five new deterministic tests cover eligibility-before-limit and duplicate startup
input, first-record ordering, configured/productive retention, explicit forgetting,
1,024 finite identities, productive turnover, priority replacement, endpoint refresh,
full-queue rejection and admission cursor behavior. The complete sync library suite
passes: 336 tests, one ignored.

Two controls model the prior policies inside the candidate helpers: no pruning
following the existing upsert sequence, and unconditional rejection of new identities
at a full pending queue. The churn and priority regressions fail as expected under
those policies, then pass after byte-for-byte source restoration. These are modeled
prior-policy controls, not exact original methods or a full baseline checkout.
Independent reviews trace inventory owners and approve the final implementation
within scope with no remaining actionable findings. Source and evidence hashes are
recorded in [the validation record](baselines/2026-09-16-execution-peer-retention.json).

Removing the unbounded clone and capping retained work are implementation-level
cost improvements. No benchmark or ingestion-throughput percentage is claimed.
There is no file-format change, additional disk write or durability barrier.
Full validation and merge results will be recorded before completion.
