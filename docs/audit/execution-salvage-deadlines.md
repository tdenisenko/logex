# Execution prefix salvage deadline

This milestone enforces the existing 12-second prefix-salvage budget across
waiting for bodies, receipts, peer retries and continuation exchanges.

## Finding B4-20

**Severity: medium, availability.** After the main paired request pipeline stops,
LogEx may have useful later chunks but a gap at the required prefix. The salvage
fallback attempts up to two missing ranges and four peers per role. Its 12-second
elapsed checks previously ran only between whole role-fetch awaits. The inner
body and ETH68/69 receipt helpers can collect positive partial responses across
many individually timed exchanges; ETH70 also continues within a block. Each
exchange starts a fresh 4–8 second timeout, so useful partial replies can keep one
role fetch running past the intended salvage window.

The main pipeline already bounds its wait slices within its separate 45-second
window. That window ends before salvage and does not protect this later phase.
The historical engine executes the plan without another enclosing salvage timer.
Finite requested hash counts bound ordinary body and ETH68/69 prefix iterations;
this finding does not claim those loops are infinite. General standalone ETH70
aggregate policy remains a separate review item.

## Correction and invariants

One pinned Tokio timer at salvage entry is reused across both rounds. A biased
select checks that timer before polling the current range future. Once the local
budget expires, salvage returns the accumulated completed-chunk count. Obsolete
elapsed checks and the start-time argument were removed from the private helper. The
normal per-exchange timers and continuation implementation remain unchanged.

The timer has priority when local expiry and a role result become ready together.
Local budget exhaustion is not converted into a peer timeout or invalid response.
Dropping the unfinished range drops its active-request guard and emits the normal
Finished accounting event. Earlier genuine failures and completed role-success
statistics remain, as do previously published paired chunks and existing buffered
suffix chunks. An incomplete private role prefix is discarded as on existing
cancellation; it is not promoted to a queryable or trusted block.

The deadline is cooperative asynchronous cancellation and cannot preempt
synchronous work within one poll. It closes outstanding response receivers and
cancels unadmitted sends. Reth may continue an already-admitted transport request;
this change bounds waiting for salvage, not the lifetime of that upstream request.
There is no new timer task, per-row validation, storage write, byte cap or global
standalone-request timeout. Successful salvage inside the existing window keeps
its normal sources and results. No benchmark or measured throughput gain is claimed.

## Validation status

Seven pure-channel paused-clock controls ran against unchanged production at
`76ecd184`: the within-budget success control passed and six regressions failed.
They cover partial body progress, separate ETH68/69/70 receipt continuations,
a simultaneous wire/local timeout and preservation of an earlier complete round.
Five stayed pending beyond the local budget; the timer-tie case instead recorded
a peer failure. The original checks are not reached while continuation awaits
remain pending, so these results do not depend on advancing std::Instant with
a paused Tokio clock. No listeners, peer connections or chain writes are used.

All eight corrected-path controls pass, including a genuine earlier wire timeout
followed by a successful retry through another body peer. They verify source IDs,
preserved chunks, exact role statistics, peer failures and balanced active guards.
Independent source review found no actionable defect in the correction.

Seven local gates, exact-head CI and merge remain pending before this milestone
can close. Evidence and source identities are recorded in
[the validation record](baselines/2026-09-16-execution-salvage-deadlines.json). The broader offline audit, volume supervision,
verified repair and subsequent live/staging acceptance remain incomplete.
Mac-mini audit tests and obsolete-artifact cleanup remain complete; no remote
work is required here.
