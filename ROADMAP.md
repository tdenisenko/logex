# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed verified logs, and serves dashboard, SQL query, JSON-RPC, gRPC, and live ERC20 transfer subscription APIs.

Active branch: `perf/fresh-historical-baseline`. When outside the home network, Mac mini operations must use `ssh -J pi-remote gremlinmaster@192.168.50.44`. The completed baseline run used `/Users/gremlinmaster/logex-fresh-baseline-src`, data dir `/Volumes/SSD 4TB/LogEx`, HTTP port `18683`, tmux session `logex`, and run dir `/Users/gremlinmaster/logex-baseline-runs/fresh-baseline-20260628-142644`; the `logex-baseline-monitor` tmux session exited normally after writing `summary.json`.

Historical sync has completed the active post-fix pivot-to-genesis baseline. The accepted scheduler keeps the critical historical fetch active, buffers cheap ready fetch plans ahead of slow header windows, prioritizes missing expected fetches before lookahead prepare work, avoids blocking ordered writes on expensive post-write refill when prepared batches are already queued, forces a limited local-work refill while prepares or writes are waiting, discards same-sequence work planned against stale expected child headers, lets write-path refills fill the adaptive active pipeline, gives expected historical fetches full body/receipt lane budget while capping lookahead lane budget, adds bounded early redundancy for the first expected-prefix chunks, keeps the next fetch sequence cursor monotonic after ordered writes advance the expected cursor, preserves one missing execution-client-family probe when truncating the body/receipt candidate pool, skips expensive prefix salvage when the body/receipt live plan already has an acceptable contiguous prefix, and defers background compaction planning while historical sync is incomplete so maintenance scans cannot starve ordered historical writes. The post-fix run reached genesis without repeated liveness stalls, kept peers healthy, and showed median physical RX near the 300 Mbps link ceiling; another clean wall-clock benchmark is not required unless a new post-fix health problem appears.

## Completed Since Last Run

- Reproduced the zero-progress stall through the Pi jump host: the floor stayed pinned while peers and active downloads remained present.
- Added critical-path repair for missing expected historical fetches without resetting buffered lookahead.
- Aborted stale historical fetch work below the expected sequence so obsolete attempts release request reservations.
- Confirmed the patch moved the remote floor off the pinned block and restored high instantaneous throughput; warmed samples still show bursty ordered progress.
- Split cheap ready fetch plan buffering from the active/heavy historical fetch pipeline.
- Measured the ready-plan change on the remote warmed run: average floor movement improved from `161` to `199` blocks/sec, low windows fell from `12` to `6`, and zero windows fell from `3` to `1`.
- Prioritized missing expected fetch refill ahead of lookahead prepare work so later buffered work cannot delay the next block range needed to advance the verified floor.
- Avoided blocking the ordered write loop on post-write pipeline refill when prepared historical batches are already queued.
- Added proactive missing-expected refill immediately after ordered writes advance the historical cursor.
- Measured the latest remote warmed run at `273` blocks/sec average with `1` low window and `0` zero windows after peers warmed to the low/mid 30s.
- Added an active-download-aware post-write refill gate so prepared backlog can no longer hide an empty active body/receipt pipeline.
- Measured the follow-up remote warmed run at `326` blocks/sec average with `0` low windows and `0` zero windows after peers warmed past 20 serving peers.
- Re-ran remote validation through `pi-remote` after direct network access failed outside the home network.
- Rejected a completed-buffer overflow experiment: it measured `279` blocks/sec with `3` low windows and `1` zero window.
- Rejected an eight-lane active-target experiment: it measured `326` blocks/sec with `1` low window and `1` zero window versus the accepted baseline at `324` blocks/sec with `0` low windows and `0` zero windows on the same route.
- Added active fetch child-header tracking so expected-sequence work can be discarded immediately when ordered writes advance to a different child header.
- Increased write-path refill headroom so active body/receipt downloads can refill to the adaptive pipeline depth instead of staying capped at four total refill slots.
- Measured the active-refill/stale-child build at `264` blocks/sec average with `4` low windows and `1` zero window; it reduced active-depth collapse but did not eliminate peer-tail stalls.
- Rejected a shorter expected-fetch hedge delay after it produced `2` zero windows within the first few minutes despite more than 25 serving peers.
- Re-ran the remote tests through `pi-remote` after the Mac mini direct route became unreachable outside the home network.
- Rejected a lookahead-promotion experiment for missing expected historical fetches: it measured `126.2` blocks/sec with `8` low windows and `2` zero windows, below the accepted baseline.
- Rejected a partial-prefix salvage skip experiment: it measured `107.9` blocks/sec with `7` low windows and `1` zero window, and changed burst shape without improving floor movement.
- Restored and rebuilt the accepted baseline on the Mac mini tmux session after each rejected experiment.
- Added priority-aware body/receipt fetch budgeting: the expected historical sequence keeps full live prefix scheduling, while lookahead sequences use a capped prefix lane so they cannot consume all body request slots.
- Measured priority budgeting on two warmed remote samples through `pi-remote`: `172.1` blocks/sec with `3` low windows and `1` zero window, then `156.4` blocks/sec with `5` low windows and `3` zero windows. This beat the immediate post-jump baseline average but did not eliminate ordered bursts.
- Rejected a wider full-priority candidate-pool experiment: it measured `140.7` blocks/sec with `11` low windows and `7` zero windows, then was reverted locally and remotely.
- Added local-work historical fetch refill while prepare tasks are waiting, using the same bounded refill path already used during writes.
- Measured the prepare-refill change after peers warmed to 29-32 serving peers: `238.4` blocks/sec average with `5` low windows and `2` zero windows, improving the previous warmed jump-host baseline (`175.9`, `7`, `6`) and removing the previously observed `active_fetches = 0` idle windows from the final sample.
- Validated locally with focused scheduler, sequence-gap, historical fetch tests, and `cargo check` for touched crates.
- Rejected a dense-prefix yield-size experiment: it measured `216.1` blocks/sec with `2` low windows and `0` zero windows and did not remove long body/receipt plan tails.
- Added bounded early redundancy for the first full-priority body/receipt prefix chunks; remote samples measured `378.0` blocks/sec with `1` low/`1` zero window and `327.7` blocks/sec with `0` low/`0` zero windows.
- Validated the accepted redundancy change with `cargo fmt --check`, `cargo test -p logex-sync body_receipt_ -- --nocapture`, `cargo test -p logex-sync historical_ -- --nocapture`, and `cargo check -p logex-node`.
- Reproduced a restart-era dry spell where `historical_fetch_expected_sequence` advanced ahead of `historical_fetch_next_sequence`, causing new work to be queued under stale sequence numbers until timeout recovery.
- Enforced the monotonic fetch cursor invariant after single fetch completion, materialized lookahead advancement, and ordered coalesced writes.
- Deployed the fix to the Mac mini through `pi-remote`; the follow-up 5 minute sample measured `287.9` blocks/sec with `0` low windows and `0` zero windows.
- Fixed strict clippy warnings in the touched scheduler/body-receipt areas.
- Re-ran the remote benchmark through `pi-remote` after a network-unreachable sample; the current route is valid and bulk download traffic is local, not through the WireGuard dashboard tunnel.
- Preserved a missing execution-client-family probe when the historical body/receipt candidate pool is truncated, so connected Nethermind peers do not remain permanently outside the request pool when the fastest/proven prefix is Geth-heavy.
- Measured the peer-family probe build at `297.3` blocks/sec with `2` low windows and `1` zero window; the sample showed Nethermind peers entering the serving set and improved the previous warmed post-cursor sample (`235.7`, `4`, `2`), but did not eliminate the prepared-backlog zero window.
- Rejected a 256-block live progress-target experiment: it measured `173.1` blocks/sec with `5` low windows and `1` zero window, below the accepted checkpoint.
- Identified body/receipt prefix salvage as an avoidable long tail: pre-change role logs showed salvage running despite an already acceptable contiguous prefix, with plans taking up to about `21s`.
- Added an accepted-prefix gate before salvage so live body/receipt plans return usable contiguous progress immediately instead of spending the salvage timeout on an optional prefix repair.
- Measured the salvage-gate build through `pi-remote`: `354.8` blocks/sec, `2` low windows, and `0` zero windows on the existing run. Candidate-window role logs showed salvage on only `2/417` plans and `7` plans over `10s`.
- Re-ran the accepted salvage-gate build through `pi-remote` after the direct route became unavailable: `306.6` blocks/sec, `2` low windows, and `0` zero windows, with physical download traffic on the local interface and WireGuard near idle.
- Rejected a full-priority 2 second partial-prefix flush experiment after it produced a zero-progress window and stayed around `190` blocks/sec before the sample was stopped.
- Rejected a wider dense active-pipeline experiment after it produced a zero-progress window and stayed around `179` blocks/sec before the sample was stopped.
- Restored the accepted baseline locally and on the Mac mini tmux session after both rejected experiments; the remote client is running on the accepted build with data dir `/Volumes/SSD 4TB/LogEx`.
- Rejected a planned-prefix residual carry-forward experiment: the cold sample was smooth (`266.4` blocks/sec, `0` low/`0` zero), but the warmed sample fell below baseline and hit `2` low windows before it was stopped.
- Restored the accepted baseline locally and on the Mac mini tmux session after the residual carry-forward experiment.
- Rejected an async residual body/receipt carry-forward experiment: it validated locally but measured only `123.2` blocks/sec with `11` low windows and `0` zero windows after peers reached 20+ connected, far below the accepted baseline.
- Restored the accepted baseline locally and on the Mac mini tmux session after the async residual experiment.
- Re-ran the restored accepted baseline through `pi-remote`: the warmed sample measured `348.3` blocks/sec with `1` low window and `0` zero windows, with physical RX repeatedly near the 300 Mbps network ceiling.
- Ran a longer 30 minute restored-baseline sample through `pi-remote`: `644,617` blocks over `1,785s`, `361.1` blocks/sec average, `2` low windows, and `0` zero windows while connected peers ranged roughly from the low 40s to low 80s.
- Started the destructive fresh baseline after explicit approval to reset the active data dir.
- Used `BASELINE_RESET_MODE=discard-incomplete` so the existing full-sync backup stayed intact while only the incomplete active `/Volumes/SSD 4TB/LogEx` contents were removed.
- Built the remote release binary, restarted LogEx in tmux, restored the preserved peer cache, and started the baseline monitor at `/Users/gremlinmaster/logex-baseline-runs/fresh-baseline-20260628-142644`.
- Verified the fresh run status endpoint, tmux sessions, active data dir, and preserved full-sync backup.
- Investigated the fresh-run two-hour zero-progress stall at block `11377745`.
- Confirmed the stall was a liveness bug: an ordered historical batch waited about `7,445,409ms` before it could commit while background storage maintenance scanned segment metadata under the storage read lock.
- Deferred background compaction planning while historical sync is incomplete; historical write batches still use their synchronous compacted write path.
- Deployed the fix to the Mac mini, restarted LogEx in tmux, and confirmed the historical floor advanced past the stalled range after restart.
- Updated the baseline monitor automation to watch for post-fix stalls and health issues until genesis, then clean up the remaining todo and delete itself if the run completes cleanly.
- Completed the active post-fix historical baseline to genesis: `summary.json` marked completion with `last_floor = 0` and `937` samples.
- Verified live head tracking after completion with two `/status` samples one minute apart; the live head advanced from block `25423032` to `25423037`.
- Reviewed recent service and monitor logs after completion; only normal discovery warnings were present and no monitor errors were found.
- Recorded post-fix health metrics: `p50` historical rate `139,605` logs/sec and `1,332` blocks/sec, `p90` physical RX `305 Mbps`, connected peers `p50` `97`/`p90` `103`, serving peers `p50` `30`, peak RSS `6.8 GB`, minimum disk free `476.6 GB`, `32` low windows, and `10` zero windows.
- Opened PR #97 (`Optimize historical execution sync scheduler`) for the completed historical sync performance branch.
- Ran local CI-equivalent validation successfully: `cargo fmt --all -- --check`, `cargo check --workspace`, `cargo clippy --workspace -- -D warnings`, and `cargo test --workspace`.
- Confirmed GitHub Actions jobs for PR #97 currently fail before running any workflow steps because Actions quota is unavailable; merge is deferred until checks can be rerun.

## Remaining TODOs

1. Conclude the historical sync performance PR.
   - Reason: the active post-fix baseline reached genesis without repeated liveness stalls and remained bounded by the available network rather than a confirmed code bottleneck.
   - Completion criteria: rerun PR #97 GitHub Actions after quota is available, confirm checks pass, and merge when checks allow.

## Design Decisions

- Historical backfill keeps ordered verification as the commit boundary.
  - Why: logs are valid only after block/receipt data is cryptographically checked and written in canonical reverse order.
  - Tradeoff: unordered downloads can run ahead, but the scheduler must explicitly protect the next-needed sequence.

- Missing expected historical fetches are refilled without discarding buffered lookahead.
  - Why: resetting all lookahead wastes useful work and creates more burstiness.
  - Alternative considered: full pipeline reset, which fixed some gaps but caused avoidable churn.

- Stale fetch work below the expected sequence is aborted.
  - Why: those results can no longer advance the floor and otherwise keep body/receipt reservations occupied.
  - Tradeoff: a small amount of already-started network work may be discarded to keep critical slots available.

- Ready fetch plans use a separate cheap buffer from active/heavy fetched data.
  - Why: slow reverse-header planning windows were letting active body/receipt downloads run dry even when memory and bandwidth were available.
  - Tradeoff: the scheduler keeps more header/plan metadata in memory, while active receipt/body downloads remain capped by the pipeline depth.

- Ordered writes no longer wait on non-critical post-write refill when prepared batches are ready.
  - Why: a measured stall spent about 24 seconds in refill after a batch was already written, preventing the next verified prepared batch from advancing the floor.
  - Tradeoff: refill may run slightly later when there is prepared backlog, so active download depth still needs a follow-up refill policy that keeps the network busier without increasing memory pressure.

- Post-write refill uses active body/receipt depth, not only buffered inventory.
  - Why: ready/completed/prepared backlog can look healthy while active network downloads have drained.
  - Tradeoff: the write loop may briefly block on a limited write-path refill when active downloads are below the floor, but it avoids multi-window floor stalls.

- Expected-sequence active fetch attempts remember their planned child header.
  - Why: ordered coalescing can advance the expected child while a same-sequence fetch planned from the old child is still active or queued.
  - Tradeoff: the scheduler may discard a small amount of in-flight work, but it avoids waiting for a fetch that cannot advance the floor.

- Rejected active-depth-only tuning as a production strategy.
  - Why: both a completed-buffer overflow gate and an eight-lane active target failed to improve the accepted warmed baseline without adding low/zero windows.
  - Alternative considered: keep the constants-only changes; rejected because the improvement was not meaningful and stability regressed.

- Rejected shorter expected-fetch hedge timing.
  - Why: duplicating the head-of-line fetch earlier increased zero-progress windows under high serving-peer counts.
  - Alternative considered: keep the 2 second hedge; rejected in favor of the previous 4 second head-of-line delay.

- Historical body/receipt fetch plans now carry scheduling priority.
  - Why: lookahead fetches were able to saturate body request slots while the expected sequence was the only work that could advance the verified floor.
  - Tradeoff: lookahead work may complete more slowly, but expected-sequence latency and average floor movement improve in warmed samples.

- Historical fetch refill now runs during local prepare waits as well as writes.
  - Why: a remote sample showed prepared work queued while active body/receipt downloads fell to zero.
  - Tradeoff: prepare waits may spend a small amount of time on bounded refill work, but the downloader is less likely to go idle between ordered writes.

- Full-priority historical body/receipt plans now hedge the first prefix chunks immediately when enough peers are available.
  - Why: remote logs showed long plan tails caused by early prefix chunks lagging while later chunks completed.
  - Tradeoff: this spends extra bandwidth on the chunks that gate ordered verification, so it is limited to full-priority expected work and does not apply to lookahead plans.

- The historical fetch sequence cursor is monotonic with the expected cursor.
  - Why: ordered writes and materialized lookahead can advance the expected sequence by multiple batches; future refills must not reuse stale sequence ids below that cursor.
  - Tradeoff: none intended; old sequence ids are already obsolete once the ordered cursor advances.

- Body/receipt candidate truncation preserves one probe for any missing execution-client family.
  - Why: sorting by proven request performance can make the top candidate pool Geth-heavy and prevent connected Nethermind peers from ever becoming serving peers.
  - Tradeoff: the pool may temporarily exceed the fast-pool size by a small number of client-family probes, but the active pipeline and per-peer request limits still bound memory and network work.

- Body/receipt prefix salvage runs only when no acceptable contiguous prefix exists.
  - Why: the completion path can safely accept a verified contiguous prefix; spending up to the salvage timeout after that point creates head-of-line latency without increasing validity.
  - Tradeoff: the scheduler may return smaller batches instead of trying to repair more of the prefix immediately, but the next ordered fetch covers the remaining range and avoids long idle windows.

- Do not start the global live chunk scheduler unless long-run evidence justifies the architecture risk.
  - Why: the restored accepted baseline produced a warmed `348.3` blocks/sec sample and a 30 minute `361.1` blocks/sec sample with only `2` low windows and no zero windows while the physical link was repeatedly near the 300 Mbps ceiling.
  - Alternative considered: immediately rewrite plan-level body/receipt scheduling into a global chunk scheduler.
  - Tradeoff: delaying the rewrite avoids destabilizing a strong baseline, but the full-run benchmark must still prove the scheduler remains stable outside short warmed samples.

- Fresh baseline resets can discard incomplete active data without touching existing full-sync backups.
  - Why: after a partially synced experiment, replacing a known-good full backup with incomplete data would make recovery harder.
  - Alternative considered: always move the active data dir into a full-sync backup slot.
  - Tradeoff: `discard-incomplete` is destructive for the active run, so it requires the explicit confirmation flag and should only be used when a separate full backup already exists.

- Background compaction is deferred until historical sync reaches genesis.
  - Why: dense historical ingest already writes compacted segments synchronously, while background compaction planning can scan thousands of segment manifests and starve the ordered historical writer behind the storage lock.
  - Alternative considered: keep background compaction active during historical sync and tune scan frequency; rejected because the observed stall held the verified floor for about two hours.
  - Tradeoff: any opportunistic background maintenance waits until historical sync completes, but the critical sync path remains live and compressed.

- End-to-end wall-clock sync time is not a blocker for this PR when the run is network-bound.
  - Why: post-fix samples show the client can drive the physical link near the available 300 Mbps download limit, so another fresh run would mainly remeasure infrastructure capacity.
  - Alternative considered: reset and rerun from scratch to obtain an uncontaminated wall-clock number.
  - Tradeoff: the contaminated run is not a clean benchmark, but it remains sufficient to validate liveness if it reaches genesis without repeated post-fix stalls.

## Challenges and Resolutions

- Challenge: direct Mac mini access failed outside the home network.
  - Resolution: reran checks and throughput samples through `pi-remote`.
  - Remaining: use the jump host unless direct LAN access is confirmed.

- Challenge: the scheduler entered a state with active fetches but no active expected fetch, no prepare-ready work, and no floor movement.
  - Resolution: added stale-work cleanup and missing-expected refill.
  - Remaining: single-interval zero windows still occur, so the broader live scheduler is not complete.

- Challenge: active body/receipt downloads ran low while waiting for slow reverse-header planning.
  - Resolution: added a separate ready-plan buffer so cheap queued plans can hide header latency.
  - Remaining: ordered prepare/write still causes shorter burstiness.

- Challenge: expected-sequence holes were detected only after several prepared lookahead batches had accumulated.
  - Resolution: moved missing-expected refill ahead of lookahead prepare work and added a proactive refill after ordered writes advance the cursor.
  - Remaining: dense ranges can still drain active downloads while many prepared batches wait to be written.

- Challenge: prepared backlog hid active body/receipt download starvation.
  - Resolution: post-write refill now considers active body/receipt fetch count and forces a limited write-path refill when active downloads fall below the floor.
  - Remaining: any next architectural pass should be a global live chunk scheduler or a full-run benchmark proving the current scheduler is bounded by network/runtime conditions.

- Challenge: active-depth experiments looked promising in spot metrics but failed warmed samples.
  - Resolution: reverted both rejected experiments locally and remotely, restored the accepted baseline, and left the Mac mini client running on the baseline build.
  - Remaining: compare future changes only against the accepted baseline and keep only changes that improve longer samples.

- Challenge: same-sequence fetches sometimes remained active after the expected child header changed.
  - Resolution: active attempts now store their planned child header and the cursor-advance path discards mismatched expected-sequence queued, completed, and active work.
  - Remaining: peer-tail body/receipt responses can still block the ordered floor even when active depth is healthy.

- Challenge: lookahead fetches competed with expected fetches for the same body request slots.
  - Resolution: added priority-aware body/receipt plan budgeting so expected work keeps full live prefix capacity and lookahead work is capped.
  - Remaining: ordered floor movement still has single-window stalls, so a true global live request scheduler remains open.

- Challenge: prepared batches could accumulate while active historical body/receipt downloads dropped to zero.
  - Resolution: local-work refill now runs during prepare waits and writes; warmed confirmation sample improved to `238.4` blocks/sec with `2` zero windows.
  - Remaining: active fetches can still stall behind slow peer tails, so the next scheduler work should target global live chunk scheduling or active expected-lane repair.

- Challenge: shrinking dense plan yield size reduced return blocks but did not remove body/receipt long tails.
  - Resolution: reverted the dense-prefix experiment and kept the accepted baseline.
  - Remaining: use sample data, not constants-only changes, to justify any future yield-size tuning.

- Challenge: expected prefix chunks could lag behind later completed chunks.
  - Resolution: added bounded immediate redundancy for the first full-priority prefix chunks; remote samples improved while keeping failures controlled.
  - Remaining: longer full-run validation is still needed before concluding the PR.

- Challenge: after restart, the scheduler briefly queued new work below the already-advanced expected sequence and recovered only after timeout refill.
  - Resolution: kept `historical_fetch_next_sequence` aligned with `historical_fetch_expected_sequence` on every expected-cursor advance.
  - Remaining: head-of-line refills still happen under slow peer tails, but they no longer strand the queue with `next < expected`.

- Challenge: many Nethermind peers were connected but none were serving body/receipt work in a warmed run.
  - Resolution: preserved one missing client-family probe after candidate sorting and truncation.
  - Remaining: peer diversity improved, but a prepared-backlog zero window still occurred, so the next improvement must target ordered scheduler flow rather than discovery.

- Challenge: a 256-block live progress target reduced per-plan target size but lowered overall floor movement.
  - Resolution: reverted locally and remotely after the benchmark regressed to `173.1` blocks/sec.
  - Remaining: use targeted tail-latency fixes rather than lowering the whole progress target.

- Challenge: body/receipt salvage could run even after the live plan had enough contiguous verified progress to complete.
  - Resolution: added an accepted-prefix gate before salvage and kept the candidate after a `354.8` blocks/sec, zero-window benchmark.
  - Remaining: some low windows remain when prepared backlog grows, so longer-run validation is still required.

- Challenge: two post-salvage scheduler candidates regressed zero-window behavior.
  - Resolution: reverted the 2 second full-priority partial-prefix flush and wider dense active-pipeline experiments locally and remotely.
  - Remaining: the next serious scheduler change should be a measured architectural change to global live chunk scheduling, not another constants-only tuning pass.

- Challenge: carrying the original planned prefix and residual chunks forward smoothed cold progress but reduced warmed throughput.
  - Resolution: reverted the residual carry-forward experiment locally and remotely after the warmed sample regressed below the accepted baseline.
  - Remaining: preserving lookahead validity needs a true global chunk scheduler, not synchronous residual-gap filling after each partial prefix.

- Challenge: draining/refilling the historical pipeline while async residual body/receipt work ran still reduced warmed throughput.
  - Resolution: rejected and reverted the async residual experiment after a `123.2` blocks/sec sample with `11` low windows.
  - Remaining: the next production-grade scheduler step should be a global chunk-level scheduler or a full-run proof that the current accepted scheduler is the practical baseline.

- Challenge: the rejected async residual sample made the current branch look worse than it was.
  - Resolution: restored the accepted build and reran a warmed baseline through `pi-remote`; it recovered to `348.3` blocks/sec with `1` low window and `0` zero windows.
  - Remaining: use a fresh full-run baseline, not another speculative experiment, before taking on a risky global scheduler rewrite.

- Challenge: the active run is useful for stability but is not a fresh pivot-to-genesis baseline.
  - Resolution: collected a 30 minute stability sample and identified the existing guarded fresh-baseline script.
  - Remaining: resolved; the post-fix baseline reached genesis and live head tracking continued afterward.

- Challenge: the default fresh-baseline path refuses to reset when the active data dir is not fully synced.
  - Resolution: used the guarded `discard-incomplete` mode so the known full-sync backup was preserved and only the incomplete active data was deleted.
  - Remaining: resolved; the run reached genesis and another clean wall-clock benchmark is not required while network capacity is the practical bottleneck.

- Challenge: the fresh run appeared alive but made no historical progress for nearly two hours.
  - Resolution: sampled the running process and matched the stall to background compaction/profile-rewrite planning scanning storage metadata while the ordered historical writer waited; background compaction is now skipped while historical sync is incomplete.
  - Remaining: resolved; the post-fix run reached genesis without a repeated liveness stall.

- Challenge: the baseline monitor was still framed around a clean wall-clock benchmark after the stall fix.
  - Resolution: updated the heartbeat instructions to monitor post-fix liveness and health until genesis, then remove itself and clear or revise the todo if no apparent problems remain.
  - Remaining: resolved; genesis was reached and the automation is being removed.

## Dead Code and Obsolescence Cleanup

- Reverted rejected chunk-size and partial-flush timing experiments before this pass.
- Current branch contains only accepted scheduler changes: stale-work critical refill, ready-plan buffering, expected-fetch priority, non-blocking post-write refill, proactive expected refill, active-download-aware post-write refill, expected-child mismatch cleanup, and adaptive write-path active refill.
- Reverted rejected completed-buffer overflow, eight-lane active-target, and shorter expected-hedge experiments before committing.
- Reverted rejected lookahead-promotion and partial-prefix salvage skip experiments locally and remotely.
- Reverted rejected wider full-priority candidate-pool experiment locally and remotely after it worsened low/zero windows.
- Inspected the priority-budget diff for stale experiment leftovers; no obsolete code remained beyond rejected experiment reverts.
- Reverted the rejected dense-prefix yield-size experiment locally and remotely before accepting the prefix-redundancy change.
- Inspected the new redundancy path for rejected experiment leftovers; no stale dense-prefix code remains.
- Fixed clippy-only issues in the scheduler and body/receipt tests; no functional dead code was removed in this pass.
- Inspected the peer-family probe change for experimental leftovers; it is limited to candidate truncation and focused tests.
- Reverted the rejected 256-block progress-target experiment locally and remotely before keeping the salvage-gate change.
- Inspected the salvage-gate diff for obsolete experiment leftovers; no rejected progress-target code remains.
- Reverted the rejected 2 second full-priority partial-prefix flush and wider dense active-pipeline experiments locally and remotely; no code from either experiment remains.
- Reverted the rejected residual carry-forward experiment locally and remotely; the obsolete contiguous-progress helper removal was also reverted with the experiment.
- Reverted the rejected async residual body/receipt carry-forward experiment locally and remotely; no code from that candidate remains.
- Inspected the accepted scheduler after the warmed baseline; no new dead code was introduced because the async residual candidate was fully reverted.
- Inspected `local-ops/start-fresh-baseline-run.sh`; it remains the guarded path for the required fresh baseline and was not run because it deletes/moves the remote data directory.
- Rechecked the guarded fresh-baseline reset path before running it; no obsolete production code was removed in this pass.
- Inspected background storage maintenance after the stall and removed its ability to run compaction planning concurrently with incomplete historical sync.
- Rechecked the roadmap after baseline completion and removed obsolete fresh-run TODO criteria.
- No production code was identified as safe to remove beyond stale experiment cleanup.

## Git Workflow

- Current branch: `perf/fresh-historical-baseline`.
- New branch created this run: no, continuing the active performance branch.
- Commits made during this run: `docs: record rejected scheduler experiments`; `perf: prioritize expected historical fetches`; `perf: refill historical fetches during prepare waits`; `perf: hedge critical historical prefix chunks`; `perf: keep historical fetch cursor monotonic`; `perf: preserve client-family probes in body receipt pool`; `perf: skip salvage for accepted body receipt prefixes`; `docs: record rejected live scheduler experiments`; `docs: record rejected residual carry-forward experiment`; `docs: record rejected async residual experiment`; `docs: record warmed baseline benchmark`; `docs: record long baseline sample`; `docs: record fresh baseline reset`; `fix: defer compaction during historical sync`; `docs: update baseline monitor criteria`; `docs: record completed historical baseline`; `docs: note historical sync PR CI blocker`.
- Pull request status: PR #97 is open and ready for final CI once GitHub Actions quota is available.
- Merge status: not merged.
- Blockers: GitHub Actions quota is unavailable; PR checks fail immediately with no runner steps or logs. Local CI-equivalent checks pass.

## Known Issues or Risks

- The completed run is not a clean wall-clock benchmark because it includes the known pre-fix two-hour stall and restart, but post-fix liveness is validated.
- PR #97 cannot be merged until GitHub Actions quota is restored and the hosted checks can run.
- Peer count and routing mode affect comparability; record both for any future benchmark.
- A global live chunk scheduler would be a material architecture change; do not start it unless a future post-fix run shows repeated low/zero-progress windows that cannot be explained by network, disk, or density changes.
