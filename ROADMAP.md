# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed verified logs, and serves them through the dashboard, query APIs, and live transfer notifications.

Active branch: `perf/historical-sync-live-scheduler` for PR #96. The branch now uses the chunk-owned live body/receipt scheduler for dense historical EL sync and keeps the Mac mini client running from `/Volumes/SSD 4TB/LogEx` on HTTP port `18683`. When outside the home network, checks route through `ssh pi-remote` to `gremlinmaster@192.168.50.44`.

Latest accepted candidate: expected historical fetch retries bypass the ordinary request-pressure refill gate after the head-of-line delay, while still keeping the existing two-attempt cap per sequence. Remote Pi-routed validation improved the observed stall mode from baseline `384.8` blocks/sec with `2` low windows and `1` zero window to `599.3` blocks/sec with `0` low windows and `0` zero windows over a 291 second sample. A later Pi-routed rerun after restoring the accepted baseline measured `596.6` blocks/sec with `0` low windows and `0` zero windows over 295 seconds.

## Completed Since Last Run

- Re-ran remote validation through the Raspberry Pi jump host after direct Mac mini access failed.
- Confirmed the Mac mini is reachable through `ssh -J pi-remote gremlinmaster@192.168.50.44` and reran the throughput sampler with `SSH_JUMP_HOST=pi-remote`.
- Rejected and reverted two scheduler candidates that did not improve the real sample:
  - Parallel late prefix salvage.
  - Primary residual suffix preservation.
- Kept the expected-fetch retry admission change because it removed zero-progress windows in the longer remote sample.
- Removed the now-unused `SyncEngine` request-pressure wrapper made obsolete by the new retry decision.
- Validated locally with `cargo test -p logex-sync` and `cargo clippy -p logex-sync -- -D warnings`.
- Deployed the accepted candidate to the Mac mini, rebuilt `logex-node --release`, restarted under tmux, and left the client running.
- Tested and rejected a 2 second historical body/receipt role timeout floor: remote validation fell to `460.6` blocks/sec with `2` low windows and `2` zero windows over 294 seconds.
- Tested and rejected lowering the serving-peer candidate-pool threshold from `16` to `8`: remote validation fell to `391.3` blocks/sec with `6` low windows and `3` zero windows over 289 seconds.
- Tested and rejected a short `750ms` missing-expected-fetch retry delay before head-of-line reset: remote validation fell to `424.6` blocks/sec with `3` low windows and `1` zero window over 290 seconds.
- Restored the accepted timeout behavior on the Mac mini, rebuilt `logex-node --release`, restarted under tmux, and confirmed `/status` responds through the Pi jump host.
- Restored the accepted baseline after the rejected retry-delay candidate and reran the sampler through `pi-remote`: `596.6` blocks/sec, `0` low windows, and `0` zero windows over 295 seconds.
- Tested and rejected an adaptive sparse-prefix progress target: it raised contiguous plan progress from about `540` to `917` blocks, but increased plan/write latency and sampled at `584.7` blocks/sec with no low/zero windows, which was not a meaningful improvement over baseline.
- Restored the accepted baseline on the Mac mini after the rejected sparse-prefix candidate and left the client running under tmux.
- Tested and rejected prefix-wide stale role repair: focused tests and clippy passed, but the remote sample immediately regressed into repeated low/zero-progress windows, so the change was reverted.
- Restored the accepted baseline on the Mac mini again after the rejected prefix-wide repair candidate.
- Reran validation through `ssh -J pi-remote` after the direct Mac mini network-unreachable error; the accepted baseline sample measured `429.1` blocks/sec with `2` low windows and `0` zero windows while serving peers were still limited.
- Tested and rejected a per-plan peer isolation candidate that capped per-peer role in-flight selection and treated transport failures as bad for both live body/receipt roles: focused tests passed, but the remote sample regressed to `459.6` blocks/sec with `3` low windows and `1` zero window, so the code was reverted.
- Restored the accepted baseline on the Mac mini and left it running under tmux from `/Volumes/SSD 4TB/LogEx`.
- After peer warm-up recovered through the Pi route, reran the accepted baseline and measured `647.7` blocks/sec over 295 seconds with `1` low window and `1` zero window while peers climbed to `64` connected and `25` serving.

## Remaining TODOs

1. Validate the live scheduler over a full historical run.
   - Reason: short dense samples now look healthy, but full-sync performance varies by peer mix, routing, and log density.
   - Completion criteria: record start-to-genesis time, p50/p90/max logs/sec, actual floor blocks/sec, low/zero-progress windows, bandwidth, CPU, memory, disk, peer counts, resets, and failures.

2. Continue scheduler admission work only where measurements show idle resources.
   - Reason: the latest fix addresses head-of-line duplicate admission, but plan p90 remains high in some windows.
   - Completion criteria: add or keep only changes that improve longer remote samples against the new baseline without increasing low/zero-progress windows.

3. Investigate peer-tail mitigation without reducing the global request timeout floor.
   - Reason: a shorter 2 second timeout, a lower serving-pool threshold, a short missing-expected retry delay, a larger sparse-prefix progress target, and prefix-wide stale repair did not improve sustained remote throughput, so the remaining tail-latency fix likely needs more precise prefix peer selection, per-role demotion, scheduler admission, or better production metrics rather than more broad timing constants.
   - Completion criteria: identify a targeted change that improves p90 plan/body-receipt latency and remote throughput without reducing serving peer stability or adding zero-progress windows.

4. Complete EL production hardening.
   - Reason: scheduler changes must not weaken checkpoint freshness, forward sync, reorg handling, restart safety, low-disk behavior, query correctness, or dashboard access.
   - Completion criteria: tests or smokes cover fresh-checkpoint enforcement, stale restart rejection, CL tracking, EL forward sync, EL reverse sync, invalid peer data, reorg handling, low disk behavior, authenticated dashboard access, and a clean full-sync candidate run.

## Design Decisions

- Dense historical body/receipt sync uses the live chunk-owned scheduler.
  - Why: it tracks per-chunk role ownership and repairs prefix-critical gaps without relying on separate body and receipt loops finishing together.
  - Alternative considered: keep the old decoupled dense path. It was removed because it was dormant and harder to reason about.

- Expected-sequence duplicate retry bypasses ordinary request-pressure refill after head-of-line delay.
  - Why: the expected sequence is the only fetch that can advance the verified floor; letting lookahead saturate request slots can create zero-progress windows.
  - Tradeoff: this can briefly exceed the conservative refill pressure, but retries remain bounded by the existing per-sequence attempt cap.

- Historical reverse sync remains ordered at storage advancement.
  - Why: downloads, extraction, and writes may overlap, but verified floor movement must preserve parent-chain and receipt-root validation semantics.

## Challenges and Resolutions

- Challenge: progress plunged to zero while peers and network RX were still active.
  - Resolution: identified the expected-fetch head-of-line pressure gate and allowed bounded expected retries to bypass it.
  - Remaining: full-run validation is still required.

- Challenge: some experiments looked plausible but did not improve the real run.
  - Resolution: rejected and reverted them, then restored the remote client before continuing.

- Challenge: lowering the historical body/receipt role timeout floor looked plausible because plan p90 was tail-latency bound.
  - Resolution: tested it on the Mac mini and rejected it after the sample regressed to `460.6` blocks/sec with two zero-progress windows.
  - Remaining: pursue more targeted prefix peer selection or per-role demotion instead of blanket timeout reduction.

- Challenge: lowering the serving-peer candidate-pool threshold looked plausible because real samples often had fewer than 16 serving peers.
  - Resolution: tested a threshold of `8` and rejected it after the sample regressed to `391.3` blocks/sec with three zero-progress windows.
  - Remaining: avoid broad serving-pool filtering changes; focus on direct evidence from prefix-role tail events.

- Challenge: retrying missing expected fetches before the full head-of-line reset looked like it could reduce idle time.
  - Resolution: tested a `750ms` retry delay and rejected it after the sample regressed to `424.6` blocks/sec with one zero-progress window.
  - Remaining: use stronger prefix-tail evidence before making another scheduler change.

- Challenge: increasing sparse plan progress targets looked like it could reduce request-plan boundary overhead.
  - Resolution: tested a peer-aware larger prefix target; it increased contiguous blocks per plan but did not improve sustained throughput, so it was reverted.
  - Remaining: avoid larger-prefix tuning without a full-run or side-by-side result that clearly beats the accepted baseline.

- Challenge: proactively repairing multiple stale prefix chunks looked like it could prevent the next prefix chunk from becoming the next tail.
  - Resolution: implemented and tested prefix-wide stale role repair, then rejected it after the remote sample produced repeated low/zero-progress windows within the first minute.
  - Remaining: avoid increasing in-plan hedge fanout without stronger per-peer demotion or measured slot isolation.

- Challenge: per-plan peer isolation looked like it could prevent one failing peer from consuming multiple live chunk roles before global failure accounting caught up.
  - Resolution: tested per-peer role caps and cross-role transport-failure isolation, then rejected it after the remote sample produced more low/zero windows than the accepted baseline.
  - Remaining: peer-tail mitigation still needs better evidence from role-level metrics before changing admission or demotion behavior.

- Challenge: direct Mac mini SSH was unavailable from the current network.
  - Resolution: reran all operational checks and the throughput sample through `pi-remote`.

- Challenge: immediate post-restart samples looked much slower than the accepted baseline.
  - Resolution: waited for peer warm-up and reran the sampler; throughput recovered to `647.7` blocks/sec, confirming the earlier weak sample was mostly peer warm-up/mix rather than a code regression.

## Dead Code and Obsolescence Cleanup

- Removed the obsolete `SyncEngine::historical_body_receipt_request_pressure_allows_refill` wrapper.
- Rechecked scheduler candidates and reverted unproductive code before committing.
- Removed the untracked `.DS_Store` workspace noise.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`.
- New branch created this run: no.
- Commits made during this run: `dd63451 fix: prioritize stalled historical fetch retries`, `49c7274 docs: record pi-routed scheduler validation`, `c0efda0 docs: record rejected timeout candidate`, `7c30fe0 docs: record rejected serving-pool candidate`, `17743d4 docs: record restored scheduler baseline`, `ab384e9 docs: record rejected sparse prefix candidate`, `0b64380 docs: record rejected prefix repair candidate`, and `33e4b19 docs: record rejected peer isolation candidate` were committed and pushed. A follow-up docs commit for the warmed baseline sample is pending.
- Pull request status: PR #96 remains the active draft performance PR.
- Merge status: not ready until longer validation/CI are reviewed.
- Blockers: none.

## Known Issues or Risks

- The accepted sample is materially better but still short compared with a full sync.
- Peer count and routing mode materially affect results; benchmark notes must include serving peers and network mode.
- Further scheduler work should be measured against this new baseline, not against older rejected experiments.
