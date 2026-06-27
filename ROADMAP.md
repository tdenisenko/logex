# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed verified logs, and serves dashboard, SQL query, JSON-RPC, gRPC, and live ERC20 transfer subscription APIs.

Active branch: `perf/historical-sync-live-scheduler` for PR #96. Remote Mac mini checks must use `ssh -J pi-remote gremlinmaster@192.168.50.44` when outside the home network. The remote client is running from `/Volumes/SSD 4TB/LogEx` on HTTP port `18683`.

The latest Pi-routed validation reached genesis from historical floor `4,889,536` in `2,765.7s` (`46m05.7s`) on 2026-06-27 UTC, processing `4,856,264` historical blocks and `61,503,899` logs during that resumed run. Post-genesis live smoke passed: over 65 seconds the EL head advanced from `25,410,603` to `25,410,608`, historical floor stayed at `0`, and EL peers stayed at `77` connected / `28` serving.

## Completed Since Last Run

- Reran remote status, log, disk, and live-head validation through the Raspberry Pi jump host after direct Mac mini access failed.
- Parsed the remote run log from `/Users/gremlinmaster/logex-src/run/logex-pr96-baseline-restored-20260627-165855.log`.
- Confirmed the run had no severe storage, panic, fatal, low-disk, or corruption markers. Three receipt-root mismatch warnings were invalid peer responses that were rejected.
- Confirmed post-genesis forward sync still advances while historical sync remains complete.
- Fixed `storage_used_bytes` undercounting sealed segment indexes by invalidating cached segment sizes when direct `indexes/` files change.
- Added a regression test for index files added after a sealed segment size was cached.
- Validated with `cargo test -p logex-node -p logex-sync -p logex-server` and `cargo clippy -p logex-node -p logex-sync -p logex-server -- -D warnings`.

## Remaining TODOs

1. Finish PR #96 validation and merge.
   - Reason: the branch now has a completed resumed-to-genesis run, live-head smoke, storage metrics fix, and passing local validation, but CI and final PR state still need to be checked.
   - Completion criteria: branch is pushed, GitHub checks pass, PR is marked ready if needed, and the PR is merged.

2. Establish the next performance baseline from a fresh dense-range run.
   - Reason: the latest completed run covered the remaining sparse historical range, not a fresh pivot-to-genesis run through the log-dense ranges.
   - Completion criteria: start a new branch after PR #96, run with a resource sampler, record wall-clock sync time, logs/sec, blocks/sec, peer counts, bandwidth, CPU, memory, disk, low/zero-progress windows, and compare against the 4 hour full-sync goal.

3. Continue historical sync optimization only from measured bottlenecks.
   - Reason: broad scheduler tweaks repeatedly regressed throughput or zero-progress windows; further changes should target proven bottlenecks.
   - Completion criteria: keep only changes that improve longer remote samples without increasing low/zero-progress windows or weakening validation.

## Design Decisions

- Dense historical body/receipt sync uses the chunk-owned live scheduler.
  - Why: it tracks per-chunk role ownership and repairs prefix-critical gaps while preserving ordered verified floor advancement.
  - Tradeoff: scheduler complexity is higher, so changes are accepted only with remote measurement.

- Expected-sequence duplicate retries may bypass ordinary request-pressure refill after head-of-line delay.
  - Why: the expected sequence is the only fetch that can advance the verified floor, and lookahead saturation caused zero-progress windows.
  - Tradeoff: retries can briefly exceed the conservative refill pressure, but remain bounded by the per-sequence attempt cap.

- Storage metrics keep the sealed-segment size cache but include direct index-file signatures.
  - Why: this preserves cheap recurring dashboard refreshes while preventing stale cached sizes from excluding indexes built after the first scan.
  - Alternative considered: disabling the segment cache, which would make `/status` perform a large recursive scan too often.

## Challenges and Resolutions

- Challenge: direct Mac mini access failed from the current network.
  - Resolution: reran operational checks and log collection through `pi-remote`.
  - Remaining: none for this run.

- Challenge: `storage_used_bytes` reported about `359G` while filesystem usage under `segments/` was about `729G`.
  - Resolution: identified stale sealed-segment size cache entries that missed later index files and added cache invalidation coverage.
  - Remaining: deploy the branch and confirm the dashboard reports the full data-dir footprint after refresh.

- Challenge: bad peers returned receipt data with mismatched roots during the completed run.
  - Resolution: validation rejected those responses and continued without severe errors.
  - Remaining: none observed in this run.

## Dead Code and Obsolescence Cleanup

- Rechecked storage metrics caching and remote data-dir layout; no obsolete production files were removed during this pass.
- Earlier rejected scheduler experiments remain reverted; no rejected experiment code is currently staged.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`.
- New branch created this run: no.
- Commits made during this run: pending.
- Pull request status: PR #96 remains the active performance PR.
- Merge status: pending final validation, push, CI check, and PR readiness.
- Blockers: none known.

## Known Issues or Risks

- The completed run proves resumed sparse-range completion, not a fresh dense-range full sync; the next branch still needs a clean resource-sampled benchmark against the 4 hour goal.
- Full indexed storage footprint is larger than the older compressed-log-only estimate. After the metrics fix is deployed, the dashboard should expose the full footprint so index/storage tradeoffs can be evaluated explicitly.
