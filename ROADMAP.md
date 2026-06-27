# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed verified logs, and serves dashboard, SQL query, JSON-RPC, gRPC, and live ERC20 transfer subscription APIs.

Active branch: `perf/fresh-historical-baseline`. Remote Mac mini checks must use `ssh -J pi-remote gremlinmaster@192.168.50.44` when outside the home network. A fresh baseline run is active from clean `origin/master` source at `/Users/gremlinmaster/logex-baseline-src`, using data dir `/Volumes/SSD 4TB/LogEx`, HTTP port `18683`, and monitor output under `/Users/gremlinmaster/logex-baseline-runs/fresh-baseline-20260627-182542`.

The latest Pi-routed validation reached genesis from historical floor `4,889,536` in `2,765.7s` (`46m05.7s`) on 2026-06-27 UTC, processing `4,856,264` historical blocks and `61,503,899` logs during that resumed run. Post-genesis live smoke passed: over 65 seconds the EL head advanced from `25,410,603` to `25,410,608`, historical floor stayed at `0`, and EL peers stayed at `77` connected / `28` serving.

## Completed Since Last Run

- Merged PR #96 into `master` with all GitHub checks passing.
- Created `perf/fresh-historical-baseline` for the next measured full-run baseline.
- Confirmed the Mac mini is reachable through `pi-remote`, the current client is full-synced, and the remote source checkout is dirty on an older performance branch, so the next run must use a separate clean source worktree.
- Audited remote disk use: current `/Volumes/SSD 4TB/LogEx` is about `732G`, old backup `/Volumes/SSD 4TB/LogEx-full-sync-20260626-170349` is about `805G`, and the volume has about `268G` free before cleanup.
- Started a fresh dense-range baseline from clean `origin/master`, preserved the previous full sync at `/Volumes/SSD 4TB/LogEx-full-sync-20260627-182542`, removed the older backup, and confirmed public VPS dashboard access.
- Early fresh-run sampling reached `62` connected / `49` serving peers, with current status around `956k logs/sec`; monitor samples show p50 `~362k logs/sec`, p95 `~681k`, max `~926k`, and physical RX commonly near the `300 Mbps` link limit.
- Investigated zero floor-advance sample windows; they occurred while physical RX remained high and were followed by larger ordered floor advances, so they are not currently evidence of an idle scheduler stall.
- Reran remote status, log, disk, and live-head validation through the Raspberry Pi jump host after direct Mac mini access failed.
- Parsed the remote run log from `/Users/gremlinmaster/logex-src/run/logex-pr96-baseline-restored-20260627-165855.log`.
- Confirmed the run had no severe storage, panic, fatal, low-disk, or corruption markers. Three receipt-root mismatch warnings were invalid peer responses that were rejected.
- Confirmed post-genesis forward sync still advances while historical sync remains complete.
- Fixed `storage_used_bytes` undercounting sealed segment indexes by invalidating cached segment sizes when direct `indexes/` files change.
- Added a regression test for index files added after a sealed segment size was cached.
- Validated with `cargo test -p logex-node -p logex-sync -p logex-server` and `cargo clippy -p logex-node -p logex-sync -p logex-server -- -D warnings`.

## Remaining TODOs

1. Establish the next performance baseline from a fresh dense-range run.
   - Reason: the latest completed run covered the remaining sparse historical range, not a fresh pivot-to-genesis run through the log-dense ranges.
   - Completion criteria: let the active fresh run reach genesis or fail with a diagnosed cause, then record wall-clock sync time, logs/sec, blocks/sec, peer counts, bandwidth, CPU, memory, disk, low/zero-progress windows, routing mode, resets/failures, and compare against the 4 hour full-sync goal.

2. Continue historical sync optimization only from measured bottlenecks.
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

- Challenge: the two-endpoint checkpoint command failed because `https://lodestar-mainnet.chainsafe.io` returned 404 for the selected checkpoint header while PublicNode succeeded.
  - Resolution: restarted the fresh baseline with `https://ethereum-beacon-api.publicnode.com` only.
  - Remaining: choose a stable multi-source checkpoint policy or service before treating that endpoint set as production default.

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

- Current branch: `perf/fresh-historical-baseline`.
- New branch created this run: `perf/fresh-historical-baseline`.
- Commits made during this run: pending.
- Pull request status: not created yet for this branch.
- Merge status: PR #96 merged; this branch is pending baseline work.
- Blockers: none known.

## Known Issues or Risks

- The completed run proves resumed sparse-range completion, not a fresh dense-range full sync; the next branch still needs a clean resource-sampled benchmark against the 4 hour goal.
- Full indexed storage footprint is larger than the older compressed-log-only estimate. After the metrics fix is deployed, the dashboard should expose the full footprint so index/storage tradeoffs can be evaluated explicitly.
