# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput-v3`.

The Mac mini WireGuard/VPS path is healthy again after replacing the stale-interface wrapper with a health-checking LaunchDaemon script. The wrapper now treats a tunnel as healthy only if the VPS tunnel IP responds or the WireGuard handshake is recent; otherwise it restarts the tunnel and restores the full-tunnel routes while preserving LAN access.

Historical sync performance work has revalidated the earlier high-throughput commits and isolated the main regression to the storage coalescing path introduced after `b7129fe`. The active candidate keeps sparse historical coalescing for disk efficiency, writes dense historical batches directly as compacted sealed segments, lowers the dense body/receipt decoupled-pipeline threshold so medium-sized serving pools avoid paired request head-of-line blocking, and caps parallel chunk requests per peer to reduce damage from slow peers. Mac mini benchmarks recovered 800k-900k+ logs/sec peaks and materially higher sustained throughput, while still showing periodic peer timeout clusters that remain the next bottleneck.

The active Mac mini client now runs from the main data directory `/Volumes/SSD 4TB/LogEx` on HTTP port `18683`. Old `/Volumes/SSD 4TB/LogEx*` experiment directories were removed, leaving only the main data directory.

Latest regression check: the apparent drop to roughly 20k logs/sec was caused by running from a stale data directory while the forward CL-anchored EL path was catching up to head. Once the forward path reached head, historical backfill returned to the prior high-throughput range on the same code/data path. The consensus sync loop now drains already-ready historical work and primes the historical fetch pipeline before forward anchor batches, so reverse body/receipt downloads can keep running while live catch-up proceeds.

Resumed historical backfill is no longer forced to wait for a fresh CL head before entering the consensus-mode sync loop. Fresh data directories still require a recent checkpoint and consensus-verified pivot; only data directories with a persisted verified historical floor can resume reverse EL sync while forward/CL tracking catches up.

Historical validation is independent from CL and forward sync after a checkpoint-backed pivot exists. Runtime scheduling is still cooperative inside one `SyncEngine`/`PeerManager`, so reverse fetches run in background tasks but planning/accounting/ingestion can still share turns with forward catch-up until the peer scheduler is split into an actor. Historical body/receipt plans now stream chunk-level request feedback back to the engine so slow-peer accounting can affect later plan construction before the whole fetch plan completes.

## Completed Since Last Run

- Diagnosed the WireGuard outage as a stale utun/routes state: the interface existed, but the UDP path/handshake was stale, so the old wrapper did not force a restart.
- Confirmed the repaired tunnel has working VPS egress, tunnel ping, DNAT/forwarding counters, and dashboard access through the VPS.
- Built a clean `master` binary on the Mac mini after fixing the SSH PATH for Homebrew `protoc`.
- Restarted LogEx gracefully in tmux without resetting the data directory and benchmarked `master` against the same data/tunnel path.
- Reverted the earlier v3 scheduler experiments that regressed throughput versus `master`.
- Added a narrower peer-load scheduler change for body/receipt attempts and benchmarked it on the Mac mini.
- Measured the peer-load experiment at roughly 212k average logs/sec versus roughly 195k for the refreshed baseline, with plan p95 improving from about 21.3s to about 13.6s and timeout failures per plan dropping from about 2.0 to about 0.7.
- Validated the restored baseline with `cargo fmt --all -- --check`, `cargo test -p logex-sync`, `cargo clippy -p logex-sync --all-targets -- -D warnings`, and `cargo check --workspace`.
- Benchmarked earlier commit states on the Mac mini:
  - `6e6cbd9`: recovered 830k logs/sec max with roughly 385k last-24-sample average, but with high timeout churn.
  - `8136e0a`: recovered 953k logs/sec max with roughly 382k last-24-sample average after warm-up.
  - `812b308`: best earlier balanced baseline, roughly 405k all-window average and 851k max.
  - `6268e82` and `b7129fe`: retained high peaks but had weaker p10/tail behavior than `812b308`.
  - `0e55883` and current master-line storage: dropped back near the 160k-235k range, identifying storage coalescing as the main regression.
- Added adaptive historical storage writes: dense historical batches now bypass raw staging and are written as compacted sealed segments immediately, while sparse batches still coalesce into an active historical segment.
- Added storage tests covering dense sub-target compacted writes and sparse historical coalescing.
- Rebuilt and deployed the adaptive storage branch on the Mac mini; the live benchmark recovered roughly 531k last-24-sample average and 992k max in the first parsed window, then continued showing 300k-890k dashboard samples as peers warmed up.
- Rejected an attempted high-peer dense lookahead increase because it did not activate during the test window and did not provide evidence of improvement.
- Lowered the dense body/receipt decoupled-pipeline threshold from 12 to 8 peers; the remote benchmark reduced paired fallback plan average latency from roughly 15.0s to roughly 9.6s, kept contiguous prefixes at 512/512, and reached roughly 493k last-120-sample average with 867k max while preserving the 6-plan dense fetch cap.
- Rejected a 4s pipelined body/receipt timeout because it increased timeout churn and damaged contiguous prefixes despite producing some higher dashboard samples.
- Capped parallel chunk requests per peer from 4 to 2; the extended remote benchmark reached roughly 521k last-12-sample average with 379k p10, kept 512-block contiguous prefixes, and reduced partial-batch churn to one partial across 835 batches.
- Investigated the stopped remote client. The main `/Volumes/SSD 4TB/LogEx` run had shut down cleanly from `SIGINT`; a benchmark run had stopped with `storage write error: Too many open files (os error 24)`.
- Added startup file-descriptor hardening so LogEx raises the process soft `RLIMIT_NOFILE` to 16,384 when the OS hard limit permits it.
- Cleaned remote experiment data under `/Volumes/SSD 4TB/LogEx*`, preserving only `/Volumes/SSD 4TB/LogEx`.
- Rebuilt and restarted the Mac mini client in tmux against `/Volumes/SSD 4TB/LogEx`; startup raised the descriptor limit from 256 to 16,384, storage integrity passed, and `/status` reported healthy EL/CL peer warm-up with historical sync moving.
- Validated locally with `cargo fmt --all -- --check`, `cargo check --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace`.
- Reverted the temporary post-regression body/receipt fetch experiments made during the stale-data-dir investigation; the local and Mac mini source trees are back to the branch state before those experiments.
- Confirmed the running Mac mini client was rebuilt and restarted from the reverted code, stayed near head, and recovered historical throughput during peer warm-up.
- Added bounded historical-ready draining in the consensus loop so completed historical fetch/prepare work can be ingested before the next forward anchor batch while still preventing forward catch-up starvation.
- Validated the scheduling fix with `cargo fmt --all -- --check`, `cargo check --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace`.
- Rebuilt and restarted the Mac mini client in tmux on `/Volumes/SSD 4TB/LogEx`; after peer warm-up it stayed near head and historical throughput recovered into the expected range.
- Removed the remaining live-lag historical backfill gate and now primes the historical fetch pipeline before forward anchor ingestion, allowing historical body/receipt downloads to overlap forward catch-up after the checkpoint/pivot exists.
- Revalidated with `cargo fmt --all -- --check`, `cargo check -p logex-sync`, `cargo test -p logex-sync`, `cargo check --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace`.
- Deployed the rebuilt binary to the Mac mini, restarted it in tmux on the main data directory, and confirmed `/status` stayed near head while historical reverse sync continued with active fetch lookahead.
- Confirmed one remaining startup coupling: consensus-mode sync could wait for CL anchors before entering the main loop, preventing a stored verified historical floor from resuming reverse EL backfill during CL/forward catch-up.
- Changed the startup gate so consensus mode can enter the main loop when either consensus anchors are ready or verified historical backfill can resume from the persisted floor with an eligible EL peer.
- Added unit coverage proving historical resume does not bypass fresh-checkpoint startup and does not run when historical sync is disabled.
- Rejected an uncommitted per-chunk in-flight peer rebalancing experiment after Mac mini benchmarks showed lower average logs/sec and higher timeout churn than the accepted branch state.
- Rejected an uncommitted role-specific peer weakness and low inherited-limit experiment after live Mac mini benchmarks showed lower average logs/sec, worse body/receipt p95 latency, and higher timeout churn than the accepted branch state.
- Confirmed historical validation is cryptographically independent from CL/forward sync after a checkpoint-backed pivot exists, but found one remaining scheduler coupling: after forward anchor ingestion, the consensus loop could call the blocking historical path and wait for the next reverse fetch.
- Replaced the post-forward blocking historical call with a nonblocking ready-work service that drains completed historical batches and primes reverse fetches without waiting on the next historical network response while forward anchors are available.
- Revalidated with `cargo fmt --all -- --check`, `cargo check -p logex-sync`, `cargo test -p logex-sync`, `cargo clippy -p logex-sync --all-targets -- -D warnings`, `cargo check --workspace`, and `cargo test --workspace`.
- Deployed the rebuilt binary to the Mac mini, restarted the existing `/Volumes/SSD 4TB/LogEx` run in tmux without resetting data, and confirmed `/status` stayed near head while historical backfill resumed after peer warm-up.
- Rejected the uncommitted per-fetch peer-candidate rotation experiment after live Mac mini sampling showed lower average logs/sec and higher timeout churn than the committed baseline.
- Restored, rebuilt, and restarted the committed baseline on the Mac mini; after peer warm-up, `/status` showed forward tracking near head and historical reverse sync active at roughly 198k logs/sec with 12 serving EL peers.
- Rechecked historical/forward/CL coupling: no CL live-head wait remains after a persisted verified historical floor exists, but a full runtime split still requires a peer-request scheduler actor because `SyncEngine` currently owns the mutable `PeerManager`.
- Added chunk-level historical body/receipt request accounting events so background fetch plans can report successes/timeouts to `PeerManager` before the final plan outcome is materialized.
- Added unit coverage for streamed request-accounting emission and kept final plan accounting one-shot to avoid double-counting.
- Validated the change with `cargo fmt --all -- --check`, `cargo test -p logex-sync`, `cargo clippy -p logex-sync --all-targets -- -D warnings`, and `cargo check --workspace`.
- Deployed the candidate to the Mac mini without resetting `/Volumes/SSD 4TB/LogEx`; the benchmark window showed a modest logs/sec distribution improvement and lower per-plan failure counts, but not enough to close the peer-tail TODO.

## Remaining TODOs

1. Reduce peer-tail sawtooth in dense historical sync.
   - Reason: The goal is to reduce full historical sync toward the 4-hour target without relying on short spikes.
   - Completion criteria: Use body/receipt plan latency, active fetch count, buffer depth, serving-peer mix, CPU, memory, disk, and network observations to reduce timeout-cluster low-throughput minutes without increasing failure churn or memory risk. Benchmark only after forward catch-up is at head, or report forward catch-up contention separately. The latest chunk-level accounting change improves feedback latency but does not complete this TODO; next work should compare a full peer-request scheduler actor or explicit cross-plan reservations against the accepted baseline.

2. Validate stale-forward catch-up fairness.
   - Reason: The consensus loop now drains ready historical work, primes historical downloads before forward batches, and can resume verified historical backfill before CL head tracking is ready. This still needs longer stale-resume runtime validation.
   - Completion criteria: Restart from a stale but valid data directory or reproduce the condition in a controlled test and confirm forward sync still reaches head while historical fetches stay active and historical batches keep making steady progress. If cooperative scheduling still materially suppresses historical throughput during forward catch-up, split peer request scheduling/accounting into an actor so forward and historical workers can submit work independently.

3. Match production-client P2P behavior more closely.
   - Reason: The target is Geth/Nethermind-class peer retention and sync performance.
   - Completion criteria: Audit relevant Geth/Nethermind peer scoring, request scheduling, timeout, and peer-retention behavior; implement compatible changes only when benchmarks show they beat the restored baseline.

4. Complete release hardening.
   - Reason: Production readiness depends on verification safety, restart safety, and predictable operations.
   - Completion criteria: Smokes or tests cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a clean full-sync release-candidate run.

## Design Decisions

- Use sustained logs/sec, p95 plan/body-receipt latency, and timeout rate as the main benchmark criteria; peak logs/sec alone is not enough to keep a change.
- Keep the `master` body/receipt scheduler as the performance baseline until a live benchmark proves a replacement is better, then compare that result with earlier high-throughput commit states.
- Preserve useful stabilization changes when they improve tail latency without sacrificing sustained throughput.
- Historical storage should be density-aware: dense batches are already large enough to amortize compacted segment overhead, while sparse ranges need raw staging/coalescing to avoid tiny segment and disk-usage growth.
- Dense body/receipt fetches should enter the decoupled body/receipt path with at least 8 eligible peers; remote benchmarks showed this reduces paired fallback latency without increasing the active dense fetch cap.
- Limit parallel chunk requests per peer to 2 so one slow peer cannot occupy many chunk slots before timeout accounting demotes it; the tradeoff is slightly lower theoretical burst capacity for a better sustained floor.
- Raise the process open-file soft limit at startup on Unix platforms. The client needs enough descriptors for P2P sockets plus storage segment reads/writes; relying on macOS's default soft limit of 256 is too fragile for long production runs.
- WireGuard health must be based on actual tunnel liveness, not just whether a utun interface exists.
- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.
- Historical throughput regressions must account for stale forward catch-up. The consensus-anchored loop now drains up to two already-ready historical batches and primes historical body/receipt fetches before forward anchor work, keeping historical downloads overlapped with live catch-up without starving the forward path.
- Fresh sync must still be anchored by a recent CL checkpoint. Resumed historical sync may proceed without a currently available CL head only when storage already contains a persisted verified historical floor header.
- When forward consensus anchors are available, the scheduler should only service ready historical work and keep reverse fetches primed; it should not block forward progress waiting for a historical network response. If no forward anchors are available, historical backfill may use the blocking path because there is no forward work to contend with.
- Full runtime independence between live forward sync and historical reverse sync requires moving EL request scheduling/accounting out of the single mutable `SyncEngine` loop. Until then, reverse downloads can run concurrently, but ingestion and peer-plan creation remain cooperative.
- Background historical body/receipt fetches should stream chunk-level request feedback to the engine. This keeps peer timeout/throughput scoring fresher for subsequent plans while preserving a single final outcome for validation/ingestion.

## Challenges and Resolutions

- Challenge: WireGuard suddenly stopped working without a recent config edit.
  - Resolution: Identified the stale-interface failure mode and installed a health-checking wrapper that restarts the tunnel when ping/handshake checks fail.

- Challenge: The v3 branch had accumulated many plausible scheduler changes but live throughput was worse than prior baselines.
  - Resolution: Built and ran clean `master` on the same Mac/data/tunnel path, confirmed it was materially better, and reverted the broad v3 scheduler changes.

- Challenge: The latest peer-load scheduler change improved request tail latency but not enough to meet the throughput target.
  - Resolution: Kept it as a stabilization candidate and moved the next benchmark step to explicit earlier-commit testing.

- Challenge: Current master-line storage was much slower than the high-throughput earlier commits.
  - Resolution: Benchmarked the relevant commit sequence and found the drop at the storage coalescing change. Implemented adaptive dense/sparse storage so dense batches use the fast compacted write path and sparse batches retain coalescing.

- Challenge: Adaptive storage recovered high peaks but still shows low-throughput minutes with many serving peers.
  - Resolution: Treat the remaining bottleneck as peer-tail/downloader scheduling rather than storage. Rejected high-peer dense lookahead as unproven and the 4s timeout as too damaging to contiguous prefixes, then kept the lower decoupled threshold and per-peer chunk cap because they improved sustained throughput and reduced partial churn.

- Challenge: Clean remote master build failed because `protoc` was not on the non-interactive SSH PATH.
  - Resolution: Confirmed Homebrew protobuf was already installed and rebuilt with `/usr/local/bin` on PATH.

- Challenge: A remote benchmark run stopped with `Too many open files` while the Mac mini shell soft limit was 256.
  - Resolution: Added Unix startup logic to raise the soft file-descriptor limit to 16,384 where permitted. The restarted client confirmed the new limit in logs and continued running from the main data directory.

- Challenge: A stale main data directory made historical throughput appear to regress to roughly 20k logs/sec.
  - Resolution: Reverted the temporary fetch-path changes made during that investigation and compared logs/status across the same run. The forward path was still catching up to head, and historical batches were interleaved behind forward work; after head catch-up, historical throughput returned to the prior high range. Added bounded ready-historical draining before forward anchor work to reduce this coupling.

- Challenge: Historical backfill was cryptographically independent from CL after pivot creation but still schedulable only through the consensus loop.
  - Resolution: Removed the remaining live-lag helper and added pre-forward historical pipeline priming so historical body/receipt fetch tasks are launched before forward catch-up work. A full peer-manager actor split remains the larger architectural option if future benchmarks show cooperative scheduling is still insufficient.

- Challenge: Consensus-mode startup still waited for CL anchor availability before historical backfill could resume from disk.
  - Resolution: Added a verified-floor resume gate so stored historical work can enter the main loop with eligible EL peers even if forward/CL tracking is still waiting. Fresh sync and disabled historical mode remain blocked from this path.

- Challenge: A per-chunk in-flight peer rebalancing experiment looked plausible from code inspection but worsened remote behavior.
  - Resolution: Reverted the uncommitted experiment and recorded it as rejected because average logs/sec fell and timeout churn increased on the Mac mini benchmark.

- Challenge: Role-specific peer weakness tracking and lower inherited request limits matched production-client ideas in principle, but the combined experiment reduced throughput on the live Mac mini run.
  - Resolution: Reverted the experiment before committing code. The result suggests that the current bottleneck is not solved by shrinking peer request sizes globally; future work should use narrower capacity estimation or per-peer request sizing that proves higher sustained throughput before it is kept.

- Challenge: Historical sync was logically independent from CL after pivot creation, but forward catch-up could still share the same cooperative loop in a way that made one side wait for the other's next network response.
  - Resolution: Kept the single-engine architecture for now, but changed the forward-anchors path to perform only nonblocking historical service after forward work. This preserves background reverse downloads and ready-batch ingestion without introducing a full peer-manager actor split yet.

- Challenge: Historical reverse sync was expected to be fully independent from forward/CL sync, but the current architecture still has one owner for mutable peer scheduling state.
  - Resolution: Confirmed no CL live-head gate remains after a verified floor exists, restored the accepted baseline, and left the full actor split as the next architectural step if stale-forward validation proves cooperative scheduling is still a bottleneck.

- Challenge: Concurrent historical fetch plans could keep using stale peer scores until a whole plan completed.
  - Resolution: Added streamed chunk-level body/receipt request accounting events. The first Mac mini benchmark showed modest improvement, but the remaining low-throughput minutes indicate peer-tail scheduling is not fully solved.

## Dead Code and Obsolescence Cleanup

- Inspected the historical scheduler changes in `crates/logex-sync/src/engine/anchored.rs`, `crates/logex-sync/src/engine/mod.rs`, and `crates/logex-sync/src/p2p/peer_manager/requests.rs`.
- Inspected the storage regression in `crates/logex-storage/src/native/storage.rs`, `crates/logex-storage/src/native/segment.rs`, and `crates/logex-storage/src/partition.rs`.
- Removed the obsolete broad v3 scheduler delta from the branch by restoring the proven master implementation before adding the narrower peer-load change.
- Reverted the unproven high-peer dense lookahead experiment before committing; only the decoupled threshold change remains from this pass.
- Reverted the failed 4s timeout experiment before committing; only the per-peer chunk cap remains from the latest pass.
- Removed remote experiment data directories matching `/Volumes/SSD 4TB/LogEx*` except the main `/Volumes/SSD 4TB/LogEx` directory.
- Reverted uncommitted body/receipt request-window and early-prefix experiments from `crates/logex-sync/src/p2p/peer_manager/requests.rs` after identifying stale forward catch-up as the actual cause of the low dashboard rate.
- Inspected `crates/logex-sync/src/engine/anchored.rs` for stale forward/historical scheduling coupling and kept the fix scoped to ready historical work; no abandoned helper paths were left behind.
- Removed the obsolete `should_run_historical_backfill` helper and its test because historical backfill is now controlled by the verified historical floor and peer availability rather than forward-sync lag.
- Inspected consensus-mode startup in `crates/logex-sync/src/engine/anchored.rs` and kept the resume fix scoped to persisted historical floor state; no fresh-sync bypass path was added.
- Reverted the uncommitted per-chunk in-flight scheduler experiment in `crates/logex-sync/src/p2p/peer_manager/requests.rs`; no experimental code remains from that attempt.
- Reverted the uncommitted role-weakness scheduler experiment in `crates/logex-sync/src/p2p/peer_manager/{mod.rs,lifecycle.rs,state.rs}` after remote benchmarking showed it regressed the accepted baseline.
- Inspected the consensus-mode scheduler in `crates/logex-sync/src/engine/anchored.rs` and consolidated duplicated ready-historical servicing into a single helper; no abandoned blocking post-forward path remains.
- Reverted the uncommitted per-fetch peer-candidate rotation experiment in `crates/logex-sync/src/engine/anchored.rs` and `crates/logex-sync/src/p2p/peer_manager/requests.rs`; no code from that rejected run remains.
- Inspected and updated `crates/logex-sync/src/engine/{mod.rs,anchored.rs}` and `crates/logex-sync/src/p2p/peer_manager/{mod.rs,requests.rs}` for streamed historical request accounting. No dead experimental scheduler code was left in place.
- Removed no unrelated production code; the storage change reuses the prior compacted segment writer and retains the existing sparse staging path.

## Git Workflow

- Current branch: `perf/historical-sync-throughput-v3`
- New branch created this run: no
- Commits made during this run: `fa7b806 perf: stabilize body receipt peer scheduling`; `2e29818 perf: adapt historical storage by log density`; `10d97ff perf: lower dense decoupled peer threshold`; `c10dcac perf: cap per-peer chunk concurrency`; `1f42521 fix: raise file descriptor limit at startup`; `9336a4b docs: record stale forward catch-up throughput diagnosis`; `f90d9ec perf: drain ready historical work before forward sync`; `2071657 perf: prime historical fetches before forward sync`; `perf: resume historical sync before consensus head`; `docs: update historical resume git workflow`; `docs: record rejected peer weakness experiment`; `perf: avoid post-forward historical waits`; `docs: clarify historical sync scheduling independence`; streamed historical request accounting change prepared this run.
- Pull request status: draft PR open at `https://github.com/tdenisenko/logex/pull/95`.
- Merge status: not merged
- Blockers: none for local code validation; performance target still requires longer remote benchmarking and peer-tail mitigation.

## Known Issues or Risks

- Adaptive storage recovered the 800k-900k+ peak range, but total sync-time improvement depends on reducing low-throughput peer-tail minutes.
- Earlier commit tests must use isolated test data dirs or carefully verified compatibility so old storage code does not mutate the main `/Volumes/SSD 4TB/LogEx` data.
- The branch is not ready for PR/merge until validation passes and a longer remote benchmark confirms a measured improvement over the selected baseline.
