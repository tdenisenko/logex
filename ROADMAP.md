# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-queue-v2`. The branch is focused on EL historical sync throughput and peer-tail stability.

Current benchmark state: the Mac mini remote is running on `/Volumes/SSD 4TB/LogEx` through the VPS full tunnel on HTTP port `18683`. The data directory was reset once for a fresh dense-range benchmark and must not be reset again until this sync reaches genesis. Historical sync remains fetch-bound by body/receipt peer response tails, not CPU, RAM, or disk IO. The retained pipeline uses density-aware 1,024-block body/receipt return windows, split body and receipt chunk queues for dense prefixes, a 6-deep dense historical fetch pipeline, 6-second body/receipt request timeouts, early peer accounting for asynchronous fetch outcomes, macOS memory-aware sizing, split active/completed fetch budgeting when memory is healthy, two-stage prepare lookahead, and per-request exclusion of peers that fail decoupled body/receipt chunks.

Draft PR: https://github.com/tdenisenko/logex/pull/93

## Completed Since Last Run

- Reset the remote benchmark data directory once, preserving only `discovery-secret` and `known-peers.json`, so dense recent ranges can be measured again. This reset must not be repeated until the current run reaches genesis.
- Reverted the 4,096-block dense window experiment before reset after a fixed sample regressed to about 101k actual logs/sec.
- Retained the measured useful changes from the reset run: two-stage historical prepare lookahead and local exclusion of peers that fail decoupled body/receipt chunks.
- Benchmarked and rejected lowering the split dense minimum peer threshold from 12 to 8; fixed samples were noisy and not materially better than baseline.
- Benchmarked and rejected plan-time peer rotation; it regressed from about 151k to about 126k actual logs/sec in comparable 5-minute samples.
- Benchmarked and rejected a 1-second chunk hedge delay; it caused poor peer recovery and throughput collapse during warm-up.
- Benchmarked and rejected one-per-peer and two-per-peer shared request coordinator experiments; both throttled body/receipt fetching and regressed actual committed logs/sec.
- Checked local geth/nethermind sources. The relevant reference pattern is a central idle-peer queue with per-peer capacity and timeout-based unreserve/reschedule, which is larger than a timing-constant tweak.

## Remaining TODOs

1. Improve body/receipt peer-tail handling.
   - Reason: Historical sync is still limited by slow or timeout-prone peers delaying contiguous prefixes.
   - Completion criteria: A benchmark shows a sustained, meaningful improvement over the retained density-aware pipeline without increasing validation risk, memory risk, or peer churn. The most likely next candidate is a geth-style idle-peer/capacity queue for historical body and receipt tasks.

2. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a live release-candidate run from a clean data directory.

## Design Decisions

- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.
- Checkpoints are accepted only when they are recent relative to a checkpoint-sync endpoint. Persisted state that is too stale now requires a fresh recent checkpoint.
- Historical body/receipt sync may download below a residual gap before that gap is written, but data is still committed only after cryptographic validation and in chain order.
- Dense historical ranges use smaller 512-block fetch windows with bounded lookahead, because this reduced queue loss in partial-prefix cases while keeping memory use controlled.
- Global depth-8 fetching was rejected, but density-gated depth-8 fetching is retained for dense-but-not-extreme ranges after live testing showed zero residual batches in the sampled window and better body/receipt p50 than the prior retained run.
- Active downloads and completed fetch buffering are budgeted separately while available memory is healthy; low-memory mode still uses the conservative combined pending cap.
- Body/receipt chunk attempts still keep intra-chunk fallback peers, because one-peer chunk attempts returned failures faster but caused residual gaps and near-stalls.
- The paired body/receipt plan window still allows roughly two paired chunks per peer; a one-paired-chunk-per-peer policy was closer to geth's busy-peer model but did not improve sustained remote throughput in this workload.
- The body/receipt plan timeout stays at 45 seconds, because an 18-second cap increased partial/residual work in dense ranges.
- Body/receipt request timeout remains 6 seconds with a 3-second hedge delay. A 4-second timeout lowered some short samples but increased timeout density, peer churn, and p50/p90 fetch latency over a larger sample.
- Peer accounting is applied when fetch outcomes are received, not only when they are ingested in order, because queued fetch plans otherwise reused peers that had already timed out in completed asynchronous work.
- Dense ranges cap body/receipt return windows at 1,024 blocks, fetch body and receipt chunks independently, accept minimum valid contiguous split prefixes, and use a 6-deep historical fetch pipeline. Receipt peer attribution remains per block so validation failures still penalize the serving peer that supplied the bad receipts.
- Stale PR #91 was audited instead of merged because its large downloader changes would discard the newer residual-gap and overlap architecture; only low-risk pieces with direct tests were retained.

## Challenges and Resolutions

- Challenge: Partial body/receipt prefixes caused long queue resets even when hundreds of contiguous blocks were valid.
  - Resolution: Accepted one full chunk as recoverable progress, repaired the residual gap, and continued lookahead below the residual boundary.

- Challenge: Healthy dense lookahead was reset when transient peer-count changes lowered the computed buffer depth.
  - Resolution: Removed that reset path; only memory pressure can now force a healthy lookahead reset.

- Challenge: Increasing active fetch depth to 8 looked promising but increased timeout churn and below-prefix failures.
  - Resolution: Reverted the broad depth-8 change, then retained a narrower density-gated depth-8 path for dense ranges where the 512-block cap keeps memory bounded.

- Challenge: An older open performance PR contained a mix of obsolete downloader changes and useful small optimizations.
  - Resolution: Kept the storage metadata append optimization, receipt bloom cache preallocation, and CL log-level downgrade; rejected the stale downloader diff.

- Challenge: Several plausible peer-tail mitigations improved one metric while hurting ordered progress.
  - Resolution: Reverted larger dial fanout, depth-8 fetches, one-peer chunk attempts, larger fetch buffers, and an 18-second plan timeout after live benchmarks showed worse residuals or stalls.

- Challenge: Medium-density ranges still produced large partial body/receipt tails.
  - Resolution: Capped medium-density batches by target row count, treated 300+ rows/block as dense, and changed residual repair to keep verified partial progress instead of restarting with sequential fetches.

- Challenge: A graceful-restart test exposed a storage recovery gap where WAL replay had already advanced hot columns but canonical bitmap repair did not complete before startup integrity verification.
  - Resolution: WAL replay now verifies already-applied hot rows against the WAL before repairing metadata, and startup repairs recoverable hot canonical bitmap length mismatches before integrity verification.

- Challenge: Completed fetch buffers could fill and throttle new downloads even when CPU, disk, and memory were healthy.
  - Resolution: Split the healthy-memory budget so active downloads can stay full while completed fetches wait to be ingested; low-memory mode keeps the previous combined cap.

- Challenge: Reducing each body/receipt plan to one paired chunk per peer reduced some fetch-tail latency but also lowered active fetch utilization.
  - Resolution: Reverted the experiment after remote samples failed to show a sustained logs/sec improvement.

- Challenge: Slow peers were scored only when queued fetch outcomes were consumed in sequence, allowing timed-out peers to appear in multiple future plans.
  - Resolution: Drained success/failure accounting when fetch outcomes arrive, while preserving ordered validation and writes.

- Challenge: Shorter body/receipt timeouts looked promising in isolated samples but made the run more bursty.
  - Resolution: Reverted the 4-second timeout and 2-second hedge delay after the larger sample regressed to 8.3s p50 and 18.3s p90 body/receipt latency.

- Challenge: GitHub CI clippy failed on generated tonic code, not handwritten application code.
  - Resolution: Added a module-scoped generated-code allowance for `clippy::result_large_err` at the protobuf include boundary.

- Challenge: Dense ranges were still sensitive to slow body/receipt peers.
  - Resolution: Capped dense return windows to 1,024 blocks and reduced dense fetch depth to 4 after live samples showed lower timeout churn than depth 5, depth 8, shorter request timeouts, or deeper prepare lookahead.

- Challenge: Pairing body and receipt chunks made each prefix depend on the slower side of each peer pair.
  - Resolution: Added a split dense-prefix path that fetches bodies and receipts through independent queues, preserves receipt peer provenance, and falls back to the paired path if the split path cannot produce a prefix.

- Challenge: Split-prefix fetching still fell back to the paired path when tail chunks failed, even if the leading prefix was usable.
  - Resolution: The split path now returns a validated contiguous partial prefix once it reaches the existing accepted-prefix threshold.

- Challenge: Several follow-up split-path tweaks looked plausible but did not improve actual committed progress.
  - Resolution: Rejected depth 8, bounded decoupled hedging, and larger initial request limits after live samples regressed logs/sec or reintroduced residual/fallback churn.

- Challenge: Dense-range samples after the fresh reset showed that local processing was not the bottleneck; body/receipt fetches still spent 8-26 seconds waiting on peer tails.
  - Resolution: Rejected plan-time peer rotation, 1-second hedging, and simple shared peer semaphores after live samples regressed. Geth/nethermind source review points to a larger idle-peer/capacity queue as the next meaningful direction.

## Dead Code and Obsolescence Cleanup

- Reverted rejected broad depth-8, depth-5, prefix-hedge, decoupled-hedge, larger-initial-request-limit, dial-fanout, one-peer chunk, one-paired-chunk-per-peer, larger-buffer, shorter-plan-timeout, deeper-prepare, shorter-request-timeout, plan-time peer rotation, 1-second hedge delay, lowered split-path peer threshold, and shared peer coordinator experiments before leaving the remote running.
- Rechecked the historical fetch/prepare/residual code paths and retained only changes that improved correctness or benchmark stability.
- Compared the stale performance PR against the current branch and did not carry over obsolete downloader code.
- Removed stray remote-root source copies created by a mistaken rsync destination during deployment.

## Git Workflow

- Current branch: `perf/historical-sync-queue-v2`
- New branch created this run: no
- Commits made during this run: `perf: improve historical downloader overlap`; `perf: salvage storage write optimizations`; `docs: record stale performance pr cleanup`; `perf: tune historical fetch tail handling`; `perf: reduce historical residual churn`; `fix: recover hot segment wal replay`; `perf: keep historical fetches active`; `perf: apply historical peer accounting early`; `fix: allow generated tonic clippy lint`; `perf: tune dense historical fetches`; `perf: split dense body receipt fetches`; pending commit for the retained reset-run tuning
- Pull request status: draft PR #93 open
- Merge status: not merged
- Stale PR cleanup: PR #91 was closed and remote branch `fix/historical-fetch-stalls` was deleted after useful changes were salvaged.
- Blockers: remaining EL historical sync performance work is still in progress.

## Known Issues or Risks

- Historical sync is still body/receipt fetch-tail bound; peer timeout clusters can hold back contiguous progress.
- The remote benchmark after restarts needs warm peer pools before logs/sec samples are comparable.
- Repeated restarts temporarily depress serving-peer counts; avoid more restarts unless testing a validated candidate.
- Verification-critical security review is still required before a production-ready release.
