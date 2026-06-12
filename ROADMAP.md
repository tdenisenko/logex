# Roadmap

## Current Status

LogEx verifies CL from a recent checkpoint, uses CL-authenticated execution headers as the EL pivot, verifies EL history back to genesis by default, follows new head blocks, and exposes verified logs through the dashboard, SQL endpoint, JSON-RPC, gRPC, and WebSocket.

Active branch: `perf/historical-sync-throughput`. The branch is focused on EL historical sync throughput and peer-retention stability.

Current benchmark blocker: the Mac mini split-tunnel gateway keeps local outbound traffic off the VPS, but the VPS currently source-NATs forwarded inbound TCP peers to `10.66.0.1`. Under that topology the client is seeing only roughly 4-5 serving peers and about 50k-120k logs/sec, so further code throughput tests are not comparable with the previous 400k+ logs/sec baseline.

## Completed Since Last Run

- Verified the client still runs on `192.168.50.44` with data at `/Volumes/SSD 4TB/LogEx` and HTTP port `18683`.
- Tested a 512-block dense historical fetch window and rejected it because it underperformed the 1,024-block baseline.
- Restored the 1,024-block dense fetch window and restarted the remote client from a clean data directory while preserving peer identity/cache files.
- Confirmed active sync no longer builds query indexes during historical backfill, avoiding unnecessary local contention.
- Confirmed the retained body/receipt hedge settings pass targeted tests.

## Remaining TODOs

1. Restore a valid high-peer benchmark environment.
   - Reason: Current split-tunnel source NAT caps useful serving peers and invalidates throughput comparisons.
   - Completion criteria: The benchmark host reaches a stable serving-peer pool comparable to the previous 80-90 peer run, or a new public-host setup is selected.

2. Improve historical fetch utilization beyond the current baseline.
   - Reason: The pipeline is still fetch-bound and can lose useful completed work when a blocking prefix chunk delays a batch.
   - Completion criteria: A measured benchmark beats the restored 1,024-block baseline by a meaningful margin without increasing data loss risk, memory risk, or peer churn.

3. Complete release hardening.
   - Reason: Production readiness depends on verification safety, graceful shutdown, and deployment safety.
   - Completion criteria: Tests or smokes cover bootstrap, CL updates, EL live sync, EL reverse sync, invalid peer data, reorgs, restart/resume, low disk, auth, exposed listener policy, and a live release-candidate run from a clean data directory.

## Design Decisions

- Historical sync remains independent from CL live-head waiting after the checkpoint/pivot is established; only forward/live EL tracking depends on CL head and reorg handling.
- Query index builds are deferred during active historical sync. This favors full-sync throughput and lets index catch-up run after the node is idle.
- Dense historical backfill currently uses a 1,024-block fetch window. A 512-block window reduced reset sensitivity but lost too much batch efficiency in live testing.
- The current split-tunnel setup is useful for reducing local outbound traffic through the VPS, but it is not equivalent to a clean public node because inbound peers are source-NATed at the VPS.

## Challenges and Resolutions

- Challenge: The 512-block dense-window experiment looked plausible because it reduced contiguous-prefix exposure.
  - Resolution: Benchmarked it on the remote Mac mini, observed materially lower logs/sec, and reverted it.

- Challenge: The local release binary was copied to the Mac mini once and failed with `bad CPU type in executable`.
  - Resolution: Synced source and rebuilt on the Mac mini itself.

- Challenge: Current split-tunnel networking reduced serving peers compared with earlier full-tunnel/public-host tests.
  - Resolution: Identified VPS TCP source NAT as the active topology issue; no code-side throughput conclusions should be drawn from this setup.

## Dead Code and Obsolescence Cleanup

- Reverted the rejected 512-block dense-window experiment.
- Rechecked the touched sync/index paths for temporary experiment code; retained only the query-index deferral and body/receipt hedge changes that remain justified by prior benchmark results.
- No obsolete production files were removed in this pass.

## Git Workflow

- Current branch: `perf/historical-sync-throughput`
- New branch created this run: no
- Commits made during this run: none yet
- Pull request status: not ready
- Merge status: not merged
- Blockers: high-peer benchmark environment is currently invalid under split-tunnel source NAT.

## Known Issues or Risks

- The current Mac mini split tunnel keeps default outbound traffic local, but inbound TCP peers forwarded by the VPS are source-NATed to the tunnel IP. This can reduce peer retention and makes LogEx unlike a normal public Ethereum node.
- A source-preserving split tunnel would require Mac-side policy routing/PF handling for replies from forwarded sessions, or running LogEx directly on a public host.
- The historical body/receipt pipeline still commits only contiguous prefixes. A correct suffix-cache or gap-fill redesign may be needed for the next substantial throughput gain.
- Verification-critical security review is still required before a production-ready release.
