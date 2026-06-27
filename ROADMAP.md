# Roadmap

## Current Status

LogEx verifies a recent checkpoint-backed consensus pivot, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed log segments, and serves verified logs through the dashboard, query APIs, and live transfer notifications.

The active work is PR #96 on branch `perf/historical-sync-live-scheduler`. This branch focuses on making historical EL sync stable under peer-tail latency while preserving ordered cryptographic verification and storage writes.

The Mac mini client runs from `/Volumes/SSD 4TB/LogEx` on HTTP port `18683`. When outside the home network, operational checks use the Raspberry Pi jump host before reaching `gremlinmaster@192.168.50.44`. The current production candidate routes dense historical body/receipt batches through the live chunk-owned scheduler; the obsolete decoupled dense fast path has been removed. The live scheduler now consumes body/receipt reservations when requests actually start, discards mismatched expected-sequence attempts without resetting lookahead, refills a missing expected sequence from inside the wait loop, periodically runs bounded critical-path refill while waiting for the expected fetch, and keeps a bounded probe for connected execution client families that have no serving representative. The latest Pi-routed spot check after the direct-network failure measured `395.8` actual blocks/sec with the client reachable through the jump host.

## Completed Since Last Run

- Revalidated the scheduler through the required Pi jump route after direct Mac mini access became unavailable.
- Rejected a retry-cap experiment that allowed a third expected-prefix attempt.
  - Reason: it did not improve the corrected-route sample.
  - Result: reverted it and restored the remote binary before continuing.
- Added bounded critical-path refill to the expected-fetch wait timer.
  - Reason: the old timer path only started already-ready plans and could leave the downloader underfilled until another fetch/header event arrived.
  - Result: the wait loop now runs the same bounded refill used elsewhere while keeping duplicate attempts capped.
- Validated locally with `cargo fmt --check`, `cargo test -p logex-sync`, and `cargo clippy -p logex-sync -- -D warnings`.
- Deployed the changed source file to the Mac mini through `pi-remote`, rebuilt `logex-node --release`, restarted LogEx without clearing data, and sampled `/Users/gremlinmaster/logex-src/run/logex-throughput-v3-20260627-025217.log`.
- Remote validation: ten minutes, `233,826` blocks advanced, `382.7` actual blocks/sec, `3` low-progress windows, `0` zero-progress windows, with the client left running.
- Added bounded client-family probes to body/receipt candidate retention.
  - Reason: the remote pool showed many connected Nethermind peers but zero serving Nethermind peers, which left candidate selection over-dependent on already-serving Geth peers.
  - Result: after redeploying through `pi-remote`, Nethermind peers entered the serving set during warm-up and a ten-minute sample advanced `234,278` blocks at `383.4` actual blocks/sec.
- Ran a longer Pi-routed scheduler validation sample.
  - Result: `350,999` blocks over `1,221s`, `287.5` actual blocks/sec, `12` low-progress windows, and `8` zero floor-advance windows while active fetches and RX remained nonzero.
  - Interpretation: the current remaining burstiness is ordered floor advancement and prepare/write cadence, not a request scheduler that has stopped feeding downloads.
- Removed the disabled decoupled dense body/receipt scheduler path.
  - Reason: production now always uses the live chunk-owned scheduler, and keeping a dormant second scheduler increased maintenance risk.
  - Result: deleted the decoupled selector, executor, request loops, helpers, and decoupled-only tests; `cargo test -p logex-sync` and `cargo clippy -p logex-sync -- -D warnings` pass.
- Reran the remote throughput sampler through `pi-remote` after direct Mac mini access returned a network-unreachable error.
  - Result: the client remained healthy and advanced `41,956` blocks over `106s` at `395.8` actual blocks/sec, with `68-72` connected peers and one short zero-advance floor window.
- Fixed the GitHub workspace clippy failure caused by the newer `chunks_exact_to_as_chunks` lint in `logex-storage` and `logex-cl`.
  - Result: page decoding, CL proof helpers, and CL RPC decoders now use fixed-size slice chunks where lengths are already validated; `cargo clippy --workspace -- -D warnings` passes locally.

## Remaining TODOs

1. Validate the live scheduler over a long historical run.
   - Reason: five-minute dense samples prove the stall mode improved, but full-run performance varies by log density and peer mix.
   - Completion criteria: record start-to-genesis time, p50/p90/max logs/sec, actual floor blocks/sec, low/zero-progress windows, bandwidth, CPU, memory, disk, peer counts, resets, and failures.
   - Current progress: the latest dense sample improved over the prior `313.8` blocks/sec comparison and eliminated zero-progress windows, but a full fresh run is still required before closing this TODO.

2. Complete the live request scheduler admission design.
   - Reason: the current live scheduler is better than the decoupled path, but still relies on conservative slot admission and can leave useful bandwidth idle when peer-tail latency rises. The rejected overdraft experiment showed that simply borrowing more slots increases duplicate pressure and hurts end-to-end progress.
   - Completion criteria: implement admission that keeps enough independent prefix work active without overfilling per-peer request slots; expose focused debug metrics for live backlog, prefix wait age, retry/hedge counts, peer timeout share, bandwidth use, and write/prepare pressure; validate against the kept baseline with longer samples and no recurring zero-progress windows.
   - Current progress: refill diagnostics exposed reservation overcounting, mismatched expected-attempt resets, a missing-expected-fetch wait-loop gap, an underfilled timer path, and candidate retention that could starve unproven client families. These are fixed and remotely validated. The latest long sample shows active downloads during low floor-advance windows, so the next substantial improvement should target ordered materialization/write cadence unless future samples show idle bandwidth.

3. Complete EL production hardening.
   - Reason: scheduler changes must not weaken checkpoint freshness, forward sync, reorg handling, restart safety, low-disk behavior, query correctness, or dashboard access.
   - Completion criteria: tests or smokes cover fresh-checkpoint enforcement, stale restart rejection, CL tracking, EL forward sync, EL reverse sync, invalid peer data, reorg handling, low disk behavior, authenticated dashboard access, and a clean full-sync candidate run.

## Design Decisions

- Dense historical body/receipt batches now use the live chunk-owned scheduler by default.
  - Why: it keeps per-chunk body/receipt state, tracks in-flight role ownership, and can repair prefix-critical gaps without making progress depend on separate body and receipt loops completing at the same time.
  - Alternative considered: keep tuning the decoupled dense fast path. It was rejected for now because repeated samples showed prefix-tail stalls even when later work and peers were available.
  - Tradeoff: peak logs/sec may be lower than the best decoupled bursts, but sustained progress is smoother and less vulnerable to long zero-progress windows.
- The decoupled dense scheduler has been deleted instead of retained behind a disabled flag.
  - Why: dormant performance code was no longer representative of the production path and carried stale request-accounting behavior.
  - Alternative considered: keep it for future experiments. That was rejected because future experiments should start from the live scheduler baseline with explicit ownership semantics.
- Historical sync remains EL-driven after a valid recent CL checkpoint.
  - Why: CL forward tracking is required for live head and reorg safety, but historical reverse sync should not wait on CL head movement once the pivot is validated.
- Downloads, extraction, and writes may overlap, but verified storage advancement remains ordered.
  - Why: this preserves block parent-chain and receipt-root verification semantics while still allowing async work to keep the machine busy.
- Machine-specific routing scripts are operational tooling, not repository behavior.
  - Why: LogEx should run correctly regardless of whether this Mac mini uses full VPS routing, dashboard-only forwarding, or direct local networking.
- Do not increase request fanout only to hide short zero floor-advance samples.
  - Why: the latest long validation kept downloads active and RX high during those windows; more fanout would increase duplicate pressure without proving faster end-to-end sync.
  - Alternative considered: keep adding admission/fanout tweaks. That remains appropriate only if a future sample shows idle bandwidth with enough ready peers.

## Challenges and Resolutions

- Challenge: historical progress had recurring spikes and plunges despite available peers.
  - Resolution: instrumented the scheduler, identified under-owned prefix chunks in the decoupled dense path, switched dense plans to the live scheduler path, made stale in-flight live prefix roles prefix-critical before salvage, and raised the healthy active-fetch floor to six.
  - Remaining: full-run validation is still required before concluding PR #96.
- Challenge: several small scheduler experiments improved isolated metrics but regressed end-to-end samples.
  - Resolution: rejected and reverted candidates that increased duplicate pressure, reduced dense batch efficiency, or produced more low/zero-progress windows, including bounded slot overdraft, slot-margin concurrency capping, and a third expected-prefix retry attempt.
  - Remaining: future work should stop one-line tuning and move to a deliberate admission/scheduler change compared against the kept live-scheduler baseline.
- Challenge: low-progress windows still need a precise cause after the live scheduler milestone.
  - Resolution: diagnostics identified reservation double-counting, stale expected-sequence attempts, a wait-loop missing-expected-fetch gap, and timer ticks that did not refill the critical path; all are now handled without dropping valid lookahead.
  - Remaining: validate over a longer run and continue admission work only where bandwidth or CPU is demonstrably idle.
- Challenge: connected Nethermind peers were not becoming serving peers during body/receipt sync.
  - Resolution: candidate retention now keeps a bounded probe for each execution client family with no serving representative.
  - Remaining: longer runs still need to prove whether this improves full-run peer diversity beyond the dense sample.
- Challenge: floor progress still appears bursty even while downloads continue.
  - Resolution: a twenty-minute sample showed high RX and nonzero active/prepared work through the low windows, shifting the suspected bottleneck from peer-tail admission to ordered materialization/write cadence.
  - Remaining: optimize the commit/write cadence only if it improves total sync time, not just the shape of the graph.
- Challenge: the roadmap had accumulated too much experiment-by-experiment detail.
  - Resolution: condensed it to current state, decisions, and remaining work.
- Challenge: obsolete decoupled scheduler code remained after the live scheduler became the only production path.
  - Resolution: removed the disabled path and its tests, then validated the body/receipt scheduler and full `logex-sync` package.
- Challenge: GitHub clippy failed on a newer nightly lint outside the scheduler package.
  - Resolution: replaced constant-size `chunks_exact` usage in storage and CL code with fixed-size slice chunks and validated the full workspace clippy command locally.

## Dead Code and Obsolescence Cleanup

- Inspected the decoupled dense body/receipt path after selecting the live scheduler for production dense plans.
- Removed diagnostic-only status fields from the final patch because they were tied to the rejected decoupled path.
- Rechecked the stale remote warning about obsolete body/receipt metric fields; local code no longer contains those fields and the Mac mini source was synchronized to match.
- Removed the disabled decoupled dense executor, selector, request loops, helper functions, and decoupled-only tests.
- Cleaned up storage and CL chunk iteration flagged by the current workspace clippy job.
- `.DS_Store` remains untracked and unrelated.

## Git Workflow

- Current branch: `perf/historical-sync-live-scheduler`.
- New branch created this run: no.
- Commits made during this run: `perf: route dense history through live scheduler`; `perf: stabilize live historical scheduler`; `docs: record rejected scheduler admission cap`; `feat: expose historical scheduler refill diagnostics`; `fix: preserve lookahead on stale historical attempts`; `fix: refill missing historical prefix fetch`; `fix: refill historical fetches while waiting`; `perf: probe unproven execution clients`; `docs: record historical scheduler validation`; pending commit for decoupled scheduler cleanup.
- Pull request status: PR #96 remains the active draft performance PR.
- Merge status: not ready as a production-complete scheduler; can be accepted only as a measured live-scheduler milestone before the larger admission redesign.
- Blockers: none.

## Known Issues or Risks

- The live scheduler candidate improves the observed stall mode, but the latest validation is still a short sample.
- Peer mix and network routing materially affect measurements; benchmark notes must record routing mode, serving peers, and bandwidth.
