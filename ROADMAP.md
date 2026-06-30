# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Active branch: `fix/ipv6-p2p-sync`.

Historical EL sync has completed a post-fix pivot-to-genesis baseline on the Mac mini and no longer shows the two-hour liveness stall that previously blocked ordered writes. The accepted scheduler keeps expected historical work active, limits background maintenance while history is incomplete, and was bounded by the available network during the last validated run.

IPv6 validation is partially complete on temporary droplet `root@152.42.222.119` with public IPv6 `2400:6180:0:d2:0:2:fa9c:1000`. Strict IPv6 CL networking works: bounded proofs used IPv6 DNS, `--grpc-host ::1`, IPv6-only P2P bind/dial settings, and an owner firewall rejecting IPv4 egress for the LogEx runtime user; samples reached healthy CL session counts with zero LogEx IPv4 sockets. EL IPv6 transport also works: controlled two-node proofs established Eth70 execution sessions over an explicit IPv6 `enode://` bootnode while IPv4 egress was blocked. Public strict IPv6 EL discovery starts cleanly and submits IPv6 candidates, but public serving-peer acceptance is still not proven because the reachable public IPv6 execution peers observed from the droplet did not accept sessions.

Default dual-stack startup now keeps execution on the preferred public IPv4 path while allowing consensus to advertise IPv6 when a public IPv6 route is also available. This fixed the droplet default-mode stall where EL waited for CL indefinitely: the post-fix bounded smoke reached `Syncing`, CL peaked at 29 active sessions, and EL accepted 13 sessions while execution still advertised IPv4.

Public mainnet EL historical sync over strict IPv6 is not proven. The public execution DNS tree exposes very few usable IPv6 peers from the droplet; most DNS candidates refuse, reset, or time out. A corrected official `ethereum/discv4-dns-lists` snapshot audit found 594 IPv6 execution ENRs, only 14 had open IPv6 TCP from the droplet, and focused explicit-bootnode proofs against all open ENRs still did not establish an EL session. Runtime discovery can find additional IPv6 sockets, but most are guessed RLPx endpoints from discv5 records and did not accept EL sessions in bounded probes. Local audits of geth, Reth, and Nethermind did not reveal a missed official IPv6 EL bootnode source. LogEx now keeps sparse submitted EL dial candidates alive across silent timeout windows, and DNS ENRs without direct TCP fields are still retained for signed IPv6 discovery when they carry `udp6`.

Temporary IPv6 droplet clients are stopped after bounded proof windows. Do not leave a full sync running on that droplet unless an active test requires it.

## Completed Since Last Run

- Preserved DNS ENRs that have an IP address but no direct TCP endpoint so IPv6 `udp6` records can still seed signed discv5 discovery.
- Added a regression test for IPv6 UDP-only DNS ENR conversion and updated DNS peer-manager fixtures for optional direct node records.
- Rebuilt and tested the latest branch on the temporary IPv6 droplet with owner-level IPv4 egress blocked for the LogEx runtime user.
- Re-ran strict public IPv6 smoke: CL peaked at 91 active sessions and 2,618 dialable peers with zero IPv4 sockets, while EL submitted 205 IPv6 candidate dials and still accepted no public serving peer.
- Re-ran controlled two-node IPv6 EL proof: seed and client established execution sessions over IPv6, reported `eth/70` log mentions, and opened zero IPv4 sockets.
- Corrected the official `ethereum/discv4-dns-lists` ENR parser used for the IPv6 proof and selected 120 public IPv6 execution ENRs from 594 available records.
- Re-ran strict public IPv6 smoke with those 120 ENRs: CL stayed healthy, LogEx opened zero IPv4 sockets, EL submitted 1,049 IPv6 dials, and no public EL session was accepted.
- Probed all 594 public IPv6 execution ENRs directly: only 14 had an open IPv6 TCP port from the droplet.
- Re-ran a focused strict IPv6 proof against all 14 TCP-open ENRs: LogEx submitted 419 IPv6 dials, opened zero IPv4 sockets, and still established no EL session.
- Confirmed the temporary droplet proof state was cleaned up after bounded tests: no LogEx process, owner IPv4 reject rule, or resolver override remained.

## Remaining TODOs

1. Decide the production policy for strict IPv6-only EL sync.
   - Reason: strict IPv6 CL works and EL transport works with explicit IPv6 peers, but public mainnet IPv6 EL peer availability is not sufficient for proven historical sync.
   - Completion criteria: either demonstrate historical EL progress using only IPv6 sockets against reliable public IPv6 execution peers, approve and validate a bounded heuristic candidate source such as CL-discovered IPv6 peer addresses, or deliberately scope strict IPv6 EL as an advanced explicit-bootnode mode while default startup uses IPv4, dual-family, or outbound known-peer paths when available.

2. Complete true dual-stack inbound support if it remains a product goal.
   - Reason: current automatic selection can advertise one family and dial both routed families, but true simultaneous IPv4 and IPv6 advertised inbound identity/listeners require a larger Reth integration or composite peer-manager design.
   - Completion criteria: either implement and validate true dual inbound identity or document the single-advertised-family design as intentional.

3. Conclude the IPv6 P2P branch.
   - Reason: the branch contains useful IPv6 socket, address-family, checkpoint, DNS, bootnode, and diagnostic improvements, but public strict IPv6 EL sync remains unresolved.
   - Completion criteria: finish the policy decision above, run hosted checks when GitHub Actions quota is available, create the PR, and merge only when checks and review criteria are satisfied.

## Design Decisions

- Strict IPv6 mode must be socket-clean.
  - Why: users with IPv6-only public reachability need confidence that LogEx can run without silently using IPv4.
  - Tradeoff: this makes public EL peer scarcity visible instead of masking it through IPv4 fallback.

- Automatic P2P selection separates advertised address family from outbound dial families.
  - Why: home users may have public IPv4, public IPv6, CGNAT IPv4, both routes, or outbound-only connectivity.
  - Tradeoff: current behavior advertises one local public family while allowing outbound dials over all usable routed families; true dual inbound identity is left as a separate architecture decision.

- Consensus P2P may advertise IPv6 while execution advertises IPv4 in automatic dual-stack mode.
  - Why: the EL peer network is still much denser on IPv4, but the beacon network proved healthier over IPv6 on the dual-stack droplet; using the best family per layer unblocked startup without forcing strict IPv6 EL.
  - Tradeoff: the node may have separate advertised public addresses for CL and EL instead of one shared P2P family.

- Explicit NAT without an explicit bind-family override also preserves routed outbound families.
  - Why: `--nat extip:<ipv6>` is often used to advertise public IPv6 behind CGNAT IPv4, but forcing EL to strict IPv6 in that case makes sync depend on sparse public IPv6 execution peers.
  - Tradeoff: operators who need a socket-clean strict IPv6 run must also pass an explicit IPv6 `--p2p-bind-ip`.

- Execution bootnodes support explicit `enode://` and signed `enr:` records, including hostname resolution for enodes.
  - Why: public IPv6 EL discovery is sparse, so operators need a deterministic way to provide known IPv6 peers.
  - Tradeoff: signed discv5 ENRs still require same-family UDP fields because Reth discv5 rejects IPv6 signed ENRs with only generic UDP, while direct and unsigned candidates may use geth/Nethermind-compatible generic TCP/UDP fallback.

- DNS discovery events are retained even when they do not have a direct TCP endpoint.
  - Why: an IPv6 ENR with `udp6` but no `tcp6` cannot be dialed directly, but it can still seed signed discv5 and expand discovery.
  - Tradeoff: such records must remain optional direct candidates, so logging and tests now handle DNS updates without a `NodeRecord`.

- Consensus bootnodes and cached peers are filtered by dial family before discovery seeding.
  - Why: strict IPv6 mode should not seed IPv4-only ENRs or retain cached peers without a compatible dial address.
  - Tradeoff: status `bootnode_count` now means compatible seeded bootnodes, not total built-in bootnode records.

- Sparse execution dial candidates are retained across silent submitted-dial expiry.
  - Why: IPv4 discovery has enough candidates to mask failed waves, but strict IPv6 has a small public candidate set and must keep retrying instead of dropping all candidates after one timeout.
  - Tradeoff: unreachable public IPv6 endpoints will be retried periodically until better peers are found or explicit bootnodes are configured.

## Challenges and Resolutions

- Challenge: public EL IPv6 peers were effectively unavailable from the test droplet.
  - Resolution: compared LogEx behavior with geth, audited major client source, proved controlled IPv6 EL transport with explicit bootnodes, confirmed the current Reth static mainnet execution bootnodes are IPv4-only, and tested the two TCP-open public IPv6 execution ENRs found in the official discovery snapshot as explicit bootnodes.
  - Remaining: public strict IPv6 historical EL sync is still unproven.

- Challenge: default dual-stack startup preferred IPv4 for both EL and CL, but CL stayed at zero active sessions for six minutes on the IPv6 droplet.
  - Resolution: selected the CL address independently so EL keeps public IPv4 while CL uses public IPv6 when both routes are available; the post-fix smoke reached live `Syncing`.
  - Remaining: no observed default-mode blocker remains; strict public IPv6 EL is still unresolved.

- Challenge: public IPv6 probes found many TCP-open sockets that still did not become serving EL peers.
  - Resolution: traced Reth discv5 handling and confirmed it may use the discovery UDP port as a guessed RLPx TCP port when `tcp6` is missing; this is useful for compatibility but noisy under strict IPv6 scarcity.
  - Remaining: strict public IPv6 EL sync still needs either accepting public IPv6 execution peers or an explicit reliable IPv6 bootnode source.

- Challenge: enabling Reth discv4 on strict IPv6 execution bind was a plausible missing discovery path because Reth's discv4 codec supports IPv6 endpoints.
  - Resolution: tested it in a bounded strict IPv6 public smoke on the droplet; it did not produce additional EL candidates or sessions, so the experiment was reverted.
  - Remaining: no discv4 change is carried forward.

- Challenge: generic-port IPv6 signed ENRs are valid enough for direct dialing but not accepted by Reth discv5 as signed discovery bootnodes.
  - Resolution: kept signed discovery strict on `udp6` and added a regression test that preserves generic-port records through the unsigned/direct path instead.
  - Remaining: no code issue remains; this limits only signed discovery seeding for generic-port IPv6 records.

- Challenge: DNS event conversion discarded IPv6 discovery-only ENRs before they could be used as signed discv5 seeds.
  - Resolution: replaced the direct-record-only DNS update with a local update type that preserves peer id, optional direct record, fork id, and ENR.
  - Remaining: no code issue remains in that path; public strict IPv6 EL still lacks accepting peers.

- Challenge: strict IPv6 CL runs still saw family-incompatible bootnode/discovery paths.
  - Resolution: CL bootnodes and cached peers are now retained only when they have addresses compatible with the configured dial families.
  - Remaining: no code issue remains in the observed CL path; EL public IPv6 peer availability is still the blocker.

- Challenge: strict IPv6 EL candidate dials disappeared after silent timeout when no session event was produced.
  - Resolution: submitted dials now retain the original direct `NodeRecord` and requeue after the existing suppression interval.
  - Remaining: public IPv6 EL endpoints still did not accept sessions during the bounded smoke.

- Challenge: Linux droplet builds are slow without cache.
  - Resolution: synced source to the droplet and reused its existing Linux Cargo cache; macOS release binaries are not portable to the Linux proof host.
  - Remaining: document any future distributable cache recipe separately if build-time work becomes a product task.

## Dead Code and Obsolescence Cleanup

- Inspected the IPv6 branch for proof-only leftovers; no temporary scripts, binaries, data dirs, resolver overrides, or firewall rules are intended to remain on the droplet after bounded tests.
- Confirmed the latest droplet proof cleanup left no LogEx process, owner IPv4 block, or resolver override active.
- Replaced the obsolete family-agnostic CL bootnode/cache seeding path with dial-family-aware helpers.
- Replaced the submitted-dial timestamp-only map with a small `SubmittedDial` record so expired direct candidates can be retried instead of discarded.
- Replaced the imported direct-record-only DNS update shape with the local optional-direct-record representation; no additional production code was identified as safe to remove in this run.
- Reverted the strict IPv6 discv4 experiment after it failed to improve public EL discovery.

## Git Workflow

- Current branch: `fix/ipv6-p2p-sync`.
- New branch created this run: no; continued the existing IPv6 validation branch.
- Commits made this run: `docs: record ipv6-only validation`.
- Pull request status: not created; the task is not complete while public strict IPv6 EL sync policy remains unresolved.
- Merge status: not merged.
- Blockers: public IPv6 EL peer availability is unresolved; GitHub Actions quota has previously blocked hosted validation.

## Known Issues or Risks

- Pure IPv6 EL historical sync may be impractical on current public mainnet peer availability without a reliable IPv6 execution peer source.
- True simultaneous IPv4 and IPv6 inbound identity is not implemented yet.
- Future benchmark comparisons must record routing mode, peer counts, and whether traffic is routed through the VPS, dashboard-only WireGuard, or local networking.
- Do not leave long-running full syncs on temporary proof droplets unless the user explicitly asks for that test.
