# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Active branch: `fix/ipv6-p2p-sync`.

Historical EL sync has completed a post-fix pivot-to-genesis baseline on the Mac mini and no longer shows the two-hour liveness stall that previously blocked ordered writes. The accepted scheduler keeps expected historical work active, limits background maintenance while history is incomplete, and was bounded by the available network during the last validated run.

IPv6 validation is complete enough for this branch on temporary droplet `root@152.42.222.119` with public IPv6 `2400:6180:0:d2:0:2:fa9c:1000`. Strict IPv6 CL networking works with IPv6 DNS, `--grpc-host ::1`, IPv6-only P2P bind/dial settings, and an owner firewall rejecting IPv4 egress for the LogEx runtime user. EL IPv6 transport works in both controlled two-node proofs and public mainnet: bounded strict-IPv6 proofs reached `Syncing`, accepted serving public Reth EL peers over IPv6, tracked CL over IPv6, and moved historical sync backward while opening zero LogEx IPv4 sockets. The latest resolver fix joins chunked EIP-1459 DNS TXT records and periodically re-syncs the DNS tree, increasing accepted strict-IPv6 DNS execution candidates from 41 in the earlier 15-minute proof to 144 within a 4-minute official-ENR proof.

Default dual-stack startup now keeps execution on the preferred public IPv4 path while allowing consensus to advertise IPv6 when a public IPv6 route is also available. This fixed the droplet default-mode stall where EL waited for CL indefinitely: the post-fix bounded smoke reached `Syncing`, CL peaked at 29 active sessions, and EL accepted 13 sessions while execution still advertised IPv4.

Public mainnet EL peer availability over strict IPv6 is sparse, so performance is expected to be lower and startup may take longer than IPv4 or dual-stack mode. A corrected official `ethereum/discv4-dns-lists` snapshot audit found 594 IPv6 execution ENRs and only 14 open IPv6 TCP endpoints from the droplet, but the latest bounded proof showed runtime discovery can still find an accepting public IPv6 EL peer. LogEx keeps sparse submitted EL dial candidates alive across silent timeout windows, and DNS ENRs without direct TCP fields are retained for signed IPv6 discovery when they carry `udp6`.

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
- Re-ran a bounded strict IPv6 proof with Reth session tracing: LogEx reached `Syncing`, accepted one serving public Reth EL peer over IPv6, CL reached 379 dialable peers, historical sync moved from block 25,432,012 to 25,431,716, and zero LogEx IPv4 sockets were opened.
- Re-ran the strict IPv6 proof with the full audited IPv6 ENR set: LogEx reached `Syncing`, kept listen/advertise/dial families strictly IPv6, accepted two serving public Reth EL peers, submitted 582 IPv6 EL dials, rejected 88 same-source IPv4 DNS candidates, reached 624 CL dialable peers, and moved historical sync backward by 30,215 blocks over 30 samples.
- Replaced the family-aware execution DNS resolver with a local TXT-joining resolver after auditing Reth's resolver and confirming it only used the first TXT chunk from chunked EIP-1459 DNS records.
- Added periodic DNS tree re-bootstrap for the family-aware execution DNS service so a transient empty root lookup does not leave strict IPv6 with zero DNS candidates for the whole run.
- Re-ran strict IPv6 official-ENR smoke after the resolver fix: LogEx opened zero IPv4 sockets, CL reached 75 active sessions and 3,281 dialable peers, EL accepted 144 IPv6 DNS execution candidates within four minutes, and the temporary droplet process/firewall/resolver cleanup was confirmed afterward.
- Confirmed the temporary droplet proof state was cleaned up after bounded tests and rechecked it after the final proof summary: no LogEx process, owner IPv4 reject rule, or resolver override remained.
- Re-ran focused local regression checks for IPv6 P2P selection, consensus family selection, execution peer-manager DNS/bootnode/retry handling, and formatting.

## Remaining TODOs

1. Complete true dual-stack inbound support if it remains a product goal.
   - Reason: current automatic selection can advertise one family and dial both routed families, but true simultaneous IPv4 and IPv6 advertised inbound identity/listeners require a larger Reth integration or composite peer-manager design.
   - Completion criteria: either implement and validate true dual inbound identity or document the single-advertised-family design as intentional.

2. Conclude the IPv6 P2P branch.
   - Reason: the branch contains useful IPv6 socket, address-family, checkpoint, DNS, bootnode, and diagnostic improvements.
   - Completion criteria: run hosted checks when GitHub Actions quota is available and merge PR #98 only when checks and review criteria are satisfied.

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

- Family-aware execution DNS discovery uses a TXT-joining resolver and periodic tree re-bootstrap.
  - Why: mainnet EIP-1459 branch records are often split into multiple DNS TXT chunks, and using only the first chunk silently drops most IPv6 candidates; a transient empty root lookup also should not permanently disable DNS discovery for the run.
  - Tradeoff: this keeps one small resolver wrapper in LogEx instead of relying directly on Reth's default resolver behavior.

- Consensus bootnodes and cached peers are filtered by dial family before discovery seeding.
  - Why: strict IPv6 mode should not seed IPv4-only ENRs or retain cached peers without a compatible dial address.
  - Tradeoff: status `bootnode_count` now means compatible seeded bootnodes, not total built-in bootnode records.

- Sparse execution dial candidates are retained across silent submitted-dial expiry.
  - Why: IPv4 discovery has enough candidates to mask failed waves, but strict IPv6 has a small public candidate set and must keep retrying instead of dropping all candidates after one timeout.
  - Tradeoff: unreachable public IPv6 endpoints will be retried periodically until better peers are found or explicit bootnodes are configured.

## Challenges and Resolutions

- Challenge: public EL IPv6 peers were effectively unavailable from the test droplet.
  - Resolution: compared LogEx behavior with geth, audited major client source, proved controlled IPv6 EL transport with explicit bootnodes, confirmed the current Reth static mainnet execution bootnodes are IPv4-only, and finally proved public strict IPv6 EL sync with an inbound public Reth peer discovered during a bounded run.
  - Remaining: public IPv6 EL peer availability is sparse, so strict IPv6 performance should be treated as best-effort unless the operator supplies reliable IPv6 execution bootnodes.

- Challenge: default dual-stack startup preferred IPv4 for both EL and CL, but CL stayed at zero active sessions for six minutes on the IPv6 droplet.
  - Resolution: selected the CL address independently so EL keeps public IPv4 while CL uses public IPv6 when both routes are available; the post-fix smoke reached live `Syncing`.
  - Remaining: no observed default-mode blocker remains; strict public IPv6 EL is functionally proven but remains peer-scarcity limited.

- Challenge: public IPv6 probes found many TCP-open sockets that still did not become serving EL peers.
  - Resolution: traced Reth discv5 handling and confirmed it may use the discovery UDP port as a guessed RLPx TCP port when `tcp6` is missing; this is useful for compatibility but noisy under strict IPv6 scarcity.
  - Remaining: no correctness blocker remains; sparse/slow public IPv6 EL discovery is still an operational caveat.

- Challenge: enabling Reth discv4 on strict IPv6 execution bind was a plausible missing discovery path because Reth's discv4 codec supports IPv6 endpoints.
  - Resolution: tested it in a bounded strict IPv6 public smoke on the droplet; it did not produce additional EL candidates or sessions, so the experiment was reverted.
  - Remaining: no discv4 change is carried forward.

- Challenge: generic-port IPv6 signed ENRs are valid enough for direct dialing but not accepted by Reth discv5 as signed discovery bootnodes.
  - Resolution: kept signed discovery strict on `udp6` and added a regression test that preserves generic-port records through the unsigned/direct path instead.
  - Remaining: no code issue remains; this limits only signed discovery seeding for generic-port IPv6 records.

- Challenge: DNS event conversion discarded IPv6 discovery-only ENRs before they could be used as signed discv5 seeds.
  - Resolution: replaced the direct-record-only DNS update with a local update type that preserves peer id, optional direct record, fork id, and ENR.
  - Remaining: no code issue remains in that path; public strict IPv6 EL still lacks accepting peers.

- Challenge: strict IPv6 DNS discovery undercounted public execution ENRs and sometimes stayed at zero candidates for a whole proof run.
  - Resolution: audited live EIP-1459 records and Reth's DNS resolver, added a TXT-joining resolver for family-aware execution DNS, and periodically re-synced the DNS tree after startup. The next official-ENR proof reached 144 accepted IPv6 candidates in four minutes with zero IPv4 sockets.
  - Remaining: public IPv6 EL serving peers are still sparse, so candidate count is improved but peer acceptance remains lower than IPv4.

- Challenge: strict IPv6 CL runs still saw family-incompatible bootnode/discovery paths.
  - Resolution: CL bootnodes and cached peers are now retained only when they have addresses compatible with the configured dial families.
  - Remaining: no code issue remains in the observed CL path; EL public IPv6 peer availability is still the blocker.

- Challenge: strict IPv6 EL candidate dials disappeared after silent timeout when no session event was produced.
  - Resolution: submitted dials now retain the original direct `NodeRecord` and requeue after the existing suppression interval.
  - Remaining: no correctness blocker remains after the later bounded proof accepted a public IPv6 serving peer.

- Challenge: Linux droplet builds are slow without cache.
  - Resolution: synced source to the droplet and reused its existing Linux Cargo cache; macOS release binaries are not portable to the Linux proof host.
  - Remaining: document any future distributable cache recipe separately if build-time work becomes a product task.

## Dead Code and Obsolescence Cleanup

- Inspected the IPv6 branch for proof-only leftovers; no temporary scripts, binaries, data dirs, resolver overrides, or firewall rules are intended to remain on the droplet after bounded tests.
- Confirmed the latest droplet proof cleanup left no LogEx process, owner IPv4 block, or resolver override active.
- Repaired the temporary droplet Linux binary after an invalid macOS binary upload was detected during a trace probe.
- Replaced the obsolete family-agnostic CL bootnode/cache seeding path with dial-family-aware helpers.
- Replaced the submitted-dial timestamp-only map with a small `SubmittedDial` record so expired direct candidates can be retried instead of discarded.
- Replaced the imported direct-record-only DNS update shape with the local optional-direct-record representation; no additional production code was identified as safe to remove in this run.
- Replaced the direct Reth DNS resolver use in the family-aware execution path with a local wrapper that preserves chunked TXT records; the default family-agnostic Reth path was not reintroduced.
- Reverted the strict IPv6 discv4 experiment after it failed to improve public EL discovery.

## Git Workflow

- Current branch: `fix/ipv6-p2p-sync`.
- New branch created this run: no; continued the existing IPv6 validation branch.
- Commits made this run: `docs: record ipv6-only validation`; `docs: record strict ipv6 public proof`; `docs: update ipv6 branch workflow`; `docs: record strict ipv6 runtime proof`; `docs: record ipv6 validation checks`; `fix: join dns txt chunks for ipv6 discovery`.
- Pull request status: draft PR #98 created at https://github.com/tdenisenko/logex/pull/98.
- Remote branch status: `fix/ipv6-p2p-sync` is pushed to `origin`.
- Merge status: not merged.
- Blockers: GitHub Actions quota has previously blocked hosted validation.

## Known Issues or Risks

- Pure IPv6 EL historical sync works, but public IPv6 execution peers are sparse; IPv4 or dual-stack mode will usually retain more peers and perform better.
- True simultaneous IPv4 and IPv6 inbound identity is not implemented yet.
- Future benchmark comparisons must record routing mode, peer counts, and whether traffic is routed through the VPS, dashboard-only WireGuard, or local networking.
- Do not leave long-running full syncs on temporary proof droplets unless the user explicitly asks for that test.
