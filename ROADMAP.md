# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Active branch: `fix/ipv6-p2p-sync`.

Historical EL sync has completed a post-fix pivot-to-genesis baseline on the Mac mini and no longer shows the two-hour liveness stall that previously blocked ordered writes. The accepted scheduler keeps expected historical work active, limits background maintenance while history is incomplete, and was bounded by the available network during the last validated run.

IPv6-only mode is functionally validated on temporary droplet `root@152.42.222.119` with public IPv6 `2400:6180:0:d2:0:2:fa9c:1000`. Strict IPv6 CL networking works with IPv6 DNS, `--grpc-host ::1`, IPv6-only P2P bind/dial settings, and an owner firewall rejecting IPv4 egress for the LogEx runtime user. Public mainnet strict IPv6 EL sync works when reliable IPv6 execution bootnodes/known peers are available: the latest bounded proof opened zero IPv4 sockets, listened/advertised/dialed only IPv6, accepted one serving public EL peer, tracked live head, moved historical sync backward, and persisted the proven IPv6 enode to `known-peers.json`. Public DNS-only EL discovery remains sparse and non-deterministic, so the README now recommends reliable IPv6 EL bootnodes or a warmed known-peer cache for production strict IPv6 runs.

Default dual-stack startup now keeps execution on the preferred public IPv4 path while allowing consensus to advertise IPv6 when a public IPv6 route is also available. This fixed the droplet default-mode stall where EL waited for CL indefinitely: the post-fix bounded smoke reached `Syncing`, CL peaked at 29 active sessions, and EL accepted 13 sessions while execution still advertised IPv4.

Public mainnet EL peer availability over strict IPv6 is sparse, so performance is expected to be lower and startup may take longer than IPv4 or dual-stack mode. A corrected official `ethereum/discv4-dns-lists` snapshot audit found 594 IPv6 execution ENRs and only 14 open IPv6 TCP endpoints from the droplet, and the latest post-resolver bounded proofs submitted thousands of strict-IPv6 EL dials without an accepted serving session. Earlier bounded proof windows did show public IPv6 EL sessions are possible, but startup is not deterministic enough to claim IPv4-like performance from public discovery alone. LogEx keeps sparse submitted EL dial candidates alive across silent timeout windows, and DNS ENRs without direct TCP fields are retained for signed IPv6 discovery when they carry `udp6`.

Temporary IPv6 droplet clients are stopped after bounded proof windows. Do not leave a full sync running on that droplet unless an active test requires it.

## Completed Since Last Run

- Fixed execution known-peer persistence for submitted outbound dials. Successfully connected peers from `pending_dials` were previously treated as not restart-dialable because session establishment checked only the pending queue.
- Changed the peer-cache baseline so configured execution bootnodes are not treated as already persisted; a configured bootnode is written to `known-peers.json` only after it proves reachable.
- Rebuilt the temporary IPv6 droplet binary from the patched source and ran bounded strict IPv6-only proofs with owner-level IPv4 egress blocked.
- Confirmed strict IPv6-only public EL sync with audited IPv6 enode bootnodes: LogEx opened zero IPv4 sockets, listened/advertised/dialed only IPv6, accepted one serving public EL peer, tracked live head, moved historical sync backward, and persisted one proven IPv6 enode to `known-peers.json`.
- Reconfirmed cleanup after the latest bounded IPv6 test: no LogEx process, LogEx socket, owner IPv4 reject rule, or temporary data directory remained active on the droplet.
- Documented the production strict-IPv6 operating mode in `README.md`: automatic family selection, outbound-only known-peer fallback, strict IPv6 command shape, sparse public EL IPv6 discovery, and the recommendation to use reliable IPv6 EL bootnodes or a warmed known-peer cache.
- Updated the strict IPv6 startup warning to mention that proven serving peers are cached for restart.
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
- Re-ran a 15-minute strict IPv6 official-ENR proof after the resolver fix: LogEx opened zero IPv4 sockets, CL reached 88 active sessions and 2,316 dialable peers, EL accepted 143 IPv6 DNS candidates and submitted 4,030 EL dials, but no public EL session was accepted.
- Re-ran a 10-minute strict IPv6 proof with the audited TCP-open IPv6 ENR set: LogEx opened zero IPv4 sockets and submitted 1,243 EL dials, but no public EL session was accepted in that window.
- Re-tested enabling Reth discv4 under strict IPv6 after the DNS resolver fix: it opened zero IPv4 sockets and submitted 2,653 EL dials, but accepted no EL session, so the experiment was reverted.
- Installed geth 1.17.4 on the temporary droplet and ran two bounded IPv6-only comparison windows with IPv4 egress blocked for the geth user. Geth formed zero EL peers with default mainnet bootnodes and also zero EL peers with the audited IPv6 ENRs converted to `enode://` bootnodes.
- Confirmed the temporary droplet proof state was cleaned up after bounded tests and rechecked it after the final proof summary: no LogEx process, owner IPv4 reject rule, or resolver override remained.
- Re-ran focused local regression checks for IPv6 P2P selection, consensus family selection, execution peer-manager DNS/bootnode/retry handling, and formatting.
- Added advanced dashboard diagnostics for P2P address mode, listen/dial/advertised address families, startup P2P warnings, and execution bootstrap warnings.
- Added REST coverage to ensure execution bootstrap warnings are serialized in `/status`.
- Reconfirmed the temporary IPv6 droplet is clean after bounded tests: no long-running LogEx/geth process, owner IPv4 reject rule, or P2P/dashboard listener remains active.
- Audited Reth 1.11.3 execution networking for true dual-stack inbound support. The current Reth integration exposes one RLPx TCP listener and one advertised local execution node record, and Reth's discv5 dual-stack conversion path explicitly leaves RLPx dual-stack unimplemented. This means LogEx can safely dial both families today, but true simultaneous IPv4+IPv6 advertised EL inbound requires a larger composite network-manager design or upstream Reth support.
- Made outbound-only startup report when no persisted execution known peers are available, and log when outbound-only mode can seed from persisted known peers.
- Added runtime tests for outbound-only known-peer fallback warnings.
- Re-ran the strict public IPv6 proof with the Stakely checkpoint endpoint and owner-level IPv4 egress blocked: LogEx listened, advertised, and dialed only IPv6; CL peaked at 52 active sessions and 3,037 dialable peers; EL submitted 890 IPv6 dials but accepted no public serving session.
- Fixed the controlled IPv6 proof harness after it reused the default execution discovery UDP port, then re-ran it successfully: seed and client established execution sessions over IPv6, logged `eth/70`, and opened zero IPv4 sockets.
- Added execution bootnode accounting to `/status` and the advanced dashboard so strict IPv6 operators can distinguish usable configured bootnodes from records rejected by the selected address family.
- Added a bootstrap warning for the case where configured execution bootnodes were provided but none match the active P2P family.
- Re-ran a fresh 10-sample strict public IPv6 smoke on the temporary droplet: LogEx opened zero IPv4 sockets, CL peaked at 81 active sessions and 3,078 dialable peers, EL accepted 144 DNS candidates and submitted 903 IPv6 dials, but no public EL session was accepted.
- Reconfirmed the temporary IPv6 droplet cleanup after that smoke: no LogEx process, P2P/dashboard listener, or owner IPv4 reject rule remained active.
- Probed 40 DNS-discovered IPv6 execution TCP endpoints from the droplet; all 40 direct IPv6 TCP connects failed, matching the repeated public strict-IPv6 EL session failures.
- Re-ran a short strict public IPv6 smoke on the temporary droplet: LogEx listened, advertised, and dialed only IPv6, opened zero IPv4 sockets, CL reached 14 active sessions and 452 dialable peers, EL accepted 46 DNS candidates and submitted 52 dials, but accepted no public EL session.
- Reconfirmed cleanup after that smoke: no LogEx process, P2P/dashboard listener, or owner IPv4 reject rule remained active.
- Refreshed the temporary IPv6 droplet source from the current branch, rebuilt the Linux release binary with the existing droplet Cargo cache, and reran bounded IPv6-only proofs.
- Re-ran the controlled two-node IPv6 EL proof on the refreshed binary: the client recorded one `eth/70` execution session over `[2400:6180:0:d2:0:2:fa9c:1000]` with zero IPv4 sockets, while the data-less seed immediately disconnected it as not useful for serving.
- Re-ran a four-minute strict public IPv6 smoke on the refreshed binary: LogEx opened zero IPv4 sockets, CL reached 13 active sessions and 510 dialable peers, EL accepted 143 IPv6 DNS candidates and submitted 875 strict-IPv6 dials, but no public EL serving session was accepted.
- Reconfirmed cleanup after the refreshed proof: no LogEx process, P2P/dashboard listener, or owner IPv4 reject rule remained active.
- Re-ran a checkpoint-enabled strict public IPv6 smoke on the refreshed binary: LogEx resolved the Stakely checkpoint, bootstrapped CL to execution block 25,433,190, opened zero IPv4 sockets, reached 92 active CL sessions and 2,804 CL dialable peers, accepted 144 IPv6 DNS EL candidates, and submitted 902 strict-IPv6 EL dials with no public EL serving session accepted.
- Reconfirmed cleanup after the checkpoint-enabled proof: no LogEx process, P2P/dashboard listener, or owner IPv4 reject rule remained active.
- Cleared stale temporary IPv4 input reject rules left from earlier droplet experiments so subsequent default or dual-stack comparisons are not contaminated by old proof harness state.
- Re-ran a checkpoint-enabled strict IPv6 smoke on the refreshed binary: LogEx resolved the Stakely checkpoint, listened/advertised/dialed only IPv6, opened zero IPv4 sockets, reached 85 active CL sessions and 3,726 CL dialable peers, accepted 144 IPv6 DNS EL candidates, accepted one public Geth `eth/70` serving peer over IPv6, and moved historical sync with a low but non-zero rate.
- Re-ran a 12-minute checkpoint-enabled strict IPv6 proof with 62 audited IPv6 ENRs passed as configured execution bootnodes: LogEx accepted all 62 as direct candidates, opened zero IPv4 sockets, reached 96 active CL sessions and 3,833 CL dialable peers, submitted 2,772 strict-IPv6 EL dials, but accepted no public EL serving session.
- Re-ran the built-in DNS strict IPv6 checkpoint smoke for eight minutes: LogEx opened zero IPv4 sockets, reached 94 active CL sessions and 3,724 CL dialable peers, accepted 143 IPv6 DNS EL candidates, submitted 876 strict-IPv6 EL dials, but accepted no public EL serving session.
- Reconfirmed cleanup after the latest bounded IPv6 tests: no LogEx process, LogEx socket, owner IPv4 reject rule, or temporary input reject rule remained active on the droplet.
- Rechecked the temporary IPv6 droplet after the latest branch update: no LogEx process, P2P/dashboard listener, or temporary LogEx data directory remained under `/root`.
- Clarified runtime, `/status` fixture, and README dual-stack wording so automatic mode is described as one advertised execution family plus dual-family outbound dialing, not true simultaneous EL IPv4+IPv6 inbound.
- Changed dual-family DNS direct candidate selection so ENRs with both IPv4 and IPv6 endpoints are spread deterministically across both families instead of always collapsing to IPv4 after the initial seed set.

## Remaining TODOs

1. Decide the execution-layer dual-stack inbound architecture.
   - Reason: the current automatic selection advertises one EL address family and dials both routed families. Reth 1.11.3's network manager binds one RLPx TCP listener and one advertised local node record; its discv5 dual-stack path leaves RLPx dual-stack conversion unimplemented. Forcing dual-stack through that API would be fragile and could misadvertise reachability.
   - Completion criteria: choose and complete one path: keep the single-advertised-family EL design as intentional for this branch; implement a composite/two-manager EL network layer that safely advertises IPv4 and IPv6 inbound identities; or move to an upstream Reth version/API that supports true dual-stack RLPx.

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

- Configured execution bootnodes are surfaced with accepted and family-rejected counts.
  - Why: strict IPv6 should not silently suppress the no-bootnode warning just because IPv4-only or otherwise incompatible records were supplied.
  - Tradeoff: this adds diagnostic fields to status/dashboard without changing peer selection behavior.

- DNS discovery events are retained even when they do not have a direct TCP endpoint.
  - Why: an IPv6 ENR with `udp6` but no `tcp6` cannot be dialed directly, but it can still seed signed discv5 and expand discovery.
  - Tradeoff: such records must remain optional direct candidates, so logging and tests now handle DNS updates without a `NodeRecord`.

- Family-aware execution DNS discovery uses a TXT-joining resolver and periodic tree re-bootstrap.
  - Why: mainnet EIP-1459 branch records are often split into multiple DNS TXT chunks, and using only the first chunk silently drops most IPv6 candidates; a transient empty root lookup also should not permanently disable DNS discovery for the run.
  - Tradeoff: this keeps one small resolver wrapper in LogEx instead of relying directly on Reth's default resolver behavior.

- Dual-family DNS direct dialing spreads dual-endpoint ENRs across IPv4 and IPv6.
  - Why: automatic dual-stack mode should exercise both outbound families when both routes are usable; always selecting the IPv4 endpoint from a dual-endpoint ENR underused IPv6 after the initial DNS seed collection.
  - Tradeoff: each Reth execution peer id still maps to one pending direct dial at a time, so this is deterministic spreading across peers rather than simultaneous IPv4 and IPv6 dials to the same peer.

- Consensus bootnodes and cached peers are filtered by dial family before discovery seeding.
  - Why: strict IPv6 mode should not seed IPv4-only ENRs or retain cached peers without a compatible dial address.
  - Tradeoff: status `bootnode_count` now means compatible seeded bootnodes, not total built-in bootnode records.

- Sparse execution dial candidates are retained across silent submitted-dial expiry.
  - Why: IPv4 discovery has enough candidates to mask failed waves, but strict IPv6 has a small public candidate set and must keep retrying instead of dropping all candidates after one timeout.
  - Tradeoff: unreachable public IPv6 endpoints will be retried periodically until better peers are found or explicit bootnodes are configured.

- Outbound-only nodes surface empty known-peer cache state at startup.
  - Why: when no usable public address exists, LogEx should behave like common EL clients by seeding from persisted peers when possible; an empty peer cache is operationally different from a populated fallback cache.
  - Tradeoff: fresh outbound-only data directories now show one extra startup/dashboard warning until serving peers are learned and persisted.

- True EL dual-stack inbound was not forced through the current Reth API.
  - Why: Reth 1.11.3 exposes one RLPx TCP listener and one advertised local node record through `NetworkManager`, and its `reth_discv5::Discv5::try_into_reachable` branch for `IpMode::DualStack` is explicitly unimplemented.
  - Alternatives considered: bind LogEx execution to IPv6 wildcard while advertising IPv4, or inject custom ENR fields. Both risk inaccurate reachability and platform-specific socket behavior.
  - Tradeoff: LogEx currently uses the production-safe behavior: prefer public IPv4 for EL when available, fall back to public IPv6 when IPv4 is not usable, and dial both routed families when possible. True simultaneous EL inbound needs a dedicated architecture change.

## Challenges and Resolutions

- Challenge: public EL IPv6 peers were effectively unavailable from the test droplet.
  - Resolution: compared LogEx behavior with geth, audited major client source, proved controlled IPv6 EL transport with explicit bootnodes, confirmed the current Reth static mainnet execution bootnodes are IPv4-only, kept explicit IPv6 startup warnings visible in status/dashboard, and finally proved public mainnet EL sync through audited IPv6 enode bootnodes with zero IPv4 sockets.
  - Remaining: current public IPv6 EL discovery is not deterministic; DNS-only strict IPv6 should be documented as best-effort unless reliable IPv6 execution bootnodes or a warmed known-peer cache are available.

- Challenge: strict IPv6 accepted EL peers were not persisted for restart.
  - Resolution: session establishment now recovers the original `SubmittedDial` node record, and configured bootnodes are persisted only after a reachable session proves them useful. The latest proof persisted one public IPv6 enode after it served.
  - Remaining: no code issue remains in the observed restart-cache path.

- Challenge: strict IPv6 with incompatible configured bootnodes was hard to diagnose.
  - Resolution: added execution bootnode counters for accepted direct candidates, accepted signed discovery ENRs, and family rejections, plus a bootstrap warning when all configured bootnodes are rejected by the active family.
  - Remaining: this improves operator feedback but does not solve public IPv6 EL peer scarcity.

- Challenge: the controlled two-node IPv6 EL proof initially failed after both nodes reused the default execution discovery UDP port.
  - Resolution: gave the seed and client distinct execution discovery ports, reran the proof, and confirmed both nodes established IPv6 execution sessions with `eth/70` log mentions and zero IPv4 sockets.
  - Remaining: no harness blocker remains for controlled EL transport validation.

- Challenge: true simultaneous EL IPv4+IPv6 inbound is not a small configuration change in the current Reth integration.
  - Resolution: audited the local Reth 1.11.3 source and confirmed `NetworkManager` creates one `ConnectionListener`, `NetworkHandle::local_enr()` serializes one family from one `NodeRecord`, and `reth_discv5` has an explicit unimplemented dual-stack RLPx conversion branch.
  - Remaining: a product/architecture decision is needed before implementing a composite execution network manager or declaring single-advertised-family EL behavior intentional.

- Challenge: outbound-only fallback was technically present but not visible enough in status.
  - Resolution: moved known-peer loading before initial status publication, added an empty-cache warning, and kept populated-cache startup as an info log.
  - Remaining: no code blocker remains for the current known-peer fallback behavior; fully validating it still requires a restart test with a populated peer cache.

- Challenge: default dual-stack startup preferred IPv4 for both EL and CL, but CL stayed at zero active sessions for six minutes on the IPv6 droplet.
  - Resolution: selected the CL address independently so EL keeps public IPv4 while CL uses public IPv6 when both routes are available; the post-fix smoke reached live `Syncing`.
  - Remaining: no observed default-mode blocker remains; strict public IPv6 EL is functionally proven but remains peer-scarcity limited.

- Challenge: public IPv6 probes found many TCP-open sockets that still did not become serving EL peers.
  - Resolution: traced Reth discv5 handling and confirmed it may use the discovery UDP port as a guessed RLPx TCP port when `tcp6` is missing; this is useful for compatibility but noisy under strict IPv6 scarcity.
  - Remaining: no correctness blocker remains; sparse/slow public IPv6 EL discovery is still an operational caveat.

- Challenge: enabling Reth discv4 on strict IPv6 execution bind was a plausible missing discovery path because Reth's discv4 codec supports IPv6 endpoints.
  - Resolution: tested it before and after the DNS resolver fix in bounded strict IPv6 public smokes on the droplet; the latest test submitted 2,653 EL dials and accepted no EL session, so the experiment was reverted.
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
- Confirmed the temporary enode proof harness removed its data directory and left no LogEx process, socket, or owner IPv4 reject rule active.
- Inspected README and CLI help coverage for IPv6 operation; added the missing strict IPv6 production recipe rather than duplicating proof-only harness details.
- Confirmed the latest droplet proof cleanup left no LogEx process, owner IPv4 block, or resolver override active.
- Confirmed the geth comparison cleanup left no geth process or owner IPv4 block active.
- Repaired the temporary droplet Linux binary after an invalid macOS binary upload was detected during a bounded trace probe.
- Replaced the obsolete family-agnostic CL bootnode/cache seeding path with dial-family-aware helpers.
- Replaced the submitted-dial timestamp-only map with a small `SubmittedDial` record so expired direct candidates can be retried instead of discarded.
- Replaced the imported direct-record-only DNS update shape with the local optional-direct-record representation; no additional production code was identified as safe to remove in this run.
- Replaced the direct Reth DNS resolver use in the family-aware execution path with a local wrapper that preserves chunked TXT records; the default family-agnostic Reth path was not reintroduced.
- Reverted the strict IPv6 discv4 experiment again after the post-resolver smoke also failed to improve public EL discovery.
- Surfaced existing P2P and execution bootstrap warnings in the dashboard advanced metrics instead of leaving them available only through raw `/status`.
- Added configured execution bootnode counters to the existing execution network status path; no obsolete dashboard fields were removed.
- Audited Reth's execution network, listener, node record, and discv5 dual-stack paths; no safe dead code removal followed from that audit.
- Inspected the outbound-only known-peer fallback path and kept the existing dial-family filtering; no obsolete code was identified there.
- Inspected the dual-family DNS candidate path and replaced the obsolete IPv4-first assumption for dual-endpoint ENRs with deterministic family spreading.

## Git Workflow

- Current branch: `fix/ipv6-p2p-sync`.
- New branch created this run: no; continued the existing IPv6 validation branch.
- Commits made this run: `docs: record ipv6-only validation`; `docs: record strict ipv6 public proof`; `docs: update ipv6 branch workflow`; `docs: record strict ipv6 runtime proof`; `docs: record ipv6 validation checks`; `fix: join dns txt chunks for ipv6 discovery`; `docs: record ipv6 peer scarcity proof`; `docs: record geth ipv6 peer comparison`; `fix: surface p2p bootstrap warnings`; `docs: record dual-stack execution audit`; `fix: report outbound-only known-peer fallback`; `docs: record current ipv6 proof status`; `fix: expose execution bootnode family rejections`; `docs: record latest strict ipv6 public smoke`; `docs: record refreshed ipv6 proof`; `docs: record checkpoint ipv6 proof`; `docs: record latest ipv6 droplet proof`; `fix: persist ipv6 execution peers after submitted dials`; `docs: document strict ipv6 production mode`; `fix: clarify dual-stack p2p warnings`; `fix: spread dual-stack dns candidates`.
- Pull request status: draft PR #98 created at https://github.com/tdenisenko/logex/pull/98.
- Remote branch status: `fix/ipv6-p2p-sync` is pushed to `origin`.
- Merge status: not merged.
- Blockers: GitHub Actions quota has previously blocked hosted validation.

## Known Issues or Risks

- Pure IPv6 EL transport works and can sync when an EL session is accepted, but public IPv6 execution peers are sparse and acceptance is not deterministic; IPv4 or dual-stack mode will usually retain more peers and perform better.
- True simultaneous IPv4 and IPv6 EL inbound identity is not implemented yet; current Reth 1.11.3 integration makes this a composite-network or upstream-support task rather than a small config change.
- Future benchmark comparisons must record routing mode, peer counts, and whether traffic is routed through the VPS, dashboard-only WireGuard, or local networking.
- Do not leave long-running full syncs on temporary proof droplets unless the user explicitly asks for that test.
