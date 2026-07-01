# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Active branch: `fix/ipv6-p2p-sync`.

The historical execution sync scheduler work from PR #97 is ready once its rerun GitHub checks pass. The accepted scheduler keeps expected historical work active, reduces ordered-write stalls, and defers maintenance work that previously caused long history-sync pauses. A full post-fix baseline completed on the Mac mini with the remaining throughput bounded by the available network rather than by the earlier two-hour liveness stall.

The IPv6 P2P work in PR #98 is production-ready for merge with one documented operational caveat: strict public IPv6 execution peers are sparse on mainnet, so DNS-only IPv6 EL discovery may start slowly or fail to find a serving peer in short windows. The implementation itself is useful and not proof-only. It adds automatic P2P address-family selection, strict IPv6 EL/CL support, IPv6 execution DNS/discv5 bootstrapping, signed ENR and hostname bootnode support, known-peer fallback diagnostics, and dashboard/status visibility.

Default startup behavior is:

- Prefer a usable locally owned public IPv4 address for execution P2P when available.
- Fall back to a usable locally owned public IPv6 address when IPv4 is unavailable or not reachable.
- Dial both routed outbound families where possible, while advertising one execution inbound family because the current Reth network manager exposes one RLPx listener and one execution node record.
- Run outbound-only from persisted known peers, bootnodes, and DNS discovery when no public address is usable.
- Let consensus use IPv6 independently on dual-stack hosts when that is the healthier beacon-network path.

Temporary IPv6 droplet tests have been stopped. Do not keep a full sync running on temporary proof infrastructure unless an active test explicitly requires it.

## Completed Since Last Run

- Audited the IPv6 branch for production relevance and normal IPv4 impact.
- Confirmed the branch contains production code paths rather than temporary proof harnesses: CLI/config options, runtime address-family selection, CL dial-family filtering, EL DNS/discv5 bootstrap support, signed ENR parsing, known-peer fallback, and status/dashboard diagnostics.
- Confirmed no geth/Nethermind static bootnode list was copied into defaults because bounded testing showed it did not improve public strict-IPv6 EL peer acceptance.
- Condensed the roadmap to keep the merge handoff focused on current behavior, remaining risks, and the PR readiness state.

## Remaining TODOs

- Merge PR #97 after its rerun GitHub checks pass.
  - Reason: PR #98 is stacked on the historical sync scheduler work and should not merge before its base PR is green.
  - Completion criteria: PR #97 format, check, clippy, and test jobs pass; PR #97 is merged into `master`.

- Merge PR #98 after PR #97 lands and PR #98 checks pass on the final head.
  - Reason: the IPv6 work is production-useful but should enter `master` only after the stacked base is merged and CI validates the final branch state.
  - Completion criteria: PR #98 is marked ready, all required checks pass, the branch is mergeable, and the PR is merged into `master`.

- Treat strict DNS-only public IPv6 EL discovery as best-effort.
  - Reason: LogEx can establish EL sessions over IPv6 and stays socket-clean in strict IPv6 mode, but public mainnet IPv6 execution peers are sparse and not deterministic from short bounded windows.
  - Completion criteria: documentation keeps this caveat clear; operators can provide reliable IPv6 execution bootnodes or rely on a warmed known-peer cache for deterministic strict IPv6 startup.

## Design Decisions

- Automatic execution P2P prefers public IPv4 before public IPv6.
  - Why: the execution network is still much denser on IPv4, and IPv4 remains the best default when it is publicly usable.
  - Alternatives considered: always prefer IPv6 on dual-stack hosts, or advertise both IPv4 and IPv6. Always preferring IPv6 reduced EL peer availability; advertising both requires a larger Reth network-manager architecture change.
  - Tradeoff: dual-stack hosts advertise one execution family, but still dial both outbound families where routes exist.

- Consensus P2P can choose IPv6 even when execution advertises IPv4.
  - Why: beacon peers were healthier over IPv6 in droplet testing, while execution peers were healthier over IPv4.
  - Alternatives considered: force CL to use the execution family. That made dual-stack startup less reliable.
  - Tradeoff: status must report EL and CL networking separately, which the dashboard and `/status` now do.

- Strict IPv6 execution binds disable Reth discv4 and use family-aware DNS/discv5 seeding.
  - Why: strict IPv6 mode must not leak IPv4 sockets, and Reth's default execution bootnodes are IPv4-oriented.
  - Alternatives considered: keep discv4 enabled for IPv6 binds. Bounded testing did not improve public EL acceptance, so that experiment was not kept.
  - Tradeoff: strict IPv6 relies on signed ENRs, IPv6 DNS candidates, explicit IPv6 bootnodes, and known-peer persistence.

- Known-peer persistence includes proven dialable peers, not only fully productive serving peers.
  - Why: sparse-family and outbound-only modes need a restart seed cache once a peer proves it can complete an Ethereum handshake.
  - Alternatives considered: persist only data-serving peers. That was stricter but weakened restart recovery in sparse IPv6 environments.
  - Tradeoff: peers are still filtered by family, nonzero tip, quarantine state, and bootstrap-node status before being retained.

- Public address-family detection uses short outbound TCP reachability probes.
  - Why: a configured route or interface address does not prove that the family is usable for public P2P.
  - Alternatives considered: inspect local interfaces or UDP route selection only. Those methods misclassified blocked IPv4 paths during testing.
  - Tradeoff: unusual networks may need explicit `--nat extip:<ip>` and `--p2p-bind-ip` overrides if probe targets are blocked but Ethereum P2P is still usable.

## Challenges and Resolutions

- Challenge: public strict-IPv6 EL peers are sparse and intermittent.
  - Resolution: proved controlled IPv6 EL transport, observed successful public IPv6 EL sessions in bounded windows, compared with geth behavior, audited geth/Nethermind bootnode sources, added better DNS/discv5 handling, and documented the remaining peer-availability caveat.
  - Remaining: deterministic strict IPv6 EL startup requires reliable IPv6 execution bootnodes or a warmed known-peer cache.

- Challenge: true simultaneous EL IPv4+IPv6 inbound is not exposed by the current Reth 1.11.3 integration.
  - Resolution: kept the production-safe one-advertised-family behavior and documented that true dual inbound requires a future composite execution network manager or upstream Reth support.
  - Remaining: no blocker for automatic IPv4-to-IPv6 fallback or strict IPv6 operation.

- Challenge: strict IPv6 could lose sparse candidates after silent dial expiry.
  - Resolution: submitted dials retain the original `NodeRecord` and are requeued after the suppression interval when still eligible.
  - Remaining: no correctness blocker remains in the observed retry path.

- Challenge: checkpoint endpoints varied in supported Beacon API shapes.
  - Resolution: checkpoint resolution now uses a multi-source default quorum and fallback Beacon API shapes for finalized checkpoints.
  - Remaining: no known startup blocker remains for the tested default sources.

## Dead Code and Obsolescence Cleanup

- Inspected the IPv6 branch diff across runtime selection, CL networking, EL peer management, status serialization, dashboard fields, CLI/config options, and dependency additions.
- No temporary proof scripts, droplet paths, hardcoded test IPs, firewall rules, or one-off bootnode lists remain in production code.
- The added `reth-discv5` dependency is production-relevant for signed execution ENR/discv5 support. The `enr` dev-dependency is used only by tests.
- The roadmap itself was cleaned up by replacing repeated proof-run logs with concise production conclusions and remaining risks.
- No additional safe code removal was identified before merge.

## Git Workflow

- Current branch: `fix/ipv6-p2p-sync`.
- Pull requests:
  - PR #97: `perf/fresh-historical-baseline`, ready after rerun checks pass.
  - PR #98: `fix/ipv6-p2p-sync`, draft until PR #97 lands and final checks are confirmed.
- Commits made during this cleanup pass: `docs: clean up ipv6 merge roadmap`.
- Merge status: not merged yet.
- Blockers: PR #97 test job was still running when this cleanup began; no code blocker is known.

## Known Issues or Risks

- Pure IPv6 EL transport works, but public IPv6 execution peer availability is sparse. IPv4 or dual-stack mode will usually retain more peers and perform better.
- True simultaneous IPv4 and IPv6 EL inbound identity is future architecture work; current behavior is one advertised EL family with dual-family outbound dialing where possible.
- Future benchmark comparisons must record routing mode, peer counts, and whether traffic is routed through VPS, dashboard-only WireGuard, or local networking.
