# Roadmap

## Current Status

LogEx starts from a recent consensus checkpoint, tracks the live head, reverse-syncs execution history toward genesis, stores compressed verified logs, and serves the dashboard, SQL query API, JSON-RPC, gRPC, and live ERC20 transfer subscriptions.

Active branch: `fix/ipv6-p2p-sync`.

Historical EL sync has completed a post-fix pivot-to-genesis baseline on the Mac mini and no longer shows the two-hour liveness stall that previously blocked ordered writes. The accepted scheduler keeps expected historical work active, limits background maintenance while history is incomplete, and was bounded by the available network during the last validated run.

IPv6 validation is partially complete on temporary droplet `root@152.42.222.119` with public IPv6 `2400:6180:0:d2:0:2:fa9c:1000`. Strict IPv6 CL networking works: bounded proofs used IPv6 DNS, `--grpc-host ::1`, IPv6-only P2P bind/dial settings, and an owner firewall rejecting IPv4 egress for the LogEx runtime user; samples reached healthy CL session counts with zero LogEx IPv4 sockets. EL IPv6 transport also works in controlled conditions: two LogEx nodes established Eth70 execution sessions over an explicit IPv6 `enode://` bootnode while IPv4 egress was blocked.

Public mainnet EL historical sync over strict IPv6 is not proven. The public execution DNS tree exposes very few usable IPv6 peers from the droplet; repeated LogEx proofs expired all public IPv6 EL dials, and a bounded official geth comparison also formed zero peers under the same IPv6-only conditions. Local audits of geth, Reth, and Nethermind did not reveal a missed official IPv6 EL bootnode source.

Temporary IPv6 droplet clients are stopped after bounded proof windows. Do not leave a full sync running on that droplet unless an active test requires it.

## Completed Since Last Run

- Audited geth, Reth, and Nethermind source for execution-layer IPv6 bootnodes, DNS discovery, ENR endpoint handling, and peer admission behavior.
- Re-ran bounded strict IPv6 droplet checks: CL remained healthy over IPv6-only sockets; public EL IPv6 candidates still expired without accepted sessions; controlled explicit IPv6 EL bootnodes established Eth70 sessions.
- Filtered consensus bootnode and cached-peer seeding by configured dial family so strict IPv6 mode no longer inserts IPv4-only CL ENRs into discovery or cached dial targets.
- Verified the family-filter change with focused `logex-cl` tests, `cargo test -p logex-cl`, `cargo test -p logex-node p2p_selection`, `cargo check -p logex-cl -p logex-node`, `cargo clippy -p logex-cl -p logex-node -- -D warnings`, and `cargo fmt --all -- --check`.
- Confirmed the temporary droplet proof state was cleaned up: no LogEx process, P2P/dashboard listener, owner IPv4 reject rule, or resolver override remained after testing.

## Remaining TODOs

1. Decide the production policy for strict IPv6-only EL sync.
   - Reason: strict IPv6 CL works and EL transport works with explicit IPv6 peers, but public mainnet IPv6 EL peer availability is not sufficient for proven historical sync.
   - Completion criteria: either demonstrate historical EL progress using only IPv6 sockets against reliable public IPv6 execution peers, or deliberately scope strict IPv6 EL as an advanced explicit-bootnode mode while default startup uses IPv4, dual-family, or outbound known-peer paths when available.

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

- Execution bootnodes support explicit `enode://` and signed `enr:` records, including hostname resolution for enodes.
  - Why: public IPv6 EL discovery is sparse, so operators need a deterministic way to provide known IPv6 peers.
  - Tradeoff: signed discv5 ENRs still require same-family UDP fields, while direct RLPx candidates may use geth/Nethermind-compatible generic TCP fallback.

- Consensus bootnodes and cached peers are filtered by dial family before discovery seeding.
  - Why: strict IPv6 mode should not seed IPv4-only ENRs or retain cached peers without a compatible dial address.
  - Tradeoff: status `bootnode_count` now means compatible seeded bootnodes, not total built-in bootnode records.

## Challenges and Resolutions

- Challenge: public EL IPv6 peers were effectively unavailable from the test droplet.
  - Resolution: compared LogEx behavior with geth, audited major client source, and proved controlled IPv6 EL transport with explicit bootnodes.
  - Remaining: public strict IPv6 historical EL sync is still unproven.

- Challenge: strict IPv6 CL runs still saw family-incompatible bootnode/discovery paths.
  - Resolution: CL bootnodes and cached peers are now retained only when they have addresses compatible with the configured dial families.
  - Remaining: rerun a short droplet smoke after this specific CL filter if a final public proof is required.

- Challenge: Linux droplet builds are slow without cache.
  - Resolution: local Cargo cache/source rsync was used for proof runs; generated caches should stay outside Git.
  - Remaining: document any future distributable cache recipe separately if build-time work becomes a product task.

## Dead Code and Obsolescence Cleanup

- Inspected the IPv6 branch for proof-only leftovers; no temporary scripts, binaries, data dirs, resolver overrides, or firewall rules are intended to remain on the droplet after bounded tests.
- Replaced the obsolete family-agnostic CL bootnode/cache seeding path with dial-family-aware helpers.
- No additional production code was identified as safe to remove in this run.

## Git Workflow

- Current branch: `fix/ipv6-p2p-sync`.
- New branch created this run: no; continued the existing IPv6 validation branch.
- Commits made this run: pending.
- Pull request status: not created; the task is not complete while public strict IPv6 EL sync policy remains unresolved.
- Merge status: not merged.
- Blockers: public IPv6 EL peer availability is unresolved; GitHub Actions quota has previously blocked hosted validation.

## Known Issues or Risks

- Pure IPv6 EL historical sync may be impractical on current public mainnet peer availability without a reliable IPv6 execution peer source.
- True simultaneous IPv4 and IPv6 inbound identity is not implemented yet.
- Future benchmark comparisons must record routing mode, peer counts, and whether traffic is routed through the VPS, dashboard-only WireGuard, or local networking.
- Do not leave long-running full syncs on temporary proof droplets unless the user explicitly asks for that test.
