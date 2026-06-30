# Roadmap

## Current Status

LogEx starts from a recent CL checkpoint, tracks the live execution head, reverse-syncs EL history toward genesis, stores compressed verified logs, and serves dashboard, SQL query, JSON-RPC, gRPC, and live ERC20 transfer subscription APIs.

Active branch: `fix/ipv6-p2p-sync`. When outside the home network, Mac mini operations must use `ssh -J pi-remote gremlinmaster@192.168.50.44`. The completed historical baseline used `/Users/gremlinmaster/logex-fresh-baseline-src`, data dir `/Volumes/SSD 4TB/LogEx`, HTTP port `18683`, tmux session `logex`, and run dir `/Users/gremlinmaster/logex-baseline-runs/fresh-baseline-20260628-142644`; the `logex-baseline-monitor` tmux session exited normally after writing `summary.json`.

Historical sync has completed the active post-fix pivot-to-genesis baseline. The accepted scheduler keeps the critical historical fetch active, buffers cheap ready fetch plans ahead of slow header windows, prioritizes missing expected fetches before lookahead prepare work, avoids blocking ordered writes on expensive post-write refill when prepared batches are already queued, forces a limited local-work refill while prepares or writes are waiting, discards same-sequence work planned against stale expected child headers, lets write-path refills fill the adaptive active pipeline, gives expected historical fetches full body/receipt lane budget while capping lookahead lane budget, adds bounded early redundancy for the first expected-prefix chunks, keeps the next fetch sequence cursor monotonic after ordered writes advance the expected cursor, preserves one missing execution-client-family probe when truncating the body/receipt candidate pool, skips expensive prefix salvage when the body/receipt live plan already has an acceptable contiguous prefix, and defers background compaction planning while historical sync is incomplete so maintenance scans cannot starve ordered historical writes. The post-fix run reached genesis without repeated liveness stalls, kept peers healthy, and showed median physical RX near the 300 Mbps link ceiling; another clean wall-clock benchmark is not required unless a new post-fix health problem appears.

IPv6 P2P validation is materially complete for socket and address-family behavior on DigitalOcean droplet `root@152.42.222.119` with LogEx bound to IPv6 address `2400:6180:0:d2:0:2:fa9c:1000`. CL discovery and libp2p work over strict IPv6: the latest public proof used the built-in checkpoint quorum, IPv6 DNS, `--grpc-host ::1`, and an owner firewall rejecting IPv4 egress for the LogEx runtime user; it reached 101 active CL sessions, 2,060 dialable CL peers, zero IPv4 LogEx sockets, and zero `::ffff:` IPv4-mapped dials. EL IPv6 transport is correct in controlled conditions: the current branch established Eth70 execution sessions between two LogEx instances over IPv6 only using both `enode://` and signed `enr:` execution bootnodes, with IPv4 egress blocked for the runtime user and zero matching IPv4 sockets. Signed ENR bootnodes are parsed, converted into family-compatible direct RLPx candidates when they advertise `ip6` plus either `tcp6` or generic `tcp`, and passed into Reth discv5 only when they advertise compatible UDP fields. DNS-discovered execution ENRs follow the same split: direct RLPx candidates use geth/Nethermind-compatible generic TCP/UDP fallback for IPv6 endpoints, while signed discv5 seeds still require same-family UDP so Reth does not abort startup with incompatible ENRs. LogEx now polls the Reth DNS discovery service directly, widens the DNS record cache for the current public tree size, and queues initial DNS IPv6 bootnodes as direct RLPx candidates instead of only seeding discovery. Public mainnet EL historical sync is still not proven because public IPv6 execution peers are effectively unavailable from the test droplet: a full geth DNS-tree crawl found 3,000 execution ENRs, 142 IPv6-capable ENRs, and only 2 TCP-reachable IPv6 endpoints; seeding those two explicit signed ENRs still produced zero accepted Eth sessions while 61 of 63 submitted dials expired in the shortened proof. A bounded official geth `v1.17.4` comparison on the same droplet also formed zero peers with IPv4 egress blocked; with `--netrestrict 2000::/3`, geth filtered every default execution bootstrap node as IPv4-only, and without netrestrict it kept trying IPv4 candidates but still reached zero peers. Default `--nat any` now selects locally owned public IPv4 first, locally owned public IPv6 second, and outbound-only otherwise. Automatic mode separates the advertised public address from outbound dial families: dual-public hosts advertise IPv4 while accepting IPv4 and IPv6 outbound candidates, and public-IPv6/private-IPv4 hosts advertise IPv6 while still accepting IPv4 outbound candidates. `/status` reports listen, advertised, and dial families separately so this behavior is explicit instead of implying true dual inbound support. Strict IPv6-only mode now reports a startup/status warning when no explicit IPv6 execution bootnodes are configured and also reports repeated dial-expiration warnings when configured or discovered execution candidates never become accepted sessions. The default checkpoint source is now a 2-of-3 IPv6-capable mainnet quorum, and checkpoint resolution supports Beacon headers, block/root fallback for any block id, and finalized state checkpoint endpoints. The temporary droplet clients are stopped after bounded proof windows, and the latest proof artifacts are removed after validation.

## Completed Since Last Run

- Added explicit P2P bind-family support through `--p2p-bind-ip` and config key `p2p_bind_ip`.
- Propagated IPv4/IPv6 bind and external addresses into both EL and CL P2P configuration.
- Updated CL ENR/listener/dial filtering so IPv6 mode advertises `ip6`/`tcp6`/`udp6`/`quic6` and dials only IPv6 multiaddrs.
- Updated EL P2P startup so IPv6 mode binds/listens on IPv6, filters known peers by address family, disables Reth's IPv4-oriented discovery paths, and uses a family-aware DNS discovery feed.
- Added focused tests for IPv6 CLI parsing, CL ENR/dial filtering, EL DNS candidate conversion, DNS crawl sizing, and consensus-head startup readiness.
- Deployed and tested the branch on IPv6-only remote runtime constraints: no `logexv6` IPv4 sockets were observed and no fresh IPv4 EL dial attempts remained after disabling Reth default DNS discovery.
- Confirmed CL P2P works over IPv6 on the droplet; EL receives IPv6 DNS candidates but did not establish a mainnet execution session during the observed windows.
- Re-enabled Reth discv4 for IPv6 mode with a family-aware IPv6 DNS bootnode pre-seed, while keeping Reth's default DNS conversion disabled so IPv4 ENR fields are not dialed in IPv6-only mode.
- Fixed stale EL submitted-dial accounting so expired failed dials are pruned even when the pending queue is empty.
- Added configurable execution discv5 support for IPv6 binds and exposed `--execution-discv5-port` so multiple LogEx instances can run on one IPv6 test host without UDP port conflicts.
- Added `--execution-bootnode`/`execution_bootnodes` support for explicit execution-layer enode seeds; configured bootnodes are parsed, bind-family filtered, added to known peers, and seeded into discovery/direct dialing.
- Completed a controlled IPv6-only EL transport proof on the droplet: a client and seed LogEx node used separate IPv6 EL/CL/discv5 ports, `logexv6` IPv4 egress was rejected, an Eth70 session stayed established over `[2400:6180:0:d2:0:2:fa9c:1000]:30304`, CL kept active IPv6 peers, and no IPv4 sockets remained.
- Stopped the temporary IPv6 droplet clients and removed the temporary IPv4 reject rule after the proof window; no full sync is left running there.
- Implemented default automatic P2P address-family selection for `--nat any`: locally owned public IPv4 is preferred, locally owned public IPv6 is the fallback, and hosts without a public local address use outbound-only mode instead of advertising an API-discovered NAT address they do not own.
- Verified the selector on the IPv6 droplet: default startup selected `auto-public-ipv4` with external IP `152.42.222.119`; forced IPv6 bind plus default `--nat any` selected `auto-public-ipv6` and advertised `[2400:6180:0:d2:0:2:fa9c:1000]`.
- Exposed P2P address selection in `/status` through `p2p_address_mode`, `p2p_bind_ip`, `p2p_external_ip`, and `p2p_warnings`, then verified the IPv6 droplet status response reported `auto-public-ipv6` with the droplet IPv6 address.
- Added execution peer-discovery counters to `/status` so IPv6-only runs show cumulative DNS candidates, family rejections, submitted dials, and expired submitted dials after live pending queues drain.
- Ran a 15 minute bounded public IPv6-only sample using IPv6-reachable checkpoint source `https://ethereum-beacon-api.publicnode.com`; CL stayed healthy over IPv6, no IPv4 LogEx sockets appeared, but EL did not establish a public mainnet session.
- Installed official geth `v1.17.4` on the IPv6 droplet for a bounded comparison, then tested with IPv4 egress blocked under a dedicated runtime user. Geth also formed zero execution peers; strict IPv6 `--netrestrict 2000::/3` filtered the default bootstrap nodes as IPv4-only, and the non-netrestrict run kept trying IPv4 candidates but never established an IPv6 peer.
- Stopped and removed the temporary geth process, data dir, IPv4 reject rule, and binary after the comparison; no full sync or temporary IPv6 test process is left running on the droplet.
- Split advertised/bind address family from outbound dial address families in the execution peer manager.
- Updated default auto selection so a host with public IPv4 and public IPv6 advertises IPv4 while accepting both IPv4 and IPv6 direct-dial candidates from DNS, known peers, and explicit execution bootnodes.
- Added `p2p_dial_families` to `/status` so users can see whether LogEx is dialing IPv4, IPv6, or both.
- Revalidated the IPv6 droplet after the dial-family change: default startup reported `auto-public-ipv4`, bind `0.0.0.0`, external `152.42.222.119`, and dial families `["ipv4","ipv6"]`; explicit `--p2p-bind-ip ::` reported `auto-public-ipv6`, dial family `["ipv6"]`, `ss -4` showed no LogEx sockets, and CL reached 20 active IPv6 sessions.
- Verified relevant local checks: `cargo fmt --all -- --check`, focused `logex-sync` P2P tests, `logex-cl --lib`, targeted `logex-node` startup tests, and `cargo clippy -p logex-sync -- -D warnings`.
- Reproduced the zero-progress stall through the Pi jump host: the floor stayed pinned while peers and active downloads remained present.
- Added critical-path repair for missing expected historical fetches without resetting buffered lookahead.
- Aborted stale historical fetch work below the expected sequence so obsolete attempts release request reservations.
- Confirmed the patch moved the remote floor off the pinned block and restored high instantaneous throughput; warmed samples still show bursty ordered progress.
- Split cheap ready fetch plan buffering from the active/heavy historical fetch pipeline.
- Measured the ready-plan change on the remote warmed run: average floor movement improved from `161` to `199` blocks/sec, low windows fell from `12` to `6`, and zero windows fell from `3` to `1`.
- Prioritized missing expected fetch refill ahead of lookahead prepare work so later buffered work cannot delay the next block range needed to advance the verified floor.
- Avoided blocking the ordered write loop on post-write pipeline refill when prepared historical batches are already queued.
- Added proactive missing-expected refill immediately after ordered writes advance the historical cursor.
- Measured the latest remote warmed run at `273` blocks/sec average with `1` low window and `0` zero windows after peers warmed to the low/mid 30s.
- Added an active-download-aware post-write refill gate so prepared backlog can no longer hide an empty active body/receipt pipeline.
- Measured the follow-up remote warmed run at `326` blocks/sec average with `0` low windows and `0` zero windows after peers warmed past 20 serving peers.
- Re-ran remote validation through `pi-remote` after direct network access failed outside the home network.
- Rejected a completed-buffer overflow experiment: it measured `279` blocks/sec with `3` low windows and `1` zero window.
- Rejected an eight-lane active-target experiment: it measured `326` blocks/sec with `1` low window and `1` zero window versus the accepted baseline at `324` blocks/sec with `0` low windows and `0` zero windows on the same route.
- Added active fetch child-header tracking so expected-sequence work can be discarded immediately when ordered writes advance to a different child header.
- Increased write-path refill headroom so active body/receipt downloads can refill to the adaptive pipeline depth instead of staying capped at four total refill slots.
- Measured the active-refill/stale-child build at `264` blocks/sec average with `4` low windows and `1` zero window; it reduced active-depth collapse but did not eliminate peer-tail stalls.
- Rejected a shorter expected-fetch hedge delay after it produced `2` zero windows within the first few minutes despite more than 25 serving peers.
- Re-ran the remote tests through `pi-remote` after the Mac mini direct route became unreachable outside the home network.
- Rejected a lookahead-promotion experiment for missing expected historical fetches: it measured `126.2` blocks/sec with `8` low windows and `2` zero windows, below the accepted baseline.
- Rejected a partial-prefix salvage skip experiment: it measured `107.9` blocks/sec with `7` low windows and `1` zero window, and changed burst shape without improving floor movement.
- Restored and rebuilt the accepted baseline on the Mac mini tmux session after each rejected experiment.
- Added priority-aware body/receipt fetch budgeting: the expected historical sequence keeps full live prefix scheduling, while lookahead sequences use a capped prefix lane so they cannot consume all body request slots.
- Measured priority budgeting on two warmed remote samples through `pi-remote`: `172.1` blocks/sec with `3` low windows and `1` zero window, then `156.4` blocks/sec with `5` low windows and `3` zero windows. This beat the immediate post-jump baseline average but did not eliminate ordered bursts.
- Rejected a wider full-priority candidate-pool experiment: it measured `140.7` blocks/sec with `11` low windows and `7` zero windows, then was reverted locally and remotely.
- Added local-work historical fetch refill while prepare tasks are waiting, using the same bounded refill path already used during writes.
- Measured the prepare-refill change after peers warmed to 29-32 serving peers: `238.4` blocks/sec average with `5` low windows and `2` zero windows, improving the previous warmed jump-host baseline (`175.9`, `7`, `6`) and removing the previously observed `active_fetches = 0` idle windows from the final sample.
- Validated locally with focused scheduler, sequence-gap, historical fetch tests, and `cargo check` for touched crates.
- Rejected a dense-prefix yield-size experiment: it measured `216.1` blocks/sec with `2` low windows and `0` zero windows and did not remove long body/receipt plan tails.
- Added bounded early redundancy for the first full-priority body/receipt prefix chunks; remote samples measured `378.0` blocks/sec with `1` low/`1` zero window and `327.7` blocks/sec with `0` low/`0` zero windows.
- Validated the accepted redundancy change with `cargo fmt --check`, `cargo test -p logex-sync body_receipt_ -- --nocapture`, `cargo test -p logex-sync historical_ -- --nocapture`, and `cargo check -p logex-node`.
- Reproduced a restart-era dry spell where `historical_fetch_expected_sequence` advanced ahead of `historical_fetch_next_sequence`, causing new work to be queued under stale sequence numbers until timeout recovery.
- Enforced the monotonic fetch cursor invariant after single fetch completion, materialized lookahead advancement, and ordered coalesced writes.
- Deployed the fix to the Mac mini through `pi-remote`; the follow-up 5 minute sample measured `287.9` blocks/sec with `0` low windows and `0` zero windows.
- Fixed strict clippy warnings in the touched scheduler/body-receipt areas.
- Re-ran the remote benchmark through `pi-remote` after a network-unreachable sample; the current route is valid and bulk download traffic is local, not through the WireGuard dashboard tunnel.
- Preserved a missing execution-client-family probe when the historical body/receipt candidate pool is truncated, so connected Nethermind peers do not remain permanently outside the request pool when the fastest/proven prefix is Geth-heavy.
- Measured the peer-family probe build at `297.3` blocks/sec with `2` low windows and `1` zero window; the sample showed Nethermind peers entering the serving set and improved the previous warmed post-cursor sample (`235.7`, `4`, `2`), but did not eliminate the prepared-backlog zero window.
- Rejected a 256-block live progress-target experiment: it measured `173.1` blocks/sec with `5` low windows and `1` zero window, below the accepted checkpoint.
- Identified body/receipt prefix salvage as an avoidable long tail: pre-change role logs showed salvage running despite an already acceptable contiguous prefix, with plans taking up to about `21s`.
- Added an accepted-prefix gate before salvage so live body/receipt plans return usable contiguous progress immediately instead of spending the salvage timeout on an optional prefix repair.
- Measured the salvage-gate build through `pi-remote`: `354.8` blocks/sec, `2` low windows, and `0` zero windows on the existing run. Candidate-window role logs showed salvage on only `2/417` plans and `7` plans over `10s`.
- Re-ran the accepted salvage-gate build through `pi-remote` after the direct route became unavailable: `306.6` blocks/sec, `2` low windows, and `0` zero windows, with physical download traffic on the local interface and WireGuard near idle.
- Rejected a full-priority 2 second partial-prefix flush experiment after it produced a zero-progress window and stayed around `190` blocks/sec before the sample was stopped.
- Rejected a wider dense active-pipeline experiment after it produced a zero-progress window and stayed around `179` blocks/sec before the sample was stopped.
- Restored the accepted baseline locally and on the Mac mini tmux session after both rejected experiments; the remote client is running on the accepted build with data dir `/Volumes/SSD 4TB/LogEx`.
- Rejected a planned-prefix residual carry-forward experiment: the cold sample was smooth (`266.4` blocks/sec, `0` low/`0` zero), but the warmed sample fell below baseline and hit `2` low windows before it was stopped.
- Restored the accepted baseline locally and on the Mac mini tmux session after the residual carry-forward experiment.
- Rejected an async residual body/receipt carry-forward experiment: it validated locally but measured only `123.2` blocks/sec with `11` low windows and `0` zero windows after peers reached 20+ connected, far below the accepted baseline.
- Restored the accepted baseline locally and on the Mac mini tmux session after the async residual experiment.
- Re-ran the restored accepted baseline through `pi-remote`: the warmed sample measured `348.3` blocks/sec with `1` low window and `0` zero windows, with physical RX repeatedly near the 300 Mbps network ceiling.
- Ran a longer 30 minute restored-baseline sample through `pi-remote`: `644,617` blocks over `1,785s`, `361.1` blocks/sec average, `2` low windows, and `0` zero windows while connected peers ranged roughly from the low 40s to low 80s.
- Started the destructive fresh baseline after explicit approval to reset the active data dir.
- Used `BASELINE_RESET_MODE=discard-incomplete` so the existing full-sync backup stayed intact while only the incomplete active `/Volumes/SSD 4TB/LogEx` contents were removed.
- Built the remote release binary, restarted LogEx in tmux, restored the preserved peer cache, and started the baseline monitor at `/Users/gremlinmaster/logex-baseline-runs/fresh-baseline-20260628-142644`.
- Verified the fresh run status endpoint, tmux sessions, active data dir, and preserved full-sync backup.
- Investigated the fresh-run two-hour zero-progress stall at block `11377745`.
- Confirmed the stall was a liveness bug: an ordered historical batch waited about `7,445,409ms` before it could commit while background storage maintenance scanned segment metadata under the storage read lock.
- Deferred background compaction planning while historical sync is incomplete; historical write batches still use their synchronous compacted write path.
- Deployed the fix to the Mac mini, restarted LogEx in tmux, and confirmed the historical floor advanced past the stalled range after restart.
- Updated the baseline monitor automation to watch for post-fix stalls and health issues until genesis, then clean up the remaining todo and delete itself if the run completes cleanly.
- Completed the active post-fix historical baseline to genesis: `summary.json` marked completion with `last_floor = 0` and `937` samples.
- Verified live head tracking after completion with two `/status` samples one minute apart; the live head advanced from block `25423032` to `25423037`.
- Reviewed recent service and monitor logs after completion; only normal discovery warnings were present and no monitor errors were found.
- Recorded post-fix health metrics: `p50` historical rate `139,605` logs/sec and `1,332` blocks/sec, `p90` physical RX `305 Mbps`, connected peers `p50` `97`/`p90` `103`, serving peers `p50` `30`, peak RSS `6.8 GB`, minimum disk free `476.6 GB`, `32` low windows, and `10` zero windows.
- Opened PR #97 (`Optimize historical execution sync scheduler`) for the completed historical sync performance branch.
- Ran local CI-equivalent validation successfully: `cargo fmt --all -- --check`, `cargo check --workspace`, `cargo clippy --workspace -- -D warnings`, and `cargo test --workspace`.
- Confirmed GitHub Actions jobs for PR #97 currently fail before running any workflow steps because Actions quota is unavailable; merge is deferred until checks can be rerun.
- Ran a fresh 15 minute strict IPv6-only proof on the DigitalOcean droplet with IPv4 egress rejected for the LogEx runtime user and IPv6 DNS configured for the proof window.
- Confirmed the strict IPv6-only proof was socket-clean: `p2p_bind_ip = ::`, `p2p_dial_families = ["ipv6"]`, zero IPv4 LogEx sockets in the collected `ss` samples, and no leftover LogEx process or firewall rule after cleanup.
- Confirmed CL works over strict IPv6 in the proof window, holding 46-67 active sessions.
- Confirmed public mainnet EL IPv6 discovery remains the blocker rather than local socket configuration: 18 accepted IPv6 DNS execution candidates, 351 IPv4-only DNS candidates rejected, 18 submitted EL dials, 18 dial expirations, 0 execution sessions, and no historical floor.
- Added a startup/status warning for strict IPv6-only execution sync without explicit `--execution-bootnode`, because public EL IPv6 discovery is currently too sparse to treat as reliable by default.
- Rebuilt the updated binary on the IPv6 droplet and ran a short follow-up smoke check that reported the warning in `/status`, reached 19 CL sessions, submitted 22 IPv6 EL dials, and showed `SOCKET4_COUNT=0`; temporary proof data was removed afterward.
- Ran a trace-level public IPv6 EL candidate probe and extracted the actual DNS-discovered execution candidate endpoints.
- Confirmed every extracted public IPv6 EL candidate had a closed or timed-out advertised TCP port, while CL addresses in the same log were reachable; this confirms the public EL candidate source is stale/unreachable rather than LogEx missing a local IPv6 socket path.
- Updated automatic address selection so the advertised address family and outbound dial families are separate: automatic public IPv6 fallback still advertises IPv6, but accepts IPv4 outbound candidates when an IPv4 route exists; outbound-only mode likewise keeps both routed families available.
- Rebuilt on the IPv6 droplet and verified default dual-public auto mode reports `auto-public-ipv4`, bind `0.0.0.0`, external IPv4 `152.42.222.119`, dial families `["ipv4","ipv6"]`, and reached CL plus EL peers in a one minute smoke run.
- Added a derived `execution_network.bootstrap_warning` field to `/status` so unreachable or wrong-family execution bootstrap conditions are visible without interpreting raw counters.
- Validated the warning on the IPv6 droplet in strict IPv6-only mode: CL reached 88 active sessions, `SOCKET4_COUNT=0`, all 22 submitted EL dials expired, and `/status` reported the new bootstrap warning. The temporary process, firewall rule, resolver override, and data dir were removed afterward.
- Verified the default checkpoint source gap for strict IPv6-only startup: `https://mainnet.checkpoint.sigp.io` resolved IPv4-only from the droplet, while `https://ethereum-beacon-api.publicnode.com` returned finalized headers over IPv6.
- Re-ran strict IPv6-only LogEx using the IPv6-capable checkpoint URL and `--grpc-host ::1`; CL stayed healthy with 66-74 active sessions, `ss -4` showed zero LogEx sockets, and EL reported 21 IPv6 DNS execution candidates whose submitted dials all expired.
- Confirmed the temporary IPv6 droplet was clean after testing: no LogEx process, no leftover owner firewall rule, resolver symlink restored, and only the small temporary smoke data dir remained.
- Added finalized checkpoint fallback support for Beacon API providers that expose `/eth/v1/beacon/states/finalized/finality_checkpoints` instead of finalized header/block endpoints.
- Rebuilt the updated binary on the IPv6 droplet and verified strict IPv6-only startup against `https://mainnet-checkpoint-sync.stakely.io`; CL reached 46-62 active IPv6 sessions, `ss -4` showed zero LogEx sockets, and EL still reported only expired public IPv6 execution dials.
- Removed the leftover temporary strict IPv6 smoke-test data dir from the droplet after confirming no LogEx process, no owner firewall rule, and restored resolver state.
- Generalized checkpoint block/root fallback so numeric slot quorum checks work against Beacon/checkpoint providers that do not expose `/eth/v1/beacon/headers/{slot}`.
- Replaced the old IPv4-only default checkpoint endpoint with a built-in 2-of-3 IPv6-capable mainnet quorum using PublicNode, Stakely, and BeaconState.
- Verified the new default on the IPv6 droplet without passing `--checkpoint-sync-url`: the client resolved checkpoint `14662080@0xcba8...`, started HTTP/gRPC/P2P on IPv6-only sockets, received CL bootstrap payloads, and kept `SOCKET4_COUNT=0` while IPv4 egress was rejected for the LogEx runtime user.
- Updated CLI help and README checkpoint examples to reflect the built-in checkpoint quorum.
- Removed the temporary strict IPv6 default-checkpoint smoke data dir and copied test binary from the droplet after the bounded proof.
- Earlier tightened family-aware EL DNS conversion to require `tcp6`/`udp6` everywhere, then refined it after geth/Nethermind source review so direct RLPx uses generic-port fallback while signed discv5 bootstrap remains same-family UDP-gated.
- Rebuilt the current branch on the IPv6 droplet and reran bounded strict IPv6 proofs with IPv4 egress rejected for the LogEx runtime user; CL reached live head materialization over IPv6, while public EL IPv6 candidates either lacked usable endpoints or expired without accepted execution sessions.
- Verified the temporary strict IPv6 process, data dir, copied test binary, IPv4 owner reject rule, and resolver override were removed after the proof window.
- Re-synced the current branch to the IPv6 droplet, rebuilt with the existing remote target cache, and ran a controlled two-node IPv6-only proof using an explicit IPv6 execution enode.
- Confirmed the controlled proof established an Eth70 execution session over IPv6 only: the client reported `accepted_sessions = 1`, both nodes reported `p2p_bind_ip = "::"` and `p2p_dial_families = ["ipv6"]`, CL kept active IPv6 sessions, and `ss -4` showed no matching LogEx sockets.
- Removed the temporary proof source tree, target cache, copied binaries, data dirs, logs, resolver override, and owner firewall rule from the IPv6 droplet after the bounded test.
- Added signed `enr:` support to `--execution-bootnode`/`execution_bootnodes`; signed ENRs now seed discv5 directly when UDP fields match the selected discovery family and can also produce family-compatible direct RLPx candidates.
- Updated CLI help and README config docs so execution bootnodes are documented as `enode://...` or signed `enr:...` records.
- Rebuilt the branch on the IPv6 droplet and ran a bounded signed-ENR proof: the client loaded one signed ENR as both a direct IPv6 candidate and signed discv5 seed, established an Eth70 session over IPv6 only, kept active CL IPv6 sessions, and `ss -4` showed no matching LogEx sockets.
- Removed the temporary droplet source tree, target cache, copied binaries, data dirs, resolver override, owner firewall rule, and proof logs after the signed-ENR test.
- Split DNS-discovered execution ENRs into direct RLPx candidates and signed discv5 bootstrap seeds, so IPv6 UDP-only ENRs are no longer discarded just because they cannot be direct TCP dials.
- Rebuilt the current branch on the IPv6 droplet and reran the bounded signed-ENR proof with public IPv4 egress rejected for the LogEx runtime user; the client loaded one signed ENR as both a direct IPv6 candidate and signed discv5 seed, established an Eth70 session over IPv6 only, and received CL light-client bootstrap payloads over IPv6.
- Removed the temporary droplet source tree, target cache, copied binaries, data dirs, proof logs, and owner firewall rule after the latest bounded IPv6 proof.
- Added separate `p2p_listen_families`, `p2p_advertised_families`, and `p2p_dial_families` status fields so operators can distinguish local listener family, advertised public family, and outbound dial policy.
- Rejected IPv4-mapped IPv6 CL multiaddrs from dial classification so strict IPv6 mode cannot attempt `::ffff:` addresses over QUIC/TCP.
- Rebuilt on the IPv6 droplet and reran the strict public IPv6 proof after the filter fix: CL reached 84 active sessions and 3,575 dialable peers, `SOCKET4_COUNT` stayed at 0, the proof log contained zero `::ffff:` entries, and public EL stayed blocked by missing usable IPv6 execution endpoints.
- Removed the temporary droplet source tree, copied proof binary, proof data, proof logs, test user, and owner firewall rule after validation.
- Aligned IPv6 ENR port fallback with geth and Nethermind for direct RLPx dials: DNS and signed ENR records that advertise `ip6` may use family-specific `tcp6`/`udp6` or generic `tcp`/`udp` for direct dial candidates.
- Kept signed discv5 bootstrap stricter than direct RLPx: IPv6 signed ENRs still require explicit `udp6` before they are passed to Reth discv5, because generic `udp` caused Reth to reject the ENR as incompatible during startup.
- Rebuilt on the IPv6 droplet and reran a bounded strict IPv6-only public proof. The run kept `SOCKET4_COUNT=0`, CL held 66-81 active IPv6 sessions and reached 3,730 dialable peers, but public EL still accepted only 21 IPv6 DNS candidates and all 21 submitted dials expired with zero execution sessions.
- Ran a short default automatic-mode smoke after the strict proof. It selected `auto-public-ipv4`, advertised `152.42.222.119`, and reported outbound dial families `["ipv4","ipv6"]`; the smoke was stopped before consensus warmup and was not used as an EL sync proof.
- Decoded the current public execution DNS tree locally: it contained 2,409 ENRs, 114 IPv6 ENRs, and those IPv6 records advertised generic `tcp`/`udp` ports rather than `tcp6`/`udp6`, matching the geth/Nethermind generic-port fallback requirement.
- Replaced the bounded Reth DNS `node_record_stream()` listener with direct polling of `DnsDiscoveryService`, widened the DNS cache to cover the public tree, and preserved fork-id parsing in LogEx's family-aware DNS conversion.
- Queued initial DNS IPv6 bootnodes as direct RLPx dial candidates after peer-manager startup, so they are no longer consumed only as discovery seeds.
- Rebuilt on the IPv6 droplet and reran a strict IPv6-only public proof with IPv4 egress rejected for the LogEx runtime user. The run kept zero IPv4 sockets, CL ended with 88 active sessions and 3,145 dialable peers, EL accepted/submitted 45 IPv6 DNS candidates, all 45 submitted dials expired, and no public execution session was established.
- Crawled the full current geth execution DNS tree and audited IPv6 reachability from the droplet: 3,000 ENRs, 142 IPv6-capable ENRs, only 2 TCP-reachable IPv6 endpoints, and both failed to become accepted Eth sessions when configured explicitly in LogEx.
- Extended the derived execution bootstrap warning so repeated expired dials with configured/discovered candidates are visible even when a small number of candidates remain pending for retry.
- Rebuilt on the IPv6 droplet and reran a shortened strict IPv6-only proof. CL reached 101 active sessions with zero IPv4 LogEx sockets, EL stayed at zero accepted sessions with 61 of 63 dials expired, and `/status` reported the new repeated-expiration warning.
- Removed the temporary strict IPv6 proof data, proof binaries, scripts, resolver override, source/build cache, and owner firewall rule from the droplet after validation.

## Remaining TODOs

1. Decide the production policy for strict IPv6-only EL sync.
   - Reason: CL works over IPv6 and EL transport/discovery is socket/family-clean, but the current public execution IPv6 peer set is not sufficient for mainnet EL sync. A full geth DNS-tree audit found only two TCP-reachable IPv6 EL endpoints, and both failed to establish Eth sessions when explicitly configured.
   - Completion criteria: either demonstrate EL historical progress with only IPv6 sockets against reliable public IPv6 execution peers, or accept strict IPv6-only EL as an advanced/explicit-bootnode mode while default startup uses proven IPv4/dual-family paths when available.

2. Complete dual-stack address-family support.
   - Reason: home users should not have to know whether they have public IPv4, CGNAT IPv4, usable IPv6, both usable families, or only outbound connectivity.
   - Completion criteria: LogEx uses both IPv4 and IPv6 discovery/sync paths when both are usable, or a deliberate product decision documents single-family behavior; current progress supports IPv4-advertised dual-public hosts accepting IPv6 outbound candidates, but true dual advertised identity/listeners still need either Reth dual-family support or a composite peer manager.

3. Conclude the historical sync performance PR.
   - Reason: the active post-fix baseline reached genesis without repeated liveness stalls and remained bounded by the available network rather than a confirmed code bottleneck.
   - Completion criteria: rerun PR #97 GitHub Actions after quota is available, confirm checks pass, and merge when checks allow.

## Design Decisions

- Historical backfill keeps ordered verification as the commit boundary.
  - Why: logs are valid only after block/receipt data is cryptographically checked and written in canonical reverse order.
  - Tradeoff: unordered downloads can run ahead, but the scheduler must explicitly protect the next-needed sequence.

- Missing expected historical fetches are refilled without discarding buffered lookahead.
  - Why: resetting all lookahead wastes useful work and creates more burstiness.
  - Alternative considered: full pipeline reset, which fixed some gaps but caused avoidable churn.

- Stale fetch work below the expected sequence is aborted.
  - Why: those results can no longer advance the floor and otherwise keep body/receipt reservations occupied.
  - Tradeoff: a small amount of already-started network work may be discarded to keep critical slots available.

- Ready fetch plans use a separate cheap buffer from active/heavy fetched data.
  - Why: slow reverse-header planning windows were letting active body/receipt downloads run dry even when memory and bandwidth were available.
  - Tradeoff: the scheduler keeps more header/plan metadata in memory, while active receipt/body downloads remain capped by the pipeline depth.

- Ordered writes no longer wait on non-critical post-write refill when prepared batches are ready.
  - Why: a measured stall spent about 24 seconds in refill after a batch was already written, preventing the next verified prepared batch from advancing the floor.
  - Tradeoff: refill may run slightly later when there is prepared backlog, so active download depth still needs a follow-up refill policy that keeps the network busier without increasing memory pressure.

- Post-write refill uses active body/receipt depth, not only buffered inventory.
  - Why: ready/completed/prepared backlog can look healthy while active network downloads have drained.
  - Tradeoff: the write loop may briefly block on a limited write-path refill when active downloads are below the floor, but it avoids multi-window floor stalls.

- Expected-sequence active fetch attempts remember their planned child header.
  - Why: ordered coalescing can advance the expected child while a same-sequence fetch planned from the old child is still active or queued.
  - Tradeoff: the scheduler may discard a small amount of in-flight work, but it avoids waiting for a fetch that cannot advance the floor.

- Rejected active-depth-only tuning as a production strategy.
  - Why: both a completed-buffer overflow gate and an eight-lane active target failed to improve the accepted warmed baseline without adding low/zero windows.
  - Alternative considered: keep the constants-only changes; rejected because the improvement was not meaningful and stability regressed.

- Rejected shorter expected-fetch hedge timing.
  - Why: duplicating the head-of-line fetch earlier increased zero-progress windows under high serving-peer counts.
  - Alternative considered: keep the 2 second hedge; rejected in favor of the previous 4 second head-of-line delay.

- Historical body/receipt fetch plans now carry scheduling priority.
  - Why: lookahead fetches were able to saturate body request slots while the expected sequence was the only work that could advance the verified floor.
  - Tradeoff: lookahead work may complete more slowly, but expected-sequence latency and average floor movement improve in warmed samples.

- Historical fetch refill now runs during local prepare waits as well as writes.
  - Why: a remote sample showed prepared work queued while active body/receipt downloads fell to zero.
  - Tradeoff: prepare waits may spend a small amount of time on bounded refill work, but the downloader is less likely to go idle between ordered writes.

- Full-priority historical body/receipt plans now hedge the first prefix chunks immediately when enough peers are available.
  - Why: remote logs showed long plan tails caused by early prefix chunks lagging while later chunks completed.
  - Tradeoff: this spends extra bandwidth on the chunks that gate ordered verification, so it is limited to full-priority expected work and does not apply to lookahead plans.

- The historical fetch sequence cursor is monotonic with the expected cursor.
  - Why: ordered writes and materialized lookahead can advance the expected sequence by multiple batches; future refills must not reuse stale sequence ids below that cursor.
  - Tradeoff: none intended; old sequence ids are already obsolete once the ordered cursor advances.

- Body/receipt candidate truncation preserves one probe for any missing execution-client family.
  - Why: sorting by proven request performance can make the top candidate pool Geth-heavy and prevent connected Nethermind peers from ever becoming serving peers.
  - Tradeoff: the pool may temporarily exceed the fast-pool size by a small number of client-family probes, but the active pipeline and per-peer request limits still bound memory and network work.

- Body/receipt prefix salvage runs only when no acceptable contiguous prefix exists.
  - Why: the completion path can safely accept a verified contiguous prefix; spending up to the salvage timeout after that point creates head-of-line latency without increasing validity.
  - Tradeoff: the scheduler may return smaller batches instead of trying to repair more of the prefix immediately, but the next ordered fetch covers the remaining range and avoids long idle windows.

- Do not start the global live chunk scheduler unless long-run evidence justifies the architecture risk.
  - Why: the restored accepted baseline produced a warmed `348.3` blocks/sec sample and a 30 minute `361.1` blocks/sec sample with only `2` low windows and no zero windows while the physical link was repeatedly near the 300 Mbps ceiling.
  - Alternative considered: immediately rewrite plan-level body/receipt scheduling into a global chunk scheduler.
  - Tradeoff: delaying the rewrite avoids destabilizing a strong baseline, but the full-run benchmark must still prove the scheduler remains stable outside short warmed samples.

- Fresh baseline resets can discard incomplete active data without touching existing full-sync backups.
  - Why: after a partially synced experiment, replacing a known-good full backup with incomplete data would make recovery harder.
  - Alternative considered: always move the active data dir into a full-sync backup slot.
  - Tradeoff: `discard-incomplete` is destructive for the active run, so it requires the explicit confirmation flag and should only be used when a separate full backup already exists.

- Background compaction is deferred until historical sync reaches genesis.
  - Why: dense historical ingest already writes compacted segments synchronously, while background compaction planning can scan thousands of segment manifests and starve the ordered historical writer behind the storage lock.
  - Alternative considered: keep background compaction active during historical sync and tune scan frequency; rejected because the observed stall held the verified floor for about two hours.
  - Tradeoff: any opportunistic background maintenance waits until historical sync completes, but the critical sync path remains live and compressed.

- End-to-end wall-clock sync time is not a blocker for this PR when the run is network-bound.
  - Why: post-fix samples show the client can drive the physical link near the available 300 Mbps download limit, so another fresh run would mainly remeasure infrastructure capacity.
  - Alternative considered: reset and rerun from scratch to obtain an uncontaminated wall-clock number.
  - Tradeoff: the contaminated run is not a clean benchmark, but it remains sufficient to validate liveness if it reaches genesis without repeated post-fix stalls.

- IPv6-only EL mode keeps Reth discv4 enabled but disables Reth's default DNS conversion.
  - Why: Reth's default DNS ENR conversion prefers IPv4 fields, which caused IPv4 dial attempts even when LogEx was explicitly bound to IPv6. Seeding discv4 from LogEx's family-aware DNS conversion preserves UDP discovery without leaking IPv4 candidates.
  - Alternative considered: disable discv4 entirely; rejected because it left EL with direct DNS dials only.
  - Tradeoff: pure IPv6 EL discovery currently depends on a small public DNS candidate set until a better EL IPv6 peer source is implemented or the wider network improves.

- Execution IPv6 mode supports explicit bootnodes and configurable discv5 ports.
  - Why: public IPv6 EL peer availability is sparse, and deterministic IPv6 tests need a reliable enode seed plus separate UDP ports when multiple LogEx instances run on one host.
  - Alternative considered: leave IPv6 validation dependent only on public DNS/discovery; rejected because public candidate refusal/timeouts cannot distinguish transport bugs from network scarcity.
  - Tradeoff: this adds two advanced P2P knobs, but defaults remain unchanged for normal users.

- Execution bootnodes accept both unsigned enodes and signed ENRs.
  - Why: execution discv5 is ENR-native, and public crawlers or operators may expose IPv6-capable peers as signed `enr:` records rather than `enode://` URLs.
  - Alternatives considered: keep only enode support; rejected because it makes strict IPv6 bootstrapping depend on a less common representation.
  - Tradeoff: signed ENRs are only useful for direct RLPx when they include family-compatible TCP fields; ENRs with only UDP fields can still seed discv5 but cannot be used as direct dial targets.

- DNS-discovered signed ENRs split direct RLPx and discv5 requirements.
  - Why: geth and Nethermind apply generic `tcp`/`udp` fallback when an ENR has `ip6` but no `tcp6`/`udp6`, so LogEx uses the same fallback for direct RLPx candidates. Reth discv5, however, rejects signed ENRs that do not have a UDP socket compatible with the selected discovery family, so signed discv5 seeds still require explicit `udp6` in strict IPv6 mode.
  - Alternatives considered: require `tcp6`/`udp6` everywhere, which avoids speculative dials but diverges from geth/Nethermind direct endpoint interpretation; or accept generic `udp` into discv5, which reproduced a Reth startup failure.
  - Tradeoff: direct-dial counters include more IPv6 candidates, but current public EL IPv6 peer availability is still sparse and all submitted public candidates expired in the bounded proof.

- Status reports listen, advertised, and dial P2P families separately.
  - Why: current production-safe dual-family behavior uses one advertised/listener family while allowing broader outbound dials, and Reth's RLPx stack does not provide true single-manager dual RLPx listener support.
  - Alternatives considered: report only `p2p_dial_families`; rejected because it can make IPv4-advertised plus IPv6-outbound mode look like full dual inbound support.
  - Tradeoff: this is diagnostic, not a substitute for a future dual-manager architecture if true dual inbound support becomes necessary.

- Default checkpoint resolution uses an IPv6-capable 2-of-3 mainnet quorum.
  - Why: the previous default endpoint was not IPv6-reachable from the droplet, while PublicNode, Stakely, and BeaconState were reachable over IPv6 and could agree on a concrete finalized slot after block/root fallback.
  - Alternatives considered: keep the old single default and require IPv6 users to pass `--checkpoint-sync-url`; rejected because it makes default strict IPv6 startup fail before P2P.
  - Tradeoff: startup depends on two of three third-party Beacon/checkpoint providers agreeing, which is stronger than one source but may make an offline source visible sooner.

- Checkpoint resolution uses ordered provider-shape fallbacks for any block id.
  - Why: checkpoint-sync providers do not all expose `/eth/v1/beacon/headers/{id}`; trying Beacon headers, Beacon blocks plus root, and finalized state checkpoints for the finalized alias keeps default quorum compatible across provider shapes.
  - Tradeoff: startup may make multiple requests to the same configured endpoint before failing, but the failures are aggregated into a single actionable error.

- Pure public IPv6-only EL sync is not production-ready without better peer sources.
  - Why: LogEx can establish controlled IPv6 Eth70 sessions and CL can maintain many IPv6 sessions, but both LogEx and official geth failed to form public mainnet execution peers when IPv4 egress was blocked on the same IPv6 droplet.
  - Alternatives considered: assume the failure is LogEx-specific and continue tuning discovery; rejected after the geth comparison showed the same public bootstrap limitation.
  - Tradeoff: strict IPv6-only remains useful for controlled tests and explicit bootnodes, while production defaults still need dual-stack or outbound IPv4 fallback behavior for execution sync.

- Strict IPv6-only execution mode warns for missing bootnodes and repeated dial expiry.
  - Why: the client can be socket-clean and still sit at zero execution peers because the public IPv6 EL peer set is sparse or configured bootnodes are not actually serving Eth sessions.
  - Alternatives considered: fail startup in strict IPv6-only mode without bootnodes; rejected because CL and controlled EL IPv6 are valid and advanced users may have their own IPv6 peers.
  - Tradeoff: startup remains permissive, but `/status` and logs make the public EL bootstrap risk visible.

- Family-aware DNS uses geth/Nethermind-compatible generic ports for IPv6 direct dials.
  - Why: ENR `tcp`/`udp` are generic endpoint fields and both geth and Nethermind fall back to them for IPv6 when `tcp6`/`udp6` are absent.
  - Alternatives considered: require explicit `tcp6` for direct RLPx; rejected because it would incorrectly drop some standards-compatible IPv6 records.
  - Tradeoff: public IPv6 EL candidates may still be stale or unreachable, so strict IPv6-only mode can remain at zero EL peers even though the direct-dial interpretation is compatible.

- DNS discovery is polled directly and initial DNS bootnodes are queued as direct candidates.
  - Why: Reth's bounded `node_record_stream()` listener can drop DNS updates under a large public tree, and the initial IPv6 DNS records were otherwise only used as discovery seeds instead of being sent to the direct RLPx scheduler.
  - Alternatives considered: keep the listener and only increase wait/cache sizes; rejected because it still leaves update delivery lossy and does not directly schedule initial bootnodes.
  - Tradeoff: LogEx now owns more DNS-service plumbing, but the behavior is explicit, testable, and keeps the direct candidate path consistent with the later DNS event path.

- IPv4-mapped IPv6 multiaddrs are not valid in strict IPv6 mode.
  - Why: `::ffff:x.y.z.w` still targets an IPv4 endpoint even though it is encoded as `/ip6`; accepting it produced QUIC send attempts to IPv4-mapped addresses during a socket-clean proof.
  - Alternatives considered: rely only on the owner firewall to block those sends; rejected because strict IPv6 mode should classify and filter them before scheduling a dial.
  - Tradeoff: a peer that only advertises IPv4-mapped IPv6 addresses is ignored until a real IPv6 or IPv4 route is allowed.

- Dual-public auto mode keeps IPv4 as the advertised execution identity and enables IPv6 only as an additional outbound dial family.
  - Why: public execution-layer IPv4 bootstrap is proven, while public IPv6-only execution peers were too sparse to rely on by default.
  - Alternatives considered: switch default dual-public hosts to IPv6-only or attempt a full two-peer-manager dual-stack rewrite immediately.
  - Tradeoff: this is not a full dual advertised identity, but it improves dual-stack reachability without destabilizing the proven IPv4 sync path.

- Public-IPv6/private-IPv4 automatic mode advertises IPv6 but keeps IPv4 outbound dials enabled.
  - Why: many home users do not have public IPv4, but still have outbound IPv4; strict public IPv6 EL discovery is currently too sparse to be the only execution peer source.
  - Alternatives considered: force strict IPv6-only whenever public IPv4 is unavailable; rejected because it strands EL sync on unreachable public IPv6 DNS candidates.
  - Tradeoff: this is not a pure IPv6-only sync, but it is the production-safe default for CGNAT/private-IPv4 plus public-IPv6 hosts. Explicit `--p2p-bind-ip ::` remains strict IPv6-only.

- Default `--nat any` uses local route-owned public address detection.
  - Why: a public IP API can return the router/ISP address for private or CGNAT hosts, which is not proof that LogEx can advertise that address for inbound peer retention.
  - Alternatives considered: keep Reth's public-IP resolver as the default; rejected because it can misadvertise home-network clients. Explicit `--nat publicip` remains available for users who want that behavior.
  - Tradeoff: port-forwarded private IPv4 hosts need explicit `--nat extip:<ip>` for deterministic inbound reachability, but the default is safer and matches the objective of avoiding false public IPv4 claims.

- Build caches should be generated outside Git.
  - Why: `sccache` and `cargo-chef` can reduce rebuild times, but cache artifacts are host/toolchain-specific and too large for the repository.
  - Alternatives considered: committing prebuilt artifacts; rejected because they are not portable across macOS/Linux and would bloat the repo.
  - Tradeoff: developers need a one-time cache setup command, but source control stays clean.
  - Suggested local cache setup:
    ```sh
    cargo install sccache cargo-chef
    export RUSTC_WRAPPER=sccache
    sccache --start-server
    cargo chef prepare --recipe-path recipe.json
    cargo chef cook --release --recipe-path recipe.json
    cargo build --release -p logex-node --bin logex
    sccache --show-stats
    ```

## Challenges and Resolutions

- Challenge: direct Mac mini access failed outside the home network.
  - Resolution: reran checks and throughput samples through `pi-remote`.
  - Remaining: use the jump host unless direct LAN access is confirmed.

- Challenge: the scheduler entered a state with active fetches but no active expected fetch, no prepare-ready work, and no floor movement.
  - Resolution: added stale-work cleanup and missing-expected refill.
  - Remaining: single-interval zero windows still occur, so the broader live scheduler is not complete.

- Challenge: active body/receipt downloads ran low while waiting for slow reverse-header planning.
  - Resolution: added a separate ready-plan buffer so cheap queued plans can hide header latency.
  - Remaining: ordered prepare/write still causes shorter burstiness.

- Challenge: expected-sequence holes were detected only after several prepared lookahead batches had accumulated.
  - Resolution: moved missing-expected refill ahead of lookahead prepare work and added a proactive refill after ordered writes advance the cursor.
  - Remaining: dense ranges can still drain active downloads while many prepared batches wait to be written.

- Challenge: prepared backlog hid active body/receipt download starvation.
  - Resolution: post-write refill now considers active body/receipt fetch count and forces a limited write-path refill when active downloads fall below the floor.
  - Remaining: any next architectural pass should be a global live chunk scheduler or a full-run benchmark proving the current scheduler is bounded by network/runtime conditions.

- Challenge: active-depth experiments looked promising in spot metrics but failed warmed samples.
  - Resolution: reverted both rejected experiments locally and remotely, restored the accepted baseline, and left the Mac mini client running on the baseline build.
  - Remaining: compare future changes only against the accepted baseline and keep only changes that improve longer samples.

- Challenge: same-sequence fetches sometimes remained active after the expected child header changed.
  - Resolution: active attempts now store their planned child header and the cursor-advance path discards mismatched expected-sequence queued, completed, and active work.
  - Remaining: peer-tail body/receipt responses can still block the ordered floor even when active depth is healthy.

- Challenge: lookahead fetches competed with expected fetches for the same body request slots.
  - Resolution: added priority-aware body/receipt plan budgeting so expected work keeps full live prefix capacity and lookahead work is capped.
  - Remaining: ordered floor movement still has single-window stalls, so a true global live request scheduler remains open.

- Challenge: prepared batches could accumulate while active historical body/receipt downloads dropped to zero.
  - Resolution: local-work refill now runs during prepare waits and writes; warmed confirmation sample improved to `238.4` blocks/sec with `2` zero windows.
  - Remaining: active fetches can still stall behind slow peer tails, so the next scheduler work should target global live chunk scheduling or active expected-lane repair.

- Challenge: shrinking dense plan yield size reduced return blocks but did not remove body/receipt long tails.
  - Resolution: reverted the dense-prefix experiment and kept the accepted baseline.
  - Remaining: use sample data, not constants-only changes, to justify any future yield-size tuning.

- Challenge: expected prefix chunks could lag behind later completed chunks.
  - Resolution: added bounded immediate redundancy for the first full-priority prefix chunks; remote samples improved while keeping failures controlled.
  - Remaining: longer full-run validation is still needed before concluding the PR.

- Challenge: after restart, the scheduler briefly queued new work below the already-advanced expected sequence and recovered only after timeout refill.
  - Resolution: kept `historical_fetch_next_sequence` aligned with `historical_fetch_expected_sequence` on every expected-cursor advance.
  - Remaining: head-of-line refills still happen under slow peer tails, but they no longer strand the queue with `next < expected`.

- Challenge: many Nethermind peers were connected but none were serving body/receipt work in a warmed run.
  - Resolution: preserved one missing client-family probe after candidate sorting and truncation.
  - Remaining: peer diversity improved, but a prepared-backlog zero window still occurred, so the next improvement must target ordered scheduler flow rather than discovery.

- Challenge: a 256-block live progress target reduced per-plan target size but lowered overall floor movement.
  - Resolution: reverted locally and remotely after the benchmark regressed to `173.1` blocks/sec.
  - Remaining: use targeted tail-latency fixes rather than lowering the whole progress target.

- Challenge: body/receipt salvage could run even after the live plan had enough contiguous verified progress to complete.
  - Resolution: added an accepted-prefix gate before salvage and kept the candidate after a `354.8` blocks/sec, zero-window benchmark.
  - Remaining: some low windows remain when prepared backlog grows, so longer-run validation is still required.

- Challenge: two post-salvage scheduler candidates regressed zero-window behavior.
  - Resolution: reverted the 2 second full-priority partial-prefix flush and wider dense active-pipeline experiments locally and remotely.
  - Remaining: the next serious scheduler change should be a measured architectural change to global live chunk scheduling, not another constants-only tuning pass.

- Challenge: carrying the original planned prefix and residual chunks forward smoothed cold progress but reduced warmed throughput.
  - Resolution: reverted the residual carry-forward experiment locally and remotely after the warmed sample regressed below the accepted baseline.
  - Remaining: preserving lookahead validity needs a true global chunk scheduler, not synchronous residual-gap filling after each partial prefix.

- Challenge: draining/refilling the historical pipeline while async residual body/receipt work ran still reduced warmed throughput.
  - Resolution: rejected and reverted the async residual experiment after a `123.2` blocks/sec sample with `11` low windows.
  - Remaining: the next production-grade scheduler step should be a global chunk-level scheduler or a full-run proof that the current accepted scheduler is the practical baseline.

- Challenge: the rejected async residual sample made the current branch look worse than it was.
  - Resolution: restored the accepted build and reran a warmed baseline through `pi-remote`; it recovered to `348.3` blocks/sec with `1` low window and `0` zero windows.
  - Remaining: use a fresh full-run baseline, not another speculative experiment, before taking on a risky global scheduler rewrite.

- Challenge: the active run is useful for stability but is not a fresh pivot-to-genesis baseline.
  - Resolution: collected a 30 minute stability sample and identified the existing guarded fresh-baseline script.
  - Remaining: resolved; the post-fix baseline reached genesis and live head tracking continued afterward.

- Challenge: the default fresh-baseline path refuses to reset when the active data dir is not fully synced.
  - Resolution: used the guarded `discard-incomplete` mode so the known full-sync backup was preserved and only the incomplete active data was deleted.
  - Remaining: resolved; the run reached genesis and another clean wall-clock benchmark is not required while network capacity is the practical bottleneck.

- Challenge: the fresh run appeared alive but made no historical progress for nearly two hours.
  - Resolution: sampled the running process and matched the stall to background compaction/profile-rewrite planning scanning storage metadata while the ordered historical writer waited; background compaction is now skipped while historical sync is incomplete.
  - Remaining: resolved; the post-fix run reached genesis without a repeated liveness stall.

- Challenge: the baseline monitor was still framed around a clean wall-clock benchmark after the stall fix.
  - Resolution: updated the heartbeat instructions to monitor post-fix liveness and health until genesis, then remove itself and clear or revise the todo if no apparent problems remain.
  - Remaining: resolved; genesis was reached and the automation is being removed.

- Challenge: Reth still attempted IPv4 EL dials in an IPv6-only run after discv4 was disabled.
  - Resolution: inspected Reth `NetworkConfigBuilder` and found default DNS discovery is enabled by default; IPv6 mode now disables Reth DNS as well and uses LogEx's family-aware DNS stream.
  - Remaining: resolved for socket/family correctness; remote logs after the fix showed zero fresh IPv4 attempts and no `logexv6` IPv4 sockets.

- Challenge: pure IPv6 EL mainnet sync did not start on the DigitalOcean droplet.
  - Resolution: verified IPv6 listeners, removed an accidental IPv6 INPUT firewall reject, confirmed CL peers over IPv6, seeded EL discv4/discv5 from family-aware IPv6 DNS bootnodes, fixed stale submitted-dial pruning, added explicit execution bootnodes, exposed dial/discovery counters, and proved controlled LogEx-to-LogEx Eth70 sessions over IPv6 only using both enode and signed ENR bootnodes.
  - Remaining: unresolved for public historical sync; bounded public LogEx samples and an official geth comparison both produced no accepted IPv6 EL session, so either a better IPv6 EL candidate source is needed or product behavior must explicitly fall back/warn.

- Challenge: it was unclear whether public IPv6 EL candidates were stale, unreachable, or failing after RLPx handshake.
  - Resolution: captured trace-level DNS candidate endpoints and probed their advertised TCP ports directly; every EL candidate timed out or refused TCP.
  - Remaining: strict public IPv6-only EL needs reliable IPv6 execution bootnodes or a different peer source before it can be claimed production-ready.

- Challenge: the DNS path needed to distinguish generic-port direct dialing from signed discv5 compatibility.
  - Resolution: aligned direct RLPx conversion with geth/Nethermind generic port fallback while keeping signed discv5 seeds restricted to same-family UDP fields so Reth starts cleanly.
  - Remaining: public mainnet EL strict IPv6 still needs real IPv6 execution bootnodes or another candidate source.

- Challenge: DNS ENRs with compatible IPv6 UDP but no compatible IPv6 TCP would have been unusable for discovery seeding.
  - Resolution: split DNS ENR handling so `ip6 + udp6` records seed Reth discv5 as signed ENRs, while direct RLPx candidates use geth/Nethermind-compatible `tcp6` or generic `tcp` fallback.
  - Remaining: the latest controlled proof validates the classification and IPv6 transport; public strict IPv6 historical sync still needs reliable IPv6 execution peers.

- Challenge: the strict public IPv6 proof submitted fewer execution dials than the decoded DNS tree suggested.
  - Resolution: decoded the live DNS tree, confirmed 114 IPv6 ENRs with generic ports, replaced the bounded DNS update listener with direct service polling, increased the cache size, and queued the initial DNS bootnodes directly. The follow-up proof submitted 45 IPv6 EL dials instead of 21 while remaining socket-clean.
  - Remaining: all public IPv6 EL dials still expired, so the unresolved issue is public execution peer reachability rather than LogEx losing DNS candidates.

- Challenge: the raw execution-network counters were enough for debugging but not clear enough for operators.
  - Resolution: `/status` now adds a derived bootstrap warning when candidates exist, repeated submitted dials expire, and no execution sessions are accepted, or when DNS returns only wrong-family candidates.
  - Remaining: the warning is diagnostic; it does not by itself make public strict IPv6-only EL sync possible.

- Challenge: a full public IPv6 execution DNS audit still did not produce a usable mainnet EL peer.
  - Resolution: crawled the full geth DNS tree, tested all IPv6 TCP endpoints from the droplet, explicitly seeded the two TCP-reachable signed ENRs, and confirmed they still did not become accepted Eth sessions while CL remained healthy and no IPv4 sockets were opened.
  - Remaining: pure public strict IPv6 EL sync needs a better IPv6 execution peer source or a product decision to scope strict IPv6 as explicit-bootnode/advanced mode.

- Challenge: the strict IPv6-only proof initially failed before P2P startup because the droplet resolver path used the local IPv4 systemd-resolved stub and the test firewall rejected IPv4 for the LogEx runtime user.
  - Resolution: reran the proof with a temporary IPv6 resolver file and restored the systemd-resolved symlink afterward.
  - Remaining: no production code change is required for this test-host artifact, but strict IPv6 deployments need working IPv6 DNS or an already resolved checkpoint source.

- Challenge: an IPv6-capable checkpoint provider used for strict IPv6 testing did not expose the finalized header/block shape LogEx previously expected.
  - Resolution: added a finalized-state `finality_checkpoints` fallback and focused parser tests, then verified strict IPv6 startup against the provider on the droplet.
  - Remaining: resolved for startup; default checkpoint resolution now uses a 2-of-3 IPv6-capable quorum.

- Challenge: multi-source checkpoint quorum failed against providers that support finalized checkpoints but not `/eth/v1/beacon/headers/{slot}`.
  - Resolution: generalized the block/root fallback to any block id, added a local HTTP fallback test, and verified default strict IPv6 startup on the droplet without passing a checkpoint URL.
  - Remaining: no checkpoint-source blocker remains for IPv6 startup.

- Challenge: default NAT discovery could misclassify CGNAT/private IPv4 as publicly reachable.
  - Resolution: default `--nat any` now checks locally owned default-route IPv4/IPv6 addresses and filters private, shared, documentation, link-local, multicast, and reserved ranges before advertising an external address.
  - Remaining: true dual-stack operation is not implemented yet; when both public families exist the current selector still chooses IPv4.

- Challenge: the previous single-family filter dropped IPv6 outbound candidates on hosts whose primary advertised address was IPv4.
  - Resolution: added an explicit outbound dial-family policy so known peers, DNS candidates, and configured execution bootnodes can be filtered by allowed outbound families rather than only the listener bind family.
  - Remaining: this does not create a second advertised IPv6 execution identity; full dual-stack inbound/discovery still requires a larger network-manager design.

- Challenge: a strict IPv6 proof still logged CL QUIC sends to IPv4-mapped IPv6 destinations.
  - Resolution: CL dial-address classification now treats `/ip6/::ffff:*` as neither IPv4 nor IPv6, so those addresses are removed by both IPv4 and IPv6 bind-family filters before dialing. Focused tests cover TCP and QUIC mapped-address rejection.
  - Remaining: resolved for CL dialing; public strict IPv6 EL historical sync still depends on reliable IPv6 execution peers.

## Dead Code and Obsolescence Cleanup

- Reverted rejected chunk-size and partial-flush timing experiments before this pass.
- Current branch contains only accepted scheduler changes: stale-work critical refill, ready-plan buffering, expected-fetch priority, non-blocking post-write refill, proactive expected refill, active-download-aware post-write refill, expected-child mismatch cleanup, and adaptive write-path active refill.
- Reverted rejected completed-buffer overflow, eight-lane active-target, and shorter expected-hedge experiments before committing.
- Reverted rejected lookahead-promotion and partial-prefix salvage skip experiments locally and remotely.
- Reverted rejected wider full-priority candidate-pool experiment locally and remotely after it worsened low/zero windows.
- Inspected the priority-budget diff for stale experiment leftovers; no obsolete code remained beyond rejected experiment reverts.
- Reverted the rejected dense-prefix yield-size experiment locally and remotely before accepting the prefix-redundancy change.
- Inspected the new redundancy path for rejected experiment leftovers; no stale dense-prefix code remains.
- Fixed clippy-only issues in the scheduler and body/receipt tests; no functional dead code was removed in this pass.
- Inspected the peer-family probe change for experimental leftovers; it is limited to candidate truncation and focused tests.
- Reverted the rejected 256-block progress-target experiment locally and remotely before keeping the salvage-gate change.
- Inspected the salvage-gate diff for obsolete experiment leftovers; no rejected progress-target code remains.
- Reverted the rejected 2 second full-priority partial-prefix flush and wider dense active-pipeline experiments locally and remotely; no code from either experiment remains.
- Reverted the rejected residual carry-forward experiment locally and remotely; the obsolete contiguous-progress helper removal was also reverted with the experiment.
- Reverted the rejected async residual body/receipt carry-forward experiment locally and remotely; no code from that candidate remains.
- Inspected the accepted scheduler after the warmed baseline; no new dead code was introduced because the async residual candidate was fully reverted.
- Inspected `local-ops/start-fresh-baseline-run.sh`; it remains the guarded path for the required fresh baseline and was not run because it deletes/moves the remote data directory.
- Rechecked the guarded fresh-baseline reset path before running it; no obsolete production code was removed in this pass.
- Inspected background storage maintenance after the stall and removed its ability to run compaction planning concurrently with incomplete historical sync.
- Rechecked the roadmap after baseline completion and removed obsolete fresh-run TODO criteria.
- No production code was identified as safe to remove beyond stale experiment cleanup.
- Inspected the IPv6 branch for experimental leftovers; the remaining code is scoped to explicit bind-family support, family-aware discovery filtering, consensus-head startup readiness, and focused tests.
- Rechecked the IPv6 branch after the controlled proof; the remaining code is scoped to family-aware DNS/discv4/discv5 seeding, explicit execution bootnodes, configurable execution discv5 ports, and stale submitted-dial pruning.
- Rechecked the automatic address-selection diff; the remaining code is scoped to startup selection helpers, public address classifiers, focused tests, and CLI help text.
- Rechecked the latest IPv6 diagnostics diff; it is limited to cumulative execution-network counters and status fixture coverage.
- Rechecked the outbound dial-family diff; no rejected experiment code was present and changes are limited to family filtering, DNS candidate conversion, status exposure, and focused tests.
- Rechecked the strict IPv6 warning diff; no obsolete code was introduced and temporary droplet proof data was removed after validation.
- Rechecked the automatic routed-family diff; it is limited to startup selection helpers and focused tests. Temporary candidate-probe/default-smoke data was removed from the IPv6 droplet.
- Rechecked the REST bootstrap-warning diff; it is limited to derived status serialization and focused tests. The strict IPv6 status-smoke artifacts were removed from the droplet.
- Rechecked the checkpoint fallback diff; it is limited to finalized checkpoint response parsing and endpoint fallback order. The remaining temporary strict IPv6 smoke-test data dir was removed from the droplet.
- Rechecked the default checkpoint quorum diff; it is limited to source defaults, block/root fallback for numeric slot checks, CLI/README wording, and focused tests. The temporary default-checkpoint smoke data and copied binary were removed from the droplet.
- Rechecked the strict IPv6 DNS filtering diff; it is limited to rejecting unusable IPv6 DNS records earlier plus trace-only candidate-field diagnostics. The bounded droplet proof left no LogEx process, owner firewall rule, resolver override, temp data dir, or copied test binary behind.
- Rechecked the current-commit controlled IPv6 proof cleanup; no LogEx/geth/reth process, owner firewall rule, resolver override, temporary proof data, copied binary, source tree, or remote target cache remains on the droplet.
- Rechecked the signed-ENR bootnode diff; it is limited to parsing/using signed ENR bootnodes, strict family filtering, CLI/README wording, and focused tests. The signed-ENR droplet proof left no LogEx/geth/reth process, owner firewall rule, resolver override, source tree, target cache, copied binary, proof data, or proof log behind.
- Rechecked the DNS signed-ENR seed diff; it is limited to preserving UDP-capable DNS ENRs for discv5 while keeping direct RLPx dials TCP-gated. The latest bounded droplet proof left no LogEx/geth/reth process, owner firewall rule, source tree, target cache, copied binary, proof data, or proof log behind.
- Rechecked the P2P family status diff; it is limited to status serialization, REST output, and focused tests. No obsolete runtime code was removed.
- Rechecked the IPv4-mapped CL dial-filter and IPv6 ENR fallback changes; they are limited to address-family classification, direct RLPx candidate conversion, signed discv5 admission, and focused tests. The latest droplet proof left no LogEx/geth/reth process, owner firewall rule, proof data, or proof log behind.
- Removed the obsolete DNS discovery task/listener path from the execution peer manager; DNS events are now drained directly from the discovery service, and temporary strict IPv6 proof artifacts were removed from the droplet after validation.
- Rechecked the repeated-expiration bootstrap-warning change; it is limited to derived REST status text and focused tests. The latest droplet source/build cache, proof data, resolver override, owner firewall rule, and local temporary DNS-audit scripts are not project code.

## Git Workflow

- Current branch: `fix/ipv6-p2p-sync`.
- New branch created this run: no; continued the existing IPv6 validation branch.
- Commits made during this branch so far: `fix: add execution ipv6 bootnode support`; `fix: auto-select usable p2p address family`; `fix: expose p2p address selection status`; `fix: expose execution discovery diagnostics`; `docs: record ipv6 execution peer comparison`; `fix: support dual-family outbound peer dials`; `fix: warn on strict ipv6 execution bootstrap`; `fix: keep routed p2p families in auto mode`; `fix: report execution bootstrap warnings`; `docs: record strict ipv6 proof results`; `fix: support finalized checkpoint fallback`; `fix: use ipv6-capable checkpoint quorum`; `fix: require ipv6 execution dns endpoints`; `docs: record controlled ipv6 proof`; `fix: support signed execution bootnode enrs`; `fix: seed ipv6 execution discovery from dns enrs`; `fix: report p2p listen and advertised families`; `fix: reject ipv4-mapped consensus dial addresses`.
- Commits made this run: `fix: queue ipv6 dns bootnodes for direct dials`; `fix: warn on repeated execution dial expiry`.
- Pull request status: no IPv6 PR yet; the task is not complete because EL IPv6 sync is not proven.
- Merge status: not merged.
- Blockers: pure IPv6 EL mainnet peer availability is unresolved; GitHub Actions quota is unavailable for hosted validation.

## Known Issues or Risks

- The completed run is not a clean wall-clock benchmark because it includes the known pre-fix two-hour stall and restart, but post-fix liveness is validated.
- PR #97 cannot be merged until GitHub Actions quota is restored and the hosted checks can run.
- Pure IPv6 EL sync may be impractical on current public mainnet peer availability without a better IPv6 execution peer source; LogEx now submits public IPv6 DNS candidates directly, but all submitted public IPv6 EL dials still expired. Do not claim production-ready IPv6 EL historical sync until this is proven or clearly scoped.
- Automatic single-family selection is implemented for public IPv4, public IPv6 fallback, and outbound-only fallback. True simultaneous IPv4+IPv6 operation is not implemented yet.
- Optional build-cache setup is documented in `ROADMAP.md`; generated `sccache`/`cargo-chef` artifacts should stay outside Git and be rebuilt per target/toolchain.
- Peer count and routing mode affect comparability; record both for any future benchmark.
- A global live chunk scheduler would be a material architecture change; do not start it unless a future post-fix run shows repeated low/zero-progress windows that cannot be explained by network, disk, or density changes.
