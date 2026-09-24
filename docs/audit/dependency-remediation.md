# Compatible dependency security updates

Initial remediation base: merged audit baseline `afe5939c` (PR #120). The pinned toolchain, Reth
`v1.11.3`, libp2p `0.56.0`, and public/storage interfaces are unchanged.

## Final offline recheck — 2026-09-25

The final review starts from PR #277 (`b93897f1`). Official cargo-audit 0.22.2,
with RustSec database `593df8c1b5ed0bcde9dddadfeeead776fa514ff8` (updated
2026-09-24), reports four vulnerability/package matches across the 857 locked
packages before remediation. The new match is
[RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285.html):
Rustls 0.23.37 accepts a TLS handshake transition that should be rejected.
The upstream advisory does not claim that this bypasses transcript authentication.

Rustls is updated to the compatible
[0.23.45 release](https://github.com/rustls/rustls/releases/tag/v%2F0.23.45).
Cargo changes only its version and checksum; the complete active reverse feature
graph is identical after substituting the version. Reqwest HTTPS and the CL
QUIC/TLS integration share this dependency. The pinned compiler, crypto provider,
certificate policy and other dependency versions are unchanged. This is a
correctness fix, with no claimed ingestion or query speedup.

The same-database recheck returns three vulnerability/package matches, three
unmaintained warnings and one unsoundness warning, with no yanked-package warnings
and no suppressed advisory IDs. Its exit code remains 1. The three remaining
matches and four warnings have the scoped dispositions in the table below;
this is not a clean advisory-counter result.

Fresh macOS ARM64 and Linux x86-64 feature graphs still exclude Hickory DNSSEC.
The final CL/EL source review traces libp2p DNS dialing, `JoiningDnsResolver` and
Reth DNS discovery through lookup APIs that construct one-query outbound
messages. UDP and TCP receive paths parse `DnsResponse::from_buffer`; TCP fallback
reuses the original request. Cache response-encoding sites are test-only. Neither
of the two reported Hickory prerequisites was found in these production roles.
This does not provide DNSSEC authentication or establish safety of every Hickory
API. Reassess when resolver roles, features or dependencies change.

All-target reverse graphs reconfirm that tracing-subscriber 0.2.25, lru 0.16.4
and derivative 2.2.0 are inactive. Paste and proc-macro-error2 remain active
build-time dependencies. The separate vendored Tonic lockfile retains its
upstream optional TLS packages, but neither default nor gzip test graphs activate
Rustls; production uses the patched root lockfile. Enabling additional standalone
Tonic features requires a separate dependency review. No lockfile entries are
manually removed to hide findings.

The closing audit PR records the exact final commit and validation results.
Earlier measurements and validation below describe PR #121, not a fresh
performance comparison of this TLS patch.

## Changes

| Dependency | Before | After | Reason |
| --- | --- | --- | --- |
| crossbeam-epoch | 0.9.18 | 0.9.21 | Invalid-pointer formatting advisory |
| h2 | 0.4.13 | 0.4.19 | Unbounded empty DATA-frame handling |
| quinn-proto | 0.11.14 | 0.11.17 | Out-of-order stream reassembly memory exhaustion |
| ruint | 1.17.2 | 1.20.0 | Shift truncation and overflow-flag correctness |
| rustls-webpki | 0.103.10 | 0.103.15 | CRL parsing panic and URI/wildcard name constraints |
| anyhow | 1.0.102 | 1.0.104 | Unsoundness warning |
| event-listener | 5.4.1 | 5.4.2 | Unsoundness warning |
| rand | 0.8.5 / 0.9.2 | 0.8.8 / 0.9.5 | Thread-local RNG/custom logger aliasing warning |
| fastrand | 2.4.0 | 2.5.0 | Replace yanked release |
| multihash | 0.19.3 | 0.19.5 | Remove active yanked/unmaintained core2 0.4.0 |
| lru | 0.16.3 | 0.16.4 | Latest compatible patch; does **not** resolve the warning below |

Cargo selected these within existing manifest constraints. Additional ark/rand
lockfile entries are resolver-selected transitive dependencies, not new direct
LogEx dependencies. No manual lockfile pruning or registry-source overrides were
used. A workspace-update dry run found no stale entries it would remove.

## Advisory outcome and remaining constraints

Using cargo-audit 0.22.2 and the same RustSec database commit as the baseline
(`5a0ebedfe8bdd2e295b171f4162f8c977bcad9a5`), vulnerability/package matches fall
from ten to three. All yanked-package warnings disappear. Three unmaintained
warnings and one unsoundness warning remain. No advisory IDs are suppressed;
`cargo audit` still exits nonzero. This is not an all-clear security report.

| Remaining finding | Evidence and disposition |
| --- | --- |
| [RUSTSEC-2026-0118](https://rustsec.org/advisories/RUSTSEC-2026-0118.html), hickory-proto 0.25.2 | Requires DNSSEC validation. macOS and Linux feature graphs enable only std, Tokio, and futures I/O; `hickory-resolver::ResolverBuilder::build` compiles the secure handle only under `__dnssec`. That path is not built in the inspected targets. |
| [RUSTSEC-2026-0119](https://rustsec.org/advisories/RUSTSEC-2026-0119.html), hickory-proto 0.25.2 | Concerns encoding messages with many records. The active resolver builds one-query requests; its UDP and TCP/multiplexer receive paths use `DnsResponse::from_buffer`, which parses received bytes without re-encoding them. `DnsResponse::from_message` calls in the inspected resolver cache are test-only. No attacker-controlled many-record encoding path was identified in this use; revisit during CL/EL DNS review and whenever DNS roles/features change. |
| [RUSTSEC-2025-0055](https://rustsec.org/advisories/RUSTSEC-2025-0055.html), tracing-subscriber 0.2.25 | No active reverse dependency under `cargo tree --workspace --edges all --target all`. The active LogEx logger is 0.3.23. Retain Cargo's lock resolution rather than deleting an inactive entry manually. |
| [RUSTSEC-2026-0253](https://rustsec.org/advisories/RUSTSEC-2026-0253.html), lru 0.16.4 | The latest compatible patch remains affected; fix requires >=0.18.2. No active reverse dependency under the same all-target graph. The reported panic-safety issue requires a key destructor panic and reuse of the cache after unwinding. Reassess before enabling a feature that activates this dependency. |
| derivative 2.2.0 | Unmaintained; absent from the active all-target graph. |
| paste 1.0.15 and proc-macro-error2 2.0.1 | Unmaintained build-time macros used through Alloy/DataFusion/Reth dependencies. Their replacement belongs with tested upstream integration changes, not an unreviewed manifest substitution. |

The Hickory fixes are on the 0.26 line while current Reth/libp2p constrain the
0.25 integration. Do not force a major resolver replacement into the current
network stack solely to change an advisory counter. These reachability findings
are scoped to the inspected build and source paths, not a proof about every
possible feature combination. The final recheck above closes the CL/EL DNS-path
review for these features.

## Verification

All six agreed local merge gates passed, including 720 tests and release node
linking. Sixty alternating release fixture runs passed exact result checks; see
[comparison and accepted performance costs](baselines/2026-09-08-dependencies.md).
Dense historical writes show a +5.84% combined median increase requiring later
write-path profiling. These security fixes are retained with that disclosed
tradeoff. The advisory tool's nonzero exit remains expected and explicitly reported.

Useful evidence commands:

```sh
cargo tree --workspace --locked --target aarch64-apple-darwin --edges features -i hickory-proto@0.25.2
cargo tree --workspace --locked --target x86_64-unknown-linux-gnu --edges features -i hickory-proto@0.25.2
cargo tree --workspace --locked --target all --edges all -i tracing-subscriber@0.2.25
cargo tree --workspace --locked --target all --edges all -i lru@0.16.4
cargo tree --workspace --locked --target all --edges all -i derivative@2.2.0
cargo audit --json
```

For source review, inspect hickory-proto 0.25.2's `xfer/dns_handle.rs`,
`xfer/dns_response.rs`, `udp/udp_client_stream.rs` and `xfer/dns_multiplexer.rs`,
and hickory-resolver 0.25.2's `resolver.rs` and `caching_client.rs`.
