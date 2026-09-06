# Dependency baseline

Audit date: 2026-09-07. Base revision: `572d4f61`.

`cargo-audit 0.22.2` examined 847 packages in the original lockfile using
RustSec database commit `5a0ebedfe8bdd2e295b171f4162f8c977bcad9a5`
(1,239 advisories, updated 2026-09-02). No advisories were ignored. The command
exited nonzero because it found ten advisory/package matches. This is an
inventory; dependency presence alone does not establish exploitability.

## Vulnerability matches

| Advisory | Locked package | Patched requirement |
| --- | --- | --- |
| [RUSTSEC-2026-0204](https://rustsec.org/advisories/RUSTSEC-2026-0204.html) | crossbeam-epoch 0.9.18 | >=0.9.20 |
| [RUSTSEC-2026-0258](https://rustsec.org/advisories/RUSTSEC-2026-0258.html) | h2 0.4.13 | >=0.4.16 |
| [RUSTSEC-2026-0119](https://rustsec.org/advisories/RUSTSEC-2026-0119.html) | hickory-proto 0.25.2 | >=0.26.1 |
| [RUSTSEC-2026-0118](https://rustsec.org/advisories/RUSTSEC-2026-0118.html) | hickory-proto 0.25.2 | No patched release listed |
| [RUSTSEC-2026-0185](https://rustsec.org/advisories/RUSTSEC-2026-0185.html) | quinn-proto 0.11.14 | >=0.11.15 |
| [RUSTSEC-2026-0220](https://rustsec.org/advisories/RUSTSEC-2026-0220.html) | ruint 1.17.2 | >=1.20.0 |
| [RUSTSEC-2026-0104](https://rustsec.org/advisories/RUSTSEC-2026-0104.html) | rustls-webpki 0.103.10 | >=0.103.13, <0.104.0-alpha.1; >=0.104.0-alpha.7 |
| [RUSTSEC-2026-0098](https://rustsec.org/advisories/RUSTSEC-2026-0098.html) | rustls-webpki 0.103.10 | >=0.103.12, <0.104.0-alpha.1; >=0.104.0-alpha.6 |
| [RUSTSEC-2026-0099](https://rustsec.org/advisories/RUSTSEC-2026-0099.html) | rustls-webpki 0.103.10 | >=0.103.12, <0.104.0-alpha.1; >=0.104.0-alpha.6 |
| [RUSTSEC-2025-0055](https://rustsec.org/advisories/RUSTSEC-2025-0055.html) | tracing-subscriber 0.2.25 | >=0.3.20 |

## Additional warnings

| Kind | Package | Advisory |
| --- | --- | --- |
| unmaintained | core2 0.4.0 | [RUSTSEC-2026-0105](https://rustsec.org/advisories/RUSTSEC-2026-0105.html) |
| unmaintained | derivative 2.2.0 | [RUSTSEC-2024-0388](https://rustsec.org/advisories/RUSTSEC-2024-0388.html) |
| unmaintained | paste 1.0.15 | [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436.html) |
| unmaintained | proc-macro-error2 2.0.1 | [RUSTSEC-2026-0173](https://rustsec.org/advisories/RUSTSEC-2026-0173.html) |
| unsound | anyhow 1.0.102 | [RUSTSEC-2026-0190](https://rustsec.org/advisories/RUSTSEC-2026-0190.html) |
| unsound | event-listener 5.4.1 | [RUSTSEC-2026-0221](https://rustsec.org/advisories/RUSTSEC-2026-0221.html) |
| unsound | lru 0.16.3 | [RUSTSEC-2026-0253](https://rustsec.org/advisories/RUSTSEC-2026-0253.html) |
| unsound | rand 0.8.5 | [RUSTSEC-2026-0097](https://rustsec.org/advisories/RUSTSEC-2026-0097.html) |
| unsound | rand 0.9.2 | [RUSTSEC-2026-0097](https://rustsec.org/advisories/RUSTSEC-2026-0097.html) |
| yanked | core2 0.4.0 | Registry yanked version |
| yanked | fastrand 2.4.0 | Registry yanked version |

## Reachability and next work

- `hickory-proto 0.25.2` is in the active native graph through both libp2p DNS
  and Reth DNS discovery. The inspected macOS feature graph does not enable
  DNSSEC. The NSEC3 advisory is therefore not proof of an active DNSSEC attack
  path; verify target-specific feature graphs before choosing remediation.
- `tracing-subscriber 0.2.25` is present in the lockfile but `cargo tree
  --workspace --edges normal,build -i tracing-subscriber@0.2.25` reports no
  active macOS dependency; repeating with `--target all` also reports none.
  Do not confuse it with the active 0.3.23 logger. Remove stale lock entries
  only through Cargo and repeat the advisory scan afterward.
- Prioritize transport and certificate parsing advisories, then arithmetic and
  unsoundness fixes. First test compatible package updates with the existing
  Reth tag and libp2p version. Do not silently upgrade the networking stack or
  ignore advisories to obtain a green report.
- Review unmaintained/yanked dependencies with their reverse dependency trees;
  distinguish build-only code from runtime exposure. Record any unavoidable
  remaining warning and the exact upstream constraint.

## Features and build surface

- The workspace uses edition 2024 and `nightly-2026-08-24`; CI retains this pin.
- Reth crates share tag `v1.11.3`. Verify compatibility as a group before any
  revision change. Alloy 1.8, DataFusion 51, and libp2p 0.56 are major integration
  boundaries, not candidates for a speculative wholesale update.
- libp2p disables defaults and explicitly enables DNS, gossip, identify,
  TCP/QUIC, request/response, noise, yamux, Tokio, and macros.
- reqwest disables defaults and enables JSON and rustls TLS. Tokio currently
  enables `full`; inventory actual feature consumers before narrowing it.
- DataFusion retains defaults, including file-format dependencies. SQL is
  guarded by `enforce_read_only_sql` and `validate_supported_tables`; the query
  audit must test those guards rather than assuming a dependency grants access.
- `logex-server/build.rs` compiles the tracked protobuf with tonic-build and
  requires protoc. Generated artifacts live under Cargo output directories.
- Linux GNU enables jemalloc while the Mac uses its system allocator. Compare
  performance within one target before comparing between targets.

## Reproduce

Install the tool outside the repository; its dependencies do not enter the
LogEx lockfile:

```sh
cargo install cargo-audit --version 0.22.2 --locked
cargo audit --json
cargo tree --workspace --locked --target all --edges normal,build
cargo tree --workspace --locked --edges features -i hickory-proto@0.25.2
```

The [RustSec database](https://github.com/RustSec/advisory-db) changes over time;
record its commit and the lockfile digest with every follow-up report.
