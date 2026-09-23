# LogEx gossipsub patch provenance

The complete published `libp2p-gossipsub` 0.49.4 package is retained here, including
original manifests, published lockfile inventory, tests, changelog, VCS information
and MIT license notices in the source files.

- crates.io archive SHA-256: `a538e571cd38f504f761c61b8f79127489ea7a7d6f05c41ca15d31ffb5726326`
- Upstream VCS commit: `2f10abba1516dcb6d6f20274de2ccba177ce5806`
- Original file inventory: `LOGEX-UPSTREAM-SHA256`
- Exact local diff: `LOGEX-PATCH.diff`

Implementation decisions, regression evidence and validation results are recorded
in the implementation pull requests. `LOGEX-PATCH.diff` is the complete change
against the published package; `tools/verify_datafusion_vendor.py` validates its
source inventory and reviewed modifications offline. Current cache and queue
limits are documented on `CacheLimits` and `QueueLimits` in the source.

Run the supplementary dependency regressions with:

```sh
CARGO_TARGET_DIR=target/gossipsub-regressions cargo test --manifest-path vendor/libp2p-gossipsub/Cargo.toml --lib logex_ --locked
```

Linux/macOS CI runs these controls alongside workspace checks. The standalone
crate uses its own lockfile; workspace CL tests/checks exercise the application's
graph. With dependencies cached, add `--offline`. CI explicitly formats the
patched vendor Rust files because this package is excluded from the workspace.

The published normalized manifest omitted the upstream `quickcheck` test
edge. The local manifest restores it at 1.0.3; its standalone lockfile also adds
env_logger 0.8.4 and uses lock format 4, without changing earlier locked versions.
The retained upstream integration `smoke` target requires test support
(`libp2p-swarm-test`) omitted from the published manifest; it is not part of the
standalone library gate.
The original sources, manifest and lockfile are recoverable from the inventory
and complete diff. The library tests and strict library Clippy remain required.
