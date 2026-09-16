# Local Reth network corrections

Upstream: [Reth v1.11.3](https://github.com/paradigmxyz/reth/tree/d6324d63e27ef6b7c49cdc9b1977c1b808234c7b/crates/net/network),
commit `d6324d63e27ef6b7c49cdc9b1977c1b808234c7b`.

All 54 tracked upstream crate files are retained. `LICENSE-MIT`, `LICENSE-APACHE`
and `rustfmt.toml` are copied unchanged from the same upstream repository root.
`LOGEX-UPSTREAM-SHA256` records these 57 original files. The standalone Cargo
manifest expands inherited package/dependency/lint settings; former workspace
path dependencies use the same git URL and v1.11.3 tag, locked to the original
commit. Package versions and dependency features are preserved.

The reviewed diff contains the manifest normalization, four changed existing Rust
files and one added private helper:

- `session/types.rs`: publish and read range numbers/hash through one tuple lock.
- `session/active.rs`, `session/mod.rs`, `session/range_update.rs`: compare and
  remember the complete advertised range on the existing delayed 384-second
  interval (including its existing missed-tick Delay policy). Queue changed
  earliest bounds, reorg hashes and regressions, then wake the session to flush.
- `eth_requests.rs`: ETH70 pagination permits one receipt above the soft target
  when no progress would otherwise occur, omits unstarted trailing blocks after
  earlier complete blocks, and marks incomplete only when receipts remain.

The 2 MiB response target remains soft. An arbitrary oversized receipt is not
promised to fit a remote hard message limit. No message-size, queue, concurrency,
network version, disk format or trusted-data validation rule is changed.

LogEx tests drive the public request handler and compile the actual private range
source files in a small test-only harness. Full upstream EVM/provider test features
are not enabled merely to exercise these network contracts. The helper tests do
not establish socket delivery or stalled-session liveness.

Run `python3 tools/verify_datafusion_vendor.py` from the repository root to verify
all three vendored crates, exact file sets and reviewed modification/addition/diff
hashes. The legacy command name remains for existing callers. CI also checks the
five changed/added Rust files with this directory's original rustfmt settings.

Review or remove this patch when upgrading Reth after confirming equivalent
upstream fixes and passing the LogEx contract regressions. Do not silently refresh
vendored files or change the lock to a different Reth commit.
