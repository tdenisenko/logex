# LogEx gossipsub duplicate-cache expiry correction

The complete published `libp2p-gossipsub` 0.49.4 package is retained here, including
original manifests, published lockfile inventory, tests, changelog, VCS information
and MIT license notices in the source files.

- crates.io archive SHA-256: `a538e571cd38f504f761c61b8f79127489ea7a7d6f05c41ca15d31ffb5726326`
- Upstream VCS commit: `2f10abba1516dcb6d6f20274de2ccba177ce5806`
- Original file inventory: `LOGEX-UPSTREAM-SHA256`
- Exact local diff: `LOGEX-PATCH.diff`

`src/time_cache.rs` now checks the stored deadline on membership reads. An ID is
present only while its deadline is strictly later than the sampled instant.
Previously, membership reads stayed true after expiry until another insertion
pruned the cache, suppressing later IHAVE requests for expired IDs in the absence
of another insertion. This does not establish a delayed usable light-client
payload or consensus-head stall.
`src/behaviour.rs` prunes both duplicate and locally published ID caches on each
completed heartbeat, releasing expired entry ownership without new messages.
First-insertion deadlines, live duplicate suppression and insertion's boolean
meaning are unchanged. Peer-score delivery records still use the same entry
semantics. No capacity limit, wire format, forwarding policy or public API changes.

Three deterministic `logex_expiry` regressions exercise exact expiry/nonrefresh
semantics, the actual in-memory IHAVE behaviour harness, and actual heartbeat
cleanup with live-ID controls. Test clocks use explicit monotonic instants and
small 20-byte IDs; these regressions do not sleep or connect to a network.

The published normalized manifest omitted the upstream `quickcheck` test
dependency although its library tests import that crate. `Cargo.toml` restores
that test-only edge pinned to 1.0.3. The standalone `Cargo.lock` adds quickcheck
1.0.3 and env_logger 0.8.4, and uses Cargo's lock format 4; every previously locked
package version is unchanged. Original content is retained by the inventory and
diff. The upstream smoke target and sources are unchanged.

Run the supplementary dependency regressions with:

```sh
CARGO_TARGET_DIR=target/gossipsub-regressions cargo test --manifest-path vendor/libp2p-gossipsub/Cargo.toml --lib logex_expiry --locked
```

Linux/macOS CI runs this command alongside normal workspace checks. It uses this
package's standalone dependency graph; workspace CL tests/checks exercise the
application's graph. With its dependencies cached, add `--offline`. Formatting
of the three patched Rust files is checked explicitly because vendor packages
are excluded from the workspace. Run `python3 tools/verify_datafusion_vendor.py`
to verify the complete archive inventory and reviewed modifications offline.

The supplementary strict Clippy attempt also surfaces pre-existing upstream
lints outside the changed hunks; these are not suppressed by this patch. The
application's strict CL/workspace Clippy gates remain required.

This correction does not impose a count/byte cap or promise allocator/RSS
shrinkage. Membership expiry works even before the next heartbeat; physical
entry pruning still requires insertion or a polled heartbeat. Unpolled behaviour
and retained collection capacity have separate lifetimes. Remove the local patch
when a pinned upstream release includes equivalent expiry behavior and passes the
same regressions.
