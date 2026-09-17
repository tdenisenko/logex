# LogEx gossipsub expiry and topic admission corrections

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
semantics. The expiry correction adds no capacity limit or wire-format change.

Three deterministic `logex_expiry` regressions exercise exact expiry/nonrefresh
semantics, the actual in-memory IHAVE behaviour harness, and actual heartbeat
cleanup with live-ID controls. Test clocks use explicit monotonic instants and
small 20-byte IDs; these regressions do not sleep or connect to a network.

## Topic admission

The incoming publication, GRAFT and PRUNE handlers now consult the configured
topic eligibility predicate before retaining message/topic state. Publications
outside that policy are ignored before transformation, hashing, duplicate/cache
insertion, metrics and application events. GRAFT filtering precedes peer-topic
insertion and explicit-peer responses. PRUNE filtering precedes backoff tracking
and peer exchange. Codec-invalid publications also receive the neutral topic
check before scoring or logging. Existing SUBSCRIBE/UNSUBSCRIBE filtering remains.

The `can_subscribe` hook therefore runs once per incoming publication or control
topic, including codec-invalid publications, in addition to subscription calls.
This is an intentional, documented contract extension for this vendored package.
Custom mutable callbacks must act as eligibility predicates, not consumable
quotas. Batch/cardinality subscription hooks remain separate and are not treated
as data/control admission rules. The default AllowAll behavior is preserved.
LogEx configures a static whitelist of its supported light-client fork topics;
active, pre-subscribed and retiring fork validation remains application-owned.

Eight `logex_topic_` controls exercise the actual in-memory handlers: neutral
ineligible receive and codec-invalid scoring, allowed Accept/Reject lifecycle,
regular/explicit and mixed GRAFT, PRUNE/backoff, subscription changes, and
AllowAll compatibility. They use tiny topic sets and payloads without network
connections. On the exact original production handlers, five controls fail and
three compatibility controls pass. The corrected complete library suite passes
162 tests; application consensus validation is checked separately.

The patch does not change the global decoded RPC frame limit or establish a
total cache/queue memory bound. Initial codec allocations still precede the
handler checks. Eligible-topic entry/byte quotas and priority-control queue
ownership remain separate audit work. LogEx also applies its existing message-
type size checks in the data transform; the message-ID algorithm is unchanged.

The published normalized manifest omitted the upstream `quickcheck` test
dependency although its library tests import that crate. `Cargo.toml` restores
that test-only edge pinned to 1.0.3. The standalone `Cargo.lock` adds quickcheck
1.0.3 and env_logger 0.8.4, and uses Cargo's lock format 4; every previously locked
package version is unchanged. Original content is retained by the inventory and
diff. The upstream smoke target and sources are unchanged.

Run the supplementary dependency regressions with:

```sh
CARGO_TARGET_DIR=target/gossipsub-regressions cargo test --manifest-path vendor/libp2p-gossipsub/Cargo.toml --lib logex_ --locked
```

Linux/macOS CI runs this command alongside normal workspace checks. It uses this
package's standalone dependency graph; workspace CL tests/checks exercise the
application's graph. With its dependencies cached, add `--offline`. Formatting
of the four patched Rust files is checked explicitly because vendor packages
are excluded from the workspace. Run `python3 tools/verify_datafusion_vendor.py`
to verify the complete archive inventory and reviewed modifications offline.

The supplementary strict Clippy attempt also surfaces pre-existing upstream
lints outside the changed hunks; these are not suppressed by this patch. The
application's strict CL/workspace Clippy gates remain required.

This correction does not impose a count/byte cap or promise allocator/RSS
shrinkage. Membership expiry works even before the next heartbeat; physical
entry pruning still requires insertion or a polled heartbeat. Unpolled behaviour
and retained collection capacity have separate lifetimes. Remove the local patch
when a pinned upstream release includes equivalent expiry and topic-admission
behavior and passes the same regressions.
