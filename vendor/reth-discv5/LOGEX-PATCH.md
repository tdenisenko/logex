# LogEx Reth discovery IPv6 TCP correction

This directory contains the complete `crates/net/discv5` source from Reth v1.11.3,
commit `d6324d63e27ef6b7c49cdc9b1977c1b808234c7b`, with that commit's MIT/Apache
licenses and formatting configuration. `LOGEX-UPSTREAM-SHA256` records all twelve
original files; `LOGEX-PATCH.diff` records the exact local differences.

`Cargo.toml` is normalized for standalone dependency use. Original dependency
versions, Git tag identities, features and workspace lints are preserved. The
workspace lockfile controls builds; no package versions are upgraded.

The sole production change in `src/lib.rs` resolves the shared TCP port for IPv6
when `tcp6` is absent. It applies before Reth's existing observed-UDP-port
heuristic. The observed address/UDP endpoint, explicit IPv6 precedence, filtering,
identity and absent-TCP heuristic remain unchanged. The public discovery event
path is tested from LogEx's workspace with an owned ephemeral loopback listener,
no bootnodes and in-process events; no external peer connection is required.

This corrects a caller that reads literal port fields rather than ENR socket
helpers. Fixing the ENR dependency alone would leave its TCP selection wrong.
Modification notices are adjacent to the changed production code. Verify the
source and patch with `python3 tools/verify_datafusion_vendor.py`. Remove this
patch when the pinned upstream adapter handles shared IPv6 TCP ports correctly
and passes the same actual-event regression.
