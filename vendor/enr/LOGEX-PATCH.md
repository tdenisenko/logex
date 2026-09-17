# LogEx ENR IPv6 endpoint correction

The complete published `enr` 0.13.0 package is retained here, including its MIT
license, original manifests, README, source, tests and VCS information.

- crates.io archive SHA-256: `851bd664a3d3a3c175cff92b2f0df02df3c541b4895d0ae307611827aae46152`
- Upstream VCS commit: `6f2b05d634c4ddfe303b14c3b9075de796764ae0`
- Original file inventory: `LOGEX-UPSTREAM-SHA256`
- Exact local diff: `LOGEX-PATCH.diff`

Only `src/lib.rs` changes. IPv6 TCP/UDP socket helpers inherit the corresponding
shared port when the IPv6-specific key is absent, as required by
[EIP-778](https://eips.ethereum.org/EIPS/eip-778). Specific values retain
precedence, including zero. An invalid present field does not select the shared
port. Address presence remains required for a socket. Literal field getters,
signing, serialization and record validation are unchanged.

The patch belongs here because discv5 uses these helpers for routing and observed
endpoint verification, beyond LogEx's own admission code. No ENR is rewritten or
re-signed. Regression controls live in the LogEx consensus/execution workspace
tests so the normal Linux/macOS gates exercise the actual patched dependency.

Run `python3 tools/verify_datafusion_vendor.py` to verify the complete package and
reviewed modification offline. Remove the local patch when a pinned upstream
release implements equivalent behavior and passes the same endpoint controls.
