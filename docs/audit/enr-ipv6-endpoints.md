# Shared IPv6 ports in signed node records

## Findings and protocol contract

[EIP-778](https://eips.ethereum.org/EIPS/eip-778) assigns shared TCP/UDP ports to
IPv6 when the corresponding IPv6-specific field is absent. The
[consensus networking specification](https://github.com/ethereum/consensus-specs/blob/master/specs/phase0/p2p-interface.md#enr-structure)
uses that record format. Port inheritance does not require an IPv4 address, does
not exchange TCP and UDP, and must not rewrite the signed record.

- **B3-67 — moderate: valid shared-port IPv6 records were unavailable to consensus
  dialing/discovery.** LogEx read only explicit IPv6 fields; pinned ENR socket
  helpers did the same, so discv5 also could not construct a contact. Three
  controls fail on original production at `6d0c272e`: CL dialing, CL discovery
  admission and actual discv5 contact conversion.
- **B4-47 — moderate: execution signed discovery seeds excluded the same records.**
  Two checks reproduced the deliberate downstream-library workaround in DNS and
  configured seed selection. An original focused run has three passing controls
  and two failures. The unsigned/direct paths already supported shared ports.
- **B4-48 — moderate: execution discovery could dial the observed UDP port instead
  of the advertised shared TCP port.** The pinned Reth adapter used its UDP-port
  heuristic when `tcp6` was absent without first consulting `tcp`. With the ENR
  fix applied but Reth's production method unchanged, its real event-adapter
  control returns TCP 9002 instead of 9001. The reproduction was repeated with
  all unrelated dependency resolutions restored before changing that method.

## Correction and invariants

The pinned `enr` 0.13.0 socket helpers now resolve the IPv6-specific field first,
then the corresponding shared field only when the key is absent. Literal getters
remain literal; explicit zero remains an explicit value. Invalid present values
in builder-created records do not fall back. The wire parser already rejects
out-of-range ports; tests preserve that rejection rather than claiming those
fixtures are wire-valid. Socket helpers still require the matching IP address.

LogEx's CL admission/dial conversion uses the corrected socket helpers. Execution
signed seed checks use the same resolved UDP endpoint, retaining their nonzero
port policy. Four duplicated DNS/signed port helpers are replaced by two generic
helpers shared across the existing key types. Superseded explicit-IPv6 rejection
assertions are replaced with signed/unsigned endpoint and identity equivalence.

Reth v1.11.3's execution adapter resolves shared TCP before its existing
observed-UDP-port heuristic. Its observed address/UDP port, explicit IPv6 override
and missing-TCP heuristic remain unchanged, including events whose record does
not advertise an IPv6 address. The record is never rewritten or re-signed. Peer
IDs, sequence authority, fork filtering and downstream validation remain.

Two narrow vendor patches are necessary: a LogEx-only change would leave discv5
routing/contact verification incorrect, and fixing ENR sockets alone would leave
Reth's literal-field conversion incorrect. The complete published ENR package and
exact pinned Reth discovery source are retained with licenses, original hashes,
reversible local diffs and removal criteria. Reth's manifest only resolves its
existing workspace inheritance. The lockfile changes source locations for these
two packages; every package version and other dependency selection is preserved.
The existing vendor verifier covers both packages, and CI checks their formatting.

No ingestion writes, storage/index formats, query semantics, background services
or production deployment are added. QUIC field semantics and other address
selection policies are unchanged. There is no benchmark or throughput claim.

## Validation and scope

The six consensus endpoint controls pass, including shared and explicit ports,
zero/invalid precedence, TCP/UDP/address-family separation, original signed bytes,
actual discv5 contacts, dual-stack preference and the library's existing mapped
IPv6 policy. All 18 focused execution IPv6 controls pass, covering both key types,
DNS/configured seeds and actual Reth discovery event conversion.

The Reth control starts an owned ephemeral IPv6 loopback listener with no
bootnodes and a five-second startup deadline. It injects ordinary in-process
session events and needs no external peer, live sync or production files. The
per-test runtime owns its services. Its absent-TCP case verifies the existing
heuristic; it does not endorse a new port inference policy.

The full consensus suite passes 360 tests with 1 existing ignored test; all 318 peer-manager tests pass.
Full workspace gates, exact-head Linux/macOS CI and merge remain pending.

Implementer review followed CL admission/retention/dial conversion, discv5 socket
selection/contact and observed-address verification, execution DNS/configured
seeds and Reth's event adapter. Vendor and lockfile changes were checked against
the original cached archive and pinned Git source. No independent review is
claimed. This closes the shared-port conformance findings only after validation
and merge; other discovery, retention, scheduling and resource reviews remain.
Live-network interoperability and release acceptance are subsequent gates.

Machine-readable evidence: [baseline](baselines/2026-09-17-enr-ipv6-endpoints.json).
