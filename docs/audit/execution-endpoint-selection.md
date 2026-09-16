# Execution TCP and discovery endpoint selection

## Findings

**B4-37, medium — an unusable preferred TCP endpoint hides a usable alternative.**
LogEx already rejects TCP port zero when admitting a pending dial. Its earlier
ENR selection paths nevertheless chose such an endpoint before considering the
other permitted address family. Configured signed records, DNS conversion and
initial bootstrap selection could therefore discard a peer with a usable alternate
TCP endpoint. A single-family configuration must still respect its family limit.

**B4-38, low — UDP availability is conflated with TCP availability.** The Reth
`NodeRecord::new_with_ports` constructor substitutes TCP when its UDP argument is
`None`. Passing an ENR's missing UDP field straight through therefore invented a
UDP endpoint. Discovery selection also accepted zero UDP ports, and configured
unsigned discovery seeds were derived from whichever TCP family had been chosen.
That could omit the discovery listener's usable UDP endpoint on the other family.

[EIP-778](https://eips.ethereum.org/EIPS/eip-778) defines TCP and UDP as separate
optional fields. Its IPv6 fallback maps each protocol to its corresponding generic
port; it does not define a fallback from UDP to TCP. Records without a TCP endpoint
can still contain useful UDP discovery information.

## Correction and preserved behavior

Direct ENR selection excludes TCP zero before applying IPv4 preference or the
existing DNS family split. The default DNS record and initial cross-family candidate
selection follow the same rule. Disabled families remain disabled; explicit IPv6
ports retain precedence over generic ports, including an explicit zero port.

Discovery seeds are collected independently for the discovery bind family. They
require nonzero UDP and permit absent/zero TCP. This preserves UDP-only records
when rejecting unusable TCP candidates and supplies configured seeds to both the
existing discv4 and unsigned discv5 paths. DNS and configured records share the
small discovery-record constructor. Their original signed identity is preserved.

ENR conversions explicitly represent absent UDP as zero in `NodeRecord`, avoiding
the constructor's bare-enode default. TCP dial submission passes no optional UDP
address when that field is zero. Bare enode parsing keeps its existing default;
signed records are not rewritten or re-signed. Pinned discv5's signed-record path
still requires explicit IPv6 UDP metadata; the existing unsigned-address path can
use the correctly resolved generic IPv6 UDP endpoint. The separate consensus-side
IPv6 fallback review remains open.

The old vector of selected TCP candidates became redundant once discovery seeds
were separate: only its length was read. It is replaced by a count. The final
pending-dial TCP-zero guard remains necessary for other input paths and is retained.
There are no new tasks, per-block operations, persisted formats or dependency changes.
No throughput measurement is claimed.

## Reproductions, review and validation

Five original controls all failed against unchanged production at `eb326cfb`.
The first narrow TCP-only candidate passed those controls, but review identified a
UDP-only discovery regression: discovery had reused the direct-TCP converter. A
new preservation control failed on that candidate before it was committed. Two
additional controls reproduced invented/zero UDP endpoints on the unchanged UDP
logic. That run had five passing controls and three failures: two existing UDP
issues and the candidate's preservation regression.

Separating the paths resolves those cases. Three existing assertions were updated:
two had disallowed unsigned discovery for UDP-only records, and one expected the
invented UDP port. Their original failed output is retained. The final focused
suite passes **290 peer-manager tests**, including ten new controls covering both
family-split branches, family restrictions, bootstrap selection, missing and zero
ports, independent discovery bind selection, generic IPv6 UDP and preserved identity.
The pinned Reth unsigned-seed conversion also accepts the UDP-only fixtures.

New controls use deterministic local record fixtures and start no sockets or
network services. The broader existing suite uses its dormant loopback fixture.
Manual source review traced configured enode/ENR admission, DNS collection,
unsigned/signed seed construction, candidate admission and TCP dial submission.
Reth's pinned port default and discovery-address conversion were inspected locally.
This is implementer review, not independent review. Actual remote dialing and a
full node startup were not exercised. No Mac mini or external volume was used.

Full workspace gates and PR/CI/merge are pending. Remaining peer rehabilitation,
request resources and other discovery paths still need their audit dispositions;
this milestone does not close the whole execution networking batch.

[Validation record](baselines/2026-09-16-execution-endpoint-selection.json).
