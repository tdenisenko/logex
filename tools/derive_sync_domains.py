"""Offline derivation of the literal mainnet sync-committee domain fixtures.

Consensus specs v1.6.0:
https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/phase0/beacon-chain.md#forkdata
https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/phase0/beacon-chain.md#compute_fork_data_root
https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/phase0/beacon-chain.md#compute_domain
https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/altair/beacon-chain.md#domain-types
https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/altair/light-client/sync-protocol.md#validate_light_client_update
https://github.com/ethereum/consensus-specs/blob/v1.6.0/configs/mainnet.yaml

The mainnet genesis validators root is pinned independently as a literal from
LogEx's established chain configuration (crates/logex-cl/src/chain.rs). This
derivation checks domain construction and boundary selection, not the external
provenance of that configured trust root. No production code is imported.

SSZ ForkData contains Version (4 bytes right-padded to one 32-byte chunk) and
Root (one 32-byte chunk). Its hash-tree-root is SHA-256 of their concatenation.
Domain = DOMAIN_SYNC_COMMITTEE || fork_data_root[:28]. The signature's fork
version is selected at max(signature_slot, 1) - 1; BPO changes no fork version.
"""

import hashlib
import json

GENESIS_VALIDATORS_ROOT = bytes.fromhex(
    "4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
)
DOMAIN_SYNC_COMMITTEE = bytes.fromhex("07000000")
FORKS = [
    ("bellatrix", 144896, 2),
    ("capella", 194048, 3),
    ("deneb", 269568, 4),
    ("electra", 364032, 5),
    ("fulu", 411392, 6),
]

domains = {}
for name, epoch, version in FORKS:
    version_chunk = bytes([version, 0, 0, 0]) + bytes(28)
    fork_data_root = hashlib.sha256(version_chunk + GENESIS_VALIDATORS_ROOT).digest()
    domains[name] = (DOMAIN_SYNC_COMMITTEE + fork_data_root[:28]).hex()

boundaries = []
for index in range(1, len(FORKS)):
    name, epoch, _ = FORKS[index]
    slot = epoch * 32
    boundaries.append({
        "fork": name,
        "cases": [
            {"signature_slot": s, "domain": domains[FORKS[index - 1][0] if s <= slot else name]}
            for s in (slot - 1, slot, slot + 1)
        ],
    })
for epoch in (412672, 419072):
    boundaries.append({
        "fork": f"bpo_at_{epoch}",
        "cases": [
            {"signature_slot": epoch * 32 + offset, "domain": domains["fulu"]}
            for offset in (-1, 0, 1)
        ],
    })
print(json.dumps({"domains": domains, "boundaries": boundaries}, indent=2))
