# Trust-path boundaries and repeated beacon hashing

This batch follows merged PR #152 (`733a248a`) on
`audit/trust-path-completion`. It reviews shared row types, checkpoint selection,
CL verification and execution validation callers. It is a bounded milestone;
persisted-state durability, network liveness and complete sync lifecycle remain
open. No live peers, production data or additional benchmark runs were used.

## Findings and changes

- **B1-20 (P2, local input consistency):** historical validation job construction
  silently dropped supplied payloads without corresponding headers and substituted
  a zero hash when a corresponding hash was absent. Both the parallel validator
  and streaming validator/extractor used this helper. Two new tests failed before
  the fix on missing hashes. Require equal header/hash counts and no more payloads
  than headers before creating jobs. Preserve intentional shorter payload prefixes
  and empty work. Propagate failures through the existing local error channel,
  without attributing caller-shape errors to peers. Examined production callers
  already derive matching header/hash slices; no authenticated peer bypass is
  demonstrated. These two constant-time count checks add no header rehash, log
  traversal, storage operation or synchronization.
- **B1-21 (implementation cost):** decoded Electra/Fulu beacon blocks computed
  their complete body/header root, then recomputed it inside the single-caller
  execution-anchor helper. Both context and no-context paths shared this work.
  Remove the helper and use the already computed root and header slot for the
  anchor. Existing exact-root and fork-context fixtures still pass. This removes
  one complete body-hashing pass; no elapsed-time or ingestion-rate gain is claimed.
- **B1-22 (documentation):** the consensus-store age limit was named and described
  as universally conservative. The Electra reference period depends on active
  balance and churn. Rename it to `MAINNET_WEAK_SUBJECTIVITY_MAX_AGE_EPOCHS` and
  describe the reference assumption. The numeric limit and runtime behavior are
  unchanged. Node startup separately applies its 256-epoch refresh policy.

The [Electra weak-subjectivity guide](https://github.com/ethereum/consensus-specs/blob/master/specs/electra/weak-subjectivity.md)
shows shorter state-derived periods for smaller active balances. Its 3,532-epoch
reference assumes at least 8,388,608 active ETH. This review does not independently
establish current active balance or implement state-derived period calculation.
The runtime refresh fence prevents treating that library constant as the normal
startup age policy; a complete refresh/failure review remains in the runtime batch.

## Reviewed paths requiring no production change in this milestone

| Boundary | Evidence and disposition |
| --- | --- |
| Shared rows | Production extraction uses checked topic/data/index conversion, preserves zero-valued topics as present and assigns receipt provenance after caller validation. Public panicking convenience constructors document preconditions and currently have only test callers. Shared consensus/partition types are data carriers, not proof verifiers. |
| Checkpoint resolution | Configured quorum is retained when sources fail; complete slot/root pairs must agree. Candidate freshness uses the quorum height. Responses have byte limits and per-source deadlines. Descriptor anchors remain explicit local trust inputs; HTTP agreement on a descriptor root does not authenticate its other fields. |
| Bootstrap and proofs | Checkpoint root/slot, committee cardinality, selected keys, BLS subgroup checks, signing domain, execution/finality branches and header-slot fork rules match examined code and pinned dependencies. Existing fork/proof mutation tests remain. No additional bypass demonstrated. |
| Light-client state transitions | Participation thresholds, committee rotation and force-update behavior preserve selected store headers. RPC/gossip publication does not simply promote every successfully decoded low-participation header. General scheduling is outside this milestone. |
| Beacon ancestry | Publication requires decreasing-slot parent linkage from a selected verified target to the checkpoint. Decoding self-consistent bytes alone does not authenticate an unrelated branch. Full proposer/state-transition reexecution is outside the established commitment trust model. |
| Forward and checkpoint-gap EL | Each forward header matches its consensus anchor; a gap validates linked headers and the terminal anchor before fetching/ingesting its payloads. |
| Reverse EL | Each parent hash/number and header rules are checked; page continuations retain the preceding page's last header. Payload jobs preserve the supplied authenticated prefix after B1-20. |
| Body and receipt commitments | Pinned Reth checks transaction, ommer and withdrawal roots; local checks cover blob gas and Osaka block size. Receipt count, ordered trie root, aggregate bloom and final gas precede extraction in examined callers. No EVM reexecution is added. |
| Transaction/log identity | Pinned Reth wire decoding leaves the transaction hash cache unset; hashing derives it from encoded transaction bytes. The extraction loop preserves empty transactions, block-global log order and topic presence, and rolls back the current block on conversion failure. |
| Empty historical blocks | Authenticated reverse headers and empty body/receipt commitments establish no rows. The optimization still advances complete-block progress through existing ingestion handling; later sync publication review remains required. |

## Independent boundary controls

Two new CL tests use literal domains derived with Python SHA-256 rather than the
production domain helper. [The derivation tool](../../tools/derive_sync_domains.py)
records the formula and provenance. Run `python3 tools/derive_sync_domains.py`.
The configured mainnet genesis validators root remains a pinned input; this does
not independently re-establish its external provenance.

The [signature-slot rule](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/altair/light-client/sync-protocol.md#validate_light_client_update)
and [domain construction](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/phase0/beacon-chain.md#compute_domain)
are checked before/at/after Capella, Deneb, Electra and Fulu activation and both
BPO boundaries. Small signed SSZ updates across Deneb and Electra accept the
expected literal domain, reject the opposite fork domain and preserve caller
state. The signer constructs its signing root independently of the production
signing-root helper. No transaction is broadcast and no network is used by tests.

A small body fixture adds local regression coverage for changed transaction order
and count, an added ommer, withdrawal value changes and missing/present withdrawal
roots. Valid controls pass. The existing anchor test now separately checks wrong
block number and hash as well as receipts root. These are commitment-boundary
controls, not executed canonical-chain fixtures.

## Validation and limits

Before B1-20, two malformed-shape tests failed and the empty/full/prefix control
passed. Both failures occurred on the first missing-hash case; later loop cases
were not reached. An earlier test-only missing `Debug` bound prevented compilation
and was corrected before the failing behavioral run. After the fix, all three
shape tests pass, covering six malformed shapes through both validators and five
valid empty/full/prefix scenarios. The fixtures contain at most two empty blocks.

Focused checks pass: 12 beacon decoder tests (one benchmark ignored), 16
light-client tests, 22 execution-validation tests and three historical-shape tests.
Full workspace gates, final source identity and CI/merge will be recorded before
completion. No timing comparison is claimed for removing the duplicate hash.

Remaining evidence and lifecycle work:

- Persisted consensus snapshots currently trust restored summaries/store fields;
  structural validation, bounded reads, transactional publication and write-failure
  handling require the CL persistence/runtime batch. Source proof checks do not
  establish on-disk corruption detection.
- `SyncEngine::new(None)` retains an explicit legacy unanchored path. Normal node
  startup resolves a checkpoint when state is absent, so this is not a demonstrated
  ordinary fresh-start bypass. Review the missing-state race and retire obsolete
  compatibility paths during sync/runtime work under the migration waiver.
- Official independent multi-fork update fixtures, genesis-default finality
  handling and historical BPO digest interop remain precise conformance leads.
  Existing synthetic tests do not prove every transition; no new defect is claimed
  without a relevant reproducer.
- The reviewed code does not reexecute Ethereum state, withdrawals or transactions.
  Receipt/body/header guarantees continue to depend on the configured checkpoint
  and authenticated ancestry.
