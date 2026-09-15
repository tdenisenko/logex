# Bounded consensus peer-cache recovery

Source `7771ddfe` makes the derived consensus peer cache recoverable after content
damage and replaces shared staging with privately owned temporary files. It keeps
trusted consensus snapshots, discovery identity and peer validation unchanged.
Nine focused controls, independent final review and all seven local workspace
gates pass. PR/CI completion remains pending.

## Findings

- **B3-35 — damaged hints prevent startup (P2, fixed):** empty, interrupted or
  otherwise invalid cache content caused `ConsensusNetwork::new` to return an
  error before bootnode setup. Initial spawning failed synchronously; supervised
  reconstruction would retry the same damaged file. Controls reproduce both the
  loader failure and actual network construction failure.
- **B3-36 — cache loading has no producer bounds (P2, fixed):** the writer selects
  at most 256 records, but the loader read the entire file and decoded an
  unrestricted vector. A valid oversized JSON file and 257 otherwise ordinary
  records were accepted. Loading now checks both byte and record limits before
  admitting any cached hints.
- **B3-37 — fixed staging can overwrite another artifact (P2, fixed):** the writer
  reused a `.json.tmp` sibling without exclusive ownership. A control confirms
  that an existing staging artifact was overwritten and removed during saving.
  Concurrent save attempts could also interfere with that shared pathname.
- **B3-38 — metadata errors appear to be an absent cache (P2, fixed):** `exists()`
  returned false for filesystem errors, causing an empty-cache result. An ordinary
  file used as a would-be parent directory reproduced the problem with the original
  loader. Only `NotFound` now means no cached hints; other open/read errors remain
  actionable failures.

## Recovery and publication

Open the file directly and read at most **1 MiB plus one detection byte**. Decode
with a typed sequence visitor that retains at most **256 records** and requires
end-of-input. The extra element is checked without constructing another peer.
The byte ceiling bounds input and parsing; it is not a claim that all process
allocations fit in 1 MiB.

For malformed, oversized or excessive-record content, move the original file
intact into a uniquely owned sibling directory named
`.known-peers-quarantine-…/known-peers.json`. Keep that directory after the rename;
subsequent opens see no cache and create no duplicate quarantine. Separate damaged
originals remain separate artifacts. No cached hints are returned until preservation
succeeds. Genuine read or preservation errors stop recovery with the relevant path
and I/O cause. No quarantine artifacts are automatically removed.

Network construction then follows its ordinary configured bootnode/discovery
path. Invalid individual ENRs in an otherwise well-formed, bounded cache retain
the existing skip behavior. Trusted consensus state is neither reset nor advanced
by cache recovery.

The writer checks the 256-record limit, the pinned ENR implementation's maximum
404-byte base64 text representation (300 raw bytes plus encoding/prefix), and the
encoded JSON byte limit. It writes a unique same-directory `NamedTempFile` and
atomically replaces the published cache. Invalid input or failed replacement does
not advance `last_persisted`; owned temporary files are cleaned up on failure.
Unrelated legacy staging artifacts are left intact.

These records are rediscoverable connection hints. Saves retain **best-effort
power-loss semantics**: losing the newest hints is acceptable, and damaged content
is recoverable. This correction adds no periodic fsync barriers to the network
loop and makes no new guarantee that the latest cache or quarantine metadata
survives power loss. Trusted snapshots retain their separate durable-write policy.
This cache recovery is separate from the planned verified corrupt-segment repair.

## Validation and limits

Five of the initial six controls failed before the fix; the ordinary read-error
control already passed. A separate run restored the original loader function from
`3ba2bb0e` while retaining the candidate test harness and reproduced the hidden
`NotADirectory` error. The exact function and source hashes are retained.

All nine final focused controls pass, covering byte-identical quarantine of
incomplete/invalid/trailing data, repeated reopen, byte/record limits, preserved
staging artifacts, real filesystem errors, failed preservation/publication cleanup,
invalid writer input, network construction with unchanged trusted state and failed
saves retaining `last_persisted`. Preservation-failure controls use an impossible
parent and a disappeared source; they do not simulate every permission or rename
race. An occupied publication destination remains intact.

All seven local gates pass on source `7771ddfe`: vendor integrity, formatting,
workspace checking, strict workspace Clippy, 1,340 workspace tests (23 ignored),
doc tests and release build. The consensus suite includes 262 passing tests with
one ignored. Independent final review binds the committed source's SHA256.
Commands, source bindings and hashed logs are retained in the
[validation record](baselines/2026-09-15-consensus-peer-cache-recovery.json).
PR/CI completion remains pending before merge.

Tests use temporary files and unpolled network objects. No discovery, peer contact,
benchmark, Mac mini work or production-data operation was performed. The change
adds bounded restart work and unique temporary-file creation for existing periodic
saves, with no row-processing work or new durability barrier. No throughput result
is claimed.

Cleanup removes whole-file string loading, unchecked sequence decoding,
exists-based absence probing, shared staging and the unused parse-error variant.
Existing canonical ENR selection, relevance filtering, cache comparison and
last-success publication behavior remain shared. Aggregate network memory,
remaining discovery/identity review and the other offline audit batches stay open.
