# Ethereum mainnet query benchmarks

`ethereum-mainnet.json` is an executable catalog of event-log questions, not an
EVM/state benchmark. Each case includes its plain-English purpose, interpretation,
SQL, Ethereum contract(s), inclusive block range and independent-check method.
Verified event ABIs and source URLs are embedded in the compact catalog. Raw API
responses, profiles and research downloads belong outside this repository.

## Running measurements

Use Python 3.11 or newer. Keep one workload active at a time on a fully synced
LogEx client. Choose an output directory outside the checkout and a private TOML
configuration containing `dashboard_password`; do not put credentials in command
arguments or the catalog.

Validate the catalog without contacting a node:

```sh
python3 tools/live_query_benchmark.py benchmarks/ethereum-mainnet.json --check-catalog
```

```sh
python3 tools/live_query_benchmark.py benchmarks/ethereum-mainnet.json \
  --url "$LOGEX_ORIGIN" --credentials-config "$PRIVATE_CONFIG" \
  --ssh-host "$OWNED_HOST" --remote-root "$OWNED_RUN_DIRECTORY" \
  --expected-identity "$IDENTITY_JSON" --output "$RESULT_DIRECTORY" \
  --case weth_wrapped_units --repeat 3
```

The deployment guard checks the existing owned process metadata, start time,
parent and exact command, then the existing `identity-check.py` verifies the
binary and volume. This harness assumes the acceptance deployment layout; it does
not initialize identity, launch/stop clients, change network settings, delete data
or flush caches. Preserve the original identity when intentionally upgrading a
client; create a separate verified deployment record for the replacement.

Measurements include wall time at the caller, server response/digest, binary
identity and before/after health snapshots. First trials are not guaranteed cold;
repeats may be warm. A timeout is an incomplete measurement, not a duration for a
completed query. Socket timeout does not prove server execution stopped. Stop and
investigate transport, admission or health failures before submitting more work.
The global cancellation endpoint is deliberately not used because it cannot
identify which caller owns the active query.

## Independent checks on real logs

```sh
python3 tools/live_query_reference.py benchmarks/ethereum-mainnet.json \
  --url "$LOGEX_ORIGIN" --credentials-config "$PRIVATE_CONFIG" \
  --ssh-host "$OWNED_HOST" --remote-root "$OWNED_RUN_DIRECTORY" \
  --expected-identity "$IDENTITY_JSON" --output "$REFERENCE_DIRECTORY"
```

The checker captures actual `eth_getLogs` responses, recursively splits any range
that reaches the response limit, and compares them with a plain SQL projection.
It then uses SQLite for relational SQL and Python integers for exact hexadecimal
amount arithmetic. It never generates substitute blockchain records. Timestamps
come from the plain LogEx projection because its RPC log format omits them; this
is independent query evaluation, not an independent consensus/storage audit.
Completed checks are saved individually so an interrupted capture retains prior
results. Each run also preserves its catalog, helper sources and source hashes.

Correctness ranges are explicit and can be smaller than performance ranges.
A matching empty result checks empty-input behavior only. It does not validate a
full-history balance or demonstrate that a rare event was exercised. Read range,
input-row count and expected output together. Use full-history event captures for
balance/ownership claims and nonempty historical ranges for rare protocol events.

The WBTC Mint/Burn cases use blocks 12,000,000–12,010,000, which include three
mints and one burn in the captured data. Their full measurement and verification
ranges match. Earlier recent-window empty results remain valid empty-input
checks, but are not a timing baseline for this historical workload. Compare
results only with matching SQL and block ranges, even when case IDs match.

## Interpretation

- ERC20 Transfer signatures also occur in ERC721, whose amount/token-ID layout
  differs. Pin the emitter and ABI before decoding.
- WETH9 Deposit/Withdrawal supplement Transfer when reconstructing token flow.
  Rebasing stETH cannot use ordinary transfer-net accounting; shares and rebase
  totals have different meanings.
- ERC1155 TransferSingle and TransferBatch both matter. Batch items require ABI
  array validation. OpenSea shared-storefront lazy minting can emit the encoded
  creator as the source, so zero-address transfers alone do not enumerate mints.
- Counts are events or addresses, not automatically transactions, humans, sales
  or economic volume. Cross-contract co-occurrence does not establish causality.
- Exact base-unit amounts remain integers/hexadecimal words. Do not compare
  amounts across assets as if their decimals or prices were interchangeable.
- Bounded lifecycle queries describe cohorts observed inside their range. They
  do not prove current state or absence of activity outside that range.
- Ethereum L1 inbox events do not prove execution or balances on another chain.

No comparable Dune execution measurements were available when this catalog was
created. Do not turn example dashboards or a different query/range/cache state
into a claimed Dune performance baseline. Release findings and large results are
maintained in the local campaign report; the catalog deliberately retains useful
queries that reveal engine limitations so later fixes can be measured honestly.
