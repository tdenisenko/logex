# SQL queries in LogEx

LogEx exposes Ethereum event logs through a read-only SQL interface. Query the
`logs` table in the dashboard, over HTTP, or through gRPC. General queries use
DataFusion; LogEx adds event-signature literals, padded address literals,
`LATEST`, an omitted-projection shorthand, and exact sums of log data.

The event filter syntax is **`topic0 = event'Transfer(address,address,uint256)'`**.
There is no `event` column or `event="Transfer(...)"` shortcut. The literal hashes
the signature; it does not decode an event's arguments.

- [Run a query](#run-a-query)
- [The logs table](#the-logs-table)
- [LogEx-specific syntax and behavior](#logex-specific-syntax-and-behavior)
- [Query examples](#query-examples)
- [Coverage, pagination and results](#coverage-pagination-and-results)
- [Supported SQL and limits](#supported-sql-and-limits)
- [Implementation references](#implementation-references)

## Run a query

Start a node using the [run instructions](README.md#run). By default, the
dashboard and HTTP API listen on `http://127.0.0.1:8577`; gRPC listens on
`127.0.0.1:8578`. The examples assume the requested blocks are inside the node's
verified coverage. Check `GET /status` before choosing a range.

### Dashboard and HTTP

Paste SQL into the dashboard's SQL editor, or send a JSON body to `POST /query`:

```bash
curl --fail-with-body --silent --show-error \
  http://127.0.0.1:8577/query \
  -H 'content-type: application/json' \
  --data-binary @- <<'JSON'
{
  "sql": "SELECT block_number, tx_hash, log_index, address, topic0, data FROM logs WHERE block_number BETWEEN 18000000 AND 18000100 AND topic0 = event'Transfer(address,address,uint256)' ORDER BY block_number DESC, tx_index DESC, log_index DESC",
  "limit": 50,
  "offset": 0
}
JSON
```

If HTTP authentication is configured, add `--user logex` to curl and enter the
dashboard password when prompted. SQL strings use single quotes; the JSON `sql`
field uses double quotes. A quoted shell heredoc, as above, avoids having to
escape the event literal's single quotes for the shell.

| Request field | Meaning |
| --- | --- |
| `sql` | One read-only SQL statement. Required. |
| `limit` | Optional extra limit on returned result rows, applied after SQL execution semantics. Omit it for no extra transport cap; explicitly setting `0` requests zero rows. |
| `offset` | Zero-based offset into the SQL result. Defaults to `0`. |

To request cancellation of the active REST SQL query:

```bash
curl --fail-with-body --silent --show-error \
  -X POST http://127.0.0.1:8577/query/cancel
```

The response is `{"canceled":true}` when cancellation was requested, or
`{"canceled":false}` when there was no active query to cancel. Cancellation is
cooperative; an already-running filesystem operation can take time to finish.

### gRPC

Call `logex.LogExService/Query` using the
[protocol definition](crates/logex-server/proto/logex.proto). Its request contains
`sql`, optional `limit`, and optional `offset`. Each returned `QueryRow.json` is
a JSON object encoded as a string; the response also includes scan and pagination
metadata. The SQL engine and coverage checks are shared with HTTP.

Ethereum JSON-RPC `eth_getLogs` and the live WebSocket subscriptions are separate
filter APIs. They do not accept SQL statements.

## The logs table

Each row is one event log. Hashes, addresses, topics and data are exposed as
lowercase, `0x`-prefixed hexadecimal **strings**, not SQL binary or numeric types.

| Column | SQL/Arrow type | Meaning |
| --- | --- | --- |
| `block_number` | `UInt64` | Block number. |
| `block_hash` | `Utf8` | 32-byte block hash. |
| `timestamp` | `UInt64` | Block timestamp, in Unix seconds. |
| `tx_hash` | `Utf8` | 32-byte transaction hash. |
| `tx_index` | `UInt64` | Transaction position within the block. |
| `log_index` | `UInt64` | Log position within the whole block, not within one transaction. |
| `address` | `Utf8` | 20-byte **emitting contract** address. |
| `topic0` | nullable `Utf8` | First 32-byte topic; usually the signature hash for a non-anonymous Solidity event. |
| `topic1` | nullable `Utf8` | Second topic; usually the first indexed argument. |
| `topic2` | nullable `Utf8` | Third topic; usually the second indexed argument. |
| `topic3` | nullable `Utf8` | Fourth topic; usually the third indexed argument. |
| `topics` | `List<Utf8>` | Virtual array of present topics, in order. An event without topics has `[]`. |
| `data` | `Utf8` | Raw non-indexed payload, as hex. Empty data is `'0x'`. |
| `data_len` | `UInt64` | Payload length in **bytes**, excluding the hex prefix. |
| `source` | `UInt64` | Stored provenance code: `0` = receipt, `1` = trace. The code alone does not authenticate a row or imply trace collection is enabled. |

Only the individual topic columns are nullable. A missing topic is SQL `NULL`,
which is different from a present all-zero topic. Use `IS NULL` or `IS NOT NULL`,
not `= NULL`. Anonymous events do not follow the signature-in-`topic0` convention.

Inspect the schema without scanning event rows:

```sql
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
```

```sql
SELECT column_name, data_type, is_nullable, ordinal_position
FROM information_schema.columns
WHERE table_name = 'logs'
ORDER BY ordinal_position;
```

These two metadata tables support simple projections, aliases, supported
predicates, ordering, and limits/offsets. They are not a full relational system
catalog: do not assume metadata joins, aggregates or CTEs work. Metadata reports
integer fields as `bigint`; the event schema above specifies their actual
unsigned Arrow type. Use `logs` as the table name in event queries.

## LogEx-specific syntax and behavior

| Feature | Example | What LogEx does |
| --- | --- | --- |
| Event-signature literal | `topic0 = event'Transfer(address,address,uint256)'` | Replaces the literal with the Keccak-256 hash of its exact text. |
| Indexed-address literal | `topic1 = address'0x1111111111111111111111111111111111111111'` | Pads a 20-byte address on the left with 12 zero bytes, producing a 32-byte topic string. |
| Captured head | `block_number = LATEST` | Replaces the bare keyword with the local head captured for this query. |
| Omitted projection | `SELECT FROM logs WHERE block_number = 18000000` | Rewrites a leading `SELECT FROM` to `SELECT * FROM`. |
| Exact data aggregation | `SUM(data)` | In supported query shapes, treats each payload as one unsigned big-endian integer and sums with arbitrary precision. |
| Exact-sum cast forms | `SUM(CAST(data AS NUMERIC))`, `SUM(data::NUMERIC)` | Recognizes precision/scale-free `NUMERIC`, `DECIMAL` or `DEC` casts inside supported exact sums. |

The prefixes `event` and `address`, and the bare `LATEST` keyword, are
case-insensitive. The prefix must immediately precede the single quote:
`event'…'` and `address'…'`, without a space or parentheses.

### Event signatures: hashing, not ABI decoding

```sql
SELECT block_number, tx_hash, topic1, topic2, data
FROM logs
WHERE block_number BETWEEN 18000000 AND 18000100
  AND topic0 = event'Transfer(address,address,uint256)'
ORDER BY block_number, tx_index, log_index
LIMIT 50;
```

For this signature, the literal becomes:

```text
0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
```

LogEx calls Keccak-256 directly on the literal's bytes. It does **not** parse or
canonicalize an ABI declaration. Supply the canonical signature yourself:
include the event name and parameter types, omit argument names and `indexed`,
and avoid extra whitespace. For example, `Transfer(address,address,uint)` and
`Transfer(address,address,uint256)` hash differently here; no alias expansion
occurs. The event name's letter case matters too.

The signature does not identify a contract or verify its interface. For a
particular token, also filter the emitting `address` and check the payload/topic
layout. ERC20 and ERC721 `Transfer(address,address,uint256)` share this signature
but place the amount/token ID differently. Standard ERC20-shaped transfers have
three topics and 32 data bytes; standard ERC721-shaped transfers have four topics
and empty data. These shape checks alone do not prove contract compliance.

### Wallet topics versus emitting addresses

Use an ordinary 20-byte hex string for the emitter, and `address'…'` for an
indexed address argument:

```sql
SELECT block_number, tx_hash, topic1 AS sender_topic, topic2 AS recipient_topic, data
FROM logs
WHERE block_number BETWEEN 18000000 AND 18000100
  AND address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
  AND topic0 = event'Transfer(address,address,uint256)'
  AND topic2 = address'0x1111111111111111111111111111111111111111'
  AND topic1 IS NOT NULL AND topic3 IS NULL AND data_len = 32
ORDER BY block_number, tx_index, log_index;
```

The wallet address above is an example; replace it with the one to inspect.
`address'0x1111111111111111111111111111111111111111'` expands to
`'0x0000000000000000000000001111111111111111111111111111111111111111'`.

The input must decode to exactly 20 bytes. Hex letter case is accepted, the
lowercase `0x` prefix is optional, and the implementation does not enforce an
address checksum. Output is lowercase with a `0x` prefix. Do not compare the
20-byte `address` column with this padded 32-byte literal.

Both literal shortcuts can appear in `IN` lists. Ordinary strings receive no
normalization: use lowercase `0x`-prefixed text for direct comparisons. A
checksummed mixed-case ordinary string is not an exact match for a lowercase
stored address.

### `LATEST` and the projection shorthand

```sql
SELECT block_number, tx_hash, log_index
FROM logs
WHERE block_number = LATEST
ORDER BY tx_index, log_index;
```

`LATEST` is the node's captured local head, not the external network head or
finalized head. It is a numeric substitution, not a string: `'latest'` stays an
ordinary SQL string. Repeated requests can capture different heads. Coverage
checks still apply, and a verified block can correctly return no logs.

```sql
SELECT FROM logs WHERE block_number = 18000000 LIMIT 10;
```

The latter is a legacy convenience for a leading `SELECT FROM`; prefer explicit
columns or `SELECT * FROM` in new queries. DataFusion also accepts the different
form `FROM logs SELECT block_number`. A bare `FROM logs` has an empty projection
and returns empty objects; it is not equivalent to `SELECT * FROM logs`.
`FROM logs DESC` is **not** an ordering shortcut. Write
`ORDER BY block_number DESC, tx_index DESC, log_index DESC`.

### Exact sums of data

`SUM(data)` is a deliberate exception to `data` being hex text. LogEx interprets
the **entire payload** as a nonnegative big-endian integer, accumulates it without
a uint256 ceiling, and returns a base-ten JSON string. An empty payload
contributes zero. An empty or all-null aggregate input returns SQL `NULL`.

For an event whose non-indexed payload is one `uint256`, use `data_len = 32` and
the correct signature, emitter and topic shape. For multi-word ABI payloads,
summing the whole payload does not sum any one ABI field. Signed ABI integers
are not sign-decoded. Token decimals are not applied automatically.

The exact path supports:

- `SUM(data)`, integer-literal sums alongside a data sum, and addition/subtraction
  of those sums. At least one sum must use `data`.
- Conditional inputs such as `SUM(CASE WHEN topic1 = address'…' THEN data ELSE 0 END)`;
  supported result branches contain data, integer literals, nested cases or nulls.
- No grouping, or one explicit group key: `address` or `topic0` through `topic3`.
  Missing topics form a SQL `NULL` group.
- A `HAVING` comparison of a projected aggregate alias with an integer literal,
  and grouped ordering by projected group/aggregate names or aliases, including
  multiple sort keys and `NULLS FIRST`/`NULLS LAST`. Totals sort numerically.
  For `HAVING`, choose an aggregate alias that is not also a source column name.
- Precision/scale-free `CAST(... AS NUMERIC)`, `DECIMAL` or `DEC`, and equivalent
  `::` casts, around supported sum inputs. These do not establish general-purpose
  hexadecimal casts elsewhere in SQL.

All aggregate projections in a supported exact query use decimal strings,
including a companion `SUM(1)`. Ordinary numeric-only queries retain the general
engine's types.

Other shapes use the general SQL engine, where `data` is still text. In
particular, do not assume exact hex aggregation for CTEs, joins, window sums,
`SUM(DISTINCT data)`, `AVG(data)`, multiple group keys, or a projection that mixes
`SUM(data)` with `COUNT(*)` or `SUM(log_index)`. Those mixtures can fail with
`Sum not supported for Utf8`. Use a separate count query, or `SUM(1)` when a
companion count is appropriate (noting its string and empty-input semantics).

## Query examples

These examples use explicit block ranges for repeatability. Substitute ranges
inside your node's verified coverage. Results describe emitted events in those
ranges; they do not establish current balances, arbitrary EVM state, generic ETH
transfers, or beneficial ownership.

### Count transfers by emitting contract

```sql
SELECT address, COUNT(*) AS transfers
FROM logs
WHERE block_number BETWEEN 18000000 AND 18000100
  AND topic0 = event'Transfer(address,address,uint256)'
  AND topic1 IS NOT NULL AND topic2 IS NOT NULL
  AND topic3 IS NULL AND data_len = 32
GROUP BY address
ORDER BY transfers DESC, address
LIMIT 20;
```

### Sum raw transferred units for one token

```sql
SELECT SUM(data) AS transferred_base_units
FROM logs
WHERE block_number BETWEEN 18000000 AND 18000100
  AND address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
  AND topic0 = event'Transfer(address,address,uint256)'
  AND topic1 IS NOT NULL AND topic2 IS NOT NULL
  AND topic3 IS NULL AND data_len = 32;
```

Apply the token's independently known decimal scale in your client using exact
arithmetic. This is transferred volume, not supply or a balance.

### Net event flow for a wallet, grouped by token

```sql
SELECT address AS token,
       SUM(CASE WHEN topic2 = address'0x1111111111111111111111111111111111111111'
                THEN data ELSE 0 END)
       - SUM(CASE WHEN topic1 = address'0x1111111111111111111111111111111111111111'
                  THEN data ELSE 0 END) AS net_units
FROM logs
WHERE block_number BETWEEN 18000000 AND 18000100
  AND topic0 = event'Transfer(address,address,uint256)'
  AND topic1 IS NOT NULL AND topic2 IS NOT NULL
  AND topic3 IS NULL AND data_len = 32
  AND (topic1 = address'0x1111111111111111111111111111111111111111'
       OR topic2 = address'0x1111111111111111111111111111111111111111')
GROUP BY address
HAVING net_units > 0
ORDER BY net_units DESC, token;
```

This reports positive net incoming units represented by matching Transfer logs
in the selected window. A self-transfer contributes equally to both sums. The
query does not account for state changes that are absent from those logs, token
decimals, or different contracts' event semantics.

### Filter a fixed-width unsigned amount

```sql
SELECT block_number, tx_hash, data
FROM logs
WHERE block_number BETWEEN 18000000 AND 18000100
  AND address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
  AND topic0 = event'Transfer(address,address,uint256)'
  AND topic1 IS NOT NULL AND topic2 IS NOT NULL
  AND topic3 IS NULL AND data_len = 32
  AND data >= '0x000000000000000000000000000000000000000000000000000000000000000a'
ORDER BY block_number, tx_index, log_index
LIMIT 50;
```

This uses ordinary string comparison. Equal-width lowercase hex words with the
same prefix sort in unsigned numeric order, so the bound above is ten base
units. This does not generalize to variable-length payloads or signed values.

### Inspect ABI words as hex text

```sql
SELECT tx_hash, log_index,
       SUBSTR(data, 3, 64) AS first_word_hex,
       SUBSTR(data, 67, 64) AS second_word_hex
FROM logs
WHERE block_number BETWEEN 18000000 AND 18000100
  AND data_len >= 64
ORDER BY block_number, tx_index, log_index
LIMIT 10;
```

`SUBSTR` is a general SQL function, not an ABI decoder. Positions are one-based:
characters 1–2 are `0x`, and each 32-byte word occupies 64 hex characters. These
slices have no `0x` prefix. Dynamic ABI values require interpreting offsets and
lengths according to the actual ABI; LogEx does not do that for you.

### Use a CTE and a window function

```sql
WITH ranked AS (
  SELECT address, tx_hash, log_index,
         ROW_NUMBER() OVER (
           PARTITION BY address
           ORDER BY block_number DESC, tx_index DESC, log_index DESC
         ) AS rn
  FROM logs
  WHERE block_number BETWEEN 18000000 AND 18000100
)
SELECT address, tx_hash, log_index
FROM ranked
WHERE rn = 1
ORDER BY address
LIMIT 20;
```

This returns the most recent matching log for each emitter in the chosen range.
CTEs, window functions, `CASE`, `IN`, `DISTINCT`, joins, and ordinary aggregates
are general SQL features, not additional LogEx literal shortcuts. See the
[mainnet query catalog](benchmarks/ethereum-mainnet.json) for more concrete
event-only analyses and their associated [benchmark documentation](benchmarks/README.md).

## Coverage, pagination and results

### Verified coverage and changing heads

`GET /status` exposes `query_coverage.verified_from_block` and
`query_coverage.verified_to_block`. A log query must fit inside the verified
range captured for its execution. Bounds are inclusive; an omitted lower bound
means genesis and an omitted upper bound means the captured verified upper
bound. During backfill, an explicit covered block range can succeed while an
unbounded query fails. Timestamp-only filtering is not a substitute for block
bounds when genesis coverage is unavailable.

Coverage checks apply to the log scans inside joins and subqueries as well.
`LIMIT`, indexes and a predicate that happens to match no rows do not permit
silently incomplete answers. Verified empty blocks legitimately return no logs.
A known cached canonical block hash can identify a covered empty block; an
unknown hash does not bypass range admission during partial backfill.

SQL reads a bounded canonical view. Later appends are excluded. A reorg or
storage restart that invalidates that view fails the request rather than
returning a mixed result. Verified head data is not necessarily finalized.
For repeatable reports, choose a fixed covered upper block and separately
establish its finality; there is no SQL `FINALIZED` shortcut.

### Pagination and ordering

SQL `LIMIT`/`OFFSET` act within the query. HTTP/gRPC `limit`/`offset` then page
that result, including aggregated results. Neither kind of output limit means
"aggregate only this many input logs." The SQL API has no hidden default row
cap; the dashboard's generated examples may explicitly add `LIMIT 500`.

Always specify a deterministic `ORDER BY` when paging. For raw logs, use
`block_number, tx_index, log_index` in one direction. Freeze the block range
across requests: offset pagination is not a retained server cursor, and using
`LATEST` on each page can change the result set. Large offsets can still require
substantial work.

The successful HTTP response includes:

| Response field | Meaning |
| --- | --- |
| `rows` | JSON objects keyed by output column names or aliases. |
| `row_count` | Number of rows in this response. |
| `total_scanned` | Execution's selected-candidate count, not total matches, total stored rows, or physical I/O. General SQL accumulates candidates across planned log scans; metadata queries use their own filtered metadata count. |
| `limit`, `offset` | Requested transport pagination; an omitted limit is reported as `0`. An explicit zero also reports `0` but returns no rows. |
| `next_offset` | Set when a positive transport limit was filled. This is a possible next page, not proof that another row exists. It can lead to an empty final page. |
| `max_limit` | `0` means no fixed transport page-size maximum. Resource limits still apply. |

### Types and precision in JSON

- Integers and finite floating-point results are JSON numbers. Preserve large
  integers when choosing a client-side JSON parser; JavaScript `Number` cannot
  represent every 64-bit integer exactly.
- Exact SQL decimals and supported exact data sums are base-ten strings. Parse
  them with decimal/big-integer types, not binary floating point.
- Arrays and objects retain their structure, and SQL nulls remain JSON `null`.
  Non-finite floating-point results also become JSON `null`.
- SQL temporal values use Arrow's textual formatting. Computed SQL binary
  values use hex without `0x`; the original log columns retain their prefix.
- Output names must be unique. Alias colliding expressions/columns explicitly;
  duplicate names produce an error rather than overwritten JSON fields.

Ordinary integer/decimal sums and decimal/duration averages reject overflowing
running subtotals, even if later values could bring the result back into range.
Exact `SUM(data)` uses arbitrary-precision integers subject to resource budgets;
it is a different path from those ordinary fixed-width aggregates.

## Supported SQL and limits

Submit one read-only query at a time, normally written as `SELECT` or `WITH`;
a trailing semicolon is allowed. Writes, DDL, `SELECT INTO`, `SHOW`, `DESCRIBE`,
`EXPLAIN` and multiple statements are not accepted. Use the metadata queries
above for introspection.
The only event table is `logs`; there are no SQL tables for balances, receipts,
transactions, or decoded contract ABIs.

Unquoted identifiers are normalized to lowercase. Double-quoted identifiers
preserve case, so `"block_number"` resolves but `"Block_Number"` does not. String
literals use single quotes and preserve their content. Prefer lowercase unique
aliases in portable queries.

Not every dialect feature accepted by a generic SQL parser is available. Among
the explicit exclusions are `FETCH` (use `LIMIT`), `FOR UPDATE/SHARE`,
`FOR JSON/XML`, `PREWHERE`, `SETTINGS`, `FORMAT`, table sampling and index hints.
LogEx does not register custom ABI-decoding SQL functions.

Current admission limits are 256 KiB of SQL text (checked before and after
literal rewriting), 128 counted syntax tokens, and parser recursion depth 16.
Literal values, whitespace/comments, commas and closing delimiters do not count
toward that syntax-token budget. Prefer literal `IN` lists to long chains of
`OR` predicates; simplify an oversized query instead of assuming it will run.

`sync --query-memory-bytes` sets the shared accounted query-memory budget
(default 1 GiB), and `--query-max-concurrent` sets shared query admission
(default 8). The memory budget is not a process-RSS ceiling. Disk spilling is
disabled; memory exhaustion returns an error, not a truncated successful result.
REST SQL also permits only one active REST SQL query at a time.

| HTTP status | Typical cause / response |
| --- | --- |
| `400` | Invalid or unsupported SQL, types, syntax complexity, or output names; inspect `error`. |
| `409` | Another REST SQL query is active, cancellation, or an invalidated query snapshot. |
| `503` | Unverified requested coverage, shared concurrency/memory capacity, or unavailable storage. Capacity errors include `status: "query_capacity"` and `resource: "concurrency"` or `"memory"`. |
| `500` | Storage/execution I/O failure. |

For efficient queries, bound the blocks, filter emitter/topic values directly,
select only the columns needed, and use `data_len` for payload-shape filtering.
Missing, stale or temporarily locked indexes can fall back to source columns;
they affect cost, not permission to omit matching rows. An output `LIMIT` alone
does not make a broad aggregate or sort inexpensive.

## Implementation references

This guide follows the repository implementation rather than assuming a generic
Ethereum SQL dialect:

- [SQL execution, schema, rewrites, exact sums and admission](crates/logex-query/src/sql.rs):
  `log_rows_schema`, `rewrite_legacy_sql`, `rewrite_event_literals`,
  `rewrite_address_literals`, `parse_native_data_sum_query`,
  `try_execute_introspection`, and `validate_sql_statement`.
- [Exact integer accumulation](crates/logex-query/src/native_sum_memory.rs),
  [coverage](crates/logex-query/src/coverage.rs),
  [captured views](crates/logex-query/src/native.rs), and
  [JSON encoding](crates/logex-query/src/json.rs).
- [HTTP request/response and cancellation](crates/logex-server/src/rest.rs),
  [query snapshot capture](crates/logex-server/src/handler.rs),
  [gRPC protocol](crates/logex-server/proto/logex.proto),
  [CLI defaults](crates/logex-node/src/cli.rs), and
  [row/provenance definitions](crates/logex-types/src/log_row.rs).
- Executable contracts for [aggregates](crates/logex-query/tests/sql_aggregate_contracts.rs),
  [identifiers](crates/logex-query/tests/sql_identifiers.rs),
  [metadata](crates/logex-query/tests/sql_metadata_semantics.rs),
  [query scope](crates/logex-query/tests/sql_query_scope.rs),
  [syntax eligibility](crates/logex-query/tests/sql_syntax_eligibility.rs), and
  [limits](crates/logex-query/tests/sql_limits.rs), plus the inline SQL tests.
