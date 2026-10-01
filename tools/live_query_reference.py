#!/usr/bin/env python3
"""Check SQL using captured Ethereum logs, SQLite and Python exact integers.

Capture uses native eth_getLogs and a plain SQL projection with identical bounds.
Their non-timestamp fields must agree exactly. Timestamps come from that plain
projection because LogEx's RPC log format does not export them. SQLite/Python
then independently evaluate the smaller, explicitly recorded correctness range.
This is not an independent consensus/storage audit or a full-range timing.
"""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import tomllib

from live_query_benchmark import Client, canonical, healthy, load_catalog, utc, verify_identity

COLUMNS = ["block_number", "block_hash", "timestamp", "tx_hash", "tx_index",
           "log_index", "address", "topic0", "topic1", "topic2", "topic3", "data", "data_len"]
NUMERIC = {"block_number", "timestamp", "tx_index", "log_index", "data_len"}


def checked(response, key):
    if response.get("http_status") != 200 or "error" in response.get("body", {}):
        raise RuntimeError("reference capture failed: " + str(response.get("http_status")))
    return response["body"][key]


def capture(client, root, addresses, low, high):
    """Split block ranges at the RPC result bound; never assume a full page is complete."""
    ident = hashlib.sha256(canonical([addresses, low, high])).hexdigest()[:20]
    target = root / (ident + ".json")
    if target.exists():
        saved = json.loads(target.read_text())
        if saved["addresses"] != addresses or saved["range"] != [low, high]:
            raise ValueError("capture cache identity mismatch")
        if "children" in saved:
            return [row for a, b in saved["children"] for row in capture(client, root, addresses, a, b)]
        sql_rows = saved["sql"]["body"]["rows"]
        rpc_rows = saved["rpc"]["body"]["result"]
    else:
        if not healthy(client.fetch("/status"), high):
            raise RuntimeError("health guard stopped reference capture")
        rpc = client.fetch("/", {"jsonrpc": "2.0", "id": 1, "method": "eth_getLogs",
              "params": [{"address": addresses, "fromBlock": hex(low), "toBlock": hex(high), "limit": 2000}]})
        rpc_rows = checked(rpc, "result")
        if len(rpc_rows) >= 2000:
            if low == high:
                raise RuntimeError("single block reaches capture limit; completeness not established")
            mid = (low + high) // 2
            client.save(target, {"observed_utc": utc(), "addresses": addresses, "range": [low, high],
                                 "children": [[low, mid], [mid + 1, high]], "bounded_rpc": rpc})
            return capture(client, root, addresses, low, mid) + capture(client, root, addresses, mid + 1, high)
        query = "SELECT " + ", ".join(COLUMNS) + " FROM logs WHERE address IN (" + ",".join(
            "'" + x + "'" for x in addresses) + f") AND block_number BETWEEN {low} AND {high} ORDER BY block_number, log_index"
        sql = client.fetch("/query", {"sql": query, "limit": 2000})
        sql_rows = checked(sql, "rows")
        if len(sql_rows) >= 2000:
            raise RuntimeError("SQL projection reaches capture bound but RPC did not")
        after = client.fetch("/status")
        if not healthy(after, high):
            raise RuntimeError("health guard stopped reference capture after projection")
        saved = {"observed_utc": utc(), "addresses": addresses, "range": [low, high],
                 "projection_sql": query, "rpc": rpc, "sql": sql, "after": after}
        client.save(target, saved)
    def normalize_sql(row):
        return {k: row[k] for k in COLUMNS if k != "timestamp"}
    def normalize_rpc(row):
        if row["removed"]:
            raise ValueError("removed log in finalized reference range")
        out = {"address": row["address"], "data": row["data"], "block_hash": row["blockHash"],
               "tx_hash": row["transactionHash"], "data_len": (len(row["data"]) - 2) // 2}
        for dest, src in [("block_number", "blockNumber"), ("tx_index", "transactionIndex"), ("log_index", "logIndex")]:
            out[dest] = int(row[src], 16)
        out.update({f"topic{i}": row["topics"][i] if i < len(row["topics"]) else None for i in range(4)})
        return out
    left = sorted(canonical(normalize_sql(x)) for x in sql_rows)
    right = sorted(canonical(normalize_rpc(x)) for x in rpc_rows)
    if left != right:
        raise ValueError("raw SQL/RPC log mismatch in " + str(target))
    return sql_rows


def substitute(sql, catalog, low=None, high=None):
    signatures = {e["signature"]: e["topic0"] for c in catalog["contracts"].values() for e in c["events"].values()}
    sql = re.sub(r"event'([^']+)'", lambda m: "'" + signatures[m[1]] + "'", sql)
    sql = re.sub(r"address'(0x[0-9a-fA-F]{40})'", lambda m: "'0x" + m[1][2:].lower().zfill(64) + "'", sql)
    if low is not None:
        sql = re.sub(r"block_number BETWEEN \d+ AND \d+", f"block_number BETWEEN {low} AND {high}", sql, flags=re.I)
    return sql


def sum_arguments(sql):
    """Extract this catalog's SUM arguments, respecting nested CASE predicates."""
    result = []
    for match in re.finditer(r"\bSUM\(", sql, re.I):
        depth, quote, start = 1, False, match.end()
        for end in range(start, len(sql)):
            char = sql[end]
            if char == "'":
                quote = not quote
            if not quote:
                depth += (char == "(") - (char == ")")
            if depth == 0:
                result.append(sql[start:end]); break
        else:
            raise ValueError("unterminated SUM in catalog")
    return result


def exact_integer(value):
    if value is None:
        return None
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)


def reference(db, case, sql):
    mode = case["reference"]
    if mode == "erc1155_batch_items":
        # Decode the two dynamic arrays independently of SQL's payload-size
        # shortcut. The selected deployed contract emits canonical equal-length
        # arrays, but do not accept that assumption without checking real bytes.
        topic = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
        total = None
        for (data,) in db.execute("SELECT data FROM logs WHERE topic0=?", (topic,)):
            raw = bytes.fromhex(data[2:])
            word = lambda offset: int.from_bytes(raw[offset:offset + 32], "big")
            if len(raw) < 128 or len(raw) % 32 or word(0) != 64:
                raise ValueError("noncanonical ERC1155 batch head")
            count = word(64)
            values_offset = 96 + 32 * count
            if (word(32) != values_offset or len(raw) != 128 + 64 * count
                    or word(values_offset) != count):
                raise ValueError("ERC1155 batch offsets/lengths disagree")
            total = (total or 0) + count
        return [{"item_entries": total}]
    if mode == "sqlite":
        cur = db.execute(sql)
        return [dict(zip([x[0] for x in cur.description], row)) for row in cur]
    projection, rest = re.split(r"\sFROM\slogs\sWHERE\s", sql, maxsplit=1, flags=re.I)
    where = re.split(r"\sGROUP BY\s|\sORDER BY\s|\sLIMIT\s", rest, maxsplit=1, flags=re.I)[0]
    sums = sum_arguments(projection)
    if mode == "exact_group_sum":
        assert len(sums) == 1
        group = re.search(r"GROUP BY (topic[123])", rest, re.I)[1]
        alias = re.search(r"SELECT\s+" + group + r"\s+AS\s+(\w+)", projection, re.I)[1]
        groups = collections.defaultdict(int)
        for key, value in db.execute(f"SELECT {group}, {sums[0]} FROM logs WHERE {where}"):
            groups[key] += exact_integer(value)
        # Catalog queries explicitly order equal sums by the projected key.
        rows = sorted(groups.items(), key=lambda x: (-x[1], x[0]))[:20]
        return [{alias: key, "total_units": str(value)} for key, value in rows]
    values = [None] * len(sums)
    for row in db.execute("SELECT " + ",".join(sums) + " FROM logs WHERE " + where):
        for i, item in enumerate(row):
            n = exact_integer(item)
            if n is not None:
                values[i] = (values[i] or 0) + n
    alias = re.search(r"\sAS\s+(\w+)\s*$", projection, re.I)[1]
    if mode == "exact_sum":
        assert len(values) == 1
        value = values[0]
    elif mode == "signed_sum":
        assert len(values) == 2 and re.search(r"\)\s*-\s*SUM\(", projection, re.I)
        value = None if None in values else values[0] - values[1]
    else:
        raise ValueError("unknown reference mode " + mode)
    return [{alias: None if value is None else str(value)}]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("catalog", type=Path)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--url", required=True)
    p.add_argument("--credentials-config", type=Path, required=True)
    p.add_argument("--ssh-host", required=True)
    p.add_argument("--remote-root", required=True)
    p.add_argument("--expected-identity", type=Path, required=True)
    p.add_argument("--case", action="append", default=[])
    args = p.parse_args()
    repo = Path(__file__).resolve().parent.parent
    root = args.output.resolve()
    if repo == root or repo in root.parents:
        p.error("reference captures must be stored outside the repository")
    if args.credentials_config.stat().st_mode & 0o077:
        p.error("credentials must be private")
    cat, catalog_raw = load_catalog(args.catalog)
    checker_raw = Path(__file__).read_bytes()
    runner_raw = Path(__file__).with_name("live_query_benchmark.py").read_bytes()
    cases = [c for c in cat["cases"] if not args.case or c["id"] in args.case]
    if not cases or set(args.case) - {c["id"] for c in cases}:
        p.error("unknown or empty case selection")
    secret = tomllib.loads(args.credentials_config.read_text())["dashboard_password"]
    client = Client(args.url, secret, "logex", 30, 8 * 1024**2)
    identity = verify_identity(args.ssh_host, args.remote_root, args.expected_identity)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    captures = root / "captures"; captures.mkdir(exist_ok=True, mode=0o700)
    output = root / utc().replace(":", "").replace("+", "_")
    output.mkdir(mode=0o700)
    report = {"started_utc": utc(), "identity": identity,
              "catalog_sha256": hashlib.sha256(catalog_raw).hexdigest(),
              "checker_sha256": hashlib.sha256(checker_raw).hexdigest(),
              "runner_sha256": hashlib.sha256(runner_raw).hexdigest(),
              "sqlite_version": sqlite3.sqlite_version, "checks": []}
    client.save(output / "run.json", report)
    client.save(output / "inputs.json", {"catalog": cat, "checker_source": checker_raw.decode(),
                                        "runner_source": runner_raw.decode()})
    cache = {}
    for case in cases:
        low, high = case["verification_from_block"], case["verification_to_block"]
        addresses = sorted(cat["contracts"][name]["address"] for name in case["contracts"])
        key = (tuple(addresses), low, high)
        try:
            if key not in cache:
                rows = capture(client, captures, addresses, low, high)
                db = sqlite3.connect(":memory:")
                db.execute("CREATE TABLE logs (" + ",".join(c + (" INTEGER" if c in NUMERIC else " TEXT") for c in COLUMNS) + ")")
                db.executemany("INSERT INTO logs VALUES (" + ",".join("?" for _ in COLUMNS) + ")", [[r[c] for c in COLUMNS] for r in rows])
                cache[key] = (db, rows)
        except (OSError, RuntimeError, ValueError, KeyError) as error:
            client.save(output / (case["id"] + ".capture-failure.json"),
                        {"id": case["id"], "range": [low, high], "error": str(error)})
            raise SystemExit("reference capture failed; earlier per-case reports are preserved") from error
        db, rows = cache[key]
        sql = re.sub(r"block_number BETWEEN \d+ AND \d+", f"block_number BETWEEN {low} AND {high}", case["sql"], flags=re.I)
        check = {"id": case["id"], "range": [low, high], "input_rows": len(rows), "sql": sql}
        try:
            expected = reference(db, case, substitute(sql, cat))
            if not healthy(client.fetch("/status"), high):
                raise RuntimeError("health guard stopped reference query")
            result = client.fetch("/query", {"sql": sql})
            after = client.fetch("/status")
            check.update({"expected": expected, "result": result, "after": after})
            actual = result.get("body", {}).get("rows")
            check["matches"] = actual == expected
            check["expected_sha256"] = hashlib.sha256(canonical(expected)).hexdigest()
            if not healthy(after, high) or result.get("http_status") not in (200, 400):
                client.save(output / (case["id"] + ".json"), check)
                raise SystemExit("reference workload stopped after transport/admission/health limit")
        except RuntimeError as error:
            check["reference_error"] = str(error)
            client.save(output / (case["id"] + ".json"), check)
            raise SystemExit("reference health guard stopped the workload") from error
        except (sqlite3.Error, ValueError, AssertionError) as error:
            check["reference_error"] = str(error)
        report["checks"].append(check)
        client.save(output / (case["id"] + ".json"), check)
        print(json.dumps({"id": case["id"], "input_rows": len(rows), "matches": check.get("matches"), "reference_error": check.get("reference_error")}), flush=True)
    report["completed_utc"] = utc()
    client.save(output / "complete.json", report)
    print(str(output))
    if not all(x.get("matches") for x in report["checks"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
