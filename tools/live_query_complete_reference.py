#!/usr/bin/env python3
"""Complete frozen-input query references from one guarded LogEx deployment.

No external Ethereum calls, node lifecycle changes, cache flushes or cancellation.
Raw inputs and evaluations belong outside Git. This checks SQL over local inputs;
it does not independently prove Ethereum history completeness.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
import time
import tomllib

from live_query_benchmark import Client, canonical, healthy, load_catalog, utc, verify_identity
from live_query_reference import COLUMNS, NUMERIC, checked, prepare_captures, reference, substitute


def sql_tokens(sql):
    """Small conservative lexer for proving an optional input-topic restriction.

    This is not a SQL executor/parser. Unsupported syntax disables pruning.
    Quoted strings are opaque, so keywords inside strings cannot affect proof.
    """
    result = []
    while sql.strip():
        sql = sql.lstrip()
        match = re.match(r"'(?:[^']|'')*'|[a-zA-Z_][a-zA-Z_0-9]*|\d+|[(),.=<>!+*/%-]", sql)
        if match is None or sql.startswith(("--", "/*")):
            return None
        token = match[0]
        result.append(token if token.startswith("'") else token.lower())
        sql = sql[len(token):]
    return result


def topic_constraint(tokens):
    """A superset of topics permitted by this expression, or None if unproven."""
    if not tokens or any(t in {"case", "select", "exists"} for t in tokens):
        return None
    if tokens[0] == "(":
        depth = 0
        for i, token in enumerate(tokens):
            depth += (token == "(") - (token == ")")
            if depth == 0:
                if i == len(tokens) - 1:
                    return topic_constraint(tokens[1:-1])
                break
    # OR is less binding than AND. BETWEEN's AND is not a boolean separator.
    for operator in ("or", "and"):
        parts, start, depth, between = [], 0, 0, False
        for i, token in enumerate(tokens):
            depth += (token == "(") - (token == ")")
            if depth < 0:
                return None
            if depth:
                continue
            if token == "between":
                between = True
            elif token == "and" and between:
                between = False
            elif token == operator:
                parts.append(tokens[start:i]);start = i + 1
        if depth:
            return None
        if parts:
            parts.append(tokens[start:])
            proven = [topic_constraint(part) for part in parts]
            if operator == "or":
                return None if any(x is None for x in proven) else set().union(*proven)
            known = [x for x in proven if x is not None]
            return set.intersection(*known) if known else None
    values = None
    if len(tokens) == 3 and tokens[:2] == ["topic0", "="]:
        values = tokens[2:]
    elif len(tokens) >= 5 and tokens[:3] == ["topic0", "in", "("] and tokens[-1] == ")":
        inner = tokens[3:-1]
        if all(t == "," for t in inner[1::2]) and len(inner) % 2:
            values = inner[::2]
    if values is None or not all(re.fullmatch(r"'0x[0-9a-f]{64}'", x) for x in values):
        return None
    return {x[1:-1] for x in values}


def input_topics(case, catalog):
    """Prune only when every direct logs read has a proven topic conjunction."""
    tokens = sql_tokens(substitute(case["sql"], catalog))
    if tokens is None:
        return None
    reads = [i for i, t in enumerate(tokens) if t in {"from", "join"} and tokens[i + 1:i + 2] == ["logs"]]
    if not reads:
        return None
    topics = set()
    for start in reads:
        if tokens[start:start + 3] != ["from", "logs", "where"]:
            return None
        depth, end = 0, start + 3
        while end < len(tokens):
            token = tokens[end]
            if depth == 0 and token in {"group", "order", "limit", "having", "union", "except", "intersect", ")"}:
                break
            depth += (token == "(") - (token == ")")
            end += 1
        constraint = topic_constraint(tokens[start + 3:end])
        if not constraint:
            return None
        topics.update(constraint)
    return sorted(topics)


def selections(cases, catalog):
    groups = {}
    for case in cases:
        addresses = sorted(catalog["contracts"][name]["address"] for name in case["contracts"])
        key = (tuple(addresses), case["from_block"], case["to_block"])
        topics = input_topics(case, catalog)
        if key not in groups:
            groups[key] = {"addresses": addresses, "from_block": key[1], "to_block": key[2],
                           "topics": topics, "cases": []}
        else:
            prior = groups[key]["topics"]
            groups[key]["topics"] = sorted(set(prior) | set(topics)) if prior and topics else None
        groups[key]["cases"].append(case["id"])
    result = []
    for selection in groups.values():
        selection["id"] = hashlib.sha256(canonical(selection)).hexdigest()[:24]
        result.append(selection)
    # Smaller ranges and deterministic order give useful early bounded results.
    return sorted(result, key=lambda x: (x["to_block"] - x["from_block"], x["id"]))


def file_ref(path):
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def load_ref(root, item):
    path = Path(item["path"])
    if path.parent != root or path.is_symlink() or file_ref(path)["sha256"] != item["sha256"]:
        raise ValueError("saved capture reference changed")
    return json.loads(path.read_text())


def normalize_rpc(rows, selection, low, high):
    result, seen = [], set()
    for row in rows:
        if row["removed"] or row["address"] not in selection["addresses"]:
            raise ValueError("unexpected removed log or emitter")
        out = {"address": row["address"], "data": row["data"], "block_hash": row["blockHash"],
               "tx_hash": row["transactionHash"], "data_len": (len(row["data"]) - 2) // 2}
        for dest, src in (("block_number", "blockNumber"), ("tx_index", "transactionIndex"), ("log_index", "logIndex")):
            out[dest] = int(row[src], 16)
        out.update({f"topic{i}": row["topics"][i] if i < len(row["topics"]) else None for i in range(4)})
        key = (out["block_hash"], out["log_index"])
        if key in seen or not low <= out["block_number"] <= high:
            raise ValueError("duplicate canonical log or out-of-range input")
        if selection["topics"] and out["topic0"] not in selection["topics"]:
            raise ValueError("unexpected event topic")
        seen.add(key);result.append(out)
    if result != sorted(result, key=lambda x: (x["block_number"], x["log_index"])):
        raise ValueError("RPC input order changed")
    return result


class Capture:
    def __init__(self, client, root, guard, limits):
        self.client, self.root, self.guard, self.limits = client, root, guard, limits
        self.bytes, self.rows = 0, 0

    def save(self, name, value):
        path = self.root / name
        self.client.save(path, value)
        self.bytes += path.stat().st_size
        if self.bytes > self.limits["max_capture_bytes"]:
            raise RuntimeError("capture byte allowance exhausted; saved evidence preserved")
        return file_ref(path)

    def leaves(self, selection, low, high):
        name = f"piece-{low}-{high}"
        path = self.root / (name + ".json")
        limit = self.limits["result_limit"]
        if path.exists():
            if path.is_symlink():
                raise ValueError("piece must not be a symlink")
            piece = json.loads(path.read_text())
            if piece["range"] != [low, high] or piece["selection_id"] != selection["id"]:
                raise ValueError("piece scope changed")
            rpc = load_ref(self.root, piece["rpc"])
        else:
            if any(self.root.glob(name + ".*.json")):
                raise RuntimeError("incomplete saved attempt requires investigation, not a silent retry")
            identity, before = self.guard(low, high)
            request = {"address": selection["addresses"], "fromBlock": hex(low), "toBlock": hex(high), "limit": limit}
            if selection["topics"]:
                request["topics"] = [selection["topics"]]
            self.save(name + ".request.json", {"utc": utc(), "request": request, "identity": identity, "before": before})
            rpc = self.client.fetch("/", {"jsonrpc": "2.0", "id": 1, "method": "eth_getLogs", "params": [request]})
            rpc_ref = self.save(name + ".rpc.json", rpc)
            rows = checked(rpc, "result")
            normalize_rpc(rows, selection, low, high)
            if len(rows) > limit:
                raise ValueError("RPC exceeded requested row bound")
            piece = {"selection_id": selection["id"], "range": [low, high], "identity_before": identity,
                     "before": before, "request": request, "rpc": rpc_ref}
            after_identity, after_rpc = self.guard(low, high)
            piece.update(identity_after_rpc=after_identity, after_rpc=after_rpc)
            if len(rows) == limit:
                if low == high:
                    raise RuntimeError("one block reached cap; complete inputs are unproven")
                middle = (low + high) // 2
                piece["children"] = [[low, middle], [middle + 1, high]]
            else:
                sql = "SELECT " + ", ".join(COLUMNS) + " FROM logs WHERE address IN (" + ",".join("'" + x + "'" for x in selection["addresses"]) + f") AND block_number BETWEEN {low} AND {high}"
                if selection["topics"]:
                    sql += " AND topic0 IN (" + ",".join("'" + x + "'" for x in selection["topics"]) + ")"
                sql += " ORDER BY block_number, log_index"
                projection = self.client.fetch("/query", {"sql": sql, "limit": limit})
                piece.update(projection_sql=sql, projection=self.save(name + ".projection.json", projection))
                checked(projection, "rows")
                piece["identity_after"], piece["after"] = self.guard(low, high)
            self.save(path.name, piece)
        rpc_rows = checked(rpc, "result")
        normalized = normalize_rpc(rpc_rows, selection, low, high)
        if "children" in piece:
            middle = (low + high) // 2
            if len(rpc_rows) != limit or piece["children"] != [[low, middle], [middle + 1, high]] or low == high:
                raise ValueError("invalid saved split")
            for start, end in piece["children"]:
                yield from self.leaves(selection, start, end)
            return
        rows = checked(load_ref(self.root, piece["projection"]), "rows")
        if len(rows) != len(normalized) or len(rows) >= limit:
            raise ValueError("incomplete projection")
        if normalized != [{k: row[k] for k in COLUMNS if k != "timestamp"} for row in rows]:
            raise ValueError("complete RPC/projection fields differ")
        self.rows += len(rows)
        if self.rows > self.limits["max_input_rows"]:
            raise RuntimeError("input row allowance exhausted; evidence preserved")
        yield piece, rows, file_ref(path)


def build_database(capture, selection):
    # This scratch database is always rebuilt from immutable reviewed pieces.
    # SQLite's uniqueness constraint rejects canonical duplicates across pieces.
    temporary = tempfile.TemporaryDirectory(prefix=".evaluation-", dir=capture.root)
    db = sqlite3.connect(Path(temporary.name) / "inputs.sqlite")
    db.execute("PRAGMA temp_store=FILE")
    db.execute("PRAGMA cache_size=-32768")
    db.execute("CREATE TABLE logs (" + ",".join(c + (" INTEGER" if c in NUMERIC else " TEXT") for c in COLUMNS) + ", UNIQUE(block_hash,log_index))")
    pieces, cursor = [], selection["from_block"]
    width = capture.limits["piece_blocks"]
    try:
        for start in range(cursor, selection["to_block"] + 1, width):
            for piece, rows, ref in capture.leaves(selection, start, min(start + width - 1, selection["to_block"])):
                if piece["range"][0] != cursor:
                    raise ValueError("input range has a gap or overlap")
                cursor = piece["range"][1] + 1
                db.executemany("INSERT INTO logs VALUES (" + ",".join("?" for _ in COLUMNS) + ")", [[r[c] for c in COLUMNS] for r in rows])
                db.commit()
                pieces.append({"reference": ref, "range": piece["range"], "rows": len(rows)})
                print(json.dumps({"phase": "input_piece", "selection": selection["id"], "range": piece["range"], "rows": len(rows), "total_rows": capture.rows}), flush=True)
        if cursor != selection["to_block"] + 1:
            raise ValueError("complete selection endpoint was not reached")
        return db, pieces, temporary
    except BaseException:
        db.close();temporary.cleanup();raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("catalog", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--credentials-config", type=Path, required=True)
    parser.add_argument("--ssh-host", required=True)
    parser.add_argument("--remote-root", required=True)
    parser.add_argument("--expected-identity", type=Path, required=True)
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument("--result-limit", type=int, default=10000)
    parser.add_argument("--piece-blocks", type=int, default=1000000)
    parser.add_argument("--max-input-rows", type=int, default=5000000)
    parser.add_argument("--max-capture-bytes", type=int, default=16 * 1024**3)
    parser.add_argument("--minimum-local-free-bytes", type=int, default=10 * 1024**3)
    parser.add_argument("--deadline-seconds", type=int, default=86400)
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.result_limit <= 10000 or min(args.piece_blocks, args.max_input_rows, args.max_capture_bytes, args.minimum_local_free_bytes, args.deadline_seconds) <= 0:
        parser.error("work allowances must be positive and result limit at most 10000")
    catalog, raw = load_catalog(args.catalog)
    cases = [c for c in catalog["cases"] if not args.case or c["id"] in args.case]
    if not cases or set(args.case) - {c["id"] for c in cases}:
        parser.error("empty/unknown case selection")
    root = args.output.resolve();repo = Path(__file__).resolve().parent.parent
    if root == repo or repo in root.parents or args.output.is_symlink():
        parser.error("complete references must be saved outside the repository, without symlinks")
    expected_raw = args.expected_identity.read_bytes();expected = json.loads(expected_raw)
    limits = {name: getattr(args, name) for name in ("result_limit", "piece_blocks", "max_input_rows", "max_capture_bytes", "minimum_local_free_bytes", "deadline_seconds")}
    plan = {"version": 1, "identity": expected, "endpoint": args.url, "ssh_host": args.ssh_host, "remote_root": args.remote_root,
            "catalog_sha256": hashlib.sha256(raw).hexdigest(), "cases": cases,
            "selections": selections(cases, catalog), "limits": limits,
            "helpers": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__), Path(__file__).with_name("live_query_reference.py"), Path(__file__).with_name("live_query_benchmark.py")]},
            "scope": "Complete selected local inputs and independent SQL/exact arithmetic. Not independent Ethereum history authentication."}
    if args.plan_only:
        print(json.dumps(plan, indent=2));return
    if args.credentials_config.stat().st_mode & 0o077:
        parser.error("credentials must be private")
    secret = tomllib.loads(args.credentials_config.read_text())["dashboard_password"]
    client = Client(args.url, secret, "logex", 300, 64 * 1024**2)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if (root / "failure.json").exists() or (root / "complete.json").exists():
        parser.error("existing terminal result must be reviewed; no silent retry or duplicate run")
    started = time.monotonic()
    def guard(low, high):
        if time.monotonic() - started >= limits["deadline_seconds"]:
            raise RuntimeError("explicit invocation deadline exhausted")
        if args.expected_identity.read_bytes() != expected_raw:
            raise RuntimeError("deployment identity changed")
        if shutil.disk_usage(root).free < limits["minimum_local_free_bytes"]:
            raise RuntimeError("local free space below explicit allowance")
        actual = verify_identity(args.ssh_host, args.remote_root, args.expected_identity)
        status = client.fetch("/status")
        if not healthy(status, high, required_from=low):
            raise RuntimeError("healthy finalized coverage is required")
        return actual, status
    try:
        identity, initial_status = guard(0, max(c["to_block"] for c in cases))
        prepare_captures(client, root, identity)
        owner = root / "plan.json"
        if owner.exists():
            if json.loads(owner.read_text()) != plan:
                raise ValueError("plan/helper/limits changed; use a new invocation")
        else:
            client.save(owner, plan)
        cases_by_id = {c["id"]: c for c in cases};results = []
        for selection in plan["selections"]:
            directory = root / "captures" / selection["id"];directory.mkdir(exist_ok=True, mode=0o700)
            if directory.is_symlink():
                raise ValueError("capture group must not be a symlink")
            capture = Capture(client, directory, guard, limits)
            capture.bytes = sum(p.stat().st_size for p in directory.iterdir() if p.is_file())
            if capture.bytes > limits["max_capture_bytes"]:
                raise RuntimeError("saved group exceeds capture allowance")
            db, pieces, temporary = build_database(capture, selection)
            try:
                for name in selection["cases"]:
                    case = cases_by_id[name];target = root / (name + ".json")
                    expected_rows = reference(db, case, substitute(case["sql"], catalog))
                    if target.exists():
                        record = json.loads(target.read_text())
                        if record["case"] != case or record["expected"] != expected_rows or not record["passed"]:
                            raise ValueError("saved reference result changed or failed")
                    else:
                        if (root / (name + ".request.json")).exists() or (root / (name + ".attempt.json")).exists():
                            raise RuntimeError("saved incomplete query requires investigation before retry")
                        before_identity, before = guard(case["from_block"], case["to_block"])
                        client.save(root / (name + ".request.json"), {"utc": utc(), "sql": case["sql"], "identity": before_identity, "before": before})
                        result = client.fetch("/query", {"sql": case["sql"]})
                        client.save(root / (name + ".attempt.json"), {"utc": utc(), "result": result, "identity": before_identity, "before": before})
                        after_identity, after = guard(case["from_block"], case["to_block"])
                        actual_rows = checked(result, "rows")
                        record = {"utc": utc(), "case": case, "selection": selection, "pieces": pieces,
                                  "input_rows": capture.rows, "expected": expected_rows,
                                  "expected_sha256": hashlib.sha256(canonical(expected_rows)).hexdigest(),
                                  "identity_before": before_identity, "identity_after": after_identity,
                                  "before": before, "result": result, "after": after,
                                  "passed": actual_rows == expected_rows, "whole_history_independent_proof": False}
                        client.save(target, record)
                    if not record["passed"]:
                        raise RuntimeError("saved exact-reference failure; investigate before more work")
                    entry = {"case": name, "reference": file_ref(target), "input_rows": capture.rows,
                             "expected_sha256": record["expected_sha256"], "seconds": record["result"]["elapsed_seconds"], "passed": True}
                    results.append(entry);print(json.dumps(entry), flush=True)
            finally:
                db.close();temporary.cleanup()
        final_identity, final_status = guard(0, max(c["to_block"] for c in cases))
        client.save(root / "complete.json", {"utc": utc(), "plan": file_ref(owner), "identity": final_identity,
                    "initial_status": initial_status, "final_status": final_status, "cases": len(cases), "results": results,
                    "complete": True, "all_exact_references_passed": True, "scope": plan["scope"]})
        print(json.dumps({"phase": "complete", "reference": file_ref(root / "complete.json"), "cases": len(cases)}), flush=True)
    except Exception as error:
        client.save(root / "failure.json", {"utc": utc(), "type": type(error).__name__, "error": str(error), "requires_investigation": True})
        raise


if __name__ == "__main__":
    main()
