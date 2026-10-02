#!/usr/bin/env python3
"""Sequential, bounded SQL measurements against a real running LogEx deployment.

Results belong outside the repository. This tool never starts/stops a node,
changes its data, clears OS caches, or cancels another caller's query.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def load_catalog(path):
    """Validate executable inputs before contacting a deployment."""
    raw = path.read_bytes()
    catalog = json.loads(raw)
    if catalog.get("version") != 1 or catalog.get("chain_id") != 1:
        raise ValueError("expected version 1 Ethereum mainnet catalog")
    contracts = catalog["contracts"]
    for contract in contracts.values():
        if not re.fullmatch(r"0x[0-9a-f]{40}", contract["address"]):
            raise ValueError("invalid mainnet emitter address")
        for event in contract["events"].values():
            if not re.fullmatch(r"0x[0-9a-f]{64}", event["topic0"]):
                raise ValueError("invalid event topic")
    ids, sqls = set(), set()
    for case in catalog["cases"]:
        ident = case["id"]
        if not re.fullmatch(r"[a-z][a-z0-9_\-]*", ident) or ident in ids:
            raise ValueError("case IDs must be safe, unique ASCII identifiers")
        ids.add(ident)
        if not all(case.get(key) for key in ("purpose", "interpretation", "contracts", "sql")):
            raise ValueError(f"incomplete case: {ident}")
        if case["sql"] in sqls:
            raise ValueError(f"duplicate SQL: {ident}")
        sqls.add(case["sql"])
        if not set(case["contracts"]) <= contracts.keys():
            raise ValueError(f"unknown contract in {ident}")
        low, high = case["from_block"], case["to_block"]
        if not (0 <= low <= case["verification_from_block"] <= case["verification_to_block"] <= high <= catalog["frozen_upper_block"]):
            raise ValueError(f"invalid inclusive block boundaries: {ident}")
        ranges = re.findall(r"block_number BETWEEN (\d+) AND (\d+)", case["sql"], re.I)
        if not ranges or any((int(a), int(b)) != (low, high) for a, b in ranges):
            raise ValueError(f"SQL and declared ranges disagree: {ident}")
        signatures = {e["signature"] for name in case["contracts"] for e in contracts[name]["events"].values()}
        if set(re.findall(r"event'([^']+)'", case["sql"])) - signatures:
            raise ValueError(f"event signature missing from emitter metadata: {ident}")
        if case["reference"] not in {"sqlite", "exact_sum", "signed_sum", "exact_group_sum", "erc1155_batch_items"}:
            raise ValueError(f"unknown reference mode: {ident}")
    if not ids:
        raise ValueError("catalog has no cases")
    return catalog, raw


def verify_identity(host, root, expected_path):
    expected = json.loads(expected_path.read_text())
    # Check owned metadata before examining either process or executing the
    # existing read-only identity checker. Never initialize identity here.
    script = '''from pathlib import Path
import json,subprocess
r=Path(ROOT); e=json.loads(EXPECTED)
m=json.loads((r/'run/process.json').read_text())
assert all(m[k]==e[k] for k in ['pid','supervisor_pid','source_commit','binary_sha256','started_utc','command','data_dir','volume_uuid'])
for pid,key in [(m['pid'],'process'),(m['supervisor_pid'],'supervisor')]:
 v=subprocess.check_output(['ps','-ww','-p',str(pid),'-o','pid=,ppid=,lstart=,command='],text=True).strip()
 assert v.split()==e[key].split()
i=json.loads(subprocess.check_output(['python3',str(r/'identity-check.py')],text=True))
assert i['identity_matches'] and i['volume_matches'] and all(i[k]==v for k,v in e.items())
print(json.dumps(i))
'''.replace("ROOT", repr(root)).replace("EXPECTED", repr(json.dumps(expected)))
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", host, "python3 -"],
        input=script, text=True, capture_output=True, check=True, timeout=40,
    )
    return json.loads(result.stdout)


class Client:
    def __init__(self, url, password, username, timeout, max_bytes):
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.username:
            raise ValueError("use an HTTP(S) endpoint without embedded credentials")
        if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
            raise ValueError("endpoint must be an origin without a path/query/fragment")
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.max_bytes = max_bytes
        self.opener = urllib.request.build_opener(NoRedirect())
        self.headers = {"Content-Type": "application/json"}
        self.secrets = []
        if password:
            token = base64.b64encode((username + ":" + password).encode()).decode()
            self.headers["Authorization"] = "Basic " + token
            self.secrets = [password.encode(), token.encode()]

    def fetch(self, path, payload=None):
        request = urllib.request.Request(
            self.url + path,
            data=None if payload is None else canonical(payload),
            headers=self.headers,
        )
        start = time.perf_counter()
        try:
            try:
                response = self.opener.open(request, timeout=self.timeout)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                raw = response.read(self.max_bytes + 1)
                status = response.status
            if len(raw) > self.max_bytes:
                return {"http_status": status, "error": "response_byte_limit", "elapsed_seconds": time.perf_counter() - start}
            body = json.loads(raw)
            if not isinstance(body, dict):
                raise ValueError("expected JSON response object")
            return {"http_status": status, "elapsed_seconds": time.perf_counter() - start,
                    "bytes": len(raw), "response_sha256": hashlib.sha256(raw).hexdigest(),
                    "body": body}
        except (TimeoutError, OSError, urllib.error.URLError, ValueError) as error:
            # A socket timeout does not prove server execution has stopped.
            # The batch stops on transport failure and requires investigation
            # before resuming. Never use the global cancellation endpoint here:
            # it cannot identify which caller owns the active query.
            return {"http_status": None, "elapsed_seconds": time.perf_counter() - start,
                    "error": type(error).__name__}

    def save(self, path, record):
        data = (json.dumps(record, indent=2, ensure_ascii=False) + "\n").encode()
        if any(secret in data for secret in self.secrets):
            raise ValueError("refusing to save credential material")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)


def measurement_case(case, verification_range=False):
    """Keep the recorded bounds identical to the SQL actually measured."""
    if not verification_range:
        return dict(case)
    low, high = case["verification_from_block"], case["verification_to_block"]
    return dict(case, from_block=low, to_block=high, sql=re.sub(
        r"block_number BETWEEN \d+ AND \d+",
        f"block_number BETWEEN {low} AND {high}", case["sql"], flags=re.IGNORECASE,
    ))


def healthy(response, required_to=0, *, required_from=0):
    """Require verified finalized coverage of the complete inclusive query range.

    Callers without an explicit lower bound retain the conservative genesis
    requirement. A recent query can run during history sync, but neither a
    stored-row floor nor the current head alone proves its range is complete.
    """
    if (type(required_from) is not int or type(required_to) is not int
            or not 0 <= required_from <= required_to):
        return False
    if response.get("http_status") != 200:
        return False
    s = response.get("body")
    if not isinstance(s, dict):
        return False
    coverage = s.get("query_coverage")
    finalized = s.get("finalized_execution_head")
    if not isinstance(coverage, dict) or not isinstance(finalized, dict):
        return False
    # Storage metrics refresh asynchronously, and startup can legitimately
    # return null. Unknown or malformed values must defer the workload rather
    # than crash its observer or silently count as a healthy zero.
    for value, minimum, maximum in (
        (coverage.get("verified_from_block"), 0, required_from),
        (coverage.get("verified_to_block"), required_to, None),
        (finalized.get("block_number"), required_to, None),
        (s.get("connected_peers"), 1, None),
        (s.get("index_lag_blocks"), 0, 64),
        (s.get("finality_lag_blocks"), 0, 512),
        (s.get("raw_log_segment_backlog"), 0, 0),
        (s.get("disk_free_bytes"), 50 * 1024**3 + 1, None),
    ):
        if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
            return False
    return (
        s.get("historical_sync_disabled") is False
        and s.get("consensus_head_fresh") is True
        and s.get("consensus_status_stale") is False
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("catalog", type=Path)
    parser.add_argument("--check-catalog", action="store_true", help="validate catalog locally without network access")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--url")
    parser.add_argument("--credentials-config", type=Path)
    parser.add_argument("--username", default="logex")
    parser.add_argument("--ssh-host")
    parser.add_argument("--remote-root")
    parser.add_argument("--expected-identity", type=Path)
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument("--verification-range", action="store_true",
                        help="run the explicitly smaller correctness range; not a full-range timing")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--max-response-bytes", type=int, default=8 * 1024**2)
    args = parser.parse_args()
    try:
        catalog, catalog_raw = load_catalog(args.catalog)
    except (ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    if args.check_catalog:
        print(f"Validated {len(catalog['cases'])} cases and {len(catalog['contracts'])} mainnet contracts")
        return
    if not all((args.output, args.url, args.ssh_host, args.remote_root, args.expected_identity)):
        parser.error("measurements require output, URL, SSH host, remote root and expected identity")
    runner_raw = Path(__file__).read_bytes()
    if args.repeat < 1 or not 0 < args.timeout <= 300 or args.max_response_bytes < 1:
        parser.error("invalid repetition, timeout or response bound")
    repo = Path(__file__).resolve().parent.parent
    output = args.output.resolve()
    if output == repo or repo in output.parents:
        parser.error("raw results must be stored outside the repository")
    cases = catalog["cases"]
    if args.case:
        unknown = set(args.case) - {c["id"] for c in cases}
        if unknown:
            parser.error("unknown case IDs: " + ", ".join(sorted(unknown)))
        cases = [c for c in cases if c["id"] in args.case]
    cases = [measurement_case(c, args.verification_range) for c in cases]
    password = os.environ.get("LOGEX_BENCH_PASSWORD", "")
    if args.credentials_config:
        if args.credentials_config.stat().st_mode & 0o077:
            parser.error("credentials config must not be accessible to group/others")
        password = tomllib.loads(args.credentials_config.read_text())["dashboard_password"]
    client = Client(args.url, password, args.username, args.timeout, args.max_response_bytes)
    identity = verify_identity(args.ssh_host, args.remote_root, args.expected_identity)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    output.mkdir(parents=True, exist_ok=True)
    run = output / stamp
    run.mkdir(mode=0o700)
    client.save(run / "inputs.json", {"catalog": catalog, "runner_source": runner_raw.decode()})
    client.save(run / "run.json", {"started_utc": utc(), "identity": identity,
                "catalog_sha256": hashlib.sha256(catalog_raw).hexdigest(),
                "runner_sha256": hashlib.sha256(runner_raw).hexdigest(),
                "endpoint": args.url, "repeat": args.repeat, "timeout_seconds": args.timeout,
                "verification_range": args.verification_range,
                "cache_policy": "OS/application caches are not flushed; repetitions may be warm",
                "case_ids": [c["id"] for c in cases]})
    failures = 0
    for case in cases:
        for repetition in range(args.repeat):
            before = client.fetch("/status")
            if not healthy(before, case["to_block"], required_from=case["from_block"]):
                client.save(run / (case["id"] + f"-{repetition}.health-stop.json"), before)
                raise SystemExit("live health guard stopped the workload")
            result = client.fetch("/query", {"sql": case["sql"]})
            after = client.fetch("/status")
            body = result.get("body", {})
            record = {"observed_utc": utc(), "case": case, "repetition": repetition,
                      "before": before, "result": result, "after": after}
            if result.get("http_status") == 200:
                record["rows_sha256"] = hashlib.sha256(canonical(body["rows"])).hexdigest()
            else:
                failures += 1
            target = run / (case["id"] + f"-{repetition}.json")
            client.save(target, record)
            print(json.dumps({"id": case["id"], "repetition": repetition,
                  "status": result.get("http_status"), "seconds": result["elapsed_seconds"],
                  "rows": body.get("row_count"), "scanned": body.get("total_scanned"),
                  "error": body.get("error", result.get("error")), "file": str(target)}), flush=True)
            if (not healthy(after, case["to_block"], required_from=case["from_block"])
                    or result.get("http_status") not in (200, 400)):
                raise SystemExit("workload stopped after transport/admission/health limit")
    client.save(run / "complete.json", {"completed_utc": utc(), "cases": len(cases),
                "repetitions": args.repeat, "failed_requests": failures})
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
