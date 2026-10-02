"""Readiness and capture-ownership controls; no query benchmark data is generated."""
import copy
import json
from pathlib import Path
import re
import tempfile
import unittest

from live_query_benchmark import Client, healthy, load_catalog, measurement_case
from live_query_reference import prepare_captures


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.status = {"http_status": 200, "body": {
            "historical_sync_disabled": False,
            "query_coverage": {"verified_from_block": 0, "verified_to_block": 26095330},
            "finalized_execution_head": {"block_number": 26095256},
            "consensus_head_fresh": True, "consensus_status_stale": False,
            "connected_peers": 96, "index_lag_blocks": 1,
            "finality_lag_blocks": 74, "raw_log_segment_backlog": 0,
            "disk_free_bytes": 576127500288,
        }}

    def test_ready_status_and_inclusive_lag_limits(self):
        self.assertTrue(healthy(self.status, 26090000))
        self.status["body"].update(index_lag_blocks=64, finality_lag_blocks=512)
        self.assertTrue(healthy(self.status, 26090000))

    def test_null_missing_or_malformed_numeric_fields_defer_work(self):
        for parent, fields in (
            (None, ["disk_free_bytes", "connected_peers", "index_lag_blocks",
                    "finality_lag_blocks", "raw_log_segment_backlog"]),
            ("query_coverage", ["verified_from_block", "verified_to_block"]),
            ("finalized_execution_head", ["block_number"]),
        ):
            for field in fields:
                for value in (None, "0", 0.0, False, True, -1):
                    with self.subTest(parent=parent, field=field, value=value):
                        response = copy.deepcopy(self.status)
                        target = response["body"] if parent is None else response["body"][parent]
                        target[field] = value
                        self.assertFalse(healthy(response, 26090000))
                        target.pop(field)
                        self.assertFalse(healthy(response, 26090000))

    def test_incomplete_status_objects_defer_work(self):
        for response in ({}, {"http_status": 503}, {"http_status": 200},
                         {"http_status": 200, "body": None}, {"http_status": 200, "body": []}):
            self.assertFalse(healthy(response, 26090000))
        for field in ("query_coverage", "finalized_execution_head"):
            for value in (None, [], 0):
                response = copy.deepcopy(self.status)
                response["body"][field] = value
                self.assertFalse(healthy(response, 26090000))

    def test_unhealthy_known_metrics_defer_work(self):
        for field, value in (("disk_free_bytes", 50 * 1024**3), ("connected_peers", 0),
                             ("index_lag_blocks", 65), ("finality_lag_blocks", 513),
                             ("raw_log_segment_backlog", 1), ("consensus_head_fresh", False),
                             ("consensus_status_stale", True), ("historical_sync_disabled", True)):
            response = copy.deepcopy(self.status)
            response["body"][field] = value
            self.assertFalse(healthy(response, 26090000))
        self.assertFalse(healthy(self.status, 26095331))
        self.status["body"]["query_coverage"]["verified_from_block"] = 1
        self.assertFalse(healthy(self.status, 26090000))

    def test_query_range_must_be_fully_verified_and_finalized(self):
        self.status["body"]["query_coverage"].update(
            verified_from_block=12000000, verified_to_block=26090000,
            stored_log_from_block=12000001,
        )
        # Inclusive verified boundaries work even when the first block has no logs.
        self.assertTrue(healthy(self.status, 26090000, required_from=12000000))
        self.assertTrue(healthy(self.status, 12000000, required_from=12000000))
        self.assertFalse(healthy(self.status, 26090000, required_from=11999999))
        self.assertFalse(healthy(self.status, 26090001, required_from=12000000))
        # Omitting the lower bound must not silently admit a full-history query.
        self.assertFalse(healthy(self.status, 26090000))
        self.status["body"]["finalized_execution_head"]["block_number"] = 26089999
        self.assertFalse(healthy(self.status, 26090000, required_from=12000000))

    def test_invalid_query_ranges_defer_work(self):
        for low, high in ((-1, 1), (2, 1), (None, 1), (0, None),
                          (True, 1), (0, False), (0.0, 1), (0, "1")):
            with self.subTest(low=low, high=high):
                self.assertFalse(healthy(self.status, high, required_from=low))


class MeasurementRangeTests(unittest.TestCase):
    def test_real_catalog_records_exact_execution_ranges_without_mutation(self):
        catalog, _ = load_catalog(
            Path(__file__).resolve().parent.parent / "benchmarks/ethereum-mainnet.json"
        )
        original = copy.deepcopy(catalog)
        for case in catalog["cases"]:
            with self.subTest(case=case["id"]):
                self.assertEqual(measurement_case(case), case)
                actual = measurement_case(case, verification_range=True)
                expected = (case["verification_from_block"], case["verification_to_block"])
                self.assertEqual((actual["from_block"], actual["to_block"]), expected)
                ranges = re.findall(r"block_number BETWEEN (\d+) AND (\d+)", actual["sql"], re.I)
                self.assertTrue(ranges)
                self.assertTrue(all((int(low), int(high)) == expected for low, high in ranges))
        self.assertEqual(catalog, original)


class CaptureOwnershipTests(unittest.TestCase):
    """Cache metadata controls only; no Ethereum records or query results generated."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.client = Client("http://127.0.0.1", "", "logex", 1, 1)
        self.identity = {
            "pid": 1, "started_utc": "2026-10-01T00:00:00+00:00",
            "source_commit": "a" * 40, "binary_sha256": "b" * 64,
            "data_identity": [1, 2],
        }

    def test_first_capture_binds_identity_and_same_deployment_can_resume(self):
        captures = prepare_captures(self.client, self.root, self.identity)
        owner = captures / "deployment.json"
        before = owner.read_bytes()
        self.assertEqual(json.loads(before), {"version": 1, "identity": self.identity})
        self.assertEqual(owner.stat().st_mode & 0o077, 0)
        self.assertEqual(prepare_captures(self.client, self.root, dict(self.identity)), captures)
        self.assertEqual(owner.read_bytes(), before)

    def test_changed_dataset_binary_or_process_cannot_reuse_captures(self):
        captures = prepare_captures(self.client, self.root, self.identity)
        before = (captures / "deployment.json").read_bytes()
        for field, value in (("data_identity", [1, 3]), ("binary_sha256", "c" * 64),
                             ("source_commit", "d" * 40), ("pid", 2),
                             ("started_utc", "2026-10-02T00:00:00+00:00")):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "another deployment"):
                prepare_captures(self.client, self.root, dict(self.identity, **{field: value}))
        self.assertEqual((captures / "deployment.json").read_bytes(), before)

    def test_new_identity_check_time_does_not_change_deployment_ownership(self):
        observed = dict(self.identity, identity_matches=True, volume_matches=True,
                        checked_utc="2026-10-01T01:00:00+00:00")
        captures = prepare_captures(self.client, self.root, observed)
        owner = captures / "deployment.json"
        before = owner.read_bytes()
        later = dict(observed, checked_utc="2026-10-01T02:00:00+00:00")
        self.assertEqual(prepare_captures(self.client, self.root, later), captures)
        self.assertEqual(owner.read_bytes(), before)
        self.assertNotIn("checked_utc", json.loads(before)["identity"])
        self.assertEqual(observed["checked_utc"], "2026-10-01T01:00:00+00:00")
        # Only the observation timestamp is transient. A changed launch is not.
        with self.assertRaisesRegex(ValueError, "another deployment"):
            prepare_captures(self.client, self.root, dict(later, started_utc=later["checked_utc"]))

    def test_legacy_capture_is_preserved_and_never_adopted(self):
        captures = self.root / "captures"
        captures.mkdir()
        legacy = captures / "legacy.json"
        legacy.write_bytes(b"preserved old capture")
        with self.assertRaisesRegex(ValueError, "no deployment identity"):
            prepare_captures(self.client, self.root, self.identity)
        self.assertEqual(legacy.read_bytes(), b"preserved old capture")
        self.assertFalse((captures / "deployment.json").exists())

    def test_malformed_or_incomplete_identity_is_rejected_without_rewriting(self):
        captures = prepare_captures(self.client, self.root, self.identity)
        owner = captures / "deployment.json"
        for raw in (b"", b"null", b"{}", b'{"version": 1}'):
            owner.write_bytes(raw)
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                prepare_captures(self.client, self.root, self.identity)
            self.assertEqual(owner.read_bytes(), raw)

    def test_symlinked_cache_or_identity_is_rejected(self):
        target = self.root / "target"
        target.mkdir()
        captures = self.root / "captures"
        captures.symlink_to(target, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "directory must not be a symlink"):
            prepare_captures(self.client, self.root, self.identity)
        captures.unlink()
        captures.mkdir()
        (captures / "deployment.json").symlink_to(target / "missing.json")
        with self.assertRaisesRegex(ValueError, "identity must not be a symlink"):
            prepare_captures(self.client, self.root, self.identity)
        self.assertFalse((target / "missing.json").exists())


if __name__ == "__main__":
    unittest.main()
