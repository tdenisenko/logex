"""Readiness-guard controls; these do not generate query benchmark data."""
import copy
from pathlib import Path
import re
import unittest

from live_query_benchmark import healthy, load_catalog, measurement_case


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


if __name__ == "__main__":
    unittest.main()
