"""Reference-capture safety controls; no benchmark results or chain data fabricated."""
from pathlib import Path
import json
import sqlite3
import tempfile
import unittest

from live_query_complete_reference import Capture, file_ref, input_topics, load_ref, selections, sql_tokens, topic_constraint

A, B, C = ("0x" + str(n) * 64 for n in (1, 2, 3))


class TopicProofTests(unittest.TestCase):
    def proof(self, expression):
        return topic_constraint(sql_tokens(expression))

    def test_boolean_proof_is_a_superset_of_sqlite_selected_values(self):
        atoms = [f"topic0='{A}'", f"topic0 IN ('{A}','{B}')", "flag=1",
                 f"NOT (topic0='{B}')", "flag BETWEEN 0 AND 2"]
        expressions = list(atoms)
        for left in atoms:
            for right in atoms:
                for operator in ("AND", "OR"):
                    expressions.append(f"({left}) {operator} ({right})")
                    expressions.append(f"({left}) {operator} (({right}) AND topic0='{C}')")
        db = sqlite3.connect(":memory:")
        try:
            for expression in expressions:
                proof = self.proof(expression)
                for topic in (A, B, C, None):
                    for flag in (0, 1, 3, None):
                        with self.subTest(expression=expression, topic=topic, flag=flag):
                            row = db.execute("SELECT 1 FROM (SELECT ? AS topic0, ? AS flag) WHERE " + expression, (topic, flag)).fetchone()
                            if row and proof is not None:
                                self.assertIn(topic, proof)
        finally:
            db.close()

    def test_or_unrestricted_negation_and_unknown_syntax_cannot_prune(self):
        for expression in (f"topic0='{A}' OR flag=1", f"NOT (topic0='{A}')",
                           f"topic0!='{A}'", f"CASE WHEN flag=1 THEN topic0='{A}' ELSE 1 END",
                           f"topic0 IN (SELECT '{A}')"):
            with self.subTest(expression=expression):
                self.assertIsNone(self.proof(expression))
        self.assertIsNone(sql_tokens("topic0 = :parameter"))
        self.assertIsNone(sql_tokens("topic0 = 'x' -- topic0 = 'y'"))
        self.assertIsNone(sql_tokens("/* hidden */ topic0 = 'x'"))

    def test_keywords_inside_literals_are_opaque(self):
        self.assertEqual(self.proof(f"topic0='{A}' AND data='OR CASE FROM logs WHERE topic0=bad'"), {A})
        self.assertEqual(self.proof(f"topic0='{A}' AND data='it''s OR'"), {A})
        self.assertEqual(self.proof(f"topic0='{A}' AND flag BETWEEN 0 AND 2"), {A})

    def test_every_base_read_must_have_a_positive_restriction(self):
        catalog = {"contracts": {}}
        def proof(sql):
            return input_topics({"sql": sql}, catalog)
        self.assertEqual(proof(f"WITH x AS (SELECT * FROM logs WHERE topic0='{A}'), y AS (SELECT * FROM logs WHERE topic0='{B}') SELECT * FROM x JOIN y ON 1=1"), [A, B])
        self.assertIsNone(proof(f"WITH x AS (SELECT * FROM logs WHERE topic0='{A}'), y AS (SELECT * FROM logs) SELECT * FROM x JOIN y ON 1=1"))
        self.assertIsNone(proof(f"SELECT * FROM logs a JOIN logs b ON a.block_number=b.block_number WHERE a.topic0='{A}'"))
        self.assertIsNone(proof(f"SELECT * FROM logs WHERE topic0='{A}' UNION SELECT * FROM logs WHERE flag=1"))

    def test_real_catalog_groups_preserve_cases_emitters_and_complete_bounds(self):
        catalog = json.loads((Path(__file__).resolve().parent.parent / "benchmarks/ethereum-mainnet.json").read_text())
        groups = selections(catalog["cases"], catalog)
        seen = set()
        for group in groups:
            for name in group["cases"]:
                self.assertNotIn(name, seen);seen.add(name)
                case = next(x for x in catalog["cases"] if x["id"] == name)
                self.assertEqual((group["from_block"], group["to_block"]), (case["from_block"], case["to_block"]))
                self.assertEqual(group["addresses"], sorted(catalog["contracts"][x]["address"] for x in case["contracts"]))
                if group["topics"] is not None:
                    self.assertIsNotNone(input_topics(case, catalog))
                    self.assertTrue(set(input_topics(case, catalog)) <= set(group["topics"]))
        self.assertEqual(seen, {x["id"] for x in catalog["cases"]})


class SavedAttemptTests(unittest.TestCase):
    def test_incomplete_attempt_refuses_before_guard_or_network(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary);(root / "piece-1-2.request.json").write_text('{}')
            def forbidden(*_):
                raise AssertionError("an incomplete request must not contact the node")
            capture = Capture(None, root, forbidden, {"result_limit": 10000})
            with self.assertRaisesRegex(RuntimeError, "incomplete saved attempt"):
                list(capture.leaves({"id": "a"}, 1, 2))

    def test_hash_or_path_change_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary);p = root / 'saved.json';p.write_text('{"value": 1}')
            ref = file_ref(p);self.assertEqual(load_ref(root, ref), {"value": 1})
            p.write_text('{"value": 2}')
            with self.assertRaisesRegex(ValueError, "changed"):
                load_ref(root, ref)
            real = root / 'real.json';real.write_text('{}');link = root / 'linked.json';link.symlink_to(real)
            with self.assertRaisesRegex(ValueError, "changed"):
                load_ref(root, file_ref(link))


if __name__ == '__main__':
    unittest.main()
