"""Small understood histories and an independent exhaustive order oracle."""
import copy
import itertools
import json
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest

import check_history as checker


def operation(identity, kind, start, end, value=None, outcome="ok", **fields):
    result = dict(id=identity, kind=kind, key="x", value=value, invoke_ns=start,
                  complete_ns=end, outcome=outcome)
    if kind == "get":
        result["read_mode"] = "linearizable"
    result.update(fields)
    return result


def history(*operations, initial=None):
    return dict(schema_version=1, clock_domain="one-observer-clock",
                initial={"x": initial}, operations=list(operations))


def exhaustive(rows, initial):
    # Enumerate every subset of pending mutations, then every permutation.
    # This deliberately does not reuse the checker's predecessor masks/search.
    required = [row for row in rows if row["outcome"] == "ok"]
    optional = [row for row in rows if row["outcome"] == "unknown"]
    for included in itertools.product((False, True), repeat=len(optional)):
        selected = required + [row for row, keep in zip(optional, included) if keep]
        for ordered in itertools.permutations(selected):
            positions = {row["id"]: index for index, row in enumerate(ordered)}
            if any(first["outcome"] == "ok" and first["complete_ns"] < second["invoke_ns"]
                   and positions[first["id"]] > positions[second["id"]]
                   for first in selected for second in selected):
                continue
            value = initial
            for row in ordered:
                if row["kind"] == "get":
                    if row["value"] != value:
                        break
                else:
                    value = row["value"]
            else:
                return True
    return False


class HistoryTests(unittest.TestCase):
    def test_sequential_put_get_delete_absence(self):
        result = checker.check(history(
            operation("w", "put", 1, 2, "a"), operation("r", "get", 3, 4, "a"),
            operation("d", "delete", 5, 6), operation("absent", "get", 7, 8)))
        self.assertEqual(result["verdict"], "PASS")

    def test_stale_completed_read_is_counterexample(self):
        result = checker.check(history(operation("w", "put", 1, 2, "new"),
            operation("r", "get", 3, 4, "old"), initial="old"), reduce=True)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual([row["id"] for row in result["keys"]["x"]["counterexample"]], ["w", "r"])

    def test_overlapping_read_can_precede_write(self):
        self.assertEqual(checker.check(history(operation("w", "put", 1, 5, "a"),
            operation("r", "get", 2, 3)))["verdict"], "PASS")

    def test_equal_timestamp_is_not_invented_order(self):
        self.assertEqual(checker.check(history(operation("w", "put", 1, 2, "a"),
            operation("r", "get", 2, 3)))["verdict"], "PASS")

    def test_unknown_write_can_commit_after_timeout(self):
        result = checker.check(history(operation("pending", "put", 1, 2, "a", "unknown"),
            operation("before", "get", 3, 4), operation("after", "get", 5, 6, "a")))
        self.assertEqual(result["verdict"], "PASS")
        self.assertEqual([row["id"] for row in result["keys"]["x"]["witness"]],
                         ["before", "pending", "after"])

    def test_unknown_write_can_be_omitted(self):
        result = checker.check(history(operation("pending", "put", 1, None, "a", "unknown"),
                                       operation("r", "get", 2, 3)))
        self.assertEqual(result["verdict"], "PASS")
        self.assertEqual(result["keys"]["x"]["witness"][0]["action"], "omit_unknown")

    def test_unknown_does_not_explain_unproposed_value(self):
        self.assertEqual(checker.check(history(operation("pending", "put", 1, 2, "a", "unknown"),
            operation("r", "get", 3, 4, "b")))["verdict"], "FAIL")

    def test_unknown_read_and_rejected_write_have_no_effect(self):
        self.assertEqual(checker.check(history(operation("w", "put", 1, 2, "a", "rejected"),
            operation("lost-read", "get", 2, None, outcome="unknown"),
            operation("r", "get", 3, 4)))["verdict"], "PASS")

    def test_protected_retry_replays_original_mutation(self):
        protected = dict(logical_id="session1:x:1", retry_protected=True)
        h = history(operation("w1", "put", 1, 2, "a", **protected),
            operation("w2", "put", 3, 4, "b"),
            operation("retry", "put", 5, 6, "a", **protected),
            operation("r", "get", 7, 8, "b"))
        self.assertEqual(checker.check(h)["verdict"], "PASS")
        h["operations"][-1]["value"] = "a"
        self.assertEqual(checker.check(h)["verdict"], "FAIL")

    def test_lost_response_and_later_success_fold_interval(self):
        protected = dict(logical_id="session1:x:1", retry_protected=True)
        result = checker.check(history(operation("lost", "put", 1, 2, "a", "unknown", **protected),
            operation("r", "get", 3, 4, "a"), operation("retry", "put", 5, 6, "a", **protected)))
        self.assertEqual(result["verdict"], "PASS")
        self.assertEqual(result["logical_operations"], 2)

    def test_multiple_keys_checked_independently(self):
        h = history(operation("x-write", "put", 1, 4, "x"),
            operation("y-write", "put", 2, 3, "y", key="y"),
            operation("y-read", "get", 5, 6, "wrong", key="y"))
        h["initial"]["y"] = None
        result = checker.check(h)
        self.assertEqual((result["verdict"], result["key"]), ("FAIL", "y"))

    def test_search_exhaustion_never_passes(self):
        h = history(operation("w", "put", 1, 2, "a"), operation("r", "get", 3, 4, "a"))
        self.assertEqual(checker.check(h, budget=1)["verdict"], "INCONCLUSIVE")
        self.assertEqual(checker.check(h, budget=0)["verdict"], "INCONCLUSIVE")
        self.assertEqual(checker.check(h, max_operations=1)["verdict"], "INCONCLUSIVE")

    def test_missing_or_inconsistent_provenance_is_inconclusive(self):
        base = history(operation("r", "get", 1, 2))
        mutations = [lambda h: h.pop("clock_domain"), lambda h: h["initial"].clear(),
                     lambda h: h["operations"][0].update(clock_domain="another-host"),
                     lambda h: h["operations"][0].update(invoke_ns=True),
                     lambda h: h["operations"][0].update(complete_ns=0),
                     lambda h: h["operations"][0].update(complete_ns=None),
                     lambda h: h["operations"][0].update(read_mode="local"),
                     lambda h: h["operations"].append(copy.deepcopy(h["operations"][0])),
                     lambda h: h["operations"][0].pop("value")]
        for mutate in mutations:
            h = copy.deepcopy(base)
            mutate(h)
            self.assertEqual(checker.check(h)["verdict"], "INCONCLUSIVE", h)

    def test_logical_identity_requires_protection_and_fixed_payload(self):
        rows = [operation("a", "put", 1, 2, "a", logical_id="logical"),
                operation("b", "put", 3, 4, "b", logical_id="logical", retry_protected=True)]
        self.assertEqual(checker.check(history(*rows))["verdict"], "INCONCLUSIVE")
        rows[0]["retry_protected"] = True
        self.assertEqual(checker.check(history(*rows))["verdict"], "INCONCLUSIVE")

    def test_reduction_preserves_all_mutations(self):
        result = checker.check(history(operation("w", "put", 1, 2, "a"),
            operation("good", "get", 3, 4, "a"), operation("bad", "get", 5, 6, "b")), reduce=True)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual([row["id"] for row in result["keys"]["x"]["counterexample"]], ["w", "bad"])

    def test_counts_keep_unknown_rejected_and_retry_attempts_visible(self):
        protected = dict(logical_id="session:k:1", retry_protected=True)
        result = checker.check(history(operation("lost", "put", 1, 2, "a", "unknown", **protected),
            operation("retry", "put", 3, 4, "a", **protected),
            operation("rejected", "put", 5, 6, "b", "rejected"),
            operation("lost-read", "get", 5, None, outcome="unknown"),
            operation("read", "get", 7, 8, "a")))
        self.assertEqual(result["verdict"], "PASS")
        self.assertEqual(result["attempts"], 5)
        self.assertEqual(result["outcomes"], {"ok": 2, "unknown": 2, "rejected": 1})
        self.assertEqual(result["successful_reads"], 1)
        self.assertEqual(result["logical_operations"], 2)
        empty = checker.check(history())
        self.assertEqual((empty["attempts"], empty["successful_reads"], empty["logical_operations"]), (0, 0, 0))

    def test_reduction_shares_one_additional_budget(self):
        h = history(operation("w", "put", 1, 2, "a"),
                    *[operation(f"good-{i}", "get", 3, 4, "a") for i in range(6)],
                    operation("bad", "get", 5, 6, "b"))
        result = checker.check(h, budget=100, reduce=True)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertLessEqual(result["states"], 100)
        self.assertLessEqual(result["keys"]["x"]["reduction"]["states"], 100)
        self.assertEqual(result["keys"]["x"]["reduction"]["budget"], 100)
        # The original counterexample stays valid if no reduction candidate can finish.
        initial, operations = checker.normalize(h)
        rows, metadata = checker.counterexample(operations, initial["x"], 1)
        self.assertFalse(metadata["complete"])
        self.assertEqual(metadata["states"], 1)
        self.assertEqual(len(rows), len(operations))

    def test_seeded_histories_match_independent_exhaustive_oracle(self):
        rng = random.Random(0xC0FFEE)
        verdicts = set()
        for case in range(150):
            rows = []
            for index in range(5):
                start = rng.randrange(8)
                kind = rng.choice(("put", "delete", "get"))
                rows.append(operation(str(index), kind, start, start + rng.randrange(4),
                    rng.choice(("a", "b", None)) if kind == "get" else
                    rng.choice(("a", "b")) if kind == "put" else None,
                    "unknown" if kind != "get" and rng.randrange(4) == 0 else "ok"))
            expected = "PASS" if exhaustive(rows, None) else "FAIL"
            verdicts.add(expected)
            self.assertEqual(checker.check(history(*rows))["verdict"], expected, (case, rows))
        self.assertEqual(verdicts, {"PASS", "FAIL"})

    def test_duplicate_json_nonfinite_and_cli_exit_codes(self):
        for text in ('{"schema_version":1,"schema_version":1}', '{"number":NaN}', '{"number":1e999}'):
            with self.assertRaises(checker.InvalidHistory):
                checker.strict_json(text)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "history.json"
            for h, expected in ((history(), 0), (history(operation("r", "get", 1, 2, "missing")), 1), ({}, 2)):
                path.write_text(json.dumps(h), encoding="utf-8")
                result = subprocess.run([sys.executable, "-B", str(Path(checker.__file__)), str(path)],
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, expected, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
