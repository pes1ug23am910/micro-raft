import hashlib
import math
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import placement_experiment as p


class PlacementTests(unittest.TestCase):
    def test_standard_md5_vectors_and_uint_order(self):
        self.assertEqual(p.digest32(""), 0xD98C1DD4)
        self.assertEqual(p.digest32("abc"), 0x98500190)

    def test_binary_search_matches_independent_exhaustive_ring_oracle(self):
        ring = p.Continuum(("a", "b", "c"))
        for value in [0, 2**32 - 1] + [max(0, point + offset) % 2**32
                                       for point, _ in ring.points for offset in (-1, 0, 1)]:
            distance, owner = min((((point - value) % 2**32), owner) for point, owner in ring.points)
            self.assertGreaterEqual(distance, 0)
            self.assertEqual(ring.owner_hash(value), owner)

    def test_addition_and_removal_preserve_unaffected_owners(self):
        before = p.Continuum(("a", "c", "e"))
        added = p.Continuum(("a", "b", "c", "e"))
        removed = p.Continuum(("a", "e"))
        changes = 0
        for value in range(0, 2**32, 429496):
            old = before.owner_hash(value)
            if added.owner_hash(value) != old:
                changes += 1
                self.assertEqual(added.owner_hash(value), "b")
            if removed.owner_hash(value) != old:
                self.assertEqual(old, "c")
        self.assertGreater(changes, 0)

    def test_conservation_and_movement_with_handmade_population(self):
        pop = {"shards": 3, "shard_loads": [dict(zip(p.METRICS, values))
                                           for values in ((2, 20, 0.5), (1, 10, 0.25), (0, 0, 0.0))],
               "totals": {"keys": 3, "logical_bytes": 30, "relative_demand": 0.75}}
        summary = p.load_summary(["a", "b", "a"], ("a", "b", "c"), pop)
        self.assertEqual(summary["keys"]["by_group"], {"a": 2, "b": 1, "c": 0})
        self.assertEqual(summary["keys"]["max_to_mean"], 2)
        self.assertAlmostEqual(summary["keys"]["coefficient_of_variation"], math.sqrt(2 / 3))
        same = p.compare(pop, ("a", "b"), ("a", "b"), 160)
        self.assertEqual(same["moved_shard_ids"], [])
        self.assertEqual(same["moved_fraction"], dict.fromkeys(p.METRICS, 0.0))

    def test_population_bytes_and_shards_match_external_definition(self):
        pop = p.population(17, 7, 99)
        expected = [0] * 7
        for rank in range(17):
            digest_hex = hashlib.sha256(f"placement/99/{rank}".encode()).hexdigest()
            expected[int(digest_hex[:16], 16) % 7] += 1
        self.assertEqual([load["keys"] for load in pop["shard_loads"]], expected)
        self.assertEqual(pop, p.population(17, 7, 99))
        self.assertNotEqual(pop["input_sha256"], p.population(17, 7, 100)["input_sha256"])

    def test_invalid_inputs_are_not_results(self):
        for count, shards, seed in ((0, 1, 0), (1_000_001, 1, 0), (1, 65, 0), (1, 1, -1)):
            with self.assertRaises(ValueError):
                p.population(count, shards, seed)
        for groups in ((), ("a", "a"), ("b", "a")):
            with self.assertRaises(ValueError):
                p.Continuum(groups)
        with self.assertRaises(ValueError):
            p.build_report(10, 2, [1, 1])

    def test_existing_output_directory_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "unrelated.txt"
            marker.write_text("retained")
            result = subprocess.run([sys.executable, str(Path(p.__file__)), "--output", directory,
                                     "--keys", "10"], capture_output=True, timeout=15)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(marker.read_text(), "retained")
            self.assertFalse((Path(directory) / "placement.json").exists())


if __name__ == "__main__":
    unittest.main()
