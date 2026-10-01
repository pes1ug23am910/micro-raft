#!/usr/bin/env python3
"""Deterministic offline shard-placement comparison; performs no network I/O."""
from __future__ import annotations

import argparse
import bisect
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import platform
import sys

MAX_KEYS = 1_000_000
MAX_SHARDS = 64
METRICS = ("keys", "logical_bytes", "relative_demand")


def digest32(label: str) -> int:
    # MD5 is used solely for placement compatibility, never authentication.
    return int.from_bytes(hashlib.md5(label.encode(), usedforsecurity=False).digest()[:4], "little")


def validate_groups(groups: tuple[str, ...]) -> None:
    if not groups or len(groups) > 16 or tuple(sorted(set(groups))) != groups:
        raise ValueError("groups must be 1..16 unique, sorted labels")
    if any(not group or len(group) > 64 or not group.isascii() for group in groups):
        raise ValueError("group labels must be bounded ASCII strings")


class Continuum:
    def __init__(self, groups: tuple[str, ...], points_per_group: int = 160):
        validate_groups(groups)
        if points_per_group not in (40, 160, 640):
            raise ValueError("points per group must be 40, 160 or 640")
        points = []
        for group in groups:
            for replica in range(points_per_group // 4):
                digest = hashlib.md5(f"{group}-{replica}".encode(), usedforsecurity=False).digest()
                for offset in range(0, 16, 4):
                    points.append((int.from_bytes(digest[offset:offset + 4], "little"), group))
        self.points = tuple(sorted(points))
        self.positions = tuple(point for point, _ in self.points)
        self.collisions = len(points) - len(set(self.positions))

    def owner_hash(self, value: int) -> str:
        if not 0 <= value < 2**32:
            raise ValueError("placement hash outside u32")
        return self.points[bisect.bisect_left(self.positions, value) % len(self.points)][1]

    def owner(self, label: str) -> str:
        return self.owner_hash(digest32(label))


def routes(groups: tuple[str, ...], shards: int, points: int | None) -> tuple[list[str], int]:
    validate_groups(groups)
    if not 1 <= shards <= MAX_SHARDS:
        raise ValueError("shards must be in 1..64")
    labels = [f"shard-{shard}" for shard in range(shards)]
    if points is None:
        return [groups[digest32(label) % len(groups)] for label in labels], 0
    ring = Continuum(groups, points)
    return [ring.owner(label) for label in labels], ring.collisions


def population(count: int, shards: int, seed: int) -> dict:
    if not 1 <= count <= MAX_KEYS or not 1 <= shards <= MAX_SHARDS or not 0 <= seed < 2**64:
        raise ValueError("invalid population size, shard count or seed")
    loads = [dict.fromkeys(METRICS, 0) for _ in range(shards)]
    witness = hashlib.sha256()
    for rank in range(count):
        key = f"placement/{seed}/{rank}"
        digest = hashlib.sha256(key.encode()).digest()
        shard = int.from_bytes(digest[:8], "big") % shards
        value_bytes = 16 + int.from_bytes(digest[-2:], "big") % 1009
        logical_bytes = len(key.encode()) + value_bytes
        demand = (rank + 1) ** -1.1
        loads[shard]["keys"] += 1
        loads[shard]["logical_bytes"] += logical_bytes
        loads[shard]["relative_demand"] += demand
        witness.update(f"{key}\t{shard}\t{value_bytes}\n".encode())
    totals = {metric: math.fsum(load[metric] for load in loads) for metric in METRICS}
    for metric in METRICS[:2]:
        totals[metric] = int(totals[metric])
    if totals["keys"] != count:
        raise AssertionError("population conservation failed")
    return {"seed": seed, "keys": count, "shards": shards,
            "input_sha256": witness.hexdigest(), "totals": totals, "shard_loads": loads}


def load_summary(mapping: list[str], groups: tuple[str, ...], pop: dict) -> dict:
    result = {}
    for metric in METRICS:
        loads = {group: math.fsum(pop["shard_loads"][shard][metric]
                                 for shard, owner in enumerate(mapping) if owner == group)
                 for group in groups}
        if metric != "relative_demand":
            loads = {group: int(value) for group, value in loads.items()}
        total = math.fsum(loads.values())
        if not math.isclose(total, pop["totals"][metric], rel_tol=1e-12, abs_tol=1e-10):
            raise AssertionError("group load conservation failed")
        mean = total / len(groups)
        cv = math.sqrt(math.fsum((value - mean)**2 for value in loads.values()) / len(groups)) / mean
        result[metric] = {"by_group": loads, "max_to_mean": max(loads.values()) / mean,
                          "min_to_mean": min(loads.values()) / mean, "coefficient_of_variation": cv}
    return result


def compare(pop: dict, before: tuple[str, ...], after: tuple[str, ...], points: int | None) -> dict:
    old, old_collisions = routes(before, pop["shards"], points)
    new, new_collisions = routes(after, pop["shards"], points)
    moved = [shard for shard in range(pop["shards"]) if old[shard] != new[shard]]
    if points is not None:
        added, removed = set(after) - set(before), set(before) - set(after)
        if added and not removed and any(new[shard] not in added for shard in moved):
            raise AssertionError("addition moved a shard between unchanged groups")
        if removed and not added and any(old[shard] not in removed for shard in moved):
            raise AssertionError("removal moved a shard previously owned by a survivor")
    moved_load = {metric: math.fsum(pop["shard_loads"][shard][metric] for shard in moved)
                  for metric in METRICS}
    for metric in METRICS[:2]:
        moved_load[metric] = int(moved_load[metric])
    return {"policy": "mod_n" if points is None else "ketama",
            "points_per_group": points, "groups_before": before, "groups_after": after,
            "routes_before": old, "routes_after": new, "moved_shard_ids": moved,
            "moved_shards": len(moved), "moved_shard_fraction": len(moved) / pop["shards"],
            "moved_load": moved_load,
            "moved_fraction": {metric: moved_load[metric] / pop["totals"][metric] for metric in METRICS},
            "before": load_summary(old, before, pop), "after": load_summary(new, after, pop),
            "continuum_collisions_before": old_collisions, "continuum_collisions_after": new_collisions}


def build_report(count: int, shards: int, seeds: list[int]) -> dict:
    if not seeds or len(seeds) > 16 or len(set(seeds)) != len(seeds):
        raise ValueError("choose 1..16 distinct seeds")
    groups = tuple(f"group-{i:02d}" for i in range(1, 9))
    cases = [("add_2_to_3", groups[:2], groups[:3]),
             ("add_3_to_4", groups[:3], groups[:4]),
             ("add_7_to_8", groups[:7], groups),
             ("remove_group_02_from_8", groups, tuple(group for group in groups if group != "group-02"))]
    populations = [population(count, shards, seed) for seed in seeds]
    results = []
    for pop in populations:
        for name, before, after in cases:
            for points in (None, 40, 160, 640):
                results.append({"scenario": name, "seed": pop["seed"],
                                "input_sha256": pop["input_sha256"], **compare(pop, before, after, points)})
    return {"schema_version": 1, "verdict": "PASS", "kind": "offline_shard_placement",
            "keys_per_seed": count, "shard_count": shards, "seeds": seeds,
            "protocol": {
                "key": "placement/{seed}/{zero_based_rank}",
                "key_to_shard": "SHA256(UTF8 key), first 8 bytes big-endian, modulo shard_count",
                "logical_value_bytes": "16 + final 2 SHA256 bytes big-endian modulo 1009",
                "logical_bytes": "UTF8 key length plus synthetic value length; excludes all storage/transfer framing and retry state",
                "relative_demand": "sum of (rank+1)^-1.1; synthetic popularity weights, not executed requests",
                "placement_label": "shard-{decimal shard ID}",
                "placement_hash": "MD5 first 4 bytes little-endian, shared by both policies",
                "mod_n": "placement_hash modulo sorted group count",
                "ketama": "equal-weight MD5 continuum; four little-endian u32 points per group-label-replica digest; first point >= key hash with wraparound",
                "collision_tie": "lexicographically first group label at identical ring position",
                "reference": "https://github.com/RJ/ketama/blob/master/libketama/ketama.c"},
            "populations": populations, "results": results,
            "limits": ["No network I/O, consensus, state transfer, measured latency or physical bytes.",
                       "Fixed shards move as units; continuum point count does not change shard count.",
                       "Equal group weights and declared labels only; three key populations are not three independently randomized rings.",
                       "A controller must commit any chosen route and complete fenced handoff before serving at the new owner."]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new output directory")
    parser.add_argument("--keys", type=int, default=100_000)
    parser.add_argument("--shards", type=int, default=64)
    parser.add_argument("--seeds", type=int, nargs="+", default=[20261001, 20261002, 20261003])
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {"schema_version": 1, "verdict": "INCOMPLETE", "python": sys.version,
                "platform": platform.platform(), "argv": sys.argv,
                "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    path = args.output / "run.json"
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    try:
        report = build_report(args.keys, args.shards, args.seeds)
        encoded = (json.dumps(report, indent=2, allow_nan=False) + "\n").encode()
        (args.output / "placement.json").write_bytes(encoded)
        manifest.update(verdict="PASS", report_sha256=hashlib.sha256(encoded).hexdigest(),
                        cases=len(report["results"]))
    except Exception as error:
        manifest.update(verdict="FAIL", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"PASS: {len(report['results'])} placement cases; output {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
