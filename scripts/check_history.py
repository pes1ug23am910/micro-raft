#!/usr/bin/env python3
"""Bounded, single-key register linearizability checking for recorded histories.

Each key is checked independently. Unknown mutations may be omitted or take
place after invocation (including after a client timeout). PASS means that a
completion exists under this model, not that every unknown operation failed.
"""
import argparse
from dataclasses import dataclass
import json
import math
from pathlib import Path
import sys


class InvalidHistory(ValueError):
    pass


class SearchExhausted(Exception):
    pass


def strict_json(text):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise InvalidHistory(f"duplicate JSON field: {key}")
            result[key] = value
        return result
    def constant(value):
        raise InvalidHistory(f"non-finite JSON number: {value}")
    def floating(value):
        result = float(value)
        if not math.isfinite(result):
            raise InvalidHistory("JSON number exceeds finite range")
        return result
    return json.loads(text, object_pairs_hook=pairs, parse_constant=constant, parse_float=floating)


def natural(value, label):
    if type(value) is not int or value < 0:
        raise InvalidHistory(f"{label} must be a nonnegative integer")
    return value


def string(value, label):
    if not isinstance(value, str) or not value:
        raise InvalidHistory(f"{label} must be a nonempty string")
    return value


@dataclass(frozen=True)
class Operation:
    identity: str
    attempts: tuple
    kind: str
    key: str
    value: str | None
    start: int
    end: int | None
    optional: bool


def normalize(history, max_operations=256):
    """Validate provenance and fold protected retries into logical mutations."""
    if not isinstance(history, dict) or type(history.get("schema_version")) is not int or history.get("schema_version") != 1:
        raise InvalidHistory("expected history schema_version 1")
    domain = string(history.get("clock_domain"), "clock_domain")
    initial = history.get("initial")
    if not isinstance(initial, dict) or any(not isinstance(key, str) or
            (value is not None and not isinstance(value, str)) for key, value in initial.items()):
        raise InvalidHistory("initial must explicitly map every observed key to a string or null")
    rows = history.get("operations")
    if not isinstance(rows, list):
        raise InvalidHistory("operations must be an array")
    if len(rows) > 100000:
        raise InvalidHistory("attempt limit exceeded (100000)")
    groups, seen = {}, set()
    for row in rows:
        if not isinstance(row, dict):
            raise InvalidHistory("operation must be an object")
        attempt = string(row.get("id"), "operation id")
        if attempt in seen:
            raise InvalidHistory(f"duplicate attempt id: {attempt}")
        seen.add(attempt)
        if row.get("clock_domain", domain) != domain:
            raise InvalidHistory("mixed clock domains cannot be ordered")
        key = string(row.get("key"), "key")
        if key not in initial:
            raise InvalidHistory(f"initial value missing for key: {key}")
        kind, outcome = row.get("kind"), row.get("outcome")
        if kind not in ("put", "delete", "get") or outcome not in ("ok", "unknown", "rejected"):
            raise InvalidHistory("unsupported operation kind or outcome")
        start = natural(row.get("invoke_ns"), "invoke_ns")
        end = row.get("complete_ns")
        if end is not None:
            natural(end, "complete_ns")
            if end < start:
                raise InvalidHistory("completion precedes invocation")
        if outcome != "unknown" and end is None:
            raise InvalidHistory("completed operation requires complete_ns")
        if kind == "put":
            value = row.get("value")
            if not isinstance(value, str):
                raise InvalidHistory("put value must be a string")
        elif kind == "delete":
            value = None
        else:
            if outcome == "ok" and row.get("read_mode") != "linearizable":
                raise InvalidHistory("successful GET must explicitly declare linearizable read mode")
            value = row.get("value")
            if outcome == "ok" and ("value" not in row or
                    (value is not None and not isinstance(value, str))):
                raise InvalidHistory("successful GET value must be a string or explicit null")
        logical = row.get("logical_id")
        if logical is not None:
            string(logical, "logical_id")
            if kind == "get" or row.get("retry_protected") is not True:
                raise InvalidHistory("logical_id requires a retry-protected mutation")
        # A definite rejection is no state transition. Unanswered reads impose
        # no value constraint and can be omitted from a completion.
        if outcome == "rejected" or (kind == "get" and outcome == "unknown"):
            continue
        identity = ("logical", logical) if logical is not None else ("attempt", attempt)
        item = dict(identity=logical or attempt, attempts=[attempt], kind=kind, key=key,
                    value=value, start=start, end=end if outcome == "ok" else None,
                    optional=outcome == "unknown")
        prior = groups.get(identity)
        if prior is None:
            groups[identity] = item
        else:
            if (prior["kind"], prior["key"], prior["value"]) != (kind, key, value):
                raise InvalidHistory("protected logical identity reused with a different payload")
            prior["attempts"].append(attempt)
            prior["start"] = min(prior["start"], start)
            if outcome == "ok":
                prior["end"] = min(prior["end"], end) if prior["end"] is not None else end
                prior["optional"] = False
    operations = []
    for fields in groups.values():
        fields["attempts"] = tuple(fields["attempts"])
        operations.append(Operation(**fields))
    if len(operations) > max_operations:
        raise InvalidHistory(f"logical operation limit exceeded ({max_operations})")
    return initial, operations


def search(operations, initial, budget):
    """Memoized backtracking over real-time-compatible sequential orders."""
    predecessors = []
    for operation in operations:
        predecessors.append(sum(1 << index for index, other in enumerate(operations)
                                if other.end is not None and other.end < operation.start))
    failed, states = set(), 0

    def visit(remaining, value):
        nonlocal states
        if remaining == 0:
            return []
        memo = (remaining, value)
        if memo in failed:
            return None
        if states >= budget:
            raise SearchExhausted()
        states += 1
        for index, operation in enumerate(operations):
            bit = 1 << index
            if not remaining & bit or predecessors[index] & remaining:
                continue
            rest = remaining ^ bit
            if operation.optional:
                witness = visit(rest, value)
                if witness is not None:
                    return [dict(id=operation.identity, attempts=operation.attempts,
                                 action="omit_unknown")] + witness
            if operation.kind == "get":
                if operation.value != value:
                    continue
                next_value = value
            else:
                next_value = operation.value
            witness = visit(rest, next_value)
            if witness is not None:
                return [dict(id=operation.identity, attempts=operation.attempts,
                             action="apply_unknown" if operation.optional else operation.kind)] + witness
        failed.add(memo)
        return None

    try:
        witness = visit((1 << len(operations)) - 1, initial)
    except SearchExhausted:
        return dict(verdict="INCONCLUSIVE", reason="search budget exhausted", states=states)
    return dict(verdict="PASS" if witness is not None else "FAIL", states=states, witness=witness)


def counterexample(operations, initial, budget):
    """Drop reads only: removing writes could manufacture missing-value errors."""
    reduced, states, complete = list(operations), 0, True
    for operation in operations:
        if operation.kind != "get":
            continue
        candidate = [item for item in reduced if item is not operation]
        if states >= budget:
            complete = False
            break
        result = search(candidate, initial, budget - states)
        states += result["states"]
        if result["verdict"] == "FAIL":
            reduced = candidate
        elif result["verdict"] == "INCONCLUSIVE":
            complete = False
            break
    rows = [dict(id=item.identity, attempts=item.attempts, kind=item.kind, key=item.key,
                 value=item.value, invoke_ns=item.start, complete_ns=item.end,
                 outcome="unknown" if item.optional else "ok") for item in reduced]
    return rows, dict(states=states, budget=budget, complete=complete)


def check(history, budget=100000, reduce=False, max_operations=256):
    if type(budget) is not int or budget < 1:
        return dict(verdict="INCONCLUSIVE", reason="search budget must be positive")
    try:
        initial, operations = normalize(history, max_operations)
    except (InvalidHistory, TypeError, ValueError) as error:
        return dict(verdict="INCONCLUSIVE", reason=str(error))
    rows = history["operations"]
    counts = dict(attempts=len(rows),
                  outcomes={outcome: sum(row["outcome"] == outcome for row in rows)
                            for outcome in ("ok", "unknown", "rejected")},
                  successful_reads=sum(row["kind"] == "get" and row["outcome"] == "ok" for row in rows),
                  logical_operations=len(operations))
    results, remaining = {}, budget
    for key in sorted({operation.key for operation in operations}):
        selected = [operation for operation in operations if operation.key == key]
        result = search(selected, initial[key], max(0, remaining))
        remaining -= result["states"]
        results[key] = result
        if result["verdict"] == "FAIL":
            if reduce:
                result["counterexample"], result["reduction"] = counterexample(selected, initial[key], budget)
            return dict(verdict="FAIL", key=key, keys=results,
                        states=budget - remaining, **counts)
    verdict = "INCONCLUSIVE" if any(row["verdict"] == "INCONCLUSIVE" for row in results.values()) else "PASS"
    return dict(verdict=verdict, keys=results, states=budget - remaining,
                **counts,
                model="independent string registers; null is absent; unknown mutations may complete or be omitted")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("history", type=Path)
    parser.add_argument("--budget", type=int, default=100000)
    parser.add_argument("--reduce", action="store_true")
    args = parser.parse_args(argv)
    try:
        with args.history.open("rb") as stream:
            raw = stream.read(64 * 1024 * 1024 + 1)
        if len(raw) > 64 * 1024 * 1024:
            raise InvalidHistory("history exceeds 64 MiB input limit")
        history = strict_json(raw.decode("utf-8"))
        result = check(history, args.budget, args.reduce)
    except (OSError, UnicodeError, ValueError, RecursionError) as error:
        result = dict(verdict="INCONCLUSIVE", reason=str(error))
    print(json.dumps(result, indent=2))
    return {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 2}[result["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
