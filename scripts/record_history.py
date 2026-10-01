#!/usr/bin/env python3
"""Record bounded HTTP attempts from one observer and check their history.

A workload declares known initial values and sequential batches of concurrent
PUT/DELETE/GET attempts, each directed at an explicit node. No redirect/retry
is automatic. Every invocation and response is synced to an append-only journal.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import sys
import threading
import time
import urllib.parse
import uuid

import check_history

_CLIENT = Path(__file__).resolve().parents[1] / "deploy/docker/client/client.py"
_spec = importlib.util.spec_from_file_location("experiment_http_client", _CLIENT)
client = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(client)


def validate_endpoints(endpoints):
    if not isinstance(endpoints, dict) or not endpoints:
        raise ValueError("endpoints must map node names to plain HTTP origins")
    for name, endpoint in endpoints.items():
        client._identity(name, "node")
        if not isinstance(endpoint, str):
            raise ValueError("endpoint must be text")
        client._endpoint(endpoint)


def validate_operation(operation, endpoints, initial):
    if not isinstance(operation, dict):
        raise ValueError("each attempt must be an object")
    identity = client._identity(operation.get("id"), "attempt id")
    node = client._identity(operation.get("node"), "node")
    if node not in endpoints:
        raise ValueError("attempt node is absent from endpoint map")
    key = operation.get("key")
    if not isinstance(key, str) or not key or len(key.encode("utf-8")) > 1024 or key not in initial:
        raise ValueError("key needs a declared initial value and 1..1024 UTF-8 bytes")
    kind = operation.get("kind")
    if kind not in ("put", "get", "delete"):
        raise ValueError("only put, get and delete attempts are supported")
    if kind == "put" and (not isinstance(operation.get("value"), str) or
            len(operation["value"].encode("utf-8")) > 65536):
        raise ValueError("PUT value must contain at most 65536 UTF-8 bytes")
    if "session_id" in operation or "sequence" in operation:
        if kind == "get" or any(type(operation.get(field)) is not int or
                not 1 <= operation[field] <= 2**64 - 1 for field in ("session_id", "sequence")):
            raise ValueError("protected mutation needs positive u64 session_id and sequence")
    return identity


def validate_workload(workload):
    if not isinstance(workload, dict) or type(workload.get("schema_version")) is not int or workload.get("schema_version") != 1:
        raise ValueError("expected workload schema_version 1")
    endpoints, initial, batches = (workload.get(field) for field in ("endpoints", "initial", "batches"))
    validate_endpoints(endpoints)
    check_history.normalize(dict(schema_version=1, clock_domain="validation", initial=initial, operations=[]))
    if not isinstance(batches, list) or not batches:
        raise ValueError("batches must be a nonempty array")
    count, identities = 0, set()
    for batch in batches:
        if not isinstance(batch, list) or not 1 <= len(batch) <= 16:
            raise ValueError("each batch must contain 1..16 concurrent attempts")
        for operation in batch:
            identity = validate_operation(operation, endpoints, initial)
            if identity in identities:
                raise ValueError("attempt ids must be unique; use separate ids for retries")
            identities.add(identity)
            count += 1
    if count > 256:
        raise ValueError("workload exceeds 256 attempts")
    return workload


def validate_header(header):
    if header.get("event") != "header" or type(header.get("schema_version")) is not int or header.get("schema_version") != 1:
        raise ValueError("missing or unsupported journal header")
    client._identity(header.get("clock_domain"), "clock_domain")
    client._identity(header.get("run_id"), "run_id")
    try:
        client._timeout(header.get("timeout_seconds"))
    except OverflowError as error:
        raise ValueError("timeout exceeds finite numeric range") from error
    validate_endpoints(header.get("endpoints"))
    check_history.normalize(dict(schema_version=1, clock_domain=header["clock_domain"],
                                 initial=header.get("initial"), operations=[]))


def validate_observation(response, operation, invocation, header):
    if not isinstance(response, dict) or type(response.get("schema_version")) is not int or response.get("schema_version") != 1:
        raise ValueError("unsupported HTTP observation schema")
    client._validate_http_result(response)
    expected = dict(node=operation["node"], op_id=operation["id"], run_id=header["run_id"],
                    cycle_id="history", clock_domain=header["clock_domain"])
    for field, value in expected.items():
        client._identity(response.get(field), field)
        if response[field] != value:
            raise ValueError(f"completion {field} does not match invocation/header")
    for field in ("invoke_monotonic_ns", "complete_monotonic_ns", "duration_ns"):
        check_history.natural(response.get(field), field)
    start, end = response["invoke_monotonic_ns"], response["complete_monotonic_ns"]
    if not invocation <= start <= end or response["duration_ns"] != end - start:
        raise ValueError("invalid response timing")


class Journal:
    def __init__(self, path, header):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.stream = self.path.open("x", encoding="utf-8", newline="\n")
        try:
            self.append(dict(event="header", **header))
            if os.name == "posix":
                client._sync_directory(self.path.parent)
        except BaseException:
            self.stream.close()
            raise

    def append(self, event):
        encoded = json.dumps(event, ensure_ascii=False, allow_nan=False) + "\n"
        with self.lock:
            self.stream.write(encoded)
            self.stream.flush()
            os.fsync(self.stream.fileno())

    def close(self):
        self.stream.close()


def attempt(operation, endpoints, journal, run_id, timeout):
    key = urllib.parse.quote(operation["key"], safe="")
    kind = operation["kind"]
    path = f"/kv/{key}"
    if "session_id" in operation:
        path = f'/sessions/{operation["session_id"]}/kv/{key}?sequence={operation["sequence"]}'
    elif kind == "get":
        path += "?consistency=linearizable"
    journal.append(dict(event="invoke", invoke_ns=time.monotonic_ns(), operation=operation))
    raw = client.request_record(operation["node"], path, timeout,
        operation["value"] if kind == "put" else None,
        method={"put": "PUT", "delete": "DELETE", "get": "GET"}[kind],
        endpoint=endpoints[operation["node"]], run_id=run_id, cycle_id="history", op_id=operation["id"])
    journal.append(dict(event="complete", id=operation["id"], response=raw))
    return raw


def completed_operation(operation, start, response):
    result = dict(id=operation["id"], kind=operation["kind"], key=operation["key"],
                  invoke_ns=start, complete_ns=None, outcome="unknown")
    if operation["kind"] == "put":
        result["value"] = operation["value"]
    if "session_id" in operation:
        # Structured JSON avoids ambiguous delimiter concatenation for arbitrary keys.
        result.update(logical_id=json.dumps([operation["session_id"], operation["key"], operation["sequence"]]),
                      retry_protected=True)
    if response is None:
        return result
    result["complete_ns"] = response["complete_monotonic_ns"]
    if operation["kind"] in ("put", "delete"):
        outcome, _index = client.classify_write(response)
        if outcome == "acknowledged":
            if "session_id" in operation:
                body = client.strict_json(response["body"])
                if any(type(body.get(field)) is not int or body[field] != operation[field]
                       for field in ("session_id", "sequence")):
                    raise ValueError("protected success does not match session identity")
            result["outcome"] = "ok"
        elif outcome not in ("outcome_unknown",):
            result["outcome"] = "rejected"
        elif response["transport_error"] is None:
            # These are the committed session errors; they do not mutate a key.
            try:
                body = client.strict_json(response["body"])
            except (ValueError, UnicodeError):
                body = {}
            errors = {"unknown_session", "session_closed", "payload_mismatch", "stale_sequence",
                      "sequence_gap", "sequence_exhausted", "session_capacity",
                      "key_capacity", "payload_capacity", "invalid_sequence", "invalid_key", "value_too_large"}
            if (isinstance(body, dict) and body.get("ok") is False and body.get("error") in errors
                    and response["status"] in (400, 404, 409, 410, 413, 507)):
                result["outcome"] = "rejected"
    elif response["transport_error"] is None and response["status"] in (200, 404):
        result.update(outcome="ok", value=response["body"] if response["status"] == 200 else None,
                      read_mode=response["headers"].get("x-raft-read-mode", "unverified"))
    return result


def read_input(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(64 * 1024 * 1024 + 1)
    if len(raw) > 64 * 1024 * 1024:
        raise ValueError("input exceeds 64 MiB limit")
    return raw


def load_journal(path):
    raw = read_input(path)
    if not raw.endswith(b"\n"):
        raise ValueError("journal ends in an incomplete record")
    # JSON strings may contain Unicode line separators. Only physical LF ends a record.
    lines = raw.split(b"\n")[:-1]
    if len(lines) > 1 + 2 * 256:
        raise ValueError("journal exceeds 256 attempts")
    events = [client.strict_json(line.decode("utf-8")) for line in lines]
    if any(not isinstance(event, dict) for event in events):
        raise ValueError("journal events must be objects")
    if not events:
        raise ValueError("missing journal header")
    header = events[0]
    validate_header(header)
    invoked, completed = {}, {}
    for event in events[1:]:
        if event.get("event") == "invoke":
            operation = event.get("operation")
            identity = validate_operation(operation, header["endpoints"], header["initial"])
            if identity in invoked:
                raise ValueError("duplicate invocation id")
            if len(invoked) >= 256:
                raise ValueError("journal exceeds 256 attempts")
            invoked[identity] = (operation, check_history.natural(event.get("invoke_ns"), "invoke_ns"))
        elif event.get("event") == "complete":
            identity, response = client._identity(event.get("id"), "attempt id"), event.get("response")
            if identity not in invoked or identity in completed:
                raise ValueError("orphan or duplicate completion")
            operation, invocation = invoked[identity]
            validate_observation(response, operation, invocation, header)
            completed[identity] = response
        else:
            raise ValueError("unsupported journal event")
    return dict(schema_version=1, clock_domain=header["clock_domain"], initial=header["initial"],
                operations=[completed_operation(operation, start, completed.get(identity))
                            for identity, (operation, start) in invoked.items()])


def record(workload, path, timeout=3):
    validate_workload(workload)
    client._timeout(timeout)
    run_id = f"history-{uuid.uuid4().hex}"
    journal = Journal(path, dict(schema_version=1, clock_domain=client.CLOCK_DOMAIN,
                                initial=workload["initial"], run_id=run_id,
                                endpoints=workload["endpoints"], timeout_seconds=timeout))
    try:
        with ThreadPoolExecutor(max_workers=16) as pool:
            for batch in workload["batches"]:
                futures = [pool.submit(attempt, operation, workload["endpoints"], journal, run_id, timeout)
                           for operation in batch]
                for future in futures:
                    future.result()
    finally:
        journal.close()
    return load_journal(path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("record")
    run.add_argument("workload", type=Path)
    run.add_argument("journal", type=Path)
    run.add_argument("--timeout", type=float, default=3)
    replay = commands.add_parser("check")
    replay.add_argument("journal", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "record":
            history = record(client.strict_json(read_input(args.workload).decode("utf-8")), args.journal, args.timeout)
        else:
            history = load_journal(args.journal)
        result = check_history.check(history, reduce=True)
    except (OSError, ValueError, TypeError, KeyError, RuntimeError, OverflowError) as error:
        result = dict(verdict="INCONCLUSIVE", reason=str(error))
    print(json.dumps(result, indent=2))
    return {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 2}[result["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
