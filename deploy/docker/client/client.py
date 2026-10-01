"""Bounded HTTP observations and a durable, serialized write-response ledger."""

import argparse
from contextlib import contextmanager
import http.client
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.parse
import uuid

try:
    import fcntl
except ImportError:
    fcntl = None

ENDPOINTS = {f"node{i}": f"http://node{i}:8100" for i in range(1, 4)}
LEDGER = Path("/ledger/acks.jsonl")
MAX_RESPONSE_BYTES = 256 * 1024
MAX_RECORD_BYTES = 4 * 1024 * 1024
MAX_LEDGER_BYTES = 32 * 1024 * 1024
WORKER_REAP_SECONDS = 1.0
REJECTIONS = {"not_leader", "timeout", "unavailable", "shutting_down", "outcome_unknown"}


def _clock_domain():
    try:
        boot = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
        namespace = os.readlink("/proc/self/ns/time")
        return f"linux-monotonic:{boot}:{namespace}"
    except OSError:
        # Durations remain valid. Cross-process ordering is deliberately unavailable.
        return f"process-monotonic:{uuid.uuid4().hex}"


CLOCK_DOMAIN = _clock_domain()


def strict_json(text):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON field {key!r}")
            result[key] = value
        return result

    def constant(value):
        raise ValueError(f"non-finite JSON number {value}")

    def floating(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("JSON number exceeds finite range")
        return result

    return json.loads(text, object_pairs_hook=pairs, parse_constant=constant, parse_float=floating)


def _timeout(value):
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError("timeout must be a finite positive number")
    if not math.isfinite(value) or value <= 0:
        raise ValueError("timeout must be a finite positive number")
    return float(value)


def _identity(value, label):
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValueError(f"{label} must be a nonempty string of at most 256 characters")
    if any(ord(character) < 32 for character in value):
        raise ValueError(f"{label} cannot contain control characters")
    return value


def _endpoint(value):
    parsed = urllib.parse.urlsplit(value)
    if (parsed.scheme != "http" or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment):
        raise ValueError("endpoint must be a plain HTTP origin without credentials or a path")
    port = 80 if parsed.port is None else parsed.port
    if not 1 <= port <= 65535:
        raise ValueError("invalid HTTP endpoint port")
    return parsed.hostname, port


def _http_worker(spec):
    """One HTTP attempt. The parent process owns the complete-operation deadline."""
    status, body, headers, error = None, b"", {}, None
    if spec.get("content_type") not in (None, "text/plain", "application/json"):
        raise ValueError("unsupported HTTP content type")
    host, port = _endpoint(spec["endpoint"])
    connection = http.client.HTTPConnection(host, port, timeout=spec["timeout_seconds"])
    try:
        data = None if spec["value"] is None else spec["value"].encode("utf-8")
        request_headers = {"Connection": "close"}
        if data is not None:
            request_headers["Content-Type"] = spec.get("content_type") or "text/plain; charset=utf-8"
        # HTTPConnection neither follows redirects nor retries requests or uses proxies.
        connection.request(spec.get("method") or ("GET" if data is None else "PUT"), spec["path"], data, request_headers)
        response = connection.getresponse()
        status = response.status
        for name, value in response.getheaders():
            name = name.lower()
            headers[name] = f"{headers[name]}, {value}" if name in headers else value
        length = response.getheader("Content-Length")
        transfer = response.getheader("Transfer-Encoding")
        if transfer is not None and (transfer.lower() != "chunked" or length is not None):
            raise ValueError("ambiguous or unsupported HTTP response framing")
        if length is not None:
            if not length.isascii() or not length.isdecimal():
                raise ValueError("invalid Content-Length")
            if int(length) > MAX_RESPONSE_BYTES:
                raise ValueError("HTTP response exceeds the byte limit")
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("HTTP response exceeds the byte limit")
        if length is not None and not response.chunked and len(body) != int(length):
            raise ValueError("truncated HTTP response body")
        body.decode("utf-8")
    except http.client.IncompleteRead as failure:
        body = failure.partial[:MAX_RESPONSE_BYTES]
        error = f"IncompleteRead: {failure}"
    except (OSError, http.client.HTTPException, ValueError) as failure:
        error = f"{type(failure).__name__}: {failure}"
    finally:
        connection.close()
    return dict(status=status, body=body.decode("utf-8", errors="replace"),
                headers=headers, transport_error=error)


def _worker_command():
    return [sys.executable, "-B", "-X", "utf8", str(Path(__file__).resolve()), "_http-worker"]


def _bounded_http(spec, timeout_seconds):
    """Terminate and reap a timed-out request, including a stuck OS DNS lookup."""
    deadline = time.monotonic() + timeout_seconds
    cleanup_deadline = None
    worker = subprocess.Popen(_worker_command(), stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        try:
            output, stderr = worker.communicate(
                json.dumps(spec).encode("utf-8"),
                timeout=max(0, deadline - time.monotonic()),
            )
        except subprocess.TimeoutExpired:
            cleanup_deadline = time.monotonic() + WORKER_REAP_SECONDS
            worker.kill()
            try:
                worker.communicate(timeout=max(0, cleanup_deadline - time.monotonic()))
            except subprocess.TimeoutExpired as failure:
                raise RuntimeError("HTTP worker could not be reaped; experiment is invalid") from failure
            return dict(status=None, body="", headers={},
                        transport_error="operation_deadline_exceeded: outcome is unknown")
        if worker.returncode != 0:
            return dict(status=None, body="", headers={},
                        transport_error=f"HTTP worker exit {worker.returncode}: {stderr.decode('utf-8', errors='replace')[:1000]}")
        result = strict_json(output)
        if not isinstance(result, dict) or set(result) != {"status", "body", "headers", "transport_error"}:
            raise ValueError("invalid HTTP worker result")
        _validate_http_result(result)
        return result
    finally:
        try:
            if worker.poll() is None:
                cleanup_deadline = cleanup_deadline or time.monotonic() + WORKER_REAP_SECONDS
                worker.kill()
                try:
                    worker.wait(timeout=max(0, cleanup_deadline - time.monotonic()))
                except subprocess.TimeoutExpired as failure:
                    raise RuntimeError("HTTP worker could not be reaped; experiment is invalid") from failure
        finally:
            for pipe in (worker.stdin, worker.stdout, worker.stderr):
                if pipe is not None:
                    pipe.close()


def request_record(node, path, timeout=3, value=None, *, endpoint=None,
                   run_id=None, cycle_id="manual", op_id=None, method=None, content_type=None):
    """Return one observation; callers must validate authority and read freshness."""
    timeout = _timeout(timeout)
    _identity(node, "node")
    endpoint = ENDPOINTS[node] if endpoint is None else endpoint
    _endpoint(endpoint)
    if not isinstance(path, str) or not path.startswith("/") or "\r" in path or "\n" in path:
        raise ValueError("request path must be an absolute HTTP path without newlines")
    if value is not None and (not isinstance(value, str) or len(value.encode("utf-8")) > 64 * 1024):
        raise ValueError("PUT value must contain at most 65536 UTF-8 bytes")
    run_id = _identity(run_id if run_id is not None else f"manual-{uuid.uuid4().hex}", "run_id")
    cycle_id = _identity(cycle_id, "cycle_id")
    op_id = _identity(op_id if op_id is not None else uuid.uuid4().hex, "op_id")
    if method not in (None, "GET", "PUT", "POST", "DELETE"):
        raise ValueError("unsupported HTTP method")
    if content_type not in (None, "text/plain", "application/json"):
        raise ValueError("unsupported HTTP content type")
    invoked = time.monotonic_ns()
    result = _bounded_http(dict(endpoint=endpoint, path=path, value=value, method=method,
                                content_type=content_type, timeout_seconds=timeout), timeout)
    completed = time.monotonic_ns()
    return dict(schema_version=1, node=node, run_id=run_id, cycle_id=cycle_id, op_id=op_id,
                clock_domain=CLOCK_DOMAIN, invoke_monotonic_ns=invoked,
                complete_monotonic_ns=completed, duration_ns=completed - invoked, **result)


def request(node, path, timeout, value=None):
    """Compatibility wrapper for callers that only need status, body, and error."""
    record = request_record(node, path, timeout, value)
    return record["status"], record["body"], record["transport_error"]


def classify_write(record):
    """Only a complete, valid success enters the acknowledged set."""
    if record["transport_error"] is not None:
        return "outcome_unknown", None
    try:
        payload = strict_json(record["body"])
    except (ValueError, UnicodeError, RecursionError):
        return "outcome_unknown", None
    if not isinstance(payload, dict):
        return "outcome_unknown", None
    index = payload.get("index")
    if (type(record["status"]) is int and record["status"] == 200 and payload.get("ok") is True
            and type(index) is int and 0 < index <= 2**64 - 1):
        return "acknowledged", index
    if record["status"] == 503 and payload.get("ok") is not True:
        outcome = payload.get("error")
        if isinstance(outcome, str) and outcome in REJECTIONS:
            return outcome, None
    return "outcome_unknown", None


def _validate_http_result(record):
    for field in ("status", "body", "headers", "transport_error"):
        if field not in record:
            raise ValueError(f"missing HTTP observation field {field}")
    status = record["status"]
    if status is not None and (type(status) is not int or not 100 <= status <= 599):
        raise ValueError("invalid HTTP status")
    if not isinstance(record["body"], str):
        raise ValueError("HTTP body must be text")
    headers = record["headers"]
    if not isinstance(headers, dict) or any(not isinstance(key, str) or key != key.lower()
            or not isinstance(value, str) for key, value in headers.items()):
        raise ValueError("HTTP headers must be lowercase text pairs")
    error = record["transport_error"]
    if error is not None and not isinstance(error, str):
        raise ValueError("invalid HTTP transport error")


def _validate_record(record, expected_sequence):
    if (not isinstance(record, dict) or type(record.get("schema_version")) is not int
            or record["schema_version"] != 1):
        raise ValueError("unsupported ledger schema; use an independent ledger for a new run")
    if type(record.get("seq")) is not int or record["seq"] != expected_sequence:
        raise ValueError("ledger sequence is invalid")
    for field in ("run_id", "cycle_id", "op_id", "clock_domain", "node"):
        _identity(record.get(field), field)
    for field in ("key", "value", "body", "response"):
        if not isinstance(record.get(field), str):
            raise ValueError(f"ledger {field} must be text")
    if not record["key"] or len(record["key"].encode("utf-8")) > 1024 or len(record["value"].encode("utf-8")) > 64 * 1024:
        raise ValueError("ledger key or value exceeds the supported input range")
    if record["response"] != record["body"]:
        raise ValueError("ledger response alias differs from body")
    for field in ("invoke_monotonic_ns", "complete_monotonic_ns", "duration_ns"):
        if type(record.get(field)) is not int or record[field] < 0:
            raise ValueError(f"ledger {field} must be a nonnegative integer")
    if record["complete_monotonic_ns"] - record["invoke_monotonic_ns"] != record["duration_ns"]:
        raise ValueError("ledger observation interval is invalid")
    _validate_http_result(record)
    if "index" not in record:
        raise ValueError("missing ledger index")
    outcome, index = classify_write(record)
    if record.get("outcome") != outcome or record.get("index") != index:
        raise ValueError("ledger outcome contradicts its observed response")
    if index is not None and type(record.get("index")) is not int:
        raise ValueError("ledger acknowledged index must not be boolean")


def decode_ledger(raw):
    """Validate complete JSONL, contiguous sequence, and unique logical operations."""
    if len(raw) > MAX_LEDGER_BYTES:
        raise ValueError("ledger exceeds the byte limit")
    if raw and not raw.endswith(b"\n"):
        raise ValueError("ledger has an incomplete final record")
    records, identities = [], set()
    for line in raw.split(b"\n")[:-1]:
        if not line or len(line) > MAX_RECORD_BYTES:
            raise ValueError("ledger record is empty or too large")
        record = strict_json(line.decode("utf-8"))
        _validate_record(record, len(records) + 1)
        identity = (record["run_id"], record["cycle_id"], record["op_id"])
        if identity in identities:
            raise ValueError("ledger repeats an operation identity")
        identities.add(identity)
        records.append(record)
    return records


@contextmanager
def _locked_ledger(path, *, write, timeout):
    if fcntl is None or not hasattr(os, "O_DIRECTORY"):
        raise OSError("durable ledgers require POSIX file locks and directory fsync")
    with Path(path).open("a+b" if write else "rb") as ledger:
        deadline = time.monotonic() + _timeout(timeout)
        operation = fcntl.LOCK_EX if write else fcntl.LOCK_SH
        while True:
            try:
                fcntl.flock(ledger.fileno(), operation | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("ledger lock acquisition timed out before request")
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
        try:
            yield ledger
        finally:
            fcntl.flock(ledger.fileno(), fcntl.LOCK_UN)


def _sync_directory(path):
    directory = os.open(Path(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def perform_put(node, key, value, timeout=3, *, ledger_path=LEDGER, endpoint=None,
                run_id=None, cycle_id="manual", op_id=None):
    """Serialize request and append; return only after file and directory sync."""
    timeout = _timeout(timeout)
    if not isinstance(key, str) or not key or len(key.encode("utf-8")) > 1024:
        raise ValueError("key must contain 1..1024 UTF-8 bytes")
    if not isinstance(value, str) or len(value.encode("utf-8")) > 64 * 1024:
        raise ValueError("value must contain at most 65536 UTF-8 bytes")
    run_id = _identity(run_id if run_id is not None else f"manual-{uuid.uuid4().hex}", "run_id")
    cycle_id = _identity(cycle_id, "cycle_id")
    op_id = _identity(op_id if op_id is not None else uuid.uuid4().hex, "op_id")
    with _locked_ledger(ledger_path, write=True, timeout=timeout) as ledger:
        ledger.seek(0)
        records = decode_ledger(ledger.read(MAX_LEDGER_BYTES + 1))
        if any((record["run_id"], record["cycle_id"], record["op_id"]) == (run_id, cycle_id, op_id)
               for record in records):
            raise ValueError("operation identity already exists; request was not repeated")
        record = request_record(node, "/kv/" + urllib.parse.quote(key, safe=""), timeout, value,
                                endpoint=endpoint, run_id=run_id, cycle_id=cycle_id, op_id=op_id)
        outcome, index = classify_write(record)
        record.update(seq=len(records) + 1, key=key, value=value, outcome=outcome,
                      index=index, response=record["body"])
        _validate_record(record, len(records) + 1)
        encoded = (json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
        if len(encoded) > MAX_RECORD_BYTES:
            raise ValueError("new ledger record exceeds the byte limit; experiment is invalid")
        ledger.seek(0, os.SEEK_END)
        if ledger.tell() + len(encoded) > MAX_LEDGER_BYTES:
            raise ValueError("ledger capacity exhausted; experiment is invalid")
        if ledger.write(encoded) != len(encoded):
            raise OSError("short ledger write; experiment is invalid")
        ledger.flush()
        os.fsync(ledger.fileno())
        _sync_directory(Path(ledger_path).parent)
    return record


def read_ledger(path=LEDGER, timeout=3):
    with _locked_ledger(path, write=False, timeout=timeout) as ledger:
        return decode_ledger(ledger.read(MAX_LEDGER_BYTES + 1))


def put(args):
    record = perform_put(args.node, args.key, args.value, args.timeout_seconds,
                         ledger_path=args.ledger_path, run_id=args.run_id,
                         cycle_id=args.cycle_id, op_id=args.op_id)
    print(json.dumps(record, ensure_ascii=False), flush=True)
    return 0 if record["outcome"] == "acknowledged" else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("put", "get", "status"):
        command = sub.add_parser(name)
        command.add_argument("--node", choices=ENDPOINTS, required=name != "status")
        command.add_argument("--timeout-seconds", type=float, default=3)
        command.add_argument("--run-id")
        command.add_argument("--cycle-id", default="manual")
        command.add_argument("--op-id")
        if name in ("put", "get"):
            command.add_argument("--key", required=True)
        if name == "put":
            command.add_argument("--value", required=True)
            command.add_argument("--ledger-path", type=Path, default=LEDGER)
    ledger = sub.add_parser("ledger")
    ledger.add_argument("--ledger-path", type=Path, default=LEDGER)
    ledger.add_argument("--timeout-seconds", type=float, default=3)
    args = parser.parse_args(argv)
    _timeout(args.timeout_seconds)
    if args.command == "ledger":
        for record in read_ledger(args.ledger_path, args.timeout_seconds):
            print(json.dumps(record, ensure_ascii=False), flush=True)
        return 0
    if args.command == "put":
        return put(args)
    nodes = [args.node] if args.node else list(ENDPOINTS)
    failed = False
    run_id = args.run_id or f"manual-{uuid.uuid4().hex}"
    op_id = args.op_id or uuid.uuid4().hex
    for node in nodes:
        path = "/status" if args.command == "status" else "/kv/" + urllib.parse.quote(args.key, safe="")
        identity = op_id if len(nodes) == 1 else f"{op_id}:{node}"
        record = request_record(node, path, args.timeout_seconds, run_id=run_id,
                                cycle_id=args.cycle_id, op_id=identity)
        print(json.dumps(record, ensure_ascii=False), flush=True)
        failed |= record["status"] != 200 or record["transport_error"] is not None
    return int(failed)


if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["_http-worker"]:
            raw = sys.stdin.buffer.read(MAX_RECORD_BYTES + 1)
            if len(raw) > MAX_RECORD_BYTES:
                raise ValueError("HTTP worker input exceeds limit")
            print(json.dumps(_http_worker(strict_json(raw))), flush=True)
        else:
            raise SystemExit(main())
    except (OSError, ValueError, RuntimeError) as failure:
        print(f"client failed: {failure}", file=sys.stderr)
        raise SystemExit(1) from failure
