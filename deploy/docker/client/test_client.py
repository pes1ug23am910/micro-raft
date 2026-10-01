"""HTTP protocol/deadline tests and durable ledger contract tests (stdlib only)."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import copy
import io
import json
from pathlib import Path
import socketserver
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

import client


def observation(*, body='{"ok":true,"index":7}', status=200, error=None,
                run_id="run", cycle_id="cycle", op_id="operation", node="node1"):
    return dict(schema_version=1, node=node, run_id=run_id, cycle_id=cycle_id,
                op_id=op_id, clock_domain="test-clock", invoke_monotonic_ns=100,
                complete_monotonic_ns=200, duration_ns=100, status=status, body=body,
                headers={"x-raft-last-applied": "7"}, transport_error=error)


def ledger_record(**changes):
    record = observation()
    record.update(seq=1, key="key", value="value", outcome="acknowledged", index=7,
                  response=record["body"])
    record.update(changes)
    return record


def encoded(records):
    return b"".join((json.dumps(row) + "\n").encode() for row in records)


def fake_request(node, path, timeout, value, **metadata):
    return observation(node=node, run_id=metadata["run_id"],
                       cycle_id=metadata["cycle_id"], op_id=metadata["op_id"])


class ReplyServer:
    """A bounded local fixture that also permits deliberately invalid HTTP framing."""

    def __init__(self, reply):
        self.reply = reply
        self.requests = []
        self.stop = threading.Event()
        owner = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.settimeout(2)
                data = b""
                try:
                    while b"\r\n\r\n" not in data:
                        part = self.request.recv(4096)
                        if not part:
                            return
                        data += part
                        if len(data) > 65536:
                            return
                    head, body = data.split(b"\r\n\r\n", 1)
                    length = 0
                    for line in head.split(b"\r\n")[1:]:
                        key, value = line.split(b":", 1)
                        if key.lower() == b"content-length":
                            length = int(value.strip())
                    while len(body) < length:
                        part = self.request.recv(min(4096, length - len(body)))
                        if not part:
                            return
                        body += part
                    owner.requests.append((head, body))
                    if callable(owner.reply):
                        owner.reply(self.request, owner.stop)
                    else:
                        self.request.sendall(owner.reply)
                except (OSError, ValueError):
                    pass

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True

        self.server = Server(("127.0.0.1", 0), Handler)
        self.endpoint = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.02}, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(1)
        if self.thread.is_alive():
            raise AssertionError("local HTTP fixture did not stop")


def http_reply(body=b'{"ok":true,"index":7}', status=b"200 OK", headers=b""):
    return (b"HTTP/1.1 " + status + b"\r\nContent-Length: " + str(len(body)).encode()
            + b"\r\nConnection: close\r\n" + headers + b"\r\n" + body)


class ClassificationTests(unittest.TestCase):
    def test_ack_requires_positive_nonboolean_u64_and_complete_success(self):
        self.assertEqual(client.classify_write(observation()), ("acknowledged", 7))
        for index in (True, False, 0, -1, 7.0, "7", None, 2**64):
            with self.subTest(index=index):
                body = json.dumps({"ok": True, "index": index})
                self.assertEqual(client.classify_write(observation(body=body)),
                                 ("outcome_unknown", None))
        self.assertEqual(client.classify_write(observation(
            body=json.dumps({"ok": True, "index": 2**64 - 1}))),
            ("acknowledged", 2**64 - 1))
        for record in (observation(error="body was truncated"), observation(status=503),
                       observation(body='{"ok":1,"index":7}')):
            self.assertEqual(client.classify_write(record), ("outcome_unknown", None))

    def test_malformed_and_ambiguous_json_remain_unknown(self):
        for body in ('{"ok":true,"index":7,"index":8}', '{"ok":true,"index":NaN}',
                     '{"ok":true,"index":7,"other":1e999}',
                     '{"ok":true,"index":7} trailing', '[7]', 'null', '',
                     '[' * 2000 + ']' * 2000):
            with self.subTest(body=body[:50]):
                self.assertEqual(client.classify_write(observation(body=body)),
                                 ("outcome_unknown", None))

    def test_server_rejections_and_client_timeouts_are_distinct(self):
        for error in client.REJECTIONS:
            record = observation(status=503, body=json.dumps({"ok": False, "error": error}))
            self.assertEqual(client.classify_write(record), (error, None))
        for error in ({"nested": True}, ["timeout"], "invented"):
            record = observation(status=503, body=json.dumps({"ok": False, "error": error}))
            self.assertEqual(client.classify_write(record), ("outcome_unknown", None))
        self.assertEqual(client.classify_write(observation(error="TimeoutError")),
                         ("outcome_unknown", None))

    def test_input_limits_fail_before_starting_request_worker(self):
        with mock.patch.object(client, "_bounded_http") as request:
            for timeout in (0, -1, float("nan"), float("inf"), True):
                with self.assertRaises(ValueError):
                    client.request_record("node1", "/status", timeout)
            for endpoint in ("http://localhost:0", "http://localhost:65536",
                             "https://localhost", "http://user:pass@localhost",
                             "http://localhost/path", "http://localhost/?query"):
                with self.assertRaises(ValueError):
                    client.request_record("node1", "/status", endpoint=endpoint)
            with self.assertRaises(ValueError):
                client.request_record("node1", "/status", op_id="line\nbreak")
            request.assert_not_called()


class HTTPTests(unittest.TestCase):
    def test_json_content_type_and_existing_default_are_sent_exactly_once(self):
        for content_type, expected in ((None, b"text/plain; charset=utf-8"),
                                       ("text/plain", b"text/plain"),
                                       ("application/json", b"application/json")):
            with ReplyServer(http_reply()) as server:
                result = client.request_record("node1", "/control", 3, '{"step":"fence"}',
                    endpoint=server.endpoint, method="POST", content_type=content_type)
                self.assertIsNone(result["transport_error"])
                self.assertEqual(len(server.requests), 1)
                head, body = server.requests[0]
                self.assertIn(b"Content-Type: " + expected + b"\r\n", head + b"\r\n")
                self.assertEqual(body, b'{"step":"fence"}')

    def test_unapproved_content_type_fails_before_http_worker(self):
        with mock.patch.object(client, "_bounded_http") as worker:
            for content_type in ("application/octet-stream", "application/json\r\nX-Injected: 1", "", 1):
                with self.assertRaises(ValueError):
                    client.request_record("node1", "/control", value="{}", method="POST", content_type=content_type)
            worker.assert_not_called()

    def test_explicit_post_and_delete_reach_server_once(self):
        for method, value, path in (("POST", "nonce", "/sessions"),
                                    ("DELETE", None, "/sessions/7/kv/k?sequence=1")):
            with ReplyServer(http_reply()) as server:
                result = client.request_record("node1", path, 3, value, endpoint=server.endpoint, method=method)
                self.assertIsNone(result["transport_error"])
                self.assertEqual(len(server.requests), 1)
                head, body = server.requests[0]
                self.assertTrue(head.startswith(f"{method} {path} HTTP/1.1".encode()))
                self.assertEqual(body, b"nonce" if value is not None else b"")
        with mock.patch.object(client, "_bounded_http") as worker:
            with self.assertRaises(ValueError):
                client.request_record("node1", "/sessions", method="TRACE")
            worker.assert_not_called()


    def test_read_watermarks_ids_and_monotonic_interval_are_retained(self):
        headers = (b"X-Raft-Term: 4\r\nX-Raft-Last-Applied: 7\r\n"
                   b"X-Raft-Commit-Index: 7\r\nX-Raft-Role: Leader\r\n")
        with ReplyServer(http_reply(b"stored value", headers=headers)) as server:
            record = client.request_record("node1", "/kv/key", 2, endpoint=server.endpoint,
                                           run_id="run", cycle_id="kill-1", op_id="read-1")
        self.assertEqual(record["body"], "stored value")
        self.assertEqual(record["headers"]["x-raft-last-applied"], "7")
        self.assertEqual(record["headers"]["x-raft-term"], "4")
        self.assertEqual((record["run_id"], record["cycle_id"], record["op_id"]),
                         ("run", "kill-1", "read-1"))
        self.assertEqual(record["duration_ns"],
                         record["complete_monotonic_ns"] - record["invoke_monotonic_ns"])
        self.assertGreater(record["duration_ns"], 0)
        self.assertTrue(record["clock_domain"])
        self.assertIsNone(record["transport_error"])

    def test_put_uses_one_exact_http_request_and_does_not_follow_redirect(self):
        with ReplyServer(http_reply(status=b"307 Temporary Redirect",
                         headers=b"Location: http://127.0.0.1:1/redirect\r\n")) as server:
            record = client.request_record("node1", "/kv/a%2Fb", 2, "value \u03bb",
                                           endpoint=server.endpoint)
        self.assertEqual(record["status"], 307)
        self.assertEqual(len(server.requests), 1)
        head, body = server.requests[0]
        self.assertTrue(head.startswith(b"PUT /kv/a%2Fb HTTP/1.1\r\n"))
        self.assertEqual(body.decode("utf-8"), "value \u03bb")
        self.assertEqual(client.classify_write(record), ("outcome_unknown", None))

    def test_incomplete_invalid_or_ambiguous_responses_never_acknowledge(self):
        ack = b'{"ok":true,"index":7}'
        malformed = [
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n" + ack,
            http_reply(b"\xff"),
            b"HTTP/1.1 200 OK\r\nContent-Length: 999999\r\n\r\n" + ack,
            b"HTTP/1.1 200 OK\r\nContent-Length: 21\r\nContent-Length: 21\r\n\r\n" + ack,
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 21\r\n\r\n" + ack,
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n" + ack,
        ]
        for raw in malformed:
            with self.subTest(raw=raw[:80]), ReplyServer(raw) as server:
                record = client.request_record("node1", "/kv/key", 2, "value", endpoint=server.endpoint)
            self.assertIsNotNone(record["transport_error"])
            self.assertEqual(client.classify_write(record), ("outcome_unknown", None))

    def test_trickling_body_obeys_complete_operation_deadline(self):
        def trickle(connection, stop):
            connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 200\r\n\r\n")
            for _ in range(200):
                if stop.wait(0.02):
                    return
                connection.sendall(b"x")

        with ReplyServer(trickle) as server:
            started = time.monotonic()
            record = client.request_record("node1", "/status", 0.3, endpoint=server.endpoint)
            elapsed = time.monotonic() - started
        self.assertLess(elapsed, 1.5)
        self.assertIn("operation_deadline_exceeded", record["transport_error"])
        self.assertEqual(client.classify_write(record), ("outcome_unknown", None))

    def test_stuck_worker_is_killed_reaped_and_cannot_act_after_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "late-action"
            program = ("import pathlib, time; time.sleep(0.8); "
                       + f"pathlib.Path({str(marker)!r}).write_text('late')")
            command = [sys.executable, "-B", "-c", program]
            with mock.patch.object(client, "_worker_command", return_value=command):
                started = time.monotonic()
                result = client._bounded_http({}, 0.15)
            self.assertLess(time.monotonic() - started, 1.5)
            self.assertIn("operation_deadline_exceeded", result["transport_error"])
            time.sleep(0.9)
            self.assertFalse(marker.exists(), "request worker survived its deadline")


class LedgerValidationTests(unittest.TestCase):
    def test_ack_and_unknown_rows_are_preserved(self):
        first = ledger_record()
        second = ledger_record(seq=2, op_id="other", outcome="outcome_unknown", index=None,
                               transport_error="operation_deadline_exceeded")
        self.assertEqual(client.decode_ledger(encoded([first, second])), [first, second])
        self.assertEqual(client.decode_ledger(b""), [])

    def test_corrupt_incomplete_duplicate_or_old_rows_fail_loudly(self):
        good = ledger_record()
        malformed = [encoded([good])[:-1], b"not json\n", b"\n", b'{"seq":1}\n',
                     encoded([ledger_record(seq=2)]), encoded([ledger_record(schema_version=True)]),
                     encoded([ledger_record(index=True)]), encoded([ledger_record(duration_ns=99)]),
                     encoded([ledger_record(outcome="outcome_unknown")]),
                     encoded([ledger_record(key="")]), encoded([ledger_record(value="x" * 65537)]),
                     encoded([ledger_record(headers={"X-Raft-Term": "4"})]),
                     encoded([good, ledger_record(seq=2)])]
        for field in ("index", "transport_error", "status", "clock_domain"):
            missing = copy.deepcopy(good)
            del missing[field]
            malformed.append(encoded([missing]))
        for raw in malformed:
            with self.subTest(raw=raw[:100]), self.assertRaises(ValueError):
                client.decode_ledger(raw)

    def test_duplicate_json_fields_cannot_change_ack_identity(self):
        raw = encoded([ledger_record()]).replace(b'"index": 7', b'"index": 7, "index": 9')
        with self.assertRaises(ValueError):
            client.decode_ledger(raw)

    def test_ledger_cli_validates_all_rows_before_any_output(self):
        with mock.patch.object(client, "read_ledger", side_effect=ValueError("corrupt tail")), \
                mock.patch("sys.stdout", new_callable=io.StringIO) as output:
            with self.assertRaisesRegex(ValueError, "corrupt tail"):
                client.main(["ledger"])
            self.assertEqual(output.getvalue(), "")


class AppendLogicTests(unittest.TestCase):
    """Portable append/error propagation tests; these do not prove POSIX durability."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "acks.jsonl"

        @contextmanager
        def unlocked(path, *, write, timeout):
            with Path(path).open("a+b" if write else "rb") as stream:
                yield stream

        self.lock = mock.patch.object(client, "_locked_ledger", side_effect=unlocked)
        self.lock.start()
        self.addCleanup(self.lock.stop)
        self.request = mock.patch.object(client, "request_record", side_effect=fake_request).start()
        self.addCleanup(mock.patch.stopall)

    def put(self, op_id="op"):
        return client.perform_put("node1", "key", "value", ledger_path=self.path,
                                  run_id="run", cycle_id="cycle", op_id=op_id)

    def test_file_then_directory_sync_precede_ack_return(self):
        order = []
        with mock.patch.object(client.os, "fsync", side_effect=lambda _: order.append("file")), \
                mock.patch.object(client, "_sync_directory", side_effect=lambda _: order.append("directory")):
            record = self.put()
        self.assertEqual(order, ["file", "directory"])
        self.assertEqual(client.decode_ledger(self.path.read_bytes()), [record])
        self.assertEqual(record["outcome"], "acknowledged")

    def test_file_or_directory_sync_failure_never_returns_ack(self):
        for failure_at in ("file", "directory"):
            with self.subTest(failure_at=failure_at):
                self.path.write_bytes(b"")
                with mock.patch.object(client.os, "fsync", side_effect=OSError("file sync failed")
                                       if failure_at == "file" else None), \
                        mock.patch.object(client, "_sync_directory", side_effect=OSError("directory sync failed")
                                          if failure_at == "directory" else None):
                    with self.assertRaisesRegex(OSError, "sync failed"):
                        self.put()

    def test_existing_corruption_or_duplicate_identity_prevents_network_request(self):
        with mock.patch.object(client, "_sync_directory"):
            self.put()
            self.request.reset_mock()
            with self.assertRaisesRegex(ValueError, "already exists"):
                self.put()
            self.request.assert_not_called()
            self.path.write_bytes(b"partial")
            with self.assertRaisesRegex(ValueError, "incomplete"):
                self.put("different")
            self.request.assert_not_called()

    def test_unknown_response_is_fsynced_and_retained(self):
        self.request.side_effect = lambda *args, **kwargs: dict(fake_request(*args, **kwargs),
                                                              transport_error="TimeoutError")
        with mock.patch.object(client, "_sync_directory"):
            record = self.put()
        self.assertEqual(record["outcome"], "outcome_unknown")
        self.assertEqual(client.decode_ledger(self.path.read_bytes()), [record])

    def test_missing_ledger_read_fails_without_creating_it(self):
        with self.assertRaises(FileNotFoundError):
            client.read_ledger(self.path)
        self.assertFalse(self.path.exists())


@unittest.skipUnless(client.fcntl is not None and hasattr(client.os, "O_DIRECTORY"),
                     "requires real POSIX file locks and directory fsync")
class PosixLedgerTests(unittest.TestCase):
    def test_real_sync_and_serialized_concurrent_writers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "acks.jsonl"
            active = 0
            peak = 0
            counter_lock = threading.Lock()

            def network(*args, **kwargs):
                nonlocal active, peak
                with counter_lock:
                    active += 1
                    peak = max(peak, active)
                time.sleep(0.03)
                with counter_lock:
                    active -= 1
                return fake_request(*args, **kwargs)

            with mock.patch.object(client, "request_record", side_effect=network), \
                    ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(client.perform_put, "node1", f"key-{i}", "value",
                                           ledger_path=path, run_id="run", cycle_id="cycle", op_id=f"op-{i}")
                           for i in range(3)]
                records = [future.result(timeout=3) for future in futures]
            self.assertEqual(peak, 1)
            self.assertEqual(sorted(record["seq"] for record in records), [1, 2, 3])
            self.assertEqual(len(client.read_ledger(path)), 3)

    def test_lock_wait_has_a_deadline_and_sends_no_request(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "acks.jsonl"
            with client._locked_ledger(path, write=True, timeout=1), \
                    mock.patch.object(client, "request_record") as request:
                started = time.monotonic()
                with self.assertRaisesRegex(TimeoutError, "lock acquisition"):
                    client.perform_put("node1", "key", "value", 0.05, ledger_path=path)
                self.assertLess(time.monotonic() - started, 0.5)
                request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
