"""Recorder provenance, durable-boundary, HTTP-mode, and interrupted-history checks."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import record_history as recorder
import check_history

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "deploy/docker/client"))
from test_client import ReplyServer, http_reply


def workload(endpoint="http://127.0.0.1:1"):
    return dict(schema_version=1, endpoints={"node1": endpoint}, initial={"k": None},
                batches=[[dict(id="r", kind="get", node="node1", key="k")]])


def response(identity="w", body='{"ok":true,"index":7}', status=200, **fields):
    result = dict(schema_version=1, op_id=identity, node="node1", run_id="run", cycle_id="history",
        clock_domain=recorder.client.CLOCK_DOMAIN, invoke_monotonic_ns=20, complete_monotonic_ns=30,
        duration_ns=10, status=status, body=body, headers={}, transport_error=None)
    result.update(fields)
    return result


class RecorderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "journal.jsonl"
        self.header = dict(schema_version=1, clock_domain=recorder.client.CLOCK_DOMAIN, initial={"k": None},
                           run_id="run", endpoints={"node1": "http://127.0.0.1:1"}, timeout_seconds=3)
        self.write = dict(id="w", node="node1", kind="put", key="k", value="a")

    def journal(self):
        return recorder.Journal(self.path, self.header)

    def test_real_http_linearizable_absence_and_local_mode_rejection(self):
        for headers, expected in ((b"X-Raft-Read-Mode: linearizable\r\n", "PASS"), (b"", "INCONCLUSIVE")):
            with ReplyServer(http_reply(b"", b"404 Not Found", headers)) as server:
                h = recorder.record(workload(server.endpoint), self.path, timeout=3)
                self.assertEqual(check_history.check(h)["verdict"], expected)
                self.assertTrue(server.requests[0][0].startswith(b"GET /kv/k?consistency=linearizable HTTP/1.1"))
                events = [json.loads(line) for line in self.path.read_text().splitlines()]
                self.assertEqual([row["event"] for row in events], ["header", "invoke", "complete"])
                self.assertEqual(h["clock_domain"], recorder.client.CLOCK_DOMAIN)
            self.path.unlink()

    def test_real_put_delete_and_protected_paths(self):
        cases = [(self.write, b"PUT /kv/k HTTP/1.1", b"a", '{"ok":true,"index":7}'),
                 (dict(self.write, id="delete", kind="delete"), b"DELETE /kv/k HTTP/1.1", b"", '{"ok":true,"index":7}'),
                 (dict(self.write, session_id=3, sequence=1), b"PUT /sessions/3/kv/k?sequence=1 HTTP/1.1", b"a",
                  '{"ok":true,"index":7,"session_id":3,"sequence":1}')]
        for operation, prefix, body, reply in cases:
            with ReplyServer(http_reply(reply.encode())) as server:
                spec = workload(server.endpoint)
                spec["batches"] = [[operation]]
                h = recorder.record(spec, self.path)
                self.assertEqual(check_history.check(h)["verdict"], "PASS")
                self.assertTrue(server.requests[0][0].startswith(prefix))
                self.assertEqual(server.requests[0][1], body)
            self.path.unlink()

    def test_interrupted_request_becomes_pending_unknown(self):
        journal = self.journal()
        journal.append(dict(event="invoke", invoke_ns=10, operation=self.write))
        journal.close()
        h = recorder.load_journal(self.path)
        self.assertEqual(h["operations"][0]["outcome"], "unknown")
        self.assertIsNone(h["operations"][0]["complete_ns"])
        self.assertEqual(check_history.check(h)["verdict"], "PASS")

    def test_reject_duplicate_orphan_wrong_clock_and_invalid_time(self):
        for mutation in ("orphan", "duplicate", "clock", "time", "id"):
            journal = self.journal()
            if mutation != "orphan":
                journal.append(dict(event="invoke", invoke_ns=10, operation=self.write))
            raw = response()
            if mutation == "clock":
                raw["clock_domain"] = "other-machine"
            if mutation == "time":
                raw["complete_monotonic_ns"] = 5
            if mutation == "id":
                raw["op_id"] = "other-request"
            event = dict(event="complete", id="w", response=raw)
            journal.append(event)
            if mutation == "duplicate":
                journal.append(event)
            journal.close()
            with self.assertRaises(ValueError, msg=mutation):
                recorder.load_journal(self.path)
            self.path.unlink()

    def test_malformed_tail_duplicate_field_or_nonobject_is_rejected(self):
        for text in ('{"event":"header"}', '{"event":"header","event":"header"}\n', '[]\n'):
            self.path.write_text(text)
            with self.assertRaises(ValueError):
                recorder.load_journal(self.path)

    def test_creation_is_exclusive_and_fsync_failure_sends_nothing(self):
        journal = self.journal()
        journal.close()
        with self.assertRaises(FileExistsError):
            self.journal()
        self.path.unlink()
        with patch.object(recorder.os, "fsync", side_effect=OSError("disk full")):
            with patch.object(recorder.client, "request_record") as request:
                with self.assertRaises(OSError):
                    recorder.record(workload(), self.path)
                request.assert_not_called()

    def test_invocation_sync_precedes_network_and_response_sync_precedes_return(self):
        observed = []
        journal = self.journal()
        def network(*args, **kwargs):
            observed.append("http")
            self.assertEqual(json.loads(self.path.read_text().splitlines()[-1])["event"], "invoke")
            return response(invoke_monotonic_ns=2**62, complete_monotonic_ns=2**62 + 1)
        with patch.object(recorder.os, "fsync", side_effect=lambda fd: observed.append("sync")):
            with patch.object(recorder.client, "request_record", side_effect=network):
                recorder.attempt(self.write, {"node1": "http://localhost:1"}, journal, "run", 3)
        journal.close()
        self.assertEqual(observed, ["sync", "http", "sync"])

    def test_protected_success_must_echo_correct_identity(self):
        protected = dict(self.write, session_id=3, sequence=2)
        for body in ('{"ok":true,"index":7}', '{"ok":true,"index":7,"session_id":4,"sequence":2}',
                     '{"ok":true,"index":7,"session_id":3,"sequence":true}'):
            with self.assertRaises(ValueError):
                recorder.completed_operation(protected, 10, response(body=body))

    def test_session_rejections_and_transport_timeouts_remain_distinct(self):
        rejected = recorder.completed_operation(self.write, 10,
            response(body='{"ok":false,"error":"payload_mismatch"}', status=409))
        self.assertEqual(rejected["outcome"], "rejected")
        unknown = recorder.completed_operation(self.write, 10,
            response(body='', status=None, transport_error="deadline"))
        self.assertEqual(unknown["outcome"], "unknown")

    def test_replay_validates_complete_header(self):
        for field in ("schema_version", "clock_domain", "initial", "run_id", "endpoints", "timeout_seconds"):
            header = dict(self.header)
            header.pop(field)
            self.path.write_text(json.dumps(dict(event="header", **header)) + "\n", encoding="utf-8")
            with self.assertRaises(ValueError, msg=field):
                recorder.load_journal(self.path)
        for update in (dict(schema_version=True), dict(run_id=""), dict(clock_domain=""),
                       dict(endpoints={"node1": 3}), dict(timeout_seconds=False),
                       dict(timeout_seconds=-1), dict(timeout_seconds=10**400), dict(initial=[])):
            header = dict(self.header, **update)
            self.path.write_text(json.dumps(dict(event="header", **header)) + "\n", encoding="utf-8")
            with self.assertRaises(ValueError, msg=update):
                recorder.load_journal(self.path)

    def test_replay_rejects_malformed_pending_invocations(self):
        for update in (dict(session_id=0, sequence="not-a-sequence"), dict(session_id=True, sequence=1),
                       dict(session_id=1), dict(sequence=1), dict(session_id=1, sequence=2**64),
                       dict(id=""), dict(node="unrelated-node"), dict(node=[]), dict(key="missing"),
                       dict(value=3), dict(kind="patch"), dict(kind="get", session_id=1, sequence=1)):
            journal = self.journal()
            journal.append(dict(event="invoke", invoke_ns=10, operation=dict(self.write, **update)))
            journal.close()
            with self.assertRaises(ValueError, msg=update):
                recorder.load_journal(self.path)
            self.path.unlink()

    def test_replay_validates_observation_provenance_and_duration(self):
        for field in ("schema_version", "node", "run_id", "cycle_id", "op_id", "clock_domain",
                      "invoke_monotonic_ns", "complete_monotonic_ns", "duration_ns"):
            for missing in (True, False):
                raw = response()
                if missing:
                    raw.pop(field)
                else:
                    raw[field] = "wrong" if isinstance(raw[field], str) else 999
                journal = self.journal()
                journal.append(dict(event="invoke", invoke_ns=10, operation=self.write))
                journal.append(dict(event="complete", id="w", response=raw))
                journal.close()
                with self.assertRaises(ValueError, msg=(field, missing)):
                    recorder.load_journal(self.path)
                self.path.unlink()

    def test_unicode_line_separators_are_json_content(self):
        journal = self.journal()
        value = "first\u2028second\u2029last"
        journal.append(dict(event="invoke", invoke_ns=10, operation=dict(self.write, value=value)))
        journal.append(dict(event="complete", id="w", response=response()))
        journal.close()
        h = recorder.load_journal(self.path)
        self.assertEqual(h["operations"][0]["value"], value)
        self.assertEqual(check_history.check(h)["verdict"], "PASS")

    def test_replay_enforces_attempt_limit_including_omittable_reads(self):
        journal = self.journal()
        for index in range(257):
            journal.append(dict(event="invoke", invoke_ns=index,
                                operation=dict(self.write, id=f"read-{index}", kind="get")))
        journal.close()
        with self.assertRaisesRegex(ValueError, "256 attempts"):
            recorder.load_journal(self.path)

    def test_valid_protected_pending_attempt_remains_unknown(self):
        journal = self.journal()
        journal.append(dict(event="invoke", invoke_ns=10,
                            operation=dict(self.write, session_id=3, sequence=1)))
        journal.close()
        h = recorder.load_journal(self.path)
        result = check_history.check(h)
        self.assertEqual(result["verdict"], "PASS")
        self.assertEqual(result["outcomes"], {"ok": 0, "unknown": 1, "rejected": 0})
        self.assertEqual(result["successful_reads"], 0)
        self.assertEqual(result["keys"]["k"]["witness"][0]["action"], "omit_unknown")

    def test_invalid_workload_fails_before_journal_creation(self):
        for mutate in (lambda h: h["initial"].clear(),
                       lambda h: h["batches"][0][0].update(session_id=1, sequence=1),
                       lambda h: h["batches"][0].append(copy.deepcopy(h["batches"][0][0])),
                       lambda h: h["batches"][0][0].update(node="missing")):
            h = workload()
            mutate(h)
            with self.assertRaises(ValueError):
                recorder.record(h, self.path)
            self.assertFalse(self.path.exists())


if __name__ == "__main__":
    unittest.main()
