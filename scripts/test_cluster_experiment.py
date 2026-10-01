"""Negative controls for the experiment's authority, ledger and cleanup oracle."""
import argparse
import contextlib
import io
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import cluster_experiment as experiment


def observation(**fields):
    result = dict(schema_version=1, node="node1", run_id="test-project", cycle_id="kill-1",
                  op_id="test-project-1", clock_domain="test-clock", invoke_monotonic_ns=10,
                  complete_monotonic_ns=20, duration_ns=10, status=200, body="",
                  headers={}, transport_error=None)
    result.update(fields)
    return result


def ack(seq=1, key="key", value="value", index=4):
    body = json.dumps(dict(ok=True, index=index))
    return observation(seq=seq, key=key, value=value, index=index, outcome="acknowledged",
                       body=body, response=body, op_id=f"test-project-{seq}")


def unknown(seq=2, key="key"):
    return observation(seq=seq, key=key, value="other", index=None, outcome="outcome_unknown",
                       status=None, body="", response="", transport_error="deadline",
                       op_id=f"test-project-{seq}")


def status(node, role, term=2, hint=2):
    return dict(node_id=node, role=role, term=term, leader_hint=hint, last_applied=4)


def read(value="value", watermark="4", role="leader", code=200):
    return observation(status=code, body=value, transport_error=None,
                headers={"x-raft-last-applied": watermark, "x-raft-role": role})


class OracleTests(unittest.TestCase):
    def test_isolated_old_leader_is_excluded_by_component(self):
        states = {"node1": status(1, "leader", 1, 1), "node2": status(2, "leader"),
                  "node3": status(3, "follower")}
        self.assertEqual(experiment.qualified_leader(states, ("node2", "node3"), 2), ("node2", 2))
        self.assertIsNone(experiment.qualified_leader(states))
        self.assertIsNone(experiment.qualified_leader(states, ("node1",)))
        self.assertIsNone(experiment.qualified_leader(states, ("node2", "node3"), 3))

    def test_follower_must_agree_on_term_and_hint(self):
        states = {"node2": status(2, "leader"), "node3": status(3, "follower", 1)}
        self.assertIsNone(experiment.qualified_leader(states, ("node2", "node3")))
        states["node3"] = status(3, "follower", hint=1)
        self.assertIsNone(experiment.qualified_leader(states, ("node2", "node3")))

    def test_same_term_pre_candidate_does_not_erase_aligned_majority(self):
        states = {"node1": status(1, "leader", 1, 1),
                  "node2": status(2, "follower", 1, 1),
                  "node3": status(3, "pre_candidate", 1, 1)}
        self.assertEqual(experiment.qualified_leader(states), ("node1", 1))
        states["node2"]["role"] = "pre_candidate"
        self.assertIsNone(experiment.qualified_leader(states))
        states["node1"]["role"] = "pre_candidate"
        states["node2"]["role"] = "follower"
        self.assertIsNone(experiment.qualified_leader(states))

    def test_newer_term_or_second_leader_cannot_be_ignored(self):
        states = {"node1": status(1, "leader", 1, 1),
                  "node2": status(2, "follower", 1, 1),
                  "node3": status(3, "pre_candidate", 2, 1)}
        self.assertIsNone(experiment.qualified_leader(states))
        states["node3"] = status(3, "leader", 1, 3)
        self.assertIsNone(experiment.qualified_leader(states))

    def test_ambiguous_overwrite_is_inconclusive(self):
        records = [ack(), unknown()]
        with self.assertRaises(experiment.Inconclusive):
            experiment.acknowledged_state(records)
        # A later ACK alone cannot resolve whether an unknown operation finishes late.
        records.append(ack(3))
        with self.assertRaises(experiment.Inconclusive):
            experiment.acknowledged_state(records)

    def test_unique_unknown_key_does_not_erase_known_ack(self):
        records = [ack(), unknown(key="different")]
        self.assertEqual(experiment.acknowledged_state(records), {"key": records[0]})

    def test_bad_ack_and_noncontiguous_ledger_fail(self):
        for record in (ack(index=True), ack(index=0), ack(seq=2)):
            with self.subTest(record=record), self.assertRaises(experiment.ExperimentError):
                experiment.acknowledged_state([record])

    def test_fence_precedes_value_oracle(self):
        with self.assertRaises(experiment.Inconclusive):
            experiment.assert_read(read("wrong", "3"), ack(), 4)
        with self.assertRaises(experiment.ExperimentError) as caught:
            experiment.assert_read(read("wrong"), ack(), 4)
        self.assertNotIsInstance(caught.exception, experiment.Inconclusive)
        experiment.assert_read(read(), ack(), 4, require_leader=True)

    def test_missing_value_after_fence_is_detected(self):
        with self.assertRaises(experiment.ExperimentError) as caught:
            experiment.assert_read(read("", code=404), ack(), 4)
        self.assertNotIsInstance(caught.exception, experiment.Inconclusive)

    def test_forged_success_is_rejected_by_observed_response(self):
        corruptions = [
            {"status": 503}, {"transport_error": "partial body"}, {"index": 5},
            {"body": '{"ok":false,"index":4}'}, {"body": '{"ok":true,"index":4,"index":5}'},
            {"body": '{"ok":true,"index":NaN}'}, {"response": "different"},
            {"body": '{"ok":true,"index":4.0}'}, {"schema_version": True},
            {"headers": {"X-Raft-Last-Applied": "4"}}, {"complete_monotonic_ns": 9},
        ]
        for change in corruptions:
            record = ack()
            record.update(change)
            if "body" in change:
                record["response"] = record["body"]
            with self.subTest(change=change), self.assertRaises(experiment.ExperimentError):
                experiment.acknowledged_state([record])

    def test_duplicate_identity_and_ambiguous_prior_write_are_rejected(self):
        duplicate = ack(seq=2)
        duplicate["op_id"] = ack()["op_id"]
        with self.assertRaisesRegex(experiment.ExperimentError, "identity"):
            experiment.acknowledged_state([ack(), duplicate])
        with self.assertRaises(experiment.Inconclusive):
            experiment.acknowledged_state([unknown(seq=1), ack(seq=2)])

    def test_non_follower_and_boolean_status_cannot_qualify_a_majority(self):
        states = {"node1": status(1, "leader", 1, 1), "node2": status(2, "follower", 1, 1)}
        for change in ({"role": "candidate"}, {"term": True}, {"leader_hint": True}):
            candidate = {name: dict(value) for name, value in states.items()}
            candidate["node2"].update(change)
            with self.subTest(change=change):
                self.assertIsNone(experiment.qualified_leader(candidate, ("node1", "node2")))
        self.assertIsNone(experiment.qualified_leader(states, ("node1", "node1")))

    def test_watermark_must_be_an_unsigned_u64_header(self):
        for watermark in ("-1", "+4", " 4", "4, 4", str(2**64)):
            with self.subTest(watermark=watermark), self.assertRaises(experiment.Inconclusive):
                experiment.assert_read(read(watermark=watermark), ack(), 4)


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.out = Path(self.temp.name) / "output"
        self.args = argparse.Namespace(out=str(self.out), compose=__file__, project="test-project",
                                       mode="smoke", fault="all", runs=1, timeout_seconds=1)
        self.runner = experiment.Runner(self.args)
        self.runner.event = Mock()

    def test_existing_images_are_paired_and_resolved_without_build(self):
        self.args.node_image = "node:old"
        with self.assertRaisesRegex(experiment.ExperimentError, "together"):
            experiment.Runner(self.args)
        self.args.client_image = "client:old"
        runner = experiment.Runner(self.args)
        self.out.mkdir()
        ids = {"node": "sha256:" + "a" * 64, "client": "sha256:" + "b" * 64}
        runner.command = Mock(side_effect=lambda argv, **kw: (
            0, json.dumps([{"Id": ids[kw["name"].removesuffix("-image")]}])))
        runner.prepare_images()
        self.assertEqual(runner.command.call_count, 2)
        self.assertEqual(runner.env["MICRO_RAFT_IMAGE"], ids["node"])
        self.assertEqual(runner.env["MICRO_RAFT_CLIENT_IMAGE"], ids["client"])
        recorded = json.loads((self.out / "image-selection.json").read_text())
        self.assertEqual(recorded["mode"], "existing")
        self.assertEqual(recorded["images"]["node"], {
            "requested": "node:old", "id": ids["node"], "repo_digests": []})
        with self.assertRaisesRegex(experiment.ExperimentError, "differs"):
            runner.require_image({"Image": "sha256:" + "c" * 64}, "node")
        runner.require_image({"Image": ids["node"]}, "node")

    def test_missing_or_malformed_existing_image_cannot_create_resources(self):
        self.args.node_image, self.args.client_image = "node:old", "client:old"
        for response in ("[]", '[{"Id":"mutable-tag"}]'):
            runner = experiment.Runner(self.args)
            runner.inventory = Mock(return_value=[])
            runner.command = Mock(return_value=(0, response))
            with self.subTest(response=response), self.assertRaises(experiment.ExperimentError):
                runner.setup()
            self.assertFalse(runner.owned)
            self.assertFalse(any("up" in call.args[0] or "run" in call.args[0]
                                 for call in runner.command.call_args_list))

    def test_default_images_build_then_pin_before_any_container_creation(self):
        self.out.mkdir()
        def command(argv, **kwargs):
            if kwargs.get("name") == "build":
                return 0, ""
            return 0, json.dumps([{"Id": "sha256:" + "d" * 64}])
        self.runner.command = Mock(side_effect=command)
        self.runner.prepare_images()
        self.assertEqual(self.runner.command.call_args_list[0].args[0], self.runner.dc + ["build"])
        self.assertEqual(json.loads((self.out / "image-selection.json").read_text())["mode"], "build")
        self.assertEqual(self.runner.env["MICRO_RAFT_IMAGE"], "sha256:" + "d" * 64)

    def test_existing_artifacts_are_never_overwritten(self):
        self.out.mkdir()
        result = self.out / "result.json"
        result.write_text("previous evidence", encoding="utf-8")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.runner.run(), 1)
        self.assertEqual(result.read_text(encoding="utf-8"), "previous evidence")

    def test_ledger_failure_prevents_fault_injection(self):
        self.runner.leader = Mock(return_value=("node1", 1))
        self.runner.put = Mock(side_effect=experiment.ExperimentError("directory fsync failed"))
        self.runner.inject = Mock()
        with self.assertRaisesRegex(experiment.ExperimentError, "fsync"):
            self.runner.cycle("kill", 1)
        self.runner.inject.assert_not_called()
        self.assertEqual(self.runner.cycles[0]["outcome"], "failed")

    def test_successful_command_is_not_proof_of_injection(self):
        self.runner.containers = {"node1": "container-1"}
        self.runner.command = Mock(return_value=(0, ""))
        self.runner.inspect = Mock(return_value={"State": {"Running": True, "Pid": 22, "StartedAt": "original"}})
        def once(description, predicate):
            if not predicate():
                raise experiment.Deadline(description)
        self.runner.wait_for = once
        with self.assertRaises(experiment.Deadline):
            self.runner.inject("kill", "node1")
        self.assertEqual(self.runner.active_fault, ("kill", "node1"))

    def test_full_network_identity_prevents_false_partition_confirmation(self):
        full_id = "a1" * 32
        info = {"Id": full_id, "Labels": {"com.docker.compose.project": "test-project"}}
        self.runner.network = experiment.owned_network_id(info, "test-project")
        self.runner.inspect = Mock(return_value={"NetworkSettings": {"Networks": {
            "raft": {"NetworkID": full_id}}}})
        self.assertFalse(self.runner.injection_confirmed("partition", "node1"))
        self.runner.inspect.return_value["NetworkSettings"]["Networks"].clear()
        self.assertTrue(self.runner.injection_confirmed("partition", "node1"))
        for invalid in ({**info, "Id": full_id[:12]}, {**info, "Labels": {}}):
            with self.subTest(info=invalid), self.assertRaises(experiment.ExperimentError):
                experiment.owned_network_id(invalid, "test-project")
    def test_held_restart_requires_clean_stop_and_defers_start(self):
        self.runner.containers = {"node1": "container-1"}
        before = {"State": {"Running": True, "Pid": 22, "StartedAt": "original"}}
        stopped = {"State": {"Running": False, "Pid": 0, "ExitCode": 0}}
        self.runner.inspect = Mock(side_effect=[before, stopped])
        self.runner.command = Mock(return_value=(0, ""))
        self.runner.inject("restart", "node1")
        self.runner.command.assert_called_once_with(
            ["docker", "stop", "--timeout", "10", "container-1"], timeout=20)
        self.assertEqual(self.runner.active_fault, ("restart", "node1"))
        self.assertEqual(self.runner.fault_started_at, "original")

    def test_forced_kill_is_not_clean_restart_confirmation(self):
        self.runner.inspect = Mock(return_value={"State": {
            "Running": False, "Pid": 0, "ExitCode": 137}})
        with self.assertRaisesRegex(experiment.ExperimentError, "did not stop cleanly"):
            self.runner.injection_confirmed("restart", "node1")
        self.assertTrue(self.runner.injection_confirmed("kill", "node1"))
    def test_term_change_makes_read_mismatch_inconclusive(self):
        before = {"node2": status(2, "leader"), "node3": status(3, "follower")}
        after = {"node2": status(2, "follower", 3, 3), "node3": status(3, "leader", 3, 3)}
        self.runner.statuses = Mock(side_effect=[before, after])
        self.runner.client = Mock(return_value=read("wrong"))
        with self.assertRaisesRegex(experiment.Inconclusive, "during read-back"):
            self.runner.read_back(("node2", "node3"), "node2", 2, [ack()], 4)

    def test_cleanup_error_cannot_produce_pass(self):
        def setup():
            self.out.mkdir()
            self.runner.output_owned = True
        self.runner.setup = setup
        self.runner.cycle = Mock()
        self.runner.finish = lambda: self.runner.errors.append("network still exists")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.runner.run(), 1)
        self.assertEqual(json.loads((self.out / "result.json").read_text())["verdict"], "FAIL")

    def test_partition_heal_restores_service_dns_alias(self):
        self.runner.containers = {"node1": "container-1"}
        self.runner.network = "network-1"
        self.runner.active_fault = ("partition", "node1")
        disconnected = {"State": {"Running": True, "Paused": False}, "NetworkSettings": {"Networks": {}}}
        connected = {"State": disconnected["State"], "NetworkSettings": {"Networks": {
            "raft": {"NetworkID": "network-1", "Aliases": ["node1"]}}}}
        self.runner.inspect = Mock(side_effect=[disconnected, connected])
        self.runner.command = Mock(return_value=(0, ""))
        self.runner.heal()
        self.runner.command.assert_called_once_with(["docker", "network", "connect", "--alias",
            "node1", "network-1", "container-1"])
        self.assertIsNone(self.runner.active_fault)

    def prepare_fake_run(self):
        def setup():
            self.out.mkdir()
            self.runner.output_owned = True
        self.runner.setup = setup
        self.runner.finish = Mock()

    def completed_cycle(self, fault, number):
        self.runner.cycles.append(dict(cycle_id=f"{fault}-{number}", fault=fault, outcome="pass"))

    def test_missing_or_wrong_planned_cycles_cannot_pass(self):
        for mutation in ("missing", "wrong", "inconclusive"):
            with self.subTest(mutation=mutation):
                self.runner.cycles = []
                self.runner.setup = Mock()
                self.runner.finish = Mock()
                if mutation == "missing":
                    self.runner.cycle = Mock()
                else:
                    self.runner.cycle = lambda fault, number: self.runner.cycles.append(dict(
                        cycle_id="pause-1" if mutation == "wrong" else "kill-1", fault=fault,
                        outcome="inconclusive" if mutation == "inconclusive" else "pass"))
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(self.runner.run(), 1)

    def test_complete_planned_cycles_and_cleanup_pass(self):
        self.prepare_fake_run()
        self.runner.cycle = self.completed_cycle
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.runner.run(), 0)
        result = json.loads((self.out / "result.json").read_text())
        self.assertEqual((result["planned"], result["attempted"], result["counts"]["pass"]), (1, 1, 1))

    def test_cleanup_exception_and_result_sync_failure_return_failure(self):
        self.prepare_fake_run()
        self.runner.cycle = self.completed_cycle
        self.runner.finish = Mock(side_effect=OSError("cleanup evidence cannot sync"))
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.runner.run(), 1)
        self.assertIn("cannot sync", (self.out / "result.json").read_text())
        self.runner.setup = Mock()
        self.runner.cycles.clear()
        self.runner.errors.clear()
        self.runner.finish = Mock()
        with patch.object(experiment, "write_evidence", side_effect=OSError("result disk full")):
            with contextlib.redirect_stdout(io.StringIO()) as printed:
                self.assertEqual(self.runner.run(), 1)
        self.assertEqual(json.loads(printed.getvalue())["verdict"], "FAIL")

    def test_failed_result_fsync_never_publishes_pass(self):
        self.prepare_fake_run()
        self.runner.cycle = self.completed_cycle
        with patch.object(experiment.os, "fsync", side_effect=OSError("cannot sync verdict")):
            with contextlib.redirect_stdout(io.StringIO()) as printed:
                self.assertEqual(self.runner.run(), 1)
        self.assertEqual(json.loads(printed.getvalue())["verdict"], "FAIL")
        self.assertFalse((self.out / "result.json").exists())
        self.assertTrue((self.out / "result.json.pending").exists())

    def test_capture_failure_does_not_skip_remaining_cleanup(self):
        self.out.mkdir()
        self.runner.owned = True
        self.runner.observer = "owned-observer-id"
        self.runner.inventory = Mock(return_value=[])
        def command(argv, **kwargs):
            if kwargs.get("name") == "nodes-log":
                raise OSError("log evidence disk full")
            return 0, ""
        self.runner.command = Mock(side_effect=command)
        self.runner.finish()
        self.assertTrue(any("nodes-log" in error for error in self.runner.errors))
        self.runner.command.assert_any_call(["docker", "rm", "-f", "owned-observer-id"])
        self.runner.command.assert_any_call(self.runner.dc + [
            "down", "--remove-orphans", "--timeout", "15"], timeout=60, name="down")
        self.assertEqual(self.runner.inventory.call_count, 3)

    def test_late_true_predicate_is_a_timeout_and_budget_is_restored(self):
        with patch.object(experiment.time, "monotonic", side_effect=[0, 0, 1.1]):
            with self.assertRaises(experiment.Deadline):
                self.runner.wait_for("late success", lambda: True)
        self.assertIsNone(self.runner.poll_deadline)

    def test_command_budget_is_limited_by_remaining_poll_time(self):
        self.out.mkdir()
        self.runner.poll_deadline = 20
        process = Mock(returncode=0)
        process.communicate.return_value = ("result", "")
        with patch.object(experiment.time, "monotonic", return_value=19.75):
            with patch.object(experiment.subprocess, "Popen", return_value=process):
                self.runner.command(["unused"], timeout=30)
        process.communicate.assert_called_once_with(timeout=0.25)
        with patch.object(experiment.time, "monotonic", return_value=20):
            with patch.object(experiment.subprocess, "Popen") as spawn:
                with self.assertRaises(experiment.Deadline):
                    self.runner.command(["unused"])
                spawn.assert_not_called()

    def test_interrupt_preserves_partial_cli_evidence(self):
        self.out.mkdir()
        process = Mock(pid=42)
        process.communicate.side_effect = [KeyboardInterrupt(), ("partial output", "canceled")]
        with patch.object(experiment.subprocess, "Popen", return_value=process), patch.object(signal, "SIGKILL", 9, create=True):
            with patch.object(experiment.os, "killpg", create=True) as kill:
                with self.assertRaises(KeyboardInterrupt):
                    self.runner.command(["docker", "mutation"])
        kill.assert_called_once_with(42, signal.SIGKILL if hasattr(signal, "SIGKILL") else 9)
        self.assertEqual((self.out / "command-00001.stdout").read_text(), "partial output")

    def test_foreign_project_resources_are_never_removed(self):
        self.runner.inventory = Mock(return_value=["foreign-container"])
        self.runner.command = Mock()
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.runner.run(), 1)
        self.assertFalse(self.runner.owned)
        self.runner.command.assert_not_called()

    def test_failed_observer_creation_does_not_claim_foreign_name(self):
        self.runner.inventory = Mock(side_effect=[[], [], [], ["network-1"]])
        self.runner.inspect = Mock()
        def command(argv, **kwargs):
            if kwargs.get("name") in ("node-image", "client-image"):
                return 0, json.dumps([{"Id": "sha256:" + "a" * 64}])
            if kwargs.get("name") == "observer-start":
                raise experiment.ExperimentError("name already belongs to another container")
            if kwargs.get("name") == "network-before":
                return 0, json.dumps([{"Id": "ab" * 32, "Labels": {"com.docker.compose.project": "test-project"}}])
            if "ps" in argv:
                return 0, "container-id"
            return 0, ""
        self.runner.command = Mock(side_effect=command)
        with self.assertRaisesRegex(experiment.ExperimentError, "name already"):
            self.runner.setup()
        self.assertIsNone(self.runner.observer)
        self.assertTrue(self.runner.owned)

    def test_same_start_timestamp_or_missing_alias_cannot_prove_heal(self):
        self.runner.containers = {"node1": "container-1"}
        self.runner.network = "network-1"
        self.runner.command = Mock(return_value=(0, ""))
        def once(description, predicate):
            if not predicate():
                raise experiment.Deadline(description)
        self.runner.wait_for = once
        for fault, aliases, started in (("kill", ["node1"], "original"),
                                        ("restart", ["node1"], "original"),
                                        ("partition", [], "new")):
            self.runner.active_fault = (fault, "node1")
            self.runner.fault_started_at = "original"
            state = {"State": {"Running": True, "Paused": False, "StartedAt": started},
                     "NetworkSettings": {"Networks": {
                         "raft": {"NetworkID": "network-1", "Aliases": aliases}}}}
            self.runner.inspect = Mock(return_value=state)
            with self.subTest(fault=fault), self.assertRaises(experiment.Deadline):
                self.runner.heal()
            self.assertIsNotNone(self.runner.active_fault)

    def test_stable_catchup_wrong_value_fails_without_retry(self):
        states = {"node1": status(1, "follower"), "node2": status(2, "leader"),
                  "node3": status(3, "follower")}
        self.runner.statuses = Mock(return_value=states)
        self.runner.client = Mock(return_value=read("wrong"))
        with self.assertRaises(experiment.ExperimentError) as caught:
            self.runner.catch_up("node1", [ack()], 4)
        self.assertNotIsInstance(caught.exception, experiment.Inconclusive)
        self.runner.client.assert_called_once()

    def test_catchup_term_change_retries_instead_of_claiming_loss(self):
        before = {"node1": status(1, "follower"), "node2": status(2, "leader"),
                  "node3": status(3, "follower")}
        after = {"node1": status(1, "follower", 3, 3), "node2": status(2, "follower", 3, 3),
                 "node3": status(3, "leader", 3, 3)}
        self.runner.statuses = Mock(side_effect=[before, after, after, after])
        self.runner.client = Mock(side_effect=[read("wrong"), read()])
        self.assertEqual(self.runner.catch_up("node1", [ack()], 4), ("node3", 3))

    def test_client_ack_identity_and_exit_code_must_match_request(self):
        self.runner.observer = "observer-id"
        for mutation in ({"node": "node3"}, {"run_id": "other-run"},
                         {"value": "other-value"}, {"status": 503}):
            self.runner.observation_no = 0
            record = ack(key="key", value="value")
            record.update(mutation)
            self.runner.command = Mock(return_value=(0, json.dumps(record) + "\n"))
            with self.subTest(mutation=mutation), self.assertRaises(experiment.ExperimentError):
                self.runner.client("put", "node1", run_id="test-project", cycle_id="kill-1",
                                   op_id="test-project-1", key="key", value="value")

    @unittest.skipUnless(os.name == "posix", "POSIX process-group cancellation")
    def test_real_child_deadline_is_bounded_and_retains_raw_output(self):
        self.out.mkdir()
        started = time.monotonic()
        with self.assertRaises(experiment.Deadline):
            self.runner.wait_for("slow child", lambda: self.runner.command([
                sys.executable, "-u", "-c", "import time; print('started'); time.sleep(30)"
            ]))
        self.assertLess(time.monotonic() - started, 3)
        self.assertIn("started", (self.out / "command-00001.stdout").read_text())


if __name__ == "__main__":
    unittest.main()
