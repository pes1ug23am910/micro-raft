#!/usr/bin/env python3
"""Bounded three-node Compose fault experiments with retained raw evidence.

Exit 0 requires the fault, majority authority, acknowledged-state read-back,
reopened-node catch-up, subsequent writes, evidence capture and cleanup checks.
Local reads are post-stabilization observations, not a linearizable-read API.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import sys
import time

NODES = ("node1", "node2", "node3")
FAULTS = ("kill", "pause", "partition", "restart")
POLL_SECONDS = 0.2
DEFINITE_REJECTIONS = {"not_leader", "timeout", "unavailable", "shutting_down"}
U64_MAX = 2**64 - 1


class ExperimentError(RuntimeError):
    pass


class Inconclusive(ExperimentError):
    pass


class Deadline(ExperimentError):
    pass


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
    return json.loads(text, object_pairs_hook=pairs, parse_constant=constant)


def validate_observation(record):
    if not isinstance(record, dict) or type(record.get("schema_version")) is not int or record["schema_version"] != 1:
        raise ExperimentError("invalid client observation schema")
    for field in ("node", "run_id", "cycle_id", "op_id", "clock_domain"):
        if not isinstance(record.get(field), str) or not record[field]:
            raise ExperimentError(f"observation has no {field}")
    for field in ("invoke_monotonic_ns", "complete_monotonic_ns", "duration_ns"):
        if type(record.get(field)) is not int or record[field] < 0:
            raise ExperimentError(f"invalid observation {field}")
    if record["complete_monotonic_ns"] - record["invoke_monotonic_ns"] != record["duration_ns"]:
        raise ExperimentError("invalid observation interval")
    status = record.get("status")
    if "status" not in record or (status is not None and (type(status) is not int or not 100 <= status <= 599)):
        raise ExperimentError("invalid observed HTTP status")
    if not isinstance(record.get("body"), str):
        raise ExperimentError("invalid observed HTTP body")
    headers = record.get("headers")
    if not isinstance(headers, dict) or any(not isinstance(key, str) or key != key.lower()
            or not isinstance(value, str) for key, value in headers.items()):
        raise ExperimentError("invalid observed HTTP headers")
    if "transport_error" not in record or (record["transport_error"] is not None
            and not isinstance(record["transport_error"], str)):
        raise ExperimentError("invalid observed transport result")


def validate_write(record):
    validate_observation(record)
    for field in ("key", "value", "response"):
        if not isinstance(record.get(field), str):
            raise ExperimentError(f"invalid ledger {field}")
    if record["response"] != record["body"]:
        raise ExperimentError("ledger response differs from observed body")
    outcome, index = "outcome_unknown", None
    if record["transport_error"] is None:
        try:
            body = strict_json(record["body"])
        except (ValueError, RecursionError):
            body = None
        if isinstance(body, dict):
            candidate = body.get("index")
            if record["status"] == 200 and body.get("ok") is True and type(candidate) is int and 0 < candidate <= U64_MAX:
                outcome, index = "acknowledged", candidate
            elif record["status"] == 503 and body.get("ok") is not True and body.get("error") in (
                    *DEFINITE_REJECTIONS, "outcome_unknown"):
                outcome = body["error"]
    if record.get("outcome") != outcome or "index" not in record or record["index"] != index:
        raise ExperimentError("ledger outcome contradicts the observed HTTP response")
    if index is not None and type(record["index"]) is not int:
        raise ExperimentError("ACK has no valid applied index")


def write_evidence(path, text):
    with path.open("w", encoding="utf-8") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())


def qualified_leader(statuses, eligible=NODES, minimum_term=0):
    """Topology is supplied by the verified fault, never inferred from role alone."""
    if (len(set(eligible)) < 2 or len(set(eligible)) != len(eligible)
            or any(node not in NODES or not isinstance(statuses.get(node), dict) for node in eligible)):
        return None
    leaders = [node for node in eligible if statuses[node].get("role") == "leader"]
    if len(leaders) != 1:
        return None
    leader = leaders[0]
    term = statuses[leader].get("term")
    if type(term) is not int or not max(0, minimum_term) <= term <= U64_MAX:
        return None
    identity = int(leader[4:])
    aligned = 0
    for node in eligible:
        status = statuses[node]
        observed_term = status.get("term")
        if (type(status.get("node_id")) is not int or status["node_id"] != int(node[4:])
                or type(observed_term) is not int or not 0 <= observed_term <= term):
            return None
        if node == leader or (status.get("role") == "follower" and observed_term == term
                and type(status.get("leader_hint")) is int and status["leader_hint"] == identity):
            aligned += 1
    # A third member campaigning at this same term does not erase the observed
    # leader/follower majority. Multiple leaders and newer terms still disqualify.
    if aligned < len(NODES) // 2 + 1:
        return None
    return leader, term


def acknowledged_state(records):
    """Return the last ACK per key, rejecting an ambiguous later overwrite."""
    latest = {}
    uncertain = set()
    identities = set()
    for sequence, record in enumerate(records, 1):
        validate_write(record)
        identity = (record["run_id"], record["cycle_id"], record["op_id"])
        if identity in identities:
            raise ExperimentError("ledger repeats an operation identity")
        identities.add(identity)
        if record.get("seq") != sequence or type(record.get("seq")) is not int:
            raise ExperimentError("ledger sequence is not contiguous")
        key = record.get("key")
        if not isinstance(key, str):
            raise ExperimentError("ledger key is invalid")
        if record.get("outcome") == "acknowledged":
            if type(record.get("index")) is not int or record["index"] <= 0:
                raise ExperimentError("ACK has no valid applied index")
            latest[key] = record
            # An earlier unknown attempt may complete late; remain conservative.
        elif record.get("outcome") not in DEFINITE_REJECTIONS:
            uncertain.add(key)
    ambiguous = set(latest).intersection(uncertain)
    if ambiguous:
        raise Inconclusive("unknown same-key operation makes final value ambiguous: " + ", ".join(sorted(ambiguous)))
    return latest


def assert_read(record, expected, fence, require_leader=False):
    validate_observation(record)
    if record.get("transport_error") is not None:
        raise Inconclusive("read transport failed")
    headers = record.get("headers", {})
    try:
        watermark = headers["x-raft-last-applied"]
        if not watermark.isascii() or not watermark.isdecimal():
            raise ValueError("watermark is not an unsigned integer")
        applied = int(watermark)
        if applied > U64_MAX:
            raise ValueError("watermark exceeds Raft index range")
    except (KeyError, TypeError, ValueError) as error:
        raise Inconclusive("read has no valid applied watermark") from error
    if applied < max(fence, expected["index"]):
        raise Inconclusive("read has not reached the acknowledged fence")
    if require_leader and headers.get("x-raft-role") != "leader":
        raise Inconclusive("reader changed role")
    if record.get("status") != 200 or record.get("body") != expected["value"]:
        raise ExperimentError("acknowledged value missing or different at applied fence")


def owned_network_id(info, project):
    if info.get("Labels", {}).get("com.docker.compose.project") != project:
        raise ExperimentError("network ownership label does not match this experiment")
    identity = info.get("Id")
    if not isinstance(identity, str) or not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise ExperimentError("network inspection did not return a full identity")
    return identity

class Runner:
    def __init__(self, args):
        self.args = args
        self.out = Path(args.out).resolve()
        self.compose = Path(args.compose).resolve()
        self.project = args.project
        self.dc = ["docker", "compose", "-p", self.project, "-f", str(self.compose)]
        node_image = getattr(args, "node_image", None)
        client_image = getattr(args, "client_image", None)
        if bool(node_image) != bool(client_image):
            raise ExperimentError("--node-image and --client-image must be supplied together")
        self.existing_images = node_image is not None
        self.requested_images = {
            "node": node_image if self.existing_images else f"micro-raft:{self.project}",
            "client": client_image if self.existing_images else f"micro-raft-client:{self.project}",
        }
        if any(not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@-]*", value)
               for value in self.requested_images.values()):
            raise ExperimentError("image references must be nonempty Docker names or IDs")
        self.image_ids = {}
        self.env = dict(os.environ, MICRO_RAFT_IMAGE=self.requested_images["node"],
                        MICRO_RAFT_CLIENT_IMAGE=self.requested_images["client"])
        self.containers = {}
        self.observer = None
        self.network = None
        self.owned = False
        self.output_owned = False
        self.active_fault = None
        self.command_no = 0
        self.operation_no = 0
        self.observation_no = 0
        self.poll_deadline = None
        self.fault_started_at = None
        self.cycles = []
        self.started_ns = time.monotonic_ns()
        self.errors = []

    def event(self, kind, **fields):
        with (self.out / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(dict(event=kind, monotonic_ns=time.monotonic_ns(), **fields)) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def command(self, argv, timeout=30, check=True, name=None):
        if self.poll_deadline is not None:
            timeout = min(timeout, self.poll_deadline - time.monotonic())
            if timeout <= 0:
                raise Deadline("poll deadline expired before command")
        self.command_no += 1
        label = name or f"command-{self.command_no:05d}"
        start = time.monotonic_ns()
        interrupted = False
        try:
            process = subprocess.Popen(argv, env=self.env, text=True, encoding="utf-8",
                                       errors="replace", stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, start_new_session=True)
            try:
                stdout, stderr = process.communicate(timeout=timeout)
                code = process.returncode
            except (subprocess.TimeoutExpired, KeyboardInterrupt) as error:
                # The daemon mutation may already have succeeded. Cleanup owns
                # project-labelled resources even if the CLI has been canceled.
                interrupted = isinstance(error, KeyboardInterrupt)
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    stdout, stderr = process.communicate(timeout=5)
                except subprocess.TimeoutExpired as reap_error:
                    stdout, stderr = "", f"CLI process group did not close its pipes: {reap_error}"
                code = 130 if interrupted else 124
        except OSError as error:
            code, stdout, stderr = 127, "", str(error)
        write_evidence(self.out / f"{label}.stdout", stdout)
        write_evidence(self.out / f"{label}.stderr", stderr)
        self.event("command", argv=argv, exit=code, started_ns=start,
                   completed_ns=time.monotonic_ns(), stdout=f"{label}.stdout", stderr=f"{label}.stderr")
        if interrupted:
            raise KeyboardInterrupt()
        if code == 124:
            raise Deadline(f"command {label} exceeded its deadline: {argv!r}")
        if check and code != 0:
            raise ExperimentError(f"command {label} exited {code}: {argv!r}")
        return code, stdout

    def inventory(self, resource):
        if resource == "container":
            argv = ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={self.project}"]
        else:
            argv = ["docker", resource, "ls", "-q", "--filter", f"label=com.docker.compose.project={self.project}"]
        return self.command(argv)[1].split()

    def prepare_images(self):
        if not self.existing_images:
            self.command(self.dc + ["build"], timeout=900, name="build")
        selections = {}
        for role, requested in self.requested_images.items():
            inspected = strict_json(self.command(["docker", "image", "inspect", requested],
                                                  name=f"{role}-image")[1])
            if (not isinstance(inspected, list) or len(inspected) != 1
                    or not isinstance(inspected[0], dict)):
                raise ExperimentError(f"invalid {role} image inspection")
            identity = inspected[0].get("Id")
            if not isinstance(identity, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", identity):
                raise ExperimentError(f"{role} image has no immutable ID")
            self.image_ids[role] = identity
            selections[role] = dict(requested=requested, id=identity,
                                    repo_digests=inspected[0].get("RepoDigests") or [])
        # Pin IDs before resource creation; a mutable tag changing after inspection
        # cannot silently select a different binary. Neither up nor run may pull.
        self.env["MICRO_RAFT_IMAGE"] = self.image_ids["node"]
        self.env["MICRO_RAFT_CLIENT_IMAGE"] = self.image_ids["client"]
        write_evidence(self.out / "image-selection.json", json.dumps(dict(
            mode="existing" if self.existing_images else "build", images=selections), indent=2) + "\n")

    def require_image(self, info, role):
        if info.get("Image") != self.image_ids.get(role) or role not in self.image_ids:
            raise ExperimentError(f"{role} container image differs from the inspected immutable ID")

    def setup(self):
        if self.out.exists() and any(self.out.iterdir()):
            raise ExperimentError("output directory must be new or empty")
        self.out.mkdir(parents=True, exist_ok=True)
        self.output_owned = True
        for resource in ("container", "network", "volume"):
            if self.inventory(resource):
                raise ExperimentError(f"project already has {resource} resources; choose a fresh namespace")
        self.command(["docker", "version", "--format", "{{json .}}"], name="docker-version")
        self.command(["docker", "compose", "version"], name="compose-version")
        self.prepare_images()
        self.command(self.dc + ["config"], name="compose-config")
        manifest = dict(schema_version=1, project=self.project, compose=str(self.compose),
                        compose_sha256=hashlib.sha256(self.compose.read_bytes()).hexdigest(),
                        clock="runner monotonic timestamps only compare with runner timestamps; HTTP intervals carry their own clock_domain",
                        platform=platform.platform(), python=sys.version, poll_seconds=POLL_SECONDS,
                        timeout_seconds=self.args.timeout_seconds, mode=self.args.mode,
                        fault=self.args.fault, runs=self.args.runs,
                        read_mode="local, topology-qualified and bracketed after a successful majority write",
                        image_mode="existing" if self.existing_images else "build",
                        requested_images=self.requested_images,
                        images={key: self.env[key] for key in ("MICRO_RAFT_IMAGE", "MICRO_RAFT_CLIENT_IMAGE")})
        write_evidence(self.out / "environment.json", json.dumps(manifest, indent=2) + "\n")
        # Ownership begins before mutation: even a failed/timed-out up can have
        # created resources that must be inspected and cleaned up.
        self.owned = True
        self.command(self.dc + ["up", "-d", "--no-build", "--pull", "never", *NODES], timeout=120, name="up")
        for node in NODES:
            ids = self.command(self.dc + ["ps", "-aq", node])[1].split()
            if len(ids) != 1:
                raise ExperimentError(f"expected exactly one container for {node}")
            self.containers[node] = ids[0]
            self.inspect(node)
        networks = self.inventory("network")
        if len(networks) != 1:
            raise ExperimentError("experiment must have exactly one private network")
        self.network = networks[0]
        network_info = strict_json(self.command(["docker", "network", "inspect", self.network],
                                                name="network-before")[1])[0]
        # `network ls -q` abbreviates IDs; container inspection uses the full ID.
        # Canonicalize once before any topology or healing comparison.
        self.network = owned_network_id(network_info, self.project)
        observer_name = f"{self.project}-observer"
        self.command(self.dc + ["run", "-d", "--no-deps", "--pull", "never", "--name", observer_name,
                               "--entrypoint", "python", "client", "-c",
                               "import time; time.sleep(86400)"], timeout=60, name="observer-start")
        observer_info = strict_json(self.command(["docker", "inspect", observer_name])[1])[0]
        if observer_info.get("Config", {}).get("Labels", {}).get("com.docker.compose.project") != self.project:
            raise ExperimentError("observer ownership label does not match this experiment")
        # Never retain the predictable name before creation succeeds: a foreign
        # container can own that name, and must never be exec'd into or removed.
        self.observer = observer_info["Id"]
        self.require_image(observer_info, "client")
        self.command(["docker", "inspect", *self.containers.values(), self.observer], name="containers-before")

    def client(self, command, node=None, **fields):
        argv = ["docker", "exec", self.observer, "python", "/app/client.py", command]
        if node is not None:
            argv += ["--node", node]
        if command != "ledger":
            self.observation_no += 1
            fields.setdefault("run_id", self.project)
            fields.setdefault("cycle_id", "observation")
            fields.setdefault("op_id", f"{self.project}-observation-{self.observation_no}")
            argv += ["--timeout-seconds", "2"]
        for key, value in fields.items():
            argv += ["--" + key.replace("_", "-"), str(value)]
        code, output = self.command(argv, timeout=10, check=False)
        if code not in (0, 1):
            raise ExperimentError(f"client infrastructure exited {code}")
        try:
            records = [strict_json(line) for line in output.splitlines() if line.strip()]
        except ValueError as error:
            raise ExperimentError("client emitted malformed JSON") from error
        if command == "ledger":
            if code != 0:
                raise ExperimentError("durable ledger could not be read")
            acknowledged_state(records)
            if any(record["run_id"] != self.project for record in records):
                raise ExperimentError("ledger contains a different run identity")
            return records
        if len(records) != 1:
            # A failed fsync must reach this path, even if the server sent ACK.
            raise ExperimentError("client produced no single durable result")
        result = records[0]
        validate_observation(result)
        for field, expected in dict(node=node, **{key: fields[key] for key in ("run_id", "cycle_id", "op_id")}).items():
            if result.get(field) != expected:
                raise ExperimentError(f"client observation has a different {field}")
        if command == "put":
            validate_write(result)
            if result["key"] != fields["key"] or result["value"] != fields["value"]:
                raise ExperimentError("client recorded a different write")
            expected_code = 0 if result["outcome"] == "acknowledged" else 1
        else:
            expected_code = int(result["status"] != 200 or result["transport_error"] is not None)
        if code != expected_code:
            raise ExperimentError("client exit status contradicts its observation")
        return result

    def statuses(self, eligible):
        statuses = {}
        for node in eligible:
            result = self.client("status", node)
            if result.get("status") == 200 and result.get("transport_error") is None:
                try:
                    value = strict_json(result["body"])
                    if isinstance(value, dict):
                        statuses[node] = value
                except (KeyError, ValueError, TypeError):
                    pass
        self.event("statuses", eligible=list(eligible), statuses=statuses)
        return statuses

    def wait_for(self, description, predicate):
        previous = self.poll_deadline
        end = time.monotonic() + self.args.timeout_seconds
        if previous is not None:
            end = min(end, previous)
        self.poll_deadline = end
        try:
            while time.monotonic() < end:
                value = predicate()
                # Work inside the predicate consumes the same overall budget.
                if time.monotonic() >= end:
                    break
                if value:
                    return value
                time.sleep(min(POLL_SECONDS, max(0, end - time.monotonic())))
            raise Deadline(description)
        finally:
            self.poll_deadline = previous

    def leader(self, eligible=NODES, minimum_term=0):
        return self.wait_for("no qualified leader before deadline", lambda: qualified_leader(
            self.statuses(eligible), eligible, minimum_term))

    def put(self, node, cycle, key):
        self.operation_no += 1
        operation = f"{self.project}-{self.operation_no}"
        result = self.client("put", node, key=key, value=operation, run_id=self.project,
                             cycle_id=cycle, op_id=operation)
        validate_write(result)
        if result.get("outcome") != "acknowledged":
            raise Inconclusive(f"write {operation} was not acknowledged: {result.get('outcome')}")
        records = self.client("ledger")
        if not records or records[-1] != result:
            raise ExperimentError("durable ledger does not contain the observed ACK")
        return result, records

    def inspect(self, node):
        output = self.command(["docker", "inspect", self.containers[node]])[1]
        state = strict_json(output)[0]
        if state.get("Config", {}).get("Labels", {}).get("com.docker.compose.project") != self.project:
            raise ExperimentError(f"{node} ownership label does not match this experiment")
        self.require_image(state, "node")
        return state

    def injection_confirmed(self, fault, node):
        state = self.inspect(node)
        if fault in ("kill", "restart"):
            stopped = not state["State"]["Running"] and state["State"]["Pid"] == 0
            if stopped and fault == "restart" and state["State"].get("ExitCode") != 0:
                raise ExperimentError("held graceful restart did not stop cleanly")
            return stopped
        if fault == "pause":
            return state["State"]["Running"] and state["State"]["Paused"]
        attached = state["NetworkSettings"]["Networks"]
        return all(network["NetworkID"] != self.network for network in attached.values())

    def inject(self, fault, node):
        before = self.inspect(node)
        self.fault_started_at = before["State"].get("StartedAt")
        if fault in ("kill", "restart") and not self.fault_started_at:
            raise ExperimentError("old node has no process start timestamp")
        self.active_fault = (fault, node)
        issue_ns = time.monotonic_ns()
        if fault == "kill":
            self.command(["docker", "kill", "--signal", "KILL", self.containers[node]])
        elif fault == "restart":
            # Hold the stopped process until a different leader proves service.
            # Exit-code validation rejects Docker's forced-kill fallback.
            self.command(["docker", "stop", "--timeout", "10", self.containers[node]], timeout=20)
        elif fault == "pause":
            self.command(["docker", "pause", self.containers[node]])
        else:
            self.command(["docker", "network", "disconnect", self.network, self.containers[node]])
        self.wait_for("fault command had no confirmed effect", lambda: self.injection_confirmed(fault, node))
        confirmed_ns = time.monotonic_ns()
        self.event("injection", fault=fault, node=node, issued_ns=issue_ns, confirmed_ns=confirmed_ns)
        return issue_ns, confirmed_ns

    def heal(self):
        if self.active_fault is None:
            return
        fault, node = self.active_fault
        state = self.inspect(node)
        if state["State"].get("Paused"):
            self.command(["docker", "unpause", self.containers[node]])
        networks = state["NetworkSettings"]["Networks"]
        if fault == "partition" and all(value["NetworkID"] != self.network for value in networks.values()):
            # Restore the service DNS identity used by the peer configuration.
            self.command(["docker", "network", "connect", "--alias", node,
                          self.network, self.containers[node]])
        if not state["State"]["Running"]:
            self.command(["docker", "start", self.containers[node]])
        def restored():
            current = self.inspect(node)
            return (current["State"]["Running"] and not current["State"]["Paused"]
                    and (fault not in ("kill", "restart") or (
                        current["State"].get("StartedAt")
                        and current["State"]["StartedAt"] != self.fault_started_at))
                    and any(value["NetworkID"] == self.network and node in (value.get("Aliases") or [])
                            for value in current["NetworkSettings"]["Networks"].values()))
        self.wait_for("old node did not resume in its original network", restored)
        self.event("healed", fault=fault, node=node)
        self.active_fault = None
        self.fault_started_at = None

    def read_back(self, eligible, leader, term, records, fence):
        expected = acknowledged_state(records)
        before = qualified_leader(self.statuses(eligible), eligible)
        if before != (leader, term):
            raise Inconclusive("leadership changed before read-back")
        reads = []
        mismatch = None
        for key, ack in expected.items():
            response = self.client("get", leader, key=key)
            reads.append(dict(key=key, expected=ack, observed=response))
            try:
                assert_read(response, ack, fence, require_leader=True)
            except ExperimentError as error:
                mismatch = error
                break
        after = qualified_leader(self.statuses(eligible), eligible)
        self.event("read_back", before=before, after=after, fence=fence, reads=reads)
        if before != after:
            raise Inconclusive("leader qualification changed during read-back")
        if mismatch:
            raise mismatch

    def catch_up(self, old_node, records, fence):
        expected = acknowledged_state(records)
        def caught_up():
            statuses = self.statuses(NODES)
            current = qualified_leader(statuses)
            if not current or any(type(statuses[node].get("last_applied")) is not int
                    or not fence <= statuses[node]["last_applied"] <= U64_MAX for node in NODES):
                return None
            reads, mismatch = [], None
            for key, ack in expected.items():
                response = self.client("get", old_node, key=key)
                reads.append(dict(key=key, expected=ack, observed=response))
                try:
                    assert_read(response, ack, fence)
                except ExperimentError as error:
                    mismatch = error
                    break
            after = qualified_leader(self.statuses(NODES))
            self.event("catch_up_read", node=old_node, fence=fence, before=current, after=after, reads=reads)
            if current != after or isinstance(mismatch, Inconclusive):
                return None
            if mismatch:
                # A stable, sufficiently applied wrong value is a failure, not
                # a transient condition to poll until the evidence disappears.
                raise mismatch
            return current
        result = self.wait_for("reopened node did not catch up", caught_up)
        self.event("catch_up", node=old_node, fence=fence, leader=result)
        return result

    def cycle(self, fault, number):
        cycle_id = f"{fault}-{number}"
        record = dict(cycle_id=cycle_id, fault=fault, outcome="running", started_ns=time.monotonic_ns())
        self.cycles.append(record)
        try:
            previous, previous_term = self.leader()
            ack, records = self.put(previous, cycle_id, "protected")
            record["old_leader"] = previous
            record["old_term"] = previous_term
            record["pre_fault_index"] = ack["index"]
            record["injection_issued_ns"], record["injection_confirmed_ns"] = self.inject(fault, previous)
            eligible = tuple(node for node in NODES if node != previous)
            leader, term = self.leader(eligible, previous_term + 1)
            record["leader_detected_ns"] = time.monotonic_ns()
            record["new_leader"], record["new_term"] = leader, term
            # Keep the failed node absent until a different leader proves service.
            if not self.injection_confirmed(fault, previous):
                raise ExperimentError("fault did not remain active through replacement")
            probe, records = self.put(leader, cycle_id, "authority-probe")
            record["service_recovered_ns"] = time.monotonic_ns()
            self.read_back(eligible, leader, term, records, probe["index"])
            self.heal()
            leader, term = self.catch_up(previous, records, probe["index"])
            post, records = self.put(leader, cycle_id, "after-heal")
            self.catch_up(previous, records, post["index"])
            self.read_back(NODES, leader, term, records, post["index"])
            record["outcome"] = "pass"
        except Inconclusive as error:
            record.update(outcome="inconclusive", error=str(error))
            raise
        except Deadline as error:
            record.update(outcome="timed_out", error=str(error))
            raise
        except (Exception, KeyboardInterrupt) as error:
            record.update(outcome="failed", error=f"{type(error).__name__}: {error}")
            raise
        finally:
            record["completed_ns"] = time.monotonic_ns()
            self.event("cycle_result", **record)

    def finish(self):
        if not self.owned:
            return
        # Every cleanup action is independent, bounded and recorded. One failure
        # must not suppress collection or removal of the remaining owned resources.
        def attempt(label, action):
            try:
                action()
            except (Exception, KeyboardInterrupt) as error:
                self.errors.append(f"{label}: {error}")
        attempt("heal", self.heal)
        captures = [
            ("compose-final", self.dc + ["config"]),
            ("ps-final", self.dc + ["ps", "-a", "--format", "json"]),
            ("nodes-log", self.dc + ["logs", "--no-color", "--timestamps"]),
            ("images-final", self.dc + ["images", "--format", "json"]),
            ("volumes-before-cleanup", ["docker", "volume", "ls", "--filter", f"label=com.docker.compose.project={self.project}"]),
        ]
        if self.observer:
            captures.append(("acks", ["docker", "exec", self.observer, "python", "/app/client.py", "ledger"]))
        for label, argv in captures:
            attempt(label, lambda label=label, argv=argv: self.command(argv, timeout=30, name=label))
        if self.observer:
            attempt("observer removal", lambda: self.command(["docker", "rm", "-f", self.observer]))
        attempt("compose down", lambda: self.command(self.dc + ["down", "--remove-orphans", "--timeout", "15"], timeout=60, name="down"))
        inventory = {}
        for resource in ("container", "network", "volume"):
            def collect(resource=resource):
                inventory[resource] = self.inventory(resource)
                if resource != "volume" and inventory[resource]:
                    raise ExperimentError(f"owned {resource} resources remain")
            attempt(f"final {resource} inventory", collect)
        write_evidence(self.out / "cleanup.json", json.dumps(dict(errors=self.errors, retained=inventory,
            retained_volumes="data preserved intentionally; remove only these named experiment volumes when no longer needed"), indent=2) + "\n")

    def run(self):
        failure = None
        faults = ["kill"] if self.args.mode == "smoke" else list(FAULTS) if self.args.fault == "all" else [self.args.fault]
        repetitions = 1 if self.args.mode == "smoke" else self.args.runs
        try:
            self.setup()
            for fault in faults:
                for number in range(1, repetitions + 1):
                    self.cycle(fault, number)
        except (Exception, KeyboardInterrupt) as error:
            failure = f"{type(error).__name__}: {error}"
        finally:
            if self.output_owned:
                try:
                    self.finish()
                except (Exception, KeyboardInterrupt) as error:
                    self.errors.append(f"cleanup/evidence: {type(error).__name__}: {error}")
        counts = {name: sum(record.get("outcome") == name for record in self.cycles)
                  for name in ("pass", "failed", "timed_out", "inconclusive", "running")}
        planned = [(fault, f"{fault}-{number}") for fault in faults for number in range(1, repetitions + 1)]
        complete = ([(record.get("fault"), record.get("cycle_id")) for record in self.cycles] == planned
                    and counts["pass"] == len(planned))
        if not complete and failure is None:
            failure = "not every planned cycle completed successfully"
        result = dict(schema_version=1, verdict="PASS" if failure is None and not self.errors else "FAIL",
                      error=failure, cleanup_errors=self.errors, planned=len(faults) * repetitions,
                      attempted=len(self.cycles), not_attempted=len(faults) * repetitions - len(self.cycles),
                      counts=counts, cycles=self.cycles, started_ns=self.started_ns, completed_ns=time.monotonic_ns())
        if self.output_owned:
            try:
                # Only publish the verdict after its file sync succeeds. A
                # failed fsync leaves an explicitly pending artifact, never PASS
                # under the result.json name.
                pending = self.out / "result.json.pending"
                write_evidence(pending, json.dumps(result, indent=2) + "\n")
                os.replace(pending, self.out / "result.json")
            except (Exception, KeyboardInterrupt) as error:
                result["verdict"] = "FAIL"
                result["cleanup_errors"].append(f"result evidence: {type(error).__name__}: {error}")
        print(json.dumps(result, indent=2))
        return 0 if result["verdict"] == "PASS" else 1


def positive(value):
    number = int(value)
    if not 0 < number <= 1000000:
        raise argparse.ArgumentTypeError("must be positive and at most 1000000")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("smoke", "faults"))
    parser.add_argument("--compose", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--timeout-seconds", type=positive, default=30)
    parser.add_argument("--fault", choices=(*FAULTS, "all"), default="all")
    parser.add_argument("--runs", type=positive, default=1)
    parser.add_argument("--node-image", help="existing local node image; requires --client-image and skips build")
    parser.add_argument("--client-image", help="existing local client image; requires --node-image and skips build")
    args = parser.parse_args()
    if bool(args.node_image) != bool(args.client_image):
        parser.error("--node-image and --client-image must be supplied together")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,48}", args.project):
        parser.error("invalid project name (maximum 49 characters)")
    if not Path(args.compose).is_file():
        parser.error("compose file does not exist")
    if os.name != 'posix':
        parser.error('run this Docker harness from Linux or WSL')
    signal.signal(signal.SIGTERM, lambda _signal, _frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    return Runner(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
