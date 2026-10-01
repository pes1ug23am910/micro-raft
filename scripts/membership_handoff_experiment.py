"""Five-process membership/handoff composition with exact retained observations.

The fixed handoff helper and bounded HTTP observer are explicit pinned inputs.
These cases use real controller/data actors, not the standalone KV endpoint.
"""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import traceback

CASES = ("replacement_and_handoff", "unavailable_target_ticket")
STAGES = {
    CASES[0]: ("seed", "abort_interlock", "held_ticket_restart", "source_membership",
               "source_new_only", "outbound_handoff", "destination_membership",
               "controller_membership", "new_only_reopen", "return_handoff"),
    CASES[1]: ("controller_membership", "ticket_acquired", "target_unavailable",
               "ticket_reopened", "target_restored"),
}
PLANNED_CLIENT_OPERATIONS = {CASES[0]: 20, CASES[1]: 0}


def positive(value):
    return type(value) is int and 0 < value < 2**64


def same_json(left, right):
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(right, sort_keys=True, allow_nan=False)


def validate_value(observed, group, epoch, key, expected):
    if (observed.get("kind") != "value" or type(observed.get("group")) is not int
            or observed["group"] != group or type(observed.get("shard")) is not int
            or observed["shard"] != 0 or type(observed.get("epoch")) is not int
            or observed["epoch"] != epoch or observed.get("key") != key
            or "value" not in observed or observed["value"] != expected):
        raise AssertionError("checked value is missing, mismatched or from another route")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_ticket(ticket, request_id, target, operation, completed=False):
    if (not isinstance(ticket, dict) or set(ticket) != {"request_id", "target", "operation", "origin", "previous_generation", "completion"}
            or ticket.get("request_id") != request_id
            or type(ticket.get("target")) is not int or ticket["target"] != target
            or not same_json(ticket.get("operation"), operation)):
        raise AssertionError("maintenance ticket identity mismatch")
    origin = ticket.get("origin", {})
    if set(origin) != {"group", "index"} or type(origin.get("group")) is not int or origin["group"] != 0 or not positive(origin.get("index")):
        raise AssertionError("maintenance ticket lacks controller log origin")
    previous = ticket["previous_generation"]
    if previous is not None and (not isinstance(previous, dict) or set(previous) != {"group", "index"}
            or type(previous["group"]) is not int or previous["group"] != 0
            or not positive(previous["index"]) or previous["index"] >= origin["index"]):
        raise AssertionError("maintenance generation is malformed or not earlier")
    completion = ticket.get("completion")
    if completed:
        if not isinstance(completion, dict) or set(completion) != {"record", "observed_applied", "released"}:
            raise AssertionError("maintenance ticket is not complete")
        record = completion.get("record", {})
        applied, released = completion.get("observed_applied", {}), completion.get("released", {})
        if (set(record) != {"operation", "first_index", "first_term", "joint", "final_index", "final_term"}
                or not same_json(record.get("operation"), operation) or not positive(record.get("first_index"))
                or not positive(record.get("final_index")) or record["final_index"] < record["first_index"]
                or not positive(record.get("first_term")) or not positive(record.get("final_term"))
                or record["final_term"] < record["first_term"]
                or type(record.get("joint")) is not bool
                or type(applied.get("group")) is not int or applied["group"] != target
                or not positive(applied.get("index")) or applied["index"] < record["final_index"]
                or type(released.get("group")) is not int or released["group"] != 0
                or not positive(released.get("index")) or released["index"] <= origin["index"]):
            raise AssertionError("completion lacks exact applied membership boundary")
        if ((record["joint"] and record["final_index"] <= record["first_index"])
                or (not record["joint"] and (record["final_index"] != record["first_index"]
                    or record["final_term"] != record["first_term"]))
                or (operation["kind"] == "set_voters" and not record["joint"])
                or (operation["kind"] == "add_learner" and record["joint"])):
            raise AssertionError("membership result has an impossible phase boundary")
        if target == 0 and (record["first_index"] <= origin["index"] or applied["index"] >= released["index"]):
            raise AssertionError("controller reconfiguration violates same-log causality")
    return ticket


class CompositionMixin:
    def __init__(self, *args, support, **kwargs):
        self.support = support
        super().__init__(*args, **kwargs)
        # Start with the helper's three-host topology, then add routable passive
        # hosts. The immutable identity must remain identical.
        initial_fingerprint = self.cluster
        support.save(self.root / "initial-three-host-topology.json", self.topology)
        for held in self.reservations:
            held.close()
        self.reservations.clear()
        ports = []
        for _ in range(20):
            held = socket.socket()
            held.bind(("127.0.0.1", 0))
            held.listen()
            self.reservations.append(held)
            ports.append(held.getsockname()[1])
        self.topology["nodes"] = [dict(id=n, http=f"127.0.0.1:{ports[(n-1)*4]}",
            raft={str(g): f"127.0.0.1:{ports[(n-1)*4+g+1]}" for g in (0, 1, 2)})
            for n in range(1, 6)]
        support.save(self.config, self.topology)
        result = subprocess.run(self.command(1) + ["--check-config"], capture_output=True,
                                timeout=20, check=False)
        (self.root / "five-host-check-config.stdout").write_bytes(result.stdout)
        (self.root / "five-host-check-config.stderr").write_bytes(result.stderr)
        if result.returncode or self.client.strict_json(result.stdout.decode())["cluster"] != initial_fingerprint:
            raise AssertionError("additional routes changed immutable cluster identity")
        self.completed_stages, self.tickets, self.replacements = [], {}, {}

    def stage(self, name):
        expected = STAGES[self.name]
        if len(self.completed_stages) >= len(expected) or expected[len(self.completed_stages)] != name:
            raise AssertionError("composition phases were skipped or reordered")
        self.completed_stages.append(name)
        self.event("stage-completed", name=name)

    def checked(self, group, query):
        def attempt():
            response = self.query(group, query)
            if response.get("outcome") in ("unknown", "unavailable", "not_leader"):
                return None
            if (response.get("outcome") != "checked" or type(response.get("group")) is not int
                    or response["group"] != group or type(response.get("node")) is not int
                    or response["node"] not in range(1, 6) or not positive(response.get("term"))
                    or not positive(response.get("context")) or not positive(response.get("index"))
                    or not positive(response.get("applied_index"))
                    or response["applied_index"] < response["index"]):
                raise AssertionError(f"malformed or rejected checked observation: {response}")
            return response
        # Only read observations retry through an election gap. Malformed proofs
        # and logical rejections fail immediately; mutations keep their outcome.
        return self.until(attempt, f"checked group{group}/{query['query']}", 25)

    def observed(self, group, query):
        return self.checked(group, query)["observed"]

    def gate(self):
        observed = self.observed(0, {"query": "maintenance_gate"})
        if observed.get("kind") != "maintenance_gate" or type(observed.get("handoff_pending")) is not bool:
            raise AssertionError("invalid maintenance gate view")
        return observed

    def ticket(self, request_id):
        observed = self.observed(0, {"query": "maintenance", "request_id": request_id})
        if observed.get("kind") != "maintenance":
            raise AssertionError("wrong maintenance query result")
        return observed["ticket"]

    def refuse(self, response, reason="busy", group=0):
        direct = response == dict(outcome="rejected", error=reason)
        committed = (response.get("outcome") == "applied" and type(response.get("group")) is int
            and response["group"] == group and positive(response.get("index"))
            and response.get("reply") == dict(kind="rejected", reply=reason))
        if not (direct or committed):
            raise AssertionError(f"expected exact {reason} refusal: {response}")
        self.logical_rejections += 1

    def applied(self, response, group):
        if (response.get("outcome") != "applied" or type(response.get("group")) is not int
                or response["group"] != group or not positive(response.get("index"))
                or response.get("reply", {}).get("kind") == "rejected"):
            raise AssertionError(f"operation not applied successfully: {response}")
        return response

    def start_cluster(self):
        for held in self.reservations:
            held.close()
        self.reservations.clear()
        for node in range(1, 6):
            self.start(node)
        self.ready()
        for node in (4, 5):
            status = self.request("/status", node=node)
            groups = status["groups"]
            if (type(status.get("node")) is not int or status["node"] != node
                    or len(groups) != 3
                    or any(type(group.get("group")) is not int for group in groups)
                    or {group["group"] for group in groups} != {0, 1, 2}
                    or any(type(group.get("node")) is not int or group["node"] != node
                        or group["role"] != "follower" or type(group.get("commit_index")) is not int
                        or group["commit_index"] != 0 for group in groups)):
                raise AssertionError("reachable unadmitted process acquired authority")
            for group in (0, 1, 2):
                response = self.request("/v1/query", dict(group=group, query=dict(query="configuration")), node=node)
                if response != dict(outcome="unavailable", reason="not_leader"):
                    raise AssertionError(f"passive direct read did not refuse leadership: {response}")
            response = self.request("/v1/execute", dict(group=0, action=dict(action="bootstrap")), node=node)
            if response.get("outcome") != "not_leader":
                raise AssertionError(f"passive direct proposal did not refuse leadership: {response}")
        self.event("passive-learners-confirmed", nodes=[4, 5])

    def add_operation(self, group, node):
        route = self.topology["nodes"][node-1]
        return dict(kind="add_learner", id=node,
                    endpoints=dict(raft=route["raft"][str(group)], http=route["http"]))

    def begin_maintenance(self, request_id, target, operation):
        self.applied(self.execute(0, dict(action="begin_maintenance", request_id=request_id,
            target=target, operation=operation)), 0)
        ticket = validate_ticket(self.ticket(request_id), request_id, target, operation)
        self.tickets[request_id] = ticket
        return ticket

    def complete_maintenance(self, request_id, target, operation):
        def advance():
            ticket = validate_ticket(self.ticket(request_id), request_id, target, operation)
            if ticket["completion"] is not None:
                return validate_ticket(ticket, request_id, target, operation, True)
            self.execute(0, dict(action="reconcile_maintenance", request_id=request_id))
            return None
        ticket = self.until(advance, f"maintenance {request_id} completes", 70)
        record_query = self.observed(target, dict(query="membership",
            request_id=f"l7-maintenance-{ticket['origin']['index']}"))
        if (record_query.get("kind") != "membership"
                or not same_json(record_query["record"], ticket["completion"]["record"])):
            raise AssertionError("controller completion differs from actual checked target record")
        gate = self.gate()
        if gate["active"] is not None or not same_json(gate["generation"], ticket["origin"]):
            raise AssertionError("completed ticket did not release exact controller generation")
        self.tickets[request_id] = ticket
        self.support.save(self.root / "maintenance-results.json", self.tickets)
        return ticket

    def maintain(self, request_id, target, operation):
        self.begin_maintenance(request_id, target, operation)
        return self.complete_maintenance(request_id, target, operation)

    def replace_group(self, group, first_added=False, fixed_survivor=None):
        for node in (4, 5):
            if node == 4 and first_added:
                continue
            self.maintain(f"g{group}-add-{node}", group, self.add_operation(group, node))
        # Require actual learner application catch-up before asking for promotion.
        state = self.checked(group, dict(query="membership", request_id=None))
        fence = state["applied_index"]
        for node in (4, 5):
            self.caught_up(node, {group: fence})
        state = self.checked(group, dict(query="membership", request_id=None))
        prior_leader = state["node"]
        survivor = fixed_survivor or next(n for n in (3, 2, 1) if n != prior_leader)
        voters = [survivor, 4, 5]
        result = self.maintain(f"g{group}-replace", group, dict(kind="set_voters", voters=voters))
        observed = self.observed(group, dict(query="membership", request_id=None))
        if not same_json(observed["effective"]["voters"], dict(phase="stable", voters=voters)):
            raise AssertionError("group did not adopt final voter set")
        self.replacements[str(group)] = dict(prior_leader=prior_leader, voters=voters,
            removed_prior_leader=prior_leader not in voters, ticket=result)
        self.support.save(self.root / "group-replacements.json", self.replacements)
        return result

    def begin_move(self, request_id, source, destination, epoch):
        response = self.execute(0, dict(action="begin_move", request_id=request_id,
            shard=0, epoch=epoch, destination=destination))
        self.applied(response, 0)
        return self.move_views(request_id, source, destination)

    def move_views(self, request_id, source=1, destination=2):
        return (self.checked(0, dict(query="transfer", request_id=request_id)),
                self.checked(source, dict(query="shard", shard=0)),
                self.checked(destination, dict(query="shard", shard=0)))

    def drive_move(self, request_id, target="complete", source=1, destination=2):
        def advance():
            views = self.move_views(request_id, source, destination)
            phase = views[0]["observed"]["transfer"]["phase"]
            if phase["phase"] == target:
                return views
            if phase["phase"] == "aborted":
                raise AssertionError("ordinary composition handoff aborted unexpectedly")
            self.request("/v1/reconcile", dict(request_id=request_id))
            return None
        return self.until(advance, f"move {request_id} reaches {target}", 90)

    def seed_data(self):
        self.keys = [f"composition-{n}" for n in range(2000)
            if int.from_bytes(hashlib.sha256(f"composition-{n}".encode()).digest()[:8], "big") % 4 == 0][:4]
        self.value = "membership-state-" + "x" * 24000
        registration = self.receipt(self.execute(1, dict(action="register", shard=0, epoch=1, nonce="session")))
        self.session = registration["session"]
        self.original = self.receipt(self.execute(1, self.mutation(self.keys[0], self.value, 1)))
        self.receipt(self.execute(1, self.mutation(self.keys[1], "deleted", 1)))
        self.deleted = self.receipt(self.execute(1, self.mutation(self.keys[1], None, 2)))
        closed = self.receipt(self.execute(1, dict(action="register", shard=0, epoch=1, nonce="closed")))
        self.closed_session = closed["session"]
        self.receipt(self.execute(1, dict(action="mutate", shard=0, epoch=1, session=self.closed_session,
            key=self.keys[2], sequence=1, value="closed-canary")))
        self.receipt(self.execute(1, dict(action="close", shard=0, epoch=1, session=self.closed_session)))

    def mutation(self, key, value, sequence, epoch=1):
        return dict(action="mutate", shard=0, epoch=epoch, session=self.session,
                    key=key, sequence=sequence, value=value)

    def verify_data(self, group, epoch, retries=True):
        values = [(self.keys[0], self.value), (self.keys[1], None), (self.keys[2], "closed-canary")]
        if hasattr(self, "new_only_receipt"):
            values.append((self.keys[3], "new-only-source"))
        for key, expected in values:
            observed = self.observed(group, dict(query="read", shard=0, epoch=epoch, key=key))
            validate_value(observed, group, epoch, key, expected)
        if retries:
            self.receipt(self.execute(group, self.mutation(self.keys[0], self.value, 1, epoch)), self.original)
            self.receipt(self.execute(group, self.mutation(self.keys[1], None, 2, epoch)), self.deleted)
            if hasattr(self, "new_only_receipt"):
                self.receipt(self.execute(group, self.mutation(self.keys[3], "new-only-source", 1, epoch)), self.new_only_receipt)
            response = self.execute(group, dict(action="mutate", shard=0, epoch=epoch, session=self.closed_session,
                key=self.keys[2], sequence=1, value="closed-canary"))
            self.support.validate_closed_rejection(response, group)
            self.logical_rejections += 1

    def hold_and_abort(self):
        self.begin_move("aborted-move", 1, 2, 1)
        self.drive_move("aborted-move", "fenced")
        blocked = dict(action="begin_maintenance", request_id="blocked-by-move", target=1,
                       operation=self.add_operation(1, 4))
        self.refuse(self.execute(0, blocked))
        self.applied(self.execute(0, dict(action="advance", request_id="aborted-move", step="abort")), 0)
        if not self.gate()["handoff_pending"]:
            raise AssertionError("abort released coordination before endpoint cleanup")
        self.refuse(self.execute(0, blocked))
        def recover():
            gate = self.gate()
            if not gate["handoff_pending"]:
                views = self.move_views("aborted-move")
                phase = views[0]["observed"]["transfer"]["phase"]
                if (phase["phase"] != "aborted" or phase["source_recovered"] is None
                        or phase["destination_recovered"] is None):
                    raise AssertionError("abort gate released without both exact receipts")
                return views
            self.request("/v1/reconcile", dict(request_id="aborted-move"))
            return None
        self.until(recover, "both abort endpoint cleanups", 60)
        self.verify_data(1, 1, retries=False)
        if self.ticket("blocked-by-move") is not None:
            raise AssertionError("refused maintenance was retained as admitted")

    def replacement_and_handoff(self):
        self.seed_data()
        self.stage("seed")
        self.hold_and_abort()
        self.stage("abort_interlock")
        operation = self.add_operation(1, 4)
        ticket = self.begin_maintenance("g1-add-4", 1, operation)
        blocked_move = dict(action="begin_move", request_id="blocked-by-ticket", shard=0, epoch=1, destination=2)
        self.refuse(self.execute(0, blocked_move))
        authority = self.checked(0, dict(query="maintenance", request_id="g1-add-4"))
        victim = authority["node"]
        self.event("held-ticket-authority", authority=authority)
        self.kill(victim, "controller leader with unapplied maintenance ticket")
        self.ready()
        if not same_json(self.ticket("g1-add-4"), ticket):
            raise AssertionError("controller leader failure changed held ticket")
        self.refuse(self.execute(0, blocked_move))
        self.start(victim)
        self.ready()
        completed = self.complete_maintenance("g1-add-4", 1, operation)
        retried = self.begin_maintenance("g1-add-4", 1, operation)
        if not same_json(retried, completed):
            raise AssertionError("exact caller retry changed completed maintenance result")
        self.stage("held_ticket_restart")
        self.replace_group(1, first_added=True)
        self.stage("source_membership")
        for node in (1, 2, 3):
            self.kill(node, "source final new-only quorum")
        self.verify_data(1, 1, retries=False)
        self.new_only_receipt = self.receipt(self.execute(1, self.mutation(self.keys[3], "new-only-source", 1)))
        for node in (1, 2, 3):
            self.start(node)
        self.ready()
        self.stage("source_new_only")
        self.begin_move("outbound", 1, 2, 1)
        views = self.drive_move("outbound")
        transfer = views[0]["observed"]["transfer"]
        image = transfer["phase"]["activation"]["ownership"]["installation"]["image"]
        if image["bytes"] <= 32 * 1024:
            raise AssertionError("composition did not transfer multiple chunks")
        self.verify_data(2, 2)
        self.support.save(self.root / "outbound-handoff.json", views)
        self.stage("outbound_handoff")
        self.replace_group(2)
        self.stage("destination_membership")
        self.replace_group(0)
        self.stage("controller_membership")
        for node in (1, 2, 3):
            self.kill(node, "all groups final new-only quorums")
        route = self.observed(0, dict(query="route", shard=0))["route"]
        if route["owner"] != 2 or route["epoch"] != 2:
            raise AssertionError("new-only controller lost route")
        self.verify_data(2, 2, retries=False)
        for node in (4, 5):
            self.kill(node, "abrupt reopen of only surviving replicas")
        for node in (4, 5):
            self.start(node)
        self.ready()
        self.verify_data(2, 2)
        self.stage("new_only_reopen")
        self.begin_move("return", 2, 1, 2)
        returned = self.drive_move("return", source=2, destination=1)
        self.verify_data(1, 3)
        self.support.save(self.root / "return-handoff.json", returned)
        self.stage("return_handoff")
        return dict(multi_chunk_bytes=image["bytes"], new_only_nodes=[4, 5],
                    original_receipt=self.original, deletion_receipt=self.deleted,
                    new_only_receipt=self.new_only_receipt)

    def unavailable_target_ticket(self):
        self.replace_group(0, fixed_survivor=3)
        self.stage("controller_membership")
        operation = self.add_operation(1, 4)
        ticket = self.begin_maintenance("target-unavailable", 1, operation)
        self.stage("ticket_acquired")
        for node in (1, 2):
            self.kill(node, "target quorum unavailable while controller keeps quorum")
        result = self.execute(0, dict(action="reconcile_maintenance", request_id="target-unavailable"))
        if result.get("outcome") not in ("unknown", "unavailable", "not_leader", "rejected"):
            raise AssertionError(f"unavailable target unexpectedly completed reconciliation: {result}")
        gate = self.gate()
        if not same_json(gate["active"], ticket) or not same_json(self.ticket("target-unavailable"), ticket):
            raise AssertionError("unknown target outcome released or changed ticket")
        self.refuse(self.execute(0, dict(action="begin_move", request_id="blocked-no-quorum",
            shard=0, epoch=1, destination=2)))
        if self.observed(0, dict(query="transfer", request_id="blocked-no-quorum"))["transfer"] is not None:
            raise AssertionError("held ticket admitted a handoff during target outage")
        self.support.save(self.root / "unavailable-target-observation.json", dict(response=result, gate=gate))
        self.stage("target_unavailable")
        for node in (3, 4, 5):
            self.kill(node, "reopen durable controller ticket without target quorum")
        for node in (3, 4, 5):
            self.start(node)
        self.until(lambda: self.query(0, dict(query="maintenance_gate")).get("outcome") == "checked",
                   "controller quorum reopened", 30)
        if not same_json(self.ticket("target-unavailable"), ticket) or not same_json(self.gate()["active"], ticket):
            raise AssertionError("controller restart forgot incomplete maintenance")
        self.stage("ticket_reopened")
        self.start(1)
        self.ready()
        completed = self.complete_maintenance("target-unavailable", 1, operation)
        if completed["origin"] != ticket["origin"]:
            raise AssertionError("recovery replaced original ticket identity")
        self.start(2)
        self.ready()
        self.stage("target_restored")
        return dict(target_outage_result=result, target_log_admission="not established by this outage case",
                    completed_ticket=completed)

    def run(self):
        self.start_cluster()
        result = getattr(self, self.name)()
        if tuple(self.completed_stages) != STAGES[self.name]:
            raise AssertionError("composition stage plan incomplete")
        if self.offered_client_operations != PLANNED_CLIENT_OPERATIONS[self.name]:
            raise AssertionError(f"client plan mismatch: {self.offered_client_operations}")
        result.update(result="PASS", completed_stages=self.completed_stages)
        return result

    def failure_diagnostics(self):
        """Bounded read-only evidence after failure; never changes the verdict."""
        deadline = time.monotonic() + 8
        rows, leaders = [], []
        nodes = list(self.live())
        tasks = [(node, "/status", None) for node in nodes]
        while tasks and time.monotonic() < deadline:
            node, path, body = tasks.pop(0)
            value = None if body is None else json.dumps(dict(cluster=self.cluster, body=body))
            record = self.client.request_record(f"node{node}", path,
                timeout=min(1, deadline-time.monotonic()), value=value,
                endpoint="http://"+self.topology["nodes"][node-1]["http"],
                run_id=self.root.parent.name, cycle_id=self.name,
                op_id=f"failure-diagnostic-{len(rows)+1}",
                method="GET" if body is None else "POST",
                content_type=None if body is None else "application/json")
            rows.append(dict(request=dict(node=node, path=path, body=body), observation=record))
            if path == "/status" and record["transport_error"] is None and record["status"] == 200:
                try:
                    status = self.client.strict_json(record["body"])
                    for group in status.get("groups", []):
                        if group.get("role") == "leader" and type(group.get("group")) is int:
                            leaders.append((node, "/v1/query", dict(group=group["group"],
                                query=dict(query="membership", request_id=None))))
                except (ValueError, KeyError, TypeError):
                    pass
            if not tasks and leaders:
                tasks, leaders = leaders, []
        self.support.save(self.root / "failure-diagnostics.json", dict(
            note="Read-only observations after failure; no effect on result or offered client plan",
            observations=rows, unattempted=len(tasks), budget_seconds=8))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "client", "handoff-helper", "source-binding", "out"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    parser.add_argument("--seed", type=int, default=20261002)
    parser.add_argument("--case-seconds", type=float, default=600)
    args = parser.parse_args()
    if not math.isfinite(args.case_seconds) or not 1 <= args.case_seconds <= 1200:
        raise ValueError("case budget outside finite1..1200seconds")
    if len(set(args.cases)) != len(args.cases) or args.out.exists():
        raise ValueError("require unique cases and a fresh output directory")
    binding = json.loads(args.source_binding.read_text(encoding="utf-8-sig"))
    if (binding.get("binary_sha256") != digest(args.binary) or binding.get("source_hashes_matched_build") is not True
            or binding.get("source_archive_sha256") != digest(args.source_binding.parent / binding["source_archive"])):
        raise ValueError("source and binary binding mismatch")
    args.out.mkdir(parents=True)
    binary_dir = args.out / "bin"
    binary_dir.mkdir()
    copied = {}
    for label, original, filename in (("binary", args.binary, args.binary.name),
            ("client", args.client, "client.py"), ("helper", args.handoff_helper, "handoff_helper.py"),
            ("harness", Path(__file__), "run_membership_handoff.py")):
        expected = digest(original)
        target = binary_dir / filename
        shutil.copy2(original, target)
        if digest(target) != expected:
            raise ValueError(f"{label} changed while copying")
        copied[label] = dict(path=target, sha256=expected)
    if copied["binary"]["sha256"] != binding["binary_sha256"]:
        raise ValueError("copied binary differs from sealed source binding")
    support = load("composition_handoff_helper", copied["helper"]["path"])
    client = load("composition_http_observer", copied["client"]["path"])
    case_type = type("CompositionCase", (CompositionMixin, support.Case), {})
    shutil.copy2(args.source_binding, args.out / "source-binding.json")
    support.save(args.out / "manifest.json", dict(schema_version=1, argv=sys.argv, seed=args.seed,
        copied={key:dict(path=str(value["path"]), sha256=value["sha256"]) for key,value in copied.items()},
        platform=sys.platform, python=sys.version, clock_domain=client.CLOCK_DOMAIN,
        processes=5, raft_groups_per_process=3, planned=list(args.cases),
        source_archive_sha256=binding["source_archive_sha256"]))
    results = []
    support.save(args.out / "results.json", dict(status="INCOMPLETE", planned=len(args.cases), attempted=0,
        unattempted=list(args.cases), results=[]))
    for ordinal, name in enumerate(args.cases):
        case = None
        interrupted = False
        try:
            case = case_type.__new__(case_type)
            case.__init__(args.out / name, name, args.seed+ordinal, copied["binary"]["path"],
                          client, args.case_seconds, support=support)
            result = case.run()
        except BaseException as error:
            result = dict(result="FAIL", reason=f"{type(error).__name__}: {error}", traceback=traceback.format_exc())
            interrupted = isinstance(error, (KeyboardInterrupt, SystemExit))
            if not interrupted and case is not None and getattr(case, "processes", None):
                try:
                    case.failure_diagnostics()
                except Exception as diagnostic_error:
                    result["diagnostic_error"] = f"{type(diagnostic_error).__name__}: {diagnostic_error}"
        finally:
            try:
                cleanup = [] if case is None else case.cleanup()
            except Exception as error:
                cleanup = [dict(error=f"cleanup raised {type(error).__name__}: {error}")]
        if cleanup:
            result.update(result="INVALID", cleanup_failures=cleanup)
        completed = [] if case is None else getattr(case, "completed_stages", [])
        offered = 0 if case is None else getattr(case, "offered_client_operations", 0)
        result.update(case=name, planned_stages=list(STAGES[name]), completed_stages=completed,
            unattempted_stages=list(STAGES[name][len(completed):]), planned_client_operations=PLANNED_CLIENT_OPERATIONS[name],
            offered_client_operations=offered, unattempted_client_operations=max(0, PLANNED_CLIENT_OPERATIONS[name]-offered),
            observations=0 if case is None else getattr(case, "observations", 0),
            acknowledged_receipt_observations=0 if case is None else len(getattr(case, "acks", [])),
            explicit_logical_rejections=0 if case is None else getattr(case, "logical_rejections", 0),
            unknown_http_or_control_observations=0 if case is None else getattr(case, "unknowns", 0))
        results.append(result)
        support.save(args.out / "results.json", dict(planned=len(args.cases), attempted=len(results),
            unattempted=list(args.cases[len(results):]), results=results))
        print(json.dumps({key:value for key,value in result.items() if key != "traceback"}), flush=True)
        if interrupted:
            break
    return 0 if len(results) == len(args.cases) and all(row["result"] == "PASS" for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
