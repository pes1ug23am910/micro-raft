"""Bounded local three-process shard handoff acceptance; retains every attempt.

Uses the project's shared HTTP observer, including its child-process operation
deadline and complete framing checks. This script is not a performance benchmark.
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

PLANNED_CLIENT_OPERATIONS = 15
PHASES = ("normal", "started", "fenced", "installing", "installed", "owned", "active", "cleaned", "complete")


def encoded(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def save(path, value):
    stage = path.with_suffix(path.suffix + ".new")
    with stage.open("xb") as stream:
        stream.write(encoded(value) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(stage, path)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def positive(value):
    return type(value) is int and 0 < value < 2**64


def validate_rejection(response, group, error):
    if (response.get("outcome") != "applied" or type(response.get("group")) is not int
            or response["group"] != group or not positive(response.get("index"))
            or response.get("reply") != {"kind": "rejected", "reply": error}):
        raise AssertionError(f"expected committed {error} rejection: {response}")


def validate_closed_rejection(response, group):
    validate_rejection(response, group, "session_closed")


def valid_origin(origin):
    return (isinstance(origin, dict) and set(origin) == {"group", "index"}
            and type(origin["group"]) is int and origin["group"] in (1, 2)
            and positive(origin["index"]))


def validate_checked(response, group):
    if not (response.get("outcome") == "checked" and type(response.get("group")) is int
            and response["group"] == group and type(response.get("node")) is int
            and response["node"] in (1, 2, 3) and positive(response.get("term"))
            and positive(response.get("context")) and positive(response.get("index"))
            and positive(response.get("applied_index")) and response["applied_index"] >= response["index"]):
        raise AssertionError("invalid read authority fence")


def planned_operations(name):
    return PLANNED_CLIENT_OPERATIONS + (name in ("fenced", "installing", "installed", "owned", "active")) + (name in ("fenced", "installing", "installed", "owned"))


def validate_receipt(response, receipt, group, action, expected=None):
    if type(response.get("group")) is not int or response["group"] != group or group not in (1, 2) or not positive(response.get("index")):
        raise AssertionError("applied response group/index mismatch")
    if set(receipt) != {"origin", "shard", "epoch", "session", "sequence"}:
        raise AssertionError("receipt schema mismatch")
    origin = receipt["origin"]
    if (not valid_origin(origin) or not valid_origin(receipt["session"]) or type(receipt["shard"]) is not int
            or receipt["shard"] != action["shard"] or not positive(receipt["epoch"])):
        raise AssertionError("receipt identity mismatch")
    if action["action"] == "register":
        if receipt["session"] != origin or receipt["sequence"] is not None:
            raise AssertionError("registration receipt mismatch")
    elif receipt["session"] != action["session"] or receipt["sequence"] != action.get("sequence"):
        raise AssertionError("receipt does not match submitted session/sequence")
    if action["action"] == "mutate" and not positive(receipt["sequence"]):
        raise AssertionError("mutation sequence must be a positive integer")
    if action["action"] == "close" and receipt["sequence"] is not None:
        raise AssertionError("close must not return a mutation sequence")
    if origin["group"] == group and origin["index"] > response["index"]:
        raise AssertionError("receipt is ahead of current group application")
    if expected is None:
        if origin != dict(group=group, index=response["index"]) or receipt["epoch"] != action["epoch"]:
            raise AssertionError("fresh operation returned unrelated historical receipt")
    elif receipt != expected:
        raise AssertionError("retry changed original namespaced receipt")


class Case:
    def __init__(self, root, name, seed, binary, client, budget, automatic=False):
        self.root, self.name, self.seed, self.binary, self.client = root, name, seed, binary, client
        self.automatic=automatic
        self.deadline = time.monotonic() + budget
        self.sequence = 0
        self.processes, self.handles, self.boots = {}, [], {}
        self.unknowns, self.observations = 0, 0
        self.offered_client_operations = 0
        root.mkdir()
        self.journal = (root / "observations.jsonl").open("xb")
        self.events = []
        self.acks = []
        self.logical_rejections = 0
        self.reservations = []
        ports = []
        for _ in range(12):
            held = socket.socket()
            held.bind(("127.0.0.1", 0))
            held.listen()
            self.reservations.append(held)
            ports.append(held.getsockname()[1])
        self.topology = dict(version=1, cluster=f"handoff-{seed}", groups=[1, 2], owners=[1, 2, 1, 2],
                             genesis_voters={str(g): [1, 2, 3] for g in (0, 1, 2)},
                             nodes=[dict(id=n, http=f"127.0.0.1:{ports[(n-1)*4]}",
                                raft={str(g): f"127.0.0.1:{ports[(n-1)*4+g+1]}" for g in (0, 1, 2)})
                                    for n in (1, 2, 3)])
        self.config = root / "topology.json"
        save(self.config, self.topology)
        command = self.command(1) + ["--check-config"]
        checked = subprocess.run(command, capture_output=True, timeout=20, check=False)
        (root / "check-config.stdout").write_bytes(checked.stdout)
        (root / "check-config.stderr").write_bytes(checked.stderr)
        if checked.returncode:
            raise RuntimeError(f"configuration check failed: {checked.returncode}")
        self.cluster = client.strict_json(checked.stdout.decode("utf-8"))["cluster"]

    def command(self, node):
        return [str(self.binary), "--topology", str(self.config), "--id", str(node),
                "--data-dir", str(self.root / f"node{node}"), "--seed", str(self.seed),
                "--snapshot-threshold", "4"] + ([] if self.automatic else ["--manual-handoff"])

    def event(self, kind, **fields):
        row = dict(kind=kind, monotonic_ns=time.monotonic_ns(), **fields)
        self.events.append(row)
        save(self.root / "events.json", self.events)

    def start(self, node):
        prior = self.processes.get(node)
        if prior is not None and prior.poll() is None:
            raise RuntimeError("refuse to replace a running owned process")
        boot = self.boots.get(node, 0) + 1
        self.boots[node] = boot
        stdout = (self.root / f"node{node}-boot{boot}.stdout.log").open("xb")
        stderr = (self.root / f"node{node}-boot{boot}.stderr.log").open("xb")
        self.handles.extend([stdout, stderr])
        process = subprocess.Popen(self.command(node), stdout=stdout, stderr=stderr,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0)
        self.processes[node] = process
        self.event("start", node=node, pid=process.pid, boot=boot, argv=self.command(node))

    def kill(self, node, reason):
        process = self.processes[node]
        if process.poll() is not None:
            raise RuntimeError(f"owned victim already exited: {node}/{process.returncode}")
        invoked = time.monotonic_ns()
        process.kill()
        code = process.wait(timeout=10)
        self.event("kill", node=node, pid=process.pid, reason=reason, invoked_monotonic_ns=invoked,
                   exit_code=code, mechanism="TerminateProcess" if os.name == "nt" else "SIGKILL")

    def live(self):
        return [node for node, process in self.processes.items() if process.poll() is None]

    def request(self, path, body=None, node=None):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("case budget exhausted; later operations unattempted")
        if node is None:
            available = self.live()
            if not available:
                raise RuntimeError("no owned running observer endpoint")
            node = available[0]
        self.sequence += 1
        value = None if body is None else encoded(dict(cluster=self.cluster, body=body)).decode("utf-8")
        record = self.client.request_record(f"node{node}", path, timeout=min(15, remaining), value=value,
            endpoint="http://" + self.topology["nodes"][node-1]["http"], run_id=self.root.parent.name,
            cycle_id=self.name, op_id=f"request-{self.sequence}", method="GET" if body is None else "POST",
            content_type=None if body is None else "application/json")
        row = dict(request=dict(path=path, node=node, body=body), observation=record)
        raw = encoded(row) + b"\n"
        if self.journal.tell() + len(raw) > 32 * 1024 * 1024:
            raise RuntimeError("observer journal capacity reached; later operations unattempted")
        self.journal.write(raw)
        self.journal.flush()
        os.fsync(self.journal.fileno())
        self.observations += 1
        if record["transport_error"] is not None or record["status"] != 200:
            self.unknowns += 1
            return dict(outcome="unknown", transport=record["transport_error"], status=record["status"])
        try:
            payload = self.client.strict_json(record["body"])
        except (ValueError, UnicodeError, RecursionError):
            self.unknowns += 1
            return dict(outcome="unknown", protocol="invalid JSON response")
        if payload.get("cluster") != self.cluster:
            raise AssertionError("response crossed cluster identity")
        if path == "/status":
            return payload
        response = payload["body"]
        if response.get("outcome") == "unknown":
            self.unknowns += 1
        return response

    def query(self, group, query):
        return self.request("/v1/checked", dict(group=group, query=query))

    def checked(self, group, query):
        response = self.query(group, query)
        if response.get("outcome") != "checked":
            raise RuntimeError(f"checked observation unavailable: {response}")
        validate_checked(response, group)
        return response

    def execute(self, group, action):
        self.last_write_request = (group, action)
        if action["action"] in ("register", "mutate", "close"):
            self.offered_client_operations += 1
        return self.request("/v1/dispatch", dict(group=group, action=action))

    def until(self, fn, label, seconds=25):
        end = min(self.deadline, time.monotonic() + seconds)
        last = None
        while time.monotonic() < end:
            last = fn()
            if last:
                return last
            time.sleep(0.05)
        raise TimeoutError(f"{label}: deadline; last observation {last}")

    def ready(self):
        def attempt():
            for group in (0, 1, 2):
                response = self.query(group, {"query": "configuration"})
                if response.get("outcome") != "checked" or response["observed"]["initialized_at"] is None:
                    return False
            return True
        self.until(attempt, "three initialized Raft groups", 30)

    def caught_up(self, node, fences):
        def attempt():
            response=self.request("/status",node=node)
            if type(response.get("node")) is not int or response["node"] != node or not isinstance(response.get("groups"),list):
                return False
            if len(response["groups"]) != 3 or any(type(group.get("group")) is not int or group["group"] not in (0,1,2) for group in response["groups"]):
                return False
            groups={group["group"]:group for group in response["groups"]}
            if len(groups) != 3:
                return False
            if not all(group in groups and type(groups[group].get("applied_index")) is int
                       and groups[group]["applied_index"] >= fence for group,fence in fences.items()):
                return False
            return response
        result=self.until(attempt,f"restarted node{node} applied checked fences",25)
        save(self.root/f"node{node}-recovered-status.json",result)

    def receipt(self, response, expected=None):
        if response.get("outcome") != "applied" or not positive(response.get("index")):
            raise RuntimeError(f"write completion not acknowledged: {response}")
        reply = response["reply"]
        if reply.get("kind") != "data" or set(reply["reply"]) != {"Receipt"}:
            raise AssertionError(f"typed receipt missing: {response}")
        receipt = reply["reply"]["Receipt"]
        group, action = self.last_write_request
        validate_receipt(response, receipt, group, action, expected)
        self.acks.append(dict(response_index=response["index"], response_group=response["group"], receipt=receipt))
        return receipt

    def views(self):
        control = self.checked(0, dict(query="transfer", request_id="move"))
        source = self.checked(1, dict(query="shard", shard=0))
        destination = self.checked(2, dict(query="shard", shard=0))
        return control, source, destination

    def at(self, label, views):
        transfer = views[0]["observed"]["transfer"]
        phase = transfer["phase"]["phase"]
        source = views[1]["observed"]["ownership"]
        dest = views[2]["observed"]["ownership"]
        return {
            "started": phase == "started",
            "fenced": phase == "fenced" and dest["state"] == "unassigned",
            "installing": phase == "fenced" and dest["state"] == "installing" and positive(dest["next_offset"]) and positive(dest["image"]["bytes"]) and dest["next_offset"] < dest["image"]["bytes"],
            "installed": phase == "installed",
            "owned": phase == "owned" and dest["state"] == "installed",
            "active": phase == "owned" and dest["state"] == "active",
            "cleaned": phase == "activated" and source["state"] == "retired",
            "complete": phase == "complete",
            "normal": phase == "complete",
        }[label]

    def drive(self, target):
        end = min(self.deadline, time.monotonic() + 70)
        while time.monotonic() < end:
            views = self.views()
            if self.at(target, views):
                return views
            phase = views[0]["observed"]["transfer"]["phase"]["phase"]
            if phase == "aborted":
                raise AssertionError("ordinary handoff unexpectedly aborted")
            if self.automatic:
                time.sleep(0.05)
                continue
            response = self.request("/v1/reconcile", dict(request_id="move"))
            if response.get("outcome") in ("unknown", "unavailable", "not_leader"):
                time.sleep(0.05)
        raise TimeoutError(f"handoff did not reach {target}")

    def phase_probes(self, session, key):
        probes=[]
        if self.name in ("fenced", "installing", "installed", "owned", "active"):
            probes.append((1,1,"unavailable"))
        if self.name in ("fenced", "installing", "installed", "owned"):
            probes.append((2,2,"stale_route" if self.name == "fenced" else "unavailable"))
        for group,epoch,error in probes:
            read=self.query(group,dict(query="read",shard=0,epoch=epoch,key=key))
            if read != dict(outcome="rejected",error=error):
                raise AssertionError(f"phase {self.name} group{group} improperly served read: {read}")
            write=self.execute(group,dict(action="mutate",shard=0,epoch=epoch,session=session,key=key,sequence=4,value="must-not-be-admitted"))
            validate_rejection(write,group,error)
            self.logical_rejections += 1
        observed=self.views()
        if not self.at(self.name,observed):
            raise AssertionError("manual handoff phase changed during fencing probes")
        return observed

    def run(self):
        for held in getattr(self, "reservations", []):
            held.close()
        self.reservations.clear()
        for node in (1, 2, 3):
            self.start(node)
        self.ready()
        keys = [f"fixture-{n}" for n in range(1000)
                if int.from_bytes(hashlib.sha256(f"fixture-{n}".encode()).digest()[:8], "big") % 4 == 0][:3]
        key, deleted, closed_key = keys
        registration = self.receipt(self.execute(1, dict(action="register", shard=0, epoch=1, nonce="session")))
        session = registration["session"]
        value = "actual-value-" + "x" * 24000
        original = self.receipt(self.execute(1, dict(action="mutate", shard=0, epoch=1, session=session, key=key, sequence=1, value=value)))
        self.receipt(self.execute(1, dict(action="mutate", shard=0, epoch=1, session=session, key=deleted, sequence=1, value="deleted-canary")))
        tombstone = self.receipt(self.execute(1, dict(action="mutate", shard=0, epoch=1, session=session, key=deleted, sequence=2, value=None)))
        for sequence in (2, 3):
            original = self.receipt(self.execute(1, dict(action="mutate", shard=0, epoch=1, session=session, key=key, sequence=sequence, value=value)))
        closed_registration = self.receipt(self.execute(1, dict(action="register", shard=0, epoch=1, nonce="closed-session")))
        closed_session = closed_registration["session"]
        self.receipt(self.execute(1, dict(action="mutate", shard=0, epoch=1, session=closed_session, key=closed_key, sequence=1, value="closed-session-canary")))
        self.receipt(self.execute(1, dict(action="close", shard=0, epoch=1, session=closed_session)))
        begun = self.execute(0, dict(action="begin_move", request_id="move", shard=0, epoch=1, destination=2))
        if begun.get("outcome") != "applied":
            raise RuntimeError(f"move request not acknowledged: {begun}")
        reached = self.drive(self.name)
        if self.name in ("fenced", "installing", "installed", "owned", "active"):
            reached = self.phase_probes(session,key)
        save(self.root / "phase-before-fault.json", reached)
        victim = None
        if self.name != "normal":
            group = {"started": 0, "fenced": 1, "installing": 2, "installed": 2, "owned": 0, "active": 2, "cleaned": 1, "complete": 0}[self.name]
            authority = self.checked(group, {"query": "configuration"})
            victim = authority["node"]
            self.event("fault-authority", group=group, authority=authority)
            self.kill(victim, "phase fault")
            self.ready()
        final = self.drive("complete")
        transfer = final[0]["observed"]["transfer"]
        activation = transfer["phase"]["activation"]
        image = activation["ownership"]["installation"]["image"]
        if image["bytes"] <= 32 * 1024:
            raise AssertionError("fixture did not exercise multiple pull chunks")
        for row in self.acks:
            receipt = row["receipt"]
            if receipt["origin"]["group"] == 1 and receipt["origin"]["index"] > image["fence"]["index"]:
                raise AssertionError("source image predates an acknowledged write")
        before = self.checked(0, dict(query="route", shard=0))
        if before["observed"]["route"]["owner"] != 2 or before["observed"]["route"]["epoch"] != 2:
            raise AssertionError("controller did not commit destination ownership")
        for stored_key, expected in ((key, value), (deleted, None), (closed_key, "closed-session-canary")):
            read = self.checked(2, dict(query="read", shard=0, epoch=2, key=stored_key))
            if read["observed"]["value"] != expected or read["applied_index"] < activation["activated"]["index"]:
                raise AssertionError("checked destination lost acknowledged data/tombstone or precedes activation")
        after = self.checked(0, dict(query="route", shard=0))
        if before["observed"]["route"] != after["observed"]["route"]:
            raise AssertionError("ownership changed around read oracle")
        self.receipt(self.execute(2, dict(action="mutate", shard=0, epoch=2, session=session, key=key, sequence=3, value=value)), original)
        self.receipt(self.execute(2, dict(action="mutate", shard=0, epoch=2, session=session, key=deleted, sequence=2, value=None)), tombstone)
        self.closed_retry(closed_session,closed_key)
        stale = self.query(1, dict(query="read", shard=0, epoch=1, key=key))
        if stale != dict(outcome="rejected", error="stale_route"):
            raise AssertionError("retired source still served an old route")
        save(self.root / "ownership-read-oracle.json", dict(before=before,after=after,transfer=transfer,acks=self.acks))
        fences={response["group"]:response["applied_index"] for response in final}
        if victim is not None:
            self.start(victim)
            self.ready()
            self.caught_up(victim,fences)
        else:
            for node in list(self.live()):
                self.kill(node, "whole-cluster reopen after committed handoff")
            for node in (1, 2, 3):
                self.start(node)
            self.ready()
            for node in (1,2,3):
                self.caught_up(node,fences)
        self.receipt(self.execute(2, dict(action="mutate", shard=0, epoch=2, session=session, key=key, sequence=3, value=value)), original)
        reopened = self.checked(2, dict(query="read", shard=0, epoch=2, key=key))
        if reopened["observed"]["value"] != value:
            raise AssertionError("reopened cluster lost installed value")
        reopened_deleted=self.checked(2,dict(query="read",shard=0,epoch=2,key=deleted))
        if reopened_deleted["observed"]["value"] is not None:
            raise AssertionError("reopened cluster lost deletion tombstone")
        self.receipt(self.execute(2,dict(action="mutate",shard=0,epoch=2,session=session,key=deleted,sequence=2,value=None)),tombstone)
        self.closed_retry(closed_session,closed_key)
        save(self.root / "final-checked-state.json", self.views())
        return dict(result="PASS", multi_chunk_bytes=image["bytes"], acknowledged_receipt_observations=len(self.acks),
                    victim=victim, source_origin=original["origin"], destination_activation=activation["activated"])

    def closed_retry(self, session, key):
        result=self.execute(2,dict(action="mutate",shard=0,epoch=2,session=session,key=key,sequence=1,value="closed-session-canary"))
        validate_closed_rejection(result, 2)
        self.logical_rejections += 1

    def cleanup(self):
        failures = []
        for node, process in self.processes.items():
            invoked=None
            try:
                if process.poll() is None:
                    invoked=time.monotonic_ns()
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass  # Natural exit raced this owned-handle termination.
            except Exception as error:
                failures.append(dict(node=node,stage="terminate",error=str(error)))
            # Reap even if termination/polling failed; an error for one owned
            # child must not prevent visiting the others or closing resources.
            try:
                code=process.wait(timeout=10)
                if invoked is not None:
                    self.event("kill",node=node,pid=process.pid,reason="owned fixture cleanup",
                               invoked_monotonic_ns=invoked,exit_code=code,
                               mechanism="TerminateProcess" if os.name=="nt" else "SIGKILL")
            except Exception as error:
                failures.append(dict(node=node,stage="reap-or-record",error=str(error)))
        for held in getattr(self, "reservations", []):
            try:
                held.close()
            except Exception as error:
                failures.append(dict(stage="close-reservation",error=str(error)))
        for stream in self.handles:
            try:
                stream.close()
            except Exception as error:
                failures.append(dict(stage="close-process-log",error=str(error)))
        if hasattr(self, "journal"):
            try:
                self.journal.close()
            except Exception as error:
                failures.append(dict(stage="close-observer-journal",error=str(error)))
        return failures


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary",type=Path,required=True)
    parser.add_argument("--client",type=Path,required=True)
    parser.add_argument("--out",type=Path,required=True)
    parser.add_argument("--source-binding",type=Path,required=True)
    parser.add_argument("--automatic-normal",action="store_true",help="Poll normal handoff; never invoke reconciliation manually")
    parser.add_argument("--cases",nargs="+",choices=PHASES,default=list(PHASES))
    parser.add_argument("--seed",type=int,default=20261001)
    parser.add_argument("--case-seconds",type=float,default=180)
    args=parser.parse_args()
    if not math.isfinite(args.case_seconds) or not 1 <= args.case_seconds <= 600:
        raise ValueError("case budget must be finite and within1..600seconds")
    if len(set(args.cases)) != len(args.cases):
        raise ValueError("case names must be unique")
    if args.automatic_normal and args.cases != ["normal"]:
        raise ValueError("automatic mode is a separate normal-only case")
    if args.out.exists():
        raise ValueError("output must be a fresh path; evidence is never reused")
    binding=json.loads(args.source_binding.read_text(encoding="utf-8-sig"))
    if (binding.get("binary_sha256") != digest(args.binary)
            or not binding.get("source_hashes_matched_build")
            or binding.get("source_archive_sha256") != digest(args.source_binding.parent/binding["source_archive"])):
        raise ValueError("binary/source binding does not match the sealed archive")
    args.out.mkdir(parents=True)
    bin_dir=args.out/"bin";bin_dir.mkdir()
    binary=bin_dir/args.binary.name;client_file=bin_dir/"client.py"
    shutil.copy2(args.binary,binary);shutil.copy2(args.client,client_file)
    shutil.copy2(Path(__file__),bin_dir/"run_handoff.py")
    if digest(binary) != binding["binary_sha256"]:
        raise ValueError("copied binary differs from sealed source binding")
    shutil.copy2(args.source_binding,args.out/"source-binding.json")
    spec=importlib.util.spec_from_file_location("bounded_handoff_observer",client_file)
    client=importlib.util.module_from_spec(spec);spec.loader.exec_module(client)
    manifest=dict(schema_version=1,argv=sys.argv,binary_sha256=digest(binary),client_sha256=digest(client_file),
                  harness_sha256=digest(bin_dir/"run_handoff.py"),seed=args.seed,planned=args.cases,platform=sys.platform,
                  python=sys.version,clock_domain=client.CLOCK_DOMAIN,processes_per_case=3,raft_groups_per_process=3,
                  automatic_normal=args.automatic_normal,
                  source_archive_sha256=binding["source_archive_sha256"],source_binding_sha256=digest(args.source_binding))
    save(args.out/"manifest.json",manifest)
    results=[]
    for ordinal,name in enumerate(args.cases):
        case=None
        try:
            case=Case.__new__(Case)
            case.__init__(args.out/name,name,args.seed+ordinal,binary,client,args.case_seconds,args.automatic_normal)
            result=case.run()
        except BaseException as error:
            result=dict(result="FAIL",reason=f"{type(error).__name__}: {error}",traceback=traceback.format_exc())
        finally:
            try:
                cleanup=[] if case is None else case.cleanup()
            except Exception as error:
                cleanup=[dict(stage="cleanup",error=str(error))]
        if cleanup:
            result.update(result="INVALID",cleanup_failures=cleanup)
        result.update(case=name,observations=0 if case is None else case.observations,
                      acknowledged_receipt_observations=0 if case is None else len(getattr(case,"acks",[])),
                      explicit_logical_rejections=0 if case is None else getattr(case,"logical_rejections",0),
                      unknown_http_or_control_observations=0 if case is None else case.unknowns,
                      planned_client_operations=planned_operations(name),
                      offered_client_operations=0 if case is None else case.offered_client_operations,
                      unattempted_client_operations=planned_operations(name) if case is None else max(0,planned_operations(name)-case.offered_client_operations))
        results.append(result)
        save(args.out/"results.json",dict(planned=len(args.cases),attempted=len(results),unattempted=args.cases[len(results):],results=results))
        print(json.dumps({k:v for k,v in result.items() if k!="traceback"}),flush=True)
    return 0 if all(result["result"]=="PASS" for result in results) else 1


if __name__=="__main__":
    raise SystemExit(main())
