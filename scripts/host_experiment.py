#!/usr/bin/env python3
"""One-observer, three-VM experiments using the existing bounded client and ACK oracle."""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shlex
import shutil
import statistics
import subprocess
import sys
import time
import urllib.parse
import uuid

import cluster_experiment as oracle

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'deploy/hosts'))
import host_agent
_spec = importlib.util.spec_from_file_location('host_http_client', ROOT / 'deploy/docker/client/client.py')
client = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(client)
CASES = ('healthy', 'delay', 'jitter', 'loss', 'partition', 'kill', 'pause', 'restart')
PROFILES = dict(delay=dict(delay_ms=25, jitter_ms=0, loss_percent=0),
                jitter=dict(delay_ms=25, jitter_ms=10, loss_percent=0),
                loss=dict(delay_ms=0, jitter_ms=0, loss_percent=10),
                partition=dict(delay_ms=0, jitter_ms=0, loss_percent=100))


def agent_config(topology, node):
    member = topology['nodes'][node]
    return dict(schema_version=1, run_id=topology['run_id'], node=node, id=int(node[-1]),
                private_ip=member['private_ip'], peers={name: row['private_ip'] for name, row in topology['nodes'].items() if name != node},
                **{key: topology[key] for key in ('raft_port', 'http_port', 'probe_port', 'binary', 'binary_sha256', 'expires_unix')})


def validate_topology(topology):
    if not isinstance(topology, dict) or topology.get('schema_version') != 1 or type(topology['schema_version']) is not int:
        raise ValueError('unsupported topology schema')
    nodes = topology.get('nodes')
    if not isinstance(nodes, dict) or set(nodes) != set(oracle.NODES):
        raise ValueError('exactly node1, node2 and node3 are required')
    identities = []
    for name, node in nodes.items():
        if not isinstance(node, dict):
            raise ValueError('node must be an object')
        identity = node.get('instance_id')
        if not isinstance(identity, str) or not identity or len(identity) > 128:
            raise ValueError('provider or hypervisor instance identity required')
        identities.append(identity)
        host_agent.validate_config(agent_config(topology, name))
        if node.get('local') is not (name == 'node1'):
            raise ValueError('node1 must be the sole local observer host')
        if name != 'node1':
            if node.get('ssh_host') != node['private_ip']:
                raise ValueError('remote agent access must use the declared private address')
            if not re.fullmatch(r'[a-z_][a-z0-9_-]{0,31}', node.get('ssh_user', '')):
                raise ValueError('invalid SSH user')
            if type(node.get('ssh_port')) is not int or not 1 <= node['ssh_port'] <= 65535:
                raise ValueError('invalid SSH port')
        for field in ('agent_path', 'config_path'):
            value = node.get(field)
            if not isinstance(value, str) or not re.fullmatch(r'/[A-Za-z0-9_./-]+', value) or '..' in Path(value).parts:
                raise ValueError('agent/config paths must be absolute without traversal')
    if len(set(identities)) != 3:
        raise ValueError('three distinct VM identities required')
    return topology


def agent_argv(node):
    remote = ['sudo', '-n', 'python3', '-B', node['agent_path'], '--config', node['config_path']]
    if node['local']:
        return remote
    return ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
            '-o', 'ConnectTimeout=5', '-o', 'ServerAliveInterval=3', '-o', 'ServerAliveCountMax=2',
            '-p', str(node['ssh_port']), node['ssh_user'] + '@' + node['ssh_host'], shlex.join(remote)]


def verify_probe(profile, baseline, observed, tc_rows):
    if observed['sent'] <= 0 or observed['received'] != len(observed['rtt_ns']):
        raise oracle.ExperimentError('invalid probe counts')
    if profile['loss_percent'] == 100:
        if observed['received'] != 0:
            raise oracle.ExperimentError('partition probe still reached its peer')
    else:
        if not observed['rtt_ns'] or not baseline['rtt_ns']:
            raise oracle.Inconclusive('no successful probe samples')
        if profile['delay_ms'] and statistics.median(observed['rtt_ns']) < statistics.median(baseline['rtt_ns']) + profile['delay_ms'] * 1e6:
            raise oracle.Inconclusive('observed probe delay did not demonstrate the requested shaping')
        if profile['jitter_ms'] and max(observed['rtt_ns']) - min(observed['rtt_ns']) < profile['jitter_ms'] * 1e6 / 2:
            raise oracle.Inconclusive('probe samples did not demonstrate jitter variation')
        if profile['loss_percent'] and observed['received'] == observed['sent']:
            raise oracle.Inconclusive('probe sample observed no packet loss')
    if not any(row.get('packets', 0) > 0 or row.get('drops', 0) > 0 or row.get('dropped', 0) > 0 for row in tc_rows):
        raise oracle.Inconclusive('netem counters do not show traffic on the shaped path')


def validate_probe(record, source, target, count):
    if not isinstance(record, dict) or record.get('source') != source or record.get('target') != target:
        raise oracle.ExperimentError('probe identity does not match request')
    for field in ('sent', 'received', 'issued_ns', 'completed_ns'):
        if type(record.get(field)) is not int or record[field] < 0:
            raise oracle.ExperimentError('invalid probe counts/timestamps')
    samples = record.get('rtt_ns')
    duration = record['completed_ns'] - record['issued_ns']
    if (record['sent'] != count or not 0 <= record['received'] <= count or duration < 0
            or not isinstance(samples, list) or len(samples) != record['received']
            or any(type(sample) is not int or not 0 <= sample <= duration for sample in samples)):
        raise oracle.ExperimentError('probe observations contradict counts/timing')


def publish_result(out, result):
    try:
        pending = out / 'result.json.pending'
        oracle.write_evidence(pending, json.dumps(result, indent=2) + '\n')
        os.replace(pending, out / 'result.json')
        client._sync_directory(out)
    except (OSError, RuntimeError) as error:
        result['verdict'] = 'FAIL'
        result['cleanup_errors'].append(f'result publication: {error}')
        # If the rename succeeded but directory sync failed, invalidate that
        # visible result when storage still permits a write. Exit zero is also
        # required; an unreadable/unwritable result is never a valid PASS.
        try:
            oracle.write_evidence(out / 'result.json', json.dumps(result, indent=2) + '\n')
        except OSError:
            pass
    return result


class HostRunner(oracle.Runner):
    def __init__(self, args):
        self.args = args
        self.topology = validate_topology(client.strict_json(Path(args.topology).read_text(encoding='utf-8')))
        self.project = self.topology['run_id']
        self.out = Path(args.out).resolve()
        self.owner_token = uuid.uuid4().hex
        self.started_ns = time.monotonic_ns()
        self.cleanup_deadline = time.monotonic() + self.topology['expires_unix'] - time.time()
        self.experiment_deadline = self.cleanup_deadline - 120
        self.poll_deadline = None
        self.owned_nodes, self.cycles, self.errors = [], [], []
        self.active_fault, self.fault_started_at = None, None
        self.operation_no = self.observation_no = self.rpc_no = 0
        self.output_owned = False
        self.baseline_probes = {}
        self.last_probes = {}
        self.agent_fingerprints = {node: hashlib.sha256(json.dumps(agent_config(self.topology, node), sort_keys=True).encode()).hexdigest() for node in oracle.NODES}

    def budget(self, desired=30, cleanup=False):
        if cleanup:
            seconds = min(desired, self.cleanup_deadline - time.monotonic())
            if seconds <= 0:
                raise oracle.Deadline('cleanup lease deadline reached; reconcile externally')
            return seconds
        end = self.experiment_deadline
        if self.poll_deadline is not None:
            end = min(end, self.poll_deadline)
        seconds = min(desired, end - time.monotonic())
        if seconds <= 0:
            raise oracle.Deadline('phase or experiment lease deadline reached')
        return seconds

    def event(self, kind, **fields):
        if not self.output_owned:
            return
        value = dict(kind=kind, monotonic_ns=time.monotonic_ns(), clock_domain=client.CLOCK_DOMAIN, **fields)
        with (self.out / 'events.jsonl').open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(value, allow_nan=False) + '\n')
            stream.flush()
            os.fsync(stream.fileno())

    def rpc(self, node, action, cleanup=False, **fields):
        self.rpc_no += 1
        argv = agent_argv(self.topology['nodes'][node])
        request = dict(action=action, owner_token=self.owner_token, **fields)
        start = time.monotonic_ns()
        process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = process.communicate(json.dumps(request).encode(), timeout=self.budget(90, cleanup))
        except BaseException:
            process.kill()
            process.communicate(timeout=5)
            raise
        self.event('agent_call', node=node, action=action, issued_ns=start, completed_ns=time.monotonic_ns(),
                   exit_code=process.returncode, request=fields)
        raw_path = self.out / f'agent-{self.rpc_no:05d}-{node}-{action}.json'
        oracle.write_evidence(raw_path, stdout.decode('utf-8', errors='replace'))
        if process.returncode != 0:
            raise oracle.ExperimentError(f'{node} {action} agent failed; retained {raw_path.name}: {stderr[-512:].decode(errors="replace")}')
        response = client.strict_json(stdout)
        if not isinstance(response, dict) or response.get('ok') is not True or 'result' not in response:
            raise oracle.ExperimentError('invalid host agent result')
        if action == 'probe':
            validate_probe(response['result'], node, fields['target'], fields.get('count', 32))
        return response['result']

    def inspect(self, node, cleanup=False):
        before = time.time()
        state = self.rpc(node, 'inspect', cleanup=cleanup)
        after = time.time()
        wall = state.get('wall_time_unix')
        if type(wall) not in (int, float) or not math.isfinite(wall) or not before - 5 <= wall <= after + 5:
            raise oracle.ExperimentError('guest UTC clock differs by more than lease tolerance')
        if any(state.get(field) != expected for field, expected in dict(run_id=self.project, node=node,
                binary_sha256=self.topology['binary_sha256'], config_sha256=self.agent_fingerprints[node],
                expires_unix=self.topology['expires_unix']).items()):
            raise oracle.ExperimentError('agent topology/software identity mismatch')
        return state

    def setup(self):
        self.budget(1)
        if not client.CLOCK_DOMAIN.startswith('linux-monotonic:'):
            raise oracle.ExperimentError('controller must run on the Linux node1 observer')
        self.out.mkdir(parents=True, exist_ok=True)
        if any(self.out.iterdir()):
            raise oracle.ExperimentError('output must be fresh and empty')
        self.output_owned = True
        oracle.write_evidence(self.out / 'topology.json', json.dumps(self.topology, indent=2) + '\n')
        oracle.write_evidence(self.out / 'controller.json', json.dumps(dict(owner_token=self.owner_token,
            clock_domain=client.CLOCK_DOMAIN, started_ns=self.started_ns, source_hashes={str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in (Path(__file__), Path(oracle.__file__), Path(host_agent.__file__), Path(client.__file__))}), indent=2) + '\n')
        # An existing ledger is an error; the shared client requires the file on reads.
        ledger = self.out / 'acks.jsonl'
        with ledger.open('x', encoding='utf-8') as stream:
            stream.flush()
            os.fsync(stream.fileno())
        client._sync_directory(self.out)
        states = []
        for node in oracle.NODES:
            self.owned_nodes.append(node)  # retain ambiguous initialize attempts for token-scoped cleanup
            self.rpc(node, 'initialize')
            states.append(self.inspect(node))
        if len({row['boot_id'] for row in states}) != 3 or len({row['machine_id'] for row in states}) != 3:
            raise oracle.ExperimentError('three distinct VM boot and machine identities required')
        if states[0]['boot_id'] not in client.CLOCK_DOMAIN:
            raise oracle.ExperimentError('observer is not colocated with node1')
        if not all(row['watchdog']['running'] and row['probe']['running'] for row in states):
            raise oracle.ExperimentError('workload expiry or probe service is absent')
        for node in oracle.NODES:
            self.rpc(node, 'start')
        self.leader()
        for source in oracle.NODES:
            for target in oracle.NODES:
                if source != target:
                    result = self.rpc(source, 'probe', target=target, count=16)
                    if result['received'] != result['sent']:
                        raise oracle.Inconclusive('baseline private-path probes lost packets')
                    self.baseline_probes[(source, target)] = result
        self.event('setup_complete', identities=states)

    def client(self, command, node=None, **fields):
        if command == 'ledger':
            records = client.read_ledger(self.out / 'acks.jsonl', timeout=self.budget(2))
            oracle.acknowledged_state(records)
            if any(row['run_id'] != self.project or row['clock_domain'] != client.CLOCK_DOMAIN for row in records):
                raise oracle.ExperimentError('ledger provenance changed')
            return records
        self.observation_no += 1
        metadata = dict(run_id=fields.get('run_id', self.project), cycle_id=fields.get('cycle_id', 'observation'),
                        op_id=fields.get('op_id', f'{self.project}-observation-{self.observation_no}'))
        endpoint = f"http://{self.topology['nodes'][node]['private_ip']}:{self.topology['http_port']}"
        timeout = self.budget(2)
        if command == 'put':
            result = client.perform_put(node, fields['key'], fields['value'], timeout=timeout,
                        ledger_path=self.out / 'acks.jsonl', endpoint=endpoint, **metadata)
            oracle.validate_write(result)
        else:
            path = '/status' if command == 'status' else '/kv/' + urllib.parse.quote(fields['key'], safe='')
            result = client.request_record(node, path, timeout, endpoint=endpoint, **metadata)
            oracle.validate_observation(result)
        self.event('http_observation', command=command, record=result)
        return result

    def injection_confirmed(self, fault, node):
        state = self.inspect(node)
        process = state['process']
        if fault in ('kill', 'restart'):
            if process['running']:
                return False
            if fault == 'restart' and (process['exit_code'] != 0 or process['exit_kind'] != '1'):
                raise oracle.ExperimentError('held graceful stop did not exit cleanly')
            return True
        if fault == 'pause':
            return process['running'] and process['paused']
        for member in oracle.NODES:
            current = state if member == node else self.inspect(member)
            shape = current.get('shape')
            targets = set(oracle.NODES) - {node} if member == node else {node}
            if not shape or set(shape['targets']) != targets or shape['profile'] != PROFILES['partition']:
                return False
            host_agent.verify_netem(current['tc']['netem'], PROFILES['partition'])
        return True

    def inject(self, fault, node):
        self.fault_started_at = self.inspect(node)['process']['invocation']
        self.active_fault = (fault, node)
        issued = time.monotonic_ns()
        if fault == 'partition':
            for member in oracle.NODES:
                targets = list(set(oracle.NODES) - {node}) if member == node else [node]
                self.rpc(member, 'shape', profile=PROFILES['partition'], targets=targets)
            for peer in oracle.NODES:
                if peer != node:
                    for source, target in ((node, peer), (peer, node)):
                        probe = self.rpc(source, 'probe', target=target, count=6)
                        tc = self.inspect(target)['tc']['netem']
                        verify_probe(PROFILES['partition'], self.baseline_probes[(source, target)], probe, tc)
                        self.event('partition_probe', probe=probe)
        else:
            self.rpc(node, 'graceful' if fault == 'restart' else fault)
        self.wait_for('fault not confirmed', lambda: self.injection_confirmed(fault, node))
        confirmed = time.monotonic_ns()
        self.event('injection', fault=fault, node=node, issued_ns=issued, confirmed_ns=confirmed)
        return issued, confirmed

    def heal(self):
        if self.active_fault is None:
            return
        fault, node = self.active_fault
        if fault == 'partition':
            for member in oracle.NODES:
                self.rpc(member, 'heal')
        elif fault == 'pause':
            self.rpc(node, 'resume')
        else:
            self.rpc(node, 'start')
        def restored():
            process = self.inspect(node)['process']
            return process['running'] and not process['paused'] and (fault not in ('kill', 'restart') or process['invocation'] != self.fault_started_at)
        self.wait_for('old node did not resume', restored)
        for peer in oracle.NODES:
            if peer != node:
                probe = self.rpc(node, 'probe', target=peer, count=6)
                if probe['received'] != probe['sent']:
                    raise oracle.Inconclusive('healed private path still loses probes')
        self.active_fault = None
        self.event('healed', fault=fault, node=node)

    def network_case(self, case, number):
        record = dict(cycle_id=f'{case}-{number}', fault=case, outcome='running', started_ns=time.monotonic_ns(),
                      planned_offered=self.args.operations, offered=0, acknowledged=0)
        self.cycles.append(record)
        try:
            if case != 'healthy':
                issued = time.monotonic_ns()
                for node in oracle.NODES:
                    self.rpc(node, 'shape', profile=PROFILES[case], targets=list(set(oracle.NODES) - {node}))
                for source in oracle.NODES:
                    for target in oracle.NODES:
                        if source == target:
                            continue
                        probe = self.rpc(source, 'probe', target=target, count=32)
                        verify_probe(PROFILES[case], self.baseline_probes[(source, target)], probe, self.inspect(target)['tc']['netem'])
                        self.event('shaped_probe', case=case, probe=probe)
                record.update(injection_issued_ns=issued, injection_confirmed_ns=time.monotonic_ns())
            offered_start = time.monotonic_ns()
            for index in range(self.args.operations):
                leader, term = self.leader()
                record['offered'] += 1
                ack, records = self.put(leader, record['cycle_id'], f'{record["cycle_id"]}-{index}')
                record['acknowledged'] += 1
                self.read_back(oracle.NODES, leader, term, records, ack['index'])
            record.update(offered_started_ns=offered_start, offered_completed_ns=time.monotonic_ns())
            for node in oracle.NODES:
                self.rpc(node, 'heal')
            leader, term = self.leader()
            ack, records = self.put(leader, record['cycle_id'], f'{record["cycle_id"]}-after-heal')
            for node in oracle.NODES:
                self.catch_up(node, records, ack['index'])
            self.read_back(oracle.NODES, leader, term, records, ack['index'])
            record['outcome'] = 'pass'
        except oracle.Inconclusive as error:
            record.update(outcome='inconclusive', error=str(error))
            raise
        except oracle.Deadline as error:
            record.update(outcome='timed_out', error=str(error))
            raise
        except BaseException as error:
            record.update(outcome='failed', error=str(error))
            raise
        finally:
            record['completed_ns'] = time.monotonic_ns()
            self.event('cycle_result', **record)

    def finish(self):
        for node in self.owned_nodes:
            try:
                self.rpc(node, 'cleanup', cleanup=True)
                deadline = time.monotonic() + 5
                while self.inspect(node, cleanup=True)['watchdog']['running']:
                    if time.monotonic() >= deadline:
                        raise oracle.ExperimentError('owned watchdog did not exit after cleanup')
                    time.sleep(0.2)
            except BaseException as error:
                self.errors.append(f'{node} cleanup: {error}')
            try:
                result = self.rpc(node, 'collect', cleanup=True)
                oracle.write_evidence(self.out / f'{node}-evidence.json', json.dumps(result, indent=2) + '\n')
                if result['status']['process']['running'] or result['status']['probe']['running'] or result['status']['watchdog']['running'] or result['status']['shape'] or result['status']['tc']['ifb']:
                    raise oracle.ExperimentError('owned process or network impairment remains')
            except BaseException as error:
                self.errors.append(f'{node} evidence/verification: {error}')
        try:
            records = self.client('ledger') if time.monotonic() < self.experiment_deadline else client.read_ledger(self.out / 'acks.jsonl')
            self.event('final_ledger', records=records)
        except BaseException as error:
            self.errors.append(f'ledger: {error}')

    def run(self):
        failure = None
        cases = CASES if self.args.case == 'all' else (self.args.case,)
        planned = [(case, number) for case in cases for number in range(1, self.args.runs + 1)]
        try:
            self.setup()
            for case, number in planned:
                if case in oracle.FAULTS:
                    self.cycle(case, number)
                else:
                    self.network_case(case, number)
        except BaseException as error:
            failure = f'{type(error).__name__}: {error}'
        finally:
            if self.output_owned:
                self.finish()
        counts = {outcome: sum(row['outcome'] == outcome for row in self.cycles)
                  for outcome in ('pass', 'failed', 'timed_out', 'inconclusive', 'running')}
        complete = ([(row.get('fault'), row.get('cycle_id')) for row in self.cycles]
                    == [(case, f'{case}-{number}') for case, number in planned]
                    and counts['pass'] == len(planned))
        result = dict(schema_version=1, verdict='PASS' if complete and failure is None and not self.errors else 'FAIL',
            failure=failure, cleanup_errors=self.errors, planned=len(planned), attempted=len(self.cycles),
            not_attempted=len(planned) - len(self.cycles), counts=counts, cycles=self.cycles,
            clock_domain=client.CLOCK_DOMAIN, started_ns=self.started_ns, completed_ns=time.monotonic_ns(),
            scope='three VM identities; acknowledged-state oracle with leadership-bracketed local reads; not a linearizability test',
            cloud_teardown='external controller must destroy owned VMs and verify provider inventory; this result does not prove billing stopped')
        if self.output_owned:
            publish_result(self.out, result)
        print(json.dumps(result, indent=2))
        return 0 if result['verdict'] == 'PASS' else 1


def prepare(args):
    topology = validate_topology(client.strict_json(Path(args.topology).read_text()))
    binary = Path(args.binary)
    if hashlib.sha256(binary.read_bytes()).hexdigest() != topology['binary_sha256']:
        raise ValueError('bundle binary hash mismatch')
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    for relative in ('scripts/host_experiment.py', 'scripts/cluster_experiment.py', 'deploy/hosts/host_agent.py', 'deploy/docker/client/client.py'):
        target = out / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    shutil.copyfile(binary, out / 'kv-node')
    (out / 'kv-node').chmod(0o755)
    for node in oracle.NODES:
        (out / f'{node}.json').write_text(json.dumps(agent_config(topology, node), indent=2) + '\n')
    (out / 'topology.json').write_text(json.dumps(topology, indent=2) + '\n')
    manifest = {path.relative_to(out).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in out.rglob('*') if path.is_file()}
    (out / 'SHA256.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    build = sub.add_parser('prepare')
    build.add_argument('--topology', required=True)
    build.add_argument('--binary', required=True)
    build.add_argument('--out', required=True)
    run = sub.add_parser('run')
    run.add_argument('--topology', required=True)
    run.add_argument('--out', required=True)
    run.add_argument('--case', choices=(*CASES, 'all'), default='healthy')
    run.add_argument('--runs', type=oracle.positive, default=1)
    run.add_argument('--operations', type=oracle.positive, default=8)
    run.add_argument('--timeout-seconds', type=oracle.positive, default=30)
    args = parser.parse_args(argv)
    try:
        if args.command == 'prepare':
            return prepare(args)
        if args.runs > 20 or args.operations > 64 or args.timeout_seconds > 120:
            raise ValueError('runs<=20, operations<=64 and phase timeout<=120 required')
        return HostRunner(args).run()
    except (Exception, KeyboardInterrupt) as error:
        print(json.dumps(dict(verdict='FAIL', error=f'{type(error).__name__}: {error}')))
        return 1


if __name__ == '__main__':
    sys.exit(main())
