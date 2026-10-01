#!/usr/bin/env python3
"""Paired local three-process WAL batching measurements, with retained raw evidence.

This measures one host and its filesystem, not multi-host or power-loss behavior.
The exact same executable, unique-key workload and per-append sync contract are
used in both modes. Child processes are deliberately terminated after readback;
only handles spawned by this invocation are touched. Their data/logs are retained.
"""
import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request


def strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def strict_json(body):
    return json.loads(body, object_pairs_hook=strict_object,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def ack(record):
    if record['transport_error'] is not None or record['status'] != 200:
        return False
    try:
        body = strict_json(record['body'])
        return (isinstance(body, dict) and body.get('ok') is True
                and type(body.get('index')) is int and 0 < body['index'] < 2 ** 64)
    except (ValueError, TypeError):
        return False


def request(port, method, path, body=None, timeout=4):
    start = time.monotonic_ns()
    result = dict(status=None, headers={}, body='', transport_error=None, invoke_ns=start)
    try:
        req = urllib.request.Request(f'http://127.0.0.1:{port}{path}', method=method,
                                     data=None if body is None else body.encode('utf-8'))
        try:
            response = urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            result.update(status=response.status, headers=dict((k.lower(), v) for k, v in response.headers.items()),
                          body=response.read(1024 * 1024 + 1).decode('utf-8'))
    except Exception as error:
        result['transport_error'] = f'{type(error).__name__}: {error}'
    result['complete_ns'] = time.monotonic_ns()
    result['duration_ns'] = result['complete_ns'] - start
    return result


def status(port):
    response = request(port, 'GET', '/status', timeout=1)
    if response['status'] != 200 or response['transport_error']:
        return None
    return strict_json(response['body'])


def save(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n', encoding='utf-8')


def reserve_ports():
    listeners = []
    try:
        for _ in range(6):
            item = socket.socket()
            item.bind(('127.0.0.1', 0))
            listeners.append(item)
        return [item.getsockname()[1] for item in listeners]
    finally:
        for item in listeners:
            item.close()


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def phase(port, prefix, count, concurrency):
    operations = [(f'{prefix}-{index:06d}', f'value-{index:06d}-' + 'x' * 128) for index in range(count)]
    deadline = time.monotonic() + 60
    def put(operation):
        key, value = operation
        remaining = deadline - time.monotonic()
        record = (request(port, 'PUT', '/kv/' + key, value, timeout=min(4, remaining)) if remaining > 0
                  else dict(status=None, headers={}, body='', transport_error='phase deadline expired',
                            invoke_ns=time.monotonic_ns(), complete_ns=time.monotonic_ns(), duration_ns=0))
        record.update(key=key, value=value, acknowledged=ack(record))
        return record
    start = time.monotonic_ns()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        records = list(pool.map(put, operations))
    duration = time.monotonic_ns() - start
    latencies = [item['duration_ns'] for item in records]
    accepted = sum(item['acknowledged'] for item in records)
    return dict(name=prefix, concurrency=concurrency, operations=count, duration_ns=duration,
                acknowledged=accepted, errors=count-accepted, throughput_ack_per_s=accepted * 1e9 / duration,
                p50_ns=percentile(latencies, .5), p95_ns=percentile(latencies, .95),
                p99_ns=percentile(latencies, .99), records=records)


def run_case(binary, directory, mode, operations, concurrency, sparse):
    directory.mkdir()
    ports = reserve_ports()
    http = ports[:3]
    raft = ports[3:]
    processes = []
    logs = []
    case = dict(mode=mode, result='FAIL', http_ports=http, raft_ports=raft, commands=[], phases=[], cleanup=[])
    try:
        for index in range(3):
            command = [str(binary), '--id', str(index + 1), '--data-dir', str(directory / f'node-{index+1}'),
                       '--http-port', str(http[index]), '--raft-port', str(raft[index]),
                       '--snapshot-threshold', '0', '--batch-records', '1' if mode == 'single' else '16',
                       '--batch-delay-ms', '0' if mode == 'single' else '2', '--peers',
                       ','.join(f'{peer+1}@127.0.0.1:{raft[peer]}' for peer in range(3) if peer != index)]
            log = (directory / f'node-{index+1}.log').open('wb')
            logs.append(log)
            environment = os.environ.copy()
            environment['RUST_LOG'] = 'warn'
            child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=environment)
            processes.append(child)
            case['commands'].append(dict(argv=command, pid=child.pid))
        deadline = time.monotonic() + 15
        leader = None
        while time.monotonic() < deadline:
            states = [status(port) for port in http]
            leaders = [index for index, state in enumerate(states) if state and state['role'] == 'leader'
                       and state['commit_index'] > 0]
            if len(leaders) == 1 and all(states):
                leader = leaders[0]
                break
            if any(child.poll() is not None for child in processes):
                raise RuntimeError('child exited during startup')
            time.sleep(.05)
        if leader is None:
            raise RuntimeError('no committed leader within startup deadline')
        case['leader_id'] = leader + 1
        case['before'] = states
        for state in states:
            if (state['batching']['record_limit'] != (1 if mode == 'single' else 16)
                    or state['batching']['delay_ms'] != (0 if mode == 'single' else 2)
                    or state['batching']['byte_limit'] != 1024 * 1024):
                raise RuntimeError('observed batch settings differ from requested mode')
        term = states[leader]['term']
        case['phases'].append(phase(http[leader], 'sparse', sparse, 1))
        case['phases'].append(phase(http[leader], 'busy', operations, concurrency))
        records = [record for item in case['phases'] for record in item['records']]
        # Keep every failed response in the artifact; an incomplete workload can
        # never become a valid paired sample through retry or error filtering.
        if not all(item['acknowledged'] for item in records):
            raise RuntimeError('workload contains a missing or invalid acknowledgement')
        max_index = max(strict_json(item['body'])['index'] for item in records)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            after = [status(port) for port in http]
            if all(state and state['last_applied'] >= max_index for state in after):
                break
            time.sleep(.05)
        else:
            raise RuntimeError('acknowledged log did not apply on all replicas')
        case['after'] = after
        if after[leader]['batching']['admitted_records'] - states[leader]['batching']['admitted_records'] != len(records):
            raise RuntimeError('admitted record count differs from exact workload')
        if any(state['batching']['max_batch_records'] > (1 if mode == 'single' else 16)
               or state['batching']['max_batch_command_bytes'] > 1024 * 1024 for state in after):
            raise RuntimeError('observed batch exceeded its configured bound')
        if after[leader]['role'] != 'leader' or after[leader]['term'] != term:
            raise RuntimeError('leadership changed during paired workload')
        for before, after_state in zip(case['before'], after):
            if before['snapshot_threshold'] != 0 or after_state['snapshot_index'] != 0:
                raise RuntimeError('compaction contaminated append sync counts')
        case['wal_append_sync_deltas'] = [after[index]['storage']['wal_append_sync_completed']
                                         - states[index]['storage']['wal_append_sync_completed'] for index in range(3)]
        fence = request(http[leader], 'GET', '/kv/' + records[-1]['key'] + '?consistency=linearizable')
        case['read_fence'] = fence
        if (fence['status'] != 200 or fence['transport_error'] or fence['body'] != records[-1]['value']
                or int(fence['headers'].get('x-raft-read-index', '0')) < max_index
                or int(fence['headers'].get('x-raft-term', '0')) != term):
            raise RuntimeError('readback fence was not confirmed')
        readback_deadline = time.monotonic() + 30
        def verify(record):
            remaining = readback_deadline - time.monotonic()
            if remaining <= 0:
                return dict(key=record['key'], correct=False, error='readback deadline expired')
            observed = request(http[leader], 'GET', '/kv/' + record['key'], timeout=min(4, remaining))
            return dict(key=record['key'], expected=record['value'], response=observed,
                        correct=(observed['status'] == 200 and observed['transport_error'] is None
                                 and observed['body'] == record['value']
                                 and int(observed['headers'].get('x-raft-last-applied', '0')) >= max_index))
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            case['readback'] = list(pool.map(verify, records))
        if not all(item['correct'] for item in case['readback']):
            raise RuntimeError('acknowledged value differs during fenced local readback')
        final = status(http[leader])
        if not final or final['role'] != 'leader' or final['term'] != term:
            raise RuntimeError('readback did not preserve the same leader term')
        case['result'] = 'PASS'
    except Exception as error:
        case['error'] = f'{type(error).__name__}: {error}'
    finally:
        for child in processes:
            try:
                already_exited = child.poll() is not None
                if not already_exited:
                    child.kill()
                code = child.wait(timeout=10)
                case['cleanup'].append(dict(pid=child.pid, already_exited=already_exited,
                                            method='owned-child-hard-termination', exit_code=code))
                if already_exited:
                    case['result'] = 'FAIL'
                    case.setdefault('error', 'child exited before planned cleanup')
            except Exception as error:
                case['cleanup'].append(dict(pid=child.pid, error=f'{type(error).__name__}: {error}'))
                case['result'] = 'FAIL'
        for log in logs:
            log.close()
        save(directory / 'result.json', case)
    return case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--pairs', type=int, default=3)
    parser.add_argument('--operations', type=int, default=256)
    parser.add_argument('--concurrency', type=int, default=32)
    parser.add_argument('--sparse', type=int, default=8)
    args = parser.parse_args()
    if not args.binary.is_file() or not (1 <= args.pairs <= 10 and 1 <= args.operations <= 10000
            and 1 <= args.concurrency <= 128 and 1 <= args.sparse <= 100):
        parser.error('binary missing or measurement bounds invalid')
    args.output.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve()
    workload = dict(operations=args.operations, concurrency=args.concurrency, sparse=args.sparse,
                    unique_keys=True, value_padding=128, snapshot_threshold=0)
    report = dict(schema=1, result='FAIL', started_unix=time.time(), platform=platform.platform(),
                  python=sys.version, binary=str(binary), binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                  cwd=str(Path.cwd()), workload=workload,
                  workload_sha256=hashlib.sha256(json.dumps(workload, sort_keys=True).encode()).hexdigest(),
                  limitations=['one host loopback TCP', 'same local filesystem', 'not physical power loss',
                               'append counters exclude other sync operations', 'debug or release determined by supplied binary'], cases=[])
    try:
        for pair in range(args.pairs):
            for mode in (('single', 'batch') if pair % 2 == 0 else ('batch', 'single')):
                name = f'pair-{pair+1}-{mode}'
                case = run_case(binary, args.output / name, mode, args.operations, args.concurrency, args.sparse)
                report['cases'].append(dict(name=name, mode=mode, result=case['result'], evidence=f'{name}/result.json'))
                print(f'{name}: {case["result"]}', flush=True)
                if case['result'] != 'PASS':
                    raise RuntimeError(f'{name} failed; partial results retained')
        if len(report['cases']) != args.pairs * 2:
            raise RuntimeError('incomplete paired workload')
        report['result'] = 'PASS'
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
    finally:
        report['finished_unix'] = time.time()
        save(args.output / 'summary.json', report)
    return 0 if report['result'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
