#!/usr/bin/env python3
"""Matched engine-only JSONL workload; no networked-service performance claim."""
import argparse
import collections
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import queue
import random
import subprocess
import threading
import time


def encoded(value):
    return json.dumps(value, separators=(',', ':'), sort_keys=True).encode()


def workload(seed, operations, key_count, value_bytes):
    rng = random.Random(seed)
    state = {}
    index = 0
    result = []
    for ordinal in range(operations):
        number = rng.randrange(key_count)
        key = f'v/{number:06d}'.encode()
        retry = f'r/{number:06d}'.encode()
        draw = rng.randrange(10)
        if draw < 2:
            request = {'op': 'get', 'key': list(key)}
            expected = {'outcome': 'ok' if key in state else 'absent', 'value': list(state[key]) if key in state else None}
            logical = 0
        else:
            index += 1
            value = None if draw < 4 else bytes(rng.randrange(256) for _ in range(value_bytes))
            cached = encoded({'request': ordinal, 'index': index, 'deleted': value is None,
                              'payload_sha256': hashlib.sha256(value or b'').hexdigest()})
            changes = [{'key': list(key), 'value': list(value) if value is not None else None},
                       {'key': list(retry), 'value': list(cached)}]
            request = {'op': 'commit', 'index': index, 'changes': changes}
            expected = {'outcome': 'ok', 'applied_index': index}
            logical = len(key) + (len(value) if value is not None else 0) + len(retry) + len(cached)
            if value is None:
                state.pop(key, None)
            else:
                state[key] = value
            state[retry] = cached
        result.append({'ordinal': ordinal, 'request': request, 'expected': expected, 'logical_bytes': logical})
    return result, [[list(key), list(value)] for key, value in sorted(state.items())], index


class WorkerStartError(RuntimeError):
    def __init__(self, message, cleanup):
        super().__init__(message)
        self.cleanup = cleanup


class Worker:
    def __init__(self, executable, engine, directory, stderr, options, timeout=15):
        self.timeout = timeout
        self.stderr = open(stderr, 'xb')
        argv = [str(executable), '--engine', engine, '--directory', str(directory),
                '--memtable-bytes', str(options['memtable_bytes']), '--table-bytes', str(options['table_bytes']),
                '--level1-bytes', str(options['level1_bytes'])]
        try:
            self.process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr)
        except BaseException:
            self.stderr.close()
            raise
        self.argv = argv
        self.lines = queue.Queue(maxsize=8)

        def read():
            while True:
                line = self.process.stdout.readline(8 * 1024 * 1024 + 1)
                self.lines.put(line)
                if not line:
                    break
        self.reader = threading.Thread(target=read, daemon=True)
        self.reader.start()
        try:
            self.ready = self.receive()
            if self.ready.get('ready') is not True or self.ready.get('schema_version') != 1:
                raise ValueError('worker did not provide a valid ready record')
        except BaseException as error:
            code = self.close()
            if not isinstance(error, Exception):
                raise
            raise WorkerStartError(str(error), {'owned_pid': self.process.pid, 'exit_code': code,
                'running': self.process.poll() is None, 'data_retained': True}) from error

    def receive(self):
        line = self.lines.get(timeout=self.timeout)
        if not line or len(line) > 8 * 1024 * 1024 or not line.endswith(b'\n'):
            raise ValueError('worker response missing or outside framing bound')
        return json.loads(line)

    def call(self, request):
        self.process.stdin.write(encoded(request) + b'\n')
        self.process.stdin.flush()
        return self.receive()

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        code = self.process.wait(timeout=10)
        self.process.stdin.close()
        self.process.stdout.close()
        self.stderr.close()
        return code


def quantiles(values):
    values = sorted(values)
    if not values:
        return {'count': 0, 'p50_ns': None, 'p95_ns': None, 'p99_ns': None}
    return {'count': len(values), **{f'p{percent}_ns': values[max(0, math.ceil(len(values)*percent/100)-1)] for percent in (50,95,99)}}


def run_one(args, engine, pair, plan, expected_rows, expected_index, output):
    directory = output / f'pair-{pair}-{engine}'
    directory.mkdir()
    options = {'memtable_bytes': 16*1024, 'table_bytes': 8*1024, 'level1_bytes': 64*1024}
    record = {'engine': engine, 'pair': pair, 'status': 'FAIL', 'planned': len(plan), 'attempted': 0,
              'observer_clock': 'one Python perf_counter_ns domain per worker run', 'options': options,
              'outcomes': {}, 'cleanup': None, 'seed': args.seed+pair,
              'scope': 'engine-only, one outstanding operation, JSONL IPC; not replicated throughput or open-loop load'}
    worker = None
    samples = []
    try:
        worker = Worker(args.worker, engine, directory/'data', directory/'stderr.log', options)
        record['worker_argv'] = worker.argv
        record['ready'] = worker.ready
        before = worker.ready['statistics']
        clock = time.perf_counter_ns()
        with (directory/'samples.jsonl').open('xb') as journal:
            for item in plan:
                sample = {'ordinal': item['ordinal'], 'request': item['request'], 'expected': item['expected'],
                          'logical_bytes': item['logical_bytes'], 'start_ns': time.perf_counter_ns()-clock}
                record['attempted'] += 1
                try:
                    response = worker.call(item['request'])
                    sample['response'] = response
                    sample['outcome'] = response.get('outcome', 'invalid')
                except Exception as error:
                    sample['outcome'] = 'unknown' if item['request']['op']=='commit' else 'error'
                    sample['error'] = repr(error)
                sample['end_ns'] = time.perf_counter_ns()-clock
                samples.append(sample)
                journal.write(encoded(sample)+b'\n')
                journal.flush()
                response = sample.get('response', {})
                if any(response.get(key) != value for key,value in item['expected'].items()):
                    raise AssertionError(f'operation {item["ordinal"]} failed or violated reference state')
                if not isinstance(response.get('engine_elapsed_ns'), int) or response['engine_elapsed_ns'] < 0:
                    raise AssertionError('missing engine interval')
            journal.flush(); os.fsync(journal.fileno())
        record['workload_elapsed_ns'] = samples[-1]['end_ns'] if samples else 0
        scan = worker.call({'op':'scan'})
        if scan.get('outcome') != 'ok' or scan.get('rows') != expected_rows or scan.get('applied_index') != expected_index:
            raise AssertionError('final atomic cells/watermark differ from reference')
        after = worker.call({'op':'statistics'})['statistics']
        record['workload_statistics'] = after
        record['workload_counter_delta'] = {key: after[key]-before[key] for key in after if type(after[key]) is int}
        record['logical_bytes'] = sum(item['logical_bytes'] for item in plan)
        if record['workload_counter_delta']['logical_bytes'] != record['logical_bytes']:
            raise AssertionError('logical-byte accounting mismatch')
        finalized = worker.call({'op':'exit'})
        if finalized.get('outcome') != 'ok' or finalized.get('exited') is not True:
            raise AssertionError('worker did not acknowledge durable finalization')
        record['final_statistics'] = finalized['statistics']
        if worker.process.wait(timeout=10) != 0:
            raise AssertionError('worker failed during durable finalization')
        record['status'] = 'PASS'
    except Exception as error:
        record['error'] = repr(error)
        if isinstance(error, WorkerStartError):
            record['cleanup'] = error.cleanup
    finally:
        if worker is not None:
            try:
                record['cleanup'] = {'owned_pid': worker.process.pid, 'exit_code': worker.close(), 'running': worker.process.poll() is None,
                                     'data_retained': True}
                if record['status']=='PASS' and (record['cleanup']['running'] or record['cleanup']['exit_code']!=0):
                    record['status']='FAIL'
                    record['error']='worker finalization or cleanup failed'
            except Exception as error:
                record['status']='FAIL'
                record['cleanup']={'owned_pid':worker.process.pid,'error':repr(error),'running':worker.process.poll() is None,'data_retained':True}
        counts = collections.Counter(sample['outcome'] for sample in samples)
        record['outcomes'] = dict(counts)
        record['unattempted'] = record['planned']-record['attempted']
        record['observer_latency'] = quantiles([sample['end_ns']-sample['start_ns'] for sample in samples if sample['outcome'] in ('ok','absent')])
        record['engine_latency'] = quantiles([sample['response']['engine_elapsed_ns'] for sample in samples
                                              if sample['outcome'] in ('ok','absent') and type(sample.get('response',{}).get('engine_elapsed_ns')) is int])
        record['error_latency'] = quantiles([sample['end_ns']-sample['start_ns'] for sample in samples if sample['outcome'] not in ('ok','absent')])
        elapsed = record.get('workload_elapsed_ns', 0)
        record['achieved_operations_per_second_including_recording'] = len(samples)*1e9/elapsed if elapsed else None
        (directory/'result.json').write_bytes(json.dumps(record,indent=2).encode()+b'\n')
    return record


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--worker',type=Path,required=True)
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--operations',type=int,default=1200)
    parser.add_argument('--pairs',type=int,default=4)
    parser.add_argument('--seed',type=int,default=20261001)
    parser.add_argument('--keys',type=int,default=128)
    parser.add_argument('--value-bytes',type=int,default=256)
    args=parser.parse_args()
    if not 10<=args.operations<=50_000 or not 2<=args.pairs<=10 or not 8<=args.keys<=2048 or not 1<=args.value_bytes<=4096:
        parser.error('workload bounds invalid')
    args.worker=args.worker.resolve(strict=True)
    args.out.mkdir(parents=True,exist_ok=False)
    manifest={'schema_version':1,'worker_sha256':hashlib.sha256(args.worker.read_bytes()).hexdigest(),
              'script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'platform':platform.platform(),'python':platform.python_version(),'parameters':vars(args).copy(),
              'measurement':'closed-loop single worker; observer latency includes IPC; engine latency includes synchronous maintenance; no coordinated-omission correction',
              'bytes':'engine-issued successful writes; disk_bytes is file logical length, not allocated blocks or physical-device writes',
              'quantiles':'nearest rank over successful operations; error latency/counts and unattempted operations retained'}
    manifest['parameters']={key:str(value) if isinstance(value,Path) else value for key,value in manifest['parameters'].items()}
    (args.out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    results=[]
    for pair in range(args.pairs):
        plan,rows,index=workload(args.seed+pair,args.operations,args.keys,args.value_bytes)
        (args.out/f'pair-{pair}-workload.json').write_bytes(encoded(plan)+b'\n')
        for engine in (('lsm','redb') if pair%2==0 else ('redb','lsm')):
            result=run_one(args,engine,pair,plan,rows,index,args.out)
            results.append(result)
            print(json.dumps({'pair':pair,'engine':engine,'status':result['status'],'attempted':result['attempted'],'outcomes':result['outcomes']}),flush=True)
    summary={'status':'PASS' if len(results)==args.pairs*2 and all(result['status']=='PASS' for result in results) else 'FAIL',
             'planned_runs':args.pairs*2,'attempted_runs':len(results),'results':results}
    (args.out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    return 0 if summary['status']=='PASS' else 1

if __name__=='__main__':
    raise SystemExit(main())
