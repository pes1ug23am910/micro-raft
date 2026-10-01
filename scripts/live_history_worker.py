#!/usr/bin/env python3
"""Persistent single-observer recording worker, controlled through bounded JSON lines."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import random
import sys
import threading
import time

import check_history
import record_history as recorder

KEYS = ('history-a', 'history-b')
NODES = ('node1', 'node2', 'node3')


def workload(seed, batches=9):
    if type(seed) is not int or not 0 <= seed < 2**64 or type(batches) is not int or not 1 <= batches <= 16:
        raise ValueError('seed must be u64 and batches must be 1..16')
    randomizer = random.Random(seed)
    result = []
    for index in range(batches):
        key = randomizer.choice(KEYS)
        kind = randomizer.choice(('put', 'put', 'delete'))
        write = dict(id=f'background-{index}-write', node=NODES[index % 3], kind=kind, key=key)
        if kind == 'put':
            write['value'] = f'seed-{seed}-value-{index}'
        reads = [dict(id=f'background-{index}-read-{node}', node=node, kind='get', key=key) for node in NODES]
        result.append([write, *reads])
    return dict(schema_version=1, endpoints={node:f'http://{node}:8100' for node in NODES},
                initial={key:None for key in KEYS}, batches=result)


class Worker:
    def __init__(self, directory, run_id, seed):
        self.directory = Path(directory)
        self.directory.mkdir(exist_ok=False)
        self.run_id = recorder.client._identity(run_id, 'run_id')
        self.workload = workload(seed)
        recorder.validate_workload(self.workload)
        self.journal = recorder.Journal(self.directory / 'journal.jsonl', dict(schema_version=1,
            clock_domain=recorder.client.CLOCK_DOMAIN, initial=self.workload['initial'], run_id=self.run_id,
            endpoints=self.workload['endpoints'], timeout_seconds=2))
        try:
            self.lock = threading.Lock()
            self.identities = set()
            self.background = None
            self.background_error = None
            self.completed_batches = 0
            self.background_started = False
            self.save('workload.json', self.workload)
            self.save('observer.json', dict(schema_version=1, run_id=run_id, seed=seed,
                clock_domain=recorder.client.CLOCK_DOMAIN, python=sys.version,
                files={Path(module.__file__).name:hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
                       for module in (recorder, check_history, recorder.client)}))
        except BaseException:
            self.journal.close()
            raise

    def save(self, name, value):
        with (self.directory / name).open('w', encoding='utf-8') as stream:
            json.dump(value, stream, indent=2, allow_nan=False)
            stream.write('\n');stream.flush()
            recorder.os.fsync(stream.fileno())
        if recorder.os.name == 'posix':
            recorder.client._sync_directory(self.directory)

    def batch(self, operations):
        if not isinstance(operations, list) or not 1 <= len(operations) <= 4:
            raise ValueError('control batch must contain 1..4 attempts')
        spec = dict(self.workload, batches=[operations])
        recorder.validate_workload(spec)
        with self.lock:
            names = {operation['id'] for operation in operations}
            if self.identities.intersection(names) or len(self.identities) + len(names) > 256:
                raise ValueError('duplicate identity or attempt budget exceeded')
            self.identities.update(names)
        with ThreadPoolExecutor(max_workers=4) as pool:
            tasks = [pool.submit(recorder.attempt, operation, self.workload['endpoints'],
                                 self.journal, self.run_id, 2) for operation in operations]
            return [task.result() for task in tasks]

    def start_background(self):
        if self.background_started:
            raise ValueError('background workload can only start once')
        self.background_started = True
        def record():
            try:
                for batch in self.workload['batches']:
                    self.batch(batch)
                    self.completed_batches += 1
                    time.sleep(.05)
            except BaseException as error:
                self.background_error = f'{type(error).__name__}: {error}'
        self.background = threading.Thread(target=record, daemon=True)
        self.background.start()
        return dict(planned_batches=len(self.workload['batches']))

    def join(self):
        if self.background is not None:
            self.background.join(timeout=45)
            if self.background.is_alive():
                raise TimeoutError('bounded background workload did not finish')
        if self.background_error:
            raise RuntimeError(self.background_error)
        return dict(started=self.background_started, completed_batches=self.completed_batches,
                    planned_batches=len(self.workload['batches']))

    def mark(self, label, fault, node):
        if label not in ('fault_issue','fault_confirmed','heal_issue','healed') or fault not in ('kill','pause','partition','restart') or node not in NODES:
            raise ValueError('invalid fault observation')
        value = dict(label=label, fault=fault, node=node, monotonic_ns=time.monotonic_ns(),
                     clock_domain=recorder.client.CLOCK_DOMAIN)
        with (self.directory / 'faults.jsonl').open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(value)+'\n');stream.flush();recorder.os.fsync(stream.fileno())
        return value

    def finish(self):
        background = self.join()
        self.journal.close()
        history = recorder.load_journal(self.directory / 'journal.jsonl')
        result = check_history.check(history, budget=100000, reduce=True)
        self.save('history.json', history)
        self.save('checker.json', result)
        self.save('completion.json', dict(background=background, attempts=len(self.identities),
                                          clock_domain=recorder.client.CLOCK_DOMAIN))
        return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',required=True)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--seed',type=int,required=True)
    args=parser.parse_args(argv)
    worker=None
    try:
        worker=Worker(args.directory,args.run_id,args.seed)
        print(json.dumps(dict(ok=True,ready=True,clock_domain=recorder.client.CLOCK_DOMAIN)),flush=True)
        while True:
            line=sys.stdin.buffer.readline(65537)
            if not line:
                raise RuntimeError('controller disconnected before finish')
            if len(line)>65536 or not line.endswith(b'\n'):
                raise ValueError('control message too large or incomplete')
            request=recorder.client.strict_json(line)
            command=request.get('command')
            if command=='batch': result=worker.batch(request.get('operations'))
            elif command=='background': result=worker.start_background()
            elif command=='join': result=worker.join()
            elif command=='mark': result=worker.mark(request.get('label'),request.get('fault'),request.get('node'))
            elif command=='finish': result=worker.finish()
            else: raise ValueError('unknown worker command')
            print(json.dumps(dict(ok=True,result=result),allow_nan=False),flush=True)
            if command=='finish': return 0
    except BaseException as error:
        print(json.dumps(dict(ok=False,error=f'{type(error).__name__}: {error}')),flush=True)
        return 1
    finally:
        if worker is not None:
            worker.journal.close()


if __name__=='__main__':
    sys.exit(main())
