#!/usr/bin/env python3
"""Real three-node checked-read histories beside the owned Compose fault oracle."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import sys
import time

import check_history
import cluster_experiment as cluster
import record_history

ROOT=Path(__file__).resolve().parents[1]
BUNDLE_FILES=('scripts/live_history_worker.py','scripts/record_history.py','scripts/check_history.py',
              'deploy/docker/client/client.py')


def acceptance(history, markers, completion, checker):
    """Require useful successful checked reads, not only an explainable empty prefix."""
    if checker.get('verdict')!='PASS':
        return dict(verdict=checker.get('verdict','INCONCLUSIVE'),reason='history checker did not finish with PASS')
    background=completion.get('background',{})
    if background.get('started') is not True or background.get('completed_batches')!=9 or background.get('planned_batches')!=9:
        return dict(verdict='INCONCLUSIVE',reason='planned background workload incomplete')
    if completion.get('attempts')!=len(history['operations']):
        return dict(verdict='INCONCLUSIVE',reason='recorded attempt count does not match worker completion')
    if completion.get('clock_domain') != history['clock_domain']:
        return dict(verdict='INCONCLUSIVE',reason='worker completion clock differs from journal')
    labels={}
    fault_identity=None
    for marker in markers:
        if (marker.get('clock_domain')!=history['clock_domain'] or marker.get('label') in labels
                or type(marker.get('monotonic_ns')) is not int):
            return dict(verdict='INCONCLUSIVE',reason='invalid or mixed observer fault markers')
        identity=(marker.get('fault'),marker.get('node'))
        if identity[0] not in cluster.FAULTS or identity[1] not in cluster.NODES or (fault_identity is not None and identity!=fault_identity):
            return dict(verdict='INCONCLUSIVE',reason='fault marker identity changed')
        fault_identity=identity
        labels[marker['label']]=marker['monotonic_ns']
    order=('fault_issue','fault_confirmed','heal_issue','healed')
    if set(labels)!=set(order) or [labels[label] for label in order]!=sorted(labels.values()):
        return dict(verdict='INCONCLUSIVE',reason='fault marker sequence incomplete or invalid')
    rows={row['id']:row for row in history['operations']}
    expected={f'background-{index}-{kind}' for index in range(9)
              for kind in ('write', 'read-node1', 'read-node2', 'read-node3')}
    expected.update(('baseline-write','baseline-read','checked-during','checked-after'))
    if set(rows)!=expected or len(rows)!=len(history['operations']):
        return dict(verdict='INCONCLUSIVE',reason='planned operation identities incomplete or duplicated')
    for identity in ('baseline-write','baseline-read','checked-during','checked-after'):
        row=rows.get(identity)
        if not row or row['outcome']!='ok' or row['key']!='history-a' or row['kind']!=('put' if identity=='baseline-write' else 'get'):
            return dict(verdict='INCONCLUSIVE',reason=f'required successful operation absent: {identity}')
        if row['kind']=='get' and row.get('read_mode')!='linearizable':
            return dict(verdict='INCONCLUSIVE',reason='required read has no checked mode')
    for identity,row in rows.items():
        if identity.startswith('background-') and (row.get('complete_ns') is None or not
                labels['fault_confirmed']<=row['invoke_ns']<=row['complete_ns']<=labels['heal_issue']):
            return dict(verdict='INCONCLUSIVE',reason='background attempt outside confirmed held fault')
    during=rows['checked-during']
    if not labels['fault_confirmed']<=during['invoke_ns']<=during['complete_ns']<=labels['heal_issue']:
        return dict(verdict='INCONCLUSIVE',reason='checked read did not complete within confirmed held fault')
    if rows['baseline-read']['complete_ns']>labels['fault_issue'] or rows['checked-after']['invoke_ns']<labels['healed']:
        return dict(verdict='INCONCLUSIVE',reason='baseline/post-heal reads do not bracket the fault')
    return dict(verdict='PASS',attempts=len(rows),successful_reads=checker['successful_reads'],
                outcomes=checker['outcomes'],held_fault_read=True)


class WorkerProxy:
    def __init__(self, runner, directory):
        self.runner=runner
        self.buffer=b''
        self.errors=(runner.out/'history-worker.stderr.log').open('wb')
        argv=['docker','exec','-i',runner.observer,'python',directory+'/scripts/live_history_worker.py',
              '--directory','/ledger/live-history','--run-id',runner.project,'--seed',str(runner.args.seed)]
        self.process=subprocess.Popen(argv,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.errors)
        self.selector=selectors.DefaultSelector()
        self.selector.register(self.process.stdout,selectors.EVENT_READ)
        try:
            ready=self.receive(10)
            if ready.get('ready') is not True or not ready.get('clock_domain','').startswith('linux-monotonic:'):
                raise cluster.ExperimentError('history worker did not establish a Linux observer clock')
            self.clock_domain=ready['clock_domain']
        except BaseException:
            self.close()
            raise

    def receive(self, timeout):
        deadline=time.monotonic()+timeout
        while b'\n' not in self.buffer:
            remaining=deadline-time.monotonic()
            if remaining<=0 or not self.selector.select(remaining):
                raise cluster.Deadline('history worker control deadline exceeded')
            data=os.read(self.process.stdout.fileno(),65536)
            if not data:
                raise cluster.ExperimentError('history worker exited before its complete response')
            self.buffer+=data
            if len(self.buffer)>2*1024*1024:
                raise cluster.ExperimentError('history worker response exceeded limit')
        line,self.buffer=self.buffer.split(b'\n',1)
        result=record_history.client.strict_json(line)
        if not isinstance(result,dict) or result.get('ok') is not True:
            raise cluster.ExperimentError(f'history worker failed: {result.get("error") if isinstance(result,dict) else "invalid response"}')
        return result

    def request(self, command, timeout=10, **fields):
        if self.runner.poll_deadline is not None:
            timeout=min(timeout,max(0,self.runner.poll_deadline-time.monotonic()))
        if timeout<=0:
            raise cluster.Deadline('history control phase deadline reached')
        self.process.stdin.write((json.dumps(dict(command=command,**fields))+'\n').encode())
        self.process.stdin.flush()
        return self.receive(timeout)['result']

    def close(self):
        try:
            if self.process.poll() is None:
                self.process.stdin.close()
                self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.kill();self.process.wait(timeout=3)
        finally:
            self.selector.close()
            self.process.stdout.close()
            self.errors.close()


class HistoryRunner(cluster.Runner):
    def __init__(self,args):
        super().__init__(args)
        self.worker=None
        self.checked_during=False
        self.checked_after=False
        self.history_result=None

    def setup(self):
        super().setup()
        bundle=self.out/'history-tools'
        manifest={}
        for relative in BUNDLE_FILES:
            target=bundle/relative
            target.parent.mkdir(parents=True,exist_ok=True)
            source=ROOT if relative=='scripts/live_history_worker.py' else self.args.sealed_source
            data=(source/relative).read_bytes()
            target.write_bytes(data)
            manifest[relative]=hashlib.sha256(data).hexdigest()
        for module, relative in ((record_history,'scripts/record_history.py'),(check_history,'scripts/check_history.py')):
            if hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()!=manifest[relative]:
                raise cluster.ExperimentError('checker/recorder outside observer differs from sealed helper source')
        # The runtime recorder must use exactly the client sealed into the image.
        observed=self.command(['docker','exec',self.observer,'python','-c',
            'import hashlib;print(hashlib.sha256(open("/app/client.py","rb").read()).hexdigest())'])[1].strip()
        if observed!=manifest['deploy/docker/client/client.py']:
            raise cluster.ExperimentError('history client source does not match immutable observer image')
        cluster.write_evidence(self.out/'history-tools.json',json.dumps(dict(seed=self.args.seed,files=manifest,
            images=self.image_ids,read_mode='linearizable',observer_clock='worker only; fault issue/confirmation/heal are observer bounds'),indent=2)+'\n')
        directory='/tmp/live-history-tools'
        self.command(['docker','exec',self.observer,'python','-c',f'import os;os.mkdir("{directory}")'])
        self.command(['docker','cp',str(bundle)+os.sep+'.',self.observer+':'+directory])
        self.worker=WorkerProxy(self,directory)
        leader,_term=self.leader()
        self.worker.request('batch',operations=[dict(id='baseline-write',kind='put',node=leader,key='history-a',value=f'baseline-{self.args.seed}')])
        self.worker.request('batch',operations=[dict(id='baseline-read',kind='get',node=leader,key='history-a')])

    def cycle(self,fault,number):
        try:
            super().cycle(fault,number)
        except BaseException:
            try:
                self.worker.request('join',timeout=50)
            except BaseException as error:
                self.errors.append(f'background history after cycle failure: {error}')
            raise
        else:
            self.worker.request('join',timeout=50)

    def inject(self,fault,node):
        self.worker.request('mark',label='fault_issue',fault=fault,node=node)
        result=super().inject(fault,node)
        self.worker.request('mark',label='fault_confirmed',fault=fault,node=node)
        self.worker.request('background')
        return result

    def heal(self):
        active=self.active_fault
        if active is not None and self.worker is not None:
            self.worker.request('mark',label='heal_issue',fault=active[0],node=active[1])
        super().heal()
        if active is not None and self.worker is not None:
            self.worker.request('mark',label='healed',fault=active[0],node=active[1])

    def read_back(self,eligible,leader,term,records,fence):
        super().read_back(eligible,leader,term,records,fence)
        if self.active_fault is not None and not self.checked_during:
            self.worker.request('batch',operations=[dict(id='checked-during',kind='get',node=leader,key='history-a')])
            self.checked_during=True
            # Hold the confirmed fault until every planned concurrent attempt is recorded.
            self.worker.request('join',timeout=50)
        elif self.active_fault is None and not self.checked_after:
            self.worker.request('batch',operations=[dict(id='checked-after',kind='get',node=leader,key='history-a')])
            self.checked_after=True

    def finish(self):
        try:
            if self.worker is not None:
                try:
                    self.worker.request('finish',timeout=55)
                    self.worker.process.wait(timeout=5)
                    if self.worker.process.returncode:
                        raise cluster.ExperimentError('history worker final exit was nonzero')
                except BaseException as error:
                    self.errors.append(f'history worker completion: {error}')
                finally:
                    self.worker.close()
                destination=self.out/'live-history'
                try:
                    self.command(['docker','cp',self.observer+':/ledger/live-history',str(destination)],timeout=30)
                    history=record_history.load_journal(destination/'journal.jsonl')
                    checker=check_history.check(history,budget=100000,reduce=True)
                    markers=[record_history.client.strict_json(line) for line in (destination/'faults.jsonl').read_bytes().split(b'\n') if line]
                    completion=record_history.client.strict_json((destination/'completion.json').read_text())
                    self.history_result=acceptance(history,markers,completion,checker)
                    cluster.write_evidence(self.out/'history-acceptance.json',json.dumps(dict(
                        acceptance=self.history_result,checker=checker,seed=self.args.seed,
                        node_image=self.image_ids.get('node'),client_image=self.image_ids.get('client')),indent=2)+'\n')
                    if self.history_result['verdict']!='PASS':
                        self.errors.append('live history acceptance: '+str(self.history_result))
                except BaseException as error:
                    self.errors.append(f'history collection/check: {error}')
        finally:
            # Root cleanup must not issue more worker RPCs after it is closed.
            self.worker=None
            super().finish()


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--compose',type=Path,default=ROOT/'deploy/docker/compose.3.yml')
    parser.add_argument('--project',required=True)
    parser.add_argument('--sealed-source',type=Path,required=True)
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--fault',choices=cluster.FAULTS,required=True)
    parser.add_argument('--seed',type=int,required=True)
    parser.add_argument('--node-image',required=True)
    parser.add_argument('--client-image',required=True)
    parser.add_argument('--timeout-seconds',type=cluster.positive,default=30)
    args=parser.parse_args(argv)
    if not 0<=args.seed<2**64:
        parser.error('--seed must be u64')
    args.mode='faults';args.runs=1
    try:
        return HistoryRunner(args).run()
    except (Exception,KeyboardInterrupt) as error:
        print(json.dumps(dict(verdict='FAIL',error=f'{type(error).__name__}: {error}')))
        return 1


if __name__=='__main__':
    sys.exit(main())
