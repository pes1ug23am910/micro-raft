import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import engine_benchmark as bench


class WorkerDouble:
    instances=[]
    fail_operation=None
    corrupt_scan=False
    mismatch_bytes=False
    response_mode=None
    cleanup_exit=0
    bad_finalization=False

    def __init__(self,*args):
        self.__class__.instances.append(self)
        self.closed=False
        self.process=SimpleNamespace(pid=12345,wait=lambda timeout:0,poll=lambda:0)
        self.argv=['fixture-worker']
        self.ready={'statistics':{'logical_bytes':0}}
        self.state={}
        self.index=0
        self.logical=0
        self.seen=0

    def call(self,request):
        op=request['op']
        if op in ('get','commit'):
            ordinal=self.seen;self.seen+=1
            if ordinal==self.fail_operation:
                raise TimeoutError('planted missing response')
            if self.response_mode=='bad_ack' and op=='commit':
                return {'outcome':'ok','applied_index':99999,'engine_elapsed_ns':1}
            if op=='get':
                value=self.state.get(bytes(request['key']))
                return {'outcome':'ok' if value is not None else 'absent','value':list(value) if value is not None else None,'engine_elapsed_ns':1}
            for change in request['changes']:
                key=bytes(change['key']);value=None if change['value'] is None else bytes(change['value'])
                self.logical+=len(key)+(len(value) if value is not None else 0)
                if value is None:self.state.pop(key,None)
                else:self.state[key]=value
            self.index=request['index']
            return {'outcome':'ok','applied_index':self.index,'engine_elapsed_ns':1}
        if op=='scan':
            rows=[[list(key),list(value)] for key,value in sorted(self.state.items())]
            if self.corrupt_scan:rows=[]
            return {'outcome':'ok','rows':rows,'applied_index':self.index}
        if op=='statistics':return {'statistics':{'logical_bytes':self.logical+int(self.mismatch_bytes)}}
        if op=='exit':return {'outcome':'error' if self.bad_finalization else 'ok','exited':not self.bad_finalization,'statistics':{'logical_bytes':self.logical}}
        raise AssertionError(op)

    def close(self):self.closed=True;return self.cleanup_exit


class BenchmarkTests(unittest.TestCase):
    def setUp(self):
        WorkerDouble.instances=[]
        WorkerDouble.fail_operation=None
        WorkerDouble.corrupt_scan=False
        WorkerDouble.mismatch_bytes=False
        WorkerDouble.response_mode=None
        WorkerDouble.cleanup_exit=0
        WorkerDouble.bad_finalization=False

    def run_fixture(self):
        with tempfile.TemporaryDirectory() as temporary:
            args=SimpleNamespace(worker=Path('fixture'),seed=73)
            plan,rows,index=bench.workload(73,20,8,16)
            with patch.object(bench,'Worker',WorkerDouble):
                result=bench.run_one(args,'lsm',0,plan,rows,index,Path(temporary))
            samples=[json.loads(line) for line in (Path(temporary)/'pair-0-lsm/samples.jsonl').read_text().splitlines()]
            return result,samples

    def test_correct_trace_and_byte_denominator_pass(self):
        result,samples=self.run_fixture()
        self.assertEqual(result['status'],'PASS')
        self.assertEqual(result['attempted'],20)
        self.assertEqual(len(samples),20)
        self.assertTrue(WorkerDouble.instances[-1].closed)

    def test_wrong_ack_is_reached_and_rejected(self):
        WorkerDouble.response_mode='bad_ack'
        result,samples=self.run_fixture()
        self.assertEqual(result['status'],'FAIL')
        self.assertTrue(any(sample.get('response',{}).get('applied_index')==99999 for sample in samples))
        self.assertTrue(WorkerDouble.instances[-1].closed)

    def test_unknown_mutation_and_unattempted_denominator_are_retained(self):
        plan,_,_=bench.workload(73,20,8,16)
        target=next(i for i,item in enumerate(plan) if item['request']['op']=='commit')
        WorkerDouble.fail_operation=target
        result,samples=self.run_fixture()
        self.assertEqual(result['status'],'FAIL')
        self.assertEqual(result['outcomes']['unknown'],1)
        self.assertEqual(result['unattempted'],20-len(samples))
        self.assertTrue(WorkerDouble.instances[-1].closed)

    def test_bad_final_state_is_not_overridden_by_successful_acks(self):
        WorkerDouble.corrupt_scan=True
        result,_=self.run_fixture()
        self.assertEqual(result['attempted'],20)
        self.assertEqual(result['status'],'FAIL')

    def test_incorrect_byte_accounting_fails(self):
        WorkerDouble.mismatch_bytes=True
        result,_=self.run_fixture()
        self.assertEqual(result['status'],'FAIL')
        self.assertIn('logical-byte',result['error'])

    def test_worker_start_failure_retains_cleanup_identity(self):
        cleanup={'owned_pid':777,'exit_code':-9,'running':False,'data_retained':True}
        with tempfile.TemporaryDirectory() as temporary:
            args=SimpleNamespace(worker=Path('fixture'),seed=73)
            with patch.object(bench,'Worker',side_effect=bench.WorkerStartError('bad ready',cleanup)):
                result=bench.run_one(args,'lsm',0,[],[],0,Path(temporary))
            self.assertEqual(result['status'],'FAIL')
            self.assertEqual(result['cleanup'],cleanup)

    def test_cleanup_failure_overrides_successful_workload(self):
        WorkerDouble.cleanup_exit=1
        result,_=self.run_fixture()
        self.assertEqual(result['attempted'],20)
        self.assertEqual(result['status'],'FAIL')
        self.assertIn('cleanup',result['error'])

    def test_missing_finalization_ack_is_not_a_pass(self):
        WorkerDouble.bad_finalization=True
        result,_=self.run_fixture()
        self.assertEqual(result['attempted'],20)
        self.assertEqual(result['status'],'FAIL')
        self.assertIn('finalization',result['error'])


if __name__=='__main__':unittest.main()
