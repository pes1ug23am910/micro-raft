"""Coverage and uncertainty controls for the live checked-read experiment."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import check_history
import live_history_experiment as experiment
import live_history_worker as worker
import record_history


def fixture():
    rows=[]
    for batch in worker.workload(123)['batches']:
        for operation in batch:
            row=dict(id=operation['id'],kind=operation['kind'],key=operation['key'],invoke_ns=25,complete_ns=35,outcome='unknown')
            if row['kind']=='put': row['value']=operation['value']
            rows.append(row)
    for identity,kind,start,end in (('baseline-write','put',1,2),('baseline-read','get',3,4),
                                   ('checked-during','get',25,30),('checked-after','get',55,60)):
        row=dict(id=identity,kind=kind,key='history-a',value='baseline',invoke_ns=start,complete_ns=end,outcome='ok')
        if kind=='get': row['read_mode']='linearizable'
        rows.append(row)
    history=dict(schema_version=1,clock_domain='one-observer',initial={key:None for key in worker.KEYS},operations=rows)
    markers=[dict(label=label,monotonic_ns=at,clock_domain='one-observer',fault='kill',node='node1')
             for label,at in (('fault_issue',10),('fault_confirmed',20),('heal_issue',40),('healed',50))]
    completion=dict(clock_domain='one-observer',attempts=40,background=dict(started=True,completed_batches=9,planned_batches=9))
    return history,markers,completion


class LiveHistoryTests(unittest.TestCase):
    def test_seeded_workload_is_complete_and_repeatable(self):
        first=worker.workload(123)
        self.assertEqual(first,worker.workload(123))
        self.assertNotEqual(first,worker.workload(124))
        record_history.validate_workload(first)
        rows=[row for batch in first['batches'] for row in batch]
        self.assertEqual(len(rows),36)
        self.assertEqual(len({row['id'] for row in rows}),36)
        self.assertEqual({row['node'] for row in rows},set(worker.NODES))
        self.assertEqual({row['kind'] for row in rows},{'put','get','delete'})
        for seed in (-1,True,2**64):
            with self.assertRaises(ValueError): worker.workload(seed)

    def test_valid_unknowns_remain_visible_in_accepted_history(self):
        history,markers,completion=fixture()
        checker=check_history.check(history)
        result=experiment.acceptance(history,markers,completion,checker)
        self.assertEqual(result['verdict'],'PASS')
        self.assertEqual(result['outcomes'],dict(ok=4,unknown=36,rejected=0))
        self.assertEqual(result['successful_reads'],3)

    def test_no_budget_exhaustion_or_partial_coverage_becomes_pass(self):
        history,markers,completion=fixture()
        for verdict in ('INCONCLUSIVE','FAIL'):
            self.assertEqual(experiment.acceptance(history,markers,completion,dict(verdict=verdict))['verdict'],verdict)
        mutations=(lambda h,m,c:h['operations'].pop(0),
                   lambda h,m,c:m.pop(),
                   lambda h,m,c:m[0].update(clock_domain='other'),
                   lambda h,m,c:m[1].update(fault='pause'),
                   lambda h,m,c:c.update(clock_domain='other'),
                   lambda h,m,c:c['background'].update(started=False),
                   lambda h,m,c:c['background'].update(completed_batches=8))
        for mutate in mutations:
            h,m,c=copy.deepcopy((history,markers,completion));mutate(h,m,c)
            result=experiment.acceptance(h,m,c,check_history.check(h))
            self.assertNotEqual(result['verdict'],'PASS')

    def test_during_read_must_complete_inside_confirmed_fault(self):
        for update in (dict(invoke_ns=19),dict(complete_ns=41),dict(outcome='unknown'),dict(read_mode='local'),dict(kind='put')):
            history,markers,completion=fixture()
            next(row for row in history['operations'] if row['id']=='checked-during').update(update)
            self.assertNotEqual(experiment.acceptance(history,markers,completion,check_history.check(history))['verdict'],'PASS')

    def test_all_background_attempts_must_finish_inside_confirmed_fault(self):
        for update in (dict(invoke_ns=19),dict(complete_ns=41),dict(complete_ns=None)):
            history,markers,completion=fixture()
            history['operations'][0].update(update)
            self.assertNotEqual(experiment.acceptance(history,markers,completion,check_history.check(history))['verdict'],'PASS')

    def test_successful_reads_must_bracket_fault(self):
        for identity,field,value in (('baseline-read','complete_ns',11),('checked-after','invoke_ns',49)):
            history,markers,completion=fixture()
            next(row for row in history['operations'] if row['id']==identity)[field]=value
            self.assertNotEqual(experiment.acceptance(history,markers,completion,check_history.check(history))['verdict'],'PASS')

    def test_impossible_checked_value_is_real_counterexample(self):
        history,markers,completion=fixture()
        next(row for row in history['operations'] if row['id']=='checked-during')['value']='never-proposed'
        checker=check_history.check(history)
        self.assertEqual(checker['verdict'],'FAIL')
        self.assertEqual(experiment.acceptance(history,markers,completion,checker)['verdict'],'FAIL')

    def test_constructor_failure_closes_its_journal(self):
        with tempfile.TemporaryDirectory() as directory:
            opened=[]
            original=record_history.Journal
            def journal(*args,**kwargs):
                instance=original(*args,**kwargs)
                opened.append(instance)
                return instance
            with patch.object(record_history,'Journal',side_effect=journal), patch.object(worker.Worker,'save',side_effect=OSError('metadata sync failed')):
                with self.assertRaisesRegex(OSError,'metadata sync failed'):
                    worker.Worker(Path(directory)/'recording','test-live-history',123)
            self.assertEqual(len(opened),1)
            self.assertTrue(opened[0].stream.closed)

    def test_worker_background_is_finite_and_never_silently_restarts(self):
        with tempfile.TemporaryDirectory() as directory:
            instance=worker.Worker(Path(directory)/'recording','test-live-history',123)
            seen=[]
            try:
                with patch.object(instance,'batch',side_effect=lambda batch:seen.extend(row['id'] for row in batch)):
                    instance.start_background()
                    result=instance.join()
                self.assertEqual(result['completed_batches'],9)
                self.assertEqual(len(seen),36)
                with self.assertRaises(ValueError): instance.start_background()
            finally:
                instance.journal.close()

    def test_worker_errors_do_not_look_like_completed_background(self):
        with tempfile.TemporaryDirectory() as directory:
            instance=worker.Worker(Path(directory)/'recording','test-live-history',123)
            try:
                with patch.object(instance,'batch',side_effect=OSError('journal sync failed')):
                    instance.start_background()
                    with self.assertRaisesRegex(RuntimeError,'journal sync failed'): instance.join()
                self.assertEqual(instance.completed_batches,0)
            finally:
                instance.journal.close()

    def test_real_worker_stdio_retains_empty_prefix_but_acceptance_rejects_it(self):
        with tempfile.TemporaryDirectory() as directory:
            recording=Path(directory)/'recording'
            result=subprocess.run([sys.executable,'-B',worker.__file__,'--directory',str(recording),
                '--run-id','stdio-fixture','--seed','123'],input='{"command":"finish"}\n',text=True,capture_output=True,timeout=10)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            replies=[json.loads(line) for line in result.stdout.splitlines()]
            self.assertTrue(replies[0]['ready'])
            history=record_history.load_journal(recording/'journal.jsonl')
            completion=json.loads((recording/'completion.json').read_text())
            checker=check_history.check(history)
            self.assertEqual(checker['verdict'],'PASS')
            self.assertEqual(experiment.acceptance(history,[],completion,checker)['verdict'],'INCONCLUSIVE')


if __name__=='__main__':
    unittest.main()
