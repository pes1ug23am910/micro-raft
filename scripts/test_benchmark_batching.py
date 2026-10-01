import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

MODULE = Path(__file__).with_name('benchmark_batching.py')
SPEC = importlib.util.spec_from_file_location('benchmark_batching', MODULE)
bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bench)


def response(body='{"ok":true,"index":1}', status=200, error=None):
    return dict(status=status, body=body, transport_error=error, headers={}, duration_ns=10)


class Child:
    next_pid = 1
    def __init__(self, *args, **kwargs):
        self.pid = Child.next_pid
        Child.next_pid += 1
        self.killed = False
    def poll(self):
        return None
    def kill(self):
        self.killed = True
    def wait(self, timeout):
        assert self.killed
        return -9


class BenchmarkTests(unittest.TestCase):
    def test_ack_rejects_ambiguous_or_invalid_success_shapes(self):
        self.assertTrue(bench.ack(response()))
        for body in ['{"ok":true,"index":true}', '{"ok":true,"index":0}',
                     '{"ok":true,"index":-1}', '{"ok":true,"index":18446744073709551616}',
                     '{"ok":1,"index":1}', '{"ok":true,"index":1,"index":2}',
                     '{"ok":true,"index":NaN}', '{"ok":true,"index":1} trailing', '[]']:
            with self.subTest(body=body):
                self.assertFalse(bench.ack(response(body)))
        self.assertFalse(bench.ack(response(status=503)))
        self.assertFalse(bench.ack(response(error='reset after response')))

    def test_phase_retains_every_failed_sample_and_has_no_success_throughput(self):
        with patch.object(bench, 'request', return_value=response(status=503)):
            result = bench.phase(1, 'busy', 5, 2)
        self.assertEqual(len(result['records']), 5)
        self.assertEqual(result['errors'], 5)
        self.assertEqual(result['acknowledged'], 0)
        self.assertEqual(result['throughput_ack_per_s'], 0)

    def test_invalid_ack_fails_case_and_still_reaps_only_its_three_children(self):
        children = []
        def spawn(*args, **kwargs):
            child = Child()
            children.append(child)
            return child
        state = dict(role='leader', commit_index=1, term=1, batching=dict(record_limit=16, delay_ms=2, byte_limit=1024*1024))
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'case'
            with patch.object(bench, 'reserve_ports', return_value=list(range(100, 106))), \
                    patch.object(bench.subprocess, 'Popen', side_effect=spawn), \
                    patch.object(bench, 'status', side_effect=lambda port: state if port == 100 else dict(state, role='follower')), \
                    patch.object(bench, 'request', return_value=response('{"ok":true,"index":false}')):
                result = bench.run_case(Path('node'), output, 'batch', 4, 2, 1)
            self.assertEqual(result['result'], 'FAIL')
            self.assertIn('invalid acknowledgement', result['error'])
            self.assertEqual(len(children), 3)
            self.assertTrue(all(child.killed for child in children))
            self.assertEqual(len(result['cleanup']), 3)
            self.assertEqual(json.loads((output / 'result.json').read_text())['result'], 'FAIL')

    def test_cleanup_failure_invalidates_an_otherwise_valid_sample(self):
        for fail_cleanup in [False, True]:
            with self.subTest(fail_cleanup=fail_cleanup), tempfile.TemporaryDirectory() as root:
                class ControlledChild(Child):
                    def wait(self, timeout):
                        if fail_cleanup:
                            raise TimeoutError('injected reap failure')
                        return super().wait(timeout)
                records = [dict(response(), key=f'key-{index}', value=f'value-{index}',
                                body=json.dumps(dict(ok=True, index=index+2)), acknowledged=True)
                           for index in range(3)]
                calls = [0]
                def observed(port):
                    before = calls[0] < 3
                    calls[0] += 1
                    return dict(role='leader' if port == 100 else 'follower', commit_index=1 if before else 4,
                                last_applied=1 if before else 4, term=1, snapshot_threshold=0, snapshot_index=0,
                                batching=dict(record_limit=16, delay_ms=2, byte_limit=1024*1024,
                                              admitted_records=0 if before else 3, max_batch_records=2,
                                              max_batch_command_bytes=100),
                                storage=dict(wal_append_sync_completed=1 if before else 3))
                def get(port, method, path, *args, **kwargs):
                    key = path.split('/kv/')[1].split('?')[0]
                    value = next(record['value'] for record in records if record['key'] == key)
                    return dict(response(value), headers={'x-raft-read-index': '4', 'x-raft-term': '1',
                                                          'x-raft-last-applied': '4'})
                with patch.object(bench, 'reserve_ports', return_value=list(range(100,106))), \
                        patch.object(bench.subprocess, 'Popen', side_effect=ControlledChild), \
                        patch.object(bench, 'status', side_effect=observed), \
                        patch.object(bench, 'phase', side_effect=[dict(records=records[:1]), dict(records=records[1:])]), \
                        patch.object(bench, 'request', side_effect=get):
                    result = bench.run_case(Path('node'), Path(root)/'case', 'batch', 2, 2, 1)
                self.assertEqual(result['result'], 'FAIL' if fail_cleanup else 'PASS')
                self.assertEqual(len(result['readback']), 3)
                if fail_cleanup:
                    self.assertTrue(all('injected reap failure' in row['error'] for row in result['cleanup']))

    def test_main_incomplete_pair_never_reports_pass(self):
        with tempfile.TemporaryDirectory() as root:
            binary = Path(root) / 'node'
            binary.write_bytes(b'test binary')
            output = Path(root) / 'evidence'
            with patch.object(bench.sys, 'argv', ['benchmark', '--binary', str(binary), '--output', str(output), '--pairs', '2']), \
                    patch.object(bench, 'run_case', side_effect=[{'result': 'PASS'}, {'result': 'FAIL'}]):
                self.assertEqual(bench.main(), 1)
            report = json.loads((output / 'summary.json').read_text())
            self.assertEqual(report['result'], 'FAIL')
            self.assertEqual(len(report['cases']), 2)

    def test_zero_or_excessive_parameters_are_rejected_before_output_creation(self):
        with tempfile.TemporaryDirectory() as root:
            binary = Path(root) / 'node'
            binary.touch()
            output = Path(root) / 'evidence'
            with patch.object(bench.sys, 'argv', ['benchmark', '--binary', str(binary), '--output', str(output), '--pairs', '0']):
                with self.assertRaises(SystemExit):
                    bench.main()
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
