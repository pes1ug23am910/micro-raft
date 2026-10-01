"""Privileged command planning and fail-closed multi-host experiment fixtures."""
import copy
import contextlib
import io
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import host_experiment as experiment
agent = experiment.host_agent


def topology():
    return dict(schema_version=1, run_id='mr-test-12345', expires_unix=int(time.time()) + 600,
        binary='/opt/micro-raft/test/kv-node', binary_sha256='a' * 64,
        raft_port=9100, http_port=8100, probe_port=9101,
        nodes={name: dict(instance_id=f'vm-{i}', private_ip=f'10.23.0.{i}', local=i == 1,
                         ssh_host=f'10.23.0.{i}', ssh_user='ubuntu', ssh_port=22,
                         agent_path='/opt/micro-raft/test/deploy/hosts/host_agent.py',
                         config_path=f'/opt/micro-raft/test/{name}.json')
               for i, name in enumerate(experiment.oracle.NODES, 1)})


def filter_row(command):
    value = lambda name: command[command.index(name) + 1]
    direction = 'src_port' if 'src_port' in command else 'dst_port'
    return dict(protocol='ip', kind='flower', chain=0, pref=int(value('pref')), options=dict(handle=1,
        keys=dict(eth_type='ipv4', ip_proto=value('ip_proto'), src_ip=value('src_ip'), dst_ip=value('dst_ip'), **{direction: int(value(direction))}),
        actions=[dict(kind='mirred', mirred_action='redirect', direction='egress', to_dev=command[-1], control_action=dict(type='stolen'))]))


class HostPlanningTests(unittest.TestCase):
    def test_three_vm_topology_and_exact_peer_map(self):
        plan = experiment.validate_topology(topology())
        config = experiment.agent_config(plan, 'node1')
        self.assertEqual(config['peers'], {'node2': '10.23.0.2', 'node3': '10.23.0.3'})
        self.assertEqual(config['private_ip'], '10.23.0.1')

    def test_reject_nonprivate_duplicate_and_mixed_observer_topology(self):
        for change in (lambda p: p['nodes']['node2'].update(private_ip='127.0.0.1'),
                       lambda p: p['nodes']['node2'].update(private_ip='8.8.8.8'),
                       lambda p: p['nodes']['node2'].update(private_ip='10.23.0.1'),
                       lambda p: p['nodes']['node2'].update(instance_id='vm-1'),
                       lambda p: p['nodes']['node2'].update(local=True),
                       lambda p: p['nodes']['node1'].update(local=False),
                       lambda p: p['nodes']['node2'].update(ssh_host='public.example'),
                       lambda p: p.update(run_id='../outside'),
                       lambda p: p.update(binary='/opt/../other'),
                       lambda p: p.update(http_port=9100)):
            plan = topology(); change(plan)
            with self.assertRaises(ValueError): experiment.validate_topology(plan)

    def test_ssh_requires_known_host_and_no_shell_interpolation(self):
        command = experiment.agent_argv(topology()['nodes']['node2'])
        self.assertIn('StrictHostKeyChecking=yes', command)
        self.assertIn('BatchMode=yes', command)
        self.assertIn('ubuntu@10.23.0.2', command)
        self.assertEqual(command[-1], 'sudo -n python3 -B /opt/micro-raft/test/deploy/hosts/host_agent.py --config /opt/micro-raft/test/node2.json')
        self.assertEqual(experiment.agent_argv(topology()['nodes']['node1'])[0:2], ['sudo', '-n'])

    def test_shape_selectors_never_impair_http_ssh_or_other_hosts(self):
        commands = agent.shape_commands('private0', 'mr-ifb', '10.23.0.1', ['10.23.0.2', '10.23.0.3'],
                                         9100, 9101, experiment.PROFILES['jitter'], 12345)
        self.assertEqual(len(commands), 9)
        for command in commands[1:]:
            self.assertIn('ingress', command)
            self.assertEqual(command[command.index('dst_ip') + 1], '10.23.0.1')
            self.assertIn(command[command.index('src_ip') + 1], ('10.23.0.2', '10.23.0.3'))
            self.assertNotIn('8100', command)
            self.assertNotIn('22', command)
            self.assertEqual(command[-1], 'mr-ifb')

    def test_profile_rejects_invalid_numbers_and_ranges(self):
        for update in (dict(delay_ms=float('nan')), dict(loss_percent=float('inf')), dict(jitter_ms=-1),
                       dict(delay_ms=True), dict(delay_ms=1001), dict(loss_percent=101), dict(jitter_ms=100)):
            profile = dict(experiment.PROFILES['delay'], **update)
            with self.assertRaises(ValueError): agent.validate_profile(profile)

    def test_exact_filter_ownership_rejects_substrings_and_changed_selectors(self):
        command = agent.shape_commands('private0', 'mr-ifb', '10.23.0.1', ['10.23.0.2'],
                                       9100, 9101, experiment.PROFILES['delay'], 12345)[1]
        original = filter_row(command)
        self.assertTrue(agent.owns_filter(original, command))
        for mutate in (lambda row: row['options']['actions'][0].update(to_dev='foreign-mr-ifb'),
                       lambda row: row['options']['actions'][0].update(mirred_action='mirror'),
                       lambda row: row['options']['actions'][0].update(direction='ingress'),
                       lambda row: row['options']['keys'].update(src_ip='10.99.0.1'),
                       lambda row: row['options']['keys'].update(src_port=22),
                       lambda row: row['options'].update(handle=2),
                       lambda row: row.update(pref=12346),
                       lambda row: row['options']['actions'].append(dict(kind='drop'))):
            row = copy.deepcopy(original); mutate(row)
            self.assertFalse(agent.owns_filter(row, command))

    def test_observed_iproute2_json_netem_units(self):
        row = dict(kind='netem', handle='1:', options={'limit':1000, 'delay':{'delay':.025,'jitter':.01}, 'loss-random':{'loss':.1}})
        profile = dict(delay_ms=25, jitter_ms=10, loss_percent=10)
        agent.verify_netem([row], profile)
        for mutate in (lambda x: x['options']['delay'].update(delay=25),
                       lambda x: x['options']['loss-random'].update(loss=10),
                       lambda x: x.update(handle='2:')):
            bad = copy.deepcopy(row); mutate(bad)
            with self.assertRaises(RuntimeError): agent.verify_netem([bad], profile)

    def test_probe_oracle_never_upgrades_missing_effect_to_pass(self):
        baseline = dict(sent=3, received=3, rtt_ns=[1_000_000] * 3)
        counters = [dict(packets=10, drops=1)]
        shaped = dict(sent=3, received=3, rtt_ns=[45_000_000,50_000_000,55_000_000])
        experiment.verify_probe(experiment.PROFILES['jitter'], baseline, shaped, counters)
        for profile, observed, stats in ((experiment.PROFILES['delay'], baseline, counters),
                (experiment.PROFILES['loss'], baseline, counters),
                (experiment.PROFILES['partition'], baseline, counters),
                (experiment.PROFILES['jitter'], shaped, [dict(packets=0,drops=0)])):
            with self.assertRaises(experiment.oracle.ExperimentError):
                experiment.verify_probe(profile, baseline, observed, stats)
        experiment.verify_probe(experiment.PROFILES['partition'], baseline, dict(sent=3,received=0,rtt_ns=[]), counters)

    def shaper_fixture(self, directory, ignore_explicit_alias=False):
        path=Path(directory)/'node.json';path.write_text(json.dumps(experiment.agent_config(topology(),'node1')))
        commands=[];state=dict(ifb=[],qdisc=[],ingress=[],egress=[],netem=[])
        instance=agent.Agent(path,base=Path(directory)/'runs');instance.root.mkdir(parents=True)
        instance.network=lambda:'private0'
        instance.network_context=lambda:dict(boot_id='fixture-boot',netns='net:[1]')
        instance.tc_state=lambda:copy.deepcopy(state)
        instance.save=lambda name,value:(instance.root/name).write_text(json.dumps(value))
        def command(argv,**kwargs):
            commands.append(argv)
            if argv[:3]==['ip','link','add']:
                state['ifb']=[dict(ifname=instance.ifb,ifindex=7,address='02:00:00:00:00:01',linkinfo=dict(info_kind='ifb'))]
            elif argv[:3]==['ip','link','set'] and 'alias' in argv and not ignore_explicit_alias:
                state['ifb'][0]['ifalias']=argv[-1]
            elif argv[:3]==['tc','qdisc','add'] and 'netem' in argv:
                state['netem']=[dict(kind='netem',handle='1:',options=dict(delay=.025,jitter=0))]
            return 0,''
        instance._command=command
        return instance,commands,state

    def test_ifb_creation_ignored_alias_is_explicitly_labeled_before_shaping(self):
        with tempfile.TemporaryDirectory() as directory:
            instance,commands,state=self.shaper_fixture(directory)
            result=instance.shape(experiment.PROFILES['delay'],['node2'])
            self.assertEqual(state['ifb'][0]['ifalias'],instance.description)
            explicit=next(i for i,cmd in enumerate(commands) if cmd[:3]==['ip','link','set'] and 'alias' in cmd)
            traffic=next(i for i,cmd in enumerate(commands) if cmd[0]=='tc')
            self.assertLess(explicit,traffic)
            self.assertEqual(result['requested']['ifb_identity'],dict(ifname=instance.ifb,ifindex=7,address='02:00:00:00:00:01',kind='ifb',boot_id='fixture-boot',netns='net:[1]'))

    def test_missing_explicit_alias_stops_before_any_traffic_and_is_not_deleted(self):
        with tempfile.TemporaryDirectory() as directory:
            instance,commands,state=self.shaper_fixture(directory,True)
            with self.assertRaisesRegex(RuntimeError,'readback mismatch'):
                instance.shape(experiment.PROFILES['delay'],['node2'])
            self.assertFalse(any(cmd[0]=='tc' for cmd in commands))
            before=len(commands)
            with self.assertRaisesRegex(RuntimeError,'ownership mismatch'):instance.heal()
            self.assertEqual(len(commands),before)
            self.assertTrue(state['ifb'])

    def test_same_alias_cannot_hide_replaced_device_or_changed_network_namespace(self):
        for field,value in [('ifindex',8),('address','02:00:00:00:00:02'),('namespace','net:[2]')]:
            with tempfile.TemporaryDirectory() as directory:
                instance,commands,state=self.shaper_fixture(directory)
                instance.shape(experiment.PROFILES['delay'],['node2'])
                if field=='namespace':instance.network_context=lambda:dict(boot_id='fixture-boot',netns=value)
                else:state['ifb'][0][field]=value
                before=len(commands)
                with self.assertRaisesRegex(RuntimeError,'ownership mismatch'):instance.heal()
                self.assertEqual(len(commands),before)

    def test_ifb_replacement_after_first_cleanup_check_is_not_deleted(self):
        with tempfile.TemporaryDirectory() as directory:
            instance,commands,state=self.shaper_fixture(directory)
            instance.shape(experiment.PROFILES['delay'],['node2'])
            reads=0
            def changed():
                nonlocal reads
                reads+=1
                if reads==2:state['ifb'][0]['ifindex']=8
                return copy.deepcopy(state)
            instance.tc_state=changed
            with self.assertRaisesRegex(RuntimeError,'ownership mismatch'):instance.heal()
            self.assertFalse(any(cmd[:3]==['ip','link','del'] for cmd in commands))

    def test_foreign_ifb_qdisc_is_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            instance,commands,state=self.shaper_fixture(directory)
            instance.shape(experiment.PROFILES['delay'],['node2'])
            state['netem'].append(dict(kind='clsact',handle='ffff:'))
            before=len(commands)
            with self.assertRaisesRegex(RuntimeError,'qdisc ownership mismatch'):instance.heal()
            self.assertEqual(len(commands),before)

    def test_wrong_owner_token_fails_before_any_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'node.json'
            config = experiment.agent_config(topology(), 'node1')
            path.write_text(json.dumps(config))
            commands=[]
            instance=agent.Agent(path, command=lambda *args,**kw: commands.append(args), base=Path(directory)/'runs')
            instance.root.mkdir(parents=True)
            (instance.root/'owner.json').write_text(json.dumps(dict(fingerprint=instance.fingerprint,config=config,owner_token='a'*32)))
            with self.assertRaises(RuntimeError): instance.dispatch(dict(action='cleanup',owner_token='b'*32))
            self.assertEqual(commands, [])

    def test_foreign_systemd_unit_is_never_stopped(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'node.json'; path.write_text(json.dumps(experiment.agent_config(topology(),'node1')))
            commands=[]
            def fake(argv,**kw):
                commands.append(argv)
                return 0, 'LoadState=loaded\nDescription=unrelated-service\nMainPID=1234\n'
            instance=agent.Agent(path,command=fake,base=Path(directory)/'runs')
            with self.assertRaises(RuntimeError): instance.stop()
            self.assertEqual(len(commands),1)
            self.assertEqual(commands[0][0:2],['systemctl','show'])

    def test_restart_recreates_stopped_unit_with_remaining_lease(self):
        instance = object.__new__(agent.Agent)
        instance.description = 'owned'
        instance.root = Path('/fixture')
        old = dict(unit='owned.service', running=False,
                   properties=dict(LoadState='loaded', ActiveState='failed'))
        missing = dict(unit='owned.service', running=False, properties=dict(LoadState='not-found'))
        active = dict(unit='owned.service', running=True, properties=dict(LoadState='loaded'))
        commands = []
        instance._command = lambda argv: commands.append(argv)
        with patch.object(instance, 'service', side_effect=[old, old, missing, active]), \
                patch.object(instance, 'remaining', side_effect=[100, 97]), \
                patch.object(instance, 'save') as saved:
            self.assertIs(instance.launch('node', ['/fixture/kv-node']), active)
        self.assertEqual(commands[:2], [['systemctl', 'stop', 'owned.service'],
                                        ['systemctl', 'reset-failed', 'owned.service']])
        self.assertEqual(commands[2][0], 'systemd-run')
        self.assertIn('--property=RuntimeMaxSec=97', commands[2])
        self.assertEqual(saved.call_args_list[0].args, ('previous-node.json', old))
        self.assertFalse(any('restart' in cmd or 'set-property' in cmd for cmd in commands))

    def test_restart_refuses_unit_that_does_not_unload(self):
        instance = object.__new__(agent.Agent)
        old = dict(unit='owned.service', running=False,
                   properties=dict(LoadState='loaded', ActiveState='inactive'))
        commands = []
        instance._command = lambda argv: commands.append(argv)
        with patch.object(instance, 'service', return_value=old), \
                patch.object(instance, 'remaining', return_value=100), \
                patch.object(instance, 'save'), patch.object(agent.time, 'monotonic', side_effect=[0, 3]):
            with self.assertRaisesRegex(RuntimeError, 'did not unload'):
                instance.launch('node', ['/fixture/kv-node'])
        self.assertEqual(commands, [['systemctl', 'stop', 'owned.service']])

    def test_lease_rejects_expired_or_excessive_duration(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'node.json'
            for delta in (-1, 5, 10000):
                plan=topology();plan['expires_unix']=int(time.time())+delta
                path.write_text(json.dumps(experiment.agent_config(plan,'node1')))
                with self.assertRaises(RuntimeError): agent.Agent(path,base=Path(directory)/'runs').remaining()

    def test_controller_phase_budget_and_cleanup_allowance_are_separate(self):
        runner=object.__new__(experiment.HostRunner)
        runner.experiment_deadline=time.monotonic()-1; runner.poll_deadline=None; runner.cleanup_deadline=time.monotonic()+120
        with self.assertRaises(experiment.oracle.Deadline): runner.budget(30)
        self.assertEqual(runner.budget(30,cleanup=True),30)
        runner.cleanup_deadline=time.monotonic()-1
        with self.assertRaises(experiment.oracle.Deadline): runner.budget(30,cleanup=True)

    def test_probe_identity_and_counts_cannot_be_substituted(self):
        original = dict(source='node1', target='node2', sent=2, received=2, rtt_ns=[10,20], issued_ns=1, completed_ns=100)
        experiment.validate_probe(original, 'node1', 'node2', 2)
        for update in (dict(source='node3'), dict(target='node3'), dict(sent=True), dict(received=3),
                       dict(issued_ns=200), dict(rtt_ns=[200,20]), dict(rtt_ns=[True,20])):
            with self.assertRaises(experiment.oracle.ExperimentError):
                experiment.validate_probe(dict(original, **update), 'node1', 'node2', 2)

    def test_directory_sync_failure_invalidates_visible_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory)
            result=dict(verdict='PASS',cleanup_errors=[])
            with patch.object(experiment.client, '_sync_directory', side_effect=OSError('sync failed')):
                experiment.publish_result(out,result)
            self.assertEqual(result['verdict'],'FAIL')
            self.assertEqual(json.loads((out/'result.json').read_text())['verdict'],'FAIL')

    def test_inspection_rejects_guest_clock_skew_before_identity_claim(self):
        runner=object.__new__(experiment.HostRunner)
        runner.rpc=lambda *a,**kw: dict(wall_time_unix=time.time()+60)
        with self.assertRaisesRegex(experiment.oracle.ExperimentError,'UTC clock'):
            runner.inspect('node1')

    def test_different_cycle_cannot_satisfy_planned_denominator(self):
        runner=object.__new__(experiment.HostRunner)
        runner.args=SimpleNamespace(case='healthy',runs=1)
        runner.cycles=[];runner.errors=[];runner.started_ns=0;runner.output_owned=False
        runner.setup=lambda:None
        runner.network_case=lambda *a:runner.cycles.append(dict(fault='loss',cycle_id='loss-1',outcome='pass'))
        with contextlib.redirect_stdout(io.StringIO()) as output:
            code=runner.run()
        self.assertEqual(code,1)
        self.assertEqual(json.loads(output.getvalue())['verdict'],'FAIL')

    def test_bundle_preserves_exact_binary_and_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);binary=root/'binary';binary.write_bytes(b'fixture-not-an-executable')
            plan=topology();plan['binary_sha256']=experiment.hashlib.sha256(binary.read_bytes()).hexdigest()
            topology_path=root/'topology.json';topology_path.write_text(json.dumps(plan))
            out=root/'bundle'
            experiment.prepare(SimpleNamespace(topology=str(topology_path),binary=str(binary),out=str(out)))
            manifest=json.loads((out/'SHA256.json').read_text())
            self.assertEqual(manifest['kv-node'],plan['binary_sha256'])
            self.assertEqual(json.loads((out/'node2.json').read_text())['private_ip'],'10.23.0.2')
            self.assertIn('deploy/docker/client/client.py',manifest)
            with self.assertRaises(FileExistsError): experiment.prepare(SimpleNamespace(topology=str(topology_path),binary=str(binary),out=str(out)))


if __name__=='__main__':
    unittest.main()
