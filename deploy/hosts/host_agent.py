#!/usr/bin/env python3
"""Scoped Linux/systemd node and ingress-netem agent for disposable test VMs.

No provider credentials or cloud lifecycle operations belong in this process.
The operator supplies a reviewed topology and an absolute workload lease.
"""
import argparse
import base64
try:
    import fcntl
except ImportError:
    fcntl = None
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time

BASE = Path('/var/lib/micro-raft-experiments')
MAX_JSON = 65536


def strict_json(text):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ValueError('duplicate JSON field')
            result[key] = value
        return result
    def finite(text):
        value = float(text)
        if not math.isfinite(value):
            raise ValueError('nonfinite JSON number')
        return value
    return json.loads(text, object_pairs_hook=pairs, parse_float=finite,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def private_ip(value):
    address = ipaddress.IPv4Address(value)
    if not any(address in ipaddress.IPv4Network(cidr) for cidr in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16')):
        raise ValueError('test address must be canonical RFC1918 IPv4')
    if str(address) != value:
        raise ValueError('noncanonical test address')
    return value


def validate_config(config):
    if not isinstance(config, dict) or type(config.get('schema_version')) is not int or config['schema_version'] != 1:
        raise ValueError('unsupported configuration schema')
    if not re.fullmatch(r'mr-[a-z0-9-]{5,32}', config.get('run_id', '')):
        raise ValueError('invalid run namespace')
    if config.get('node') not in ('node1', 'node2', 'node3'):
        raise ValueError('invalid node name')
    if type(config.get('id')) is not int or config['id'] != int(config['node'][-1]):
        raise ValueError('node identity mismatch')
    private_ip(config.get('private_ip'))
    peers = config.get('peers')
    if not isinstance(peers, dict) or set(peers) != {'node1', 'node2', 'node3'} - {config['node']}:
        raise ValueError('exactly the other two peers are required')
    if len({config['private_ip'], *(private_ip(ip) for ip in peers.values())}) != 3:
        raise ValueError('peer addresses must be distinct')
    ports = [config.get(key) for key in ('raft_port', 'http_port', 'probe_port')]
    if any(type(port) is not int or not 1024 <= port <= 65535 for port in ports) or len(set(ports)) != 3:
        raise ValueError('three distinct unprivileged ports required')
    binary = config.get('binary')
    if not isinstance(binary, str) or not re.fullmatch(r'/[A-Za-z0-9_./-]+', binary) or '..' in Path(binary).parts:
        raise ValueError('binary needs an absolute path without parent traversal')
    if not re.fullmatch(r'[0-9a-f]{64}', config.get('binary_sha256', '')):
        raise ValueError('binary SHA256 required')
    if type(config.get('expires_unix')) is not int or config['expires_unix'] <= 0:
        raise ValueError('absolute lease expiration required')
    return config


def validate_profile(profile):
    if not isinstance(profile, dict) or set(profile) != {'delay_ms', 'jitter_ms', 'loss_percent'}:
        raise ValueError('profile needs delay_ms, jitter_ms and loss_percent')
    for key, value in profile.items():
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError('profile values must be finite and nonnegative')
    if not profile['jitter_ms'] <= profile['delay_ms'] <= 1000 or profile['loss_percent'] > 100:
        raise ValueError('unsupported impairment range')
    return profile


def shape_commands(interface, ifb, local_ip, peer_ips, raft_port, probe_port, profile, pref):
    validate_profile(profile)
    commands = [['tc', 'qdisc', 'add', 'dev', ifb, 'root', 'handle', '1:', 'netem', 'limit', '1000',
                 'delay', f"{profile['delay_ms']}ms", f"{profile['jitter_ms']}ms",
                 'loss', 'random', f"{profile['loss_percent']}%"]]
    for peer in sorted(peer_ips):
        for protocol, port in (('tcp', raft_port), ('udp', probe_port)):
            for direction in ('src_port', 'dst_port'):
                commands.append(['tc', 'filter', 'add', 'dev', interface, 'ingress', 'protocol', 'ip',
                    'pref', str(pref), 'handle', '1', 'flower', 'src_ip', peer, 'dst_ip', local_ip,
                    'ip_proto', protocol, direction, str(port), 'action', 'mirred', 'egress', 'redirect', 'dev', ifb])
                pref += 1
    return commands


def owns_filter(row, command):
    """Match the exact flower selector and sole redirect, ignoring counters only."""
    value = lambda name: command[command.index(name) + 1]
    options = row.get('options', {})
    direction = 'src_port' if 'src_port' in command else 'dst_port'
    expected_keys = dict(eth_type='ipv4', ip_proto=value('ip_proto'), src_ip=value('src_ip'),
                         dst_ip=value('dst_ip'), **{direction: int(value(direction))})
    actions = options.get('actions', [])
    return (row.get('protocol') == 'ip' and row.get('kind') == 'flower' and row.get('chain', 0) == 0
            and row.get('pref') == int(value('pref')) and options.get('handle') == 1
            and options.get('keys') == expected_keys and len(actions) == 1
            and actions[0].get('kind') == 'mirred' and actions[0].get('mirred_action') == 'redirect'
            and actions[0].get('direction') == 'egress' and actions[0].get('to_dev') == command[-1]
            and actions[0].get('control_action', {}).get('type') == 'stolen')


def verify_netem(rows, profile):
    found = [row for row in rows if row.get('kind') == 'netem' and row.get('handle') == '1:']
    if len(found) != 1:
        raise RuntimeError('owned netem qdisc missing')
    options = found[0].get('options', {})
    # iproute2 JSON reports delays in seconds and random loss as a fraction.
    delay = options.get('delay', {})
    delay_value = delay.get('delay', 0) if isinstance(delay, dict) else delay
    jitter = delay.get('jitter', 0) if isinstance(delay, dict) else options.get('jitter', 0)
    loss = options.get('loss-random', {}).get('loss', 0)
    if isinstance(loss, dict):
        loss = loss.get('probability', loss.get('loss', 0))
    for actual, expected in ((delay_value, profile['delay_ms'] / 1000),
                             (jitter, profile['jitter_ms'] / 1000), (loss, profile['loss_percent'] / 100)):
        if type(actual) not in (float, int) or not math.isclose(actual, expected, rel_tol=0.0001, abs_tol=0.000001):
            raise RuntimeError('netem readback does not match requested profile')
    return found[0]


class Agent:
    def __init__(self, config_path, command=None, base=BASE):
        self.config_path = Path(config_path).resolve()
        self.config = validate_config(strict_json(self.config_path.read_text(encoding='utf-8')))
        self.root = base / self.config['run_id'] / self.config['node']
        self.fingerprint = hashlib.sha256(json.dumps(self.config, sort_keys=True).encode()).hexdigest()
        self.unit = f"{self.config['run_id']}-{self.config['node']}"
        self.ifb = 'mr' + hashlib.sha256(self.unit.encode()).hexdigest()[:11]
        self.description = 'micro-raft:' + self.unit + ':' + self.fingerprint
        self.pref = 10000 + int(self.fingerprint[:4], 16) % 40000
        self._command = command or self.command

    def command(self, argv, timeout=15, check=True):
        completed = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        if self.root.exists():
            with (self.root / 'commands.jsonl').open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(dict(at_ns=time.monotonic_ns(), argv=argv, exit=completed.returncode,
                    stdout=completed.stdout[-65536:], stderr=completed.stderr[-65536:])) + '\n')
                stream.flush()
                os.fsync(stream.fileno())
        if check and completed.returncode:
            raise RuntimeError(f'command failed ({completed.returncode}): {argv[0:4]}: {completed.stderr[-1024:]}')
        return completed.returncode, completed.stdout

    def json_command(self, argv, check=True):
        code, text = self._command(argv, check=check)
        return strict_json(text) if code == 0 else []

    def save(self, name, value):
        path = self.root / name
        temporary = path.with_suffix(path.suffix + '.pending')
        with temporary.open('w', encoding='utf-8') as stream:
            json.dump(value, stream, sort_keys=True)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def remaining(self):
        seconds = self.config['expires_unix'] - time.time()
        if not 15 < seconds <= 7200:
            raise RuntimeError('lease expired, too short, or longer than two hours')
        return int(seconds)

    def owned(self, token=None):
        saved = strict_json((self.root / 'owner.json').read_text())
        if saved.get('fingerprint') != self.fingerprint or saved.get('config') != self.config or (token is not None and saved.get('owner_token') != token):
            raise RuntimeError('run directory owner/config mismatch')

    def service(self, suffix='node'):
        unit = self.unit + '-' + suffix + '.service'
        _, text = self._command(['systemctl', 'show', unit, '--property=LoadState,Description,MainPID,ActiveState,SubState,ExecMainCode,ExecMainStatus,InvocationID'], check=False)
        result = dict(line.split('=', 1) for line in text.splitlines() if '=' in line)
        if result.get('LoadState') not in (None, 'not-found') and result.get('Description') != self.description + ':' + suffix:
            raise RuntimeError('systemd unit ownership mismatch')
        pid = int(result.get('MainPID', '0'))
        paused, start_ticks = False, None
        if pid:
            try:
                stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
                paused, start_ticks = stat[0] in ('T', 't'), int(stat[19])
            except FileNotFoundError:
                pid = 0
        identity_path = self.root / ('identity-' + suffix + '.json')
        if pid and identity_path.exists():
            expected = strict_json(identity_path.read_text())
            if expected['invocation'] == result.get('InvocationID') and (expected['pid'], expected['start_ticks']) != (pid, start_ticks):
                raise RuntimeError('service PID/start-time identity changed')
        return dict(unit=unit, pid=pid, running=pid > 0, paused=paused, start_ticks=start_ticks, invocation=result.get('InvocationID'),
                    exit_code=int(result.get('ExecMainStatus', '0')), exit_kind=result.get('ExecMainCode'), properties=result)

    def launch(self, suffix, argv):
        before = self.service(suffix)
        self.remaining()
        if before['running']:
            raise RuntimeError('owned service is already running')
        if before['properties'].get('LoadState') == 'loaded':
            # RuntimeMaxSec is not runtime-mutable on supported systemd versions.
            # Retire this verified stopped transient unit, then recreate it with
            # the remaining absolute lease instead of restarting an old duration.
            self.save('previous-' + suffix + '.json', before)
            self._command(['systemctl', 'stop', before['unit']])
            current = self.service(suffix)
            if current['properties'].get('ActiveState') == 'failed':
                self._command(['systemctl', 'reset-failed', before['unit']])
            deadline = time.monotonic() + 2
            while True:
                current = self.service(suffix)
                if current['running']:
                    raise RuntimeError('retired service unexpectedly running')
                if current['properties'].get('LoadState') == 'not-found':
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError('stopped transient service did not unload')
                time.sleep(0.05)
        seconds = self.remaining() + (60 if suffix == 'watchdog' else 0)
        self._command(['systemd-run', '--quiet', '--unit', before['unit'],
            '--description', self.description + ':' + suffix,
            '--property=Type=exec', '--property=RemainAfterExit=yes', '--property=Restart=no',
            '--property=KillMode=control-group', '--property=TimeoutStopSec=8',
            '--property=NoNewPrivileges=yes', '--property=RuntimeMaxSec=' + str(seconds),
            '--property=StandardOutput=append:' + str(self.root / (suffix + '.log')),
            '--property=StandardError=append:' + str(self.root / (suffix + '.log')), *argv])
        after = self.service(suffix)
        if not after['running']:
            raise RuntimeError('service did not start')
        self.save('identity-' + suffix + '.json', after)
        return after

    def node_argv(self):
        cfg = self.config
        raft = f"{cfg['private_ip']}:{cfg['raft_port']}"
        http = f"{cfg['private_ip']}:{cfg['http_port']}"
        return [cfg['binary'], '--id', str(cfg['id']), '--data-dir', str(self.root / 'data'),
                '--raft-listen', raft, '--raft-advertise', raft, '--http-listen', http, '--http-advertise', http,
                '--peers', ','.join(f"{peer[-1]}@{ip}:{cfg['raft_port']}" for peer, ip in sorted(cfg['peers'].items()))]

    def initialize(self, token):
        self.remaining()
        if self.root.exists():
            raise RuntimeError('run directory already exists; use a fresh namespace')
        for suffix in ('node', 'probe', 'watchdog'):
            if self.service(suffix)['properties'].get('LoadState') not in (None, 'not-found'):
                raise RuntimeError('run unit name already exists')
        binary = Path(self.config['binary'])
        if hashlib.sha256(binary.read_bytes()).hexdigest() != self.config['binary_sha256']:
            raise RuntimeError('binary hash mismatch')
        self.root.mkdir(parents=True, exist_ok=False)
        (self.root / 'data').mkdir()
        self.save('owner.json', dict(fingerprint=self.fingerprint, config=self.config, owner_token=token))
        self._command(self.node_argv() + ['--check-config'])
        addresses = self.json_command(['ip', '-j', 'address', 'show'])
        interfaces = [row['ifname'] for row in addresses if any(item.get('local') == self.config['private_ip'] for item in row.get('addr_info', []))]
        if len(interfaces) != 1 or not re.fullmatch(r'[A-Za-z0-9_.-]{1,15}', interfaces[0]):
            raise RuntimeError('private address has no unique safe interface')
        interface = interfaces[0]
        routes = self.json_command(['ip', '-j', 'route', 'show', 'default'])
        if any(row.get('dev') == interface for row in routes):
            raise RuntimeError('private test interface also carries the default route')
        for peer in self.config['peers'].values():
            route = self.json_command(['ip', '-j', 'route', 'get', peer])
            if len(route) != 1 or route[0].get('dev') != interface:
                raise RuntimeError('peer route is not on the declared private interface')
        self.save('network.json', dict(interface=interface, addresses=addresses, default_routes=routes))
        self.launch('watchdog', [sys.executable, str(Path(__file__).resolve()), '--config', str(self.config_path), 'watchdog'])
        self.launch('probe', [sys.executable, str(Path(__file__).resolve()), '--config', str(self.config_path), 'serve'])
        return self.inspect()

    def network(self):
        return strict_json((self.root / 'network.json').read_text())['interface']

    def tc_state(self):
        interface = self.network()
        return dict(qdisc=self.json_command(['tc', '-j', '-s', 'qdisc', 'show', 'dev', interface]),
                    ingress=self.json_command(['tc', '-j', '-s', 'filter', 'show', 'dev', interface, 'ingress']),
                    egress=self.json_command(['tc', '-j', '-s', 'filter', 'show', 'dev', interface, 'egress']),
                    ifb=self.json_command(['ip', '-j', '-d', 'link', 'show', 'dev', self.ifb], check=False),
                    netem=self.json_command(['tc', '-j', '-s', 'qdisc', 'show', 'dev', self.ifb], check=False))

    def network_context(self):
        return dict(boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                    netns=os.readlink('/proc/self/ns/net'))

    def ifb_identity(self, row):
        if (row.get('ifname') != self.ifb or type(row.get('ifindex')) is not int or row['ifindex'] <= 0
                or row.get('linkinfo', {}).get('info_kind') != 'ifb'
                or not re.fullmatch(r'[0-9a-f]{2}(?::[0-9a-f]{2}){5}', row.get('address', ''))):
            raise RuntimeError('IFB device identity is incomplete or unexpected')
        return dict(ifname=row['ifname'], ifindex=row['ifindex'], address=row['address'],
                    kind='ifb', **self.network_context())

    def shape(self, profile, targets):
        self.remaining()
        validate_profile(profile)
        if not isinstance(targets, list) or not targets or len(set(targets)) != len(targets) or not set(targets) <= set(self.config['peers']):
            raise ValueError('shaping targets must be configured peer names')
        self.heal()
        before = self.tc_state()
        if before['ifb']:
            raise RuntimeError('IFB name collision')
        if any(row.get('kind') == 'ingress' for row in before['qdisc']):
            raise RuntimeError('existing ingress qdisc is unsupported; left unchanged')
        if any(self.pref <= int(row.get('pref', 0)) < self.pref + 8 for row in before['ingress']):
            raise RuntimeError('filter preference collision')
        state = dict(interface=self.network(), ifb=self.ifb, profile=profile, targets=targets,
                     created_clsact=not any(row.get('kind') == 'clsact' for row in before['qdisc']), filters=[], ifb_identity=None)
        self.save('shape.json', state)
        self._command(['ip', 'link', 'add', 'name', self.ifb, 'alias', self.description, 'type', 'ifb'])
        created = self.tc_state()['ifb']
        if len(created) != 1 or created[0].get('ifalias') not in (None, '', self.description):
            raise RuntimeError('new IFB ownership is uncertain; retained for reconciliation')
        state['ifb_identity'] = self.ifb_identity(created[0])
        self.save('shape.json', state)
        # Some iproute2/kernel combinations silently ignore alias on creation.
        # An interruption before this verified label remains fail-closed in heal.
        self._command(['ip', 'link', 'set', 'dev', self.ifb, 'alias', self.description])
        labeled = self.tc_state()['ifb']
        if (len(labeled) != 1 or labeled[0].get('ifalias') != self.description
                or self.ifb_identity(labeled[0]) != state['ifb_identity']):
            raise RuntimeError('IFB alias/device readback mismatch; retained for reconciliation')
        self._command(['ip', 'link', 'set', 'dev', self.ifb, 'up'])
        if state['created_clsact']:
            self._command(['tc', 'qdisc', 'add', 'dev', self.network(), 'clsact'])
        commands = shape_commands(self.network(), self.ifb, self.config['private_ip'],
                    [self.config['peers'][node] for node in targets], self.config['raft_port'], self.config['probe_port'], profile, self.pref)
        for command in commands:
            if 'filter' in command:
                # Journal intent before mutation so a timed-out command remains cleanable.
                state['filters'].append(command)
                self.save('shape.json', state)
            self._command(command)
        observed = self.tc_state()
        verify_netem(observed['netem'], profile)
        return dict(requested=state, observed=observed)

    def require_owned_ifb(self, current, state):
        if not current['ifb']:
            return
        if (len(current['ifb']) != 1 or current['ifb'][0].get('ifalias') != self.description
                or self.ifb_identity(current['ifb'][0]) != state.get('ifb_identity')):
            raise RuntimeError('IFB ownership mismatch; not removed')
        qdiscs = current['netem']
        untouched = (not state['filters'] and (not qdiscs or (len(qdiscs) == 1
                     and qdiscs[0].get('kind') == 'noqueue' and qdiscs[0].get('handle') == '0:')))
        if not untouched:
            if len(qdiscs) != 1:
                raise RuntimeError('IFB qdisc ownership mismatch; not removed')
            verify_netem(qdiscs, state['profile'])

    def heal(self):
        path = self.root / 'shape.json'
        if not path.exists():
            return dict(shaped=False)
        state = strict_json(path.read_text())
        if state['ifb'] != self.ifb or state['interface'] != self.network():
            raise RuntimeError('shaping ownership mismatch')
        current = self.tc_state()
        self.require_owned_ifb(current, state)
        for command in state['filters']:
            pref = int(command[command.index('pref') + 1])
            rows = [row for row in current['ingress'] if int(row.get('pref', 0)) == pref]
            if rows:
                detailed = [row for row in rows if row.get('options')]
                if len(detailed) != 1 or not owns_filter(detailed[0], command):
                    raise RuntimeError('filter ownership mismatch; not removed')
                self._command(['tc', 'filter', 'del', 'dev', self.network(), 'ingress', 'protocol', 'ip',
                               'pref', str(pref), 'handle', '1', 'flower'])
        current = self.tc_state()
        self.require_owned_ifb(current, state)
        if state['created_clsact']:
            if current['ingress'] or current['egress']:
                raise RuntimeError('foreign filters appeared; shared clsact left intact')
            if any(row.get('kind') == 'clsact' for row in current['qdisc']):
                self._command(['tc', 'qdisc', 'del', 'dev', self.network(), 'clsact'])
        current = self.tc_state()
        self.require_owned_ifb(current, state)
        if current['ifb']:
            self._command(['ip', 'link', 'del', 'dev', self.ifb])
        if self.tc_state()['ifb']:
            raise RuntimeError('owned IFB survived cleanup')
        path.unlink()
        return dict(shaped=False)

    def signal_node(self, name):
        if name not in ('SIGKILL', 'SIGSTOP', 'SIGCONT'):
            raise ValueError('unsupported node signal')
        service = self.service()
        if not service['running']:
            raise RuntimeError('node process is absent')
        self._command(['systemctl', 'kill', '--kill-whom=main', '--signal=' + name, service['unit']])
        return self.service()

    def graceful(self):
        service = self.service()
        if not service['running'] or service['paused']:
            raise RuntimeError('graceful stop requires a running unpaused node')
        self._command(['systemctl', 'kill', '--kill-whom=main', '--signal=SIGTERM', service['unit']])
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            current = self.service()
            if not current['running']:
                if current['exit_kind'] != '1' or current['exit_code'] != 0:
                    raise RuntimeError('graceful stop has no verified successful exit')
                return current
            time.sleep(0.05)
        raise RuntimeError('graceful stop did not finish within ten seconds')

    def stop(self, suffix='node'):
        service = self.service(suffix)
        if service['paused']:
            self._command(['systemctl', 'kill', '--kill-whom=main', '--signal=SIGCONT', service['unit']])
        if service['properties'].get('LoadState') == 'loaded':
            self._command(['systemctl', 'stop', service['unit']], timeout=12)
        return self.service(suffix)

    def inspect(self):
        service = self.service()
        return dict(schema_version=1, run_id=self.config['run_id'], node=self.config['node'],
            config_sha256=self.fingerprint, binary_sha256=self.config['binary_sha256'], wall_time_unix=time.time(),
            boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            machine_id=Path('/etc/machine-id').read_text().strip(), kernel=os.uname().release,
            cpus=os.cpu_count(), memory=Path('/proc/meminfo').read_text(), expires_unix=self.config['expires_unix'],
            process=service, watchdog=self.service('watchdog'), probe=self.service('probe'), network=strict_json((self.root / 'network.json').read_text()),
            shape=strict_json((self.root / 'shape.json').read_text()) if (self.root / 'shape.json').exists() else None,
            tc=self.tc_state())

    def sample(self):
        service = self.service()
        pid = service['pid']
        result = dict(monotonic_ns=time.monotonic_ns(), process=service, hz=os.sysconf('SC_CLK_TCK'),
                      page_bytes=os.sysconf('SC_PAGE_SIZE'), network=Path('/proc/net/dev').read_text(),
                      diskstats=Path('/proc/diskstats').read_text(), data_bytes=sum(p.stat().st_size for p in (self.root / 'data').glob('*') if p.is_file()))
        if pid:
            try:
                stat = Path(f'/proc/{pid}/stat').read_text()
                observed_start = int(stat.rsplit(')', 1)[1].split()[19])
                io = Path(f'/proc/{pid}/io').read_text()
                after = self.service()
                if (after['pid'], after['invocation'], observed_start) != (pid, service['invocation'], service['start_ticks']):
                    result['process_changed_during_sample'] = True
                else:
                    result.update(stat=stat, io=io)
            except FileNotFoundError:
                result['process_disappeared'] = True
        return result

    def probe(self, target, count=32):
        if target not in self.config['peers'] or type(count) is not int or not 1 <= count <= 128:
            raise ValueError('invalid probe target/count')
        rtts, issued = [], time.monotonic_ns()
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind((self.config['private_ip'], 0))
            sock.settimeout(0.25)
            address = (self.config['peers'][target], self.config['probe_port'])
            for sequence in range(count):
                payload = os.urandom(16) + sequence.to_bytes(4, 'big')
                started = time.monotonic_ns()
                sock.sendto(payload, address)
                deadline = time.monotonic() + 0.25
                while time.monotonic() < deadline:
                    sock.settimeout(max(0.001, deadline - time.monotonic()))
                    try:
                        received, sender = sock.recvfrom(256)
                    except socket.timeout:
                        break
                    if sender == address and received == payload:
                        rtts.append(time.monotonic_ns() - started)
                        break
                time.sleep(0.005)
        return dict(source=self.config['node'], target=target, sent=count, received=len(rtts),
                    rtt_ns=rtts, issued_ns=issued, completed_ns=time.monotonic_ns())

    def cleanup(self, from_watchdog=False):
        errors = []
        for label, action in [('node', lambda: self.stop()), ('probe', lambda: self.stop('probe')), ('netem', self.heal)]:
            try:
                action()
            except Exception as error:
                errors.append(f'{label}: {error}')
        if not errors:
            self.save('done.json', dict(completed_ns=time.monotonic_ns()))
        result = dict(errors=errors, retained_data=str(self.root), process=self.service(), tc=self.tc_state())
        self.save('cleanup.json', result)
        if errors:
            raise RuntimeError('; '.join(errors))
        return result

    def serve(self):
        next_sample = 0
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind((self.config['private_ip'], self.config['probe_port']))
            sock.settimeout(0.2)
            while time.time() < self.config['expires_unix']:
                try:
                    data, address = sock.recvfrom(256)
                    if address[0] in self.config['peers'].values() and len(data) == 20:
                        sock.sendto(data, address)
                except socket.timeout:
                    pass
                if time.monotonic() >= next_sample:
                    with (self.root / 'samples.jsonl').open('a') as stream:
                        stream.write(json.dumps(self.sample()) + '\n')
                    next_sample = time.monotonic() + 1
                if any(p.stat().st_size > 64 * 1024 * 1024 for p in self.root.glob('*.log')):
                    raise RuntimeError('owned service log exceeded 64 MiB')

    def watchdog(self):
        deadline = time.monotonic() + max(0, self.config['expires_unix'] - time.time())
        while time.monotonic() < deadline and not (self.root / 'done.json').exists():
            time.sleep(0.5)
        if not (self.root / 'done.json').exists():
            with (self.root / 'agent.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                self.cleanup(from_watchdog=True)

    def collect(self):
        files, data, total_bytes = {}, {}, 0
        for path in self.root.iterdir():
            if path.is_file() and path.name != 'agent.lock':
                if path.stat().st_size > 16 * 1024 * 1024:
                    raise RuntimeError('evidence file exceeds 16 MiB collection limit; export directly')
                total_bytes += path.stat().st_size
                if total_bytes > 32 * 1024 * 1024:
                    raise RuntimeError('evidence exceeds total collection limit; export directly')
                files[path.name] = path.read_text(encoding='utf-8')
        for path in (self.root / 'data').rglob('*'):
            if path.is_symlink():
                raise RuntimeError('unexpected symlink in owned data directory')
            if path.is_file():
                total_bytes += path.stat().st_size
                if total_bytes > 32 * 1024 * 1024:
                    raise RuntimeError('evidence exceeds total collection limit; export directly')
                raw = path.read_bytes()
                data[path.relative_to(self.root / 'data').as_posix()] = dict(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw), base64=base64.b64encode(raw).decode('ascii'))
        return dict(files=files, data=data, status=self.inspect())

    def dispatch(self, request):
        action = request.get('action')
        token = request.get('owner_token')
        if not isinstance(token, str) or not re.fullmatch(r'[0-9a-f]{32}', token):
            raise ValueError('controller ownership token required')
        if action == 'initialize':
            return self.initialize(token)
        self.owned(token)
        if action == 'inspect': return self.inspect()
        if action == 'start':
            self.remaining()
            return self.launch('node', self.node_argv())
        if action == 'stop': return self.stop()
        if action == 'graceful': return self.graceful()
        if action == 'kill': return self.signal_node('SIGKILL')
        if action == 'pause': return self.signal_node('SIGSTOP')
        if action == 'resume': return self.signal_node('SIGCONT')
        if action == 'shape': return self.shape(request.get('profile'), request.get('targets'))
        if action == 'heal': return self.heal()
        if action == 'probe': return self.probe(request.get('target'), request.get('count', 32))
        if action == 'sample': return self.sample()
        if action == 'cleanup': return self.cleanup()
        if action == 'collect': return self.collect()
        raise ValueError('unsupported agent action')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('mode', nargs='?', choices=('request', 'serve', 'watchdog'), default='request')
    args = parser.parse_args(argv)
    try:
        if fcntl is None or not hasattr(os, 'geteuid') or os.geteuid() != 0:
            raise RuntimeError('agent requires root on a disposable owned test VM')
        agent = Agent(args.config)
        if args.mode == 'serve':
            agent.owned()
            agent.serve()
            return 0
        if args.mode == 'watchdog':
            agent.owned()
            agent.watchdog()
            return 0
        def expired(_signum, _frame):
            raise TimeoutError('agent request exceeded 90 seconds')
        signal.signal(signal.SIGALRM, expired)
        signal.alarm(90)
        raw = sys.stdin.buffer.read(MAX_JSON + 1)
        if len(raw) > MAX_JSON:
            raise ValueError('request exceeds 64 KiB')
        request = strict_json(raw)
        if not isinstance(request, dict):
            raise ValueError('request must be an object')
        if request.get('action') == 'initialize':
            result = agent.dispatch(request)
        else:
            with (agent.root / 'agent.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                result = agent.dispatch(request)
        print(json.dumps(dict(ok=True, result=result)))
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print(json.dumps(dict(ok=False, error=f'{type(error).__name__}: {error}')))
        return 1


if __name__ == '__main__':
    sys.exit(main())
