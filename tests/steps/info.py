"""Structural assertions for pgconsul-util info output."""

import json
import time
import hashlib
import io
import os
import tarfile
from pathlib import Path
from datetime import datetime, timezone

import yaml
from kazoo.exceptions import NoNodeError

from behave import then, when, use_step_matcher

from tests.steps import helpers

use_step_matcher('re')


@when('we collect (?P<mode>full|short) JSON info on host "(?P<name>[a-zA-Z0-9_-]+)"')
def collect_info(context, mode, name):
    _invoke_info(context, name, mode=mode)


def _artifact_directory(context, case):
    root = Path(os.environ.get('INFO_ARTIFACTS', 'logs/info'))
    sequence = getattr(context, 'info_sequence', 0) + 1
    context.info_sequence = sequence
    scenario = context.scenario.name.replace(' ', '_').replace('/', '_')
    directory = root / scenario / f'{sequence:03d}-{case}'
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _verify_installed_code(container, directory):
    script = """import hashlib, importlib, json
modules = {}
for name in ('cli', 'info', 'info_db', 'info_output', 'zk', 'zk_client', 'types'):
    module = importlib.import_module('pgconsul.' + name)
    modules[name] = {'path': module.__file__, 'sha256': hashlib.sha256(open(module.__file__, 'rb').read()).hexdigest()}
print(json.dumps(modules))
"""
    result = container.exec_run(['/opt/yandex/pgconsul/bin/python', '-c', script], demux=True)
    stdout, stderr = result.output
    assert result.exit_code == 0, stderr
    modules = json.loads(stdout)
    (directory / 'module-hashes.json').write_text(json.dumps({'image': container.image.id, 'modules': modules}, indent=2))
    for name, module in modules.items():
        expected = hashlib.sha256(Path(f'src/{name}.py').read_bytes()).hexdigest()
        assert module['sha256'] == expected, f'Container executes stale {name}: {module["path"]}'


def _invoke_info(context, name, *, mode='full', serializer='json', config=None, case=None, user=None, launcher=None):
    container = context.containers[helpers.resolve_container_name(context, name)]
    directory = _artifact_directory(context, case or f'{name}-{mode}-{serializer}')
    _verify_installed_code(container, directory)
    command = ['timeout', '--kill-after=1', '12'] + (launcher or ['pgconsul-util'])
    if config is not None:
        command.extend(['-c', config])
    command.append('info')
    if serializer == 'json':
        command.append('--json')
    if mode == 'short':
        command.append('--short')
    started = time.monotonic()
    result = container.exec_run(command, demux=True, user=user)
    stdout, stderr = result.output
    context.info_elapsed = time.monotonic() - started
    context.info_stdout = (stdout or b'').decode()
    context.info_stderr = (stderr or b'').decode()
    context.info_exit_code = result.exit_code
    context.info_host = helpers.container_get_fqdn(container)
    context.info_mode = mode
    (directory / f'stdout.{serializer}').write_text(context.info_stdout)
    (directory / 'stderr.log').write_text(context.info_stderr)
    (directory / 'result.json').write_text(json.dumps({'code': result.exit_code, 'elapsed_seconds': context.info_elapsed, 'command': command}, indent=2))
    assert result.exit_code == 0, context.info_stderr
    assert context.info_elapsed <= 10, f'Info exceeded budget: {context.info_elapsed}'
    document = json.loads(context.info_stdout) if serializer == 'json' else yaml.safe_load(context.info_stdout)
    assert 'sources' not in document
    context.info_document = _selected_fields(document) if mode == 'full' else document
    assert 'diagnostics' not in context.info_document
    assert ('sources' in context.info_document) == (mode == 'full')
    _check_utc_dates(context.info_document)
    children = container.exec_run(['pgrep', '-af', 'multiprocessing.spawn|multiprocessing.resource_tracker'])
    assert children.exit_code == 1, f'Info left workers behind: {children.output}'
    return context.info_document


def _selected_fields(document):
    """Unwrap full output for the existing value and provenance assertions."""
    result, sources = {}, {}
    def origin(source):
        return {'source': source, 'cached': source in ('zk_cache', 'status_db_cache', 'db_cache') if source is not None else None}
    for name, field in document.items():
        if name in ('db', 'zk', 'zk_cache', 'db_cache', 'status_db_cache', 'host'):
            result[name] = field
            continue
        assert set(field) == {'value', 'source'}, (name, field)
        result[name] = field['value']
        source = field['source']
        sources[name] = ({host: {**origin(item), 'context': f'host:{host}'} for host, item in source.items()}
                         if isinstance(source, dict) else origin(source))
    return {**result, 'sources': sources}


def _observations(document):
    """Compare selected values/producers across full JSON and human-readable text."""
    sources = document['sources']
    result = {'diag_host': document['host']}
    fields = {
        'diag_role': ('role',), 'hosts_total': ('hosts_total',), 'hosts_alive': ('hosts_alive',),
        'hosts_streaming': ('hosts_streaming',),
        'replics_info': ('replics_info',), 'primary_fqdn': ('primary_fqdn',),
        'last_change': ('last_change',), 'maintenance': ('maintenance',),
        'replication_state': ('replication_state',), 'streaming': ('streaming',), 'sync_state': ('sync_state',),
        'replay_lag_msec': ('replay_lag_msec',), 'last_failover_time': ('last_failover_time',),
        'last_switchover_time': ('last_switchover_time',),
    }
    for metric, names in fields.items():
        selected = []
        for name in names:
            provenance = sources.get(name)
            if provenance is not None and provenance['source'] is not None:
                item = {'value': document[name], **provenance}
                if metric in ('hosts_streaming', 'replics_info'):
                    item.pop('context', None)
                if metric == 'hosts_alive' and 'alive_hosts' in document:
                    item['alive_hosts'] = document['alive_hosts']
                selected.append(item)
        result[metric] = selected or None
    assert {'resetup_hosts', 'maintenance_status', 'db_alive', 'opened',
            'global_replics_info', 'local_replics_info', 'local_hosts_streaming'}.isdisjoint(document)
    backups = document['basebackups']
    result['basebackups'] = ([{'value': rows, **sources['basebackups'][host]} for host, rows in backups.items()]
                             if backups is not None else None)
    return result


def _check_utc_dates(document):
    values = [document.get(key) for key in ('last_failover_time', 'last_switchover_time')]
    maintenance = document.get('maintenance')
    if isinstance(maintenance, dict):
        values.append(maintenance.get('ts'))
    ssn = document.get('synchronous_standby_names')
    if isinstance(ssn, dict):
        values.extend(pair[1] for pair in ssn.values() if isinstance(pair, (list, tuple)) and len(pair) == 2)
    replicas = document.get('replics_info')
    if isinstance(replicas, list):
        for row in replicas:
            values.extend(row.get(key) for key in ('backend_start_ts', 'reply_time_ms'))
    receiver = document.get('wal_receiver')
    if isinstance(receiver, dict):
        values.append(receiver.get('last_msg_receipt_time_msec'))
    for value in values:
        if value is not None:
            assert isinstance(value, str) and value.endswith('Z'), value
            assert datetime.fromisoformat(value.replace('Z', '+00:00')).utcoffset().total_seconds() == 0


def _check_diagnostic_modes(document, baseline, mode):
    if mode == 'short':
        assert 'diagnostics' not in document
        assert document['host'] == baseline['diag_host']
        assert 'alive' not in document
        expected_db = 'only cache' if any(document[key] == 'available' for key in ('db_cache', 'status_db_cache')) else 'unavailable'
        assert document['db'] in ('available', expected_db)
        for key, metric in (('role', 'diag_role'), ('maintenance', 'maintenance')):
            observations = baseline[metric]
            assert document[key] == (observations[0]['value'] if observations else None)
        assert list(document)[:4] == ['db', 'zk', 'zk_cache', 'db_cache']
    else:
        assert _observations(document) == baseline


@then('short info describes a "(?P<role>primary|replica)"')
def check_short_info(context, role):
    document = context.info_document
    assert document['host'] == context.info_host and document['role'] == role
    assert document['db'] == 'available' and 'alive' not in document
    assert 'diagnostics' not in document
    if role == 'primary':
        assert 'primary' not in document and 'replication_state' in document
        assert 'streaming' not in document
    else:
        assert document['streaming'] is True
        assert 'sync_state' in document and 'replay_lag_msec' in document
        assert document['primary'] != context.info_host
        assert 'replication_state' not in document


def _has_variant(variants, value, source=None):
    return isinstance(variants, list) and any(
        variant['value'] == value and (source is None or variant['source'] == source)
        for variant in variants
    )


@then('info has role "(?P<role>primary|replica)", (?P<total>[0-9]+) members, (?P<alive>[0-9]+) alive and (?P<streaming>[0-9]+) local streaming')
def check_info_cluster(context, role, total, alive, streaming):
    document = context.info_document
    diagnostics = _observations(document)
    assert diagnostics['diag_host'] == context.info_host
    assert _has_variant(diagnostics['diag_role'], role)
    assert diagnostics['diag_role'] == [{'value': role, 'source': 'db', 'cached': False}]
    assert _has_variant(diagnostics['hosts_total'], int(total), 'zk')
    assert _has_variant(diagnostics['hosts_alive'], int(alive), 'zk')
    assert _has_variant(diagnostics['hosts_streaming'], int(streaming))
    assert _has_variant(diagnostics['primary_fqdn'], 'pgconsul_postgresql1_1.pgconsul_pgconsul_net')
    assert 'resetup_hosts' not in document
    assert _has_variant(diagnostics['maintenance'], 'off', 'zk')
    assert document['primary'] == 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'
    assert document['zk_alive'] is True
    if int(total) == int(alive):
        assert all('alive_hosts' not in variant for variant in diagnostics['hosts_alive'])
    assert document['role'] == role
    assert isinstance(document['replics_info'], (dict, list))


@then('short info has no downstream replicas')
def check_empty_short_replicas(context):
    assert context.info_document['replics_info'] == []


@then('full and short info describe their own database on every host')
def check_local_database_on_every_host(context):
    script = """from pgconsul import cli, info_db, read_config
expected = info_db.parse_dsn(read_config('/etc/pgconsul.conf').get('global', 'local_conn_string'))
original = info_db.psycopg2.connect
def local_only(*args, **kwargs):
    assert not args
    for key in ('host', 'hostaddr', 'port', 'dbname', 'user'):
        assert kwargs.get(key) == expected.get(key), 'Info changed the configured local database endpoint'
    return original(**kwargs)
info_db.psycopg2.connect = local_only
if __name__ == '__main__':
    cli.entry()
"""
    for name, container in context.containers.items():
        if not name.startswith('postgresql'):
            continue
        _exec_info(container, ['/opt/yandex/pgconsul/bin/python', '-c',
                               f"from pathlib import Path; Path('/tmp/info-local-only.py').write_text({script!r})"])
        role = _exec_info(container, ['psql', '-d', 'postgres', '-Atc', 'SELECT pg_is_in_recovery()'], user='postgres')
        expected_role = 'replica' if role == 't' else 'primary'
        rows = _exec_info(container, ['psql', '-d', 'postgres', '-Atc',
                                      "SELECT COALESCE(json_agg(client_hostname), '[]'::json) FROM pg_stat_replication "
                                      "WHERE application_name NOT IN ('pg_basebackup', 'pg_receivewal') AND state != 'backup'"], user='postgres')
        children = sorted(name or '' for name in json.loads(rows))
        for mode, serializer in (('full', 'json'), ('short', 'json'), ('full', 'yaml'), ('short', 'yaml')):
            document = _invoke_info(context, name, mode=mode, serializer=serializer, case=f'local-{name}-{mode}-{serializer}',
                                    launcher=['/opt/yandex/pgconsul/bin/python', '/tmp/info-local-only.py'])
            assert document['db'] == 'available' and document['role'] == expected_role
            assert document['host'] == helpers.container_get_fqdn(container)
            assert 'db_targets' not in document and 'primary_info' not in document
            if mode == 'full':
                assert document['sources']['role']['source'] == 'db'
                assert document['sources']['replics_info']['source'] == 'db'
                assert sorted(row['client_hostname'] or '' for row in document['replics_info']) == children
                assert all(not (origin.get('source') or '').startswith('db:') for origin in document['sources'].values())
            else:
                assert len(document['replics_info']) == len(children)


@when('we create an isolated info CLI container')
def create_info_container(context):
    container = context.docker.containers.run(
        os.environ.get('PGCONSUL_IMAGE', 'pgconsul:info-dev'), ['sleep', 'infinity'],
        name='info-cli', hostname='info-cli.example.net', detach=True,
    )
    context.containers['info-cli'] = container


def _fixture_config(container, case, db=None, status=None, *, unreadable=False, zk_hosts='127.0.0.1:1'):
    root = f'info-fixtures/{case}'
    files = {'pgconsul.conf': f'[global]\nworking_dir=/tmp/{root}\nzk_hosts={zk_hosts}\nzk_lockpath_prefix=/pgconsul/postgresql/\niteration_timeout=0.2\nlog_level=error\n'}
    for name, value in (('.pgconsul_db_state.cache', db), ('pgconsul.status', status)):
        if value is not None:
            files[name] = value if isinstance(value, str) else json.dumps(value)
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode='w') as tar:
        for path in ('info-fixtures', root):
            entry = tarfile.TarInfo(path)
            entry.type = tarfile.DIRTYPE
            entry.mode = 0o777
            tar.addfile(entry)
        for name, content in files.items():
            data = content.encode()
            entry = tarfile.TarInfo(f'{root}/{name}')
            entry.mode = 0 if unreadable and name != 'pgconsul.conf' else 0o644
            entry.size = len(data)
            tar.addfile(entry, io.BytesIO(data))
    assert container.put_archive('/tmp', archive.getvalue())
    return f'/tmp/{root}/pgconsul.conf'


@then('info bounds a connected stalled backend and blocked DNS')
def check_client_deadlines(context):
    container = context.containers['info-cli']
    major = helpers.container_get_env(container, 'PG_MAJOR')
    _exec_info(container, [f'/usr/lib/postgresql/{major}/bin/pg_ctl', '-D', f'/var/lib/postgresql/{major}/main',
                           'start', '-w', '-t', '15', '--log=/tmp/info-postgresql.log'], user='postgres')
    config = _fixture_config(container, 'deadlines', db={'role': 'replica'},
                             status={'zk_state': {'maintenance': {'status': 'enable'}}})
    scripts = {
        'stalled-sql': """import os, signal, socket, time
from pgconsul import cli, info_db
original = info_db.InfoPostgres.get_senders
def stalled(self):
    pid = self._query('SELECT pg_catalog.pg_backend_pid()')[0][0]
    with open('/tmp/info-backend.pid', 'w') as stream:
        stream.write(str(pid))
    os.kill(pid, signal.SIGSTOP)
    return original(self)
info_db.InfoPostgres.get_senders = stalled
if os.environ.get('INFO_BLOCK_DNS'):
    def blocked(*args, **kwargs):
        time.sleep(60)
    socket.getaddrinfo = blocked
if __name__ == '__main__':
    cli.entry()
""",
        'blocked-dns': """import socket, time
from pgconsul import cli
def blocked(*args, **kwargs):
    time.sleep(60)
socket.getaddrinfo = blocked
if __name__ == '__main__':
    cli.entry()
""",
    }
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode='w') as tar:
        for name, script in scripts.items():
            data = script.encode()
            entry = tarfile.TarInfo(f'info-{name}.py')
            entry.mode, entry.size = 0o644, len(data)
            tar.addfile(entry, io.BytesIO(data))
    assert container.put_archive('/tmp', archive.getvalue())
    for mode in ('full', 'short'):
        try:
            document = _invoke_info(context, 'info-cli', mode=mode, config=config, case=f'stalled-sql-{mode}',
                                    launcher=['/opt/yandex/pgconsul/bin/python', '/tmp/info-stalled-sql.py'])
            assert document['db'] == 'only cache' and document['role'] == 'primary'
            assert document['maintenance'] == 'on'
            assert context.info_elapsed < 6
            if mode == 'full':
                assert document['sources']['role']['source'] == 'db'
                assert document['wal_receiver'] is None and document['sources']['wal_receiver']['source'] == 'db'
        finally:
            # Resume the real backend after the client's deadline closed its connection.
            _exec_info(container, ['/opt/yandex/pgconsul/bin/python', '-c',
                                   "import os, signal; os.kill(int(open('/tmp/info-backend.pid').read()), signal.SIGCONT)"])
        document = _invoke_info(context, 'info-cli', mode=mode, config=config, case=f'blocked-dns-{mode}',
                                launcher=['/opt/yandex/pgconsul/bin/python', '/tmp/info-blocked-dns.py'])
        assert document['role'] == 'primary' and document['maintenance'] == 'on'
        try:
            document = _invoke_info(context, 'info-cli', mode=mode, config=config, case=f'stalled-local-db-and-zk-dns-{mode}',
                                    launcher=['env', 'INFO_BLOCK_DNS=1', '/opt/yandex/pgconsul/bin/python', '/tmp/info-stalled-sql.py'])
            assert document['db'] == 'only cache' and document['role'] == 'primary'
            assert document['zk'] == 'unavailable' and document['maintenance'] == 'on'
            assert 8 < context.info_elapsed < 10
        finally:
            _exec_info(container, ['/opt/yandex/pgconsul/bin/python', '-c',
                                   "import os, signal; os.kill(int(open('/tmp/info-backend.pid').read()), signal.SIGCONT)"])
    _exec_info(container, [f'/usr/lib/postgresql/{major}/bin/pg_ctl', '-D', f'/var/lib/postgresql/{major}/main',
                           'stop', '-m', 'fast', '-w', '-t', '15'], user='postgres')
    for mode in ('full', 'short'):
        document = _invoke_info(context, 'info-cli', mode=mode, config=config, case=f'blocked-dns-local-db-unavailable-{mode}',
                                launcher=['/opt/yandex/pgconsul/bin/python', '/tmp/info-blocked-dns.py'])
        assert document['db'] == 'only cache' and document['zk'] == 'unavailable'
        assert document['role'] == 'replica' and document['maintenance'] == 'on'




@then('the cached info fixture matrix passes')
def check_cache_fixtures(context):
    container = context.containers['info-cli']
    def row(host='replica.example.net', lag=0):
        return {'client_hostname': host, 'state': 'streaming', 'sync_state': 'quorum', 'replay_lag_msec': lag}
    cases = [
        ('missing', None, None, False),
        ('corrupt', '{broken', '{broken', False),
        ('wrong-root', '[]', '0', False),
        ('db-broken', '{broken', {'db_state': {'role': 'primary'}}, False),
        ('status-broken', {'role': 'primary'}, '{broken', False),
        ('unreadable', {'role': 'primary'}, {'db_state': {'role': 'primary'}}, True),
        ('equal', {'role': 'primary'}, {'db_state': {'role': 'primary'}}, False),
        ('different', {'role': 'primary'}, {'db_state': {'role': 'replica'}}, False),
        ('lag0', {'role': 'primary', 'replics_info': [row()]}, None, False),
        ('lag1500', {'role': 'primary', 'replics_info': [row(lag=1500)]}, None, False),
        ('duplicates', {'role': 'primary', 'replics_info': [row(), row(lag=50), row(None)]}, None, False),
        ('empty', {'role': 'primary', 'replics_info': []}, {'zk_state': {'synchronous_standby_names': {}}}, False),
        ('bad-section', None, {'db_state': [], 'zk_state': {'lock_holder': 'leader.example.net'}}, False),
    ]
    for case, db, status, unreadable in cases:
        config = _fixture_config(container, case, db, status, unreadable=unreadable)
        baseline = None
        for mode, serializer in (('full', 'json'), ('short', 'json'), ('full', 'yaml'), ('short', 'yaml')):
            document = _invoke_info(context, 'info-cli', mode=mode, serializer=serializer, config=config,
                                    case=f'{case}-{mode}-{serializer}', user='postgres')
            diagnostics = (_observations(document) if mode == 'full' else baseline)
            if baseline is None:
                baseline = diagnostics
            _check_diagnostic_modes(document, baseline, mode)
            if mode == 'full':
                assert document['primary'] is None and document['zk_alive'] is None
            if case in ('missing', 'corrupt', 'wrong-root', 'unreadable'):
                assert all(value is None for key, value in diagnostics.items() if key != 'diag_host')
            elif case in ('equal', 'db-broken', 'status-broken'):
                source = 'db_cache' if case == 'status-broken' else 'status_db_cache'
                assert diagnostics['diag_role'] == [{'value': 'primary', 'source': source, 'cached': True}]
            elif case == 'different':
                assert diagnostics['diag_role'] == [
                    {'value': 'replica', 'source': 'status_db_cache', 'cached': True},
                ]
            elif case.startswith('lag') and mode == 'full':
                lag = int(case[3:])
                selected = diagnostics['replics_info'][0]
                assert selected['source'] == 'db_cache' and selected['cached']
                assert len(selected['value']) == 1
                assert selected['value'][0]['client_hostname'] == 'replica.example.net'
                assert selected['value'][0]['replay_lag_msec'] == lag
            elif case == 'duplicates' and mode == 'full':
                assert len(diagnostics['replics_info'][0]['value']) == 3
                assert any(item['client_hostname'] is None for item in diagnostics['replics_info'][0]['value'])
            elif case == 'empty':
                assert diagnostics['hosts_total'] is None
                assert diagnostics['hosts_streaming'] == [{'value': 0, 'source': 'db_cache', 'cached': True}]
                if mode == 'full':
                    assert diagnostics['replics_info'] == [{'value': [], 'source': 'db_cache', 'cached': True}]
            elif case == 'bad-section':
                assert diagnostics['primary_fqdn'] == [{'value': 'leader.example.net', 'source': 'zk_cache', 'cached': True}]
                assert diagnostics['diag_role'] is None
    assert container.exec_run(['pgrep', '-x', 'postgres']).exit_code == 1


def _wait_info(predicate, description, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError(f'Timed out waiting for {description}')


def _exec_info(container, args, *, user=None):
    result = container.exec_run(args, user=user, demux=True)
    assert result.exit_code == 0, f'{args}: {result.output}'
    return (result.output[0] or b'').decode().strip()


def _freeze_info_sources(container, case):
    script = """import configparser, os, shutil, sys
config = configparser.RawConfigParser()
config.read('/etc/pgconsul.conf')
source = config.get('global', 'working_dir')
target = '/tmp/info-frozen-' + sys.argv[1]
os.makedirs(target, exist_ok=True)
for name in ('.pgconsul_db_state.cache', 'pgconsul.status'):
    if os.path.exists(os.path.join(source, name)):
        shutil.copyfile(os.path.join(source, name), os.path.join(target, name))
config.set('global', 'working_dir', target)
path = os.path.join(target, 'pgconsul.conf')
with open(path, 'w') as stream:
    config.write(stream)
print(path)
"""
    return _exec_info(container, ['/opt/yandex/pgconsul/bin/python', '-c', script, case])


@then('the live info resilience matrix passes')
def check_live_info(context):
    containers = context.containers
    primary, replica2, replica3 = (containers[f'postgresql{i}'] for i in (1, 2, 3))
    fqdn = {name: helpers.container_get_fqdn(containers[name]) for name in ('postgresql1', 'postgresql2', 'postgresql3')}
    prefix = '/pgconsul/postgresql/'
    client = helpers.get_zk(context, 'zookeeper1')
    client.start(timeout=10)
    def raw(key):
        try:
            return client.get(prefix + key)[0].decode()
        except NoNodeError:
            return None
    def holder(key='leader'):
        try:
            contenders = client.Lock(prefix + key).contenders()
        except NoNodeError:
            return None
        return contenders[0] if contenders else None
    def alive_names():
        return sorted(host for host in client.get_children(prefix + 'all_hosts') if holder('alive/' + host) is not None)
    def streaming_count():
        value = raw('replics_info')
        return len(json.loads(value)) if value is not None else None
    def counts(document, total, alive, streaming=None):
        diagnostics = _observations(document)
        assert _has_variant(diagnostics['hosts_total'], total, 'zk')
        assert _has_variant(diagnostics['hosts_alive'], alive, 'zk')
        if streaming is not None:
            assert _has_variant(diagnostics['hosts_streaming'], streaming)
        if alive != total:
            assert diagnostics['hosts_alive'][0]['alive_hosts'] == alive_names()
    def stop_db(container):
        _exec_info(container, ['supervisorctl', 'stop', 'pgconsul'])
        major = os.environ.get('PG_MAJOR', '14')
        _exec_info(container, [f'/usr/lib/postgresql/{major}/bin/pg_ctl', '-D', f'/var/lib/postgresql/{major}/main', 'stop', '-m', 'fast', '-w', '-t', '15'], user='postgres')
    def start_db(container):
        major = os.environ.get('PG_MAJOR', '14')
        _exec_info(container, [f'/usr/lib/postgresql/{major}/bin/pg_ctl', '-D', f'/var/lib/postgresql/{major}/main', 'start', '-w', '-t', '15', '--log=/var/log/postgresql/postgresql.log'], user='postgres')
        _exec_info(container, ['supervisorctl', 'start', 'pgconsul'])
    def event(kind, name, stamp):
        utc = datetime.fromtimestamp(float(stamp), timezone.utc).isoformat().replace('+00:00', 'Z')
        for mode, serializer in (('full', 'json'), ('short', 'json'), ('full', 'yaml'), ('short', 'yaml')):
            document = _invoke_info(context, name, mode=mode, serializer=serializer, case=f'R04-{kind}-{mode}-{serializer}')
            if mode == 'full':
                assert _has_variant(_observations(document)['last_change'], f'{kind} at {utc}', 'zk')
                assert _has_variant(_observations(document)['primary_fqdn'], fqdn[name])
            else:
                assert document['role'] == 'primary' and 'primary' not in document
            if mode == 'full' or kind == 'failover':
                assert document[f'last_{kind}_time'] == utc
        role = _exec_info(containers[name], ['psql', '-d', 'postgres', '-Atc', 'SELECT pg_is_in_recovery()'], user='postgres')
        assert role == 'f'
    try:
        _wait_info(lambda: len(alive_names()) == 3 and streaming_count() == 2, 'initial publications')
        counts(_invoke_info(context, 'postgresql1', case='H01-live'), 3, 3, 2)

        # R01: a stopped database member retains membership but loses alive and streaming.
        stop_db(replica3)
        _wait_info(lambda: len(alive_names()) == 2 and streaming_count() == 1, 'stopped replica publication')
        counts(_invoke_info(context, 'postgresql1', case='R01-member-unavailable'), 3, 2, 1)
        start_db(replica3)
        _wait_info(lambda: len(alive_names()) == 3 and streaming_count() == 2, 'replica restoration')

        # R02: stop only the daemon and independently confirm PostgreSQL streaming via SQL.
        _exec_info(replica2, ['supervisorctl', 'stop', 'pgconsul'])
        _wait_info(lambda: len(alive_names()) == 2, 'replica daemon alive-lock release')
        sql_count = _exec_info(primary, ['psql', '-d', 'postgres', '-Atc', 'SELECT count(*) FROM pg_stat_replication WHERE state=\'streaming\''], user='postgres')
        assert sql_count == '2'
        counts(_invoke_info(context, 'postgresql1', case='R02-alive-independent'), 3, 2, 2)
        _exec_info(replica2, ['supervisorctl', 'start', 'pgconsul'])
        _wait_info(lambda: len(alive_names()) == 3, 'replica daemon restoration')

        # R06: compare actual shared maintenance with both serializers.
        for mode, expected in (('enable', 'on'), ('disable', 'off')):
            _exec_info(primary, ['pgconsul-util', 'maintenance', '-m', mode])
            _wait_info(lambda: raw('maintenance') == ('enable' if mode == 'enable' else None), 'maintenance ' + mode)
            for output_mode, serializer in (('full', 'json'), ('short', 'json'), ('full', 'yaml'), ('short', 'yaml')):
                document = _invoke_info(context, 'postgresql1', mode=output_mode, serializer=serializer,
                                        case=f'R06-{mode}-{output_mode}-{serializer}')
                if output_mode == 'short':
                    assert document['maintenance'] == expected
                else:
                    assert _has_variant(_observations(document)['maintenance'], expected, 'zk')

        # R04/R05 use the existing switch and role assertions as independent oracles.
        context.execute_steps('When we do targeted switchover from container "postgresql1" to container "postgresql2"\nThen container "postgresql2" became a primary')
        _wait_info(lambda: holder() == fqdn['postgresql2'] and raw('last_switchover_time') is not None, 'switchover timestamp')
        event('switchover', 'postgresql2', raw('last_switchover_time'))
        context.execute_steps('Then container "postgresql1" is a replica of container "postgresql2"\n'
                              'Then container "postgresql3" is a replica of container "postgresql2"\n'
                              'Then container "postgresql1" is streaming from container "postgresql2"\n'
                              'Then container "postgresql3" is streaming from container "postgresql2"\n'
                              'Then container "postgresql3" is in quorum group')
        new_primary = 'postgresql2'

        # R07: freeze both sources before unavailable and stalled ZooKeeper experiments.
        test_host = containers[new_primary]
        frozen = _freeze_info_sources(test_host, 'R07')
        zk_containers = [containers[f'zookeeper{i}'] for i in (1, 2, 3)]
        client.stop()
        client.close()
        for fault in ('pause', 'stop'):
            affected = []
            try:
                for container in zk_containers:
                    getattr(container, fault)()
                    affected.append(container)
                document = _invoke_info(context, new_primary, config=frozen, case='R07-zk-' + fault)
                assert document['zk_alive'] is True and document['primary'] is None
                assert document['sources']['zk_alive'] == {'source': 'zk_cache', 'cached': True}
                assert _observations(document)['hosts_alive'] is None
                assert _observations(document)['diag_role'] == [{'value': 'primary', 'source': 'db', 'cached': False}]
            finally:
                for container in affected:
                    getattr(container, 'unpause' if fault == 'pause' else 'start')()
            client = helpers.get_zk(context, 'zookeeper1')
            client.start(timeout=20)
            _wait_info(lambda: len(alive_names()) == 3, 'ZK and daemon restoration', timeout=180)
            client.stop()
            client.close()
        client = helpers.get_zk(context, 'zookeeper1')
        client.start(timeout=20)

        # R10: keep OSes available while all database and daemon services are stopped.
        frozen = _freeze_info_sources(test_host, 'R10')
        for container in (primary, replica2, replica3):
            stop_db(container)
        _wait_info(lambda: alive_names() == [], 'all alive-locks released')
        document = _invoke_info(context, new_primary, config=frozen, case='R10-dead-databases-live-zk')
        counts(document, 3, 0)
        assert _observations(document)['diag_role'][0]['cached'] is True
        client.stop()
        client.close()
        try:
            for container in zk_containers:
                container.stop()
            document = _invoke_info(context, new_primary, config=frozen, case='R10-dead-databases-dead-zk')
            assert _observations(document)['hosts_alive'] is None
            assert _observations(document)['diag_role'][0]['cached'] is True
        finally:
            for container in zk_containers:
                container.start()
    finally:
        client.stop()
        client.close()


@then('info reports a completed failover after primary loss')
def check_completed_failover(context):
    context.execute_steps('Then container "postgresql2" is in quorum group\n'
                          'Then container "postgresql2" is streaming from container "postgresql1"\n'
                          'Then container "postgresql3" is streaming from container "postgresql1"')
    context.containers['postgresql1'].stop(timeout=10)
    context.execute_steps('Then we remember which of "postgresql2,postgresql3" became primary as "new_primary" and the other as "new_replica"')
    name = helpers.resolve_container_name(context, 'new_primary')
    client = helpers.get_zk(context, 'zookeeper1')
    try:
        client.start(timeout=10)
        key = '/pgconsul/postgresql/last_failover_time'
        _wait_info(lambda: client.exists(key) is not None, 'completed failover timestamp')
        stamp = float(client.get(key)[0])
        utc = datetime.fromtimestamp(stamp, timezone.utc).isoformat().replace('+00:00', 'Z')
        for mode, serializer in (('full', 'json'), ('short', 'json'), ('full', 'yaml'), ('short', 'yaml')):
            document = _invoke_info(context, name, mode=mode, serializer=serializer, case=f'R05-fresh-failover-{mode}-{serializer}')
            assert document['last_failover_time'] == utc
            if mode == 'full':
                assert _has_variant(_observations(document)['last_change'], f'failover at {utc}', 'zk')
                assert document['primary'] == helpers.container_get_fqdn(context.containers[name])
            else:
                assert document['role'] == 'primary' and 'primary' not in document
    finally:
        client.stop()
        client.close()


@then('info performs only ZooKeeper reads')
def check_readonly_info(context):
    host = context.containers['postgresql1']
    script = """from pgconsul import cli
from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock
def forbidden(*args, **kwargs):
    raise AssertionError('Info attempted a ZooKeeper mutation or lock operation')
for name in ('ensure_path', 'ensure_path_async', 'create', 'create_async', 'set', 'set_async', 'delete', 'delete_async', 'set_acls', 'set_acls_async', 'transaction'):
    setattr(KazooClient, name, forbidden)
Lock.acquire = forbidden
Lock.release = forbidden
if __name__ == '__main__':
    cli.entry()
"""
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode='w') as tar:
        entry = tarfile.TarInfo('info-readonly.py')
        data = script.encode()
        entry.size = len(data)
        entry.mode = 0o644
        tar.addfile(entry, io.BytesIO(data))
    assert host.put_archive('/tmp', archive.getvalue())
    launcher = ['/opt/yandex/pgconsul/bin/python', '/tmp/info-readonly.py']
    document = _invoke_info(context, 'postgresql1', case='readonly-existing-namespace', launcher=launcher)
    assert _has_variant(_observations(document)['hosts_alive'], 1, 'zk')
    config_script = """import configparser
config = configparser.RawConfigParser()
config.read('/etc/pgconsul.conf')
config.set('global', 'zk_lockpath_prefix', '/pgconsul/info-readonly-missing/')
config.set('global', 'working_dir', '/tmp/info-readonly-no-cache')
with open('/tmp/info-readonly.conf', 'w') as stream:
    config.write(stream)
"""
    _exec_info(host, ['/opt/yandex/pgconsul/bin/python', '-c', config_script])
    document = _invoke_info(context, 'postgresql1', case='readonly-missing-namespace', launcher=launcher, config='/tmp/info-readonly.conf')
    assert document['primary'] is None
    assert _observations(document)['hosts_total'] is None
    assert _observations(document)['hosts_alive'] is None
    client = helpers.get_zk(context, 'zookeeper1')
    try:
        client.start(timeout=10)
        assert client.exists('/pgconsul/info-readonly-missing') is None
    finally:
        client.stop()
        client.close()


@then('info serializes a published list with repeated and missing names')
def check_published_replica_names(context):
    host = context.containers['postgresql1']
    client = helpers.get_zk(context, 'zookeeper1')
    prefix = '/pgconsul/info-json-fixture'
    rows = []
    for index, name in enumerate(('replica.example.net', None, 'replica.example.net')):
        rows.append({'pid': index + 1, 'application_name': f'replica_{index}', 'client_hostname': name,
                     'client_addr': f'192.0.2.{index + 1}', 'state': 'streaming', 'primary_location': '0/5000000',
                     'sent_location_diff': 0, 'write_location_diff': 0, 'replay_location_diff': 0,
                     'replay_lag_msec': index, 'backend_start_ts': 1, 'reply_time_ms': 1000, 'sync_state': 'quorum'})
    try:
        client.start(timeout=10)
        client.create(prefix + '/all_hosts/' + helpers.container_get_fqdn(host) + '/replics_info', json.dumps(rows).encode(), makepath=True)
        script = """import configparser
config = configparser.RawConfigParser()
config.read('/etc/pgconsul.conf')
config.set('global', 'zk_lockpath_prefix', '/pgconsul/info-json-fixture/')
config.set('global', 'working_dir', '/tmp/info-json-no-cache')
config.set('global', 'local_conn_string', 'host=127.0.0.1 port=1 dbname=postgres connect_timeout=1')
with open('/tmp/info-json.conf', 'w') as stream:
    config.write(stream)
"""
        _exec_info(host, ['/opt/yandex/pgconsul/bin/python', '-c', script])
        baseline = None
        for mode, serializer in (('full', 'json'), ('short', 'json'), ('full', 'yaml'), ('short', 'yaml')):
            document = _invoke_info(context, 'postgresql1', mode=mode, serializer=serializer, config='/tmp/info-json.conf',
                                    case=f'published-missing-names-{mode}-{serializer}',
                                    launcher=['/opt/yandex/pgconsul/bin/python', '/tmp/info-readonly.py'])
            diagnostics = (_observations(document) if mode == 'full' else baseline)
            if baseline is None:
                baseline = diagnostics
            _check_diagnostic_modes(document, baseline, mode)
            assert _has_variant(diagnostics['hosts_streaming'], 3, 'zk')
            if mode == 'full':
                assert len(diagnostics['replics_info'][0]['value']) == 3
                assert any(item['client_hostname'] is None for item in diagnostics['replics_info'][0]['value'])
    finally:
        client.delete(prefix, recursive=True)
        client.stop()
        client.close()


@then('info renders absolute dates in UTC in all output modes')
def check_utc_fixtures(context):
    host = context.containers['postgresql1']
    client = helpers.get_zk(context, 'zookeeper1')
    prefix = '/pgconsul/info-utc-fixture'
    try:
        client.start(timeout=10)
        for case, stamp in (('fractional', 1700000000.125), ('epoch', 0), ('unknown', None), ('invalid', 'broken')):
            base = prefix + '/' + case
            milliseconds = stamp * 1000 if isinstance(stamp, (int, float)) else stamp
            backend = int(stamp) if isinstance(stamp, (int, float)) else stamp
            replica = {'client_hostname': 'replica.example.net', 'state': 'streaming', 'sync_state': 'quorum',
                       'replay_lag_msec': 1500, 'backend_start_ts': backend, 'reply_time_ms': milliseconds}
            publications = {'last_failover_time': stamp, 'last_switchover_time': stamp, 'maintenance': 'enable',
                            'maintenance/ts': stamp, 'all_hosts/' + helpers.container_get_fqdn(host) + '/replics_info': json.dumps([replica]),
                            'all_hosts/replica.example.net/synchronous_standby_names/value': 'ANY 1 (*)',
                            'all_hosts/replica.example.net/synchronous_standby_names/last_update': stamp}
            for key, value in publications.items():
                if value is not None:
                    client.create(base + '/' + key, str(value).encode(), makepath=True)
            config = _fixture_config(host, 'utc-' + case, db={
                'role': 'replica', 'wal_receiver': {'last_msg_receipt_time_msec': milliseconds}, 'sessions_ratio': 0})
            script = f"""import configparser
config = configparser.RawConfigParser()
config.read('/etc/pgconsul.conf')
config.set('global', 'zk_lockpath_prefix', {base + '/'!r})
config.set('global', 'working_dir', {'/tmp/info-fixtures/utc-' + case!r})
config.set('global', 'local_conn_string', 'host=127.0.0.1 port=1 dbname=postgres connect_timeout=1')
with open({config!r}, 'w') as stream:
    config.write(stream)
"""
            _exec_info(host, ['/opt/yandex/pgconsul/bin/python', '-c', script])
            utc = datetime.fromtimestamp(stamp, timezone.utc).isoformat().replace('+00:00', 'Z') if isinstance(stamp, (int, float)) else None
            backend_utc = datetime.fromtimestamp(backend, timezone.utc).isoformat().replace('+00:00', 'Z') if isinstance(backend, int) else None
            baseline = None
            for mode, serializer in (('full', 'json'), ('short', 'json'), ('full', 'yaml'), ('short', 'yaml')):
                document = _invoke_info(context, 'postgresql1', mode=mode, serializer=serializer, config=config,
                                        case=f'utc-{case}-{mode}-{serializer}',
                                        launcher=['/opt/yandex/pgconsul/bin/python', '/tmp/info-readonly.py'])
                assert document['last_failover_time'] == utc
                assert document['maintenance'] == 'on'
                if baseline is None:
                    baseline = _observations(document)
                _check_diagnostic_modes(document, baseline, mode)
                if mode == 'full':
                    assert document['last_switchover_time'] == utc
                    assert document['synchronous_standby_names']['replica.example.net'][1] == utc
                    assert len(document['replics_info']) == 1
                    row = document['replics_info'][0]
                    assert row['client_hostname'] == 'replica.example.net' and row['replay_lag_msec'] == 1500
                    assert row['backend_start_ts'] == backend_utc and row['reply_time_ms'] == utc
                    assert document['wal_receiver']['last_msg_receipt_time_msec'] == (utc if stamp != 0 else None)
                    assert document['sessions_ratio'] == 0
            if stamp is not None:
                assert client.get(base + '/last_failover_time')[0].decode() == str(stamp)
            cached = json.loads(_exec_info(host, ['cat', '/tmp/info-fixtures/utc-' + case + '/.pgconsul_db_state.cache']))
            assert cached['wal_receiver']['last_msg_receipt_time_msec'] == milliseconds
    finally:
        if client.exists(prefix):
            client.delete(prefix, recursive=True)
        client.stop()
        client.close()


@then('info tolerates one unavailable ZooKeeper server')
def check_zk_server_unavailable(context):
    server = context.containers['zookeeper1']
    server.stop()
    try:
        document = _invoke_info(context, 'postgresql1', case='R07-one-zk-unavailable')
        assert _has_variant(_observations(document)['hosts_alive'], 1, 'zk')
    finally:
        server.start()
