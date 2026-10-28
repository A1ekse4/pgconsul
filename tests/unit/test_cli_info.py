import copy
import json
from configparser import RawConfigParser
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

import src
src.read_config = Mock()
src.init_logging = Mock()
from src import cli
from src.zk import ZookeeperException


HOST = 'local.example.net'
CHILD = 'child.example.net'
UPSTREAM = 'upstream.example.net'
PRIMARY = 'primary.example.net'


@pytest.fixture
def config(tmp_path):
    conf = RawConfigParser()
    conf.read_dict({'global': {'working_dir': str(tmp_path), 'local_conn_string': 'dbname=postgres'}})
    return conf


@pytest.fixture
def info_zk(monkeypatch):
    zk = MagicMock()
    zk.__enter__.return_value = zk
    monkeypatch.setattr(cli, 'create_zk', lambda **_: zk)
    monkeypatch.setattr(cli.helpers, 'get_hostname', lambda: HOST)
    return zk


@pytest.mark.parametrize('short', [False, True])
@pytest.mark.parametrize('live', [False, True])
def test_info_source_priority_and_cached_fallback(monkeypatch, tmp_path, config, info_zk, short, live):
    cache = {'role': 'primary', 'opened': False, 'sessions_ratio': 0, 'replics_info': []}
    (tmp_path / '.pgconsul_db_state.cache').write_text(json.dumps(cache))
    (tmp_path / 'pgconsul.status').write_text(json.dumps({
        'db_state': {'role': 'replica'}, 'zk_state': {'maintenance': {'status': 'enable'}, 'last_failover_time': 0},
    }))
    sql = {'available': True, 'role': 'primary', 'replication_state': ('async', None), 'replics_info': []} if live else {'available': False}
    info_zk.get_state.return_value = {'alive': True, 'lock_holder': HOST, 'maintenance': {'status': None}} if live else None
    if not live:
        info_zk.get_state.side_effect = ZookeeperException('unavailable')
    monkeypatch.setattr(cli, '_get_info_db_state', lambda _: sql)
    report = cli._show_info(SimpleNamespace(short=short), config)
    assert report['db'] == ('available' if live else 'only cache')
    assert report['zk'] == ('available' if live else 'unavailable')
    if short:
        assert report['role'] == ('primary' if live else 'replica')
        assert report['downstream_replicas'] == []
        assert report['maintenance'] == ('off' if live else 'on')
        if live:
            assert report['primary'] == HOST
            assert 'upstream_host' not in report
            assert report['downstream_replication_mode'] == 'async'
    else:
        assert report['role'] == {'value': 'primary' if live else 'replica', 'source': 'db' if live else 'status_db_cache'}
        assert report['maintenance'] == {'value': 'off' if live else 'on', 'source': 'zk' if live else 'zk_cache'}
        assert report['pooler'] == {'value': 'closed', 'source': 'db_cache'}
        assert report['sessions_ratio'] == {'value': 0, 'source': 'db_cache'}
        assert report['last_failover_time']['value'] == '1970-01-01T00:00:00Z'
        if live:
            assert report['upstream_host'] == {'value': None, 'source': None}


@pytest.mark.parametrize('short', [False, True])
@pytest.mark.parametrize('zk_live', [False, True])
@pytest.mark.parametrize('sql,cache_name,expected_source', [
    ({'available': True, 'role': 'replica', 'wal_receiver': {'sender_host': UPSTREAM}}, 'pgconsul.status', 'db'),
    ({'available': True, 'role': 'replica', 'wal_receiver': None}, 'pgconsul.status', 'status_db_cache'),
    ({'available': False}, '.pgconsul_db_state.cache', 'db_cache'),
    ({'available': True, 'role': 'replica', 'wal_receiver': None}, None, None),
])
def test_replica_upstream_host_uses_actual_upstream(monkeypatch, tmp_path, config, info_zk, short, zk_live, sql, cache_name, expected_source):
    if cache_name:
        cache = {'role': 'replica', 'primary_fqdn': UPSTREAM if expected_source != 'db' else 'old.example.net'}
        (tmp_path / cache_name).write_text(json.dumps({'db_state': cache} if cache_name == 'pgconsul.status' else cache))
    info_zk.get_state.return_value = {'lock_holder': PRIMARY}
    if not zk_live:
        info_zk.get_state.side_effect = ZookeeperException('unavailable')
    monkeypatch.setattr(cli, '_get_info_db_state', lambda _: sql)
    report = cli._show_info(SimpleNamespace(short=short), config)
    upstream = UPSTREAM if expected_source else None
    primary = PRIMARY if zk_live else None
    assert report['upstream_host'] == (upstream if short else {'value': upstream, 'source': expected_source})
    assert report['primary'] == (primary if short else {'value': primary, 'source': 'zk' if zk_live else None})


@pytest.mark.parametrize('raw', ['broken', '[]', 'null', '42'])
def test_invalid_cache_is_unavailable(tmp_path, config, raw):
    (tmp_path / '.pgconsul_db_state.cache').write_text(raw)
    assert cli._get_db_state(config) is None


@pytest.mark.parametrize('short', [False, True])
@pytest.mark.parametrize('live', [False, True])
@pytest.mark.parametrize('identity', [{'application_name': 'local_example_net'}, {'client_hostname': HOST}])
@pytest.mark.parametrize('lag', [0, 125])
@pytest.mark.parametrize('cascade', [False, True])
def test_replica_sync_and_lag_from_upstream(monkeypatch, tmp_path, config, info_zk, short, live, identity, lag, cascade):
    upstream = {'replics_info': [
        {'client_hostname': CHILD, 'sync_state': 'async', 'replay_lag_msec': 999},
        {**identity, 'sync_state': 'quorum', 'replay_lag_msec': lag},
    ]}
    if cascade:
        config.set('global', 'stream_from', UPSTREAM)
        upstream['replics_info'] = [{'client_hostname': UPSTREAM, 'sync_state': 'async', 'replay_lag_msec': 999}]
    (tmp_path / 'pgconsul.status').write_text(json.dumps({'zk_state': upstream}))
    info_zk.get_state.return_value = upstream
    if not live:
        info_zk.get_state.side_effect = ZookeeperException('unavailable')
    monkeypatch.setattr(cli, '_get_info_db_state', lambda _: {'available': True, 'role': 'replica', 'replics_info': [],
                                                           'wal_receiver': {'status': 'streaming'}})
    report = cli._show_info(SimpleNamespace(short=short), config)
    info_zk.get_state.assert_called_once_with()
    for key, expected in (('upstream_sync_state', 'quorum'), ('upstream_replay_lag_msec', lag)):
        assert report.get(key) == (None if cascade else expected if short else {'value': expected, 'source': 'zk' if live else 'zk_cache'})
    assert report['downstream_replicas'] == ([] if short else {'value': [], 'source': 'db'})


@pytest.mark.parametrize('replicas', [None, [], [None], [{'client_hostname': CHILD, 'sync_state': 'sync', 'replay_lag_msec': 1}]])
def test_missing_local_replica_keeps_sync_and_lag_unknown(replicas):
    values = cli._info_values({'replics_info': replicas}, 'zk', HOST)
    assert values.get('sync_state') is None
    assert values.get('replay_lag_msec') is None


@pytest.mark.parametrize('failure', [None, 'role', 'replication_state', 'wal_receiver', 'replics_info', 'connect', 'timeout', 'disconnect'])
def test_live_sql_keeps_independent_queries_and_closes_connection(monkeypatch, config, failure):
    data = {
        'role': {'recovery': True}, 'replication_state': {'synchronous_standby_names': ''},
        'wal_receiver': {'status': 'streaming', 'sender_host': 'primary', 'last_msg_receipt_time_msec': 1700000000123},
        'replics_info': {'client_hostname': CHILD, 'state': 'streaming', 'sync_state': 'quorum', 'replay_lag_msec': 0},
    }
    connection, cursor = MagicMock(closed=False), MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    def execute(query):
        assert query.startswith(('SELECT ', 'SHOW '))
        name = ('role' if 'AS recovery' in query else 'replication_state' if query.startswith('SHOW ') else
                'wal_receiver' if 'pg_stat_wal_receiver' in query else 'replics_info')
        if name == failure:
            raise cli.psycopg2.DatabaseError('never-print')
        if name == 'wal_receiver' and failure == 'disconnect':
            connection.closed = True
            raise cli.psycopg2.OperationalError('never-print')
        cursor.description = [(key,) for key in data[name]]
        cursor.fetchall.return_value = [tuple(data[name].values())]
    cursor.execute.side_effect = execute
    connect = Mock(return_value=connection)
    if failure == 'connect':
        connect.side_effect = cli.psycopg2.OperationalError('never-print')
    monkeypatch.setattr(cli.psycopg2, 'connect', connect)
    monkeypatch.setattr(cli, 'parse_dsn', lambda _: {'options': '-c search_path='})
    monkeypatch.setattr(cli, 'wait_async_operation', Mock(side_effect=TimeoutError('never-print') if failure == 'timeout' else None))
    result = cli._get_info_db_state(config)
    assert result['available'] is (failure not in ('connect', 'timeout', 'disconnect'))
    assert 'never-print' not in repr(result)
    if failure != 'connect':
        connection.close.assert_called_once()
    if failure not in ('connect', 'timeout'):
        if failure == 'role':
            assert 'role' not in result
        else:
            assert result['role'] == 'replica'
        if failure == 'replication_state':
            assert 'replication_state' not in result
        else:
            assert result['replication_state'] == ('async', None)
        if failure in ('wal_receiver', 'replics_info'):
            assert failure not in result
    assert connect.call_args.kwargs['async_'] is True
    assert connect.call_args.kwargs['options'] == '-c search_path= -c statement_timeout=2000'


def test_local_replication_dates_secrets_and_repeated_names():
    raw = {'role': 'replica', 'replics_info': [
        {'client_hostname': name, 'state': 'streaming', 'sync_state': 'quorum', 'replay_lag_msec': 0,
         'backend_start_ts': 1700000000.987, 'reply_time_ms': 1700000000987}
        for name in (CHILD, CHILD, None)
    ], 'wal_receiver': {'status': 'streaming', 'conninfo': 'password=never-print', 'last_msg_receipt_time_msec': 0}}
    before = copy.deepcopy(raw)
    values = cli._info_values(raw, 'db', HOST)
    assert values['hosts_streaming'] == 3 and values['streaming'] is True
    assert values['wal_receiver']['last_msg_receipt_time_msec'] is None
    assert 'conninfo' not in values['wal_receiver']
    assert values['replics_info'][0]['backend_start_ts'] == '2023-11-14T22:13:20Z'
    assert values['replics_info'][0]['reply_time_ms'] == '2023-11-14T22:13:20Z'
    assert values['replics_info'][0]['replay_lag_msec'] == 0
    assert len(cli._short_replica_infos(values['replics_info'])) == 3
    assert raw == before
    zk_raw = {'replics_info': raw['replics_info'], 'last_failover_time': 0, 'last_switchover_time': 1700000000,
              'synchronous_standby_names': {HOST: ['ANY 1 (*)', '1700000000.987']}}
    zk = cli._info_values(zk_raw, 'zk', HOST)
    assert len(zk['replics_info']) == 3
    assert zk['synchronous_standby_names'][HOST][1] == '2023-11-14T22:13:20Z'
    merged, origins = cli._merge_info([('zk', zk_raw)], HOST)
    assert merged['last_change'] == 'switchover at 2023-11-14T22:13:20Z'
    assert origins['last_change'] == 'zk'


@pytest.mark.parametrize('value,milliseconds', [
    (1700000000, False), (1700000000.987, False), ('1700000000', False), ('1700000000.987', False),
    (1700000000987, True), (1700000000987.0, True), ('1700000000987', True),
])
def test_timestamp_inputs_use_the_same_utc_conversion(value, milliseconds):
    assert cli._info_timestamp(value, milliseconds) == '2023-11-14T22:13:20Z'


@pytest.mark.parametrize('value', [None, True, False, '', 'broken', float('nan'), float('inf'), 10 ** 1000])
def test_invalid_timestamps_remain_unknown(value):
    assert cli._info_timestamp(value) is None


def test_json_and_yaml_serialize_same_report(monkeypatch, capsys):
    report = {'host': HOST, 'role': {'value': 'primary', 'source': 'db'}}
    monkeypatch.setattr(cli, '_show_info', lambda *_: report)
    dumped = []
    monkeypatch.setattr(cli.yaml, 'safe_dump', lambda document, **_: dumped.append(document) or 'yaml')
    cli.show_info(SimpleNamespace(json=False), Mock())
    capsys.readouterr()
    cli.show_info(SimpleNamespace(json=True), Mock())
    assert dumped == [json.loads(capsys.readouterr().out)]


@pytest.mark.parametrize('short', [False, True])
def test_info_groups_fields_independently_of_source_order(monkeypatch, tmp_path, config, info_zk, short):
    replica = {'pid': 123, 'reply_time_ms': 1700000000987, 'backend_start_ts': 1700000000,
               'replay_location_diff': 0, 'write_location_diff': 0, 'sent_location_diff': 0,
               'primary_location': '0/123', 'replay_lag_msec': 31, 'sync_state': 'async',
               'state': 'streaming', 'application_name': 'child', 'client_addr': '192.0.2.1',
               'client_hostname': CHILD, 'extra_detail': 'preserved'}
    sql = {'available': True, 'replics_info': [replica], 'replication_state': ('async', None),
           'wal_receiver': {'sender_host': UPSTREAM, 'status': 'streaming'}, 'role': 'replica'}
    zk_state = {'last_switchover_time': 1700000000, 'last_failover_time': 1700000001,
                'lock_holder': PRIMARY, 'alive': True, 'timeline': 3, 'maintenance': {'status': 'enable'},
                'replics_info': [{'client_hostname': HOST, 'sync_state': 'quorum', 'replay_lag_msec': 50}]}
    cache = {'pgdata': '/data', 'sessions_ratio': 2, 'opened': True, 'running': True, 'alive': True,
             'extra_diagnostic': 'preserved'}
    (tmp_path / '.pgconsul_db_state.cache').write_text(json.dumps(cache))
    info_zk.get_state.return_value = zk_state
    monkeypatch.setattr(cli, '_get_info_db_state', lambda _: copy.deepcopy(sql))

    report = cli._show_info(SimpleNamespace(short=short), config)

    assert not {'hosts_total', 'hosts_alive', 'alive_hosts', 'hosts_streaming', 'replics_info',
                'primary_fqdn', 'streaming', 'sync_state', 'replay_lag_msec', 'replication_state'} & report.keys()
    common = ['db', 'zk', 'zk_cache', 'status_db_cache', 'db_cache', 'host', 'role']
    if short:
        assert list(report) == common + ['maintenance', 'primary', 'upstream_host', 'upstream_streaming', 'upstream_sync_state',
                                        'upstream_replay_lag_msec', 'downstream_replicas', 'last_failover_time']
        assert report['primary'] == PRIMARY
        assert report['upstream_host'] == UPSTREAM
    else:
        assert list(report) == common + [
            'alive', 'running', 'pooler', 'maintenance', 'sessions_ratio', 'timeline',
            'primary', 'upstream_host', 'upstream_streaming', 'upstream_sync_state', 'upstream_replay_lag_msec', 'wal_receiver',
            'downstream_replication_mode', 'downstream_replicas_streaming', 'downstream_replicas',
            'last_change', 'last_failover_time', 'last_switchover_time', 'zk_alive', 'pgdata', 'extra_diagnostic',
        ]
        row = report['downstream_replicas']['value'][0]
        assert list(row) == ['client_hostname', 'client_addr', 'application_name', 'state', 'sync_state', 'replay_lag_msec',
                             'primary_location', 'sent_location_diff', 'write_location_diff', 'replay_location_diff',
                             'reply_time_ms', 'backend_start_ts', 'pid', 'extra_detail']
        assert row['extra_detail'] == report['extra_diagnostic']['value'] == 'preserved'
        assert report['primary'] == {'value': PRIMARY, 'source': 'zk'}
    assert sql['replics_info'] == [replica]


@pytest.mark.parametrize('sql_replicas', ['missing', None, [], [{'client_hostname': CHILD, 'state': 'streaming'}]])
def test_merge_keeps_original_zk_replicas_unless_sql_supplies_them(sql_replicas):
    zk_replicas = [{'client_hostname': HOST, 'state': 'streaming'},
                   {'client_hostname': 'other.example.net', 'state': 'streaming'},
                   {'application_name': 'pg_basebackup', 'state': 'backup'}]
    sql = {'alive': False, 'sessions_ratio': 0}
    if sql_replicas != 'missing':
        sql['replics_info'] = sql_replicas
    sources = [
        ('db', sql),
        ('zk', {'replics_info': zk_replicas, 'alive': True, 'lock_holder': PRIMARY}),
        ('db_cache', {'replics_info': [{'client_hostname': 'old.example.net'}], 'alive': True, 'sessions_ratio': 10}),
    ]
    before = copy.deepcopy(sources)

    values, origins = cli._merge_info(sources, HOST)

    assert values['replics_info'] == (zk_replicas if sql_replicas == 'missing' else sql_replicas)
    assert origins['replics_info'] == ('zk' if sql_replicas == 'missing' else 'db')
    assert values['alive'] is False and values['sessions_ratio'] == 0
    assert origins['alive'] == origins['sessions_ratio'] == 'db'
    assert values['primary'] == PRIMARY and origins['primary'] == 'zk'
    assert sources == before


def test_merge_uses_cached_zk_when_live_sources_are_unavailable():
    sources = [('db', None), ('zk', None), ('zk_cache', {'lock_holder': PRIMARY, 'timeline': 3, 'alive': False}),
               ('status_db_cache', {'timeline': 2, 'alive': True}), ('db_cache', {'timeline': 1, 'opened': False})]

    values, origins = cli._merge_info(sources, HOST)

    assert values['primary'] == PRIMARY and origins['primary'] == 'zk_cache'
    assert values['timeline'] == 3 and origins['timeline'] == 'zk_cache'
    assert values['alive'] is False and origins['alive'] == 'zk_cache'
    assert values['pooler'] == 'closed' and origins['pooler'] == 'db_cache'


@pytest.mark.parametrize('value', [None, False, 0, [], {}, ''])
def test_merge_selects_first_available_source_containing_field(value):
    sources = [('db', None), ('zk', {}), ('zk_cache', {'field': value}),
               ('status_db_cache', {'field': 'older'}), ('db_cache', {'field': 'oldest'})]

    values, origins = cli._merge_info(sources, HOST)

    assert values['field'] == value and origins['field'] == 'zk_cache'


@pytest.mark.parametrize('field,result_field', [
    ('lock_holder', 'primary'),
    ('current_promoting_host', 'current_promoting_host'),
    ('switchover/state', 'switchover/state'),
    ('switchover/candidate', 'switchover/candidate'),
    ('failover_state', 'failover_state'),
])
def test_merge_does_not_restore_cleared_zk_field_from_cache(field, result_field):
    values, origins = cli._merge_info([('zk', {field: None}), ('zk_cache', {field: 'old'})], HOST)

    assert values[result_field] is None and origins[result_field] == 'zk'


@pytest.mark.parametrize('source', ['db', 'zk', 'zk_cache', 'status_db_cache', 'db_cache'])
def test_normalization_does_not_invent_absent_fields(source):
    assert cli._info_values({}, source, HOST) == {}
