import json
import pytest
from types import SimpleNamespace
from unittest.mock import Mock

import src
src.read_config = Mock()
src.init_logging = Mock()
from src import cli, info, info_output


HOST = 'replica.example.net'
PRIMARY = 'primary.example.net'


def full_report():
    local = {
        'available': True, 'role': 'replica', 'receiver': {'status': 'streaming', 'sender_host': PRIMARY},
        'senders': [
            {
                'pid': 1, 'application_name': 'receiver', 'client_addr': None, 'client_hostname': 'child.example.net', 'state': 'streaming', 'sync_state': 'quorum',
                'replay_lag_msec': None,
            },
        ],
    }
    live = {
        'alive': True, 'info_members': [PRIMARY, HOST], 'info_alive_hosts': [HOST], 'lock_holder': PRIMARY,
        'replics_info': [{'client_hostname': HOST, 'state': 'streaming', 'sync_state': 'quorum', 'replay_lag_msec': 5}], 'info_maintenance_status': '',
        'maintenance': {'status': None, 'ts': None},
    }
    return info.build_info(False, {'role': 'primary', 'replics_info': [], 'sessions_ratio': 0, 'opened': False},
                           {}, {}, live, HOST,
                           sql=local)


def test_full_values_have_per_field_sources():
    report = full_report()
    assert 'diagnostics' not in report
    assert report['host'] == HOST and report['role'] == 'replica'
    assert report['sources']['role'] == {'source': 'db', 'cached': False}
    assert report['zk_alive'] is True
    assert report['sources']['zk_alive'] == {'source': 'zk', 'cached': False}
    assert report['hosts_alive'] == 1 and report['alive_hosts'] == [HOST]
    assert report['sources']['hosts_alive'] == {'source': 'zk', 'cached': False}
    assert report['sources']['sessions_ratio'] == {'source': 'db_cache', 'cached': True}
    assert report['pooler'] == 'closed'
    assert report['sources']['pooler'] == {'source': 'db_cache', 'cached': True}
    assert {'opened', 'db_alive'}.isdisjoint(report)
    assert {'opened', 'db_alive'}.isdisjoint(report['sources'])
    assert report['primary_fqdn'] == PRIMARY
    assert report['sources']['primary_fqdn']['source'] == 'zk'
    assert report['replay_lag_msec'] is None
    assert report['sources']['replay_lag_msec']['source'] is None
    assert report['hosts_streaming'] == 1
    assert report['sources']['hosts_streaming']['source'] == 'db'
    assert len(report['replics_info']) == 1
    assert report['replics_info'][0]['client_hostname'] == 'child.example.net'
    assert report['replics_info'][0]['pid'] == 1 and report['replics_info'][0]['application_name'] == 'receiver'
    assert report['replics_info'][0]['replay_lag_msec'] is None
    assert report['sources']['replics_info']['source'] == 'db'
    assert {'global_replics_info', 'local_replics_info', 'local_hosts_streaming'}.isdisjoint(report)
    assert {'global_replics_info', 'local_replics_info', 'local_hosts_streaming'}.isdisjoint(report['sources'])


def test_full_yaml_keeps_values_and_orders_fields_without_separators(monkeypatch, capsys):
    serialized = []
    def dump(mapping, **_options):
        assert _options['sort_keys'] is False
        serialized.append(mapping)
        return '\n'.join(f'{key}: {value if isinstance(value, str) else json.dumps(value)}' for key, value in mapping.items())
    report = full_report()
    report['extra'] = []
    monkeypatch.setattr(info_output, 'yaml', SimpleNamespace(safe_dump=dump))
    monkeypatch.setattr(cli, '_show_info', lambda *_: report)
    cli.show_info(SimpleNamespace(json=False, short=False), Mock())
    output = capsys.readouterr().out
    assert output.startswith('db: available\nzk: available\n')
    assert len(serialized) == 1
    document = serialized[0]
    assert document['host'] == HOST
    assert document['role'] == {'value': 'replica', 'source': 'db'}
    assert document['pooler'] == {'value': 'closed', 'source': 'db_cache'}
    assert document['replics_info'] == {'value': report['replics_info'], 'source': 'db'}
    assert document['replay_lag_msec'] == {'value': None, 'source': None}
    assert 'sources' not in document
    assert all(set(value) == {'value', 'source'} for name, value in document.items()
               if name not in (*info_output.AVAILABILITY_FIELDS, 'host'))
    names = list(serialized[0])
    assert names.index('host') < names.index('hosts_alive') < names.index('replics_info')
    assert names[-1] == 'extra'
    assert document['extra'] == {'value': [], 'source': None}
    assert '-_-_' not in output and '"cached"' not in output


def test_full_json_is_valid_and_does_not_label_cached_fallback_as_db(monkeypatch, capsys):
    empty = {}
    internal = info.build_info(False, {'role': 'replica'}, empty, empty, {}, HOST,
                               sql={'available': True})
    monkeypatch.setattr(cli, '_show_info', lambda *_: internal)
    cli.show_info(SimpleNamespace(json=True, short=False), Mock())
    output = capsys.readouterr().out
    report = json.loads(output)
    assert report['db'] == 'available'
    assert report['role'] == {'value': 'replica', 'source': 'db_cache'}
    assert report['hosts_alive'] == {'value': None, 'source': None}
    assert 'sources' not in report and '"cached"' not in output


@pytest.mark.parametrize('live_status,cached_status,value,source', [
    ('enable', 'disable', 'on', 'zk'),
    ('', 'enable', 'off', 'zk'),
    (None, 'enable', 'on', 'zk_cache'),
    (None, None, 'off', 'zk_cache'),
])
def test_full_maintenance_has_one_status_and_preserves_live_cached_and_unknown_origins(live_status, cached_status, value, source):
    live = {'maintenance': {'status': live_status, 'ts': 1700000000}} if live_status is not None else None
    cached = {'maintenance': {'status': cached_status}}
    empty = {}
    report = info.build_info(False, empty, empty, cached, live, HOST)
    assert report['maintenance'] == value
    assert report['sources']['maintenance'] == {
        'source': source, 'cached': None if source is None else source == 'zk_cache',
    }
    assert {'maintenance_status', 'resetup_hosts'}.isdisjoint(report)
    assert {'maintenance_status', 'resetup_hosts'}.isdisjoint(report['sources'])


@pytest.mark.parametrize('opened,expected', [(True, 'opened'), (False, 'closed'), (None, None), ('false', None), (0, None), (1, None)])
def test_pooler_formats_only_boolean_cache_values_and_preserves_unknown(opened, expected):
    cache = {'opened': opened}
    empty = None
    report = info.build_info(False, cache, empty, empty, {}, HOST)
    assert report['pooler'] == expected
    assert report['sources']['pooler'] == {
        'source': 'db_cache' if expected is not None else None,
        'cached': True if expected is not None else None,
    }
    assert 'opened' not in report and 'opened' not in report['sources']


@pytest.mark.parametrize('short', [False, True])
@pytest.mark.parametrize('local_available,db_cache,status_cache,expected', [
    (True, {'alive': False}, {'alive': False}, 'available'),
    (True, None, None, 'available'),
    (False, {'alive': True}, None, 'only cache'),
    (False, None, {'alive': False}, 'only cache'),
    (False, {}, None, 'only cache'),
    (False, None, None, 'unavailable'),
])
def test_db_status_prefers_current_probe_and_distinguishes_cache_from_no_sources(short, local_available, db_cache, status_cache, expected):
    cache = db_cache
    status = status_cache
    sql = {'available': local_available}
    report = info.build_info(short, cache, status, None, {}, HOST, sql=sql)
    if not short:
        assert 'db_alive' not in report['sources']
    else:
        assert 'alive' not in report
    assert report['db'] == expected and 'db_alive' not in report


@pytest.mark.parametrize('db_alive,zk_alive', [(True, False), (False, True)])
def test_db_and_zk_liveness_have_separate_names_and_origins(db_alive, zk_alive):
    result = info.build_info(False, {'alive': db_alive}, None, None, {'alive': zk_alive}, HOST)
    assert result['alive'] is db_alive and result['zk_alive'] is zk_alive
    assert result['sources']['alive'] == {'source': 'db_cache', 'cached': True}
    assert result['sources']['zk_alive'] == {'source': 'zk', 'cached': False}


def test_cached_zk_liveness_is_preserved_and_live_false_overrides_it():
    cached = info.build_info(False, None, None, {'alive': True}, None, HOST)
    assert cached['alive'] is None and cached['zk_alive'] is True
    assert cached['sources']['zk_alive'] == {'source': 'zk_cache', 'cached': True}
    current = info.build_info(False, None, None, {'alive': True}, {'alive': False}, HOST)
    assert current['zk_alive'] is False
    assert current['sources']['zk_alive'] == {'source': 'zk', 'cached': False}


@pytest.mark.parametrize('sql_role,cached_role,expected_role', [
    ('primary', 'replica', 'primary'),
    ('replica', 'primary', 'replica'),
    ('replica', None, 'replica'),
    (None, 'replica', 'replica'),
    (None, 'primary', 'primary'),
    (None, None, None),
])
def test_full_replica_fields_and_sources_follow_the_selected_role(sql_role, cached_role, expected_role):
    cache = {'role': cached_role}
    sql = {'available': sql_role is not None, 'role': sql_role}
    global_rows = [{'client_hostname': HOST, 'state': 'streaming', 'sync_state': 'quorum', 'replay_lag_msec': 0}]
    internal = info.build_info(False, cache, None, None,
                               {'info_host_replics_info': global_rows}, HOST, sql=sql)
    report = internal
    assert report['role'] == expected_role
    for name in ('streaming', 'sync_state', 'replay_lag_msec'):
        assert (name in report) == (expected_role == 'replica')
        assert (name in report['sources']) == (expected_role == 'replica')
        if expected_role == 'replica':
            assert report[name] is None
            assert report['sources'][name] == {'source': None, 'cached': None}
    assert len(report['replics_info']) == 1
    assert all(report['replics_info'][0][key] == value for key, value in global_rows[0].items())


def replication_report(sql=None, db=None, status_db=None, status_zk=None, live=None, *, short=False):
    return info.build_info(short, db, status_db, status_zk, live, HOST, sql=sql)


@pytest.mark.parametrize('role', ['primary', 'replica'])
@pytest.mark.parametrize('empty', [False, True])
def test_full_and_short_describe_the_current_hosts_downstream(role, empty):
    row = {'pid': 1, 'application_name': 'cascade', 'client_hostname': 'child.example.net',
           'state': 'streaming', 'sync_state': 'async', 'replay_lag_msec': 0}
    local = {'available': True, 'role': role, 'senders': [] if empty else [row]}
    global_row = {**row, 'client_hostname': HOST}
    live = {'lock_holder': PRIMARY, 'replics_info': [global_row]}
    full = replication_report(local, live=live)
    short = replication_report(local, live=live, short=True)
    assert full['hosts_streaming'] == (0 if empty else 1)
    assert full['sources']['replics_info']['source'] == 'db'
    if empty:
        assert full['replics_info'] == []
    else:
        assert full['replics_info'][0]['client_hostname'] == 'child.example.net'
    assert short['replics_info'] == ([] if empty else {'child.example.net': 'streaming, sync_state async, replay_lag_msec 0'})
    assert 'db_targets' not in full and 'primary_info' not in short


@pytest.mark.parametrize('cached', [False, True])
def test_primarys_global_publication_cannot_replace_unknown_local_replica_downstream(cached):
    row = {'client_hostname': 'child.example.net', 'state': 'streaming', 'sync_state': 'async', 'replay_lag_msec': 0}
    db = {'role': 'replica', 'replics_info': [row]} if cached else None
    full = replication_report({'available': True, 'role': 'replica'}, db=db,
                              live={'lock_holder': PRIMARY, 'replics_info': [{**row, 'client_hostname': HOST}]})
    if cached:
        assert full['replics_info'][0]['client_hostname'] == 'child.example.net'
        assert full['sources']['replics_info']['source'] == 'db_cache'
    else:
        assert full['replics_info'] is None and full['hosts_streaming'] is None


@pytest.mark.parametrize('empty', [False, True])
def test_basebackup_progress_describes_only_local_donor(empty):
    row = {'client_addr': '192.0.2.5', 'client_hostname': 'child.example.net', 'application_name': 'pg_basebackup',
           'phase': 'streaming database files', 'backup_streamed': 0, 'backup_total': None}
    full = replication_report({'available': True, 'role': 'replica', 'basebackups': [] if empty else [row]})
    assert set(full['basebackups']) == {HOST}
    assert full['sources']['basebackups'][HOST] == {'source': 'db', 'cached': False, 'context': f'host:{HOST}'}
    if empty:
        assert full['basebackups'][HOST] == []
    else:
        assert full['basebackups'][HOST][0]['donor'] == HOST
        assert full['basebackups'][HOST][0]['progress_pct'] is None
    formatted = info_output.format_full(full)
    assert formatted['basebackups'] == {'value': full['basebackups'], 'source': {HOST: 'db'}}
    assert full['sources']['basebackups'][HOST]['cached'] is False


@pytest.mark.parametrize('tier', ['zk', 'status_db_cache', 'db_cache', 'unknown'])
def test_local_replication_falls_back_by_source_with_matching_count(tier):
    row = {'client_hostname': 'child.example.net', 'state': 'streaming', 'sync_state': 'quorum', 'replay_lag_msec': 0}
    db = {'role': 'primary', 'replics_info': [row]} if tier != 'unknown' else None
    status = db if tier in ('zk', 'status_db_cache') else None
    saved_zk = {'replics_info': [{**row, 'client_hostname': 'different-primary-child'}], 'lock_holder': HOST}
    live = {'info_host_replics_info': [row]} if tier == 'zk' else None
    report = replication_report(db=db, status_db=status, status_zk=saved_zk, live=live)
    if tier == 'unknown':
        assert report['replics_info'] is None and report['hosts_streaming'] is None
    else:
        assert len(report['replics_info']) == 1
        assert all(report['replics_info'][0][key] == value for key, value in row.items())
        assert report['hosts_streaming'] == 1
        assert report['sources']['replics_info']['source'] == tier
    assert report['sources']['replics_info'] == report['sources']['hosts_streaming']


@pytest.mark.parametrize('source', ['zk', 'status_db_cache', 'db_cache'])
def test_full_fallback_preserves_all_published_replication_details_and_utc_dates(source):
    row = {
        'pid': 17, 'application_name': 'replica-app', 'client_hostname': 'child.example.net', 'client_addr': '192.0.2.7', 'state': 'streaming',
        'primary_location': '0/4000', 'sent_location_diff': 0, 'write_location_diff': 512, 'replay_location_diff': 1024, 'replay_lag_msec': 0,
        'backend_start_ts': 1700000000, 'reply_time_ms': 1700000000123, 'sync_state': 'quorum',
    }
    arguments = {
        'zk': {'live': {'info_host_replics_info': [row]}},
        'status_db_cache': {'status_db': {'role': 'primary', 'replics_info': [row]}}, 'db_cache': {'db': {'role': 'primary', 'replics_info': [row]}},
    }
    full = replication_report(**arguments[source])
    assert full['replics_info'] == [{**row, 'backend_start_ts': '2023-11-14T22:13:20Z',
                                    'reply_time_ms': '2023-11-14T22:13:20.123000Z'}]
    assert full['sources']['replics_info']['source'] == source
    assert full['sources']['hosts_streaming'] == full['sources']['replics_info']
