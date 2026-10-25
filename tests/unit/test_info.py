import copy
import json
from unittest.mock import Mock

import pytest

from src import info
from src.zk import Zookeeper, ZookeeperConfig, ZookeeperException
from src.zk_client import ZkClientError


HOST = 'primary.example.net'
REPLICA = 'replica.example.net'


def row(host=REPLICA, lag=0):
    return {'client_hostname': host, 'state': 'streaming', 'sync_state': 'quorum', 'replay_lag_msec': lag}


def report(*, short=False, db=None, status_db=None, zk_cache=None, zk=None, sql=None):
    return info.build_info(short, db, status_db, zk_cache, zk, HOST, sql=sql)


def test_cache_sections_are_independent_and_preserve_known_zero(tmp_path):
    (tmp_path / '.pgconsul_db_state.cache').write_text(json.dumps({'role': 'primary', 'replics_info': [row()]}))
    (tmp_path / 'pgconsul.status').write_text(json.dumps({
        'db_state': [], 'zk_state': {'synchronous_standby_names': {HOST: ['', '1']}, 'maintenance': {'status': 'enable'}},
    }))
    config = Mock()
    config.get.return_value = str(tmp_path)
    db, status_db, zk = info.read_local_sources(config)
    result = report(db=db, status_db=status_db, zk_cache=zk)
    assert result['role'] == 'primary' and result['hosts_total'] == 1
    assert result['maintenance'] == 'on' and result['hosts_alive'] is None
    assert result['replics_info'][0]['replay_lag_msec'] == 0
    assert result['status_db_cache'] == 'unavailable'


@pytest.mark.parametrize('raw', ['broken JSON', '[]', '0', 'null', '"text"', '{"role":NaN}'])
def test_invalid_files_leave_unknown_values(tmp_path, raw):
    (tmp_path / '.pgconsul_db_state.cache').write_text(raw)
    (tmp_path / 'pgconsul.status').write_text(raw)
    config = Mock()
    config.get.return_value = str(tmp_path)
    assert info.read_local_sources(config) == (None, None, None)


def test_empty_replica_publication_wins_over_nonempty_cache():
    result = report(short=True, db={'role': 'primary', 'replics_info': [row()]}, zk={'info_host_replics_info': []})
    assert result['replics_info'] == []
    assert report(short=True)['replics_info'] is None


def test_status_db_cache_precedes_the_separate_db_cache():
    result = report(db={'role': 'primary'}, status_db={'role': 'replica'})
    assert result['role'] == 'replica'
    assert result['sources']['role'] == {'source': 'status_db_cache', 'cached': True}


def test_short_replicas_keep_duplicate_and_missing_names():
    result = report(short=True, db={'role': 'primary', 'replics_info': [row(), row(lag=50), row(None)]})
    assert len(result['replics_info']) == 3
    assert sum(item['host'] == REPLICA for item in result['replics_info']) == 2
    assert any(item['host'] is None for item in result['replics_info'])


@pytest.mark.parametrize('alive', [[HOST, REPLICA], [HOST], []])
def test_alive_counts_are_independent_of_streaming(alive):
    result = report(zk={'info_members': [HOST, REPLICA], 'info_alive_hosts': alive, 'info_host_replics_info': [row()]})
    assert result['hosts_alive'] == len(alive) and result['hosts_streaming'] == 1
    if len(alive) == 2:
        assert 'alive_hosts' not in result
    else:
        assert result['alive_hosts'] == sorted(alive)


def test_zk_diagnostic_reads_use_existing_state_without_writes():
    client = Mock()
    client.is_alive.return_value = True
    client.get.return_value = None
    client.exists.return_value = True
    client.get_children.return_value = [HOST, REPLICA]
    client.lock_version.return_value = None
    client.read_lock_contenders.side_effect = [[HOST], [HOST], []]
    zk = Zookeeper(client, ZookeeperConfig(False, 1.0, '/pgconsul/'), init_lock=False)
    state = zk.get_info_state(HOST)
    assert state['lock_holder'] == HOST
    assert state['info_members'] == [HOST, REPLICA] and state['info_alive_hosts'] == [HOST]
    zk.close()
    client.write.assert_not_called()
    client.ensure_path.assert_not_called()
    client.make_lock.assert_not_called()


def test_zk_error_propagates_to_the_source_reader():
    client = Mock()
    client.is_alive.return_value = True
    client.get.return_value = None
    client.exists.return_value = True
    client.lock_version.return_value = None
    client.read_lock_contenders.side_effect = ZkClientError('lost')
    zk = Zookeeper(client, ZookeeperConfig(False, 1.0, '/pgconsul/'), init_lock=False)
    with pytest.raises(ZookeeperException):
        zk.get_info_state(HOST)


@pytest.mark.parametrize('short', [False, True])
def test_dates_and_replication_details_remain_utc_without_mutating_sources(short):
    db = {'role': 'primary', 'opened': False, 'wal_receiver': {'last_msg_receipt_time_msec': 1700000000123}}
    zk = {'last_failover_time': 0, 'last_switchover_time': 1700000000.125,
          'synchronous_standby_names': {HOST: ('ANY 1 (*)', '1700000000.125')},
          'info_host_replics_info': [{**row(lag=1500), 'backend_start_ts': 1700000000, 'reply_time_ms': 1700000000123}]}
    before = copy.deepcopy((db, zk))
    result = report(short=short, db=db, zk=zk)
    assert result['last_failover_time'] == '1970-01-01T00:00:00Z'
    if not short:
        assert result['last_change'] == 'switchover at 2023-11-14T22:13:20.125000Z'
        assert result['replics_info'][0]['backend_start_ts'] == '2023-11-14T22:13:20Z'
        assert result['replics_info'][0]['reply_time_ms'] == '2023-11-14T22:13:20.123000Z'
        assert result['synchronous_standby_names'][HOST][1] == '2023-11-14T22:13:20.125000Z'
        assert result['wal_receiver']['last_msg_receipt_time_msec'] == '2023-11-14T22:13:20.123000Z'
    assert (db, zk) == before


@pytest.mark.parametrize('invalid', [None, True, '', 'broken', 'NaN', 'Infinity', {}, [], 10 ** 1000])
def test_invalid_dates_become_null(invalid):
    result = report(zk={'last_failover_time': invalid, 'info_host_replics_info': [{**row(), 'backend_start_ts': invalid}]})
    assert result['last_failover_time'] is None
    assert result['replics_info'][0]['backend_start_ts'] is None


def test_receiver_coalesced_zero_is_unknown_while_failover_epoch_zero_is_valid():
    result = report(db={'wal_receiver': {'last_msg_receipt_time_msec': 0}}, zk={'last_failover_time': 0})
    assert result['wal_receiver']['last_msg_receipt_time_msec'] is None
    assert result['last_failover_time'] == '1970-01-01T00:00:00Z'


@pytest.mark.parametrize('short', [False, True])
@pytest.mark.parametrize('cached_maintenance,expected', [({'status': 'enable'}, 'on'), (None, None)])
def test_host_publication_cannot_turn_failed_cluster_read_into_live_state(monkeypatch, short, cached_maintenance, expected):
    local = {'available': True, 'role': 'replica', 'receiver': {'status': 'streaming', 'sender_host': HOST}}
    sql = local
    published = [{'pid': 1, 'application_name': 'replica', 'client_addr': None,
                  'client_hostname': REPLICA, 'state': 'streaming', 'sync_state': 'quorum', 'replay_lag_msec': 7}]
    monkeypatch.setattr(info.helpers, 'get_hostname', lambda: REPLICA)
    monkeypatch.setattr(info, 'read_local_sources', lambda *_: (None, None, {'maintenance': cached_maintenance}))
    monkeypatch.setattr(info.info_db, 'read_db_source', lambda *_, **__: sql)
    monkeypatch.setattr(info, '_bounded_read', lambda read, args, timeout, default: (read(*args), True))
    monkeypatch.setattr(info, 'read_zk_sources', lambda *_: {'hostname': REPLICA, 'state': None, 'hosts': {HOST: published}})
    result = info.collect_info(None, short=short)
    assert result['zk'] == 'unavailable' and result['maintenance'] == expected
    assert result['sync_state'] == 'quorum' and result['replay_lag_msec'] == 7
    if not short:
        assert result['sources']['sync_state'] == {'source': f'zk_host:{HOST}', 'cached': False}
        assert result['sources']['maintenance']['source'] == ('zk_cache' if expected else None)


@pytest.mark.parametrize('raw,expected', [({}, None), ({'maintenance': {}}, None), ({'maintenance': 'broken'}, None),
                                        ({'maintenance': {'status': None}}, 'off'), ({'maintenance': {'status': ''}}, 'off'),
                                        ({'maintenance': {'status': 'enable'}}, 'on'), ({'maintenance': {'status': 'disable'}}, 'off')])
def test_maintenance_normalization_depends_on_observation_not_cache_flag(raw, expected):
    for cached in (False, True):
        assert info._zk_values(raw, cached=cached)['maintenance'] == expected


def test_optional_zk_errors_preserve_completed_cluster_snapshot(monkeypatch):
    client = Mock()
    client.exists.side_effect = ZkClientError('membership lost')
    client.get.side_effect = ZkClientError('host publication lost')
    zk = Zookeeper(client, ZookeeperConfig(False, 1.0, '/pgconsul/'), init_lock=False)
    core = {'lock_holder': HOST, 'maintenance': {'status': 'enable'}}
    monkeypatch.setattr(zk, 'get_state', lambda **_: core.copy())
    snapshots = []
    result = zk.get_info_state(HOST, publish=lambda data: snapshots.append(data.copy()))
    assert result == core
    assert snapshots[0] == core
    full = report(zk=result)
    assert full['zk'] == 'available' and full['maintenance'] == 'on'


@pytest.mark.parametrize('operation', ['exists', 'get_children'])
def test_cluster_read_does_not_turn_zk_transport_errors_into_absence(operation):
    client = Mock()
    client.is_alive.return_value = True
    client.get.return_value = None
    client.exists.return_value = False
    client.get_children.return_value = []
    client.read_lock_contenders.return_value = []
    getattr(client, operation).side_effect = ZkClientError('disconnected')
    zk = Zookeeper(client, ZookeeperConfig(False, 1.0, '/pgconsul/'), init_lock=False)
    with pytest.raises(ZookeeperException):
        zk.get_info_state(HOST)


@pytest.mark.parametrize('stamp,expected', [
    ('2026-10-26T12:34:56', '2026-10-26T12:34:56Z'),
    ('2026-10-26T12:34:56.123', '2026-10-26T12:34:56.123000Z'),
    ('2026-10-26T15:34:56+03:00', '2026-10-26T12:34:56Z'),
])
def test_iso_dates_without_timezone_are_utc(stamp, expected):
    assert info._utc(stamp) == expected


@pytest.mark.parametrize('source', ['db', 'zk', 'zk_cache', 'db_cache'])
def test_replica_normalization_keeps_valid_fields_and_rows(source):
    original = [row(), {**row(lag='broken'), 'state': None, 'pid': True, 'client_addr': 123,
                       'backend_start_ts': '2026-10-26T12:34:56'}, 'broken row', row(lag=42)]
    before = copy.deepcopy(original)
    raw = {'senders' if source == 'db' else 'replics_info': original}
    if source.startswith('zk'):
        raw = {'info_host_replics_info': original}
        normalized = info._zk_values(raw, cached=source == 'zk_cache')
    else:
        normalized = info._db_values(raw, HOST, sql=source == 'db')
    rows = normalized['replics_info']
    assert len(rows) == 4
    assert rows[0]['state'] == rows[3]['state'] == 'streaming'
    assert rows[3]['replay_lag_msec'] == 42
    assert rows[1]['sync_state'] == 'quorum' and rows[1]['client_hostname'] == REPLICA
    assert rows[1]['state'] is rows[1]['pid'] is rows[1]['client_addr'] is rows[1]['replay_lag_msec'] is None
    assert rows[1]['backend_start_ts'] == '2026-10-26T12:34:56Z'
    assert all(value is None for value in rows[2].values())
    assert normalized['hosts_streaming'] is None
    assert original == before
    assert len(info._short_replicas(rows)) == 4


@pytest.mark.parametrize('invalid', [True, 'broken', float('nan'), float('inf'), -float('inf')])
def test_invalid_replica_lag_does_not_erase_other_fields(invalid):
    rows = info._replicas([row(lag=invalid)])
    assert rows[0]['replay_lag_msec'] is None
    assert rows[0]['state'] == 'streaming'
    assert info._streaming_count(rows) == 1


def test_short_rows_with_unknown_states_are_sorted_and_formatted():
    rows = info._replicas([row(), {**row(), 'state': None, 'sync_state': None}])
    short = info._short_replicas(rows)
    assert len(short) == 2
    assert short[0] == {'host': REPLICA, 'info': 'unknown, sync_state unknown, replay_lag_msec 0'}
