import json
from configparser import RawConfigParser
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock

import pytest

from src import info, info_db, info_output


HOST = 'replica.example.net'
PRIMARY = 'primary.example.net'


def db_config(conn_string='dbname=postgres'):
    config = RawConfigParser()
    config.read_dict({'global': {'local_conn_string': conn_string}})
    return config


class Cursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def execute(self, query):
        assert query.startswith(('SELECT ', 'SHOW ')), 'Diagnostics attempted a mutation'
        self.connection.queries.append(query)
        result = self.connection.results[query]
        if isinstance(result, Exception):
            raise result
        self.result = result

    def fetchall(self):
        return self.result


class Connection:
    def __init__(self, *, recovery=False, senders=(), receiver=(), overrides=None):
        self.closed = False
        self.queries = []
        self.results = {
            'SELECT 1': [(1,)], info_db.ROLE_QUERY: [(recovery, 140000)], 'SHOW synchronous_standby_names': [('',)],
            info_db.SENDERS_QUERY: [row + (None,) * 6 if len(row) == 7 else row for row in senders], info_db.RECEIVER_QUERY: list(receiver),
            info_db.WAL_QUERY: [(None, None)], info_db.BACKUPS_QUERY: [],
        }
        self.results.update(overrides or {})

    def cursor(self):
        return Cursor(self)

    def close(self):
        self.closed = True


def collect(monkeypatch, connection):
    monkeypatch.setattr(info_db, 'parse_dsn', lambda _: {'dbname': 'postgres'})
    monkeypatch.setattr(info_db.psycopg2, 'connect', lambda **_: connection)
    monkeypatch.setattr(info_db, 'wait_async_operation', lambda *_: None)
    with info_db.create_info_db(db_config()) as db:
        return db.get_state()


def test_sql_collects_read_only_and_preserves_null_zero_and_nonstreaming_states(monkeypatch):
    senders = [
        (1, 'replica', '192.0.2.1', None, 'streaming', 'quorum', None), (2, 'replica2', '192.0.2.2', 'replica2', 'catchup', 'async', 0),
        (3, 'pg_basebackup', '192.0.2.3', None, 'backup', 'async', None),
    ]
    connection = Connection(senders=senders, overrides={info_db.BACKUPS_QUERY: [(3, '192.0.2.3', None, 'pg_basebackup', 'streaming database files', 10, None)]})
    target = collect(monkeypatch, connection)
    assert target.get('available') and target.get('role') == 'primary'
    assert [row.get('replay_lag_msec') for row in target.get('senders')] == [None, 0, None]
    assert [row.get('state') for row in target.get('senders')] == ['streaming', 'catchup', 'backup']
    assert target['basebackups'][0]['backup_total'] is None
    assert target['basebackups'][0]['pid'] == 3
    assert 'receive_lsn' in target and 'replay_lsn' in target
    assert target.get('replication_state') == ''
    assert connection.closed


def test_query_failure_keeps_source_available_and_completed_role(monkeypatch):
    error = info_db.psycopg2.DatabaseError('password=do-not-print')
    target = collect(monkeypatch, Connection(overrides={info_db.SENDERS_QUERY: error}))
    assert target.get('available') and target.get('role') == 'primary'
    assert target.get('senders') is None and target.get('basebackups') == []
    assert 'senders' not in target
    assert 'do-not-print' not in repr(target)


def test_full_replication_restores_all_detail_columns_from_sql_and_keeps_short_compact(monkeypatch):
    stamp = datetime(2026, 1, 2, 12, 0, 0, 123000, tzinfo=timezone(timedelta(hours=3)))
    child = 'child.example.net'
    target = collect(monkeypatch, Connection(senders=[
        (17, 'replica-app', '192.0.2.7', child, 'streaming', 'quorum', None,
         '0/4000', 0, 512, 1024, stamp, stamp),
    ]))
    sql = target
    full = report(False, sql=sql)
    assert full['replics_info'] == [{
        'pid': 17, 'application_name': 'replica-app', 'client_hostname': child, 'client_addr': '192.0.2.7',
        'state': 'streaming', 'primary_location': '0/4000', 'sent_location_diff': 0, 'write_location_diff': 512,
        'replay_location_diff': 1024, 'replay_lag_msec': None, 'backend_start_ts': '2026-01-02T09:00:00.123000Z',
        'reply_time_ms': '2026-01-02T09:00:00.123000Z', 'sync_state': 'quorum',
    }]
    assert full['sources']['replics_info']['source'] == 'db' and full['hosts_streaming'] == 1
    assert 'replics_info_details' not in full and 'replics_info_details' not in full['sources']
    assert report(sql=sql)['replics_info'] == {child: 'streaming, sync_state quorum, replay_lag_msec unknown'}


def test_unknown_current_sql_details_are_not_filled_with_stale_zk_values(monkeypatch):
    target = collect(monkeypatch, Connection(senders=[
        (17, 'app', None, 'child.example.net', 'streaming', 'quorum', None) + (None,) * 6,
    ]))
    stale = {
        'pid': 999, 'application_name': 'old-app', 'client_hostname': 'child.example.net', 'state': 'streaming', 'sync_state': 'sync', 'replay_lag_msec': 0,
        'primary_location': '0/FFFF', 'sent_location_diff': 10, 'backend_start_ts': 1700000000,
    }
    full = report(False, sql=target, zk={'replics_info': [stale]})
    row, = full['replics_info']
    assert row['pid'] == 17 and row['application_name'] == 'app'
    assert row['primary_location'] is row['sent_location_diff'] is row['backend_start_ts'] is row['replay_lag_msec'] is None
    assert full['sources']['replics_info']['source'] == 'db'


def test_full_sql_details_keep_rows_with_duplicate_and_missing_hostnames(monkeypatch):
    target = collect(monkeypatch, Connection(senders=[
        (index, 'same-app', '192.0.2.' + str(index), name, 'streaming', 'async', 0)
        for index, name in enumerate((None, 'duplicate.example.net', 'duplicate.example.net'), 1)
    ]))
    full = report(False, sql=target)
    assert full['hosts_streaming'] == 3 and len(full['replics_info']) == 3
    assert {row['pid'] for row in full['replics_info']} == {1, 2, 3}
    assert sum(row['client_hostname'] == 'duplicate.example.net' for row in full['replics_info']) == 2
    assert full['replics_info'][0]['client_hostname'] is None


def test_probe_failure_is_not_a_false_alive_measurement(monkeypatch):
    connection = Connection(overrides={'SELECT 1': info_db.psycopg2.OperationalError('lost')})
    with pytest.raises(info_db.psycopg2.OperationalError):
        collect(monkeypatch, connection)
    assert connection.queries == ['SELECT 1']
    assert connection.closed


@pytest.mark.parametrize('rows', [[(1, 'app', None, None, None, None, None)], [(1,)]])
def test_invalid_or_redacted_statistics_preserve_rows_with_unknown_fields(monkeypatch, rows):
    target = collect(monkeypatch, Connection(senders=rows))
    assert len(target['senders']) == len(rows)
    assert target['senders'][0]['state'] is None
    assert target.get('role') == 'primary'
    full = report(False, sql=target)
    assert len(full['replics_info']) == len(rows)
    assert full['hosts_streaming'] is None


def test_bad_sql_sender_field_and_row_do_not_erase_other_observations(monkeypatch):
    target = collect(monkeypatch, Connection(senders=[
        (1, 'good-app', None, 'child1', 'streaming', 'async', 0),
        (2, 'partial-app', None, 'child2', 'streaming', 123, 'bad lag'),
        (3,),
    ]))
    good, partial, broken = target['senders']
    assert good['client_hostname'] == 'child1' and good['replay_lag_msec'] == 0
    assert partial['pid'] == 2 and partial['state'] == 'streaming' and partial['client_hostname'] == 'child2'
    assert partial['sync_state'] is partial['replay_lag_msec'] is None
    assert all(value is None for value in broken.values())


def test_bad_upstream_publication_row_does_not_erase_current_hosts_statistics():
    publication = [None, {'pid': 1, 'application_name': 'replica', 'client_hostname': HOST,
                          'state': 'streaming', 'sync_state': 'quorum', 'replay_lag_msec': 7}]
    full = report(False, sql={'available': True, 'role': 'replica', 'receiver': {'sender_host': PRIMARY}},
                  zk_hosts={PRIMARY: publication})
    assert full['sync_state'] == 'quorum' and full['replay_lag_msec'] == 7
    assert full['sources']['sync_state'] == {'source': f'zk_host:{PRIMARY}', 'cached': False}


def test_receiver_preserves_all_previous_fields(monkeypatch):
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    target = collect(monkeypatch, Connection(recovery=True, receiver=[('streaming', PRIMARY, 5432, 'slot', stamp)]))
    assert target['role'] == 'replica'
    assert target['receiver'] == {'status': 'streaming', 'sender_host': PRIMARY, 'sender_port': 5432,
                                  'slot_name': 'slot', 'last_msg_receipt_time': '2026-01-01T00:00:00Z'}


@pytest.mark.parametrize('error', [info_db.psycopg2.OperationalError('lost'), info_db.psycopg2.InterfaceError('closed'), TimeoutError('stalled')])
def test_connection_failure_preserves_completed_fields_and_stops_further_queries(monkeypatch, error):
    connection = Connection(overrides={info_db.SENDERS_QUERY: error})
    target = collect(monkeypatch, connection)
    assert target['available'] is False and target['role'] == 'primary'
    assert info_db.WAL_QUERY not in connection.queries and info_db.BACKUPS_QUERY not in connection.queries
    assert connection.closed


def test_server_query_timeout_does_not_imply_connection_loss(monkeypatch):
    class QueryCanceled(info_db.psycopg2.OperationalError):
        pgcode = '57014'
    connection = Connection(overrides={info_db.SENDERS_QUERY: QueryCanceled('statement timeout')})
    target = collect(monkeypatch, connection)
    assert target['available'] is True and target['basebackups'] == []


def test_info_connects_only_to_local_dsn_even_with_upstream_and_legacy_remote_option(monkeypatch):
    config = db_config("host=/var/run/postgresql dbname=local user=reader password='a b'")
    config.set('global', 'info_remote_conn_string', 'host=other.example.net dbname=remote')
    connection = Connection(recovery=True, receiver=[('streaming', PRIMARY, 5432, 'slot', None)])
    connect = Mock(return_value=connection)
    parse = Mock(return_value={'host': '/var/run/postgresql', 'dbname': 'local', 'user': 'reader', 'password': 'a b'})
    monkeypatch.setattr(info_db, 'parse_dsn', parse)
    monkeypatch.setattr(info_db.psycopg2, 'connect', connect)
    monkeypatch.setattr(info_db, 'wait_async_operation', lambda *_: None)
    state = info_db.read_db_source(config)
    parse.assert_called_once_with(config.get('global', 'local_conn_string'))
    connect.assert_called_once()
    assert connect.call_args.kwargs['host'] == '/var/run/postgresql'
    assert connect.call_args.kwargs['password'] == 'a b'
    assert connect.call_args.kwargs['async_'] is True
    assert state['available'] and state['role'] == 'replica'
    assert state['receiver']['sender_host'] == PRIMARY
    assert connection.closed


def test_failed_local_connection_does_not_trigger_any_other_database_connection(monkeypatch):
    connect = Mock(side_effect=info_db.psycopg2.OperationalError('local unavailable'))
    monkeypatch.setattr(info_db, 'parse_dsn', lambda _: {'host': '/var/run/postgresql'})
    monkeypatch.setattr(info_db.psycopg2, 'connect', connect)
    state = info_db.read_db_source(db_config())
    assert state == {'available': False}
    connect.assert_called_once()
    full = report(False, sql=state, db={'role': 'replica', 'primary_fqdn': PRIMARY})
    assert full['db'] == 'only cache' and full['role'] == 'replica'
    assert 'db_targets' not in full and full['sources']['role']['source'] == 'db_cache'


@pytest.mark.parametrize('reported,resolved', [('192.0.2.2', '192.0.2.2'), ('192.0.2.2/32', '192.0.2.2'),
                                             ('2001:db8::2/128', '2001:db8:0:0:0:0:0:2')])
def test_own_zk_sender_matches_only_unique_hostname_or_address(reported, resolved):
    row = {'client_hostname': None, 'client_addr': reported}
    assert info_db.match_sender([row], HOST, (resolved,)) == row
    assert info_db.match_sender([row, row], HOST, (resolved,)) is None
    assert info_db.match_sender([row], HOST, ()) is None


def report(short=True, *, sql=None, zk=None, db=None, status=None, zk_hosts=None, local_addresses=()):
    return info.build_info(short, db, status, None,
                           zk, HOST, sql=sql, zk_hosts=zk_hosts, local_addresses=local_addresses)


def test_db_empty_downstream_beats_zk_and_caches_without_erasing_global_publication():
    local = {'available': True, 'role': 'primary', 'senders': [], 'replication_state': ''}
    sources = local
    row = {'client_hostname': 'other', 'state': 'streaming', 'sync_state': 'quorum', 'replay_lag_msec': 10}
    zk = {'info_host_replics_info': [row], 'replics_info': [row], 'lock_holder': PRIMARY}
    full = report(False, sql=sources, zk=zk, db={'role': 'replica', 'replics_info': [row]})
    assert full['role'] == 'primary' and full['sources']['role'] == {'source': 'db', 'cached': False}
    assert full['primary'] == PRIMARY
    assert full['primary_fqdn'] == HOST and full['sources']['primary_fqdn'] == {'source': 'db', 'cached': False}
    assert full['replics_info'] == [] and full['hosts_streaming'] == 0
    short = report(sql=sources, zk=zk)
    assert short['replics_info'] == [] and short['replication_state'] == 'async'
    assert short['role'] == 'primary' and short['db'] == 'available'
    assert 'alive' not in short
    assert 'primary' not in short and 'diagnostics' not in short


def test_local_receiver_cannot_supply_sender_sync_or_lag_and_missing_receiver_means_false():
    local = {'available': True, 'role': 'replica', 'receiver': None}
    sql = local
    short = report(sql=sql, zk={'lock_holder': PRIMARY})
    assert short['streaming'] is False
    assert short['sync_state'] is None and short['replay_lag_msec'] is None
    assert short['primary'] == PRIMARY and 'primary_info' not in short


def test_own_sync_and_lag_fall_back_to_known_upstream_publication_but_not_unknown_global():
    local = {'available': True, 'role': 'replica', 'receiver': {'status': 'streaming', 'sender_host': PRIMARY}}
    sources = local
    published = [
        {
            'pid': 3, 'application_name': 'replica', 'client_addr': '192.0.2.2', 'client_hostname': None, 'state': 'streaming', 'sync_state': 'quorum',
            'replay_lag_msec': 50,
        },
    ]
    zk = {'replics_info': published}
    assert report(sql=sources, zk=zk)['sync_state'] is None
    publications = {PRIMARY: published}
    fallback = report(sql=sources, zk=zk, zk_hosts=publications, local_addresses=('192.0.2.2',))
    assert fallback['sync_state'] == 'quorum' and fallback['replay_lag_msec'] == 50
    publications = {'different-producer': published}
    assert report(sql=sources, zk=zk, zk_hosts=publications)['sync_state'] is None


@pytest.mark.parametrize('cached', [None, {'role': 'replica', 'wal_receiver': {'slot_name': 'stale', 'conninfo': 'password=secret'}}])
def test_live_receiver_wins_as_a_complete_observation(cached):
    receiver = {'status': 'streaming', 'sender_host': PRIMARY, 'sender_port': 5432,
                'slot_name': 'current', 'last_msg_receipt_time': '2026-01-01T00:00:00Z'}
    full = report(False, sql={'available': True, 'role': 'replica', 'receiver': receiver}, db=cached)
    assert full['wal_receiver'] == {key: value for key, value in receiver.items() if key != 'last_msg_receipt_time'} | {
        'last_msg_receipt_time_msec': '2026-01-01T00:00:00Z'}
    assert full['sources']['wal_receiver'] == {'source': 'db', 'cached': False}


@pytest.mark.parametrize('observed', [True, False])
def test_absent_receiver_blocks_stale_fallback_but_failed_read_allows_it(observed):
    local = {'available': True, 'role': 'replica'}
    if observed:
        local['receiver'] = None
    cached = {'role': 'replica', 'primary_fqdn': PRIMARY,
              'wal_receiver': {'status': 'streaming', 'slot_name': 'stale', 'conninfo': 'password=secret'}}
    full = report(False, sql=local, db=cached)
    assert full['streaming'] is (not observed)
    assert full['sources']['wal_receiver']['source'] == ('db' if observed else 'db_cache')
    if observed:
        assert full['wal_receiver'] is None
        assert info.selected_upstream(local, cached) is None
    else:
        assert full['wal_receiver']['slot_name'] == 'stale'
    assert 'secret' not in repr(full)


def test_upstream_publication_reader_uses_its_host_path_without_mutations():
    from unittest.mock import Mock
    from src.zk import Zookeeper, ZookeeperConfig
    client = Mock()
    client.get.return_value = '[]'
    zk = Zookeeper(client, ZookeeperConfig(False, 1.0, '/pgconsul/'), init_lock=False)
    assert zk.get_stream_source_replics_info(PRIMARY) == []
    assert client.get.call_args.args[0] == 'all_hosts/' + PRIMARY + '/replics_info'
    client.write.assert_not_called()
    client.ensure_path.assert_not_called()
    client.make_lock.assert_not_called()


def test_db_partial_failure_falls_back_for_only_missing_fields():
    local = {'available': True, 'role': 'primary'}
    sql = local
    short = report(sql=sql, status={'role': 'replica', 'replication_state': ['sync', 'ANY 1 (*)'], 'replics_info': []})
    assert short['db'] == 'available' and short['role'] == 'primary'
    assert short['replication_state'] == 'sync, ANY 1 (*)'
    assert short['replics_info'] == []


def test_cache_availability_is_independent_by_section_and_empty_is_available(tmp_path):
    config = RawConfigParser()
    config.read_dict({'global': {'working_dir': str(tmp_path)}})
    (tmp_path / 'pgconsul.status').write_text(json.dumps({'zk_state': {}, 'db_state': []}))
    db, status, zk = info.read_local_sources(config)
    short = info.build_info(True, db, status, zk, {}, HOST)
    assert short['zk_cache'] == 'available'
    assert short['db_cache'] == short['status_db_cache'] == 'unavailable'
    assert short['db'] == 'unavailable' and short['role'] is None
    assert 'alive' not in short
    assert list(short)[:5] == ['db', 'zk', 'zk_cache', 'db_cache', 'status_db_cache']


def test_cached_alive_and_role_produce_only_cache_without_claiming_current_availability():
    short = report(db={'role': 'primary', 'alive': True, 'replics_info': []})
    assert short['db'] == 'only cache'
    assert short['role'] == 'primary' and 'alive' not in short
    assert short['replics_info'] == []


def test_switch_dates_are_utc_including_epoch_zero():
    full = report(False, zk={'last_failover_time': 0, 'last_switchover_time': 1.5})
    assert full['last_failover_time'] == '1970-01-01T00:00:00Z'
    assert full['last_switchover_time'] == '1970-01-01T00:00:01.500000Z'
    assert full['sources']['last_failover_time'] == {'source': 'zk', 'cached': False}


def test_basebackup_does_not_claim_resetup_or_streaming():
    local = {
        'available': True, 'role': 'primary',
        'senders': [
            {
                'pid': 1, 'application_name': 'pg_basebackup', 'client_addr': '192.0.2.5', 'client_hostname': None, 'state': 'backup', 'sync_state': 'async',
                'replay_lag_msec': None,
            },
        ],
        'basebackups': [
            {
                'client_addr': '192.0.2.5', 'client_hostname': None, 'application_name': 'pg_basebackup', 'phase': 'streaming database files',
                'backup_streamed': 50, 'backup_total': 100,
            },
        ],
    }
    full = report(False, sql=local)
    assert 'resetup_hosts' not in full
    assert full['hosts_streaming'] == 0 and full['replics_info'] == []
    assert full['sources']['hosts_streaming']['source'] == 'db'
    row = full['basebackups'][HOST][0]
    assert row['progress_pct'] == 50 and row['client_addr'] == '192.0.2.5'

