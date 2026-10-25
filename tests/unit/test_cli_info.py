import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import src
src.read_config = Mock()
src.init_logging = Mock()
from src import cli, info


HOST = 'primary.example.net'


@pytest.fixture(autouse=True)
def inline_source_collection(monkeypatch):
    monkeypatch.setattr(info, '_bounded_read', lambda read, args, timeout, default: (read(*args), True))


def test_info_collects_plain_source_dictionaries(monkeypatch):
    db = {'role': 'primary', 'alive': True, 'opened': False, 'replics_info': []}
    live = {'alive': True, 'replics_info': [], 'lock_holder': HOST, 'info_members': [HOST], 'info_alive_hosts': [HOST], 'maintenance': {'status': None}}
    monkeypatch.setattr(info.helpers, 'get_hostname', lambda: HOST)
    monkeypatch.setattr(info, 'read_local_sources', lambda *_: (db, {'role': 'primary'}, {}))
    monkeypatch.setattr(info, 'read_zk_sources', lambda *_: {'hostname': HOST, 'state': live})
    monkeypatch.setattr(info.info_db, 'read_db_source', lambda *_, **__: {})
    full = cli._show_info(SimpleNamespace(short=False), Mock())
    short = cli._show_info(SimpleNamespace(short=True), Mock())
    assert full['pooler'] == 'closed' and 'opened' not in full
    assert full['alive'] is True and full['primary'] == HOST and full['replics_info'] == []
    assert short == {
        'db': 'only cache', 'zk': 'available', 'zk_cache': 'available', 'db_cache': 'available', 'status_db_cache': 'available',
        'host': HOST, 'role': 'primary', 'replication_state': None,
        'last_failover_time': None, 'maintenance': 'off', 'replics_info': [],
    }


def test_zk_failure_keeps_db_cache_and_unknown_coordination_fields():
    report = info.build_info(False, {'role': 'replica', 'opened': False}, None, None, None, HOST)
    assert report['primary'] is None and report['alive'] is None and report['replics_info'] is None
    assert report['pooler'] == 'closed' and report['role'] == 'replica'
    assert report['sources']['role'] == {'source': 'db_cache', 'cached': True}


def test_show_info_places_source_below_value_in_full_json(monkeypatch, capsys):
    document = {'host': HOST, 'maintenance': 'off', 'sources': {'maintenance': {'source': 'zk', 'cached': False}}}
    monkeypatch.setattr(cli, '_show_info', lambda *_: document)
    cli.show_info(SimpleNamespace(json=True, short=False), Mock())
    assert json.loads(capsys.readouterr().out) == {'host': HOST, 'maintenance': {'value': 'off', 'source': 'zk'}}


def test_short_json_keeps_repeated_and_missing_names(monkeypatch, capsys):
    rows = [{'client_hostname': name, 'state': 'streaming', 'sync_state': 'quorum', 'replay_lag_msec': 0}
            for name in ('replica.example.net', None, 'replica.example.net')]
    document = info.build_info(True, None, None, None, {'info_host_replics_info': rows}, HOST)
    monkeypatch.setattr(cli, '_show_info', lambda *_: document)
    cli.show_info(SimpleNamespace(json=True, short=True), Mock())
    parsed = json.loads(capsys.readouterr().out)
    assert 'diagnostics' not in parsed and len(parsed['replics_info']) == 3
    assert sum(item['host'] == 'replica.example.net' for item in parsed['replics_info']) == 2
    assert any(item['host'] is None for item in parsed['replics_info'])


def test_invalid_info_arguments_exit_two(monkeypatch):
    monkeypatch.setattr('sys.argv', ['pgconsul-util', 'info', '--invalid'])
    with pytest.raises(SystemExit) as error:
        cli.parse_args()
    assert error.value.code == 2


def test_command_bug_exits_one(monkeypatch):
    def fail(*_):
        raise ValueError('programming error')
    monkeypatch.setattr(cli, 'parse_args', lambda: SimpleNamespace(config_file='unused', action=fail))
    monkeypatch.setattr(cli, 'read_config', Mock())
    monkeypatch.setattr(cli, 'init_logging', Mock())
    with pytest.raises(SystemExit) as error:
        cli.entry()
    assert error.value.code == 1


def test_sql_data_survives_zk_unavailability(monkeypatch):
    monkeypatch.setattr(info.helpers, 'get_hostname', lambda: HOST)
    monkeypatch.setattr(info, 'read_local_sources', lambda *_: (None, None, None))
    monkeypatch.setattr(info, 'read_zk_sources', lambda *_: {'hostname': HOST, 'state': None})
    monkeypatch.setattr(info.info_db, 'read_db_source', lambda *_, **__: {'available': True, 'role': 'primary', 'senders': []})
    document = cli._show_info(SimpleNamespace(short=True), Mock())
    assert document['db'] == 'available' and document['zk'] == 'unavailable'
    assert document['role'] == 'primary' and document['replics_info'] == []
