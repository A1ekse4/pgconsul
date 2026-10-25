from itertools import permutations
import multiprocessing
import signal
import time

import pytest

from src import info


def test_source_priority_does_not_depend_on_collection_order():
    sources = [(name, {'role': name}) for name in info.SOURCE_PRIORITY]
    for order in permutations(sources):
        values, origins = info.merge_sources(list(order))
        assert values['role'] == 'db'
        assert origins['role'] == {'source': 'db', 'cached': False}


@pytest.mark.parametrize('value', [False, 0, [], {}, ''])
def test_known_empty_values_do_not_trigger_fallback(value):
    values, origins = info.merge_sources([('db', {'field': value}), ('zk', {'field': 'fallback'})])
    assert values['field'] == value
    assert origins['field']['source'] == 'db'


def test_selection_is_independent_for_each_field():
    values, origins = info.merge_sources([
        ('db', {'role': 'primary', 'replication_state': None}),
        ('zk_cache', {'role': 'replica', 'maintenance': 'on'}),
        ('status_db_cache', {'replication_state': 'sync, ANY 1 (*)'}),
        ('db_cache', {'maintenance': 'off'}),
    ])
    assert values == {'role': 'primary', 'maintenance': 'on', 'replication_state': 'sync, ANY 1 (*)'}
    assert origins['maintenance'] == {'source': 'zk_cache', 'cached': True}
    assert origins['replication_state'] == {'source': 'status_db_cache', 'cached': True}


def test_host_publication_keeps_zk_priority():
    values, origins = info.merge_sources([
        ('zk_host:upstream', {'lag': 50}), ('db', {'lag': None}), ('db_cache', {'lag': 100}),
    ])
    assert values['lag'] == 50
    assert origins['lag'] == {'source': 'zk_host:upstream', 'cached': False}


def _blocked_source(*, publish):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    publish({'state': {'maintenance': {'status': 'enable'}}})
    time.sleep(60)


def _failed_source(*, publish):
    raise ValueError('password=never-print')


@pytest.mark.parametrize('reader', [_blocked_source, _failed_source])
def test_source_isolation_bounds_stalls_reaps_workers_and_sanitizes_errors(monkeypatch, reader):
    # Inherit the unit bootstrap; installed CLI integration covers spawn with real clients.
    context = multiprocessing.get_context('fork')
    monkeypatch.setattr(info.multiprocessing, 'get_context', lambda _: context)
    before = {child.pid for child in multiprocessing.active_children()}
    started = time.monotonic()
    if reader is _failed_source:
        with pytest.raises(RuntimeError, match='Info source failed: ValueError') as error:
            info._bounded_read(reader, (), 0.7, {})
        assert 'never-print' not in str(error.value)
    else:
        result, complete = info._bounded_read(reader, (), 0.7, {})
        assert result == {'state': {'maintenance': {'status': 'enable'}}}
        assert not complete
    assert time.monotonic() - started < 1.5
    assert {child.pid for child in multiprocessing.active_children()} == before


def test_interrupted_local_sql_collection_keeps_completed_fields(monkeypatch):
    sql = {'available': True, 'role': 'primary', 'replication_state': ''}
    results = iter([(sql, False), ({'hostname': 'local', 'state': None}, True)])
    monkeypatch.setattr(info, '_bounded_read', lambda *_: next(results))
    monkeypatch.setattr(info, 'read_local_sources', lambda *_: (None, None, None))
    report = info.collect_info(None, short=False)
    assert report['db'] == 'unavailable'
    assert report['role'] == 'primary'
    assert report['sources']['role']['source'] == 'db'
    assert 'db_targets' not in report


def _completed_source(*, publish):
    return {'value': 42}


def test_small_budget_still_attempts_source_read(monkeypatch):
    context = multiprocessing.get_context('fork')
    monkeypatch.setattr(info.multiprocessing, 'get_context', lambda _: context)
    result, complete = info._bounded_read(_completed_source, (), 0.2, {})
    assert complete and result == {'value': 42}


def test_collection_gives_each_source_five_seconds_and_passes_deadline_only_to_sql(monkeypatch):
    monkeypatch.setattr(info.time, 'monotonic', lambda: 100)
    monkeypatch.setattr(info, 'read_local_sources', lambda *_: (None, None, None))
    reads = []
    def bounded_read(read, args, timeout, default):
        reads.append((read, timeout))
        return default, True
    monkeypatch.setattr(info, '_bounded_read', bounded_read)
    info.collect_info(None, short=False)
    sql_read, sql_timeout = reads[0]
    zk_read, zk_timeout = reads[1]
    assert sql_timeout == zk_timeout == 5
    assert sql_read.func is info.info_db.read_db_source
    assert sql_read.keywords == {'deadline': 104.9}
    assert zk_read is info.read_zk_sources
