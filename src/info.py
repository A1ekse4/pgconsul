"""Collect source dictionaries and select info fields in priority order."""

import json
import logging
import math
import multiprocessing
import os
import socket
import time
from datetime import datetime, timezone
from functools import partial
from multiprocessing.connection import Connection
from typing import Any, Callable

from . import helpers, info_db
from .info_output import AVAILABILITY_FIELDS, FULL_GROUPS
from .zk import create_zk, ZookeeperException
from .zk_client import ZkClientError


SOURCE_PRIORITY = ('db', 'zk', 'zk_cache', 'status_db_cache', 'db_cache')
REPLICA_FIELDS = ('streaming', 'sync_state', 'replay_lag_msec')
REPLICA_COLUMNS = ('pid', 'application_name', 'client_hostname', 'client_addr', 'state', 'primary_location',
                   'sent_location_diff', 'write_location_diff', 'replay_location_diff', 'replay_lag_msec',
                   'backend_start_ts', 'reply_time_ms', 'sync_state')
REPORT_FIELDS = tuple(name for group in FULL_GROUPS[1:] for name in group)
SOURCE_BUDGET = 5
PROCESS_CLEANUP_TIMEOUT = 0.1


def _source_worker(channel: Connection, read: Callable, args: tuple) -> None:
    try:
        result = read(*args, publish=lambda data: channel.send(('partial', data)))
        channel.send(('done', result))
    except Exception as error:
        channel.send(('error', type(error).__name__))
    finally:
        channel.close()


def _bounded_read(read: Callable, args: tuple, timeout: float, default: Any) -> tuple[Any, bool]:
    """Isolate blocking DNS, client startup and cleanup as well as network reads."""
    deadline = time.monotonic() + timeout
    read_deadline = deadline - PROCESS_CLEANUP_TIMEOUT
    context = multiprocessing.get_context('spawn')
    receive, send = context.Pipe(duplex=False)
    process = context.Process(target=_source_worker, args=(send, read, args))
    result, complete, failure = default, False, None
    try:
        process.start()
        send.close()
        while receive.poll(max(0, read_deadline - time.monotonic())):
            try:
                kind, value = receive.recv()
            except EOFError:
                break
            if kind == 'error':
                failure = value
                break
            result = value
            if kind == 'done':
                complete = True
                break
            if time.monotonic() >= read_deadline:
                break
    finally:
        send.close()
        if process.pid is not None:
            process.join(max(0, min(PROCESS_CLEANUP_TIMEOUT / 2, deadline - time.monotonic())))
            if process.is_alive():
                process.terminate()
                process.join(max(0, min(PROCESS_CLEANUP_TIMEOUT / 2, deadline - time.monotonic())))
            if process.is_alive():
                process.kill()
                process.join()
            process.close()
        receive.close()
    if failure is not None:
        raise RuntimeError(f'Info source failed: {failure}')
    return result, complete


def _read_json(path: str) -> dict[str, Any] | None:
    def reject_constant(value: str) -> None:
        raise ValueError(f'Invalid JSON constant: {value}')
    try:
        with open(path, encoding='utf-8') as stream:
            value = json.load(stream, parse_constant=reject_constant)
        return value if isinstance(value, dict) else None
    except (OSError, ValueError, UnicodeError):
        logging.info('Cannot read info source %s', path)
        return None


def read_local_sources(conf) -> tuple[dict | None, dict | None, dict | None]:
    directory = conf.get('global', 'working_dir')
    db_cache = _read_json(os.path.join(directory, '.pgconsul_db_state.cache'))
    status = _read_json(os.path.join(directory, 'pgconsul.status')) or {}
    return (db_cache, status.get('db_state') if isinstance(status.get('db_state'), dict) else None,
            status.get('zk_state') if isinstance(status.get('zk_state'), dict) else None)


def read_zk_sources(conf, hostname: str | None, upstream: str | None = None, *,
                    publish: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {'hostname': hostname or socket.gethostname(), 'state': None, 'hosts': {}}
    if hostname is None:
        try:
            result['hostname'] = helpers.get_hostname()
        except OSError:
            pass
    hostname = result['hostname']
    def update(state):
        result['state'] = state
        if publish:
            publish(result)
    if publish:
        publish(result)
    try:
        with create_zk(conf, lock_contender_name=hostname, allow_disconnected=True, init_lock=False) as zk:
            try:
                update(zk.get_info_state(hostname, publish=update))
            except (ZookeeperException, ZkClientError, ValueError):
                logging.info('ZK cluster info is unavailable')
            if upstream:
                rows = zk.get_stream_source_replics_info(upstream)
                if rows is not None:
                    result['hosts'][upstream] = rows
                    if publish:
                        publish(result)
                try:
                    result['local_addresses'] = tuple(sorted({str(row[4][0]) for row in socket.getaddrinfo(hostname, None)}))
                except OSError:
                    pass
    except (ZookeeperException, ZkClientError, ValueError):
        logging.info('ZK info source is unavailable')
    return result


def collect_info(conf, *, short: bool) -> dict[str, Any]:
    deadline = time.monotonic() + 2 * SOURCE_BUDGET
    db, status_db, status_zk = read_local_sources(conf)
    sql_timeout = min(SOURCE_BUDGET, deadline - time.monotonic())
    sql_reader = partial(info_db.read_db_source, deadline=time.monotonic() + sql_timeout - PROCESS_CLEANUP_TIMEOUT)
    sql, complete = _bounded_read(sql_reader, (conf,), sql_timeout, {'available': False})
    if not complete:
        sql['available'] = False
    upstream = selected_upstream(sql, status_db, db)
    zk, _ = _bounded_read(read_zk_sources, (conf, None, upstream), min(SOURCE_BUDGET, deadline - time.monotonic()),
                         {'hostname': socket.gethostname(), 'state': None, 'hosts': {}})
    hostname, zk_state = zk['hostname'], zk['state']
    return build_info(short, db, status_db, status_zk, zk_state, hostname, sql=sql, zk_hosts=zk.get('hosts', {}),
                      local_addresses=zk.get('local_addresses', ()))


def _utc(value: Any, *, milliseconds: bool = False, zero_unknown: bool = False) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        if isinstance(value, str) and 'T' in value:
            stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            return stamp.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        seconds = float(value) / (1000 if milliseconds else 1)
        if not math.isfinite(seconds) or (zero_unknown and seconds == 0):
            return None
        return datetime.fromtimestamp(seconds, timezone.utc).isoformat().replace('+00:00', 'Z')
    except (ValueError, OverflowError, OSError):
        return None


def _replicas(value: Any) -> list[dict[str, Any]] | None:
    if not isinstance(value, list):
        return None
    rows = []
    for item in value:
        item = item if isinstance(item, dict) else {}
        row: dict[str, Any] = {}
        for key in REPLICA_COLUMNS:
            field = item.get(key)
            if key in ('backend_start_ts', 'reply_time_ms'):
                field = _utc(field, milliseconds=key == 'reply_time_ms')
            elif key in ('pid', 'sent_location_diff', 'write_location_diff', 'replay_location_diff'):
                field = field if isinstance(field, int) and not isinstance(field, bool) else None
            elif key == 'replay_lag_msec':
                valid = isinstance(field, (int, float)) and not isinstance(field, bool)
                field = field if valid and (not isinstance(field, float) or math.isfinite(field)) else None
            else:
                field = field if isinstance(field, str) else None
            row[key] = field
        rows.append(row)
    return rows


def _streaming_count(rows: list[dict[str, Any]] | None) -> int | None:
    if rows is None or any(row['state'] is None for row in rows):
        return None
    return sum(row['state'] == 'streaming' for row in rows)


def _db_values(raw: dict | None, hostname: str, *, sql=False) -> dict[str, Any]:
    if raw is None:
        return {}
    values: dict[str, Any] = {'role': raw.get('role')} if sql else raw.copy()
    role = raw.get('role') if raw.get('role') in ('primary', 'replica') else None
    values['role'] = role
    values.pop('primary_fqdn', None)
    state = raw.get('replication_state')
    if isinstance(state, (list, tuple)) and len(state) == 2:
        mode, names = state
        state = 'sync, ' + names if mode == 'sync' and isinstance(names, str) else 'async' if mode == 'async' else None
    elif isinstance(state, str):
        state = ('sync, ' + state if sql and state else state) or 'async'
    else:
        state = None
    values['replication_state'] = state
    if not sql:
        opened = values.pop('opened', None)
        values['pooler'] = 'opened' if opened is True else 'closed' if opened is False else None
    values.pop('wal_receiver', None)
    receiver_key = 'receiver' if sql else 'wal_receiver'
    receiver = raw.get(receiver_key)
    if receiver_key in raw and (receiver is None or isinstance(receiver, dict)):
        values['wal_receiver'] = None if receiver is None else {key: value for key, value in receiver.items() if key != 'conninfo'}
        if isinstance(receiver, dict):
            stamp = receiver.get('last_msg_receipt_time' if sql else 'last_msg_receipt_time_msec')
            values['wal_receiver'].pop('last_msg_receipt_time', None)
            values['wal_receiver']['last_msg_receipt_time_msec'] = _utc(stamp, milliseconds=not sql, zero_unknown=not sql)
    if role == 'replica' and receiver_key in raw:
        status = receiver.get('status') if isinstance(receiver, dict) else None
        values['streaming'] = False if receiver is None else status == 'streaming' if isinstance(status, str) else None
    rows = _replicas(raw.get('senders' if sql else 'replics_info'))
    if sql and rows is not None:
        rows = [row for row in rows if row['application_name'] not in ('pg_basebackup', 'pg_receivewal') and row['state'] != 'backup']
    values['replics_info'] = rows
    values['hosts_streaming'] = _streaming_count(rows)
    if sql and role == 'primary':
        values['primary_fqdn'] = hostname
    return values


def _zk_values(raw: dict | None, *, cached=False) -> dict[str, Any]:
    if raw is None:
        return {}
    values = {key: value for key, value in raw.items() if key in REPORT_FIELDS or key == 'lock_holder'}
    values.pop('alive', None)
    alive = raw.get('alive')
    values['zk_alive'] = alive if isinstance(alive, bool) else None
    holder = values.pop('lock_holder', None)
    values['primary_fqdn'] = holder if isinstance(holder, str) and holder else None
    for name in ('last_failover_time', 'last_switchover_time'):
        values[name] = _utc(raw.get(name))
    dated = [(values[name], kind) for kind, name in (('failover', 'last_failover_time'), ('switchover', 'last_switchover_time')) if values[name] is not None]
    if dated:
        stamp, kind = max(dated, key=lambda item: datetime.fromisoformat(item[0].replace('Z', '+00:00')))
        values['last_change'] = f'{kind} at {stamp}'
    maintenance = raw.get('maintenance')
    status = maintenance.get('status') if isinstance(maintenance, dict) else None
    observed = isinstance(maintenance, dict) and 'status' in maintenance
    values['maintenance'] = 'on' if status == 'enable' else 'off' if observed and status in ('disable', '', None) else None
    ssn = raw.get('synchronous_standby_names')
    if isinstance(ssn, dict):
        values['synchronous_standby_names'] = {host: (pair[0], _utc(pair[1])) if isinstance(pair, (list, tuple)) and len(pair) == 2 else pair
                                               for host, pair in ssn.items()}
    members, alive = raw.get('info_members'), raw.get('info_alive_hosts')
    if isinstance(members, list):
        values['hosts_total'] = len(members)
        if isinstance(alive, list):
            values['hosts_alive'] = len(alive)
            if len(alive) != len(members):
                values['alive_hosts'] = sorted(alive)
    elif cached and isinstance(ssn, dict) and ssn:
        values['hosts_total'] = len(ssn)
    rows = _replicas(raw.get('info_host_replics_info'))
    values['replics_info'] = rows
    values['hosts_streaming'] = _streaming_count(rows)
    return values


def merge_sources(sources: list[tuple[str, dict[str, Any]]]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Select known values, including an explicitly observed absent receiver."""
    values: dict[str, Any] = {}
    origins: dict[str, Any] = {}
    order = {name: index for index, name in enumerate(SOURCE_PRIORITY)}
    order['zk_host'] = order['zk']
    for source, fields in sorted(sources, key=lambda item: order[item[0].split(':', 1)[0]]):
        for name, value in fields.items():
            if (value is not None or name == 'wal_receiver') and name not in origins:
                values[name] = value
                origins[name] = {'source': source, 'cached': source in SOURCE_PRIORITY[2:]}
    return values, origins


def selected_upstream(sql: dict[str, Any], *caches: dict | None) -> str | None:
    receiver = sql.get('receiver')
    if 'receiver' in sql or sql.get('role') == 'primary':
        host = receiver.get('sender_host') if isinstance(receiver, dict) else None
    else:
        host = next((cache.get('primary_fqdn') for cache in caches if cache and cache.get('role') == 'replica'
                     and isinstance(cache.get('primary_fqdn'), str)), None)
    return host if isinstance(host, str) and host and not any(c in host for c in ('/', ',', ' ', '\n', '\r', '\t', '=')) else None


def _short_replicas(rows):
    if rows is None:
        return None
    if not rows:
        return []
    def describe(row):
        lag = 'unknown' if row['replay_lag_msec'] is None else row['replay_lag_msec']
        state = row['state'] if row['state'] is not None else 'unknown'
        sync_state = row['sync_state'] if row['sync_state'] is not None else 'unknown'
        return f'{state}, sync_state {sync_state}, replay_lag_msec {lag}'
    names = [row['client_hostname'] for row in rows]
    rows = sorted(rows, key=lambda row: (row['client_hostname'] or '', row['state'] or '', row['sync_state'] or '', str(row['replay_lag_msec'])))
    if all(names) and len(names) == len(set(names)):
        return {row['client_hostname']: describe(row) for row in rows}
    return [{'host': row['client_hostname'], 'info': describe(row)} for row in rows]


def build_info(short: bool, db_cache: dict | None, status_db: dict | None, zk_cache: dict | None,
               zk_state: dict | None, hostname: str, *, sql: dict | None = None, zk_hosts: dict | None = None,
               local_addresses: tuple[str, ...] = ()) -> dict[str, Any]:
    local = sql or {}
    sources = [('db', _db_values(local, hostname, sql=True)), ('zk', _zk_values(zk_state))]
    upstream = selected_upstream(local, status_db, db_cache)
    publications = zk_hosts or {}
    if upstream and upstream in publications:
        rows = info_db.published_senders(publications[upstream])
        published_row = info_db.match_sender(rows, hostname, local_addresses) if rows is not None else None
        if published_row is not None:
            sources.append((f'zk_host:{upstream}', {key: published_row[key] for key in ('sync_state', 'replay_lag_msec')}))
    sources.extend((('zk_cache', _zk_values(zk_cache, cached=True)),
                    ('status_db_cache', _db_values(status_db, hostname)), ('db_cache', _db_values(db_cache, hostname))))
    values, origins = merge_sources(sources)
    availability = {name: 'available' if data is not None else 'unavailable'
                    for name, data in zip(AVAILABILITY_FIELDS, (local if local.get('available') else None, zk_state, zk_cache, db_cache, status_db))}
    if not local.get('available') and (db_cache is not None or status_db is not None):
        availability['db'] = 'only cache'
    role = values.get('role')
    if short:
        names = ('replication_state',) if role == 'primary' else REPLICA_FIELDS if role == 'replica' else ()
        result = {**availability, 'host': hostname, 'role': role, **{name: values.get(name) for name in names},
                  'last_failover_time': values.get('last_failover_time'), 'maintenance': values.get('maintenance')}
        primary = values.get('primary_fqdn')
        if primary != hostname:
            result['primary'] = primary
        result['replics_info'] = _short_replicas(values.get('replics_info'))
        return result
    fields = dict.fromkeys(REPORT_FIELDS)
    result = {**availability, **fields, **{name: value for name, value in values.items() if not name.startswith('_')}, 'host': hostname}
    result['primary'] = (zk_state or {}).get('lock_holder')
    origins['primary'] = {'source': 'zk' if result['primary'] is not None else None, 'cached': False if result['primary'] is not None else None}
    backups: dict[str, Any] = {}
    backup_sources: dict[str, Any] = {}
    if local.get('basebackups') is not None:
        backups[hostname] = [{'donor': hostname, 'host': row['client_hostname'], **{key: row[key] for key in
                         ('client_addr', 'application_name', 'phase', 'backup_streamed', 'backup_total')},
                         'progress_pct': round(100 * row['backup_streamed'] / row['backup_total'], 1) if row['backup_total'] else None}
                        for row in local['basebackups']]
        backup_sources[hostname] = {'source': 'db', 'cached': False, 'context': f'host:{hostname}'}
    result['basebackups'] = backups or None
    if backups:
        origins['basebackups'] = backup_sources
    if role != 'replica':
        for name in REPLICA_FIELDS:
            result.pop(name, None)
    if 'alive_hosts' not in values:
        result.pop('alive_hosts', None)
    result['sources'] = {name: origins.get(name, {'source': None, 'cached': None}) for name in result
                         if name not in (*AVAILABILITY_FIELDS, 'host')}
    return result
