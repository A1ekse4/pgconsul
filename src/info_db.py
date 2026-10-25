"""Read-only PostgreSQL access for pgconsul-util info."""

import ipaddress
import logging
import time
from configparser import RawConfigParser
from datetime import datetime, timezone
from typing import Any, Callable

import psycopg2
from psycopg2.extensions import parse_dsn

from .pg import wait_async_operation


ROLE_QUERY = "SELECT pg_catalog.pg_is_in_recovery(), pg_catalog.current_setting('server_version_num')::integer"
SENDERS_QUERY = """SELECT pid, application_name, pg_catalog.host(client_addr), client_hostname,
    state, sync_state, (1000 * EXTRACT(epoch FROM replay_lag))::bigint,
    diagnostic_lsn::text,
    pg_catalog.pg_wal_lsn_diff(diagnostic_lsn, sent_lsn)::bigint,
    pg_catalog.pg_wal_lsn_diff(diagnostic_lsn, write_lsn)::bigint,
    pg_catalog.pg_wal_lsn_diff(diagnostic_lsn, replay_lsn)::bigint,
    backend_start, reply_time
    FROM (SELECT r.*, CASE WHEN pg_catalog.pg_is_in_recovery()
        THEN pg_catalog.pg_last_wal_replay_lsn() ELSE pg_catalog.pg_current_wal_lsn() END AS diagnostic_lsn
        FROM pg_catalog.pg_stat_replication AS r OFFSET 0) AS replication"""
RECEIVER_QUERY = """SELECT status, sender_host, sender_port, slot_name, last_msg_receipt_time
    FROM pg_catalog.pg_stat_wal_receiver"""
WAL_QUERY = 'SELECT pg_catalog.pg_last_wal_receive_lsn()::text, pg_catalog.pg_last_wal_replay_lsn()::text'
BACKUPS_QUERY = """SELECT p.pid, pg_catalog.host(r.client_addr), r.client_hostname, r.application_name,
    p.phase, p.backup_streamed, p.backup_total
    FROM pg_catalog.pg_stat_progress_basebackup AS p
    LEFT JOIN pg_catalog.pg_stat_replication AS r USING (pid)"""


def _text(value: Any, *, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    if not isinstance(value, str):
        raise ValueError('Expected a text SQL column; check statistics privileges')
    return value


def _integer(value: Any, *, optional: bool = False) -> int | None:
    if optional and value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError('Expected an integer SQL column')
    return value


def _one(rows: list[tuple], width: int) -> tuple:
    if len(rows) != 1 or len(rows[0]) != width:
        raise ValueError('Unexpected SQL result shape')
    return rows[0]


def _datetime_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError('Expected a timestamp with time zone')
    return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def _sender(row: tuple) -> dict[str, Any]:
    columns: tuple[tuple[str, Callable[[Any], Any]], ...] = (
        ('pid', _integer), ('application_name', _text), ('client_addr', _text), ('client_hostname', _text),
        ('state', _text), ('sync_state', _text), ('replay_lag_msec', _integer), ('primary_location', _text),
        ('sent_location_diff', _integer), ('write_location_diff', _integer), ('replay_location_diff', _integer),
        ('backend_start_ts', _datetime_text), ('reply_time_ms', _datetime_text),
    )
    if len(row) != len(columns):
        return dict.fromkeys(name for name, _ in columns)
    result: dict[str, Any] = {}
    for (name, normalize), value in zip(columns, row):
        try:
            result[name] = normalize(value)
        except ValueError:
            result[name] = None
    return result


class InfoPostgres:
    """Read-only SQL access to this host's configured PostgreSQL instance."""

    def __init__(self, conn_string: str, *, deadline: float | None = None):
        self._conn_string = conn_string
        self._server_version_num: int | None = None
        self._connection: Any = None
        self._deadline = deadline if deadline is not None else time.monotonic() + 5

    def connect(self) -> None:
        if not self._conn_string:
            raise ValueError('No diagnostic connection configured')
        parameters = parse_dsn(self._conn_string)
        parameters.update(application_name='pgconsul-util-info', connect_timeout='2')
        parameters['options'] = (parameters.get('options', '') + ' -c statement_timeout=2000').strip()
        self.close()
        self._server_version_num = None
        self._connection = psycopg2.connect(async_=True, **parameters)
        try:
            wait_async_operation(self._connection, self._deadline)
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except psycopg2.Error:
                pass
            finally:
                self._connection = None

    def __enter__(self) -> 'InfoPostgres':
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _query(self, query: str) -> list[tuple]:
        if time.monotonic() >= self._deadline:
            raise TimeoutError('SQL info deadline exceeded')
        if self._connection is None:
            raise psycopg2.InterfaceError('Diagnostic connection is not open')
        with self._connection.cursor() as cursor:
            cursor.execute(query)
            wait_async_operation(self._connection, self._deadline)
            return cursor.fetchall()

    def is_alive(self) -> bool:
        if _one(self._query('SELECT 1'), 1)[0] != 1:
            raise ValueError('Unexpected probe result')
        return True

    def get_role(self) -> str:
        recovery, version = _one(self._query(ROLE_QUERY), 2)
        if not isinstance(recovery, bool):
            raise ValueError('Expected a boolean recovery result')
        self._server_version_num = _integer(version)
        return 'replica' if recovery else 'primary'

    def get_replication_state(self) -> str:
        value = _one(self._query('SHOW synchronous_standby_names'), 1)[0]
        if not isinstance(value, str):
            raise ValueError('Expected synchronous_standby_names text')
        return value

    def get_senders(self) -> list[dict[str, Any]]:
        return [_sender(row) for row in self._query(SENDERS_QUERY)]

    def get_receiver(self) -> dict[str, Any] | None:
        rows = self._query(RECEIVER_QUERY)
        if not rows:
            return None
        status, host, port, slot, stamp = _one(rows, 5)
        return {'status': _text(status), 'sender_host': _text(host, optional=True),
                'sender_port': _integer(port, optional=True), 'slot_name': _text(slot, optional=True),
                'last_msg_receipt_time': _datetime_text(stamp)}

    def get_wal_lsns(self) -> tuple[str | None, str | None]:
        received, replayed = _one(self._query(WAL_QUERY), 2)
        return _text(received, optional=True), _text(replayed, optional=True)

    def get_basebackups(self) -> list[dict[str, Any]]:
        result = []
        for row in self._query(BACKUPS_QUERY):
            if len(row) != 7:
                raise ValueError('Unexpected basebackup result shape')
            result.append({
                'pid': _integer(row[0]), 'client_addr': _text(row[1], optional=True),
                'client_hostname': _text(row[2], optional=True), 'application_name': _text(row[3], optional=True),
                'phase': _text(row[4]), 'backup_streamed': _integer(row[5]), 'backup_total': _integer(row[6], optional=True),
            })
        return result

    def get_state(self, publish: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:
        data: dict[str, Any] = {'available': self.is_alive()}
        if publish:
            publish(data)
        reads: tuple[tuple[str, Callable[[], Any]], ...] = (
            ('role', self.get_role), ('replication_state', self.get_replication_state),
            ('receiver', self.get_receiver), ('senders', self.get_senders), ('wal_lsns', self.get_wal_lsns),
            ('basebackups', self.get_basebackups),
        )
        for name, read in reads:
            if name == 'basebackups' and (self._server_version_num is None or self._server_version_num < 130000):
                continue
            try:
                value = read()
                if name == 'wal_lsns':
                    data['receive_lsn'], data['replay_lsn'] = value
                else:
                    data[name] = value
            except (psycopg2.Error, ValueError, TimeoutError) as error:
                logging.info('Cannot read SQL info field %s: %s', name, type(error).__name__)
                if isinstance(error, (TimeoutError, psycopg2.InterfaceError)) or (
                    isinstance(error, psycopg2.OperationalError) and
                    (self._connection.closed or not getattr(error, 'pgcode', None))
                ):
                    data['available'] = False
            data['server_version_num'] = self._server_version_num
            if publish:
                publish(data)
            if not data['available']:
                break
        return data


def create_info_db(config: RawConfigParser, *, deadline: float | None = None) -> InfoPostgres:
    return InfoPostgres(config.get('global', 'local_conn_string', fallback=''), deadline=deadline)


def read_db_source(config: RawConfigParser, *, publish: Callable[[dict[str, Any]], None] | None = None,
                   deadline: float | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {'available': False}
    def update(data):
        result.update(data)
        if publish:
            publish(result)
    try:
        with create_info_db(config, deadline=deadline) as db:
            update(db.get_state(update))
    except (psycopg2.Error, ValueError, TimeoutError) as error:
        result['available'] = False
        logging.info('Cannot read local SQL info: %s', type(error).__name__)
    return result


def published_senders(value: Any) -> list[dict[str, Any]] | None:
    if not isinstance(value, list):
        return None
    return [_sender(tuple(row.get(key) for key in ('pid', 'application_name', 'client_addr', 'client_hostname',
                                                  'state', 'sync_state', 'replay_lag_msec')) + (None,) * 6)
            if isinstance(row, dict) else _sender(()) for row in value]


def match_sender(senders: list[dict[str, Any]], hostname: str, local_addresses: tuple[str, ...]) -> dict[str, Any] | None:
    def normalized(address: str) -> str:
        try:
            return str(ipaddress.ip_interface(address).ip)
        except ValueError:
            return address
    addresses = {normalized(address) for address in local_addresses}
    matches = [sender for sender in senders if sender.get('client_hostname') == hostname
               or (sender.get('client_addr') is not None and normalized(sender['client_addr']) in addresses)]
    return matches[0] if len(matches) == 1 else None
