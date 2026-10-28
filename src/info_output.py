"""Queries, field names and ordering for pgconsul-util info."""

INFO_QUERIES = (
    ('role', 'SELECT pg_catalog.pg_is_in_recovery() AS recovery'),
    ('replication_state', 'SHOW synchronous_standby_names'),
    ('wal_receiver', '''SELECT status, sender_host, sender_port, slot_name,
            (1000 * EXTRACT(epoch FROM last_msg_receipt_time))::bigint AS last_msg_receipt_time_msec FROM pg_catalog.pg_stat_wal_receiver'''),
    ('replics_info', '''SELECT pid, application_name, client_hostname, pg_catalog.host(client_addr) AS client_addr,
            state, sync_state, (1000 * EXTRACT(epoch FROM replay_lag))::bigint AS replay_lag_msec,
            diagnostic_lsn::text AS primary_location,
            pg_catalog.pg_wal_lsn_diff(diagnostic_lsn, sent_lsn)::bigint AS sent_location_diff,
            pg_catalog.pg_wal_lsn_diff(diagnostic_lsn, write_lsn)::bigint AS write_location_diff,
            pg_catalog.pg_wal_lsn_diff(diagnostic_lsn, replay_lsn)::bigint AS replay_location_diff,
            EXTRACT(epoch FROM backend_start)::float8 AS backend_start_ts,
            (1000 * EXTRACT(epoch FROM reply_time))::bigint AS reply_time_ms
            FROM (SELECT r.*, CASE WHEN pg_catalog.pg_is_in_recovery()
                THEN pg_catalog.pg_last_wal_replay_lsn() ELSE pg_catalog.pg_current_wal_lsn() END AS diagnostic_lsn
                FROM pg_catalog.pg_stat_replication AS r OFFSET 0) AS replication'''),
)

FIELD_NAMES = {
    'hosts_total': 'cluster_hosts_total',
    'hosts_alive': 'cluster_hosts_alive',
    'alive_hosts': 'cluster_alive_hosts',
    'primary_fqdn': 'upstream_host',
    'streaming': 'upstream_streaming',
    'sync_state': 'upstream_sync_state',
    'replay_lag_msec': 'upstream_replay_lag_msec',
    'replication_state': 'downstream_replication_mode',
    'hosts_streaming': 'downstream_replicas_streaming',
    'replics_info': 'downstream_replicas',
}

FIELD_ORDER = (
    'db', 'zk', 'zk_cache', 'status_db_cache', 'db_cache',
    'host', 'role',
    'alive', 'running', 'pooler', 'maintenance', 'sessions_ratio', 'timeline',
    'primary', 'primary_fqdn',
    'streaming', 'sync_state', 'replay_lag_msec', 'wal_receiver',
    'replication_state', 'hosts_streaming', 'replics_info',
    'hosts_total', 'hosts_alive', 'alive_hosts', 'single_node', 'synchronous_standby_names',
    'failover_must_be_reset', 'failover_state', 'current_promoting_host',
    'switchover', 'switchover/candidate', 'switchover/side_replicas', 'switchover/state',
    'last_change', 'last_failover_time', 'last_switchover_time',
    'zk_alive', 'last_leader', 'lock_version', 'pgdata',
)


REPLICA_FIELD_ORDER = (
    'client_hostname', 'client_addr', 'application_name', 'state', 'sync_state', 'replay_lag_msec',
    'primary_location', 'sent_location_diff', 'write_location_diff', 'replay_location_diff',
    'reply_time_ms', 'backend_start_ts', 'pid',
)


def format_info(info):
    return {FIELD_NAMES.get(key, key): info[key] for key in (*FIELD_ORDER, *info) if key in info}
