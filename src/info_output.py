"""Format selected info fields with their sources in display order."""

from typing import Any

import yaml


AVAILABILITY_FIELDS = ('db', 'zk', 'zk_cache', 'db_cache', 'status_db_cache')
FULL_GROUPS = (
    AVAILABILITY_FIELDS,
    ('host', 'role', 'alive', 'running', 'pooler', 'pgdata', 'sessions_ratio'),
    ('primary_fqdn', 'primary', 'zk_alive', 'single_node', 'hosts_total', 'hosts_alive', 'alive_hosts', 'last_leader', 'timeline', 'lock_version'),
    ('replication_state', 'streaming', 'sync_state', 'replay_lag_msec', 'wal_receiver'),
    ('hosts_streaming', 'replics_info', 'synchronous_standby_names'),
    ('last_change', 'last_failover_time', 'last_switchover_time', 'failover_state', 'failover_must_be_reset', 'current_promoting_host',
     'switchover', 'switchover/candidate', 'switchover/side_replicas', 'switchover/state'),
    ('maintenance',),
    ('basebackups',),
)


def format_full(report: dict[str, Any]) -> dict[str, Any]:
    fields = {name: report[name] for group in FULL_GROUPS for name in group if name in report}
    fields.update({name: value for name, value in report.items() if name not in fields and name != 'sources'})
    origins = report.get('sources', {})
    for name, value in fields.items():
        if name in (*AVAILABILITY_FIELDS, 'host'):
            continue
        origin = origins.get(name, {})
        source = origin.get('source') if 'source' in origin else {host: item.get('source') for host, item in origin.items()} or None
        fields[name] = {'value': value, 'source': source}
    return fields


def render_full(report: dict[str, Any]) -> str:
    return yaml.safe_dump(format_full(report), sort_keys=False, indent=4, width=200).rstrip()
