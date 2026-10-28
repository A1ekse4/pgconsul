# coding: utf-8
"""
Various utility functions:
    - Zookeeper structures init
    - Scheduled switchover
"""
import argparse
import contextlib
import functools
import json
import yaml
import socket
import sys
import logging
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extensions import parse_dsn

from . import read_config, init_logging
from .zk import create_zk, Zookeeper, ZookeeperException
from . import helpers
from . import utils
from .exceptions import SwitchoverException, FailoverException, ResetException
from .info_output import INFO_QUERIES, REPLICA_FIELD_ORDER, format_info
from .pg import wait_async_operation
from .zk_client import ZkClientError


class ParseHosts(argparse.Action):
    """
    Check validity of provided hostnames
    """

    def __call__(self, parser, namespace, values, option_string=None):
        for value in values:
            try:
                socket.getaddrinfo(value, 0)
            except Exception as exc:
                raise ValueError('invalid hostname: %s: %s' % (value, exc))
            namespace.members.append(value)


def entry():
    """
    Entry point.
    """
    opts = parse_args()
    conf = read_config(
        filename=opts.config_file,
        options=opts,
    )
    init_logging(conf, is_foreground=True)
    try:
        opts.action(opts, conf)
    except (KeyboardInterrupt, EOFError):
        logging.error('abort')
        sys.exit(1)
    except RuntimeError as err:
        logging.error(err)
        sys.exit(1)
    except Exception as exc:
        logging.exception(exc)
        sys.exit(1)


def maintenance_enabled(zk: Zookeeper):
    """
    Returns True if all hosts confirmed that maintenance is enabled.
    """
    for host in zk.get_alive_hosts():
        if zk.get_host_maintenance_status(host) != 'enable':
            return False
    return True


def maintenance_disabled(zk: Zookeeper):
    """
    Common maintenance node should be deleted
    """
    return zk.get_maintenance_status() is None


def _wait_maintenance_enabled(zk: Zookeeper, timeout):
    is_maintenance_enabled = functools.partial(maintenance_enabled, zk)
    if not helpers.await_for(is_maintenance_enabled, timeout, 'enabling maintenance mode'):
        # Return cluster to last state, i.e. disable maintenance.
        zk.write_maintenance_status('disable')
        raise TimeoutError
    logging.info('Success')


def _wait_maintenance_disabled(zk: Zookeeper, timeout):
    is_maintenance_disabled = functools.partial(maintenance_disabled, zk)
    if not helpers.await_for(is_maintenance_disabled, timeout, 'disabling maintenance mode'):
        # Return cluster to the last state, i.e. enable maintenance.
        # There is obvious race condition between time when primary deletes this node
        # and we write value here. We assume that big timeout will help us here.
        zk.write_maintenance_status('enable')
        raise TimeoutError
    logging.info('Success')


def enable_maintenance(zk: Zookeeper, timeout, wait_all):
    zk.write_maintenance_status('enable')
    if wait_all:
        _wait_maintenance_enabled(zk, timeout)


def maintenance(opts, conf):
    """
    Enable or disable maintenance mode.
    """
    with create_zk(config=conf) as zk:
        if opts.mode == 'enable':
            enable_maintenance(zk, opts.timeout, opts.wait_all)
        elif opts.mode == 'disable':
            zk.write_maintenance_status('disable')
            if opts.wait_all:
                _wait_maintenance_disabled(zk, opts.timeout)
        elif opts.mode == 'show':
            val = zk.get_maintenance_status() or 'disable'
            print('{val}d'.format(val=val))


def initzk(opts, conf):
    """
    Creates structures in zk.MEMBERS_PATH corresponding
    to members` names or checks if it has been done earlier.
    ! We override iteration_timeout here because it's timeout for ZK operations,
    for initzk is not important how fast zk response, but it's use in cluster restore
    and can fail if zk didn't response for 1 second
    """
    conf.set('global', 'iteration_timeout', 5)
    try:
        zk: Zookeeper = create_zk(config=conf,)
    except Exception as exc:
        # ZK is unreachable: connection failure is indistinguishable from "not initialized".
        if opts.test:
            logging.exception('Could not connect to ZK (KazooTimeoutError)')
            sys.exit(2)
        path = f'{Zookeeper.MEMBERS_PATH}/{opts.members[0]}'
        raise RuntimeError(f'Could not create path "{path}" in ZK') from exc
    with zk:
        for host in opts.members:
            if opts.test:
                path = zk.get_member_path(host)
                logging.debug(f'Fetching path "{path}"...')
                if not zk.member_exists(host):
                    logging.debug(f'Path "{path}" not found in ZK, initialization has not been performed earlier')
                    sys.exit(2)
            else:
                path = zk.get_member_path(host)
                logging.debug('creating "%s"...', path)
                if not zk.ensure_member(host):
                    raise RuntimeError(f'Could not create path "{path}" in ZK')
        if opts.test:
            logging.debug('Initialization for all fqdns has been performed earlier')
        else:
            logging.debug('ZK structures are initialized')


def switchover(opts, conf):
    """
    Perform planned switchover.
    """
    if opts.yes:
        logging.warning('The -y/--yes option is deprecated and will be removed soon')
    try:
        switch = utils.Switchover(
            conf=conf, primary=opts.primary, timeline=opts.timeline, new_primary=opts.destination, timeout=opts.timeout, from_cli=True,
        )
        if opts.reset:
            return switch.reset(force=True)
        is_switchover_possible = switch.plan_switchover()
        if not is_switchover_possible:
            sys.exit(1)
        logging.info('switchover %(primary)s (timeline: %(timeline)s)', switch.plan())
        # perform returns False on soft-fail.
        # right now it happens when an unexpected host has become
        # the new primary instead of intended sync replica.
        if not switch.is_possible():
            logging.error('Switchover is impossible now.')
            sys.exit(1)
        if not switch.perform(opts.replicas, block=opts.block):
            sys.exit(2)
    except SwitchoverException as exc:
        logging.error('unable to switchover: %s', exc)
        sys.exit(1)


def failover(opts, conf):
    """
    Operations during failover.
    """
    try:
        fail = utils.Failover(conf=conf)
        if opts.reset:
            return fail.reset()
    except FailoverException as exc:
        logging.error('unable to reset failover state: %s', exc)
        sys.exit(1)


def reset_all(opts, conf):
    """
    Resets all nodes in ZK, except for zk.MEMBERS_PATH
    """
    conf.set('global', 'iteration_timeout', 5)
    with create_zk(config=conf) as zk:
        logging.debug("resetting all ZK nodes")
        all_nodes = zk.get_root_children()
        if all_nodes is None:
            logging.error("Could not get nodes to reset")
            all_nodes = []
        nodes_to_delete = [x for x in all_nodes if x not in (zk.MEMBERS_PATH, zk.MAINTENANCE_PATH)]
        if not opts.force:
            prompt = f'Nodes to delete: {", ".join(nodes_to_delete + [zk.MAINTENANCE_PATH])}\n' \
                     f'This is a potentially dangerous action. Proceed [y/n]?\n'
            ans = input(prompt)
            if ans == 'n':
                return
            elif ans != 'y':
                print('Incorrect value, please type "y" or "n"')
                return
        enable_maintenance(zk, opts.timeout, True)
        for node in nodes_to_delete:
            logging.debug(f'resetting path "{node}"')
            if not zk.delete(node, recursive=True):
                raise ResetException(f'Could not reset node "{node}" in ZK')

        logging.debug(f'resetting path "{zk.MAINTENANCE_PATH}"')
        try:
            zk.write_maintenance_status('disable')
            # An in-flight enable iteration may add a child; daemons will retry after seeing disable.
            zk.delete_maintenance()
            deleted = helpers.await_for(
                functools.partial(maintenance_disabled, zk), opts.timeout, 'deleting maintenance node'
            )
        except ZookeeperException as exc:
            raise ResetException(f'Could not reset node "{zk.MAINTENANCE_PATH}" in ZK') from exc
        if not deleted:
            raise ResetException(f'Could not reset node "{zk.MAINTENANCE_PATH}" in ZK')
        logging.debug("ZK structures are reset")


def show_info(opts, conf):
    """
    Show cluster's information
    """
    info = _show_info(opts, conf)
    if info is not None:
        if opts.json:
            print(json.dumps(info, sort_keys=False, indent=4))
        else:
            print(yaml.safe_dump(info, sort_keys=False, indent=4, width=200))


def _show_info(opts, conf):
    try:
        hostname = helpers.get_hostname()
    except OSError:
        hostname = socket.gethostname()
    zk_state = None
    try:
        with create_zk(config=conf, allow_disconnected=True) as zk:
            zk_state = zk.get_state()
    except (ZookeeperException, ZkClientError, ValueError):
        logging.info('ZK info source is unavailable')
    sql = _get_info_db_state(conf)
    status = _get_db_state(conf, 'pgconsul.status') or {}
    sources = [('db', sql if sql.get('available') else None), ('zk', zk_state),
               ('zk_cache', status.get('zk_state')), ('status_db_cache', status.get('db_state')),
               ('db_cache', _get_db_state(conf))]
    availability = {name: 'available' if isinstance(raw, dict) else 'unavailable' for name, raw in sources}
    if not sql.get('available') and any(availability[name] == 'available' for name in ('db_cache', 'status_db_cache')):
        availability['db'] = 'only cache'
    values, origins = _merge_info(sources, hostname)
    role = values.get('role')
    if role == 'primary':
        values['primary_fqdn'], origins['primary_fqdn'] = None, None
    if opts.short:
        role_fields = {
            'primary': ('replication_state',),
            'replica': ('streaming', 'sync_state', 'replay_lag_msec'),
        }.get(role, ())
        fields = ('role', 'primary', 'last_failover_time', 'maintenance', *role_fields)
        result = {**availability, 'host': hostname, **{key: values.get(key) for key in fields},
                  'replics_info': _short_replica_infos(values.get('replics_info'))}
        if role != 'primary':
            result['primary_fqdn'] = values.get('primary_fqdn')
        return format_info(result)
    return format_info({**availability, 'host': hostname,
                        **{key: {'value': value, 'source': origins.get(key)} for key, value in values.items()}})


def _merge_info(sources, hostname):
    values: dict = dict.fromkeys(('role', 'primary', 'primary_fqdn', 'replics_info', 'maintenance', 'replication_state'))
    origins: dict = {}
    for name, raw in sources:
        if not isinstance(raw, dict):
            continue
        for key, value in _info_values(raw, name, hostname).items():
            if key not in origins:
                values[key], origins[key] = value, name
    changes = [(values[key], key) for key in ('last_failover_time', 'last_switchover_time') if values.get(key)]
    if changes:
        stamp, key = max(changes, key=lambda item: datetime.fromisoformat(item[0].replace('Z', '+00:00')))
        values['last_change'] = f'{"failover" if key == "last_failover_time" else "switchover"} at {stamp}'
        origins['last_change'] = origins[key]
    return values, origins


def _get_db_state(conf, filename='.pgconsul_db_state.cache'):
    fname = '%s/%s' % (conf.get('global', 'working_dir'), filename)
    try:
        with open(fname, 'r') as fobj:
            value = json.load(fobj, parse_constant=lambda _: None)
        return value if isinstance(value, dict) else None
    except (OSError, ValueError):
        logging.info("Can't load pgconsul status from %s, skipping", fname)
        return None


def _get_info_db_state(conf):
    data: dict = {'available': False}
    try:
        parameters = parse_dsn(conf.get('global', 'local_conn_string'))
        parameters.update(application_name='pgconsul-util-info', connect_timeout='2')
        parameters['options'] = (parameters.get('options', '') + ' -c statement_timeout=2000').strip()
        deadline = time.monotonic() + 5
        # shortcut: client timeouts do not bound DNS; use process isolation if a hard CLI deadline is required.
        with contextlib.closing(psycopg2.connect(async_=True, **parameters)) as connection:
            wait_async_operation(connection, deadline)
            data['available'] = True
            for name, query in INFO_QUERIES:
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(query)
                        wait_async_operation(connection, deadline)
                        columns = [column[0] for column in cursor.description]
                        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data[name] = rows if name == 'replics_info' else rows[0] if rows else None
                except psycopg2.DatabaseError as error:
                    if isinstance(error, psycopg2.OperationalError) and (connection.closed or not getattr(error, 'pgcode', None)):
                        raise
                    logging.info('Cannot read SQL info field %s: %s', name, type(error).__name__)
    except (psycopg2.Error, ValueError, TimeoutError) as error:
        data['available'] = False
        logging.info('Cannot read local SQL info: %s', type(error).__name__)
    if 'role' in data:
        recovery = (data['role'] or {}).get('recovery')
        data['role'] = ('replica' if recovery else 'primary') if isinstance(recovery, bool) else None
    if 'replication_state' in data:
        state = (data['replication_state'] or {}).get('synchronous_standby_names')
        data['replication_state'] = ('sync', state) if state else ('async', None) if state == '' else None
    return data


def _info_values(raw, source, hostname):
    values = {key: value for key, value in raw.items() if not key.startswith('info_') and key != 'available'}
    if 'role' in values and values['role'] not in ('primary', 'replica'):
        values['role'] = None
    if source.startswith('zk'):
        if 'alive' in values:
            values['zk_alive'] = values['alive']
        if 'lock_holder' in values:
            values['primary'] = values.pop('lock_holder')
        app_name = helpers.app_name_from_fqdn(hostname)
        for replica in raw.get('replics_info') or []:
            if isinstance(replica, dict) and (replica.get('application_name') == app_name or replica.get('client_hostname') == hostname):
                values.update({key: replica[key] for key in ('sync_state', 'replay_lag_msec') if key in replica})
                break
        if 'maintenance' in values:
            maintenance = values['maintenance']
            values['maintenance'] = ('on' if maintenance['status'] == 'enable' else 'off') if isinstance(maintenance, dict) and 'status' in maintenance else None
        for key in ('last_failover_time', 'last_switchover_time'):
            if key in values:
                values[key] = _info_timestamp(values[key])
        ssn = raw.get('synchronous_standby_names')
        if isinstance(ssn, dict):
            values['synchronous_standby_names'] = {host: [pair[0], _info_timestamp(pair[1])]
                                                  if isinstance(pair, (list, tuple)) and len(pair) == 2 else pair for host, pair in ssn.items()}
    else:
        if 'opened' in values:
            opened = values.pop('opened')
            values['pooler'] = 'opened' if opened is True else 'closed' if opened is False else None
        state = raw.get('replication_state')
        if isinstance(state, (list, tuple)) and len(state) == 2:
            values['replication_state'] = f'sync, {state[1]}' if state[0] == 'sync' else 'async'
        receiver = raw.get('wal_receiver')
        if isinstance(receiver, dict):
            values['wal_receiver'] = {key: value for key, value in receiver.items() if key != 'conninfo'}
            values['wal_receiver']['last_msg_receipt_time_msec'] = _info_timestamp(receiver.get('last_msg_receipt_time_msec') or None, milliseconds=True)
            if 'sender_host' in receiver:
                values['primary_fqdn'] = receiver['sender_host']
        if raw.get('role') == 'replica' and 'wal_receiver' in raw:
            values['streaming'] = isinstance(receiver, dict) and receiver.get('status') == 'streaming'
    rows = values.get('replics_info')
    if isinstance(rows, list):
        rows = [row.copy() for row in rows if isinstance(row, dict)]
        for row in rows:
            for key in ('backend_start_ts', 'reply_time_ms'):
                if key in row:
                    row[key] = _info_timestamp(row[key], milliseconds=key == 'reply_time_ms')
        values['replics_info'] = [{key: row[key] for key in (*REPLICA_FIELD_ORDER, *row) if key in row} for row in rows]
        values['hosts_streaming'] = sum(row['state'] == 'streaming' for row in rows) if all(row.get('state') is not None for row in rows) else None
    return values


def _info_timestamp(value, milliseconds=False):
    try:
        if isinstance(value, bool):
            return None
        seconds = int(float(value) / (1000 if milliseconds else 1))
        return datetime.fromtimestamp(seconds, timezone.utc).isoformat().replace('+00:00', 'Z')
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _short_replica_infos(replics):
    if replics is None:
        return None
    rows = []
    for replica in replics:
        state = replica.get('state') or 'unknown'
        sync_state = replica.get('sync_state') or 'unknown'
        lag = replica.get('replay_lag_msec')
        info = f'{state}, sync_state {sync_state}, replay_lag_msec {lag if lag is not None else "unknown"}'
        rows.append({'host': replica.get('client_hostname'), 'info': info})
    names = [row['host'] for row in rows]
    return {row['host']: row['info'] for row in rows} if rows and all(names) and len(names) == len(set(names)) else rows


def parse_args():
    """
    Parse multiple commands.
    """
    arg = argparse.ArgumentParser(
        description="""
        pgconsul utility
        """
    )
    arg.add_argument(
        '-c',
        '--config',
        dest='config_file',
        type=str,
        metavar='<path>',
        default='/etc/pgconsul.conf',
        help='path to pgconsul main config file',
    )
    arg.add_argument(
        '--zk',
        type=str,
        dest='zk_hosts',
        metavar='<fqdn:port>,[<fqdn:port>,...]',
        help='override config zookeeper connection string',
    )
    arg.add_argument(
        '--zk-prefix',
        metavar='<path>',
        type=str,
        dest='zk_lockpath_prefix',
        help='override config zookeeper path prefix',
    )
    arg.set_defaults(action=lambda *_: arg.print_help())

    subarg = arg.add_subparsers(
        help='possible actions', title='subcommands', description='for more info, see <subcommand> -h'
    )

    # Init ZK command
    initzk_arg = subarg.add_parser('initzk', help='define zookeeper structures')
    initzk_arg.add_argument(
        'members',
        metavar='<fqdn> [<fqdn> ...]',
        action=ParseHosts,
        default=[],
        nargs='+',
        help='Space-separated list of cluster members hostnames',
    )
    initzk_arg.add_argument(
        '-t',
        '--test',
        action='store_true',
        default=False,
        help='Check if zookeeper initialization had already been performed for given hosts. Returns 0 if it had.',
    )
    initzk_arg.set_defaults(action=initzk)

    maintenance_arg = subarg.add_parser('maintenance', aliases=['maint'], help='maintenance mode')
    maintenance_arg.add_argument(
        '-m', '--mode', metavar='[enable, disable, show]', default='enable', help='Enable or disable maintenance mode'
    )
    maintenance_arg.add_argument(
        '-w',
        '--wait_all',
        help='Wait for all alive high-availability hosts finish entering/exiting maintenance mode',
        action='store_true',
        default=False,
    )
    maintenance_arg.add_argument(
        '-t', '--timeout', help='Set timeout for maintenance command with --wait_all option', type=int, default=5 * 60
    )
    maintenance_arg.set_defaults(action=maintenance)

    # Info command
    info_arg = subarg.add_parser('info', help='info about cluster')
    info_arg.add_argument(
        '-s',
        '--short',
        help='short output',
        action='store_true',
        default=False,
    )
    info_arg.add_argument(
        '-j',
        '--json',
        help='show output in json format',
        action='store_true',
        default=False,
    )
    info_arg.set_defaults(action=show_info)

    # Scheduled switchover command
    switch_arg = subarg.add_parser(
        'switchover',
        help='perform graceful switchover',
        description="""
        Perform graceful switchover of the current primary.
        The default is to auto-detect its hostname and
        timeline in ZK.
        This behaviour can be overridden with options below.
        """,
    )
    switch_arg.add_argument('-d', '--destination', help='sets host where to switch', default=None, metavar='<fqdn>')
    switch_arg.add_argument(
        '-b', '--block', help='block until switchover completes or fails', default=False, action='store_true'
    )
    switch_arg.add_argument(
        '-t',
        '--timeout',
        help='limit each step to this amount of seconds',
        type=int,
        default=60,
        metavar='<sec>',
    )
    switch_arg.add_argument(
        '-y', '--yes', help='deprecated; confirmation is assumed', default=False, action='store_true'
    )
    switch_arg.add_argument(
        '-r',
        '--reset',
        help='reset switchover state in ZK (potentially disruptive)',
        default=False,
        action='store_true',
    )
    switch_arg.add_argument(
        '--replicas',
        help='if in blocking mode, wait until this number of replicas become online',
        type=int,
        default=None,
        metavar='<int>',
    )
    switch_arg.add_argument('--primary', help='override current primary hostname', default=None, metavar='<fqdn>')
    switch_arg.add_argument('--timeline', help='override current primary timeline', default=None, metavar='<fqdn>')
    switch_arg.set_defaults(action=switchover)

    fail_arg = subarg.add_parser(
        'failover',
        help='operations on failover state',
        description="""
           Change state of current failover.
           """,
    )
    fail_arg.add_argument(
        '-r',
        '--reset',
        help='reset failover state in ZK (potentially disruptive)',
        default=False,
        action='store_true',
    )
    fail_arg.set_defaults(action=failover)

    reset_all_arg = subarg.add_parser('reset-all', help='reset all nodes except members')
    reset_all_arg.add_argument(
        '-f',
        '--force',
        help='do not prompt',
        default=False,
        action='store_true',
    )
    reset_all_arg.add_argument(
        '-t', '--timeout', help='Set timeout for reset all command', type=int, default=5 * 60
    )
    reset_all_arg.set_defaults(action=reset_all)

    try:
        return arg.parse_args()
    except ValueError as err:
        arg.exit(message='%s\n' % err)
        exit(1)
