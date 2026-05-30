#!/usr/bin/env python3
"""Minimal vlanmgrd-style bridge: CONFIG_DB VLAN → APPL_DB VLAN_TABLE.

Watches CONFIG_DB for VLAN changes via ``SubscriberStateTable``, then
publishes desired state into APPL_DB via ``ProducerStateTable``.  Also
demonstrates consuming the PortsOrch APPL response channel and observing
STATE_DB port state updates.

Data flow::

    CONFIG_DB VLAN|Vlan100
        → [vlanmgrd SubscriberStateTable.pop]
        → APPL_DB _VLAN_TABLE:Vlan100 (pending, via ProducerStateTable.set)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import _path_setup  # noqa: F401

from common.db_logging import emit_redis_marker
from common.schema import (
    APP_VLAN_TABLE_NAME,
    APPL_DB,
    APPL_RESPONSE_CHANNEL_NAME,
    APPL_STATE_DB,
    CFG_VLAN_TABLE_NAME,
    CONFIG_DB,
    OP_DEL,
    OP_SET,
    STATE_DB,
    STATE_PORT_TABLE_NAME,
    VLAN_PREFIX,
)
from common.select_loop import SelectLoop
from common.swss import field_value_pairs, load_db_config, swsscommon


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _vlan_id_from_key(key: str) -> str:
    """Extract the numeric VLAN ID from a key like ``Vlan100``."""
    if key.startswith(VLAN_PREFIX):
        return key[len(VLAN_PREFIX):]
    return ""


# ---------------------------------------------------------------------------
# APPL_DB publishing
# ---------------------------------------------------------------------------

def publish_set(
    appl_db: Any,
    appl_table: Any,
    key: str,
    field_values: list[tuple[str, str]],
) -> None:
    """Publish a SET to APPL_DB VLAN_TABLE via ProducerStateTable."""
    config = {field: value for field, value in field_values}
    vlan_id = config.get("vlanid", _vlan_id_from_key(key))
    appl_values = {"vlanid": vlan_id}

    emit_redis_marker(
        appl_db, "vlanmgrd", "before",
        "ProducerStateTable.set", APPL_DB, APP_VLAN_TABLE_NAME, key,
    )
    appl_table.set(key, field_value_pairs(appl_values))
    emit_redis_marker(
        appl_db, "vlanmgrd", "after",
        "ProducerStateTable.set", APPL_DB, APP_VLAN_TABLE_NAME, key,
    )

    print("vlanmgrd: %s %s|%s SET -> %s %s:%s SET" % (
        CONFIG_DB, CFG_VLAN_TABLE_NAME, key,
        APPL_DB, APP_VLAN_TABLE_NAME, key,
    ))
    print('  fields: {"vlanid": "%s"}' % vlan_id)


def publish_delete(appl_db: Any, appl_table: Any, key: str) -> None:
    """Publish a DEL to APPL_DB VLAN_TABLE via ProducerStateTable."""
    emit_redis_marker(
        appl_db, "vlanmgrd", "before",
        "ProducerStateTable.delete", APPL_DB, APP_VLAN_TABLE_NAME, key,
    )
    appl_table.delete(key)
    emit_redis_marker(
        appl_db, "vlanmgrd", "after",
        "ProducerStateTable.delete", APPL_DB, APP_VLAN_TABLE_NAME, key,
    )
    print("vlanmgrd: %s %s|%s DEL -> %s %s:%s DEL" % (
        CONFIG_DB, CFG_VLAN_TABLE_NAME, key,
        APPL_DB, APP_VLAN_TABLE_NAME, key,
    ))


# ---------------------------------------------------------------------------
# Sub-commands
# ---------------------------------------------------------------------------

def wait_for_appl_response(args: argparse.Namespace) -> None:
    """Block until a response arrives on the APPL response channel."""
    key_filter = "%s%s" % (VLAN_PREFIX, args.vlan_id)
    appl_state_db = swsscommon.DBConnector(APPL_STATE_DB, 0, False)
    response_consumer = swsscommon.NotificationConsumer(
        appl_state_db, APPL_RESPONSE_CHANNEL_NAME,
    )
    select_loop = SelectLoop()

    print("vlanmgrd: waiting for %s response channel %s:%s" % (
        APPL_STATE_DB, APPL_RESPONSE_CHANNEL_NAME, key_filter,
    ))

    def handle_response(_selectable: Any) -> object | None:
        emit_redis_marker(
            appl_state_db, "vlanmgrd", "before",
            "NotificationConsumer.pop", APPL_STATE_DB,
            APPL_RESPONSE_CHANNEL_NAME, key_filter,
        )
        op, data, field_values = response_consumer.pop()
        emit_redis_marker(
            appl_state_db, "vlanmgrd", "after",
            "NotificationConsumer.pop", APPL_STATE_DB,
            APPL_RESPONSE_CHANNEL_NAME, key_filter,
        )
        if data != key_filter:
            print("vlanmgrd: ignoring APPL response %s %s" % (op, data))
            return None

        print("vlanmgrd: APPL response %s %s" % (op, data))
        for field, value in field_values:
            print("  %s=%s" % (field, value))

        if not args.watch:
            return SelectLoop.STOP
        return None

    select_loop.add(response_consumer, handle_response)
    select_loop.run()


def read_existing_port_state(state_db: Any, port: str) -> bool:
    """Read and print existing STATE_DB PORT_TABLE entry for *port*."""
    state_table = swsscommon.Table(state_db, STATE_PORT_TABLE_NAME)
    status, field_values = state_table.get(port)
    if not status:
        return False

    print("vlanmgrd: %s %s|%s SET" % (STATE_DB, STATE_PORT_TABLE_NAME, port))
    for field, value in field_values:
        print("  %s=%s" % (field, value))
    return True


def watch_state_port(args: argparse.Namespace) -> None:
    """Watch STATE_DB PORT_TABLE for updates on a specific port."""
    state_db = swsscommon.DBConnector(STATE_DB, 0, False)

    print("vlanmgrd: waiting for %s %s|%s updates" % (
        STATE_DB, STATE_PORT_TABLE_NAME, args.state_port,
    ))
    if not args.watch and read_existing_port_state(state_db, args.state_port):
        return

    state_subscriber = swsscommon.SubscriberStateTable(state_db, STATE_PORT_TABLE_NAME)
    select_loop = SelectLoop()

    def handle_state_update(_selectable: Any) -> object | None:
        key, op, field_values = state_subscriber.pop()
        if key != args.state_port:
            return None
        print("vlanmgrd: %s %s|%s %s" % (STATE_DB, STATE_PORT_TABLE_NAME, key, op))
        for field, value in field_values:
            print("  %s=%s" % (field, value))
        if not args.watch:
            return SelectLoop.STOP
        return None

    select_loop.add(state_subscriber, handle_state_update)
    select_loop.run()


def replay_config(
    config_db: Any,
    appl_db: Any,
    appl_table: Any,
    key_filter: str,
) -> bool:
    """One-shot replay: read existing CONFIG_DB entry and publish to APPL_DB."""
    config_table = swsscommon.Table(config_db, CFG_VLAN_TABLE_NAME)
    emit_redis_marker(
        config_db, "vlanmgrd", "before",
        "Table.get", CONFIG_DB, CFG_VLAN_TABLE_NAME, key_filter,
    )
    status, field_values = config_table.get(key_filter)
    emit_redis_marker(
        config_db, "vlanmgrd", "after",
        "Table.get", CONFIG_DB, CFG_VLAN_TABLE_NAME, key_filter,
    )
    if not status:
        return False

    publish_set(appl_db, appl_table, key_filter, field_values)
    return True


def watch_config_updates(
    args: argparse.Namespace,
    config_db: Any,
    appl_db: Any,
    appl_table: Any,
    key_filter: str,
) -> None:
    """Subscribe to CONFIG_DB VLAN changes and forward to APPL_DB."""
    config_subscriber = swsscommon.SubscriberStateTable(config_db, CFG_VLAN_TABLE_NAME)
    select_loop = SelectLoop()

    def handle_config_update(_selectable: Any) -> object | None:
        key, op, field_values = config_subscriber.pop()
        if key != key_filter:
            return None

        if op == OP_SET:
            publish_set(appl_db, appl_table, key, field_values)
        elif op == OP_DEL:
            publish_delete(appl_db, appl_table, key)
        else:
            print("vlanmgrd: ignoring %s %s|%s op %s" % (
                CONFIG_DB, CFG_VLAN_TABLE_NAME, key, op,
            ))
        return None

    select_loop.add(config_subscriber, handle_config_update)
    select_loop.run()


def bridge_config_to_appl(args: argparse.Namespace) -> None:
    """Main bridge logic: replay existing config then watch for changes."""
    key_filter = "%s%s" % (VLAN_PREFIX, args.vlan_id)
    config_db = swsscommon.DBConnector(CONFIG_DB, 0, False)
    appl_db = swsscommon.DBConnector(APPL_DB, 0, False)
    appl_table = swsscommon.ProducerStateTable(appl_db, APP_VLAN_TABLE_NAME)

    print("vlanmgrd: replaying %s %s|%s" % (
        CONFIG_DB, CFG_VLAN_TABLE_NAME, key_filter,
    ))
    replay_config(config_db, appl_db, appl_table, key_filter)

    print("vlanmgrd: watching %s %s|%s for updates" % (
        CONFIG_DB, CFG_VLAN_TABLE_NAME, key_filter,
    ))
    watch_config_updates(args, config_db, appl_db, appl_table, key_filter)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point for the vlanmgrd example script."""
    parser = argparse.ArgumentParser(
        description="Subscribe to CONFIG_DB VLAN changes and publish APPL_DB VLAN_TABLE updates."
    )
    parser.add_argument("--vlan-id", default="100", help="only process this VLAN ID")
    parser.add_argument(
        "--state-port",
        help="watch/read STATE_DB PORT_TABLE for this port and exit",
    )
    parser.add_argument(
        "--wait-appl-response", action="store_true",
        help="wait for PortsOrch APPL response channel and exit",
    )
    parser.add_argument(
        "--watch", action="store_true",
        help="keep --wait-appl-response or --state-port running after first event",
    )
    parser.add_argument("--db-config", help="path to database_config.json")
    args = parser.parse_args()

    load_db_config(args.db_config)

    if args.wait_appl_response:
        wait_for_appl_response(args)
    elif args.state_port:
        watch_state_port(args)
    else:
        bridge_config_to_appl(args)


if __name__ == "__main__":
    main()
