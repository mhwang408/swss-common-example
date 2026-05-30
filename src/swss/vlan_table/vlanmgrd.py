#!/usr/bin/env python3
#
# Minimal vlanmgrd-style bridge:
#   CONFIG_DB VLAN|Vlan100 -> APPL_DB VLAN_TABLE:Vlan100

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.custom_schema import APP_VLAN_TABLE_NAME
from common.custom_schema import APPL_RESPONSE_CHANNEL_NAME
from common.custom_schema import CFG_VLAN_TABLE_NAME
from common.custom_schema import STATE_PORT_TABLE_NAME
from common.custom_schema import VLAN_PREFIX
from common.db_logging import emit_redis_marker
from common.select_loop import SelectLoop
from common.swss import field_value_pairs
from common.swss import load_db_config
from common.swss import swsscommon


def default_vlan_id_from_key(key):
    if key.startswith(VLAN_PREFIX):
        return key[len(VLAN_PREFIX):]
    return ""

# Database: CONFIG_DB
# Table|Key->Value: VLAN|Vlan100 -> {"vlanid": "100"}
#     |
#     V
#   [vlanmgrd]
#     |
#     V
# Database: APPL_DB
# Table:Key->Value: VLAN_TABLE:Vlan100 -> {"vlanid": "100"}

def publish_set(appl_db, appl_table, key, field_values):
    config = {field: value for field, value in field_values}
    vlan_id = config.get("vlanid", default_vlan_id_from_key(key))
    appl_values = {"vlanid": vlan_id}
    emit_redis_marker(appl_db, "vlanmgrd", "before", "ProducerStateTable.set", "APPL_DB", APP_VLAN_TABLE_NAME, key)
    appl_table.set(key, field_value_pairs(appl_values))
    emit_redis_marker(appl_db, "vlanmgrd", "after", "ProducerStateTable.set", "APPL_DB", APP_VLAN_TABLE_NAME, key)

    print("vlanmgrd: CONFIG_DB %s|%s SET -> APPL_DB %s:%s SET" % (
        CFG_VLAN_TABLE_NAME,
        key,
        APP_VLAN_TABLE_NAME,
        key,
    ))
    print('  fields: {"vlanid": "%s"}' % vlan_id)


def publish_delete(appl_db, appl_table, key):
    emit_redis_marker(appl_db, "vlanmgrd", "before", "ProducerStateTable.delete", "APPL_DB", APP_VLAN_TABLE_NAME, key)
    appl_table.delete(key)
    emit_redis_marker(appl_db, "vlanmgrd", "after", "ProducerStateTable.delete", "APPL_DB", APP_VLAN_TABLE_NAME, key)
    print("vlanmgrd: CONFIG_DB %s|%s DEL -> APPL_DB %s:%s DEL" % (
        CFG_VLAN_TABLE_NAME,
        key,
        APP_VLAN_TABLE_NAME,
        key,
    ))


def wait_for_appl_response(args):
    key_filter = "%s%s" % (VLAN_PREFIX, args.vlan_id)
    appl_state_db = swsscommon.DBConnector("APPL_STATE_DB", 0, False)
    response_consumer = swsscommon.NotificationConsumer(appl_state_db, APPL_RESPONSE_CHANNEL_NAME)
    select_loop = SelectLoop(swsscommon)

    print("vlanmgrd: waiting for APPL_STATE_DB response channel %s:%s" % (
        APPL_RESPONSE_CHANNEL_NAME,
        key_filter,
    ))

    def handle_response(_selectable):
        emit_redis_marker(appl_state_db, "vlanmgrd", "before", "NotificationConsumer.pop", "APPL_STATE_DB", APPL_RESPONSE_CHANNEL_NAME, key_filter)
        op, data, field_values = response_consumer.pop()
        emit_redis_marker(appl_state_db, "vlanmgrd", "after", "NotificationConsumer.pop", "APPL_STATE_DB", APPL_RESPONSE_CHANNEL_NAME, key_filter)
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


def read_existing_port_state(state_db, port):
    state_table = swsscommon.Table(state_db, STATE_PORT_TABLE_NAME)
    status, field_values = state_table.get(port)
    if not status:
        return False

    print("vlanmgrd: STATE_DB %s|%s SET" % (STATE_PORT_TABLE_NAME, port))
    for field, value in field_values:
        print("  %s=%s" % (field, value))
    return True


def watch_state_port(args):
    state_db = swsscommon.DBConnector("STATE_DB", 0, False)

    print("vlanmgrd: waiting for STATE_DB %s|%s updates" % (STATE_PORT_TABLE_NAME, args.state_port))
    if not args.watch and read_existing_port_state(state_db, args.state_port):
        return

    state_subscriber = swsscommon.SubscriberStateTable(state_db, STATE_PORT_TABLE_NAME)
    select_loop = SelectLoop(swsscommon)

    def handle_state_update(_selectable):
        key, op, field_values = state_subscriber.pop()
        if key != args.state_port:
            return None
        print("vlanmgrd: STATE_DB %s|%s %s" % (STATE_PORT_TABLE_NAME, key, op))
        for field, value in field_values:
            print("  %s=%s" % (field, value))
        if not args.watch:
            return SelectLoop.STOP
        return None

    select_loop.add(state_subscriber, handle_state_update)
    select_loop.run()


def replay_config(config_db, appl_db, appl_table, key_filter):
    config_table = swsscommon.Table(config_db, CFG_VLAN_TABLE_NAME)
    emit_redis_marker(config_db, "vlanmgrd", "before", "Table.get", "CONFIG_DB", CFG_VLAN_TABLE_NAME, key_filter)
    status, field_values = config_table.get(key_filter)
    emit_redis_marker(config_db, "vlanmgrd", "after", "Table.get", "CONFIG_DB", CFG_VLAN_TABLE_NAME, key_filter)
    if not status:
        return False

    publish_set(appl_db, appl_table, key_filter, field_values)
    return True


def watch_config_updates(args, config_db, appl_db, appl_table, key_filter):
    config_subscriber = swsscommon.SubscriberStateTable(config_db, CFG_VLAN_TABLE_NAME)
    select_loop = SelectLoop(swsscommon)

    def handle_config_update(_selectable):
        key, op, field_values = config_subscriber.pop()
        if key != key_filter:
            return None

        if op == "SET":
            publish_set(appl_db, appl_table, key, field_values)
        elif op == "DEL":
            publish_delete(appl_db, appl_table, key)
        else:
            print("vlanmgrd: ignoring CONFIG_DB %s|%s op %s" % (CFG_VLAN_TABLE_NAME, key, op))

        if not args.watch:
            return SelectLoop.STOP
        return None

    select_loop.add(config_subscriber, handle_config_update)
    select_loop.run()


def bridge_config_to_appl(args):
    key_filter = "%s%s" % (VLAN_PREFIX, args.vlan_id)
    config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
    appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
    appl_table = swsscommon.ProducerStateTable(appl_db, APP_VLAN_TABLE_NAME)

    print("vlanmgrd: waiting for CONFIG_DB %s|%s updates" % (CFG_VLAN_TABLE_NAME, key_filter))
    if not args.watch and replay_config(config_db, appl_db, appl_table, key_filter):
        return

    watch_config_updates(args, config_db, appl_db, appl_table, key_filter)


def main():
    parser = argparse.ArgumentParser(
        description="Subscribe to CONFIG_DB VLAN changes and publish APPL_DB VLAN_TABLE updates."
    )
    parser.add_argument("--vlan-id", default="100", help="only process this VLAN ID")
    parser.add_argument("--state-port", help="watch/read STATE_DB PORT_TABLE for this port and exit")
    parser.add_argument(
        "--wait-appl-response",
        action="store_true",
        help="wait for PortsOrch APPL response channel and exit",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="continue processing CONFIG_DB updates instead of exiting after one matching event",
    )
    parser.add_argument(
        "--db-config",
        help="path to database_config.json; useful when running Redis in a local host container",
    )
    args = parser.parse_args()

    load_db_config(args.db_config)

    if args.wait_appl_response:
        wait_for_appl_response(args)
        return

    if args.state_port:
        watch_state_port(args)
        return

    bridge_config_to_appl(args)


if __name__ == "__main__":
    main()
