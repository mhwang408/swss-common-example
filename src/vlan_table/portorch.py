#!/usr/bin/env python3
#
# Minimal PortsOrch-style APPL_DB consumer:
#   ConsumerStateTable(APPL_DB, "VLAN_TABLE").pop()
#   sai_vlan_api->create_vlan() represented by ProducerTable(ASIC_DB, ...).set()

import argparse

from vlan_schema import APP_VLAN_TABLE_NAME as APPL_TABLE
from vlan_schema import ASIC_NOTIFICATIONS_CHANNEL_NAME
from vlan_schema import ASIC_RESPONSE_CHANNEL_NAME
from vlan_schema import ASIC_VLAN_TABLE_NAME as ASIC_TABLE
from vlan_schema import STATE_PORT_TABLE_NAME
from vlan_schema import VLAN_PREFIX
from vlan_schema import asic_vlan_key
from vlan_log import add_log_argument
from vlan_log import configure_logger
from vlan_log import emit_redis_marker
from vlan_log import log_table_event
from swsscommon_compat import load_swsscommon


swsscommon = load_swsscommon()


def field_value_pairs(fields):
    return swsscommon.FieldValuePairs([(str(k), str(v)) for k, v in fields.items()])


def wait_for_sai_response(asic_db, logger, request_key):
    response_consumer = swsscommon.NotificationConsumer(asic_db, ASIC_RESPONSE_CHANNEL_NAME)
    selector = swsscommon.Select()
    selector.addSelectable(response_consumer)

    print("PortsOrch: waiting for ASIC_DB response channel %s" % ASIC_RESPONSE_CHANNEL_NAME)
    while True:
        state, selectable = selector.select()
        if state != swsscommon.Select.OBJECT:
            continue

        emit_redis_marker(asic_db, "portorch", "before", "NotificationConsumer.pop", "ASIC_DB", ASIC_RESPONSE_CHANNEL_NAME, request_key)
        op, data, field_values = response_consumer.pop()
        emit_redis_marker(asic_db, "portorch", "after", "NotificationConsumer.pop", "ASIC_DB", ASIC_RESPONSE_CHANNEL_NAME, request_key)
        if data != request_key:
            continue

        log_table_event(
            logger,
            "portorch",
            "NotificationConsumer.pop",
            "READ",
            "ASIC_DB",
            ASIC_RESPONSE_CHANNEL_NAME,
            data,
            op=op,
            fields=field_values,
            note="SAI response channel delivers syncd operation result to sairedis/orchagent",
        )
        print("PortsOrch: SAI response %s %s" % (op, data))
        for field, value in field_values:
            print("  %s=%s" % (field, value))
        return op, data, field_values


def process_vlan_update(args, logger):
    key_filter = "%s%s" % (VLAN_PREFIX, args.vlan_id)
    appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
    asic_db = swsscommon.DBConnector("ASIC_DB", 0, False)
    vlan_consumer = swsscommon.ConsumerStateTable(appl_db, APPL_TABLE)
    asic_producer = swsscommon.ProducerTable(asic_db, ASIC_TABLE)
    selector = swsscommon.Select()
    selector.addSelectable(vlan_consumer)

    print("PortsOrch: waiting for APPL_DB %s:%s updates" % (APPL_TABLE, key_filter))

    while True:
        state, selectable = selector.select()
        if state != swsscommon.Select.OBJECT:
            continue

        emit_redis_marker(appl_db, "portorch", "before", "ConsumerStateTable.pop", "APPL_DB", APPL_TABLE, key_filter)
        key, op, field_values = vlan_consumer.pop()
        emit_redis_marker(appl_db, "portorch", "after", "ConsumerStateTable.pop", "APPL_DB", APPL_TABLE, key_filter)
        if key != key_filter:
            continue

        log_table_event(
            logger,
            "portorch",
            "ConsumerStateTable.pop",
            "READ",
            "APPL_DB",
            APPL_TABLE,
            key,
            op=op,
            fields=field_values,
            note="ConsumerStateTable pop materializes APPL_DB final table content",
        )

        print("PortsOrch: APPL_DB update %s:%s %s" % (APPL_TABLE, key, op))
        for field, value in field_values:
            print("  %s=%s" % (field, value))

        appl_fields = {field: value for field, value in field_values}
        vlan_id = appl_fields.get("vlanid", args.vlan_id)
        asic_key = asic_vlan_key(vlan_id)
        if op == "SET":
            asic_fields = {
                "SAI_VLAN_ATTR_VLAN_ID": vlan_id,
                "source": "PortsOrch",
            }
            emit_redis_marker(asic_db, "portorch", "before", "ProducerTable.set", "ASIC_DB", ASIC_TABLE, asic_key)
            asic_producer.set(asic_key, field_value_pairs(asic_fields))
            emit_redis_marker(asic_db, "portorch", "after", "ProducerTable.set", "ASIC_DB", ASIC_TABLE, asic_key)
            log_table_event(
                logger,
                "portorch",
                "ProducerTable.set",
                "WRITE",
                "ASIC_DB",
                ASIC_TABLE,
                asic_key,
                op="SET",
                fields=asic_fields.items(),
                note="SAI request path: ProducerTable enqueues ordered ASIC_DB operation for syncd",
            )
            print("PortsOrch: queued SAI create request %s:%s" % (ASIC_TABLE, asic_key))
        elif op == "DEL":
            emit_redis_marker(asic_db, "portorch", "before", "ProducerTable.delete", "ASIC_DB", ASIC_TABLE, asic_key)
            asic_producer.delete(asic_key)
            emit_redis_marker(asic_db, "portorch", "after", "ProducerTable.delete", "ASIC_DB", ASIC_TABLE, asic_key)
            log_table_event(
                logger,
                "portorch",
                "ProducerTable.delete",
                "WRITE",
                "ASIC_DB",
                ASIC_TABLE,
                asic_key,
                op="DEL",
                note="SAI request path: ProducerTable enqueues ordered ASIC_DB delete for syncd",
            )
            print("PortsOrch: queued SAI remove request %s:%s" % (ASIC_TABLE, asic_key))

        if args.wait_sai_response:
            wait_for_sai_response(asic_db, logger, asic_key)

        if not args.watch:
            return


def process_asic_notification(args, logger):
    asic_db = swsscommon.DBConnector("ASIC_DB", 0, False)
    state_db = swsscommon.DBConnector("STATE_DB", 0, False)
    notification_consumer = swsscommon.NotificationConsumer(asic_db, ASIC_NOTIFICATIONS_CHANNEL_NAME)
    port_state_table = swsscommon.Table(state_db, STATE_PORT_TABLE_NAME)
    selector = swsscommon.Select()
    selector.addSelectable(notification_consumer)

    print("PortsOrch: waiting for ASIC_DB:%s async notifications" % ASIC_NOTIFICATIONS_CHANNEL_NAME)

    while True:
        state, selectable = selector.select()
        if state != swsscommon.Select.OBJECT:
            continue

        emit_redis_marker(asic_db, "portorch", "before", "NotificationConsumer.pop", "ASIC_DB", ASIC_NOTIFICATIONS_CHANNEL_NAME, args.port)
        op, data, field_values = notification_consumer.pop()
        emit_redis_marker(asic_db, "portorch", "after", "NotificationConsumer.pop", "ASIC_DB", ASIC_NOTIFICATIONS_CHANNEL_NAME, args.port)
        if op != "port_state_change":
            print("PortsOrch: ignoring async notification %s %s" % (op, data))
            if not args.watch:
                return
            continue

        fields = {field: value for field, value in field_values}
        port = fields.get("port", data or args.port)
        oper_status = fields.get("oper_status", args.oper_status)
        values = {"state": oper_status, "source": "PortsOrch"}
        emit_redis_marker(state_db, "portorch", "before", "Table.set", "STATE_DB", STATE_PORT_TABLE_NAME, port)
        port_state_table.set(port, field_value_pairs(values))
        emit_redis_marker(state_db, "portorch", "after", "Table.set", "STATE_DB", STATE_PORT_TABLE_NAME, port)

        log_table_event(
            logger,
            "portorch",
            "NotificationConsumer.pop+Table.set",
            "WRITE",
            "STATE_DB",
            STATE_PORT_TABLE_NAME,
            port,
            op="SET",
            fields=values.items(),
            note="Async notification path: syncd notification is converted into STATE_DB for mgrd consumers",
        )
        print("PortsOrch: async %s for %s -> STATE_DB %s|%s" % (
            op,
            port,
            STATE_PORT_TABLE_NAME,
            port,
        ))
        for field, value in values.items():
            print("  %s=%s" % (field, value))

        if not args.watch:
            return


def main():
    parser = argparse.ArgumentParser(
        description="Consume APPL_DB VLAN_TABLE updates like SONiC PortsOrch."
    )
    parser.add_argument("--vlan-id", default="100", help="only process this VLAN ID")
    parser.add_argument("--port", default="Ethernet0", help="port name for async notification demo")
    parser.add_argument("--oper-status", default="ok", help="STATE_DB port state value for async notification demo")
    parser.add_argument(
        "--notification-only",
        action="store_true",
        help="consume ASIC_DB:NOTIFICATIONS and write STATE_DB instead of processing VLAN_TABLE",
    )
    parser.add_argument(
        "--wait-sai-response",
        action="store_true",
        help="wait for syncd's ASIC DB response channel after enqueueing the SAI request",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="continue processing updates instead of exiting after one matching event",
    )
    parser.add_argument(
        "--db-config",
        help="path to database_config.json; useful when running Redis in a local host container",
    )
    add_log_argument(parser)
    args = parser.parse_args()
    logger, _ = configure_logger(args.log_file)

    if args.db_config:
        swsscommon.SonicDBConfig.load_sonic_db_config(args.db_config)

    if args.notification_only:
        process_asic_notification(args, logger)
    else:
        process_vlan_update(args, logger)


if __name__ == "__main__":
    main()
