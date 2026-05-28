#!/usr/bin/env python3
#
# Minimal syncd-style ASIC_DB consumer:
#   ConsumerTable(ASIC_DB, "ASIC_STATE:SAI_OBJECT_TYPE_VLAN").pop()

import argparse

from vlan_schema import ASIC_NOTIFICATIONS_CHANNEL_NAME
from vlan_schema import ASIC_RESPONSE_CHANNEL_NAME
from vlan_schema import ASIC_VLAN_TABLE_NAME as ASIC_TABLE
from vlan_schema import asic_vlan_key
from vlan_log import add_log_argument
from vlan_log import configure_logger
from vlan_log import emit_redis_marker
from vlan_log import log_table_event
from swsscommon_compat import load_swsscommon


swsscommon = load_swsscommon()


def field_value_pairs(fields):
    return swsscommon.FieldValuePairs([(str(k), str(v)) for k, v in fields.items()])


def main():
    parser = argparse.ArgumentParser(
        description="Consume ASIC_DB VLAN updates like a tiny syncd and pretend to write ASIC."
    )
    parser.add_argument("--vlan-id", default="100", help="only print this VLAN ID")
    parser.add_argument("--port", default="Ethernet0", help="port name for async notification demo")
    parser.add_argument("--oper-status", default="ok", help="port state for async notification demo")
    parser.add_argument(
        "--send-port-notification",
        action="store_true",
        help="send one ASIC_DB:NOTIFICATIONS port_state_change event and exit",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="continue processing ASIC_DB updates instead of exiting after one matching event",
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

    key_filter = asic_vlan_key(args.vlan_id)
    asic_db = swsscommon.DBConnector("ASIC_DB", 0, False)

    if args.send_port_notification:
        notification_producer = swsscommon.NotificationProducer(asic_db, ASIC_NOTIFICATIONS_CHANNEL_NAME)
        values = field_value_pairs({
            "port": args.port,
            "oper_status": args.oper_status,
            "source": "syncd",
        })
        emit_redis_marker(asic_db, "syncd", "before", "NotificationProducer.send", "ASIC_DB", ASIC_NOTIFICATIONS_CHANNEL_NAME, args.port)
        notification_producer.send("port_state_change", args.port, values)
        emit_redis_marker(asic_db, "syncd", "after", "NotificationProducer.send", "ASIC_DB", ASIC_NOTIFICATIONS_CHANNEL_NAME, args.port)
        print("syncd: sent ASIC_DB:%s port_state_change %s %s" % (
            ASIC_NOTIFICATIONS_CHANNEL_NAME,
            args.port,
            args.oper_status,
        ))
        return

    asic_consumer = swsscommon.ConsumerTable(asic_db, ASIC_TABLE)
    response_producer = swsscommon.NotificationProducer(asic_db, ASIC_RESPONSE_CHANNEL_NAME)
    selector = swsscommon.Select()
    selector.addSelectable(asic_consumer)

    print("syncd: waiting for ASIC_DB %s:%s updates" % (ASIC_TABLE, key_filter))

    while True:
        state, selectable = selector.select()
        if state != swsscommon.Select.OBJECT:
            continue

        emit_redis_marker(asic_db, "syncd", "before", "ConsumerTable.pop", "ASIC_DB", ASIC_TABLE, key_filter)
        key, op, field_values = asic_consumer.pop()
        emit_redis_marker(asic_db, "syncd", "after", "ConsumerTable.pop", "ASIC_DB", ASIC_TABLE, key_filter)
        if key != key_filter:
            continue

        log_table_event(
            logger,
            "syncd",
            "ConsumerTable.pop",
            "READ",
            "ASIC_DB",
            ASIC_TABLE,
            key,
            op=op,
            fields=field_values,
            note="ConsumerTable pop materializes ASIC_DB final hash before fake ASIC write",
        )
        log_table_event(
            logger,
            "syncd",
            "fake_asic_write",
            "WRITE",
            "ASIC",
            ASIC_TABLE,
            key,
            op=op,
            fields=field_values,
            note="No real ASIC access in this example",
        )

        print("syncd: ASIC_DB update %s:%s %s" % (ASIC_TABLE, key, op))
        for field, value in field_values:
            print("  %s=%s" % (field, value))
        print("syncd: pretend write ASIC %s %s" % (op, key))

        response_fields = field_value_pairs({
            "status": "SAI_STATUS_SUCCESS",
            "request_op": op,
            "source": "syncd",
        })
        emit_redis_marker(asic_db, "syncd", "before", "NotificationProducer.send", "ASIC_DB", ASIC_RESPONSE_CHANNEL_NAME, key)
        response_producer.send("SAI_STATUS_SUCCESS", key, response_fields)
        emit_redis_marker(asic_db, "syncd", "after", "NotificationProducer.send", "ASIC_DB", ASIC_RESPONSE_CHANNEL_NAME, key)
        print("syncd: sent ASIC_DB response channel %s for %s" % (
            ASIC_RESPONSE_CHANNEL_NAME,
            key,
        ))

        if not args.watch:
            return


if __name__ == "__main__":
    main()
