#!/usr/bin/env python3
#
# Minimal syncd-style ASIC_DB consumer:
#   ConsumerTable(ASIC_DB, "ASIC_STATE:SAI_OBJECT_TYPE_VLAN").pop()

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.custom_schema import ASIC_NOTIFICATIONS_CHANNEL_NAME
from common.custom_schema import ASIC_GET_RESPONSE_OP
from common.custom_schema import ASIC_GET_RESPONSE_TABLE_NAME
from common.custom_schema import ASIC_VLAN_TABLE_NAME
from common.custom_schema import asic_vlan_key
from common.db_logging import emit_redis_marker
from common.select_loop import SelectLoop
from common.swss import field_value_pairs
from common.swss import load_db_config
from common.swss import swsscommon


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
    args = parser.parse_args()

    load_db_config(args.db_config)

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

    asic_consumer = swsscommon.ConsumerTable(asic_db, ASIC_VLAN_TABLE_NAME)
    response_producer = swsscommon.ProducerTable(asic_db, ASIC_GET_RESPONSE_TABLE_NAME)
    select_loop = SelectLoop()

    print("syncd: waiting for ASIC_DB %s:%s updates" % (ASIC_VLAN_TABLE_NAME, key_filter))

    def handle_asic_update(_selectable):
        emit_redis_marker(asic_db, "syncd", "before", "ConsumerTable.pop", "ASIC_DB", ASIC_VLAN_TABLE_NAME, key_filter)
        key, op, field_values = asic_consumer.pop()
        emit_redis_marker(asic_db, "syncd", "after", "ConsumerTable.pop", "ASIC_DB", ASIC_VLAN_TABLE_NAME, key_filter)
        if key != key_filter:
            return None

        print("syncd: ASIC_DB update %s:%s %s" % (ASIC_VLAN_TABLE_NAME, key, op))
        for field, value in field_values:
            print("  %s=%s" % (field, value))
        print("syncd: pretend write ASIC %s %s" % (op, key))

        response_fields = field_value_pairs({
            "err_str": "",
            "request_key": key,
            "request_op": op,
            "source": "syncd",
        })
        emit_redis_marker(asic_db, "syncd", "before", "ProducerTable.set", "ASIC_DB", ASIC_GET_RESPONSE_TABLE_NAME, key)
        response_producer.set("SAI_STATUS_SUCCESS", response_fields, ASIC_GET_RESPONSE_OP)
        emit_redis_marker(asic_db, "syncd", "after", "ProducerTable.set", "ASIC_DB", ASIC_GET_RESPONSE_TABLE_NAME, key)
        print("syncd: queued ASIC_DB %s %s for %s" % (
            ASIC_GET_RESPONSE_TABLE_NAME,
            ASIC_GET_RESPONSE_OP,
            key,
        ))

        if not args.watch:
            return SelectLoop.STOP
        return None

    select_loop.add(asic_consumer, handle_asic_update)
    select_loop.run()


if __name__ == "__main__":
    main()
