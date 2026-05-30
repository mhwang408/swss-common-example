#!/usr/bin/env python3
"""Minimal syncd-style ASIC_DB consumer.

Consumes ordered ASIC operations from the queue written by PortsOrch,
pretends to program the ASIC, and sends back a sairedis GETRESPONSE.
Can also emit async port-state-change notifications::

    ASIC_DB queue → [syncd ConsumerTable.pop] → fake ASIC write
                  → ASIC_DB GETRESPONSE (ProducerTable)
                  → ASIC_DB NOTIFICATIONS (NotificationProducer)
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
    ASIC_DB,
    ASIC_GET_RESPONSE_OP,
    ASIC_GET_RESPONSE_TABLE_NAME,
    ASIC_NOTIFICATIONS_CHANNEL_NAME,
    ASIC_VLAN_TABLE_NAME,
    NOTIFICATION_PORT_STATE_CHANGE,
    asic_vlan_key,
)
from common.select_loop import SelectLoop
from common.swss import field_value_pairs, load_db_config, swsscommon


# ---------------------------------------------------------------------------
# Async notification sender (standalone mode)
# ---------------------------------------------------------------------------

def send_port_notification(args: argparse.Namespace) -> None:
    """Send a single port_state_change notification on ASIC_DB:NOTIFICATIONS."""
    asic_db = swsscommon.DBConnector(ASIC_DB, 0, False)
    notification_producer = swsscommon.NotificationProducer(
        asic_db, ASIC_NOTIFICATIONS_CHANNEL_NAME,
    )
    values = field_value_pairs({
        "port": args.port,
        "oper_status": args.oper_status,
        "source": "syncd",
    })

    emit_redis_marker(
        asic_db, "syncd", "before",
        "NotificationProducer.send", ASIC_DB,
        ASIC_NOTIFICATIONS_CHANNEL_NAME, args.port,
    )
    notification_producer.send(NOTIFICATION_PORT_STATE_CHANGE, args.port, values)
    emit_redis_marker(
        asic_db, "syncd", "after",
        "NotificationProducer.send", ASIC_DB,
        ASIC_NOTIFICATIONS_CHANNEL_NAME, args.port,
    )

    print("syncd: sent %s:%s %s %s %s" % (
        ASIC_DB, ASIC_NOTIFICATIONS_CHANNEL_NAME,
        NOTIFICATION_PORT_STATE_CHANGE, args.port, args.oper_status,
    ))


# ---------------------------------------------------------------------------
# ASIC consumer loop
# ---------------------------------------------------------------------------

def run_asic_consumer(args: argparse.Namespace) -> None:
    """Consume ASIC_DB VLAN operations and reply with GETRESPONSE."""
    key_filter = asic_vlan_key(args.vlan_id)
    asic_db = swsscommon.DBConnector(ASIC_DB, 0, False)
    asic_consumer = swsscommon.ConsumerTable(asic_db, ASIC_VLAN_TABLE_NAME)
    response_producer = swsscommon.ProducerTable(asic_db, ASIC_GET_RESPONSE_TABLE_NAME)
    select_loop = SelectLoop()

    print("syncd: waiting for %s %s:%s updates" % (
        ASIC_DB, ASIC_VLAN_TABLE_NAME, key_filter,
    ))

    def handle_asic_update(_selectable: Any) -> object | None:
        emit_redis_marker(
            asic_db, "syncd", "before",
            "ConsumerTable.pop", ASIC_DB, ASIC_VLAN_TABLE_NAME, key_filter,
        )
        key, op, field_values = asic_consumer.pop()
        emit_redis_marker(
            asic_db, "syncd", "after",
            "ConsumerTable.pop", ASIC_DB, ASIC_VLAN_TABLE_NAME, key_filter,
        )
        if key != key_filter:
            return None

        print("syncd: %s update %s:%s %s" % (ASIC_DB, ASIC_VLAN_TABLE_NAME, key, op))
        for field, value in field_values:
            print("  %s=%s" % (field, value))
        print("syncd: pretend write ASIC %s %s" % (op, key))

        # Send sairedis GETRESPONSE back to PortsOrch
        response_fields = field_value_pairs({
            "err_str": "",
            "request_key": key,
            "request_op": op,
            "source": "syncd",
        })
        emit_redis_marker(
            asic_db, "syncd", "before",
            "ProducerTable.set", ASIC_DB, ASIC_GET_RESPONSE_TABLE_NAME, key,
        )
        response_producer.set("SAI_STATUS_SUCCESS", response_fields, ASIC_GET_RESPONSE_OP)
        emit_redis_marker(
            asic_db, "syncd", "after",
            "ProducerTable.set", ASIC_DB, ASIC_GET_RESPONSE_TABLE_NAME, key,
        )
        print("syncd: queued %s %s %s for %s" % (
            ASIC_DB, ASIC_GET_RESPONSE_TABLE_NAME, ASIC_GET_RESPONSE_OP, key,
        ))
        return None

    select_loop.add(asic_consumer, handle_asic_update)
    select_loop.run()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point for the syncd example script."""
    parser = argparse.ArgumentParser(
        description="Consume ASIC_DB VLAN updates like a tiny syncd."
    )
    parser.add_argument("--vlan-id", default="100", help="only print this VLAN ID")
    parser.add_argument("--port", default="Ethernet0", help="port name for notification demo")
    parser.add_argument("--oper-status", default="ok", help="port state for notification demo")
    parser.add_argument(
        "--send-port-notification", action="store_true",
        help="send one ASIC_DB:NOTIFICATIONS port_state_change event and exit",
    )
    parser.add_argument("--db-config", help="path to database_config.json")
    args = parser.parse_args()

    load_db_config(args.db_config)

    if args.send_port_notification:
        send_port_notification(args)
    else:
        run_asic_consumer(args)


if __name__ == "__main__":
    main()
