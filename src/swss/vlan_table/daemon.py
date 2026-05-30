#!/usr/bin/env python3
"""Run syncd, portorch, and vlanmgrd together as a single daemon process.

This is the main entry point for the VLAN example.  It starts all three
SONiC-style components in one process sharing a single event loop, so the
entire pipeline runs in one container alongside the Redis ``database``
container.

The daemon handles any VLAN — no ID filter required.

Usage::

    python3 src/swss/vlan_table/daemon.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import _path_setup  # noqa: F401

from common.schema import (
    APP_VLAN_TABLE_NAME,
    APPL_DB,
    APPL_RESPONSE_CHANNEL_NAME,
    APPL_STATE_DB,
    ASIC_DB,
    ASIC_GET_RESPONSE_OP,
    ASIC_GET_RESPONSE_TABLE_NAME,
    ASIC_NOTIFICATIONS_CHANNEL_NAME,
    ASIC_VLAN_TABLE_NAME,
    CFG_VLAN_TABLE_NAME,
    CONFIG_DB,
    NOTIFICATION_PORT_STATE_CHANGE,
    OP_DEL,
    OP_SET,
    STATE_DB,
    STATE_PORT_TABLE_NAME,
    VLAN_PREFIX,
    asic_vlan_key,
)
from common.select_loop import SelectLoop
from common.swss import field_value_pairs, load_db_config, swsscommon


def _vlan_id_from_key(key: str) -> str:
    """Extract numeric VLAN ID from a key like ``Vlan100``."""
    return key[len(VLAN_PREFIX):] if key.startswith(VLAN_PREFIX) else key


def main() -> None:
    """Start syncd + portorch + vlanmgrd in a shared event loop."""
    parser = argparse.ArgumentParser(
        description="Run syncd, portorch, and vlanmgrd together as one daemon."
    )
    parser.add_argument("--db-config", help="path to database_config.json")
    args = parser.parse_args()

    load_db_config(args.db_config)

    # Track in-flight ASIC keys to map GETRESPONSE back to VLAN keys
    asic_to_vlan: dict[str, str] = {}

    # --- DB connections ---
    config_db = swsscommon.DBConnector(CONFIG_DB, 0, False)
    appl_db = swsscommon.DBConnector(APPL_DB, 0, False)
    asic_db = swsscommon.DBConnector(ASIC_DB, 0, False)
    state_db = swsscommon.DBConnector(STATE_DB, 0, False)
    appl_state_db = swsscommon.DBConnector(APPL_STATE_DB, 0, False)

    # --- vlanmgrd resources ---
    config_subscriber = swsscommon.SubscriberStateTable(config_db, CFG_VLAN_TABLE_NAME)
    appl_producer = swsscommon.ProducerStateTable(appl_db, APP_VLAN_TABLE_NAME)

    # --- portorch resources ---
    vlan_consumer = swsscommon.ConsumerStateTable(appl_db, APP_VLAN_TABLE_NAME)
    asic_producer = swsscommon.ProducerTable(asic_db, ASIC_VLAN_TABLE_NAME)
    response_consumer = swsscommon.ConsumerTable(asic_db, ASIC_GET_RESPONSE_TABLE_NAME)
    appl_response_producer = swsscommon.NotificationProducer(
        appl_state_db, APPL_RESPONSE_CHANNEL_NAME,
    )
    notification_consumer = swsscommon.NotificationConsumer(
        asic_db, ASIC_NOTIFICATIONS_CHANNEL_NAME,
    )
    port_state_table = swsscommon.Table(state_db, STATE_PORT_TABLE_NAME)

    # --- syncd resources ---
    asic_consumer = swsscommon.ConsumerTable(asic_db, ASIC_VLAN_TABLE_NAME)
    getresponse_producer = swsscommon.ProducerTable(asic_db, ASIC_GET_RESPONSE_TABLE_NAME)

    # --- vlanmgrd: replay all existing VLAN config ---
    config_table = swsscommon.Table(config_db, CFG_VLAN_TABLE_NAME)
    keys = config_table.getKeys()
    for key in keys:
        status, fvs = config_table.get(key)
        if not status:
            continue
        config = {f: v for f, v in fvs}
        vid = config.get("vlanid", _vlan_id_from_key(key))
        appl_producer.set(key, field_value_pairs({"vlanid": vid}))
        print("vlanmgrd: replayed %s %s|%s" % (CONFIG_DB, CFG_VLAN_TABLE_NAME, key))

    # --- handlers ---

    def handle_config_update(_sel: Any) -> object | None:
        """vlanmgrd: forward CONFIG_DB VLAN changes to APPL_DB."""
        key, op, fvs = config_subscriber.pop()
        if not key.startswith(VLAN_PREFIX):
            return None
        if op == OP_SET:
            config = {f: v for f, v in fvs}
            vid = config.get("vlanid", _vlan_id_from_key(key))
            appl_producer.set(key, field_value_pairs({"vlanid": vid}))
            print("vlanmgrd: %s %s|%s SET -> %s %s:%s" % (
                CONFIG_DB, CFG_VLAN_TABLE_NAME, key, APPL_DB, APP_VLAN_TABLE_NAME, key,
            ))
        elif op == OP_DEL:
            appl_producer.delete(key)
            print("vlanmgrd: %s %s|%s DEL -> %s %s:%s" % (
                CONFIG_DB, CFG_VLAN_TABLE_NAME, key, APPL_DB, APP_VLAN_TABLE_NAME, key,
            ))
        return None

    def handle_vlan_update(_sel: Any) -> object | None:
        """portorch: consume APPL_DB VLAN_TABLE, enqueue ASIC operation."""
        key, op, fvs = vlan_consumer.pop()
        if not key.startswith(VLAN_PREFIX):
            return None
        print("PortsOrch: %s %s:%s %s" % (APPL_DB, APP_VLAN_TABLE_NAME, key, op))
        vid = _vlan_id_from_key(key)
        ak = asic_vlan_key(vid)
        if op == OP_SET:
            fields = {f: v for f, v in fvs}
            asic_producer.set(ak, field_value_pairs({
                "SAI_VLAN_ATTR_VLAN_ID": fields.get("vlanid", vid),
                "source": "PortsOrch",
            }))
            asic_to_vlan[ak] = key
            print("PortsOrch: queued SAI create %s:%s" % (ASIC_VLAN_TABLE_NAME, ak))
        elif op == OP_DEL:
            asic_producer.delete(ak)
            asic_to_vlan[ak] = key
            print("PortsOrch: queued SAI remove %s:%s" % (ASIC_VLAN_TABLE_NAME, ak))
        return None

    def handle_asic_update(_sel: Any) -> object | None:
        """syncd: consume ASIC_DB operations, send GETRESPONSE."""
        key, op, fvs = asic_consumer.pop()
        if not key:
            return None
        print("syncd: %s %s:%s %s" % (ASIC_DB, ASIC_VLAN_TABLE_NAME, key, op))
        print("syncd: pretend write ASIC %s %s" % (op, key))
        getresponse_producer.set("SAI_STATUS_SUCCESS", field_value_pairs({
            "err_str": "",
            "request_key": key,
            "request_op": op,
            "source": "syncd",
        }), ASIC_GET_RESPONSE_OP)
        print("syncd: sent GETRESPONSE for %s" % key)
        return None

    def handle_getresponse(_sel: Any) -> object | None:
        """portorch: read GETRESPONSE, publish APPL response."""
        status, op, fvs = response_consumer.pop()
        if op != ASIC_GET_RESPONSE_OP:
            return None
        fields = {f: v for f, v in fvs}
        rkey = fields.get("request_key", "")
        vlan_key = asic_to_vlan.pop(rkey, "")
        if not vlan_key:
            return None
        print("PortsOrch: ASIC response %s %s" % (status, rkey))
        orch_status = "SWSS_RC_SUCCESS" if status == "SAI_STATUS_SUCCESS" else "SWSS_RC_UNKNOWN"
        appl_response_producer.send(orch_status, vlan_key, field_value_pairs({
            "err_str": fields.get("err_str", ""),
            "asic_key": rkey,
            "sai_status": status,
            "source": "PortsOrch",
        }))
        print("PortsOrch: sent APPL response for %s" % vlan_key)
        return None

    def handle_notification(_sel: Any) -> object | None:
        """portorch: async ASIC notification -> STATE_DB."""
        op, data, fvs = notification_consumer.pop()
        if op != NOTIFICATION_PORT_STATE_CHANGE:
            return None
        fields = {f: v for f, v in fvs}
        port = fields.get("port", data)
        oper_status = fields.get("oper_status", "unknown")
        port_state_table.set(port, field_value_pairs({"state": oper_status, "source": "PortsOrch"}))
        print("PortsOrch: %s %s -> %s %s|%s" % (op, port, STATE_DB, STATE_PORT_TABLE_NAME, port))
        return None

    # --- event loop ---
    select_loop = SelectLoop()
    select_loop.add(config_subscriber, handle_config_update)
    select_loop.add(vlan_consumer, handle_vlan_update)
    select_loop.add(asic_consumer, handle_asic_update)
    select_loop.add(response_consumer, handle_getresponse)
    select_loop.add(notification_consumer, handle_notification)

    print("daemon: syncd + portorch + vlanmgrd ready")
    select_loop.run()


if __name__ == "__main__":
    main()
