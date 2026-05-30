#!/usr/bin/env python3
"""Bridge custom CONFIG_DB table changes into APPL_DB.

Subscribes to CONFIG_DB ``CUSTOM_CONFIG_TABLE`` via ``SubscriberStateTable``
and publishes desired state into APPL_DB ``CUSTOM_APPL_TABLE`` via
``ProducerStateTable``::

    CONFIG_DB CUSTOM_CONFIG_TABLE|demo
        → [bridge SubscriberStateTable.pop]
        → APPL_DB _CUSTOM_APPL_TABLE:demo (pending, via ProducerStateTable.set)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import _path_setup  # noqa: F401

from common.schema import (
    APPL_DB,
    CONFIG_DB,
    EXAMPLE_APP_CUSTOM_APPL_TABLE_NAME,
    EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME,
    OP_DEL,
    OP_SET,
)
from common.select_loop import SelectLoop
from common.swss import field_value_pairs, load_db_config, swsscommon


def publish_set(appl_table: Any, key: str, field_values: list[tuple[str, str]]) -> None:
    """Transform CONFIG_DB fields and publish a SET to APPL_DB."""
    config = {field: value for field, value in field_values}
    appl_values = {
        "admin_status": "up" if config.get("enabled", "false").lower() == "true" else "down",
        "poll_interval": config.get("interval", "0"),
        "source_table": EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME,
        "source_key": key,
        "published_at": str(int(time.time())),
    }
    appl_table.set(key, field_value_pairs(appl_values))

    print("Received %s %s|%s SET -> %s %s:%s SET" % (
        CONFIG_DB, EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME, key,
        APPL_DB, EXAMPLE_APP_CUSTOM_APPL_TABLE_NAME, key,
    ))
    for field, value in appl_values.items():
        print("  %s=%s" % (field, value))


def publish_delete(appl_table: Any, key: str) -> None:
    """Publish a DEL to APPL_DB for the given key."""
    appl_table.delete(key)
    print("Received %s %s|%s DEL -> %s %s:%s DEL" % (
        CONFIG_DB, EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME, key,
        APPL_DB, EXAMPLE_APP_CUSTOM_APPL_TABLE_NAME, key,
    ))


def main() -> None:
    """Subscribe to CONFIG_DB changes and bridge them to APPL_DB."""
    parser = argparse.ArgumentParser(
        description="Subscribe to a custom CONFIG_DB table and publish APPL_DB updates."
    )
    parser.add_argument("--key", default="demo", help="table entry key")
    parser.add_argument(
        "--watch", action="store_true",
        help="continue processing instead of exiting after one event",
    )
    parser.add_argument("--db-config", help="path to database_config.json")
    args = parser.parse_args()

    load_db_config(args.db_config)

    config_db = swsscommon.DBConnector(CONFIG_DB, 0, False)
    appl_db = swsscommon.DBConnector(APPL_DB, 0, False)
    config_subscriber = swsscommon.SubscriberStateTable(
        config_db, EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME,
    )
    appl_table = swsscommon.ProducerStateTable(
        appl_db, EXAMPLE_APP_CUSTOM_APPL_TABLE_NAME,
    )
    select_loop = SelectLoop()

    print("Waiting for %s %s|%s updates" % (
        CONFIG_DB, EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME, args.key,
    ))

    def handle_config_update(_selectable: Any) -> object | None:
        key, op, field_values = config_subscriber.pop()
        if key != args.key:
            return None

        if op == OP_SET:
            publish_set(appl_table, key, field_values)
        elif op == OP_DEL:
            publish_delete(appl_table, key)
        else:
            print("Ignoring %s %s|%s op %s" % (
                CONFIG_DB, EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME, key, op,
            ))

        if not args.watch:
            return SelectLoop.STOP
        return None

    select_loop.add(config_subscriber, handle_config_update)
    select_loop.run()


if __name__ == "__main__":
    main()
