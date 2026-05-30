#!/usr/bin/env python3
"""Emulate ``config vlan add/del <vlan_id>`` writes to CONFIG_DB.

This is the entry point of the VLAN data path.  It writes (or deletes) a
durable hash in CONFIG_DB using the ``Table`` API, which materializes the
final Redis key immediately::

    CONFIG_DB  VLAN|Vlan100  {"vlanid": "100"}
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import _path_setup  # noqa: F401

from common.db_logging import emit_redis_marker
from common.schema import CFG_VLAN_TABLE_NAME, CONFIG_DB, VLAN_PREFIX
from common.swss import field_value_pairs, load_db_config, swsscommon


def vlan_key(vlan_id: str) -> str:
    """Build the CONFIG_DB VLAN entry key (e.g. ``Vlan100``)."""
    return "%s%s" % (VLAN_PREFIX, vlan_id)


def main() -> None:
    """Parse CLI args and write/delete a VLAN entry in CONFIG_DB."""
    parser = argparse.ArgumentParser(
        description="Emulate 'config vlan add/del <vlan_id>' writes to CONFIG_DB."
    )
    parser.add_argument("operation", choices=["add", "del"], help="config vlan operation")
    parser.add_argument("vlan_id", help="numeric VLAN ID, for example 100")
    parser.add_argument(
        "--db-config",
        help="path to database_config.json",
    )
    args = parser.parse_args()

    load_db_config(args.db_config)

    config_db = swsscommon.DBConnector(CONFIG_DB, 0, False)
    config_table = swsscommon.Table(config_db, CFG_VLAN_TABLE_NAME)
    key = vlan_key(args.vlan_id)

    if args.operation == "add":
        values = {"vlanid": args.vlan_id}
        emit_redis_marker(
            config_db, "config_vlan_command", "before",
            "Table.set", CONFIG_DB, CFG_VLAN_TABLE_NAME, key,
        )
        config_table.set(key, field_value_pairs(values))
        emit_redis_marker(
            config_db, "config_vlan_command", "after",
            "Table.set", CONFIG_DB, CFG_VLAN_TABLE_NAME, key,
        )
        print("config vlan add %s" % args.vlan_id)
        print("  wrote %s %s|%s" % (CONFIG_DB, CFG_VLAN_TABLE_NAME, key))
        print('  fields: {"vlanid": "%s"}' % args.vlan_id)
    else:
        emit_redis_marker(
            config_db, "config_vlan_command", "before",
            "Table.delete", CONFIG_DB, CFG_VLAN_TABLE_NAME, key,
        )
        config_table.delete(key)
        emit_redis_marker(
            config_db, "config_vlan_command", "after",
            "Table.delete", CONFIG_DB, CFG_VLAN_TABLE_NAME, key,
        )
        print("config vlan del %s" % args.vlan_id)
        print("  deleted %s %s|%s" % (CONFIG_DB, CFG_VLAN_TABLE_NAME, key))


if __name__ == "__main__":
    main()
