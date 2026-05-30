#!/usr/bin/env python3
#
# Minimal stand-in for:
#   config vlan add 100
#   config vlan del 100
#
# Example Redis key:
#   DB 4: VLAN|Vlan100

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.custom_schema import CFG_VLAN_TABLE_NAME as CONFIG_TABLE
from common.custom_schema import VLAN_PREFIX
from common.db_logging import add_log_argument
from common.db_logging import configure_logger
from common.db_logging import emit_redis_marker
from common.db_logging import log_table_event
from common.swss import field_value_pairs
from common.swss import load_db_config
from common.swss import swsscommon


def vlan_key(vlan_id):
    return "%s%s" % (VLAN_PREFIX, vlan_id)


def main():
    parser = argparse.ArgumentParser(
        description="Emulate 'config vlan add/del <vlan_id>' writes to CONFIG_DB."
    )
    parser.add_argument("operation", choices=["add", "del"], help="config vlan operation")
    parser.add_argument("vlan_id", help="numeric VLAN ID, for example 100")
    parser.add_argument(
        "--db-config",
        help="path to database_config.json; useful when running Redis in a local host container",
    )
    add_log_argument(parser)
    args = parser.parse_args()
    logger, _ = configure_logger(args.log_file)

    load_db_config(args.db_config)

    config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
    config_table = swsscommon.Table(config_db, CONFIG_TABLE)
    key = vlan_key(args.vlan_id)

    # usage: config vlan add/del 100
    # Database: CONFIG_DB (DB ID: 4)
    # Key/Value: VLAN|Vlan100 -> {"vlanid": "100"}
    if args.operation == "add":
        values = {"vlanid": args.vlan_id}
        emit_redis_marker(config_db, "config_vlan_command", "before", "Table.set", "CONFIG_DB", CONFIG_TABLE, key)
        config_table.set(key, field_value_pairs(values))
        emit_redis_marker(config_db, "config_vlan_command", "after", "Table.set", "CONFIG_DB", CONFIG_TABLE, key)
        log_table_event(
            logger,
            "config_vlan_command",
            "Table.set",
            "WRITE",
            "CONFIG_DB",
            CONFIG_TABLE,
            key,
            op="SET",
            fields=values.items(),
            note="Table writer materializes CONFIG_DB hash content immediately",
        )
        print("config vlan add %s" % args.vlan_id)
        print("  wrote CONFIG_DB %s|%s" % (CONFIG_TABLE, key))
        print('  fields: {"vlanid": "%s"}' % args.vlan_id)
    else:
        emit_redis_marker(config_db, "config_vlan_command", "before", "Table.delete", "CONFIG_DB", CONFIG_TABLE, key)
        config_table.delete(key)
        emit_redis_marker(config_db, "config_vlan_command", "after", "Table.delete", "CONFIG_DB", CONFIG_TABLE, key)
        log_table_event(
            logger,
            "config_vlan_command",
            "Table.delete",
            "WRITE",
            "CONFIG_DB",
            CONFIG_TABLE,
            key,
            op="DEL",
            note="Table writer deletes CONFIG_DB hash content immediately",
        )
        print("config vlan del %s" % args.vlan_id)
        print("  deleted CONFIG_DB %s|%s" % (CONFIG_TABLE, key))


if __name__ == "__main__":
    main()
