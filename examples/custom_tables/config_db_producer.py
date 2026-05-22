#!/usr/bin/env python3
#
# Minimal producer for a custom CONFIG_DB table.
#
# Example Redis key:
#   DB 4: CUSTOM_CONFIG_TABLE|demo

import argparse
import time

from swsscommon import swsscommon


CONFIG_TABLE = "CUSTOM_CONFIG_TABLE"


def field_value_pairs(fields):
    return swsscommon.FieldValuePairs([(str(k), str(v)) for k, v in fields.items()])


def main():
    parser = argparse.ArgumentParser(
        description="Write one entry into a custom table in CONFIG_DB."
    )
    parser.add_argument("--key", default="demo", help="table entry key")
    parser.add_argument("--enabled", default="true", help="sample config field")
    parser.add_argument("--interval", default="10", help="sample config field")
    parser.add_argument(
        "--tcp",
        action="store_true",
        help="connect to Redis by TCP instead of the SONiC Unix socket",
    )
    parser.add_argument(
        "--db-config",
        help="path to database_config.json; useful when running Redis in a local host container",
    )
    args = parser.parse_args()

    if args.db_config:
        swsscommon.SonicDBConfig.load_sonic_db_config(args.db_config)

    config_db = swsscommon.DBConnector("CONFIG_DB", 0, args.tcp)
    config_table = swsscommon.Table(config_db, CONFIG_TABLE)

    values = {
        "enabled": args.enabled,
        "interval": args.interval,
        "updated_at": str(int(time.time())),
    }
    config_table.set(args.key, field_value_pairs(values))

    print("Wrote CONFIG_DB entry")
    print("  table: %s" % CONFIG_TABLE)
    print("  key: %s" % args.key)
    print("  redis key: %s|%s" % (CONFIG_TABLE, args.key))
    for field, value in values.items():
        print("  %s=%s" % (field, value))


if __name__ == "__main__":
    main()
