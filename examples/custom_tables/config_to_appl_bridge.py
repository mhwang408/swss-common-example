#!/usr/bin/env python3
#
# Minimal consumer of the custom CONFIG_DB table and producer of a custom
# APPL_DB table.
#
# Example Redis keys:
#   DB 4: CUSTOM_CONFIG_TABLE|demo
#   DB 0: CUSTOM_APPL_TABLE:demo

import argparse
import time

from swsscommon import swsscommon


CONFIG_TABLE = "CUSTOM_CONFIG_TABLE"
APPL_TABLE = "CUSTOM_APPL_TABLE"


def field_value_pairs(fields):
    return swsscommon.FieldValuePairs([(str(k), str(v)) for k, v in fields.items()])


def table_values_to_dict(field_values):
    return {field: value for field, value in field_values}


def bridge_entry(config_table, appl_table, key):
    found, field_values = config_table.get(key)
    if not found:
        return False

    config = table_values_to_dict(field_values)
    appl_values = {
        "admin_status": "up" if config.get("enabled", "false").lower() == "true" else "down",
        "poll_interval": config.get("interval", "0"),
        "source_table": CONFIG_TABLE,
        "source_key": key,
        "published_at": str(int(time.time())),
    }
    appl_table.set(key, field_value_pairs(appl_values))

    print("Read CONFIG_DB %s|%s and wrote APPL_DB %s:%s" % (
        CONFIG_TABLE,
        key,
        APPL_TABLE,
        key,
    ))
    for field, value in appl_values.items():
        print("  %s=%s" % (field, value))

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Read a custom CONFIG_DB table entry and publish an APPL_DB entry."
    )
    parser.add_argument("--key", default="demo", help="table entry key")
    parser.add_argument(
        "--watch",
        action="store_true",
        help="poll CONFIG_DB and republish whenever the config changes",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="seconds between CONFIG_DB polls when --watch is used",
    )
    args = parser.parse_args()

    config_db = swsscommon.DBConnector("CONFIG_DB", 0, True)
    appl_db = swsscommon.DBConnector("APPL_DB", 0, True)
    config_table = swsscommon.Table(config_db, CONFIG_TABLE)
    appl_table = swsscommon.Table(appl_db, APPL_TABLE)

    if not args.watch:
        if not bridge_entry(config_table, appl_table, args.key):
            raise SystemExit(
                "CONFIG_DB entry %s|%s was not found. Run config_db_producer.py first."
                % (CONFIG_TABLE, args.key)
            )
        return

    last_seen = None
    while True:
        found, field_values = config_table.get(args.key)
        current = tuple(sorted(field_values)) if found else None
        if current != last_seen:
            if found:
                bridge_entry(config_table, appl_table, args.key)
            else:
                print("Waiting for CONFIG_DB %s|%s" % (CONFIG_TABLE, args.key))
            last_seen = current
        time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
