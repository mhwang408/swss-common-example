#!/usr/bin/env python3
#
# Minimal event-driven consumer of the custom CONFIG_DB table and producer of a
# custom APPL_DB table.
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


def publish_set(appl_table, key, field_values):
    config = {field: value for field, value in field_values}
    appl_values = {
        "admin_status": "up" if config.get("enabled", "false").lower() == "true" else "down",
        "poll_interval": config.get("interval", "0"),
        "source_table": CONFIG_TABLE,
        "source_key": key,
        "published_at": str(int(time.time())),
    }
    appl_table.set(key, field_value_pairs(appl_values))

    print("Received CONFIG_DB %s|%s SET and published APPL_DB %s:%s SET" % (
        CONFIG_TABLE,
        key,
        APPL_TABLE,
        key,
    ))
    for field, value in appl_values.items():
        print("  %s=%s" % (field, value))


def publish_delete(appl_table, key):
    appl_table.delete(key)
    print("Received CONFIG_DB %s|%s DEL and published APPL_DB %s:%s DEL" % (
        CONFIG_TABLE,
        key,
        APPL_TABLE,
        key,
    ))


def main():
    parser = argparse.ArgumentParser(
        description="Subscribe to a custom CONFIG_DB table and publish APPL_DB updates."
    )
    parser.add_argument("--key", default="demo", help="table entry key")
    parser.add_argument(
        "--watch",
        action="store_true",
        help="continue processing CONFIG_DB updates instead of exiting after one matching event",
    )
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
    appl_db = swsscommon.DBConnector("APPL_DB", 0, args.tcp)
    config_subscriber = swsscommon.SubscriberStateTable(config_db, CONFIG_TABLE)
    appl_table = swsscommon.ProducerStateTable(appl_db, APPL_TABLE)
    selector = swsscommon.Select()
    selector.addSelectable(config_subscriber)

    print("Waiting for CONFIG_DB %s|%s updates" % (CONFIG_TABLE, args.key))

    while True:
        state, selectable = selector.select()
        if state != swsscommon.Select.OBJECT:
            continue

        key, op, field_values = selectable.pop()
        if key != args.key:
            continue

        if op == "SET":
            publish_set(appl_table, key, field_values)
        elif op == "DEL":
            publish_delete(appl_table, key)
        else:
            print("Ignoring CONFIG_DB %s|%s op %s" % (CONFIG_TABLE, key, op))

        if not args.watch:
            return


if __name__ == "__main__":
    main()
