#!/usr/bin/env python3
#
# Minimal syncd-style ASIC_DB consumer:
#   ConsumerTable(ASIC_DB, "ASIC_STATE:SAI_OBJECT_TYPE_VLAN").pop()

import argparse

from vlan_schema import ASIC_VLAN_TABLE_NAME as ASIC_TABLE
from vlan_schema import asic_vlan_key
from vlan_log import add_log_argument
from vlan_log import configure_logger
from vlan_log import emit_redis_marker
from vlan_log import log_hash_snapshot
from vlan_log import log_table_event
from swsscommon_compat import load_swsscommon


swsscommon = load_swsscommon()


def main():
    parser = argparse.ArgumentParser(
        description="Consume ASIC_DB VLAN updates like a tiny syncd and pretend to write ASIC."
    )
    parser.add_argument("--vlan-id", default="100", help="only print this VLAN ID")
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
    logger, log_path = configure_logger(args.log_file)

    if args.db_config:
        swsscommon.SonicDBConfig.load_sonic_db_config(args.db_config)

    key_filter = asic_vlan_key(args.vlan_id)
    asic_db = swsscommon.DBConnector("ASIC_DB", 0, False)
    asic_consumer = swsscommon.ConsumerTable(asic_db, ASIC_TABLE)
    selector = swsscommon.Select()
    selector.addSelectable(asic_consumer)

    print("syncd: waiting for ASIC_DB %s:%s updates" % (ASIC_TABLE, key_filter))
    print("syncd: db log %s" % log_path)

    while True:
        state, selectable = selector.select()
        if state != swsscommon.Select.OBJECT:
            continue

        final_key = "%s:%s" % (ASIC_TABLE, key_filter)
        log_hash_snapshot(
            logger,
            "syncd",
            "before ConsumerTable.pop final ASIC_DB hash",
            "ASIC_DB",
            final_key,
            asic_db.hgetall(final_key),
        )
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
        log_hash_snapshot(
            logger,
            "syncd",
            "after ConsumerTable.pop final ASIC_DB hash",
            "ASIC_DB",
            "%s:%s" % (ASIC_TABLE, key),
            asic_db.hgetall("%s:%s" % (ASIC_TABLE, key)),
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

        if not args.watch:
            return


if __name__ == "__main__":
    main()
