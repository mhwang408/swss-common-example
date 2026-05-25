#!/usr/bin/env python3
#
# Minimal VlanOrch-style APPL_DB consumer:
#   ConsumerStateTable(APPL_DB, "VLAN_TABLE").pop()

import argparse

from vlan_schema import APP_VLAN_TABLE_NAME as APPL_TABLE
from vlan_schema import VLAN_PREFIX
from swsscommon_compat import load_swsscommon


swsscommon = load_swsscommon()


def main():
    parser = argparse.ArgumentParser(
        description="Consume APPL_DB VLAN_TABLE updates like a tiny VlanOrch."
    )
    parser.add_argument("--vlan-id", default="100", help="only print this VLAN ID")
    parser.add_argument(
        "--watch",
        action="store_true",
        help="continue processing APPL_DB updates instead of exiting after one matching event",
    )
    parser.add_argument(
        "--db-config",
        help="path to database_config.json; useful when running Redis in a local host container",
    )
    args = parser.parse_args()

    if args.db_config:
        swsscommon.SonicDBConfig.load_sonic_db_config(args.db_config)

    key_filter = "%s%s" % (VLAN_PREFIX, args.vlan_id)
    appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
    vlan_consumer = swsscommon.ConsumerStateTable(appl_db, APPL_TABLE)
    selector = swsscommon.Select()
    selector.addSelectable(vlan_consumer)

    print("VlanOrch: waiting for APPL_DB %s:%s updates" % (APPL_TABLE, key_filter))

    while True:
        state, selectable = selector.select()
        if state != swsscommon.Select.OBJECT:
            continue

        key, op, field_values = vlan_consumer.pop()
        if key != key_filter:
            continue

        print("VlanOrch: APPL_DB update %s:%s %s" % (APPL_TABLE, key, op))
        for field, value in field_values:
            print("  %s=%s" % (field, value))

        if not args.watch:
            return


if __name__ == "__main__":
    main()
