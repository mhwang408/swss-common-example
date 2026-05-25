#!/usr/bin/env python3
#
# Minimal VlanOrch-style APPL_DB consumer:
#   ConsumerStateTable(APPL_DB, "VLAN_TABLE").pop()

import argparse

from vlan_schema import APP_VLAN_TABLE_NAME as APPL_TABLE
from vlan_schema import ASIC_VLAN_TABLE_NAME as ASIC_TABLE
from vlan_schema import VLAN_PREFIX
from vlan_schema import asic_vlan_key
from vlan_log import add_log_argument
from vlan_log import configure_logger
from vlan_log import log_table_event
from swsscommon_compat import load_swsscommon


swsscommon = load_swsscommon()


def field_value_pairs(fields):
    return swsscommon.FieldValuePairs([(str(k), str(v)) for k, v in fields.items()])


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
    add_log_argument(parser)
    args = parser.parse_args()
    logger, log_path = configure_logger(args.log_file)

    if args.db_config:
        swsscommon.SonicDBConfig.load_sonic_db_config(args.db_config)

    key_filter = "%s%s" % (VLAN_PREFIX, args.vlan_id)
    appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
    asic_db = swsscommon.DBConnector("ASIC_DB", 0, False)
    vlan_consumer = swsscommon.ConsumerStateTable(appl_db, APPL_TABLE)
    asic_producer = swsscommon.ProducerTable(asic_db, ASIC_TABLE)
    selector = swsscommon.Select()
    selector.addSelectable(vlan_consumer)

    print("VlanOrch: waiting for APPL_DB %s:%s updates" % (APPL_TABLE, key_filter))
    print("VlanOrch: db log %s" % log_path)

    while True:
        state, selectable = selector.select()
        if state != swsscommon.Select.OBJECT:
            continue

        key, op, field_values = vlan_consumer.pop()
        if key != key_filter:
            continue
        log_table_event(
            logger,
            "vlanorch",
            "ConsumerStateTable.pop",
            "READ",
            "APPL_DB",
            APPL_TABLE,
            key,
            op=op,
            fields=field_values,
            note="ConsumerStateTable pop materializes APPL_DB final table content",
        )

        print("VlanOrch: APPL_DB update %s:%s %s" % (APPL_TABLE, key, op))
        for field, value in field_values:
            print("  %s=%s" % (field, value))

        appl_fields = {field: value for field, value in field_values}
        vlan_id = appl_fields.get("vlanid", args.vlan_id)
        asic_key = asic_vlan_key(vlan_id)
        if op == "SET":
            asic_fields = {
                "SAI_VLAN_ATTR_VLAN_ID": vlan_id,
                "source": "VlanOrch",
            }
            asic_producer.set(asic_key, field_value_pairs(asic_fields))
            log_table_event(
                logger,
                "vlanorch",
                "ProducerTable.set",
                "WRITE",
                "ASIC_DB",
                ASIC_TABLE,
                asic_key,
                op="SET",
                fields=asic_fields.items(),
                note="ProducerTable enqueues ASIC_DB update; ConsumerTable materializes ASIC_DB content",
            )
            print("VlanOrch: queued ASIC_DB %s:%s SET" % (ASIC_TABLE, asic_key))
        elif op == "DEL":
            asic_producer.delete(asic_key)
            log_table_event(
                logger,
                "vlanorch",
                "ProducerTable.delete",
                "WRITE",
                "ASIC_DB",
                ASIC_TABLE,
                asic_key,
                op="DEL",
                note="ProducerTable enqueues ASIC_DB delete; ConsumerTable materializes deletion",
            )
            print("VlanOrch: queued ASIC_DB %s:%s DEL" % (ASIC_TABLE, asic_key))

        if not args.watch:
            return


if __name__ == "__main__":
    main()
