#!/usr/bin/env python3
#
# Minimal vlanmgrd-style bridge:
#   CONFIG_DB VLAN|Vlan100 -> APPL_DB VLAN_TABLE:Vlan100

import argparse

from vlan_schema import APP_VLAN_TABLE_NAME as APPL_TABLE
from vlan_schema import CFG_VLAN_TABLE_NAME as CONFIG_TABLE
from vlan_schema import VLAN_PREFIX
from vlan_log import add_log_argument
from vlan_log import configure_logger
from vlan_log import emit_redis_marker
from vlan_log import log_hash_snapshot
from vlan_log import log_table_event
from swsscommon_compat import load_swsscommon


swsscommon = load_swsscommon()


def field_value_pairs(fields):
    return swsscommon.FieldValuePairs([(str(k), str(v)) for k, v in fields.items()])


def default_vlan_id_from_key(key):
    if key.startswith(VLAN_PREFIX):
        return key[len(VLAN_PREFIX):]
    return ""

# Database: CONFIG_DB
# Table|Key->Value: VLAN|Vlan100 -> {"vlanid": "100"}
#     |
#     V
#   [vlanmgrd]
#     |
#     V
# Database: APPL_DB
# Table:Key->Value: VLAN_TABLE:Vlan100 -> {"vlanid": "100"}

def publish_set(appl_db, appl_table, logger, key, field_values):
    config = {field: value for field, value in field_values}
    vlan_id = config.get("vlanid", default_vlan_id_from_key(key))
    appl_values = {"vlanid": vlan_id}
    final_key = "%s:%s" % (APPL_TABLE, key)
    pending_key = "_%s:%s" % (APPL_TABLE, key)
    log_hash_snapshot(
        logger,
        "vlanmgrd",
        "before ProducerStateTable.set final APPL_DB hash",
        "APPL_DB",
        final_key,
        appl_db.hgetall(final_key),
    )
    emit_redis_marker(appl_db, "vlanmgrd", "before", "ProducerStateTable.set", "APPL_DB", APPL_TABLE, key)
    appl_table.set(key, field_value_pairs(appl_values))
    emit_redis_marker(appl_db, "vlanmgrd", "after", "ProducerStateTable.set", "APPL_DB", APPL_TABLE, key)
    log_table_event(
        logger,
        "vlanmgrd",
        "ProducerStateTable.set",
        "WRITE",
        "APPL_DB",
        APPL_TABLE,
        key,
        op="SET",
        fields=appl_values.items(),
        note="ProducerStateTable writes pending state; ConsumerStateTable materializes APPL_DB content",
    )
    log_hash_snapshot(
        logger,
        "vlanmgrd",
        "after ProducerStateTable.set final APPL_DB hash",
        "APPL_DB",
        final_key,
        appl_db.hgetall(final_key),
    )
    log_hash_snapshot(
        logger,
        "vlanmgrd",
        "after ProducerStateTable.set pending APPL_DB hash",
        "APPL_DB",
        pending_key,
        appl_db.hgetall(pending_key),
    )

    print("vlanmgrd: CONFIG_DB %s|%s SET -> APPL_DB %s:%s SET" % (
        CONFIG_TABLE,
        key,
        APPL_TABLE,
        key,
    ))
    print('  fields: {"vlanid": "%s"}' % vlan_id)


def publish_delete(appl_db, appl_table, logger, key):
    final_key = "%s:%s" % (APPL_TABLE, key)
    log_hash_snapshot(
        logger,
        "vlanmgrd",
        "before ProducerStateTable.delete final APPL_DB hash",
        "APPL_DB",
        final_key,
        appl_db.hgetall(final_key),
    )
    emit_redis_marker(appl_db, "vlanmgrd", "before", "ProducerStateTable.delete", "APPL_DB", APPL_TABLE, key)
    appl_table.delete(key)
    emit_redis_marker(appl_db, "vlanmgrd", "after", "ProducerStateTable.delete", "APPL_DB", APPL_TABLE, key)
    log_table_event(
        logger,
        "vlanmgrd",
        "ProducerStateTable.delete",
        "WRITE",
        "APPL_DB",
        APPL_TABLE,
        key,
        op="DEL",
        note="ProducerStateTable queues delete pending state; ConsumerStateTable materializes deletion",
    )
    log_hash_snapshot(
        logger,
        "vlanmgrd",
        "after ProducerStateTable.delete final APPL_DB hash",
        "APPL_DB",
        final_key,
        appl_db.hgetall(final_key),
    )
    print("vlanmgrd: CONFIG_DB %s|%s DEL -> APPL_DB %s:%s DEL" % (
        CONFIG_TABLE,
        key,
        APPL_TABLE,
        key,
    ))


def main():
    parser = argparse.ArgumentParser(
        description="Subscribe to CONFIG_DB VLAN changes and publish APPL_DB VLAN_TABLE updates."
    )
    parser.add_argument("--vlan-id", default="100", help="only process this VLAN ID")
    parser.add_argument(
        "--watch",
        action="store_true",
        help="continue processing CONFIG_DB updates instead of exiting after one matching event",
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
    config_db = swsscommon.DBConnector("CONFIG_DB", 0, False)
    appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
    config_table = swsscommon.Table(config_db, CONFIG_TABLE)
    config_subscriber = swsscommon.SubscriberStateTable(config_db, CONFIG_TABLE)
    appl_table = swsscommon.ProducerStateTable(appl_db, APPL_TABLE)
    selector = swsscommon.Select()
    selector.addSelectable(config_subscriber)

    print("vlanmgrd: waiting for CONFIG_DB %s|%s updates" % (CONFIG_TABLE, key_filter))
    print("vlanmgrd: db log %s" % log_path)

    if not args.watch:
        emit_redis_marker(config_db, "vlanmgrd", "before", "Table.get", "CONFIG_DB", CONFIG_TABLE, key_filter)
        status, field_values = config_table.get(key_filter)
        emit_redis_marker(config_db, "vlanmgrd", "after", "Table.get", "CONFIG_DB", CONFIG_TABLE, key_filter)
        if status:
            log_table_event(
                logger,
                "vlanmgrd",
                "Table.get",
                "READ",
                "CONFIG_DB",
                CONFIG_TABLE,
                key_filter,
                op="SET",
                fields=field_values,
                note="One-shot replay reads existing CONFIG_DB content",
            )
            publish_set(appl_db, appl_table, logger, key_filter, field_values)
            return

    while True:
        state, selectable = selector.select()
        if state != swsscommon.Select.OBJECT:
            continue

        key, op, field_values = config_subscriber.pop()
        if key != key_filter:
            continue
        log_table_event(
            logger,
            "vlanmgrd",
            "SubscriberStateTable.pop",
            "READ",
            "CONFIG_DB",
            CONFIG_TABLE,
            key,
            op=op,
            fields=field_values,
            note="Subscriber reads CONFIG_DB content already materialized by Table writer",
        )

        if op == "SET":
            publish_set(appl_db, appl_table, logger, key, field_values)
        elif op == "DEL":
            publish_delete(appl_db, appl_table, logger, key)
        else:
            print("vlanmgrd: ignoring CONFIG_DB %s|%s op %s" % (CONFIG_TABLE, key, op))

        if not args.watch:
            return


if __name__ == "__main__":
    main()
