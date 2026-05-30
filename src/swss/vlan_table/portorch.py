#!/usr/bin/env python3
#
# Minimal PortsOrch-style APPL_DB consumer:
#   ConsumerStateTable(APPL_DB, "VLAN_TABLE").pop()
#   sai_vlan_api->create_vlan() represented by ProducerTable(ASIC_DB, ...).set()

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.custom_schema import APP_VLAN_TABLE_NAME as APPL_TABLE
from common.custom_schema import APPL_RESPONSE_CHANNEL_NAME
from common.custom_schema import ASIC_GET_RESPONSE_OP
from common.custom_schema import ASIC_GET_RESPONSE_TABLE_NAME
from common.custom_schema import ASIC_NOTIFICATIONS_CHANNEL_NAME
from common.custom_schema import ASIC_VLAN_TABLE_NAME as ASIC_TABLE
from common.custom_schema import STATE_PORT_TABLE_NAME
from common.custom_schema import VLAN_PREFIX
from common.custom_schema import asic_vlan_key
from common.db_logging import add_log_argument
from common.db_logging import configure_logger
from common.db_logging import emit_redis_marker
from common.db_logging import log_table_event
from common.select_loop import SelectLoop
from common.swss import field_value_pairs
from common.swss import load_db_config
from common.swss import swsscommon


def pop_sai_response(asic_db, logger, response_consumer, request_key):
    emit_redis_marker(asic_db, "portorch", "before", "ConsumerTable.pop", "ASIC_DB", ASIC_GET_RESPONSE_TABLE_NAME, request_key)
    status, op, field_values = response_consumer.pop()
    emit_redis_marker(asic_db, "portorch", "after", "ConsumerTable.pop", "ASIC_DB", ASIC_GET_RESPONSE_TABLE_NAME, request_key)
    response_fields = {field: value for field, value in field_values}
    response_key = response_fields.get("request_key", "")
    if op != ASIC_GET_RESPONSE_OP or response_key != request_key:
        print("PortsOrch: ignoring ASIC response %s %s" % (op, status))
        return False, status, response_key, field_values

    log_table_event(
        logger,
        "portorch",
        "ConsumerTable.pop",
        "READ",
        "ASIC_DB",
        ASIC_GET_RESPONSE_TABLE_NAME,
        status,
        op=op,
        fields=field_values,
        note="sairedis GETRESPONSE table delivers syncd operation result to the requester",
    )
    print("PortsOrch: ASIC response %s %s" % (status, response_key))
    for field, value in field_values:
        print("  %s=%s" % (field, value))
    return True, status, response_key, field_values


def publish_appl_response(appl_state_db, logger, producer, vlan_key, sai_op, asic_key, sai_field_values):
    sai_fields = {field: value for field, value in sai_field_values}
    orch_status = "SWSS_RC_SUCCESS" if sai_op == "SAI_STATUS_SUCCESS" else "SWSS_RC_UNKNOWN"
    response_fields = {
        "err_str": sai_fields.get("err_str", ""),
        "asic_key": asic_key,
        "sai_status": sai_op,
        "sai_request_op": sai_fields.get("request_op", ""),
        "source": "PortsOrch",
    }
    emit_redis_marker(appl_state_db, "portorch", "before", "NotificationProducer.send", "APPL_STATE_DB", APPL_RESPONSE_CHANNEL_NAME, vlan_key)
    producer.send(orch_status, vlan_key, field_value_pairs(response_fields))
    emit_redis_marker(appl_state_db, "portorch", "after", "NotificationProducer.send", "APPL_STATE_DB", APPL_RESPONSE_CHANNEL_NAME, vlan_key)
    log_table_event(
        logger,
        "portorch",
        "NotificationProducer.send",
        "WRITE",
        "APPL_STATE_DB",
        APPL_RESPONSE_CHANNEL_NAME,
        vlan_key,
        op=orch_status,
        fields=response_fields.items(),
        note="PortsOrch propagates ASIC operation result to the APPL response channel",
    )
    print("PortsOrch: sent APPL response channel %s for %s" % (APPL_RESPONSE_CHANNEL_NAME, vlan_key))


def process_vlan_update(args, logger):
    key_filter = "%s%s" % (VLAN_PREFIX, args.vlan_id)
    appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
    appl_state_db = swsscommon.DBConnector("APPL_STATE_DB", 0, False)
    asic_db = swsscommon.DBConnector("ASIC_DB", 0, False)
    vlan_consumer = swsscommon.ConsumerStateTable(appl_db, APPL_TABLE)
    asic_producer = swsscommon.ProducerTable(asic_db, ASIC_TABLE)
    appl_response_producer = swsscommon.NotificationProducer(appl_state_db, APPL_RESPONSE_CHANNEL_NAME)
    response_consumer = None
    select_loop = SelectLoop(swsscommon)
    if args.wait_sai_response:
        response_consumer = swsscommon.ConsumerTable(asic_db, ASIC_GET_RESPONSE_TABLE_NAME)

    print("PortsOrch: waiting for APPL_DB %s:%s updates" % (APPL_TABLE, key_filter))
    if args.wait_sai_response:
        print("PortsOrch: waiting for ASIC_DB %s %s responses" % (ASIC_GET_RESPONSE_TABLE_NAME, ASIC_GET_RESPONSE_OP))

    pending_responses = {}

    def handle_sai_response(_selectable):
        if not pending_responses:
            pop_sai_response(asic_db, logger, response_consumer, "")
            return None

        matched, sai_op, asic_key, field_values = pop_sai_response(
            asic_db,
            logger,
            response_consumer,
            next(iter(pending_responses)),
        )
        if matched:
            vlan_key = pending_responses.pop(asic_key)
            publish_appl_response(appl_state_db, logger, appl_response_producer, vlan_key, sai_op, asic_key, field_values)
            if not args.watch:
                return SelectLoop.STOP
        return None

    def handle_vlan_update(_selectable):
        emit_redis_marker(appl_db, "portorch", "before", "ConsumerStateTable.pop", "APPL_DB", APPL_TABLE, key_filter)
        key, op, field_values = vlan_consumer.pop()
        emit_redis_marker(appl_db, "portorch", "after", "ConsumerStateTable.pop", "APPL_DB", APPL_TABLE, key_filter)
        if key != key_filter:
            return None

        log_table_event(
            logger,
            "portorch",
            "ConsumerStateTable.pop",
            "READ",
            "APPL_DB",
            APPL_TABLE,
            key,
            op=op,
            fields=field_values,
            note="ConsumerStateTable pop materializes APPL_DB final table content",
        )

        print("PortsOrch: APPL_DB update %s:%s %s" % (APPL_TABLE, key, op))
        for field, value in field_values:
            print("  %s=%s" % (field, value))

        appl_fields = {field: value for field, value in field_values}
        vlan_id = appl_fields.get("vlanid", args.vlan_id)
        asic_key = asic_vlan_key(vlan_id)
        if op == "SET":
            asic_fields = {
                "SAI_VLAN_ATTR_VLAN_ID": vlan_id,
                "source": "PortsOrch",
            }
            emit_redis_marker(asic_db, "portorch", "before", "ProducerTable.set", "ASIC_DB", ASIC_TABLE, asic_key)
            asic_producer.set(asic_key, field_value_pairs(asic_fields))
            emit_redis_marker(asic_db, "portorch", "after", "ProducerTable.set", "ASIC_DB", ASIC_TABLE, asic_key)
            log_table_event(
                logger,
                "portorch",
                "ProducerTable.set",
                "WRITE",
                "ASIC_DB",
                ASIC_TABLE,
                asic_key,
                op="SET",
                fields=asic_fields.items(),
                note="SAI request path: ProducerTable enqueues ordered ASIC_DB operation for syncd",
            )
            print("PortsOrch: queued SAI create request %s:%s" % (ASIC_TABLE, asic_key))
            if args.wait_sai_response:
                pending_responses[asic_key] = key
        elif op == "DEL":
            emit_redis_marker(asic_db, "portorch", "before", "ProducerTable.delete", "ASIC_DB", ASIC_TABLE, asic_key)
            asic_producer.delete(asic_key)
            emit_redis_marker(asic_db, "portorch", "after", "ProducerTable.delete", "ASIC_DB", ASIC_TABLE, asic_key)
            log_table_event(
                logger,
                "portorch",
                "ProducerTable.delete",
                "WRITE",
                "ASIC_DB",
                ASIC_TABLE,
                asic_key,
                op="DEL",
                note="SAI request path: ProducerTable enqueues ordered ASIC_DB delete for syncd",
            )
            print("PortsOrch: queued SAI remove request %s:%s" % (ASIC_TABLE, asic_key))
            if args.wait_sai_response:
                pending_responses[asic_key] = key

        if not args.watch and not args.wait_sai_response:
            return SelectLoop.STOP
        return None

    select_loop.add(vlan_consumer, handle_vlan_update)
    if response_consumer is not None:
        select_loop.add(response_consumer, handle_sai_response)
    select_loop.run()


def process_asic_notification(args, logger):
    asic_db = swsscommon.DBConnector("ASIC_DB", 0, False)
    state_db = swsscommon.DBConnector("STATE_DB", 0, False)
    notification_consumer = swsscommon.NotificationConsumer(asic_db, ASIC_NOTIFICATIONS_CHANNEL_NAME)
    port_state_table = swsscommon.Table(state_db, STATE_PORT_TABLE_NAME)
    select_loop = SelectLoop(swsscommon)

    print("PortsOrch: waiting for ASIC_DB:%s async notifications" % ASIC_NOTIFICATIONS_CHANNEL_NAME)

    def handle_notification(_selectable):
        emit_redis_marker(asic_db, "portorch", "before", "NotificationConsumer.pop", "ASIC_DB", ASIC_NOTIFICATIONS_CHANNEL_NAME, args.port)
        op, data, field_values = notification_consumer.pop()
        emit_redis_marker(asic_db, "portorch", "after", "NotificationConsumer.pop", "ASIC_DB", ASIC_NOTIFICATIONS_CHANNEL_NAME, args.port)
        if op != "port_state_change":
            print("PortsOrch: ignoring async notification %s %s" % (op, data))
            if not args.watch:
                return SelectLoop.STOP
            return None

        fields = {field: value for field, value in field_values}
        port = fields.get("port", data or args.port)
        oper_status = fields.get("oper_status", args.oper_status)
        values = {"state": oper_status, "source": "PortsOrch"}
        emit_redis_marker(state_db, "portorch", "before", "Table.set", "STATE_DB", STATE_PORT_TABLE_NAME, port)
        port_state_table.set(port, field_value_pairs(values))
        emit_redis_marker(state_db, "portorch", "after", "Table.set", "STATE_DB", STATE_PORT_TABLE_NAME, port)

        log_table_event(
            logger,
            "portorch",
            "NotificationConsumer.pop+Table.set",
            "WRITE",
            "STATE_DB",
            STATE_PORT_TABLE_NAME,
            port,
            op="SET",
            fields=values.items(),
            note="Async notification path: syncd notification is converted into STATE_DB for mgrd consumers",
        )
        print("PortsOrch: async %s for %s -> STATE_DB %s|%s" % (
            op,
            port,
            STATE_PORT_TABLE_NAME,
            port,
        ))
        for field, value in values.items():
            print("  %s=%s" % (field, value))

        if not args.watch:
            return SelectLoop.STOP
        return None

    select_loop.add(notification_consumer, handle_notification)
    select_loop.run()


def main():
    parser = argparse.ArgumentParser(
        description="Consume APPL_DB VLAN_TABLE updates like SONiC PortsOrch."
    )
    parser.add_argument("--vlan-id", default="100", help="only process this VLAN ID")
    parser.add_argument("--port", default="Ethernet0", help="port name for async notification demo")
    parser.add_argument("--oper-status", default="ok", help="STATE_DB port state value for async notification demo")
    parser.add_argument(
        "--notification-only",
        action="store_true",
        help="consume ASIC_DB:NOTIFICATIONS and write STATE_DB instead of processing VLAN_TABLE",
    )
    parser.add_argument(
        "--wait-sai-response",
        action="store_true",
        help="wait for syncd's ASIC DB response channel after enqueueing the SAI request",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="continue processing updates instead of exiting after one matching event",
    )
    parser.add_argument(
        "--db-config",
        help="path to database_config.json; useful when running Redis in a local host container",
    )
    add_log_argument(parser)
    args = parser.parse_args()
    logger, _ = configure_logger(args.log_file)

    load_db_config(args.db_config)

    if args.notification_only:
        process_asic_notification(args, logger)
    else:
        process_vlan_update(args, logger)


if __name__ == "__main__":
    main()
