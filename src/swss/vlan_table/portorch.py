#!/usr/bin/env python3
#
# Minimal PortsOrch-style APPL_DB consumer:
#   ConsumerStateTable(APPL_DB, "VLAN_TABLE").pop()
#   sai_vlan_api->create_vlan() represented by ProducerTable(ASIC_DB, ...).set()

import argparse
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.custom_schema import APP_VLAN_TABLE_NAME
from common.custom_schema import APPL_RESPONSE_CHANNEL_NAME
from common.custom_schema import ASIC_GET_RESPONSE_OP
from common.custom_schema import ASIC_GET_RESPONSE_TABLE_NAME
from common.custom_schema import ASIC_NOTIFICATIONS_CHANNEL_NAME
from common.custom_schema import ASIC_VLAN_TABLE_NAME
from common.custom_schema import STATE_PORT_TABLE_NAME
from common.custom_schema import VLAN_PREFIX
from common.custom_schema import asic_vlan_key
from common.db_logging import marked_redis_operation
from common.select_loop import SelectLoop
from common.swss import field_value_pairs
from common.swss import load_db_config
from common.swss import swsscommon


class VlanRequestState(Enum):
    APPL_RECEIVED = "APPL_RECEIVED"
    ASIC_SENT = "ASIC_SENT"
    ASIC_RESPONDED = "ASIC_RESPONDED"
    APPL_RESPONDED = "APPL_RESPONDED"


@dataclass
class VlanRequest:
    vlan_key: str
    vlan_id: str
    asic_key: str
    operation: str
    state: VlanRequestState = VlanRequestState.APPL_RECEIVED

    def move_to(self, state):
        self.state = state


class PortsOrchDemo:
    def __init__(self, args):
        self.args = args
        self.vlan_key_filter = "%s%s" % (VLAN_PREFIX, args.vlan_id)
        self.requests_by_asic_key = {}

    def run(self):
        if self.args.notification_only:
            self.run_notification_flow()
            return
        self.run_vlan_flow()

    def run_vlan_flow(self):
        self.appl_db = swsscommon.DBConnector("APPL_DB", 0, False)
        self.appl_state_db = swsscommon.DBConnector("APPL_STATE_DB", 0, False)
        self.asic_db = swsscommon.DBConnector("ASIC_DB", 0, False)
        self.vlan_consumer = swsscommon.ConsumerStateTable(self.appl_db, APP_VLAN_TABLE_NAME)
        self.asic_producer = swsscommon.ProducerTable(self.asic_db, ASIC_VLAN_TABLE_NAME)
        self.appl_response_producer = swsscommon.NotificationProducer(
            self.appl_state_db,
            APPL_RESPONSE_CHANNEL_NAME,
        )
        self.response_consumer = None
        select_loop = SelectLoop()

        if self.args.wait_sai_response:
            self.response_consumer = swsscommon.ConsumerTable(
                self.asic_db,
                ASIC_GET_RESPONSE_TABLE_NAME,
            )

        print("PortsOrch: waiting for APPL_DB %s:%s updates" % (
            APP_VLAN_TABLE_NAME,
            self.vlan_key_filter,
        ))
        if self.args.wait_sai_response:
            print("PortsOrch: waiting for ASIC_DB %s %s responses" % (
                ASIC_GET_RESPONSE_TABLE_NAME,
                ASIC_GET_RESPONSE_OP,
            ))

        select_loop.add(self.vlan_consumer, self.handle_vlan_update)
        if self.response_consumer is not None:
            select_loop.add(self.response_consumer, self.handle_sai_response)
        select_loop.run()

    def handle_vlan_update(self, _selectable):
        with marked_redis_operation(
            self.appl_db,
            "portorch",
            "ConsumerStateTable.pop",
            "APPL_DB",
            APP_VLAN_TABLE_NAME,
            self.vlan_key_filter,
        ):
            key, op, field_values = self.vlan_consumer.pop()
        if key != self.vlan_key_filter:
            return None

        print("PortsOrch: APPL_DB update %s:%s %s" % (APP_VLAN_TABLE_NAME, key, op))
        for field, value in field_values:
            print("  %s=%s" % (field, value))

        request = self.build_vlan_request(key, op, field_values)
        if request.operation == "SET":
            self.send_sai_create_request(request)
        elif request.operation == "DEL":
            self.send_sai_remove_request(request)
        else:
            print("PortsOrch: ignoring APPL_DB %s:%s op %s" % (
                APP_VLAN_TABLE_NAME,
                key,
                op,
            ))
            return None

        if self.args.wait_sai_response:
            self.requests_by_asic_key[request.asic_key] = request
        elif not self.args.watch:
            return SelectLoop.STOP
        return None

    def build_vlan_request(self, vlan_key, operation, field_values):
        appl_fields = {field: value for field, value in field_values}
        vlan_id = appl_fields.get("vlanid", self.args.vlan_id)
        return VlanRequest(
            vlan_key=vlan_key,
            vlan_id=vlan_id,
            asic_key=asic_vlan_key(vlan_id),
            operation=operation,
        )

    def send_sai_create_request(self, request):
        asic_fields = {
            "SAI_VLAN_ATTR_VLAN_ID": request.vlan_id,
            "source": "PortsOrch",
        }
        with marked_redis_operation(
            self.asic_db,
            "portorch",
            "ProducerTable.set",
            "ASIC_DB",
            ASIC_VLAN_TABLE_NAME,
            request.asic_key,
        ):
            self.asic_producer.set(request.asic_key, field_value_pairs(asic_fields))
        request.move_to(VlanRequestState.ASIC_SENT)
        print("PortsOrch: queued SAI create request %s:%s" % (
            ASIC_VLAN_TABLE_NAME,
            request.asic_key,
        ))

    def send_sai_remove_request(self, request):
        with marked_redis_operation(
            self.asic_db,
            "portorch",
            "ProducerTable.delete",
            "ASIC_DB",
            ASIC_VLAN_TABLE_NAME,
            request.asic_key,
        ):
            self.asic_producer.delete(request.asic_key)
        request.move_to(VlanRequestState.ASIC_SENT)
        print("PortsOrch: queued SAI remove request %s:%s" % (
            ASIC_VLAN_TABLE_NAME,
            request.asic_key,
        ))

    def handle_sai_response(self, _selectable):
        if not self.requests_by_asic_key:
            self.pop_sai_response("")
            return None

        expected_asic_key = next(iter(self.requests_by_asic_key))
        matched, sai_op, asic_key, field_values = self.pop_sai_response(expected_asic_key)
        if not matched:
            return None

        request = self.requests_by_asic_key.pop(asic_key)
        request.move_to(VlanRequestState.ASIC_RESPONDED)
        self.publish_appl_response(request, sai_op, field_values)
        request.move_to(VlanRequestState.APPL_RESPONDED)
        if not self.args.watch:
            return SelectLoop.STOP
        return None

    def pop_sai_response(self, request_key):
        with marked_redis_operation(
            self.asic_db,
            "portorch",
            "ConsumerTable.pop",
            "ASIC_DB",
            ASIC_GET_RESPONSE_TABLE_NAME,
            request_key,
        ):
            status, op, field_values = self.response_consumer.pop()
        response_fields = {field: value for field, value in field_values}
        response_key = response_fields.get("request_key", "")
        if op != ASIC_GET_RESPONSE_OP or response_key != request_key:
            print("PortsOrch: ignoring ASIC response %s %s" % (op, status))
            return False, status, response_key, field_values

        print("PortsOrch: ASIC response %s %s" % (status, response_key))
        for field, value in field_values:
            print("  %s=%s" % (field, value))
        return True, status, response_key, field_values

    def publish_appl_response(self, request, sai_op, sai_field_values):
        sai_fields = {field: value for field, value in sai_field_values}
        orch_status = "SWSS_RC_SUCCESS" if sai_op == "SAI_STATUS_SUCCESS" else "SWSS_RC_UNKNOWN"
        response_fields = {
            "err_str": sai_fields.get("err_str", ""),
            "asic_key": request.asic_key,
            "sai_status": sai_op,
            "sai_request_op": sai_fields.get("request_op", ""),
            "source": "PortsOrch",
        }
        with marked_redis_operation(
            self.appl_state_db,
            "portorch",
            "NotificationProducer.send",
            "APPL_STATE_DB",
            APPL_RESPONSE_CHANNEL_NAME,
            request.vlan_key,
        ):
            self.appl_response_producer.send(
                orch_status,
                request.vlan_key,
                field_value_pairs(response_fields),
            )
        print("PortsOrch: sent APPL response channel %s for %s" % (
            APPL_RESPONSE_CHANNEL_NAME,
            request.vlan_key,
        ))

    def run_notification_flow(self):
        self.asic_db = swsscommon.DBConnector("ASIC_DB", 0, False)
        self.state_db = swsscommon.DBConnector("STATE_DB", 0, False)
        self.notification_consumer = swsscommon.NotificationConsumer(
            self.asic_db,
            ASIC_NOTIFICATIONS_CHANNEL_NAME,
        )
        self.port_state_table = swsscommon.Table(self.state_db, STATE_PORT_TABLE_NAME)
        select_loop = SelectLoop()

        print("PortsOrch: waiting for ASIC_DB:%s async notifications" % (
            ASIC_NOTIFICATIONS_CHANNEL_NAME,
        ))
        select_loop.add(self.notification_consumer, self.handle_notification)
        select_loop.run()

    def handle_notification(self, _selectable):
        with marked_redis_operation(
            self.asic_db,
            "portorch",
            "NotificationConsumer.pop",
            "ASIC_DB",
            ASIC_NOTIFICATIONS_CHANNEL_NAME,
            self.args.port,
        ):
            op, data, field_values = self.notification_consumer.pop()
        if op != "port_state_change":
            print("PortsOrch: ignoring async notification %s %s" % (op, data))
            if not self.args.watch:
                return SelectLoop.STOP
            return None

        fields = {field: value for field, value in field_values}
        port = fields.get("port", data or self.args.port)
        oper_status = fields.get("oper_status", self.args.oper_status)
        values = {"state": oper_status, "source": "PortsOrch"}
        with marked_redis_operation(
            self.state_db,
            "portorch",
            "Table.set",
            "STATE_DB",
            STATE_PORT_TABLE_NAME,
            port,
        ):
            self.port_state_table.set(port, field_value_pairs(values))

        print("PortsOrch: async %s for %s -> STATE_DB %s|%s" % (
            op,
            port,
            STATE_PORT_TABLE_NAME,
            port,
        ))
        for field, value in values.items():
            print("  %s=%s" % (field, value))

        if not self.args.watch:
            return SelectLoop.STOP
        return None


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
    args = parser.parse_args()

    load_db_config(args.db_config)
    PortsOrchDemo(args).run()


if __name__ == "__main__":
    main()
