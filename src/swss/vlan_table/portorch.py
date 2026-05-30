#!/usr/bin/env python3
"""Minimal PortsOrch-style APPL_DB consumer for the VLAN flow.

Consumes APPL_DB VLAN_TABLE desired state via ``ConsumerStateTable``,
translates it into ordered ASIC operations via ``ProducerTable``, optionally
waits for syncd's GETRESPONSE, and publishes the result back on the APPL
response channel.

Data flow::

    APPL_DB VLAN_TABLE:Vlan100 (materialized by ConsumerStateTable.pop)
        → [PortsOrch]
        → ASIC_DB queue ASIC_STATE:SAI_OBJECT_TYPE_VLAN (ProducerTable.set)
        → syncd GETRESPONSE
        → APPL_STATE_DB response channel (NotificationProducer.send)

For the async notification flow (ASIC_DB:NOTIFICATIONS → STATE_DB), see
``notification_orch.py``.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import _path_setup  # noqa: F401

from common.db_logging import marked_redis_operation
from common.schema import (
    APP_VLAN_TABLE_NAME,
    APPL_DB,
    APPL_RESPONSE_CHANNEL_NAME,
    APPL_STATE_DB,
    ASIC_DB,
    ASIC_GET_RESPONSE_OP,
    ASIC_GET_RESPONSE_TABLE_NAME,
    ASIC_VLAN_TABLE_NAME,
    OP_DEL,
    OP_SET,
    VLAN_PREFIX,
    asic_vlan_key,
)
from common.select_loop import SelectLoop
from common.swss import field_value_pairs, load_db_config, swsscommon
from vlan_table.notification_orch import NotificationFlowOrch


# ---------------------------------------------------------------------------
# VLAN request lifecycle
# ---------------------------------------------------------------------------

class VlanRequestState(Enum):
    """Tracks a VLAN request through the orch → syncd → response pipeline."""

    APPL_RECEIVED = "APPL_RECEIVED"
    ASIC_SENT = "ASIC_SENT"
    ASIC_RESPONDED = "ASIC_RESPONDED"
    APPL_RESPONDED = "APPL_RESPONDED"


@dataclass
class VlanRequest:
    """In-flight VLAN request state."""

    vlan_key: str
    vlan_id: str
    asic_key: str
    operation: str
    state: VlanRequestState = VlanRequestState.APPL_RECEIVED

    def move_to(self, state: VlanRequestState) -> None:
        """Advance the request to the next lifecycle state."""
        self.state = state


# ---------------------------------------------------------------------------
# VlanFlowOrch
# ---------------------------------------------------------------------------

class VlanFlowOrch:
    """Consume APPL_DB VLAN updates, produce ASIC operations, handle responses.

    This class models the VLAN-specific portion of PortsOrch: it consumes
    desired state from APPL_DB, enqueues SAI operations into ASIC_DB, and
    optionally waits for syncd's GETRESPONSE before publishing the result
    on the APPL response channel.

    Args:
        args: Parsed CLI arguments.
    """

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self._vlan_key_filter = "%s%s" % (VLAN_PREFIX, args.vlan_id)
        self._requests_by_asic_key: dict[str, VlanRequest] = {}

    def run(self) -> None:
        """Set up DB connections and run the VLAN event loop."""
        self._appl_db = swsscommon.DBConnector(APPL_DB, 0, False)
        self._appl_state_db = swsscommon.DBConnector(APPL_STATE_DB, 0, False)
        self._asic_db = swsscommon.DBConnector(ASIC_DB, 0, False)

        self._vlan_consumer = swsscommon.ConsumerStateTable(
            self._appl_db, APP_VLAN_TABLE_NAME,
        )
        self._asic_producer = swsscommon.ProducerTable(
            self._asic_db, ASIC_VLAN_TABLE_NAME,
        )
        self._appl_response_producer = swsscommon.NotificationProducer(
            self._appl_state_db, APPL_RESPONSE_CHANNEL_NAME,
        )
        self._response_consumer: Any = None

        select_loop = SelectLoop()

        if self.args.wait_sai_response:
            self._response_consumer = swsscommon.ConsumerTable(
                self._asic_db, ASIC_GET_RESPONSE_TABLE_NAME,
            )

        print("PortsOrch: waiting for %s %s:%s updates" % (
            APPL_DB, APP_VLAN_TABLE_NAME, self._vlan_key_filter,
        ))
        if self.args.wait_sai_response:
            print("PortsOrch: waiting for %s %s %s responses" % (
                ASIC_DB, ASIC_GET_RESPONSE_TABLE_NAME, ASIC_GET_RESPONSE_OP,
            ))

        select_loop.add(self._vlan_consumer, self._handle_vlan_update)
        if self._response_consumer is not None:
            select_loop.add(self._response_consumer, self._handle_sai_response)
        select_loop.run()

    # ------------------------------------------------------------------
    # APPL_DB VLAN consumer
    # ------------------------------------------------------------------

    def _handle_vlan_update(self, _selectable: Any) -> object | None:
        """Pop one APPL_DB VLAN_TABLE entry and enqueue the SAI operation."""
        with marked_redis_operation(
            self._appl_db, "portorch",
            "ConsumerStateTable.pop", APPL_DB,
            APP_VLAN_TABLE_NAME, self._vlan_key_filter,
        ):
            key, op, field_values = self._vlan_consumer.pop()

        if key != self._vlan_key_filter:
            return None

        print("PortsOrch: %s update %s:%s %s" % (APPL_DB, APP_VLAN_TABLE_NAME, key, op))
        for field, value in field_values:
            print("  %s=%s" % (field, value))

        request = self._build_request(key, op, field_values)

        if request.operation == OP_SET:
            self._send_sai_create(request)
        elif request.operation == OP_DEL:
            self._send_sai_remove(request)
        else:
            print("PortsOrch: ignoring %s %s:%s op %s" % (
                APPL_DB, APP_VLAN_TABLE_NAME, key, op,
            ))
            return None

        if self.args.wait_sai_response:
            self._requests_by_asic_key[request.asic_key] = request
        return None

    def _build_request(
        self,
        vlan_key: str,
        operation: str,
        field_values: list[tuple[str, str]],
    ) -> VlanRequest:
        """Construct a VlanRequest from the APPL_DB pop result."""
        appl_fields = {field: value for field, value in field_values}
        vlan_id = appl_fields.get("vlanid", self.args.vlan_id)
        return VlanRequest(
            vlan_key=vlan_key,
            vlan_id=vlan_id,
            asic_key=asic_vlan_key(vlan_id),
            operation=operation,
        )

    # ------------------------------------------------------------------
    # ASIC_DB producer
    # ------------------------------------------------------------------

    def _send_sai_create(self, request: VlanRequest) -> None:
        """Enqueue a SAI create-vlan operation into ASIC_DB."""
        asic_fields = {
            "SAI_VLAN_ATTR_VLAN_ID": request.vlan_id,
            "source": "PortsOrch",
        }
        with marked_redis_operation(
            self._asic_db, "portorch",
            "ProducerTable.set", ASIC_DB,
            ASIC_VLAN_TABLE_NAME, request.asic_key,
        ):
            self._asic_producer.set(request.asic_key, field_value_pairs(asic_fields))
        request.move_to(VlanRequestState.ASIC_SENT)
        print("PortsOrch: queued SAI create request %s:%s" % (
            ASIC_VLAN_TABLE_NAME, request.asic_key,
        ))

    def _send_sai_remove(self, request: VlanRequest) -> None:
        """Enqueue a SAI remove-vlan operation into ASIC_DB."""
        with marked_redis_operation(
            self._asic_db, "portorch",
            "ProducerTable.delete", ASIC_DB,
            ASIC_VLAN_TABLE_NAME, request.asic_key,
        ):
            self._asic_producer.delete(request.asic_key)
        request.move_to(VlanRequestState.ASIC_SENT)
        print("PortsOrch: queued SAI remove request %s:%s" % (
            ASIC_VLAN_TABLE_NAME, request.asic_key,
        ))

    # ------------------------------------------------------------------
    # ASIC_DB GETRESPONSE consumer
    # ------------------------------------------------------------------

    def _handle_sai_response(self, _selectable: Any) -> object | None:
        """Pop a GETRESPONSE entry and publish the APPL response."""
        if not self._requests_by_asic_key:
            self._pop_sai_response("")
            return None

        expected_key = next(iter(self._requests_by_asic_key))
        matched, sai_op, asic_key, field_values = self._pop_sai_response(expected_key)
        if not matched:
            return None

        request = self._requests_by_asic_key.pop(asic_key)
        request.move_to(VlanRequestState.ASIC_RESPONDED)
        self._publish_appl_response(request, sai_op, field_values)
        request.move_to(VlanRequestState.APPL_RESPONDED)
        return None

    def _pop_sai_response(
        self, request_key: str,
    ) -> tuple[bool, str, str, list[tuple[str, str]]]:
        """Pop one GETRESPONSE entry and check if it matches *request_key*."""
        with marked_redis_operation(
            self._asic_db, "portorch",
            "ConsumerTable.pop", ASIC_DB,
            ASIC_GET_RESPONSE_TABLE_NAME, request_key,
        ):
            status, op, field_values = self._response_consumer.pop()

        response_fields = {field: value for field, value in field_values}
        response_key = response_fields.get("request_key", "")

        if op != ASIC_GET_RESPONSE_OP or response_key != request_key:
            print("PortsOrch: ignoring ASIC response %s %s" % (op, status))
            return False, status, response_key, field_values

        print("PortsOrch: ASIC response %s %s" % (status, response_key))
        for field, value in field_values:
            print("  %s=%s" % (field, value))
        return True, status, response_key, field_values

    # ------------------------------------------------------------------
    # APPL response publisher
    # ------------------------------------------------------------------

    def _publish_appl_response(
        self,
        request: VlanRequest,
        sai_op: str,
        sai_field_values: list[tuple[str, str]],
    ) -> None:
        """Send the orchestration result on the APPL response channel."""
        sai_fields = {field: value for field, value in sai_field_values}
        orch_status = (
            "SWSS_RC_SUCCESS" if sai_op == "SAI_STATUS_SUCCESS" else "SWSS_RC_UNKNOWN"
        )
        response_fields = {
            "err_str": sai_fields.get("err_str", ""),
            "asic_key": request.asic_key,
            "sai_status": sai_op,
            "sai_request_op": sai_fields.get("request_op", ""),
            "source": "PortsOrch",
        }
        with marked_redis_operation(
            self._appl_state_db, "portorch",
            "NotificationProducer.send", APPL_STATE_DB,
            APPL_RESPONSE_CHANNEL_NAME, request.vlan_key,
        ):
            self._appl_response_producer.send(
                orch_status, request.vlan_key, field_value_pairs(response_fields),
            )
        print("PortsOrch: sent APPL response channel %s for %s" % (
            APPL_RESPONSE_CHANNEL_NAME, request.vlan_key,
        ))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point — dispatches to VlanFlowOrch or NotificationFlowOrch."""
    parser = argparse.ArgumentParser(
        description="Consume APPL_DB VLAN_TABLE updates like SONiC PortsOrch."
    )
    parser.add_argument("--vlan-id", default="100", help="only process this VLAN ID")
    parser.add_argument("--port", default="Ethernet0", help="port name for notification demo")
    parser.add_argument(
        "--oper-status", default="ok",
        help="STATE_DB port state value for notification demo",
    )
    parser.add_argument(
        "--notification-only", action="store_true",
        help="consume ASIC_DB:NOTIFICATIONS and write STATE_DB instead of VLAN_TABLE",
    )
    parser.add_argument(
        "--wait-sai-response", action="store_true",
        help="wait for syncd's ASIC DB response after enqueueing the SAI request",
    )
    parser.add_argument("--db-config", help="path to database_config.json")
    args = parser.parse_args()

    load_db_config(args.db_config)

    if args.notification_only:
        NotificationFlowOrch(args).run()
    else:
        VlanFlowOrch(args).run()


if __name__ == "__main__":
    main()
