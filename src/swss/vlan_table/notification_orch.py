#!/usr/bin/env python3
"""ASIC notification flow: ASIC_DB:NOTIFICATIONS → STATE_DB PORT_TABLE.

This module handles the asynchronous notification path where syncd publishes
port-state-change events and PortsOrch converts them into STATE_DB entries
that other daemons (like vlanmgrd) can observe::

    ASIC_DB NOTIFICATIONS channel
        → [PortsOrch NotificationConsumer.pop]
        → STATE_DB PORT_TABLE|Ethernet0  {"state": "ok"}
"""

from __future__ import annotations

import argparse
from typing import Any

from common.db_logging import marked_redis_operation
from common.schema import (
    ASIC_DB,
    ASIC_NOTIFICATIONS_CHANNEL_NAME,
    NOTIFICATION_PORT_STATE_CHANGE,
    STATE_DB,
    STATE_PORT_TABLE_NAME,
)
from common.select_loop import SelectLoop
from common.swss import field_value_pairs, swsscommon


class NotificationFlowOrch:
    """Consume ASIC_DB async notifications and write STATE_DB port state.

    This class models the subset of PortsOrch that handles asynchronous
    notifications from syncd (e.g. port link state changes) independently
    of the VLAN request/response lifecycle.

    Args:
        args: Parsed CLI arguments (expects ``port``, ``oper_status``, ``watch``).
    """

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args

    def run(self) -> None:
        """Set up consumers and run the notification event loop."""
        self._asic_db = swsscommon.DBConnector(ASIC_DB, 0, False)
        self._state_db = swsscommon.DBConnector(STATE_DB, 0, False)
        self._notification_consumer = swsscommon.NotificationConsumer(
            self._asic_db, ASIC_NOTIFICATIONS_CHANNEL_NAME,
        )
        self._port_state_table = swsscommon.Table(self._state_db, STATE_PORT_TABLE_NAME)

        select_loop = SelectLoop()
        print("PortsOrch: waiting for %s:%s async notifications" % (
            ASIC_DB, ASIC_NOTIFICATIONS_CHANNEL_NAME,
        ))
        select_loop.add(self._notification_consumer, self._handle_notification)
        select_loop.run()

    def _handle_notification(self, _selectable: Any) -> object | None:
        """Process one async notification from ASIC_DB."""
        with marked_redis_operation(
            self._asic_db, "portorch",
            "NotificationConsumer.pop", ASIC_DB,
            ASIC_NOTIFICATIONS_CHANNEL_NAME, self.args.port,
        ):
            op, data, field_values = self._notification_consumer.pop()

        if op != NOTIFICATION_PORT_STATE_CHANGE:
            print("PortsOrch: ignoring async notification %s %s" % (op, data))
            if not self.args.watch:
                return SelectLoop.STOP
            return None

        fields = {field: value for field, value in field_values}
        port = fields.get("port", data or self.args.port)
        oper_status = fields.get("oper_status", self.args.oper_status)
        values = {"state": oper_status, "source": "PortsOrch"}

        with marked_redis_operation(
            self._state_db, "portorch",
            "Table.set", STATE_DB, STATE_PORT_TABLE_NAME, port,
        ):
            self._port_state_table.set(port, field_value_pairs(values))

        print("PortsOrch: async %s for %s -> %s %s|%s" % (
            op, port, STATE_DB, STATE_PORT_TABLE_NAME, port,
        ))
        for field, value in values.items():
            print("  %s=%s" % (field, value))

        if not self.args.watch:
            return SelectLoop.STOP
        return None
