"""Shared table-name and operation constants for the teaching examples.

All magic strings (DB names, table names, operations, notification channels)
are defined here so that consuming scripts reference a single source of truth.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# SONiC database logical names
# ---------------------------------------------------------------------------
CONFIG_DB: str = "CONFIG_DB"
APPL_DB: str = "APPL_DB"
ASIC_DB: str = "ASIC_DB"
STATE_DB: str = "STATE_DB"
APPL_STATE_DB: str = "APPL_STATE_DB"

# ---------------------------------------------------------------------------
# Table operation strings returned by SubscriberStateTable / ConsumerTable
# ---------------------------------------------------------------------------
OP_SET: str = "SET"
OP_DEL: str = "DEL"

# ---------------------------------------------------------------------------
# VLAN table names (mirrors sonic-swss-common schema.h)
# ---------------------------------------------------------------------------
CFG_VLAN_TABLE_NAME: str = "VLAN"
APP_VLAN_TABLE_NAME: str = "VLAN_TABLE"
ASIC_VLAN_TABLE_NAME: str = "ASIC_STATE:SAI_OBJECT_TYPE_VLAN"
VLAN_PREFIX: str = "Vlan"

# ---------------------------------------------------------------------------
# ASIC_DB response / notification tables
# ---------------------------------------------------------------------------
ASIC_GET_RESPONSE_TABLE_NAME: str = "GETRESPONSE"
ASIC_GET_RESPONSE_OP: str = "getresponse"
ASIC_NOTIFICATIONS_CHANNEL_NAME: str = "NOTIFICATIONS"

# ---------------------------------------------------------------------------
# Notification operation strings
# ---------------------------------------------------------------------------
NOTIFICATION_PORT_STATE_CHANGE: str = "port_state_change"

# ---------------------------------------------------------------------------
# STATE_DB tables
# ---------------------------------------------------------------------------
STATE_PORT_TABLE_NAME: str = "PORT_TABLE"

# ---------------------------------------------------------------------------
# Custom example table names
# ---------------------------------------------------------------------------
EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME: str = "CUSTOM_CONFIG_TABLE"
EXAMPLE_APP_CUSTOM_APPL_TABLE_NAME: str = "CUSTOM_APPL_TABLE"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def asic_vlan_key(vlan_id: int | str) -> str:
    """Generate a fake ASIC OID key for a VLAN.

    Args:
        vlan_id: Numeric VLAN ID (e.g. 100).

    Returns:
        A string like ``oid:0x2600000000NNNN``.
    """
    return "oid:0x2600000000%04d" % int(vlan_id)


def response_channel_name(db_name: str, table_name: str) -> str:
    """Build the SONiC-style response channel name.

    Args:
        db_name: Logical DB name (e.g. ``APPL_DB``).
        table_name: Table name (e.g. ``VLAN_TABLE``).

    Returns:
        Channel name like ``APPL_DB_VLAN_TABLE_RESPONSE_CHANNEL``.
    """
    return "%s_%s_RESPONSE_CHANNEL" % (db_name, table_name)


APPL_RESPONSE_CHANNEL_NAME: str = response_channel_name(APPL_DB, APP_VLAN_TABLE_NAME)
"""Pre-built response channel for the VLAN APPL flow."""
