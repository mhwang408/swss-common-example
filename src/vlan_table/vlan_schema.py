# Python constants for the VLAN CONFIG_DB -> APPL_DB teaching example.

CFG_VLAN_TABLE_NAME = "VLAN"
APP_VLAN_TABLE_NAME = "VLAN_TABLE"
ASIC_VLAN_TABLE_NAME = "ASIC_STATE:SAI_OBJECT_TYPE_VLAN"
VLAN_PREFIX = "Vlan"


def asic_vlan_key(vlan_id):
    return "oid:0x2600000000%04d" % int(vlan_id)
