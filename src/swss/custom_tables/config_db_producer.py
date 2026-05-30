#!/usr/bin/env python3
"""Write one entry into a custom CONFIG_DB table.

Demonstrates the simplest SONiC write pattern: using ``Table.set()`` to
materialize a durable hash directly in CONFIG_DB::

    CONFIG_DB  CUSTOM_CONFIG_TABLE|demo  {"enabled": "true", "interval": "10", ...}
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import _path_setup  # noqa: F401

from common.schema import CONFIG_DB, EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME
from common.swss import field_value_pairs, load_db_config, swsscommon


def main() -> None:
    """Parse CLI args and write a config entry to CONFIG_DB."""
    parser = argparse.ArgumentParser(
        description="Write one entry into a custom table in CONFIG_DB."
    )
    parser.add_argument("--key", default="demo", help="table entry key")
    parser.add_argument("--enabled", default="true", help="sample config field")
    parser.add_argument("--interval", default="10", help="sample config field")
    parser.add_argument("--db-config", help="path to database_config.json")
    args = parser.parse_args()

    load_db_config(args.db_config)

    config_db = swsscommon.DBConnector(CONFIG_DB, 0, False)
    config_table = swsscommon.Table(config_db, EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME)

    values = {
        "enabled": args.enabled,
        "interval": args.interval,
        "updated_at": str(int(time.time())),
    }
    config_table.set(args.key, field_value_pairs(values))

    print("Wrote %s entry" % CONFIG_DB)
    print("  table: %s" % EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME)
    print("  key: %s" % args.key)
    print("  redis key: %s|%s" % (EXAMPLE_CFG_CUSTOM_CONFIG_TABLE_NAME, args.key))
    for field, value in values.items():
        print("  %s=%s" % (field, value))


if __name__ == "__main__":
    main()
