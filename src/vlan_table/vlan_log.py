"""Shared file logging for the VLAN table teaching scripts."""

from __future__ import annotations

import json
import logging
from pathlib import Path


DEFAULT_LOG_FILE = "/var/run/redis/vlan_table_db.log"


def add_log_argument(parser):
    parser.add_argument(
        "--log-file",
        default=DEFAULT_LOG_FILE,
        help="file that records DB table reads/writes for verification",
    )


def configure_logger(log_file):
    path = Path(log_file)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        path = Path("vlan_table_db.log")

    logger = logging.getLogger("vlan_table")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    handler = logging.FileHandler(path)
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    logger.addHandler(handler)
    return logger, str(path)


def fields_to_dict(field_values):
    return {str(field): str(value) for field, value in field_values}


def log_table_event(logger, actor, api, action, db_name, table, key, op="", fields=None, note=""):
    payload = {
        "actor": actor,
        "api": api,
        "action": action,
        "db": db_name,
        "table": table,
        "key": key,
    }
    if op:
        payload["op"] = op
    if fields is not None:
        payload["fields"] = fields_to_dict(fields)
    if note:
        payload["note"] = note
    logger.info(json.dumps(payload, sort_keys=True))
