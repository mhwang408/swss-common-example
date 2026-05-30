"""Shared file and Redis marker logging for DB table examples."""

from __future__ import annotations

import json
import logging
import os
import time
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

    if path.exists() and not os.access(path, os.W_OK):
        path.unlink()

    logger = logging.getLogger("db_table_examples")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    handler = logging.FileHandler(path)
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    logger.addHandler(handler)
    return logger, str(path)


def fields_to_dict(field_values):
    return {str(field): str(value) for field, value in field_values}


def log_table_event(logger, actor, api, action, db_name, table, key, op="", fields=None, note=""):
    # Disabled: Redis MONITOR plus __VERIFY_MARKER events are the source of truth
    # for ordered verification. File-level event logs are intentionally quiet.
    return
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


def log_hash_snapshot(logger, actor, label, db_name, redis_key, fields):
    # Disabled: script-level Redis checks provide snapshots outside the marked
    # API regions, keeping MONITOR output clean inside before/after markers.
    return
    log_table_event(
        logger,
        actor,
        "DBConnector.hgetall",
        "SNAPSHOT",
        db_name,
        "",
        redis_key,
        fields=fields.items(),
        note=label,
    )


def emit_redis_marker(db, actor, phase, api, db_name, table, key):
    marker_key = "__VERIFY_MARKER:%s:%d" % (actor, time.time_ns())
    payload = {
        "actor": actor,
        "phase": phase,
        "api": api,
        "db": db_name,
        "table": table,
        "key": key,
    }
    db.hset(marker_key, "event", json.dumps(payload, sort_keys=True))
