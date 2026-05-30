"""Shared Redis marker logging for DB table examples."""

from __future__ import annotations

import json
import time
from contextlib import contextmanager


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


@contextmanager
def marked_redis_operation(db, actor, api, db_name, table, key):
    emit_redis_marker(db, actor, "before", api, db_name, table, key)
    yield
    emit_redis_marker(db, actor, "after", api, db_name, table, key)
