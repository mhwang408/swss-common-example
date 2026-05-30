"""Redis marker logging for verifying table API behavior.

Emits ``__VERIFY_MARKER`` keys before and after each table operation so that
Redis MONITOR output can be grouped and filtered by the verification scripts.
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from typing import Any, Generator


def emit_redis_marker(
    db: Any,
    actor: str,
    phase: str,
    api: str,
    db_name: str,
    table: str,
    key: str,
) -> None:
    """Write a verification marker into Redis.

    Args:
        db: A ``DBConnector`` instance to write the marker into.
        actor: Component name (e.g. ``"vlanmgrd"``).
        phase: Either ``"before"`` or ``"after"``.
        api: The table API being called (e.g. ``"Table.set"``).
        db_name: Logical DB name (e.g. ``"CONFIG_DB"``).
        table: Table name.
        key: Entry key.
    """
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
def marked_redis_operation(
    db: Any,
    actor: str,
    api: str,
    db_name: str,
    table: str,
    key: str,
) -> Generator[None, None, None]:
    """Context manager that emits before/after markers around a table operation.

    Args:
        db: A ``DBConnector`` instance.
        actor: Component name.
        api: The table API being called.
        db_name: Logical DB name.
        table: Table name.
        key: Entry key.

    Yields:
        Nothing. The caller performs the table operation inside the block.
    """
    emit_redis_marker(db, actor, "before", api, db_name, table, key)
    yield
    emit_redis_marker(db, actor, "after", api, db_name, table, key)
