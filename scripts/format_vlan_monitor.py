#!/usr/bin/env python3
"""Format Redis MONITOR output around VLAN verification markers."""

from __future__ import annotations

import json
import re
import shlex
import sys


LINE_RE = re.compile(r"^([0-9]+\.[0-9]+) \[(\d+|lua)(?: [^\]]+)?\] (.*)$")


NOISE_COMMANDS = {
    "EXEC",
    "LLEN",
    "MULTI",
    "PUBLISH",
    "SCARD",
    "SCRIPT",
    "SELECT",
    "SUBSCRIBE",
    "WATCH",
}


def parse_line(line):
    match = LINE_RE.match(line.strip())
    if not match:
        return None

    timestamp, db, command_text = match.groups()
    try:
        parts = shlex.split(command_text)
    except ValueError:
        return None
    if not parts:
        return None

    return {
        "timestamp": timestamp,
        "db": db,
        "command": parts[0],
        "args": parts[1:],
    }


def marker_from(entry):
    if entry["command"] != "HSET":
        return None
    args = entry["args"]
    if len(args) < 3 or not args[0].startswith("__VERIFY_MARKER:") or args[1] != "event":
        return None
    try:
        payload = json.loads(args[2])
    except json.JSONDecodeError:
        return None
    payload["timestamp"] = entry["timestamp"]
    payload["redis_db"] = entry["db"]
    return payload


def compact(entry):
    command = entry["command"]
    args = entry["args"]
    db = entry["db"]
    timestamp = entry["timestamp"]

    if command == "EVALSHA":
        keys = args[2:]
        return "%s [db%s] EVALSHA keys=%s" % (timestamp, db, " ".join(keys[:4]))
    if command in {"HSET", "HGETALL", "SADD", "SPOP", "SREM", "SMEMBERS", "DEL", "LPUSH", "LRANGE", "LTRIM"}:
        return "%s [db%s] %s %s" % (timestamp, db, command, " ".join(args))
    return "%s [db%s] %s %s" % (timestamp, db, command, " ".join(args))


def main():
    if len(sys.argv) != 2:
        print("usage: format_vlan_monitor.py <monitor-log>", file=sys.stderr)
        return 2

    active = None
    operations = []
    sections = []

    with open(sys.argv[1], encoding="utf-8") as fp:
        for raw_line in fp:
            entry = parse_line(raw_line)
            if not entry:
                continue

            marker = marker_from(entry)
            if marker:
                if marker.get("phase") == "before":
                    active = marker
                    operations = []
                elif marker.get("phase") == "after" and active:
                    sections.append((active, marker, operations))
                    active = None
                    operations = []
                continue

            if not active:
                continue
            if entry["command"] in NOISE_COMMANDS:
                continue
            operations.append(entry)

    for before, after, ops in sections:
        print(
            "## %s %s %s:%s"
            % (before["actor"], before["api"], before["db"], before["table"])
        )
        print("before=%s after=%s key=%s" % (before["timestamp"], after["timestamp"], before["key"]))
        if not ops:
            print("  (no Redis operations captured inside marker)")
        for entry in ops:
            print("  %s" % compact(entry))
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
