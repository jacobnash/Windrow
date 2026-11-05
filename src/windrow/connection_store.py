"""
Connection persistence - the one genuinely new piece Windrow needed for this
feature (no config file or DB existed anywhere in this project before).
A JSON file, not sqlite3: every write already funnels through a single
asyncio loop thread (see connection_manager.py), so there's no
concurrent-writer problem for sqlite3's transactions to solve, and the
payload (a device address, a poll interval, and a list of point dicts) has
no schema advantage over "a list of dicts, in a file."

Known, accepted gap: no file locking - single process per state file is an
assumption, not enforced. Fine per YAGNI unless multi-process is actually
needed.
"""

import json
import os
import uuid


def load(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def save(path: str, connections: list[dict]) -> None:
    """Atomic write: temp file + os.replace, both stdlib and POSIX-atomic -
    a crash mid-write never leaves a half-written connections.json."""
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(connections, f, indent=2)
    os.replace(tmp_path, path)


def new_connection_id() -> str:
    return uuid.uuid4().hex
