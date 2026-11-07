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


# Field checks shared by both connection managers' update_connection: a
# PATCH (or a command-channel update) lands straight in the state file and
# the poll loop, so a wrong type has to be refused here, as ValueError
# (api.py turns that into a 400 / an {"ok": false} result), rather than
# crash a poll task later where nobody sees it.


def check_number(name: str, value, minimum: float = 0.0) -> float:
    """A number above `minimum` - bool is an int subclass, so refused explicitly."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= minimum:
        raise ValueError(f"{name} must be a number greater than {minimum:g}, got {value!r}")
    return value


def check_int(name: str, value, low: int, high: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValueError(f"{name} must be an integer {low}-{high}, got {value!r}")
    return value


def check_text(name: str, value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string, got {value!r}")
    return value


def check_topic_prefix(value) -> str:
    """MQTT wildcards in a publish topic make the broker drop the
    connection; a trailing slash would make every topic `a//label`."""
    value = check_text("topic_prefix", value if not isinstance(value, str) else value.rstrip("/"))
    if "+" in value or "#" in value:
        raise ValueError(f"topic_prefix can't contain MQTT wildcards (+ or #), got {value!r}")
    return value


def check_patch(patch, allowed: set[str]) -> dict:
    """An unknown key is refused instead of ignored: a typo'd field
    (`poll_intervall`) silently doing nothing is worse than a 400."""
    if not isinstance(patch, dict):
        raise ValueError(f"patch must be a JSON object, got {type(patch).__name__}")  # noqa: TRY004 - ValueError means 400 / ok:false
    unknown = sorted(set(patch) - allowed)
    if unknown:
        raise ValueError(f"can't update {', '.join(unknown)}; allowed: {', '.join(sorted(allowed))}")
    if not patch:
        raise ValueError(f"nothing to update; allowed: {', '.join(sorted(allowed))}")
    return patch
