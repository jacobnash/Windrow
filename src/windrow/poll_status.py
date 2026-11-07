"""
Per-connection poll health, kept in memory beside each manager's records
(Baler#110): when it last polled, last read anything, last published a
value, and what the last failure said. It answers "is this connector
actually working?" without anyone opening the equipment page.

Never persisted and never written into a connection record - a record is
what the operator configured, this is what happened since the process
started. Both managers reset a connection's entry when it's updated or
deleted, so an error from the old address doesn't outlive the fix.
"""

import time

# How many failing point labels a snapshot names; the count is always exact.
MAX_FAILING_LISTED = 20
# Errors are device/library text; keep one from bloating a list response.
MAX_ERROR_LEN = 300


class PollStatus:
    def __init__(self, now=time.time):
        self._now = now
        self._by_id: dict[str, dict] = {}

    def _entry(self, connection_id: str) -> dict:
        entry = self._by_id.get(connection_id)
        if entry is None:
            entry = {
                "last_poll_at": None, "last_success_at": None, "last_value_at": None,
                "last_error": None, "last_error_at": None, "failing": set(),
            }
            self._by_id[connection_id] = entry
        return entry

    def reset(self, connection_id: str) -> None:
        self._by_id.pop(connection_id, None)

    def success(self, connection_id: str, label: str, published: bool) -> None:
        now = self._now()
        entry = self._entry(connection_id)
        entry["last_poll_at"] = entry["last_success_at"] = now
        if published:
            entry["last_value_at"] = now
        entry["failing"].discard(label)

    def failure(self, connection_id: str, labels, error) -> None:
        """One failed read (a label) or a failed connect (every label)."""
        now = self._now()
        entry = self._entry(connection_id)
        entry["last_poll_at"] = entry["last_error_at"] = now
        entry["last_error"] = str(error)[:MAX_ERROR_LEN]
        entry["failing"].update([labels] if isinstance(labels, str) else labels)

    def snapshot(self, connection_id: str, point_count: int) -> dict:
        """JSON-safe; times are unix seconds. `state` is waiting (nothing
        polled yet), ok, partial (some points failing) or error (all)."""
        entry = self._by_id.get(connection_id) or self._entry(connection_id)
        failing = sorted(entry["failing"])
        if entry["last_poll_at"] is None:
            state = "waiting"
        elif not failing:
            state = "ok"
        elif point_count and len(failing) >= point_count:
            state = "error"
        else:
            state = "partial"
        return {
            "state": state,
            "last_poll_at": entry["last_poll_at"],
            "last_success_at": entry["last_success_at"],
            "last_value_at": entry["last_value_at"],
            "last_error": entry["last_error"],
            "last_error_at": entry["last_error_at"],
            "failing_count": len(failing),
            "failing_points": failing[:MAX_FAILING_LISTED],
        }
