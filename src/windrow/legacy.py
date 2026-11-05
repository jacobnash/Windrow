"""
Backward compatibility with installs made before the FBF -> Windrow rename.

A site upgraded in place keeps working without touching its config:

  - env(): every WINDROW_* variable falls back to its old FBF_* name.
  - adopt_state_file(): a default state file still under its old fbf-*
    name (provisioned connections, device registry, the unsent-readings
    spool) is moved to its windrow-* name on first start, instead of the
    site silently starting over with an empty one.

MQTT topics keep their `fbf/` prefix on purpose: every point's identity in
Baler (`urn:point:fbf/...`) and its history are keyed on it.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

OLD_PREFIX = "FBF_"
NEW_PREFIX = "WINDROW_"


def env(name: str, default: str | None = None) -> str | None:
    """`env("WINDROW_SITE_ID")` -> WINDROW_SITE_ID, else FBF_SITE_ID, else default."""
    value = os.environ.get(name)
    if value:
        return value
    if name.startswith(NEW_PREFIX):
        old = os.environ.get(OLD_PREFIX + name[len(NEW_PREFIX):])
        if old:
            return old
    return default


def adopt_state_file(path: str) -> str:
    """If `path` is a windrow-* file that doesn't exist yet but its fbf-*
    predecessor does (same directory), rename the old one into place.
    Returns `path` either way. Never overwrites an existing file."""
    directory, base = os.path.split(path)
    if not base.startswith("windrow-") or os.path.exists(path):
        return path
    old = os.path.join(directory, "fbf-" + base[len("windrow-"):])
    if os.path.exists(old):
        os.replace(old, path)
        log.warning("adopted pre-rename state file %s -> %s", old, path)
    return path
