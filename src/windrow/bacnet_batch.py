"""
Per-device ReadPropertyMultiple batching (Baler#127).

Polling stays one asyncio task per point (connection_manager._poll_point:
each point keeps its own interval, back-off and hisCollect policy), but a
point no longer sends its own ReadProperty. It asks its device's
ReadBatcher, which collects every read asked of that device within a short
window - across all connections on the device, since one controller often
backs many equipment connections (the mock hospital puts ~80 on one
device) - and sends one ReadPropertyMultiple per `max_points` objects.
connection_manager aligns each device's poll ticks so a tick's reads do
arrive together.

Falling back, in order of how sure we are the device can't do better:
- Reject unrecognized-service, or a services-class Error: the device doesn't
  do RPM. Every later read to it is a single ReadProperty.
- Abort buffer-overflow / segmentation-not-supported / apdu-too-long: the
  answer didn't fit. Halve the batch size for the next tick; read this
  batch singly so nothing is lost now.
- A timeout: the device isn't answering. Fail the whole batch and every
  batch after it in this flush - fanning out to N single reads that each
  wait out the timeout is what Baler#47's slot cap exists to stop.
- Anything else (a mismatched or undecodable answer): read this batch
  singly; the single read has its own checks.

A read asked with status=True (Windrow#20: the point's connection publishes
reading quality) also asks the object's status-flags and reliability in the
same RPM, and resolves to a bacnet_read.Sample. A single ReadProperty
fallback reads present-value only - tripling the requests to a device that
can't batch isn't worth it - so it resolves to the bare value, as before.
"""

from __future__ import annotations

import asyncio
import os
from collections import Counter
from collections.abc import Awaitable, Callable
from typing import Any

from windrow.bacnet_read import RPMUnsupported

# Seconds a batcher waits for more reads before sending. Points on one
# device tick together (same loop iteration), so this only has to cover
# scheduling jitter.
COALESCE_WINDOW = float(os.environ.get("WINDROW_RPM_WINDOW", "0.02"))
# Hard cap on objects per ReadPropertyMultiple, whatever the device's APDU
# size suggests.
MAX_RPM_POINTS = int(os.environ.get("WINDROW_RPM_MAX_POINTS", "50"))
# Bytes one present-value result takes in an RPM answer, rounded up: object
# id (5) + property id (2) + an opening/closing tag pair per level (4) + the
# value (up to 5 for a Real) + slack for a Null/enumeration/status choice.
_BYTES_PER_RESULT = 20
# What status-flags (property id + tags + a 4-bit bitstring, ~7) and
# reliability (an enumeration ~6, or an error ~8) add per object, rounded up.
_STATUS_BYTES = 16
_APDU_OVERHEAD = 8


def points_per_request(max_apdu: int | None, cap: int = MAX_RPM_POINTS, status: bool = False) -> int:
    """How many present-values fit one unsegmented answer from a device
    that accepts `max_apdu` bytes (from its I-Am; MS/TP devices are often
    480, BACnet/IP 1476). Unknown: just the cap. `status`: each object's
    status-flags and reliability come back too."""
    if not max_apdu:
        return cap
    per = _BYTES_PER_RESULT + (_STATUS_BYTES if status else 0)
    return max(1, min(cap, (int(max_apdu) - _APDU_OVERHEAD) // per))


ReadOne = Callable[[Any, str, str], Awaitable[Any]]
# read_many(app, address, objids) or, when some reads want quality,
# read_many(app, address, objids, status=[objids among them])
ReadMany = Callable[..., Awaitable[dict[str, Any]]]


class ReadBatcher:
    def __init__(
        self,
        app,
        address: str,
        slots: Callable[[], asyncio.Semaphore],
        read_one: ReadOne,
        read_many: ReadMany,
        stats: Counter,
        max_points: int = MAX_RPM_POINTS,
        window: float | None = None,
    ):
        self.app = app
        self.address = address
        self._slots = slots
        self._read_one = read_one
        self._read_many = read_many
        self.stats = stats
        self.max_points = max(1, max_points)
        self.window = COALESCE_WINDOW if window is None else window
        self.rpm_supported = True
        self._pending: list[tuple[str, asyncio.Future, bool]] = []
        self._timer: asyncio.TimerHandle | None = None
        self._running: set[asyncio.Task] = set()
        self._lock: asyncio.Lock | None = None

    async def read(self, objid: str, status: bool = False) -> Any:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending.append((objid, future, status))
        if self._timer is None:
            self._timer = loop.call_later(self.window, self._flush)
        return await future

    def _flush(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        batch, self._pending = self._pending, []
        if not batch:
            return
        task = asyncio.get_running_loop().create_task(self._run(batch))
        self._running.add(task)
        task.add_done_callback(self._running.discard)

    async def _run(self, batch: list[tuple[str, asyncio.Future, bool]]) -> None:
        # One flush at a time per device, its chunks one after another: a
        # controller answers one RPM at a time anyway, and a dead one then
        # costs one timeout per flush, not one per chunk.
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            await self._run_locked(batch)

    async def _run_locked(self, batch: list[tuple[str, asyncio.Future, bool]]) -> None:
        # Two connections can poll the same object; ask once, answer both.
        waiters: dict[str, list[asyncio.Future]] = {}
        want_status: set[str] = set()
        for objid, future, status in batch:
            if not future.done():  # its point task was cancelled meanwhile
                waiters.setdefault(objid, []).append(future)
                if status:
                    want_status.add(objid)
        objids = list(waiters)
        try:
            if not self.rpm_supported or len(objids) == 1:
                await self._singles(objids, waiters)
                return
            for start in range(0, len(objids), self.max_points):
                chunk = objids[start:start + self.max_points]
                if len(chunk) == 1:
                    await self._singles(chunk, waiters)
                    continue
                try:
                    async with self._slots():
                        self.stats["read_property_multiple"] += 1
                        status = [objid for objid in chunk if objid in want_status]
                        if status:
                            results = await self._read_many(self.app, self.address, chunk, status=status)
                        else:
                            results = await self._read_many(self.app, self.address, chunk)
                except RPMUnsupported as exc:
                    if exc.too_big and self.max_points > 1:
                        self.max_points = max(1, len(chunk) // 2)
                        print(f"{self.address}: {exc}; batching {self.max_points} objects per request from now on")
                    else:
                        self.rpm_supported = False
                        print(f"{self.address}: {exc}; using single ReadProperty for this device")
                    await self._singles(chunk, waiters)
                    continue
                except (TimeoutError, asyncio.TimeoutError) as exc:
                    # Not answering: fail this chunk and the rest without asking.
                    for objid in objids[start:]:
                        _fail(waiters[objid], exc)
                    return
                except Exception as exc:  # noqa: BLE001 - re-asked one at a time, with the single read's own checks
                    print(f"{self.address}: ReadPropertyMultiple failed ({exc!r}); reading {len(chunk)} objects singly")
                    await self._singles(chunk, waiters)
                    continue
                for objid in chunk:
                    if objid in results:
                        _resolve(waiters[objid], results[objid])
                    else:
                        _fail(waiters[objid], LookupError(f"{self.address} left {objid} out of its answer"))
        finally:
            # nothing waits forever, whatever went wrong above
            for futures in waiters.values():
                _fail(futures, RuntimeError("batch ended without an answer"))

    async def _singles(self, objids: list[str], waiters: dict[str, list[asyncio.Future]]) -> None:
        async def one(objid: str) -> None:
            try:
                async with self._slots():
                    self.stats["read_property"] += 1
                    value = await self._read_one(self.app, self.address, objid)
            except Exception as exc:  # noqa: BLE001 - handed to the point task, which counts the failure
                _fail(waiters[objid], exc)
            else:
                _resolve(waiters[objid], value)

        await asyncio.gather(*(one(objid) for objid in objids))


def _resolve(futures: list[asyncio.Future], value: Any) -> None:
    for future in futures:
        if not future.done():
            future.set_result(value)


def _fail(futures: list[asyncio.Future], exc: BaseException) -> None:
    for future in futures:
        if not future.done():
            future.set_exception(exc)
