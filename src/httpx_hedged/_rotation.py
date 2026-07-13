"""Shared lazy, pull-based window-rotation decision logic.

Every windowed structure in this package (``WindowedSketch``,
``RollingRateCounter``, the circuit breaker's ``_ErrorWindow``) holds a
current/previous pair of accumulators and decides whether to rotate or reset
them lazily, on the next call that touches the structure, rather than via a
background thread or asyncio task. This avoids spinning one rotation task per
tracked key, which matters once state is tracked per-endpoint rather than
just per-host, since a service can have dozens of endpoints.
"""

from __future__ import annotations

from enum import Enum, auto


class RotateAction(Enum):
    """What a windowed structure should do given elapsed time since rotation."""

    NONE = auto()
    ROTATE = auto()
    RESET = auto()


def next_action(
    window_start: float, window_duration: float, now: float
) -> RotateAction:
    """Decide whether a window should rotate or reset given elapsed time.

    - Less than one ``window_duration`` has elapsed: do nothing.
    - Between one and two window durations: rotate (current becomes
      previous, a fresh current begins).
    - Two or more window durations have elapsed: the structure has been idle
      long enough that "previous" data is stale too, so reset entirely
      rather than rotate.
    """
    elapsed = now - window_start
    if elapsed < window_duration:
        return RotateAction.NONE
    if elapsed < 2 * window_duration:
        return RotateAction.ROTATE
    return RotateAction.RESET
