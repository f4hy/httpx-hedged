"""WindowedSketch: sliding-window quantile estimation over DDSketch pairs."""

from __future__ import annotations

import math
import threading
import time

from httpx_hedged._rotation import RotateAction, next_action
from httpx_hedged.sketch._ddsketch import DDSketch

_DEFAULT_WINDOW_DURATION = 30.0  # seconds


class WindowedSketch:
    """Maintains a sliding window over two DDSketches that rotate lazily.

    Quantile queries merge both sketches, giving a window that spans 1x to
    2x the configured duration. ``add`` always writes to the current
    sketch. Rotation is decided lazily on each call rather than by a
    background thread/task -- see ``httpx_hedged._rotation`` -- since a
    service can have many independently-tracked endpoints, and spinning one
    rotation task per endpoint does not scale.

    The rotation scheme::

        t=0:  current=A, previous=empty
        t=30: current=B, previous=A       (A covers [0,30))
        t=60: current=C, previous=B       (A is dropped)
        t=90+ (idle):                      hard reset, both sketches emptied

    This class is thread-safe.

    Args:
        relative_accuracy: DDSketch relative accuracy (default: 0.01).
        window_duration: Rotation interval in seconds (default: 30.0).
    """

    def __init__(
        self,
        relative_accuracy: float = 0.01,
        window_duration: float = _DEFAULT_WINDOW_DURATION,
    ) -> None:
        if window_duration <= 0:
            window_duration = _DEFAULT_WINDOW_DURATION
        self._relative_accuracy = relative_accuracy
        self._window_duration = window_duration
        self._lock = threading.Lock()
        self._current = DDSketch(relative_accuracy)
        self._previous = DDSketch(relative_accuracy)
        self._window_start = time.monotonic()

    def _maybe_rotate_locked(self) -> None:
        """Rotate or reset if enough time has passed. Caller must hold the lock."""
        now = time.monotonic()
        action = next_action(self._window_start, self._window_duration, now)
        if action is RotateAction.NONE:
            return
        if action is RotateAction.ROTATE:
            self._previous = self._current
            self._current = DDSketch(self._relative_accuracy)
        else:  # RESET
            self._previous = DDSketch(self._relative_accuracy)
            self._current = DDSketch(self._relative_accuracy)
        self._window_start = now

    def add(self, value: float) -> None:
        """Record a latency sample (in seconds) to the current sketch."""
        with self._lock:
            self._maybe_rotate_locked()
            self._current.add(value)

    def quantile(self, q: float) -> float:
        """Return the estimated quantile q in [0, 1] over the sliding window.

        Returns math.nan if no data has been recorded.
        """
        with self._lock:
            self._maybe_rotate_locked()
            if self._current.count == 0 and self._previous.count == 0:
                return math.nan
            merged = DDSketch(self._relative_accuracy)
            merged.merge(self._previous)
            merged.merge(self._current)
            return merged.quantile(q)

    def rotate(self) -> None:
        """Force an immediate rotation. Mostly useful for testing."""
        with self._lock:
            self._previous = self._current
            self._current = DDSketch(self._relative_accuracy)
            self._window_start = time.monotonic()
