"""Rolling request-rate estimation, used to auto-size the hedge token bucket.

hedge-python requires callers to hand-tune a single ``estimated_rps`` value
per host. That is workable for one host, but this library tracks state per
*endpoint*, and a service with many endpoints of very different traffic
volumes would require hand-tuning a guess for each one -- exactly the kind
of per-endpoint configuration burden this rewrite is trying to avoid. By
default we estimate the rate automatically instead.
"""

from __future__ import annotations

import threading
import time

from httpx_hedged._rotation import RotateAction, next_action

_DEFAULT_WINDOW_DURATION = (
    10.0  # seconds; shorter than the latency sketch window since RPS shifts faster
)


class RollingRateCounter:
    """Estimates requests-per-second over a rolling window.

    Uses the same lazy current/previous rotation scheme as
    ``WindowedSketch``, but counts requests rather than latencies.
    """

    def __init__(self, window_duration: float = _DEFAULT_WINDOW_DURATION) -> None:
        if window_duration <= 0:
            window_duration = _DEFAULT_WINDOW_DURATION
        self._window_duration = window_duration
        self._lock = threading.Lock()
        self._current = 0
        self._previous = 0
        self._window_start = time.monotonic()

    def _maybe_rotate_locked(self) -> None:
        now = time.monotonic()
        action = next_action(self._window_start, self._window_duration, now)
        if action is RotateAction.NONE:
            return
        if action is RotateAction.ROTATE:
            self._previous = self._current
            self._current = 0
        else:  # RESET
            self._previous = 0
            self._current = 0
        self._window_start = now

    def increment(self) -> None:
        """Record one request."""
        with self._lock:
            self._maybe_rotate_locked()
            self._current += 1

    def rate_per_second(self) -> float:
        """Return the estimated requests-per-second, as a sliding-window average.

        Weights the previous window's count by the fraction of it that
        still falls within the trailing ``window_duration``-sized lookback
        from now, rather than always dividing by a fixed ``2 *
        window_duration`` -- that fixed divisor systematically
        underestimates by up to 2x right after a rotation, when the
        current window has accumulated almost nothing yet but the full
        previous window's count is still discounted as if it were only
        half-relevant.
        """
        with self._lock:
            self._maybe_rotate_locked()
            elapsed = time.monotonic() - self._window_start
            weight = max(0.0, 1.0 - elapsed / self._window_duration)
            weighted = self._previous * weight + self._current
            return weighted / self._window_duration
