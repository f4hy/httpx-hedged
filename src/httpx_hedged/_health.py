"""Health-based circuit breaker that suppresses hedging during outages.

hedge-python has no concept of request success/failure at all -- its token
bucket caps hedge *volume* but has no idea whether the backend is actually
healthy. This module adds a circuit breaker, tracked independently at both
the host level and the per-endpoint level, so that either "one endpoint is
struggling" or "the whole host is struggling" stops hedging without
requiring the two to be conflated.

Tripping the breaker only ever suppresses the *hedge* request. The primary
request always goes through and its result or exception is always returned
to the caller -- this is deliberately not a request-blocking circuit
breaker, only a hedge-suppressing one, so hedging can't pile extra load
onto an already-failing backend.

Known limitation: health is recorded from the winning task's outcome only.
A cancelled loser's real outcome is unknowable, and losers are cancelled
deliberately -- not doing so would defeat the breaker's purpose of reducing
load on a struggling backend.
"""

from __future__ import annotations

import threading
import time
from enum import Enum, auto
from typing import TYPE_CHECKING

from httpx_hedged._config import CircuitBreakerConfig
from httpx_hedged._rotation import RotateAction, next_action

if TYPE_CHECKING:
    from collections.abc import Callable


class CircuitState(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class _ErrorWindow:
    """Lazy dual-window success/failure counter, same rotation scheme as sketches."""

    def __init__(self, window_duration: float) -> None:
        self._window_duration = window_duration
        self._current_total = 0
        self._current_failures = 0
        self._previous_total = 0
        self._previous_failures = 0
        self._window_start = time.monotonic()

    def _maybe_rotate(self) -> None:
        now = time.monotonic()
        action = next_action(self._window_start, self._window_duration, now)
        if action is RotateAction.NONE:
            return
        if action is RotateAction.ROTATE:
            self._previous_total = self._current_total
            self._previous_failures = self._current_failures
        else:  # RESET
            self._previous_total = 0
            self._previous_failures = 0
        self._current_total = 0
        self._current_failures = 0
        self._window_start = now

    def record(self, ok: bool) -> None:
        self._maybe_rotate()
        self._current_total += 1
        if not ok:
            self._current_failures += 1

    def sample_count(self) -> int:
        self._maybe_rotate()
        return self._current_total + self._previous_total

    def error_rate(self) -> float:
        self._maybe_rotate()
        total = self._current_total + self._previous_total
        if total == 0:
            return 0.0
        failures = self._current_failures + self._previous_failures
        return failures / total

    def reset(self) -> None:
        self._current_total = 0
        self._current_failures = 0
        self._previous_total = 0
        self._previous_failures = 0
        self._window_start = time.monotonic()


class CircuitBreaker:
    """A closed/open/half-open circuit breaker gating whether hedging is allowed.

    Not itself thread-safe across concurrent mutation from multiple
    threads; callers (``HealthRegistry``) hold their own lock.

    Args:
        config: Breaker thresholds and timing.
        on_open: Called (with no arguments) each time the breaker
            transitions into the OPEN state, whether from CLOSED or from a
            failed HALF_OPEN trial. Useful for alerting -- see the README's
            observability section for a logging example.
    """

    def __init__(
        self,
        config: CircuitBreakerConfig,
        on_open: Callable[[], None] | None = None,
    ) -> None:
        self._config = config
        self._on_open = on_open
        self._window = _ErrorWindow(config.window_duration)
        self._state = CircuitState.CLOSED
        self._opened_at: float = 0.0
        self._half_open_trials = 0
        self._half_open_failures = 0

    @property
    def state(self) -> CircuitState:
        return self._state

    def record_result(self, ok: bool) -> None:
        if self._state is CircuitState.HALF_OPEN:
            self._half_open_trials += 1
            if not ok:
                self._half_open_failures += 1
            if self._half_open_trials >= self._config.half_open_max_trial:
                if (
                    self._half_open_failures / self._half_open_trials
                    >= self._config.error_rate_threshold
                ):
                    self._reopen()
                else:
                    self._close()
            return

        # CLOSED (or OPEN, where a result can still arrive from a primary
        # request even though hedging is suppressed -- keep tracking it).
        self._window.record(ok)
        if (
            self._state is CircuitState.CLOSED
            and self._window.sample_count() >= self._config.min_samples
            and self._window.error_rate() >= self._config.error_rate_threshold
        ):
            self._open()

    def allow_hedge(self) -> bool:
        if self._state is CircuitState.CLOSED:
            return True
        if self._state is CircuitState.OPEN:
            if time.monotonic() - self._opened_at >= self._config.cooldown:
                self._enter_half_open()
                return True
            return False
        # HALF_OPEN
        return self._half_open_trials < self._config.half_open_max_trial

    def _open(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        if self._on_open is not None:
            self._on_open()

    def _reopen(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        self._half_open_trials = 0
        self._half_open_failures = 0
        if self._on_open is not None:
            self._on_open()

    def _enter_half_open(self) -> None:
        self._state = CircuitState.HALF_OPEN
        self._half_open_trials = 0
        self._half_open_failures = 0

    def _close(self) -> None:
        self._state = CircuitState.CLOSED
        self._half_open_trials = 0
        self._half_open_failures = 0
        self._window.reset()


class HealthRegistry:
    """Owns one ``CircuitBreaker`` per host and one per endpoint key.

    Both tiers are consulted independently: a host-level trip disables
    hedging for every endpoint on that host, while an endpoint-level trip
    disables hedging only for that endpoint, leaving sibling endpoints on
    the same host unaffected.

    Args:
        on_circuit_open: Called each time a breaker (host- or
            endpoint-scoped) transitions into the OPEN state, as
            ``on_circuit_open(scope, key)`` where ``scope`` is ``"host"``
            or ``"endpoint"`` and ``key`` is the host name or endpoint key
            that tripped. Intended for alerting -- see the README's
            observability section for a logging example.
    """

    def __init__(
        self, on_circuit_open: Callable[[str, str], None] | None = None
    ) -> None:
        self._lock = threading.Lock()
        self._on_circuit_open = on_circuit_open
        self._host_breakers: dict[str, CircuitBreaker] = {}
        self._endpoint_breakers: dict[str, CircuitBreaker] = {}

    def breaker_for_host(
        self, host: str, config: CircuitBreakerConfig
    ) -> CircuitBreaker:
        with self._lock:
            breaker = self._host_breakers.get(host)
            if breaker is None:
                breaker = CircuitBreaker(
                    config, on_open=self._on_open_callback("host", host)
                )
                self._host_breakers[host] = breaker
            return breaker

    def breaker_for_endpoint(
        self, key: str, config: CircuitBreakerConfig
    ) -> CircuitBreaker:
        with self._lock:
            breaker = self._endpoint_breakers.get(key)
            if breaker is None:
                breaker = CircuitBreaker(
                    config, on_open=self._on_open_callback("endpoint", key)
                )
                self._endpoint_breakers[key] = breaker
            return breaker

    def _on_open_callback(self, scope: str, key: str) -> Callable[[], None] | None:
        if self._on_circuit_open is None:
            return None
        callback = self._on_circuit_open
        return lambda: callback(scope, key)

    def record_result(
        self,
        host: str,
        key: str,
        host_config: CircuitBreakerConfig,
        key_config: CircuitBreakerConfig,
        ok: bool,
    ) -> None:
        self.breaker_for_host(host, host_config).record_result(ok)
        self.breaker_for_endpoint(key, key_config).record_result(ok)

    def hedging_allowed(
        self,
        host: str,
        key: str,
        host_config: CircuitBreakerConfig,
        key_config: CircuitBreakerConfig,
    ) -> bool:
        return (
            self.breaker_for_host(host, host_config).allow_hedge()
            and self.breaker_for_endpoint(key, key_config).allow_hedge()
        )

    def host_state(self, host: str) -> CircuitState | None:
        with self._lock:
            breaker = self._host_breakers.get(host)
        return breaker.state if breaker else None

    def endpoint_state(self, key: str) -> CircuitState | None:
        with self._lock:
            breaker = self._endpoint_breakers.get(key)
        return breaker.state if breaker else None
