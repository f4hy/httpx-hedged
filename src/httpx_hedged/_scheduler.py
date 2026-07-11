"""Core async hedge scheduling: per-key state and the race-then-cancel logic.

Adapted from hedge-python's ``HedgeScheduler`` (``transport/_base.py``), with
two changes: state is keyed per *endpoint* rather than only per *host* (see
``_matcher.py``), and the win/lose bookkeeping always records the outcome
(latency + health) even when the winning task raised an exception. In
hedge-python, ``winner_task.result()`` is called before recording, so an
exception there silently skips recording entirely -- this rewrite records
first, then re-raises.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import time
from typing import TYPE_CHECKING, Any, TypeVar, cast
from urllib.parse import urlparse

from httpx_hedged._health import HealthRegistry
from httpx_hedged._rate import RollingRateCounter
from httpx_hedged._stats import Stats, StatsRegistry
from httpx_hedged.budget import TokenBucket
from httpx_hedged.sketch import WindowedSketch

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine

    from httpx_hedged._options import EffectiveConfig

T = TypeVar("T")

_DEFAULT_RPS_SEED = 100.0
_SKETCH_RELATIVE_ACCURACY = 0.01


def extract_host(url: str) -> str:
    """Extract a sanitized host key from a URL.

    Returns ``hostname:port`` (when port is present) or just ``hostname``,
    stripping any userinfo (``user:pass@``) to avoid retaining credentials
    in per-host keys. IPv6 addresses are bracket-wrapped when a port is
    present to keep the key unambiguous (e.g. ``[::1]:8080``).
    """
    parsed = urlparse(url)
    hostname = parsed.hostname or ""
    try:
        port = parsed.port
    except ValueError:
        port = None
    if port is not None:
        if ":" in hostname:
            return f"[{hostname}]:{port}"
        return f"{hostname}:{port}"
    return hostname or url


class _EndpointState:
    """Per-key (per-endpoint or per-host-fallback) hedge state."""

    def __init__(self, config: EffectiveConfig, stats: Stats) -> None:
        self.config = config
        self.sketch = WindowedSketch(
            relative_accuracy=_SKETCH_RELATIVE_ACCURACY,
            window_duration=config.window_duration,
        )
        self.counter = 0
        if config.estimated_rps is not None:
            self.token_bucket = TokenBucket(config.budget_percent, config.estimated_rps)
            self.rate_counter: RollingRateCounter | None = None
        else:
            self.token_bucket = TokenBucket(config.budget_percent, _DEFAULT_RPS_SEED)
            self.rate_counter = RollingRateCounter(config.rps_window_duration)
        self.stats = stats


class HedgeScheduler:
    """Shared async hedge scheduling logic, used by ``HedgedTransport``.

    Manages per-key sketches, warmup counters, rate estimation, token
    bucket budget, and the race-then-cancel logic. Consults the shared
    ``HealthRegistry`` to suppress hedging (never the primary request)
    while a host or endpoint circuit breaker is open.
    """

    def __init__(self, health: HealthRegistry, stats_registry: StatsRegistry) -> None:
        self._health = health
        self._stats_registry = stats_registry
        self._states: dict[str, _EndpointState] = {}

    def state_for(self, key: str, config: EffectiveConfig) -> _EndpointState:
        """Get or create the state for a key. ``config`` is only used on creation."""
        state = self._states.get(key)
        if state is None:
            state = _EndpointState(config, self._stats_registry.for_key(key))
            self._states[key] = state
        return state

    def compute_hedge_delay(self, state: _EndpointState) -> float:
        """Compute the hedge delay in seconds for the current request on this key."""
        config = state.config
        if config.is_hardcoded:
            assert config.hedge_delay is not None
            return max(config.hedge_delay, config.min_delay)

        if state.counter <= config.warmup_requests:
            state.stats.increment_warmup()
            delay = config.warmup_delay
        else:
            estimate = state.sketch.quantile(config.percentile)
            delay = (
                estimate
                if estimate > 0 and not math.isnan(estimate)
                else config.warmup_delay
            )

        return max(delay, config.min_delay)

    async def execute_with_hedge(
        self,
        *,
        key: str,
        host: str,
        config: EffectiveConfig,
        primary_func: Callable[[], Awaitable[T]],
        hedge_func: Callable[[], Awaitable[T]],
        classify: Callable[[T], bool],
        can_hedge: bool,
    ) -> T:
        """Execute the primary request with hedge racing logic.

        Args:
            key: Per-endpoint (or per-host-fallback) key for sketch/budget/health
                tracking.
            host: Host key, used for the host-level circuit breaker tier.
            config: Resolved config for this key.
            primary_func: Async callable that performs the primary request.
            hedge_func: Async callable that performs the hedge request.
            classify: Callable that returns True if a completed result should
                count as a success for circuit-breaker purposes.
            can_hedge: Whether the request is safe to hedge (idempotent).

        Returns:
            The result from whichever request finishes first.
        """
        state = self.state_for(key, config)
        state.stats.increment_total()

        state.counter += 1
        if state.rate_counter is not None:
            state.rate_counter.increment()
            state.token_bucket.set_rps(state.rate_counter.rate_per_second())

        hedge_delay = self.compute_hedge_delay(state)
        start = time.monotonic()

        primary_coro = cast("Coroutine[Any, Any, T]", primary_func())
        primary_task: asyncio.Task[T] = asyncio.create_task(primary_coro)

        done, _ = await asyncio.wait({primary_task}, timeout=hedge_delay)
        if done:
            return await self._finish(state, host, key, primary_task, start, classify)

        if not can_hedge:
            await asyncio.wait({primary_task})
            return await self._finish(state, host, key, primary_task, start, classify)

        breaker_cfg = state.config.circuit_breaker
        if not self._health.hedging_allowed(host, key, breaker_cfg, breaker_cfg):
            state.stats.increment_circuit_blocked()
            await asyncio.wait({primary_task})
            return await self._finish(state, host, key, primary_task, start, classify)

        if not state.token_bucket.try_acquire():
            state.stats.increment_budget_exhausted()
            await asyncio.wait({primary_task})
            return await self._finish(state, host, key, primary_task, start, classify)

        state.stats.increment_hedged()
        hedge_coro = cast("Coroutine[Any, Any, T]", hedge_func())
        hedge_task: asyncio.Task[T] = asyncio.create_task(hedge_coro)

        done, pending = await asyncio.wait(
            {primary_task, hedge_task}, return_when=asyncio.FIRST_COMPLETED
        )
        winner_task = done.pop()

        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

        if winner_task is primary_task:
            state.stats.increment_primary_wins()
        else:
            state.stats.increment_hedge_wins()

        return await self._finish(
            state, host, key, cast("asyncio.Task[T]", winner_task), start, classify
        )

    async def _finish(
        self,
        state: _EndpointState,
        host: str,
        key: str,
        winner_task: asyncio.Task[T],
        start: float,
        classify: Callable[[T], bool],
    ) -> T:
        """Record latency and health outcome, then return the result or re-raise.

        Recording always happens before the result is returned or the
        exception is re-raised -- unlike hedge-python, where an exception
        from ``winner_task.result()`` silently skips recording.
        """
        elapsed = time.monotonic() - start
        breaker_cfg = state.config.circuit_breaker
        try:
            result = winner_task.result()
        except Exception:
            state.sketch.add(elapsed)
            self._health.record_result(host, key, breaker_cfg, breaker_cfg, False)
            state.stats.increment_errors()
            raise

        state.sketch.add(elapsed)
        ok = classify(result)
        self._health.record_result(host, key, breaker_cfg, breaker_cfg, ok)
        if not ok:
            state.stats.increment_errors()
        return result

    def close(self) -> None:
        """No-op: windowed structures rotate lazily, so there's nothing to stop.

        Kept for API symmetry with the rest of the transport's lifecycle.
        """
