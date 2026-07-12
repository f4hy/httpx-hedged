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
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, TypeVar, cast
from urllib.parse import urlparse

from httpx_hedged._bounded import BoundedRegistry
from httpx_hedged._config import CircuitBreakerConfig, EffectiveConfig
from httpx_hedged._health import HealthRegistry
from httpx_hedged._rate import RollingRateCounter
from httpx_hedged._stats import Stats, StatsRegistry
from httpx_hedged.budget import TokenBucket
from httpx_hedged.sketch import WindowedSketch

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

    Args:
        health: Shared circuit-breaker registry.
        stats_registry: Shared per-key statistics registry.
        host_circuit_breaker: Circuit-breaker configuration used for the
            *host* tier, independent of whichever endpoint's config happens
            to be resolved for a given request -- a host isn't owned by any
            one endpoint, so its breaker thresholds must not depend on
            request arrival order (see ``_should_hedge``/``_finish``, which
            always pass this rather than the per-request resolved config
            for the host side of ``HealthRegistry`` calls).
        on_hedge_fired: Called with the key each time a hedge request is
            actually launched (after all gates -- idempotency, circuit
            breaker, budget -- have passed). Intended for metrics -- see
            the README's observability section for an example.
    """

    def __init__(
        self,
        health: HealthRegistry,
        stats_registry: StatsRegistry,
        host_circuit_breaker: CircuitBreakerConfig | None = None,
        on_hedge_fired: Callable[[str], None] | None = None,
    ) -> None:
        self._health = health
        self._stats_registry = stats_registry
        self._host_circuit_breaker = host_circuit_breaker or CircuitBreakerConfig()
        self._on_hedge_fired = on_hedge_fired
        self._states: BoundedRegistry[_EndpointState] = BoundedRegistry()

    def state_for(self, key: str, config: EffectiveConfig) -> _EndpointState:
        """Get or create the state for a key. ``config`` is only used on creation."""
        return self._states.get_or_create(
            key, lambda: _EndpointState(config, self._stats_registry.for_key(key))
        )

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
        discard: Callable[[T], Awaitable[None]] | None = None,
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
            discard: Optional async callable used to release a losing
                task's already-completed result (e.g. closing an
                ``httpx.Response`` to release its pooled connection) when
                the primary and hedge happen to finish in the same
                event-loop pass.

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

        primary_task: asyncio.Task[T] = asyncio.create_task(
            cast("Coroutine[Any, Any, T]", primary_func())
        )
        hedge_task: asyncio.Task[T] | None = None
        tasks = {primary_task}

        try:
            done, _ = await asyncio.wait(tasks, timeout=hedge_delay)
            if not done:
                if self._should_hedge(state, host, key, can_hedge):
                    state.stats.increment_hedged()
                    if self._on_hedge_fired is not None:
                        self._on_hedge_fired(key)
                    hedge_task = asyncio.create_task(
                        cast("Coroutine[Any, Any, T]", hedge_func())
                    )
                    tasks.add(hedge_task)
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            winner_task = (
                primary_task
                if primary_task in done
                else cast("asyncio.Task[T]", hedge_task)
            )

            if hedge_task is not None:
                if winner_task is primary_task:
                    state.stats.increment_primary_wins()
                else:
                    state.stats.increment_hedge_wins()
                loser_task = hedge_task if winner_task is primary_task else primary_task
                if not loser_task.done():
                    loser_task.cancel()
                await asyncio.wait({loser_task})
                await self._discard(loser_task, discard)

            return await self._finish(state, host, key, winner_task, start, classify)
        finally:
            # Reached on the happy path too, where every task is already
            # done and this is a no-op -- but if this coroutine itself is
            # cancelled (e.g. the caller wrapped the request in a timeout)
            # while blocked on one of the awaits above, asyncio.wait does
            # not cancel the tasks it was waiting on, so they'd otherwise
            # keep running detached, holding a pooled connection open.
            pending = {task for task in tasks if not task.done()}
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending)

    def _should_hedge(
        self, state: _EndpointState, host: str, key: str, can_hedge: bool
    ) -> bool:
        """Check the hedge gates: idempotency, circuit breaker, then budget."""
        if not can_hedge:
            return False
        if not self._health.hedging_allowed(
            host, key, self._host_circuit_breaker, state.config.circuit_breaker
        ):
            state.stats.increment_circuit_blocked()
            return False
        if not state.token_bucket.try_acquire():
            state.stats.increment_budget_exhausted()
            return False
        return True

    async def _discard(
        self, task: asyncio.Task[T], discard: Callable[[T], Awaitable[None]] | None
    ) -> None:
        """Retrieve a losing task's outcome so it neither leaks a resource nor
        logs "Task exception was never retrieved", whether it was cancelled
        mid-flight or had already completed (primary and hedge finishing in
        the same event-loop pass)."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            return
        if discard is not None:
            with contextlib.suppress(Exception):
                await discard(task.result())

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
            self._health.record_result(
                host, key, self._host_circuit_breaker, breaker_cfg, False
            )
            state.stats.increment_errors()
            raise

        state.sketch.add(elapsed)
        ok = classify(result)
        self._health.record_result(
            host, key, self._host_circuit_breaker, breaker_cfg, ok
        )
        if not ok:
            state.stats.increment_errors()
        return result

    def close(self) -> None:
        """Noop."""
