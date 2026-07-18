"""Core async hedge scheduling: per-key state and the race-then-cancel logic.

Adapted from hedge-python's ``HedgeScheduler`` (``transport/_base.py``), with
two changes: state is keyed per *endpoint* rather than only per *host* (see
``_matcher.py``), and the win/lose bookkeeping always records the outcome
(latency + health) even when the winning task raised an exception. In
hedge-python, ``winner_task.result()`` is called before recording, so an
exception there silently skips recording entirely. This rewrite records
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
    """Per-key (per-endpoint or per-host-fallback) hedge state.

    Shared by both ``HedgeScheduler`` (async) and ``SyncHedgeScheduler``
    (sync). ``sketch``/``token_bucket``/``rate_counter``/``stats`` are each
    internally thread-safe already. ``counter`` is a plain unlocked int:
    under the sync scheduler's worker threads a racing ``+= 1`` can
    occasionally lose an increment, which at worst extends the warmup phase
    by a request — not worth a per-request lock on the async path.
    """

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

    def increment_counter(self) -> None:
        self.counter += 1


def compute_hedge_delay(state: _EndpointState) -> float:
    """Compute the hedge delay in seconds for the current request on this key.

    Pure function of ``state`` — shared verbatim by ``HedgeScheduler`` and
    ``SyncHedgeScheduler``, neither of which touches an async/thread
    primitive here.
    """
    config = state.config
    if config.hedge_delay is not None:
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


def begin_request(state: _EndpointState) -> tuple[float, float]:
    """Per-request bookkeeping run before the race starts, shared by both
    schedulers: bump the total/warmup counters, feed the RPS estimate into
    the token bucket, and compute the hedge delay.

    Returns ``(hedge_delay, start)``, where ``start`` is the
    ``time.monotonic()`` timestamp the request's latency is measured from.
    """
    state.stats.increment_total()
    state.increment_counter()
    if state.rate_counter is not None:
        state.rate_counter.increment()
        state.token_bucket.set_rps(state.rate_counter.rate_per_second())
    return compute_hedge_delay(state), time.monotonic()


def record_race_winner(state: _EndpointState, primary_won: bool) -> None:
    """Record which side won a race in which a hedge was actually fired."""
    if primary_won:
        state.stats.increment_primary_wins()
    else:
        state.stats.increment_hedge_wins()


def should_hedge(
    state: _EndpointState,
    host: str,
    key: str,
    can_hedge: bool,
    health: HealthRegistry,
    host_circuit_breaker: CircuitBreakerConfig,
) -> bool:
    """Check the hedge gates: idempotency, circuit breaker, then budget.

    Shared by both schedulers; ``health``/``state.token_bucket`` are each
    internally thread-safe already, so this function itself touches no
    concurrency primitive.
    """
    if not can_hedge:
        return False
    if not health.hedging_allowed(
        host, key, host_circuit_breaker, state.config.circuit_breaker
    ):
        state.stats.increment_circuit_blocked()
        return False
    if not state.token_bucket.try_acquire():
        state.stats.increment_budget_exhausted()
        return False
    return True


def record_outcome(
    state: _EndpointState,
    host: str,
    key: str,
    health: HealthRegistry,
    host_circuit_breaker: CircuitBreakerConfig,
    start: float,
    get_result: Callable[[], T],
    classify: Callable[[T], bool],
) -> T:
    """Record latency and health outcome, then return the result or re-raise.

    Recording always happens before the result is returned or the
    exception is re-raised. In hedge-python, by contrast, an exception
    from ``winner_task.result()`` silently skips recording.

    ``get_result`` is usually ``winner_task.result`` (async) or
    ``winner_future.result`` (sync) — plain, non-blocking accessors once the
    winner is already done — so this function itself needs no async/thread
    primitive. The sync scheduler's no-hedge fast path instead passes the
    request callable itself, which is why latency is measured when
    ``get_result`` returns rather than on entry.
    """
    breaker_cfg = state.config.circuit_breaker
    try:
        result = get_result()
    except Exception:
        state.sketch.add(time.monotonic() - start)
        health.record_result(host, key, host_circuit_breaker, breaker_cfg, False)
        state.stats.increment_errors()
        raise

    state.sketch.add(time.monotonic() - start)
    ok = classify(result)
    health.record_result(host, key, host_circuit_breaker, breaker_cfg, ok)
    if not ok:
        state.stats.increment_errors()
    return result


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
            to be resolved for a given request. A host isn't owned by any
            one endpoint, so its breaker thresholds must not depend on
            request arrival order (``execute_with_hedge`` always passes
            this rather than the per-request resolved config for the host
            side of ``HealthRegistry`` calls).
        on_hedge_fired: Called with the key each time a hedge request is
            actually launched, after the idempotency, circuit-breaker, and
            budget gates have all passed. Intended for metrics; see the
            README's observability section for an example.
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

    def latency_quantile(self, key: str, q: float) -> float | None:
        """Return the current estimated latency (seconds) at quantile ``q``
        for a tracked key, or None if the key isn't tracked yet or has no
        recorded samples."""
        state = self._states.get(key)
        if state is None:
            return None
        estimate = state.sketch.quantile(q)
        return None if math.isnan(estimate) else estimate

    def compute_hedge_delay(self, state: _EndpointState) -> float:
        """Compute the hedge delay in seconds for the current request on this key."""
        return compute_hedge_delay(state)

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
        hedge_delay, start = begin_request(state)

        primary_task: asyncio.Task[T] = asyncio.create_task(
            cast("Coroutine[Any, Any, T]", primary_func())
        )
        hedge_task: asyncio.Task[T] | None = None
        tasks = {primary_task}

        try:
            done, _ = await asyncio.wait(tasks, timeout=hedge_delay)
            if not done:
                if should_hedge(
                    state,
                    host,
                    key,
                    can_hedge,
                    self._health,
                    self._host_circuit_breaker,
                ):
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
                record_race_winner(state, winner_task is primary_task)
                loser_task = hedge_task if winner_task is primary_task else primary_task
                if not loser_task.done():
                    loser_task.cancel()
                await asyncio.wait({loser_task})
                await self._discard(loser_task, discard)

            return record_outcome(
                state,
                host,
                key,
                self._health,
                self._host_circuit_breaker,
                start,
                winner_task.result,
                classify,
            )
        finally:
            # Reached on the happy path too, where every task is already
            # done and this is a no-op. But if this coroutine itself is
            # cancelled (e.g. the caller wrapped the request in a timeout)
            # while blocked on one of the awaits above, asyncio.wait does
            # not cancel the tasks it was waiting on, so they'd otherwise
            # keep running detached, holding a pooled connection open.
            pending = {task for task in tasks if not task.done()}
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending)

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

    def close(self) -> None:
        """Noop."""
