"""Sync counterpart to ``_scheduler.HedgeScheduler``, for ``SyncHedgedTransport``.

Same race-then-cancel shape as the async scheduler, but built on
``concurrent.futures.ThreadPoolExecutor`` instead of ``asyncio.create_task``,
since a sync ``httpx.Client`` has no event loop to schedule coroutines on.

The one real behavioral difference from the async scheduler: a losing OS
thread blocked on a socket read cannot be interrupted the way
``asyncio.Task.cancel()`` interrupts a coroutine at its next ``await``. So
unlike the async scheduler (which always resolves the loser, by cancelling
and awaiting it, before returning), this scheduler returns the winner's
result immediately and lets the loser finish in the background whenever its
blocking call happens to return, discarding its result via a done-callback
at that point. ``loser_future.cancel()`` is still attempted first, but it
only succeeds if the loser is still queued and hasn't started running yet
(e.g. it never got a worker thread before losing) — once a thread is mid
socket-read, cancellation is a no-op.

This means hedging through this scheduler *without a request timeout
configured on the inner transport is unsafe*: a losing primary or hedge with
no timeout can block its worker thread forever, and enough of those
piling up exhausts the thread pool. Always pair this with an
``httpx.Timeout`` on the wrapped client/transport.
"""

from __future__ import annotations

import contextlib
import math
import threading
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import TypeVar, cast

from httpx_hedged._bounded import BoundedRegistry
from httpx_hedged._config import CircuitBreakerConfig, EffectiveConfig
from httpx_hedged._health import HealthRegistry
from httpx_hedged._scheduler import (
    _EndpointState,
    begin_request,
    compute_hedge_delay,
    record_outcome,
    record_race_winner,
    should_hedge,
)
from httpx_hedged._stats import StatsRegistry

T = TypeVar("T")

# 2x httpx's own default connection-pool ceiling (httpx.Limits().max_connections
# == 100), to leave headroom for orphaned loser threads piling up alongside
# legitimate concurrent primary/hedge work.
_DEFAULT_MAX_WORKERS = 200


class SyncHedgeScheduler:
    """Shared sync hedge scheduling logic, used by ``SyncHedgedTransport``.

    See the module docstring for how loser-thread handling differs from the
    async ``HedgeScheduler``.

    Args:
        health: Shared circuit-breaker registry.
        stats_registry: Shared per-key statistics registry.
        host_circuit_breaker: Circuit-breaker configuration used for the
            *host* tier, independent of whichever endpoint's config happens
            to be resolved for a given request (see ``HedgeScheduler`` for
            the full rationale, identical here).
        on_hedge_fired: Called with the key each time a hedge request is
            actually launched, after the idempotency, circuit-breaker, and
            budget gates have all passed.
        max_workers: Size of the internal thread pool. Ignored if
            ``executor`` is given. Defaults to ``_DEFAULT_MAX_WORKERS``.
        executor: A pre-built ``ThreadPoolExecutor`` to use instead of
            creating one. Still shut down by ``close()`` regardless of who
            constructed it, matching ``HedgedTransport.aclose()`` always
            closing its inner transport regardless of who constructed that.
    """

    def __init__(
        self,
        health: HealthRegistry,
        stats_registry: StatsRegistry,
        host_circuit_breaker: CircuitBreakerConfig | None = None,
        on_hedge_fired: Callable[[str], None] | None = None,
        max_workers: int | None = None,
        executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self._health = health
        self._stats_registry = stats_registry
        self._host_circuit_breaker = host_circuit_breaker or CircuitBreakerConfig()
        self._on_hedge_fired = on_hedge_fired
        self._states: BoundedRegistry[_EndpointState] = BoundedRegistry()
        self._states_lock = threading.Lock()
        self._executor = executor or ThreadPoolExecutor(
            max_workers=max_workers or _DEFAULT_MAX_WORKERS
        )

    def state_for(self, key: str, config: EffectiveConfig) -> _EndpointState:
        """Get or create the state for a key. ``config`` is only used on creation.

        Locked because ``BoundedRegistry`` itself is not thread-safe and a
        shared sync ``httpx.Client`` can call in from many worker threads
        at once — unlike the async scheduler, which runs on one event-loop
        thread and needs no lock here.
        """
        with self._states_lock:
            return self._states.get_or_create(
                key, lambda: _EndpointState(config, self._stats_registry.for_key(key))
            )

    def latency_quantile(self, key: str, q: float) -> float | None:
        """Return the current estimated latency (seconds) at quantile ``q``
        for a tracked key, or None if the key isn't tracked yet or has no
        recorded samples."""
        with self._states_lock:
            state = self._states.get(key)
        if state is None:
            return None
        estimate = state.sketch.quantile(q)
        return None if math.isnan(estimate) else estimate

    def compute_hedge_delay(self, state: _EndpointState) -> float:
        """Compute the hedge delay in seconds for the current request on this key."""
        return compute_hedge_delay(state)

    def execute_with_hedge(
        self,
        *,
        key: str,
        host: str,
        config: EffectiveConfig,
        primary_func: Callable[[], T],
        hedge_func: Callable[[], T],
        classify: Callable[[T], bool],
        can_hedge: bool,
        discard: Callable[[T], None] | None = None,
    ) -> T:
        """Execute the primary request with hedge racing logic.

        Args mirror ``HedgeScheduler.execute_with_hedge`` except
        ``primary_func``/``hedge_func``/``discard`` are plain sync
        callables. See the module docstring for how loser handling differs:
        this returns as soon as a winner is available, without waiting for
        the loser to finish.
        """
        state = self.state_for(key, config)
        hedge_delay, start = begin_request(state)

        if not can_hedge:
            # A hedge can never fire for this request, so skip the executor
            # race entirely and run the primary on the calling thread — a
            # write-heavy workload would otherwise burn two threads per
            # request for no possible benefit. record_outcome measures
            # latency when the callable returns, so recording is identical
            # to the raced path.
            return record_outcome(
                state,
                host,
                key,
                self._health,
                self._host_circuit_breaker,
                start,
                primary_func,
                classify,
            )

        primary_future: Future[T] = self._executor.submit(primary_func)
        hedge_future: Future[T] | None = None
        futures = {primary_future}

        done, _ = wait(futures, timeout=hedge_delay)
        if not done:
            if should_hedge(
                state, host, key, can_hedge, self._health, self._host_circuit_breaker
            ):
                state.stats.increment_hedged()
                if self._on_hedge_fired is not None:
                    self._on_hedge_fired(key)
                hedge_future = self._executor.submit(hedge_func)
                futures.add(hedge_future)
            done, _ = wait(futures, return_when=FIRST_COMPLETED)

        winner_future = (
            primary_future
            if primary_future in done
            else cast("Future[T]", hedge_future)
        )

        if hedge_future is not None:
            record_race_winner(state, winner_future is primary_future)
            loser_future = (
                hedge_future if winner_future is primary_future else primary_future
            )
            loser_future.cancel()
            if discard is not None:
                loser_future.add_done_callback(lambda f: self._discard(f, discard))

        return record_outcome(
            state,
            host,
            key,
            self._health,
            self._host_circuit_breaker,
            start,
            winner_future.result,
            classify,
        )

    def _discard(self, future: Future[T], discard: Callable[[T], None]) -> None:
        """Release a losing future's already-completed result (e.g. closing
        an ``httpx.Response`` to free its pooled connection), whether it
        was cancelled before it started or completed on its own after the
        winner was already returned. Runs as a ``Future`` done-callback —
        on whichever thread completes the future, or synchronously if it
        was already done when attached — so errors are suppressed rather
        than left for ``concurrent.futures`` to swallow-and-log.

        Note ``concurrent.futures.CancelledError`` subclasses ``Exception``
        (unlike ``asyncio.CancelledError``, which subclasses
        ``BaseException``); the ``.cancelled()`` check below still handles
        it correctly, exactly as in the async scheduler's ``_discard``, but
        don't assume the two exception hierarchies are interchangeable.
        """
        if future.cancelled():
            return
        exc = future.exception()
        if exc is not None:
            return
        with contextlib.suppress(Exception):
            discard(future.result())

    def close(self) -> None:
        """Shut down the internal thread pool.

        Blocks until every in-flight future finishes, including any
        orphaned loser still running in the background — this can hang if a
        loser is blocked on a socket with no timeout configured on the
        inner transport. Also cancels anything still queued but not yet
        started. Shuts down the executor even if it was supplied by the
        caller via ``executor=``, matching ``HedgedTransport.aclose()``
        always closing its inner transport regardless of who constructed
        it.
        """
        self._executor.shutdown(wait=True, cancel_futures=True)
