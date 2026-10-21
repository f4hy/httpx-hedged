"""Unit tests for httpx_hedged._scheduler_sync.SyncHedgeScheduler."""

from __future__ import annotations

import math
import threading
import time
from concurrent.futures import Future

import pytest

from httpx_hedged._config import (
    CircuitBreakerConfig,
    EffectiveConfig,
    EndpointConfig,
    HedgeConfig,
    resolve,
)
from httpx_hedged._health import HealthRegistry
from httpx_hedged._scheduler_sync import SyncHedgeScheduler
from httpx_hedged._stats import StatsRegistry


def make_scheduler() -> tuple[SyncHedgeScheduler, HealthRegistry, StatsRegistry]:
    health = HealthRegistry()
    stats = StatsRegistry()
    return SyncHedgeScheduler(health, stats, max_workers=8), health, stats


def always_ok(_result: object) -> bool:
    return True


# compute_hedge_delay is a one-line delegation to the shared
# _scheduler.compute_hedge_delay, already covered by test_scheduler.py, so
# it is not re-tested here.

# --- latency_quantile ---------------------------------------------------------


def test_latency_quantile_is_none_for_an_untracked_key() -> None:
    scheduler, _health, _stats = make_scheduler()
    assert scheduler.latency_quantile("never-seen", 0.9) is None


def test_latency_quantile_is_none_before_any_samples() -> None:
    scheduler, _health, _stats = make_scheduler()
    config = resolve(None, HedgeConfig())
    scheduler.state_for("k", config)  # creates the state, but adds no samples
    assert scheduler.latency_quantile("k", 0.9) is None


def test_latency_quantile_reflects_recorded_samples() -> None:
    scheduler, _health, _stats = make_scheduler()
    config = resolve(None, HedgeConfig())
    state = scheduler.state_for("k", config)
    for v in range(1, 101):
        state.sketch.add(v / 1000.0)  # 0.001..0.1 seconds
    p90 = scheduler.latency_quantile("k", 0.9)
    assert p90 is not None
    assert 0.085 <= p90 <= 0.095


# --- execute_with_hedge: race behavior ---------------------------------------

# Real short sleeps, not fake_clock, matching test_scheduler.py's rationale
# (patching time.monotonic would desync from real thread/event-loop
# scheduling either way). Kept well below 1s, unlike the async suite: an
# asyncio task sleeping for a "never wins" 1.0s primary gets cancelled
# almost instantly, but a losing sync thread can't be interrupted -- it
# keeps running for its full duration in the background regardless, so long
# "never finishes" sleeps here would actually cost that much wall time.


def fast_ok() -> str:
    return "ok"


def slow_then_ok(delay: float) -> str:
    time.sleep(delay)
    return "slow-ok"


def hardcoded_config(delay: float, **overrides: object) -> EffectiveConfig:
    return resolve(
        EndpointConfig(hedge_delay=delay), HedgeConfig(min_delay=0.0, **overrides)
    )


def test_fast_primary_never_hedges() -> None:
    scheduler, _health, stats = make_scheduler()
    config = hardcoded_config(1.0)
    result = scheduler.execute_with_hedge(
        key="k",
        host="h",
        config=config,
        primary_func=fast_ok,
        hedge_func=fast_ok,
        classify=always_ok,
        can_hedge=True,
    )
    assert result == "ok"
    snap = stats.snapshot("k")
    assert snap is not None
    assert snap.hedged_requests == 0


def test_slow_primary_triggers_hedge_and_hedge_wins() -> None:
    scheduler, _health, stats = make_scheduler()
    config = hardcoded_config(0.01)
    result = scheduler.execute_with_hedge(
        key="k",
        host="h",
        config=config,
        primary_func=lambda: slow_then_ok(0.3),
        hedge_func=lambda: slow_then_ok(0.02),
        classify=always_ok,
        can_hedge=True,
    )
    assert result == "slow-ok"
    snap = stats.snapshot("k")
    assert snap is not None
    assert snap.hedged_requests == 1
    assert snap.hedge_wins == 1


def test_non_idempotent_never_hedges() -> None:
    scheduler, _health, stats = make_scheduler()
    config = hardcoded_config(0.01)
    result = scheduler.execute_with_hedge(
        key="k",
        host="h",
        config=config,
        primary_func=lambda: slow_then_ok(0.03),
        hedge_func=fast_ok,
        classify=always_ok,
        can_hedge=False,
    )
    assert result == "slow-ok"
    snap = stats.snapshot("k")
    assert snap is not None
    assert snap.hedged_requests == 0


def test_budget_exhaustion_suppresses_hedge_but_primary_completes() -> None:
    scheduler, _health, stats = make_scheduler()
    config = hardcoded_config(0.01, budget_percent=0.0, estimated_rps=100.0)
    # TokenBucket always grants an initial burst of >=1 token even at a
    # zero refill rate; drain it up front so this call observes exhaustion.
    state = scheduler.state_for("k", config)
    assert state.token_bucket.try_acquire() is True
    assert state.token_bucket.try_acquire() is False

    result = scheduler.execute_with_hedge(
        key="k",
        host="h",
        config=config,
        primary_func=lambda: slow_then_ok(0.02),
        hedge_func=fast_ok,
        classify=always_ok,
        can_hedge=True,
    )
    assert result == "slow-ok"
    snap = stats.snapshot("k")
    assert snap is not None
    assert snap.budget_exhausted == 1
    assert snap.hedged_requests == 0


def test_circuit_open_suppresses_hedge_but_primary_completes() -> None:
    scheduler, health, stats = make_scheduler()
    breaker_cfg = CircuitBreakerConfig(min_samples=1, error_rate_threshold=0.0)
    config = resolve(
        EndpointConfig(hedge_delay=0.01, circuit_breaker=breaker_cfg),
        HedgeConfig(min_delay=0.0),
    )
    # Trip the breaker before the request under test.
    health.record_result("h", "k", breaker_cfg, breaker_cfg, False)
    assert health.hedging_allowed("h", "k", breaker_cfg, breaker_cfg) is False

    result = scheduler.execute_with_hedge(
        key="k",
        host="h",
        config=config,
        primary_func=lambda: slow_then_ok(0.02),
        hedge_func=fast_ok,
        classify=always_ok,
        can_hedge=True,
    )
    assert result == "slow-ok"
    snap = stats.snapshot("k")
    assert snap is not None
    assert snap.circuit_blocked == 1
    assert snap.hedged_requests == 0


# --- execute_with_hedge: recording happens before propagating exceptions ----


def raise_error() -> str:
    raise ValueError("boom")


def test_exception_from_primary_is_recorded_then_reraised() -> None:
    scheduler, health, stats = make_scheduler()
    config = hardcoded_config(1.0)  # never hedges; primary alone raises
    with pytest.raises(ValueError):
        scheduler.execute_with_hedge(
            key="k",
            host="h",
            config=config,
            primary_func=raise_error,
            hedge_func=fast_ok,
            classify=always_ok,
            can_hedge=True,
        )
    snap = stats.snapshot("k")
    assert snap is not None
    assert snap.errors == 1
    # health was informed of the failure despite the exception; a single
    # sample is well below the default min_samples, so it hasn't tripped.
    assert (
        health.hedging_allowed("h", "k", config.circuit_breaker, config.circuit_breaker)
        is True
    )
    state = scheduler.state_for("k", config)
    assert not math.isnan(state.sketch.quantile(0.5))  # latency was still recorded


def test_exception_from_hedge_winner_is_recorded_then_reraised() -> None:
    scheduler, _health, stats = make_scheduler()
    config = hardcoded_config(0.01)

    def slow_fail() -> str:
        time.sleep(0.02)
        raise ValueError("hedge failed")

    with pytest.raises(ValueError):
        scheduler.execute_with_hedge(
            key="k",
            host="h",
            config=config,
            primary_func=lambda: slow_then_ok(0.3),
            hedge_func=slow_fail,
            classify=always_ok,
            can_hedge=True,
        )
    snap = stats.snapshot("k")
    assert snap is not None
    assert snap.errors == 1


# --- loser cleanup: the sync-specific behavioral difference from async ------
#
# The async scheduler always resolves (cancels + awaits) the loser before
# execute_with_hedge() returns, so discard has always-already-happened
# semantics there. A losing OS thread can't be interrupted that way, so here
# the loser keeps running in the background and discard fires whenever it
# eventually finishes -- these tests synchronize on that via threading.Event
# rather than asserting state immediately after the call returns.


def test_discard_releases_a_loser_that_completed_successfully() -> None:
    """When a losing future happens to already be done by the time it's
    discarded, its result must still be handed to ``discard`` so callers
    can release it (e.g. closing an httpx.Response to free its pooled
    connection)."""
    scheduler, _health, _stats = make_scheduler()
    released: list[str] = []

    def discard(value: str) -> None:
        released.append(value)

    future: Future[str] = Future()
    future.set_result("ok")
    scheduler._discard(future, discard)
    assert released == ["ok"]


def test_discard_skips_a_cancelled_loser() -> None:
    scheduler, _health, _stats = make_scheduler()
    called = False

    def discard(_: str) -> None:
        nonlocal called
        called = True

    future: Future[str] = Future()
    assert future.cancel() is True  # never submitted, so still cancellable
    scheduler._discard(future, discard)
    assert called is False


def test_discard_skips_a_loser_that_raised() -> None:
    scheduler, _health, _stats = make_scheduler()
    called = False

    def discard(_: str) -> None:
        nonlocal called
        called = True

    future: Future[str] = Future()
    future.set_exception(ValueError("boom"))
    scheduler._discard(future, discard)
    assert called is False


def test_discard_eventually_fires_for_a_loser_still_running_in_the_background() -> None:
    """The loser (primary, here) can't be cancelled mid-flight, so it's
    still running when execute_with_hedge() returns; discard must not have
    fired yet at that point, but must fire once the loser actually
    finishes."""
    scheduler, _health, _stats = make_scheduler()
    config = hardcoded_config(0.01)
    discarded: list[str] = []
    event = threading.Event()

    def discard(value: str) -> None:
        discarded.append(value)
        event.set()

    result = scheduler.execute_with_hedge(
        key="k",
        host="h",
        config=config,
        primary_func=lambda: slow_then_ok(0.3),
        hedge_func=lambda: slow_then_ok(0.02),
        classify=always_ok,
        can_hedge=True,
        discard=discard,
    )
    assert result == "slow-ok"
    assert discarded == []  # the loser (primary) is still mid-sleep

    assert event.wait(timeout=1.0) is True
    assert discarded == ["slow-ok"]


# --- per-key isolation --------------------------------------------------------


def test_states_are_isolated_per_key() -> None:
    scheduler, _health, _stats = make_scheduler()
    config_a = hardcoded_config(1.0)
    config_b = hardcoded_config(1.0)
    scheduler.execute_with_hedge(
        key="a",
        host="h",
        config=config_a,
        primary_func=fast_ok,
        hedge_func=fast_ok,
        classify=always_ok,
        can_hedge=True,
    )
    scheduler.execute_with_hedge(
        key="b",
        host="h",
        config=config_b,
        primary_func=fast_ok,
        hedge_func=fast_ok,
        classify=always_ok,
        can_hedge=True,
    )
    state_a = scheduler.state_for("a", config_a)
    state_b = scheduler.state_for("b", config_b)
    assert state_a is not state_b
    assert state_a.sketch is not state_b.sketch


# --- close ---------------------------------------------------------------


def test_close_shuts_down_the_executor() -> None:
    scheduler, _health, _stats = make_scheduler()
    scheduler.close()
    with pytest.raises(RuntimeError):
        scheduler._executor.submit(fast_ok)
