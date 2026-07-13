"""Unit tests for httpx_hedged._scheduler.HedgeScheduler."""

from __future__ import annotations

import asyncio
import contextlib
import math

import pytest

from httpx_hedged._config import (
    CircuitBreakerConfig,
    EffectiveConfig,
    EndpointConfig,
    HedgeConfig,
    resolve,
)
from httpx_hedged._health import HealthRegistry
from httpx_hedged._scheduler import HedgeScheduler, extract_host
from httpx_hedged._stats import StatsRegistry


def make_scheduler() -> tuple[HedgeScheduler, HealthRegistry, StatsRegistry]:
    health = HealthRegistry()
    stats = StatsRegistry()
    return HedgeScheduler(health, stats), health, stats


def always_ok(_result: object) -> bool:
    return True


# --- extract_host -----------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://example.com/path", "example.com"),
        ("https://example.com:8443/path", "example.com:8443"),
        ("https://user:pass@example.com/path", "example.com"),
        ("https://[::1]:8080/path", "[::1]:8080"),
    ],
)
def test_extract_host(url: str, expected: str) -> None:
    assert extract_host(url) == expected


# --- compute_hedge_delay -----------------------------------------------------


def test_warmup_uses_fixed_delay() -> None:
    scheduler, _health, _stats = make_scheduler()
    config = resolve(None, HedgeConfig(warmup_requests=3, warmup_delay=0.02))
    state = scheduler.state_for("k", config)
    state.counter = 1
    assert scheduler.compute_hedge_delay(state) == 0.02


def test_post_warmup_uses_sketch_quantile() -> None:
    scheduler, _health, _stats = make_scheduler()
    config = resolve(
        None, HedgeConfig(warmup_requests=0, percentile=0.9, min_delay=0.0)
    )
    state = scheduler.state_for("k", config)
    for v in range(1, 101):
        state.sketch.add(v / 1000.0)  # 0.001..0.1 seconds
    state.counter = 1
    delay = scheduler.compute_hedge_delay(state)
    assert 0.085 <= delay <= 0.095


def test_hardcoded_delay_skips_sketch() -> None:
    scheduler, _health, _stats = make_scheduler()
    config = resolve(EndpointConfig(hedge_delay=0.5), HedgeConfig(min_delay=0.0))
    state = scheduler.state_for("k", config)
    assert scheduler.compute_hedge_delay(state) == 0.5


def test_min_delay_floors_the_result() -> None:
    scheduler, _health, _stats = make_scheduler()
    config = resolve(EndpointConfig(hedge_delay=0.001, min_delay=0.05), HedgeConfig())
    state = scheduler.state_for("k", config)
    assert scheduler.compute_hedge_delay(state) == 0.05


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


async def fast_ok() -> str:
    return "ok"


async def slow_then_ok(delay: float) -> str:
    await asyncio.sleep(delay)
    return "slow-ok"


def hardcoded_config(delay: float, **overrides: object) -> EffectiveConfig:
    return resolve(
        EndpointConfig(hedge_delay=delay), HedgeConfig(min_delay=0.0, **overrides)
    )


async def test_fast_primary_never_hedges() -> None:
    scheduler, _health, stats = make_scheduler()
    config = hardcoded_config(1.0)
    result = await scheduler.execute_with_hedge(
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


async def test_slow_primary_triggers_hedge_and_hedge_wins() -> None:
    scheduler, _health, stats = make_scheduler()
    config = hardcoded_config(0.01)
    result = await scheduler.execute_with_hedge(
        key="k",
        host="h",
        config=config,
        primary_func=lambda: slow_then_ok(1.0),
        hedge_func=lambda: slow_then_ok(0.02),
        classify=always_ok,
        can_hedge=True,
    )
    assert result == "slow-ok"
    snap = stats.snapshot("k")
    assert snap is not None
    assert snap.hedged_requests == 1
    assert snap.hedge_wins == 1


async def test_non_idempotent_never_hedges() -> None:
    scheduler, _health, stats = make_scheduler()
    config = hardcoded_config(0.01)
    result = await scheduler.execute_with_hedge(
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


async def test_budget_exhaustion_suppresses_hedge_but_primary_completes() -> None:
    scheduler, _health, stats = make_scheduler()
    config = hardcoded_config(0.01, budget_percent=0.0, estimated_rps=100.0)
    # TokenBucket always grants an initial burst of >=1 token even at a
    # zero refill rate; drain it up front so this call observes exhaustion.
    state = scheduler.state_for("k", config)
    assert state.token_bucket.try_acquire() is True
    assert state.token_bucket.try_acquire() is False

    result = await scheduler.execute_with_hedge(
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


async def test_circuit_open_suppresses_hedge_but_primary_completes() -> None:
    scheduler, health, stats = make_scheduler()
    breaker_cfg = CircuitBreakerConfig(min_samples=1, error_rate_threshold=0.0)
    config = resolve(
        EndpointConfig(hedge_delay=0.01, circuit_breaker=breaker_cfg),
        HedgeConfig(min_delay=0.0),
    )
    # Trip the breaker before the request under test.
    health.record_result("h", "k", breaker_cfg, breaker_cfg, False)
    assert health.hedging_allowed("h", "k", breaker_cfg, breaker_cfg) is False

    result = await scheduler.execute_with_hedge(
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


async def raise_error() -> str:
    raise ValueError("boom")


async def test_exception_from_primary_is_recorded_then_reraised() -> None:
    scheduler, health, stats = make_scheduler()
    config = hardcoded_config(1.0)  # never hedges; primary alone raises
    with pytest.raises(ValueError):
        await scheduler.execute_with_hedge(
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


async def test_exception_from_hedge_winner_is_recorded_then_reraised() -> None:
    scheduler, _health, stats = make_scheduler()
    config = hardcoded_config(0.01)

    async def slow_fail() -> str:
        await asyncio.sleep(0.02)
        raise ValueError("hedge failed")

    with pytest.raises(ValueError):
        await scheduler.execute_with_hedge(
            key="k",
            host="h",
            config=config,
            primary_func=lambda: slow_then_ok(1.0),
            hedge_func=slow_fail,
            classify=always_ok,
            can_hedge=True,
        )
    snap = stats.snapshot("k")
    assert snap is not None
    assert snap.errors == 1


# --- cancellation safety and loser cleanup -----------------------------------


async def test_external_cancellation_does_not_leak_the_primary_task() -> None:
    """If the caller cancels the whole call (e.g. asyncio.wait_for), the
    in-flight primary must actually be cancelled, not left running detached
    in the background."""
    scheduler, _health, _stats = make_scheduler()
    config = hardcoded_config(1.0)  # hedge delay far longer than the timeout below
    ran_to_completion = False

    async def slow_primary() -> str:
        nonlocal ran_to_completion
        await asyncio.sleep(1.0)
        ran_to_completion = True
        return "ok"

    async def call() -> str:
        return await scheduler.execute_with_hedge(
            key="k",
            host="h",
            config=config,
            primary_func=slow_primary,
            hedge_func=fast_ok,
            classify=always_ok,
            can_hedge=True,
        )

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(call(), timeout=0.02)

    # Give the event loop a chance to run any orphaned background task; if
    # the primary leaked, it would complete here.
    await asyncio.sleep(0.05)
    assert ran_to_completion is False


async def test_discard_releases_a_loser_that_completed_successfully() -> None:
    """When the primary and hedge finish in the same event-loop pass, the
    non-winning task is never cancelled (it's already done), so its result
    must still be handed to ``discard`` so callers can release it (e.g.
    closing an httpx.Response to free its pooled connection)."""
    scheduler, _health, _stats = make_scheduler()
    released: list[str] = []

    async def discard(value: str) -> None:
        released.append(value)

    task = asyncio.create_task(fast_ok())
    await task
    await scheduler._discard(task, discard)
    assert released == ["ok"]


async def test_discard_skips_a_cancelled_loser() -> None:
    scheduler, _health, _stats = make_scheduler()
    called = False

    async def discard(_: str) -> None:
        nonlocal called
        called = True

    task = asyncio.create_task(slow_then_ok(10.0))
    await asyncio.sleep(0)  # let it start
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    await scheduler._discard(task, discard)
    assert called is False


async def test_discard_skips_a_loser_that_raised() -> None:
    scheduler, _health, _stats = make_scheduler()
    called = False

    async def discard(_: str) -> None:
        nonlocal called
        called = True

    task = asyncio.create_task(raise_error())
    with contextlib.suppress(ValueError):
        await task

    # Must not re-raise the loser's exception, and must not call discard.
    await scheduler._discard(task, discard)
    assert called is False


async def test_discard_not_called_for_a_genuinely_cancelled_loser_in_a_race() -> None:
    """The common case: the loser is still mid-flight when cancelled, so it
    never produced a result, and discard must not be invoked for it."""
    scheduler, _health, _stats = make_scheduler()
    config = hardcoded_config(0.01)
    discarded: list[str] = []

    async def discard(value: str) -> None:
        discarded.append(value)

    result = await scheduler.execute_with_hedge(
        key="k",
        host="h",
        config=config,
        primary_func=lambda: slow_then_ok(1.0),
        hedge_func=lambda: slow_then_ok(0.02),
        classify=always_ok,
        can_hedge=True,
        discard=discard,
    )
    assert result == "slow-ok"
    assert discarded == []


# --- per-key isolation --------------------------------------------------------


async def test_states_are_isolated_per_key() -> None:
    scheduler, _health, _stats = make_scheduler()
    config_a = hardcoded_config(1.0)
    config_b = hardcoded_config(1.0)
    await scheduler.execute_with_hedge(
        key="a",
        host="h",
        config=config_a,
        primary_func=fast_ok,
        hedge_func=fast_ok,
        classify=always_ok,
        can_hedge=True,
    )
    await scheduler.execute_with_hedge(
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
