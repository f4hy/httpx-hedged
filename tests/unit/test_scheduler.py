"""Unit tests for httpx_hedged._scheduler.HedgeScheduler."""

from __future__ import annotations

import asyncio
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
