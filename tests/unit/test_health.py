"""Unit tests for httpx_hedged._health: circuit breaker state machine."""

from __future__ import annotations

from collections.abc import Callable

from httpx_hedged._health import CircuitBreaker, CircuitState, HealthRegistry
from httpx_hedged._options import CircuitBreakerConfig


def make_breaker(**overrides: object) -> CircuitBreaker:
    config = CircuitBreakerConfig(
        error_rate_threshold=0.5,
        min_samples=10,
        window_duration=30.0,
        cooldown=30.0,
        half_open_max_trial=5,
    )
    for key, value in overrides.items():
        setattr(config, key, value)
    return CircuitBreaker(config)


def test_closed_allows_hedging(fake_clock: Callable[[float], None]) -> None:
    breaker = make_breaker()
    assert breaker.state is CircuitState.CLOSED
    assert breaker.allow_hedge() is True


def test_stays_closed_below_min_samples_even_with_all_failures(
    fake_clock: Callable[[float], None],
) -> None:
    breaker = make_breaker(min_samples=10)
    for _ in range(9):
        breaker.record_result(False)
    assert breaker.state is CircuitState.CLOSED
    assert breaker.allow_hedge() is True


def test_trips_open_at_threshold_and_min_samples(
    fake_clock: Callable[[float], None],
) -> None:
    breaker = make_breaker(min_samples=10, error_rate_threshold=0.5)
    for _ in range(5):
        breaker.record_result(True)
    for _ in range(5):
        breaker.record_result(False)
    assert breaker.state is CircuitState.OPEN
    assert breaker.allow_hedge() is False


def test_open_blocks_hedging_during_cooldown(
    fake_clock: Callable[[float], None],
) -> None:
    breaker = make_breaker(min_samples=2, error_rate_threshold=0.5, cooldown=30.0)
    breaker.record_result(False)
    breaker.record_result(False)
    assert breaker.state is CircuitState.OPEN

    fake_clock(29.0)
    assert breaker.allow_hedge() is False


def test_open_transitions_to_half_open_after_cooldown(
    fake_clock: Callable[[float], None],
) -> None:
    breaker = make_breaker(min_samples=2, error_rate_threshold=0.5, cooldown=30.0)
    breaker.record_result(False)
    breaker.record_result(False)
    assert breaker.state is CircuitState.OPEN

    fake_clock(31.0)
    assert breaker.allow_hedge() is True
    assert breaker.state is CircuitState.HALF_OPEN


def test_half_open_closes_after_successful_trials(
    fake_clock: Callable[[float], None],
) -> None:
    breaker = make_breaker(
        min_samples=2, error_rate_threshold=0.5, cooldown=30.0, half_open_max_trial=3
    )
    breaker.record_result(False)
    breaker.record_result(False)
    fake_clock(31.0)
    breaker.allow_hedge()  # transitions to HALF_OPEN
    assert breaker.state is CircuitState.HALF_OPEN

    for _ in range(3):
        breaker.record_result(True)
    assert breaker.state is CircuitState.CLOSED
    assert breaker.allow_hedge() is True


def test_half_open_reopens_after_failed_trials(
    fake_clock: Callable[[float], None],
) -> None:
    breaker = make_breaker(
        min_samples=2, error_rate_threshold=0.5, cooldown=30.0, half_open_max_trial=3
    )
    breaker.record_result(False)
    breaker.record_result(False)
    fake_clock(31.0)
    breaker.allow_hedge()  # transitions to HALF_OPEN

    for _ in range(3):
        breaker.record_result(False)
    assert breaker.state is CircuitState.OPEN
    assert breaker.allow_hedge() is False


def test_half_open_limits_concurrent_trials() -> None:
    breaker = make_breaker(half_open_max_trial=2)
    breaker._enter_half_open()  # type: ignore[attr-defined]
    assert breaker.allow_hedge() is True
    assert breaker.allow_hedge() is True
    breaker.record_result(True)
    breaker.record_result(True)
    # after half_open_max_trial results recorded, breaker has already closed
    assert breaker.state is CircuitState.CLOSED


class TestHealthRegistry:
    def test_host_breaker_blocks_all_endpoints_on_that_host(
        self, fake_clock: Callable[[float], None]
    ) -> None:
        registry = HealthRegistry()
        host_cfg = CircuitBreakerConfig(min_samples=2, error_rate_threshold=0.5)
        endpoint_a_cfg = CircuitBreakerConfig(min_samples=2, error_rate_threshold=0.5)
        endpoint_b_cfg = CircuitBreakerConfig(min_samples=2, error_rate_threshold=0.5)

        # Trip the host breaker via endpoint A's failures.
        registry.record_result("host1", "endpoint:a", host_cfg, endpoint_a_cfg, False)
        registry.record_result("host1", "endpoint:a", host_cfg, endpoint_a_cfg, False)

        assert (
            registry.hedging_allowed("host1", "endpoint:a", host_cfg, endpoint_a_cfg)
            is False
        )
        # Endpoint B never failed itself, but the shared host breaker is open.
        assert (
            registry.hedging_allowed("host1", "endpoint:b", host_cfg, endpoint_b_cfg)
            is False
        )

    def test_endpoint_breaker_does_not_affect_siblings(
        self, fake_clock: Callable[[float], None]
    ) -> None:
        registry = HealthRegistry()
        host_cfg = CircuitBreakerConfig(
            min_samples=100, error_rate_threshold=0.5
        )  # host never trips
        endpoint_a_cfg = CircuitBreakerConfig(min_samples=2, error_rate_threshold=0.5)
        endpoint_b_cfg = CircuitBreakerConfig(min_samples=2, error_rate_threshold=0.5)

        registry.record_result("host1", "endpoint:a", host_cfg, endpoint_a_cfg, False)
        registry.record_result("host1", "endpoint:a", host_cfg, endpoint_a_cfg, False)

        assert (
            registry.hedging_allowed("host1", "endpoint:a", host_cfg, endpoint_a_cfg)
            is False
        )
        assert (
            registry.hedging_allowed("host1", "endpoint:b", host_cfg, endpoint_b_cfg)
            is True
        )
