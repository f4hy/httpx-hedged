"""Unit tests for httpx_hedged._rate.RollingRateCounter."""

from __future__ import annotations

from collections.abc import Callable

from httpx_hedged._rate import RollingRateCounter


def test_rate_zero_with_no_samples(fake_clock: Callable[[float], None]) -> None:
    counter = RollingRateCounter(window_duration=10.0)
    assert counter.rate_per_second() == 0.0


def test_rate_reflects_increments_within_window(
    fake_clock: Callable[[float], None],
) -> None:
    counter = RollingRateCounter(window_duration=10.0)
    for _ in range(50):
        counter.increment()
    # 50 requests counted in "current" only, so far; divisor is 2*window_duration
    assert counter.rate_per_second() == 50 / 20.0


def test_rotation_carries_previous_window_into_estimate(
    fake_clock: Callable[[float], None],
) -> None:
    counter = RollingRateCounter(window_duration=10.0)
    for _ in range(20):
        counter.increment()
    fake_clock(11.0)  # rotate: 20 moves to previous, current starts fresh
    for _ in range(10):
        counter.increment()
    assert counter.rate_per_second() == (20 + 10) / 20.0


def test_long_idle_resets_rate_to_zero(fake_clock: Callable[[float], None]) -> None:
    counter = RollingRateCounter(window_duration=10.0)
    for _ in range(20):
        counter.increment()
    fake_clock(25.0)  # beyond 2x window -- reset
    assert counter.rate_per_second() == 0.0
