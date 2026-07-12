"""Unit tests for httpx_hedged._rate.RollingRateCounter."""

from __future__ import annotations

from collections.abc import Callable

import pytest

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
    # No time has elapsed since window_start, so the previous window (empty)
    # carries full weight (irrelevant, since it's zero) and the divisor is
    # just window_duration.
    assert counter.rate_per_second() == 50 / 10.0


def test_rotation_carries_previous_window_into_estimate(
    fake_clock: Callable[[float], None],
) -> None:
    counter = RollingRateCounter(window_duration=10.0)
    for _ in range(20):
        counter.increment()
    fake_clock(11.0)  # rotate: 20 moves to previous, current starts fresh
    for _ in range(10):
        counter.increment()
    # No time has elapsed since the rotation, so the previous window's count
    # carries full weight.
    assert counter.rate_per_second() == (20 + 10) / 10.0


def test_rate_weights_previous_window_by_remaining_overlap(
    fake_clock: Callable[[float], None],
) -> None:
    """At steady state, the estimate should track the true rate throughout
    the window -- not just right after a rotation -- by discounting the
    previous window's count as it falls further outside the trailing
    window_duration-sized lookback from now."""
    counter = RollingRateCounter(window_duration=10.0)
    for _ in range(20):  # 2/s over the first window
        counter.increment()
    fake_clock(11.0)  # rotate: 20 moves to previous, current starts fresh
    for _ in range(10):
        counter.increment()
    fake_clock(5.0)  # halfway through the new current window
    # previous window is half outside the trailing lookback now, so it's
    # weighted at 50%.
    assert counter.rate_per_second() == pytest.approx((20 * 0.5 + 10) / 10.0)


def test_long_idle_resets_rate_to_zero(fake_clock: Callable[[float], None]) -> None:
    counter = RollingRateCounter(window_duration=10.0)
    for _ in range(20):
        counter.increment()
    fake_clock(25.0)  # beyond 2x window -- reset
    assert counter.rate_per_second() == 0.0
