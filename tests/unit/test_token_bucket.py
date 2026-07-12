"""Unit tests for httpx_hedged.budget._token_bucket.TokenBucket."""

from __future__ import annotations

from collections.abc import Callable

from httpx_hedged.budget._token_bucket import TokenBucket


def test_burst_capacity_then_denial(fake_clock: Callable[[float], None]) -> None:
    # rate = 100 * 10/100 = 10 tokens/sec, max_burst = 20
    bucket = TokenBucket(budget_percent=10.0, estimated_rps=100.0)
    acquired = sum(1 for _ in range(30) if bucket.try_acquire())
    assert acquired == 20  # exactly the burst capacity, no time has passed to refill
    assert bucket.try_acquire() is False


def test_refill_over_time_allows_more_acquisitions(
    fake_clock: Callable[[float], None],
) -> None:
    bucket = TokenBucket(
        budget_percent=10.0, estimated_rps=100.0
    )  # 10 tokens/sec, burst 20
    for _ in range(20):
        assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False

    fake_clock(1.0)  # refills 10 tokens
    acquired = sum(1 for _ in range(15) if bucket.try_acquire())
    assert acquired == 10


def test_set_rps_updates_rate_and_caps_existing_tokens(
    fake_clock: Callable[[float], None],
) -> None:
    bucket = TokenBucket(budget_percent=10.0, estimated_rps=100.0)  # burst 20, full
    bucket.set_rps(10.0)  # rate = 1 token/sec, max_burst = max(2, 1) = 2
    acquired = sum(1 for _ in range(10) if bucket.try_acquire())
    assert acquired == 2


def test_low_rps_still_allows_minimum_burst_of_one(
    fake_clock: Callable[[float], None],
) -> None:
    bucket = TokenBucket(budget_percent=10.0, estimated_rps=0.0)
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False
