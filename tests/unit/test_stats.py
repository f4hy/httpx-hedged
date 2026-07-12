"""Unit tests for httpx_hedged._stats: Stats and StatsRegistry."""

from __future__ import annotations

from httpx_hedged._stats import StatsRegistry


def test_hedge_rate_zero_division_guard() -> None:
    registry = StatsRegistry()
    stats = registry.for_key("a")
    assert stats.hedge_rate() == 0.0


def test_snapshot_reflects_recorded_counters() -> None:
    registry = StatsRegistry()
    stats = registry.for_key("a")
    stats.increment_total()
    stats.increment_total()
    stats.increment_hedged()
    stats.increment_hedge_wins()
    stats.increment_circuit_blocked()
    stats.increment_errors()

    snap = registry.snapshot("a")
    assert snap is not None
    assert snap.total_requests == 2
    assert snap.hedged_requests == 1
    assert snap.hedge_wins == 1
    assert snap.circuit_blocked == 1
    assert snap.errors == 1
    assert stats.hedge_rate() == 0.5


def test_unknown_key_snapshot_is_none() -> None:
    registry = StatsRegistry()
    assert registry.snapshot("missing") is None


def test_keys_are_isolated_from_each_other() -> None:
    registry = StatsRegistry()
    registry.for_key("a").increment_total()
    registry.for_key("a").increment_total()
    registry.for_key("b").increment_total()

    snapshots = registry.all_snapshots()
    assert snapshots["a"].total_requests == 2
    assert snapshots["b"].total_requests == 1


def test_global_snapshot_aggregates_across_keys() -> None:
    registry = StatsRegistry()
    registry.for_key("a").increment_total()
    registry.for_key("a").increment_hedged()
    registry.for_key("b").increment_total()
    registry.for_key("b").increment_errors()

    total = registry.global_snapshot()
    assert total.total_requests == 2
    assert total.hedged_requests == 1
    assert total.errors == 1


def test_global_snapshot_with_no_keys_is_zeroed() -> None:
    registry = StatsRegistry()
    total = registry.global_snapshot()
    assert total.total_requests == 0
