"""Thread-safe, per-key statistics for hedge operations."""

from __future__ import annotations

import threading
from dataclasses import dataclass

from httpx_hedged._bounded import BoundedRegistry


class Stats:
    """Thread-safe counters for hedge operations on a single key.

    All fields use a lock for atomic updates and are safe to read concurrently.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.total_requests: int = 0
        self.hedged_requests: int = 0
        self.hedge_wins: int = 0
        self.primary_wins: int = 0
        self.budget_exhausted: int = 0
        self.warmup_requests: int = 0
        self.circuit_blocked: int = 0
        self.errors: int = 0

    def increment_total(self) -> None:
        with self._lock:
            self.total_requests += 1

    def increment_hedged(self) -> None:
        with self._lock:
            self.hedged_requests += 1

    def increment_hedge_wins(self) -> None:
        with self._lock:
            self.hedge_wins += 1

    def increment_primary_wins(self) -> None:
        with self._lock:
            self.primary_wins += 1

    def increment_budget_exhausted(self) -> None:
        with self._lock:
            self.budget_exhausted += 1

    def increment_warmup(self) -> None:
        with self._lock:
            self.warmup_requests += 1

    def increment_circuit_blocked(self) -> None:
        with self._lock:
            self.circuit_blocked += 1

    def increment_errors(self) -> None:
        with self._lock:
            self.errors += 1

    def snapshot(self, key: str = "") -> StatsSnapshot:
        """Take a consistent point-in-time copy of all counters."""
        with self._lock:
            return StatsSnapshot(
                key=key,
                total_requests=self.total_requests,
                hedged_requests=self.hedged_requests,
                hedge_wins=self.hedge_wins,
                primary_wins=self.primary_wins,
                budget_exhausted=self.budget_exhausted,
                warmup_requests=self.warmup_requests,
                circuit_blocked=self.circuit_blocked,
                errors=self.errors,
            )

    def hedge_rate(self) -> float:
        """Return hedged_requests / total_requests, or 0.0 if no requests."""
        with self._lock:
            if self.total_requests == 0:
                return 0.0
            return self.hedged_requests / self.total_requests


@dataclass(frozen=True, slots=True)
class StatsSnapshot:
    """Immutable point-in-time snapshot of Stats for a single key."""

    key: str
    total_requests: int
    hedged_requests: int
    hedge_wins: int
    primary_wins: int
    budget_exhausted: int
    warmup_requests: int
    circuit_blocked: int
    errors: int

    def __add__(self, other: StatsSnapshot) -> StatsSnapshot:
        return StatsSnapshot(
            key="*",
            total_requests=self.total_requests + other.total_requests,
            hedged_requests=self.hedged_requests + other.hedged_requests,
            hedge_wins=self.hedge_wins + other.hedge_wins,
            primary_wins=self.primary_wins + other.primary_wins,
            budget_exhausted=self.budget_exhausted + other.budget_exhausted,
            warmup_requests=self.warmup_requests + other.warmup_requests,
            circuit_blocked=self.circuit_blocked + other.circuit_blocked,
            errors=self.errors + other.errors,
        )


_EMPTY_SNAPSHOT = StatsSnapshot(
    key="*",
    total_requests=0,
    hedged_requests=0,
    hedge_wins=0,
    primary_wins=0,
    budget_exhausted=0,
    warmup_requests=0,
    circuit_blocked=0,
    errors=0,
)


class StatsRegistry:
    """Owns one ``Stats`` object per tracked key plus a global aggregate view.

    hedge-python only ever exposes a single global ``Stats`` object; since
    this library tracks hedge behavior per endpoint, per-key breakdown is
    the point of observability here.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stats: BoundedRegistry[Stats] = BoundedRegistry()

    def for_key(self, key: str) -> Stats:
        """Get or create the ``Stats`` object for a key."""
        with self._lock:
            return self._stats.get_or_create(key, Stats)

    def snapshot(self, key: str) -> StatsSnapshot | None:
        """Return a snapshot for a key, or None if the key has no recorded stats."""
        with self._lock:
            stats = self._stats.get(key)
        if stats is None:
            return None
        return stats.snapshot(key)

    def all_snapshots(self) -> dict[str, StatsSnapshot]:
        """Return a snapshot for every tracked key."""
        with self._lock:
            items = list(self._stats.items())
        return {key: stats.snapshot(key) for key, stats in items}

    def global_snapshot(self) -> StatsSnapshot:
        """Return the sum of every tracked key's stats."""
        total = _EMPTY_SNAPSHOT
        for snapshot in self.all_snapshots().values():
            total = total + snapshot
        return total
