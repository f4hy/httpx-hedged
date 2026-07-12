"""Adaptive, per-endpoint request hedging transport for httpx.

Learns per-endpoint latency distributions using DDSketch, fires a backup
request when the primary exceeds its estimated percentile (or a hardcoded
delay), caps hedge rate with a token bucket, and stops hedging entirely
(without blocking the primary request) when a host or endpoint circuit
breaker trips.

Inspired by hedge-python (https://github.com/sunhailin-Leo/hedge-python),
adapted to key hedge state per endpoint rather than per host so a single
service with many routes of very different latency/RPS profiles doesn't
have those profiles mixed into one shared estimate.
"""

from __future__ import annotations

from httpx_hedged._config import CircuitBreakerConfig, EndpointConfig, HedgeConfig
from httpx_hedged._health import CircuitBreaker, CircuitState, HealthRegistry
from httpx_hedged._matcher import Route, UnknownHedgeEndpointError
from httpx_hedged._stats import Stats, StatsRegistry, StatsSnapshot
from httpx_hedged.transport import HedgedTransport

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "EndpointConfig",
    "HealthRegistry",
    "HedgeConfig",
    "HedgedTransport",
    "Route",
    "Stats",
    "StatsRegistry",
    "StatsSnapshot",
    "UnknownHedgeEndpointError",
]

__version__ = "0.2.0"
