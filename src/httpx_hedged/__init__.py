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

Works with either ``httpx`` (``httpx_hedged.HedgedTransport`` /
``SyncHedgedTransport``, install ``httpx-hedged[httpx]``) or ``httpx2``
(``httpx_hedged.httpx2.HedgedTransport`` / ``SyncHedgedTransport``, install
``httpx-hedged[httpx2]``). Neither is a hard dependency, so the httpx
transports are imported lazily on first access.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from httpx_hedged._config import CircuitBreakerConfig, EndpointConfig, HedgeConfig
from httpx_hedged._health import CircuitBreaker, CircuitState, HealthRegistry
from httpx_hedged._matcher import Route, UnknownHedgeEndpointError
from httpx_hedged._stats import Stats, StatsRegistry, StatsSnapshot
from httpx_hedged._version import __version__

if TYPE_CHECKING:
    from httpx_hedged.sync_transport import SyncHedgedTransport
    from httpx_hedged.transport import HedgedTransport

_LAZY = {
    "HedgedTransport": "httpx_hedged.transport",
    "SyncHedgedTransport": "httpx_hedged.sync_transport",
}

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
    "SyncHedgedTransport",
    "UnknownHedgeEndpointError",
    "__version__",
]


def __getattr__(name: str) -> object:
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module), name)
    globals()[name] = value
    return value
