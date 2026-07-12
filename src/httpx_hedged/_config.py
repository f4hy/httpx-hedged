"""Configuration for hedge behavior, and default/per-endpoint override resolution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypeVar


@dataclass(slots=True)
class CircuitBreakerConfig:
    """Configuration for the health circuit breaker that gates hedging.

    Tripping the breaker only ever suppresses the *hedge* request -- the
    primary request is always sent and its result or exception is always
    returned to the caller normally.
    """

    #: Fraction of failed requests (0-1) that trips the breaker open.
    error_rate_threshold: float = 0.5

    #: Minimum number of samples in the current window before the breaker
    #: is allowed to trip, avoiding trips on tiny samples.
    min_samples: int = 20

    #: Rolling window size in seconds for the error-rate estimate.
    window_duration: float = 30.0

    #: Seconds the breaker stays open before allowing a half-open trial.
    cooldown: float = 30.0

    #: Number of trial requests allowed through while half-open before
    #: deciding to close or re-open.
    half_open_max_trial: int = 5

    #: Whether an HTTP 5xx response counts as a failure for
    #: circuit-breaker purposes.
    treat_5xx_as_failure: bool = True


@dataclass(slots=True)
class HedgeConfig:
    """Transport-wide default hedge configuration."""

    #: Sketch quantile used as hedge trigger.
    percentile: float = 0.90

    #: Maximum concurrent hedge requests per call.
    max_hedges: int = 1

    #: Max hedge rate as percent of total traffic.
    budget_percent: float = 10.0

    #: Expected requests per second. If None, the rate is estimated
    #: automatically from observed traffic instead of requiring a manual
    #: guess.
    estimated_rps: float | None = None

    #: Rolling window in seconds for RPS auto-estimation.
    rps_window_duration: float = 10.0

    #: Floor on the hedge delay in seconds.
    min_delay: float = 0.001

    #: Number of initial requests using a fixed delay before the sketch
    #: is trusted.
    warmup_requests: int = 20

    #: Fixed hedge delay during warmup, in seconds.
    warmup_delay: float = 0.01

    #: Latency sketch window rotation interval in seconds.
    window_duration: float = 30.0

    #: Health circuit-breaker configuration.
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)


@dataclass(slots=True)
class EndpointConfig:
    """Per-endpoint override of the transport's default ``HedgeConfig``.

    Every field defaults to None, meaning "inherit the transport default."
    """

    #: Hardcode a fixed hedge delay for this endpoint instead of learning
    #: one adaptively from a DDSketch. The endpoint still records latency
    #: into its sketch for observability; it just isn't consulted for the
    #: hedge-delay decision.
    hedge_delay: float | None = None

    #: Sketch quantile used as hedge trigger.
    percentile: float | None = None

    #: Maximum concurrent hedge requests per call.
    max_hedges: int | None = None

    #: Max hedge rate as percent of total traffic.
    budget_percent: float | None = None

    #: Expected requests per second. If None, the rate is estimated
    #: automatically from observed traffic instead of requiring a manual
    #: guess.
    estimated_rps: float | None = None

    #: Rolling window in seconds for RPS auto-estimation.
    rps_window_duration: float | None = None

    #: Floor on the hedge delay in seconds.
    min_delay: float | None = None

    #: Number of initial requests using a fixed delay before the sketch
    #: is trusted.
    warmup_requests: int | None = None

    #: Fixed hedge delay during warmup, in seconds.
    warmup_delay: float | None = None

    #: Latency sketch window rotation interval in seconds.
    window_duration: float | None = None

    #: Health circuit-breaker configuration. When set, replaces the
    #: default breaker config as a whole object rather than being merged
    #: field-by-field -- for a partial override, use
    #: ``dataclasses.replace(default.circuit_breaker, ...)``.
    circuit_breaker: CircuitBreakerConfig | None = None


@dataclass(frozen=True, slots=True)
class EffectiveConfig:
    """Fully-resolved hedge configuration for a single key (no more None fields)."""

    percentile: float
    max_hedges: int
    budget_percent: float
    estimated_rps: float | None
    rps_window_duration: float
    min_delay: float
    warmup_requests: int
    warmup_delay: float
    window_duration: float
    circuit_breaker: CircuitBreakerConfig
    hedge_delay: float | None = None

    @property
    def is_hardcoded(self) -> bool:
        return self.hedge_delay is not None


_T = TypeVar("_T")


def _pick(override: _T | None, fallback: _T) -> _T:
    return override if override is not None else fallback


def resolve(endpoint: EndpointConfig | None, default: HedgeConfig) -> EffectiveConfig:
    """Merge a per-endpoint override onto the transport default.

    For each field, the endpoint's value is used if it is not None,
    otherwise the default's value is used. Cheap enough to call on every
    request rather than caching the result.
    """
    override = endpoint if endpoint is not None else EndpointConfig()
    return EffectiveConfig(
        percentile=_pick(override.percentile, default.percentile),
        max_hedges=_pick(override.max_hedges, default.max_hedges),
        budget_percent=_pick(override.budget_percent, default.budget_percent),
        estimated_rps=_pick(override.estimated_rps, default.estimated_rps),
        rps_window_duration=_pick(
            override.rps_window_duration, default.rps_window_duration
        ),
        min_delay=_pick(override.min_delay, default.min_delay),
        warmup_requests=_pick(override.warmup_requests, default.warmup_requests),
        warmup_delay=_pick(override.warmup_delay, default.warmup_delay),
        window_duration=_pick(override.window_duration, default.window_duration),
        circuit_breaker=_pick(override.circuit_breaker, default.circuit_breaker),
        hedge_delay=override.hedge_delay,
    )
