"""Configuration for hedge behavior, and default/per-endpoint override resolution."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class CircuitBreakerConfig:
    """Configuration for the health circuit breaker that gates hedging.

    Tripping the breaker only ever suppresses the *hedge* request -- the
    primary request is always sent and its result or exception is always
    returned to the caller normally.

    Attributes:
        error_rate_threshold: Fraction of failed requests (0-1) that trips
            the breaker open.
        min_samples: Minimum number of samples in the current window before
            the breaker is allowed to trip, avoiding trips on tiny samples.
        window_duration: Rolling window size in seconds for the error-rate
            estimate.
        cooldown: Seconds the breaker stays open before allowing a
            half-open trial.
        half_open_max_trial: Number of trial requests allowed through while
            half-open before deciding to close or re-open.
        treat_5xx_as_failure: Whether an HTTP 5xx response counts as a
            failure for circuit-breaker purposes.
    """

    error_rate_threshold: float = 0.5
    min_samples: int = 20
    window_duration: float = 30.0
    cooldown: float = 30.0
    half_open_max_trial: int = 5
    treat_5xx_as_failure: bool = True


@dataclass(slots=True)
class HedgeConfig:
    """Transport-wide default hedge configuration.

    Attributes:
        percentile: Sketch quantile used as hedge trigger (default: 0.90).
        max_hedges: Maximum concurrent hedge requests per call (default: 1).
        budget_percent: Max hedge rate as percent of total traffic (default: 10.0).
        estimated_rps: Expected requests per second. If None (default), the
            rate is estimated automatically from observed traffic instead of
            requiring a manual guess.
        rps_window_duration: Rolling window in seconds for RPS auto-estimation.
        min_delay: Floor on the hedge delay in seconds (default: 0.001).
        warmup_requests: Number of initial requests using fixed delay (default: 20).
        warmup_delay: Fixed hedge delay during warmup in seconds (default: 0.01).
        window_duration: Latency sketch window rotation interval in seconds
            (default: 30.0).
        circuit_breaker: Health circuit-breaker configuration.
    """

    percentile: float = 0.90
    max_hedges: int = 1
    budget_percent: float = 10.0
    estimated_rps: float | None = None
    rps_window_duration: float = 10.0
    min_delay: float = 0.001
    warmup_requests: int = 20
    warmup_delay: float = 0.01
    window_duration: float = 30.0
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)


@dataclass(slots=True)
class EndpointConfig:
    """Per-endpoint override of the transport's default ``HedgeConfig``.

    Every field defaults to None, meaning "inherit the transport default."
    Set ``hedge_delay`` to hardcode a fixed hedge delay for this endpoint
    instead of learning one adaptively from a DDSketch.

    ``circuit_breaker``, when set, replaces the default breaker config as a
    whole object rather than being merged field-by-field -- for a partial
    override, use ``dataclasses.replace(default.circuit_breaker, ...)``.
    """

    hedge_delay: float | None = None
    percentile: float | None = None
    max_hedges: int | None = None
    budget_percent: float | None = None
    estimated_rps: float | None = None
    rps_window_duration: float | None = None
    min_delay: float | None = None
    warmup_requests: int | None = None
    warmup_delay: float | None = None
    window_duration: float | None = None
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


def resolve(endpoint: EndpointConfig | None, default: HedgeConfig) -> EffectiveConfig:
    """Merge a per-endpoint override onto the transport default.

    For each field, the endpoint's value is used if it is not None,
    otherwise the default's value is used. Cheap enough to call on every
    request rather than caching the result.
    """
    if endpoint is None:
        return EffectiveConfig(
            percentile=default.percentile,
            max_hedges=default.max_hedges,
            budget_percent=default.budget_percent,
            estimated_rps=default.estimated_rps,
            rps_window_duration=default.rps_window_duration,
            min_delay=default.min_delay,
            warmup_requests=default.warmup_requests,
            warmup_delay=default.warmup_delay,
            window_duration=default.window_duration,
            circuit_breaker=default.circuit_breaker,
            hedge_delay=None,
        )

    return EffectiveConfig(
        percentile=endpoint.percentile
        if endpoint.percentile is not None
        else default.percentile,
        max_hedges=endpoint.max_hedges
        if endpoint.max_hedges is not None
        else default.max_hedges,
        budget_percent=(
            endpoint.budget_percent
            if endpoint.budget_percent is not None
            else default.budget_percent
        ),
        estimated_rps=(
            endpoint.estimated_rps
            if endpoint.estimated_rps is not None
            else default.estimated_rps
        ),
        rps_window_duration=(
            endpoint.rps_window_duration
            if endpoint.rps_window_duration is not None
            else default.rps_window_duration
        ),
        min_delay=endpoint.min_delay
        if endpoint.min_delay is not None
        else default.min_delay,
        warmup_requests=(
            endpoint.warmup_requests
            if endpoint.warmup_requests is not None
            else default.warmup_requests
        ),
        warmup_delay=endpoint.warmup_delay
        if endpoint.warmup_delay is not None
        else default.warmup_delay,
        window_duration=(
            endpoint.window_duration
            if endpoint.window_duration is not None
            else default.window_duration
        ),
        circuit_breaker=(
            endpoint.circuit_breaker
            if endpoint.circuit_breaker is not None
            else default.circuit_breaker
        ),
        hedge_delay=endpoint.hedge_delay,
    )
