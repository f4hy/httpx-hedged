"""Configuration for hedge behavior, and default/per-endpoint override resolution."""

from __future__ import annotations

from dataclasses import dataclass, field


def _validate_common(config: HedgeConfig | EndpointConfig) -> None:
    """Validate the fields shared by ``HedgeConfig`` and ``EndpointConfig``.

    Maybe we should just use pydantic to get this stuff build in...

    Every check is skipped for a ``None`` value, since ``EndpointConfig``
    fields default to None (meaning "inherit the transport default") and
    are validated for real once ``resolve()`` merges them onto a
    ``HedgeConfig``, which has no ``None`` fields.
    """
    if config.percentile is not None and not 0 < config.percentile < 1:
        raise ValueError(f"percentile must be between 0 and 1, got {config.percentile}")
    if config.budget_percent is not None and config.budget_percent < 0:
        raise ValueError(f"budget_percent must be >= 0, got {config.budget_percent}")
    if config.estimated_rps is not None and config.estimated_rps <= 0:
        raise ValueError(f"estimated_rps must be > 0, got {config.estimated_rps}")
    if config.rps_window_duration is not None and config.rps_window_duration <= 0:
        raise ValueError(
            f"rps_window_duration must be > 0, got {config.rps_window_duration}"
        )
    if config.min_delay is not None and config.min_delay < 0:
        raise ValueError(f"min_delay must be >= 0, got {config.min_delay}")
    if config.warmup_requests is not None and config.warmup_requests < 0:
        raise ValueError(f"warmup_requests must be >= 0, got {config.warmup_requests}")
    if config.warmup_delay is not None and config.warmup_delay < 0:
        raise ValueError(f"warmup_delay must be >= 0, got {config.warmup_delay}")
    if config.window_duration is not None and config.window_duration <= 0:
        raise ValueError(f"window_duration must be > 0, got {config.window_duration}")


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

    def __post_init__(self) -> None:
        if not 0 <= self.error_rate_threshold <= 1:
            raise ValueError(
                "error_rate_threshold must be between 0 and 1, got "
                f"{self.error_rate_threshold}"
            )
        if self.min_samples < 1:
            raise ValueError(f"min_samples must be >= 1, got {self.min_samples}")
        if self.window_duration <= 0:
            raise ValueError(f"window_duration must be > 0, got {self.window_duration}")
        if self.cooldown < 0:
            raise ValueError(f"cooldown must be >= 0, got {self.cooldown}")
        if self.half_open_max_trial < 1:
            raise ValueError(
                f"half_open_max_trial must be >= 1, got {self.half_open_max_trial}"
            )


@dataclass(slots=True)
class HedgeConfig:
    """Transport-wide default hedge configuration."""

    #: Sketch quantile used as hedge trigger.
    percentile: float = 0.90

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

    def __post_init__(self) -> None:
        _validate_common(self)


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

    def __post_init__(self) -> None:
        if self.hedge_delay is not None and self.hedge_delay < 0:
            raise ValueError(f"hedge_delay must be >= 0, got {self.hedge_delay}")
        _validate_common(self)


@dataclass(frozen=True, slots=True)
class EffectiveConfig:
    """Fully-resolved hedge configuration for a single key (no more None fields)."""

    percentile: float
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


def _pick[T](override: T | None, fallback: T) -> T:
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
