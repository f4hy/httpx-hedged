"""Unit tests for httpx_hedged._options: EndpointConfig/HedgeConfig merge semantics."""

from __future__ import annotations

from httpx_hedged._options import (
    CircuitBreakerConfig,
    EndpointConfig,
    HedgeConfig,
    resolve,
)


def test_no_endpoint_override_uses_default_verbatim() -> None:
    default = HedgeConfig(percentile=0.95, budget_percent=5.0)
    resolved = resolve(None, default)
    assert resolved.percentile == 0.95
    assert resolved.budget_percent == 5.0
    assert resolved.hedge_delay is None
    assert resolved.is_hardcoded is False


def test_partial_override_inherits_unset_fields() -> None:
    default = HedgeConfig(percentile=0.90, warmup_requests=20)
    endpoint = EndpointConfig(percentile=0.99)
    resolved = resolve(endpoint, default)
    assert resolved.percentile == 0.99
    assert resolved.warmup_requests == 20  # inherited


def test_hedge_delay_sets_hardcoded_mode() -> None:
    default = HedgeConfig()
    endpoint = EndpointConfig(hedge_delay=0.05)
    resolved = resolve(endpoint, default)
    assert resolved.hedge_delay == 0.05
    assert resolved.is_hardcoded is True


def test_circuit_breaker_override_replaces_whole_object() -> None:
    default = HedgeConfig(
        circuit_breaker=CircuitBreakerConfig(error_rate_threshold=0.5)
    )
    override = CircuitBreakerConfig(error_rate_threshold=0.2, min_samples=5)
    endpoint = EndpointConfig(circuit_breaker=override)
    resolved = resolve(endpoint, default)
    assert resolved.circuit_breaker is override
    assert resolved.circuit_breaker.error_rate_threshold == 0.2


def test_estimated_rps_none_means_auto_estimate_by_default() -> None:
    default = HedgeConfig()
    resolved = resolve(None, default)
    assert resolved.estimated_rps is None


def test_estimated_rps_explicit_pin_is_preserved() -> None:
    default = HedgeConfig(estimated_rps=None)
    endpoint = EndpointConfig(estimated_rps=42.0)
    resolved = resolve(endpoint, default)
    assert resolved.estimated_rps == 42.0
