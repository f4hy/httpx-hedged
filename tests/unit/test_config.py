"""Unit tests for httpx_hedged._config: EndpointConfig/HedgeConfig merge semantics."""

from __future__ import annotations

import pytest

from httpx_hedged._config import (
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


# --- validation ----------------------------------------------------------


def test_percentile_out_of_range_is_rejected() -> None:
    with pytest.raises(ValueError, match="percentile"):
        HedgeConfig(percentile=95)  # the "thought it was a percent" mistake
    with pytest.raises(ValueError, match="percentile"):
        EndpointConfig(percentile=1.5)


def test_negative_budget_percent_is_rejected() -> None:
    with pytest.raises(ValueError, match="budget_percent"):
        HedgeConfig(budget_percent=-1.0)


def test_non_positive_estimated_rps_is_rejected() -> None:
    with pytest.raises(ValueError, match="estimated_rps"):
        HedgeConfig(estimated_rps=0.0)


def test_negative_delays_are_rejected() -> None:
    with pytest.raises(ValueError, match="min_delay"):
        HedgeConfig(min_delay=-0.01)
    with pytest.raises(ValueError, match="warmup_delay"):
        HedgeConfig(warmup_delay=-0.01)
    with pytest.raises(ValueError, match="hedge_delay"):
        EndpointConfig(hedge_delay=-0.01)


def test_non_positive_window_durations_are_rejected() -> None:
    with pytest.raises(ValueError, match="window_duration"):
        HedgeConfig(window_duration=0.0)
    with pytest.raises(ValueError, match="rps_window_duration"):
        HedgeConfig(rps_window_duration=0.0)


def test_endpoint_config_none_fields_skip_validation() -> None:
    # Every field defaults to None ("inherit the default"); that must not
    # be rejected as an invalid percentile/delay/etc.
    EndpointConfig()


def test_circuit_breaker_config_validation() -> None:
    with pytest.raises(ValueError, match="error_rate_threshold"):
        CircuitBreakerConfig(error_rate_threshold=1.5)
    with pytest.raises(ValueError, match="min_samples"):
        CircuitBreakerConfig(min_samples=0)
    with pytest.raises(ValueError, match="window_duration"):
        CircuitBreakerConfig(window_duration=0.0)
    with pytest.raises(ValueError, match="cooldown"):
        CircuitBreakerConfig(cooldown=-1.0)
    with pytest.raises(ValueError, match="half_open_max_trial"):
        CircuitBreakerConfig(half_open_max_trial=0)
