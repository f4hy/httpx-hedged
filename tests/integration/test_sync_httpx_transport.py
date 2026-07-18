"""Integration tests for SyncHedgedTransport against a stub inner transport."""

from __future__ import annotations

import httpx
import pytest

from httpx_hedged import CircuitBreakerConfig, CircuitState, EndpointConfig, HedgeConfig
from httpx_hedged._config import resolve
from httpx_hedged.sync_transport import SyncHedgedTransport
from tests.conftest import SyncScriptedTransport, sync_delayed_response, sync_failing


def test_passthrough_returns_response_and_records_stats() -> None:
    inner = SyncScriptedTransport([sync_delayed_response(0.0)])
    transport = SyncHedgedTransport(
        inner=inner, default_config=HedgeConfig(min_delay=0.0)
    )
    with httpx.Client(transport=transport) as client:
        resp = client.get("https://api.example.com/anything")
    assert resp.status_code == 200

    global_stats = transport.stats.global_snapshot()
    assert global_stats.total_requests == 1
    transport.close()


def test_two_endpoints_share_one_transport_with_independent_delays() -> None:
    """Regression test for the scenario in the filed hedge-python issue:

    GET /api/v1/fast-lookup (fast, high RPS) and GET /api/v1/bulk-export
    (slow, low RPS) share one SyncHedgedTransport/one inner transport, but
    each endpoint's hedge delay reflects only its own traffic.
    """
    inner = SyncScriptedTransport([sync_delayed_response(0.0)])
    default_config = HedgeConfig(min_delay=0.0, warmup_requests=0)
    transport = SyncHedgedTransport(inner=inner, default_config=default_config)
    transport.register("GET", "/api/v1/fast-lookup", EndpointConfig(percentile=0.9))
    transport.register("GET", "/api/v1/bulk-export", EndpointConfig(percentile=0.9))

    endpoint_config = resolve(EndpointConfig(percentile=0.9), default_config)
    fast_state = transport._scheduler.state_for(
        "endpoint:GET /api/v1/fast-lookup", endpoint_config
    )
    bulk_state = transport._scheduler.state_for(
        "endpoint:GET /api/v1/bulk-export", endpoint_config
    )
    assert fast_state is not bulk_state
    assert fast_state.sketch is not bulk_state.sketch

    for v in range(1, 101):
        fast_state.sketch.add(v / 10_000.0)  # ~0.0001..0.01s (fast-lookup profile)
        bulk_state.sketch.add(v / 10.0)  # ~0.1..10s (bulk-export profile)
    fast_state.counter = bulk_state.counter = 1

    fast_delay = transport._scheduler.compute_hedge_delay(fast_state)
    bulk_delay = transport._scheduler.compute_hedge_delay(bulk_state)

    assert fast_delay < 0.02
    assert bulk_delay > 5.0
    transport.close()


def test_explicit_extension_override_selects_endpoint_config() -> None:
    inner = SyncScriptedTransport([sync_delayed_response(0.0)])
    transport = SyncHedgedTransport(
        inner=inner, default_config=HedgeConfig(min_delay=0.0)
    )
    transport.register(
        "GET", "/does/not/match", EndpointConfig(hedge_delay=0.5), name="pinned"
    )

    request = httpx.Request(
        "GET",
        "https://api.example.com/anything",
        extensions={"hedge_endpoint": "pinned"},
    )
    with httpx.Client(transport=transport) as client:
        resp = client.send(request)
    assert resp.status_code == 200
    snap = transport.stats.snapshot("endpoint:pinned")
    assert snap is not None
    assert snap.total_requests == 1
    transport.close()


def test_unmatched_request_falls_back_to_host_default() -> None:
    inner = SyncScriptedTransport([sync_delayed_response(0.0)])
    transport = SyncHedgedTransport(
        inner=inner, default_config=HedgeConfig(min_delay=0.0)
    )
    with httpx.Client(transport=transport) as client:
        resp = client.get("https://api.example.com/unregistered")
    assert resp.status_code == 200
    snap = transport.stats.snapshot("host:api.example.com")
    assert snap is not None
    assert snap.total_requests == 1
    transport.close()


def test_post_never_hedges_even_when_slow() -> None:
    inner = SyncScriptedTransport([sync_delayed_response(0.05)])
    transport = SyncHedgedTransport(
        inner=inner,
        default_config=HedgeConfig(
            min_delay=0.0, warmup_delay=0.001, warmup_requests=5
        ),
    )
    with httpx.Client(transport=transport) as client:
        resp = client.post("https://api.example.com/anything", json={})
    assert resp.status_code == 200
    assert inner.calls == 1
    snap = transport.stats.snapshot("host:api.example.com")
    assert snap is not None
    assert snap.hedged_requests == 0
    transport.close()


def test_latency_quantile_reflects_traffic_through_the_transport() -> None:
    inner = SyncScriptedTransport([sync_delayed_response(0.01)])
    transport = SyncHedgedTransport(
        inner=inner, default_config=HedgeConfig(min_delay=0.0)
    )
    transport.register("GET", "/api/v1/search", EndpointConfig())

    assert transport.latency_quantile("endpoint:GET /api/v1/search", 0.9) is None

    with httpx.Client(transport=transport) as client:
        client.get("https://api.example.com/api/v1/search")

    p90 = transport.latency_quantile("endpoint:GET /api/v1/search", 0.9)
    assert p90 is not None
    assert p90 > 0
    transport.close()


def test_get_with_streamed_body_never_hedges_even_when_slow() -> None:
    inner = SyncScriptedTransport([sync_delayed_response(0.05)])
    transport = SyncHedgedTransport(
        inner=inner,
        default_config=HedgeConfig(
            min_delay=0.0, warmup_delay=0.001, warmup_requests=5
        ),
    )

    def body() -> object:
        yield b"streamed"

    request = httpx.Request("GET", "https://api.example.com/anything", content=body())
    with httpx.Client(transport=transport) as client:
        resp = client.send(request)
    assert resp.status_code == 200
    # A hedge would have sent the same one-shot stream twice; the gate
    # must have suppressed it, so the inner transport only sees one call.
    assert inner.calls == 1
    snap = transport.stats.snapshot("host:api.example.com")
    assert snap is not None
    assert snap.hedged_requests == 0
    transport.close()


def test_host_breaker_uses_transport_default_not_first_endpoint_touched() -> None:
    inner = SyncScriptedTransport([sync_failing(delay=0.0)])
    aggressive = CircuitBreakerConfig(min_samples=1, error_rate_threshold=0.0)
    transport = SyncHedgedTransport(
        inner=inner, default_config=HedgeConfig(min_delay=0.0)
    )  # default breaker: min_samples=20, so a single failure can't trip it
    transport.register("GET", "/aggressive", EndpointConfig(circuit_breaker=aggressive))

    with httpx.Client(transport=transport) as client:
        with pytest.raises(RuntimeError):
            client.get("https://api.example.com/aggressive")

    # A single failure trips the endpoint breaker (min_samples=1) but must
    # not trip the host breaker: the host tier always uses the
    # transport-wide default config, regardless of which endpoint's
    # override happens to touch the host first.
    assert (
        transport.health.endpoint_state("endpoint:GET /aggressive") is CircuitState.OPEN
    )
    assert transport.health.host_state("api.example.com") is CircuitState.CLOSED
    transport.close()


def test_circuit_breaker_opens_under_failures_and_primary_still_responds() -> None:
    inner = SyncScriptedTransport([sync_failing(delay=0.0)])
    breaker = CircuitBreakerConfig(
        min_samples=2, error_rate_threshold=0.5, cooldown=60.0
    )
    transport = SyncHedgedTransport(
        inner=inner,
        default_config=HedgeConfig(min_delay=0.0, circuit_breaker=breaker),
    )
    with httpx.Client(transport=transport) as client:
        for _ in range(2):
            with pytest.raises(RuntimeError):
                client.get("https://api.example.com/flaky")

    assert transport.health.host_state("api.example.com") is CircuitState.OPEN
    snap = transport.stats.snapshot("host:api.example.com")
    assert snap is not None
    assert snap.errors == 2
    transport.close()


def test_on_circuit_open_callback_fires_with_scope_and_key() -> None:
    events: list[tuple[str, str]] = []
    inner = SyncScriptedTransport([sync_failing(delay=0.0)])
    breaker = CircuitBreakerConfig(
        min_samples=2, error_rate_threshold=0.5, cooldown=60.0
    )
    transport = SyncHedgedTransport(
        inner=inner,
        default_config=HedgeConfig(min_delay=0.0, circuit_breaker=breaker),
        on_circuit_open=lambda scope, key: events.append((scope, key)),
    )
    with httpx.Client(transport=transport) as client:
        for _ in range(2):
            with pytest.raises(RuntimeError):
                client.get("https://api.example.com/flaky")

    # Both the host and the (fallback) endpoint-key breaker share the same
    # traffic here, so both trip and both fire the callback.
    assert ("host", "api.example.com") in events
    assert ("endpoint", "host:api.example.com") in events
    transport.close()


def test_on_hedge_fired_callback_fires_only_when_a_hedge_is_actually_sent() -> None:
    fired: list[str] = []
    inner = SyncScriptedTransport([sync_delayed_response(0.05)])
    transport = SyncHedgedTransport(
        inner=inner,
        default_config=HedgeConfig(min_delay=0.0, warmup_delay=0.001),
        on_hedge_fired=fired.append,
    )
    transport.register("GET", "/fast", EndpointConfig(hedge_delay=1.0))  # never hedges
    transport.register(
        "GET", "/slow", EndpointConfig(hedge_delay=0.001)
    )  # always hedges

    with httpx.Client(transport=transport) as client:
        client.get("https://api.example.com/fast")
        client.get("https://api.example.com/slow")

    assert fired == ["endpoint:GET /slow"]
    transport.close()


def test_close_closes_inner_transport() -> None:
    inner = SyncScriptedTransport([sync_delayed_response(0.0)])
    closed = {"value": False}

    def close() -> None:
        closed["value"] = True

    inner.close = close  # type: ignore[method-assign]
    transport = SyncHedgedTransport(inner=inner)
    transport.close()
    assert closed["value"] is True
