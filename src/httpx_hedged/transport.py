"""Hedged transport for httpx.AsyncClient.

Usage::

    import httpx
    from httpx_hedged import EndpointConfig, HedgeConfig, HedgedTransport

    transport = HedgedTransport()
    transport.register("GET", "/api/v1/fast-lookup", EndpointConfig(percentile=0.90))
    transport.register("GET", "/api/v1/bulk-export", EndpointConfig(percentile=0.90))

    async with httpx.AsyncClient(transport=transport) as client:
        resp = await client.get("https://api.example.com/api/v1/fast-lookup")
"""

from __future__ import annotations

import httpx

from httpx_hedged._core import AsyncHedgingCore


class HedgedTransport(
    AsyncHedgingCore[httpx.Request, httpx.Response], httpx.AsyncBaseTransport
):
    """An httpx async transport that adds adaptive, per-endpoint hedged requests.

    Wraps a single inner transport (default: ``httpx.AsyncHTTPTransport``,
    one connection pool) and races a backup request when the primary
    exceeds its estimated latency percentile, or a hardcoded delay, for
    endpoints registered with ``EndpointConfig(hedge_delay=...)``.

    Endpoints are identified by registering method + path patterns via
    ``register()``; a request can also be tagged directly with
    ``extensions={"hedge_endpoint": "name"}`` to bypass pattern matching.
    Unmatched requests fall back to a default config scoped per host.

    A circuit breaker independently tracks host-level and endpoint-level
    health; when either trips, hedging is suppressed for that scope (the
    primary request is always still sent and its result/exception is always
    returned normally).

    Args:
        inner: The underlying transport to wrap. Defaults to a new
            ``httpx.AsyncHTTPTransport()``.
        default_config: Hedge configuration used for any request that
            doesn't match a registered endpoint. Defaults to ``HedgeConfig()``.
        routes: Endpoints to register up front (equivalent to calling
            ``register()`` for each one after construction).
        on_hedge_fired: Called with the key each time a hedge request is
            actually launched. Intended for metrics; see the README's
            observability section for an example.
        on_circuit_open: Called as ``on_circuit_open(scope, key)`` each
            time a host- or endpoint-scoped circuit breaker trips open
            (``scope`` is ``"host"`` or ``"endpoint"``). Intended for
            alerting; see the README's observability section for an
            example.
    """

    def _default_inner(self) -> httpx.AsyncBaseTransport:
        return httpx.AsyncHTTPTransport()
