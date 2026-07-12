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

from typing import TYPE_CHECKING

import httpx

from httpx_hedged._config import EndpointConfig, HedgeConfig, resolve
from httpx_hedged._health import HealthRegistry
from httpx_hedged._matcher import EndpointMatcher
from httpx_hedged._scheduler import HedgeScheduler, extract_host
from httpx_hedged._stats import StatsRegistry

if TYPE_CHECKING:
    from collections.abc import Callable

    from httpx_hedged._matcher import Route

_IDEMPOTENT_METHODS = ("GET", "HEAD", "OPTIONS")


class HedgedTransport(httpx.AsyncBaseTransport):
    """An httpx async transport that adds adaptive, per-endpoint hedged requests.

    Wraps a single inner transport (default: ``httpx.AsyncHTTPTransport``,
    one connection pool) and races a backup request when the primary
    exceeds its estimated latency percentile -- or a hardcoded delay, for
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
            actually launched. Intended for metrics -- see the README's
            observability section for an example.
        on_circuit_open: Called as ``on_circuit_open(scope, key)`` each
            time a host- or endpoint-scoped circuit breaker trips open
            (``scope`` is ``"host"`` or ``"endpoint"``). Intended for
            alerting -- see the README's observability section for an
            example.
    """

    def __init__(
        self,
        inner: httpx.AsyncBaseTransport | None = None,
        default_config: HedgeConfig | None = None,
        routes: list[Route] | None = None,
        on_hedge_fired: Callable[[str], None] | None = None,
        on_circuit_open: Callable[[str, str], None] | None = None,
    ) -> None:
        self._inner = inner or httpx.AsyncHTTPTransport()
        self._default_config = default_config or HedgeConfig()
        self._matcher = EndpointMatcher()
        self._stats = StatsRegistry()
        self._health = HealthRegistry(on_circuit_open=on_circuit_open)
        self._scheduler = HedgeScheduler(
            self._health, self._stats, on_hedge_fired=on_hedge_fired
        )

        for route in routes or []:
            self.register(
                route.method, route.path_pattern, route.config, name=route.name
            )

    def register(
        self,
        method: str,
        path_pattern: str,
        config: EndpointConfig,
        *,
        name: str | None = None,
    ) -> str:
        """Register a per-endpoint hedge config for a method + path pattern.

        ``path_pattern`` segments may contain ``{name}`` placeholders or a
        bare ``*`` to match any single path segment (e.g.
        ``/api/v1/users/{id}``). Routes are matched in registration order,
        first match wins -- register more specific patterns first.

        Returns the resolved endpoint name (used as the key in ``stats``
        and as the value for ``extensions={"hedge_endpoint": name}``).
        """
        return self._matcher.register(method, path_pattern, config, name=name)

    @property
    def stats(self) -> StatsRegistry:
        """Per-endpoint and aggregate hedge statistics."""
        return self._stats

    @property
    def health(self) -> HealthRegistry:
        """Host- and endpoint-level circuit breaker state."""
        return self._health

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Handle an outgoing request with adaptive, per-endpoint hedging."""
        host = extract_host(str(request.url))
        route = self._matcher.match(request)

        if route is not None:
            key = f"endpoint:{route.name}"
            resolved = resolve(route.config, self._default_config)
        else:
            key = f"host:{host}"
            resolved = resolve(None, self._default_config)

        can_hedge = request.method.upper() in _IDEMPOTENT_METHODS
        treat_5xx_as_failure = resolved.circuit_breaker.treat_5xx_as_failure

        async def do_request() -> httpx.Response:
            return await self._inner.handle_async_request(request)

        def classify(response: httpx.Response) -> bool:
            return not (treat_5xx_as_failure and response.status_code >= 500)

        return await self._scheduler.execute_with_hedge(
            key=key,
            host=host,
            config=resolved,
            primary_func=do_request,
            hedge_func=do_request,
            classify=classify,
            can_hedge=can_hedge,
        )

    async def aclose(self) -> None:
        """Close the transport and the wrapped inner transport."""
        self._scheduler.close()
        await self._inner.aclose()
