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

from collections.abc import Callable
from typing import Protocol

import httpx

from httpx_hedged._config import EffectiveConfig, EndpointConfig, HedgeConfig, resolve
from httpx_hedged._health import HealthRegistry
from httpx_hedged._matcher import EndpointMatcher, Route
from httpx_hedged._scheduler import HedgeScheduler, extract_host
from httpx_hedged._stats import StatsRegistry

_IDEMPOTENT_METHODS = ("GET", "HEAD", "OPTIONS")


def _has_body(request: httpx.Request) -> bool:
    """Whether a request carries a body that a hedge can't safely re-send.

    The primary and hedge both send the same ``httpx.Request`` object; a
    body backed by a one-shot async stream (e.g. ``content=some_generator``)
    would be consumed by whichever of the two reads it first, corrupting or
    failing the other. Idempotent methods essentially never carry a body in
    normal use, so this only ever changes behavior for that edge case.
    """
    content_length = request.headers.get("content-length")
    if content_length not in (None, "0"):
        return True
    return request.headers.get("transfer-encoding", "").lower() == "chunked"


def _resolve_request(
    matcher: EndpointMatcher, default_config: HedgeConfig, request: httpx.Request
) -> tuple[str, str, EffectiveConfig, bool]:
    """Resolve a request to its hedge key, host, effective config, and hedge
    eligibility.

    Pure computation, independent of sync/async, shared by
    ``HedgedTransport`` and ``SyncHedgedTransport``.

    Returns:
        ``(key, host, resolved, can_hedge)``.
    """
    host = extract_host(str(request.url))
    route = matcher.match(request)

    if route is not None:
        key = f"endpoint:{route.name}"
        resolved = resolve(route.config, default_config)
    else:
        key = f"host:{host}"
        resolved = resolve(None, default_config)

    can_hedge = request.method.upper() in _IDEMPOTENT_METHODS and not _has_body(request)
    return key, host, resolved, can_hedge


def _classify_for(resolved: EffectiveConfig) -> Callable[[httpx.Response], bool]:
    """Build the circuit-breaker success classifier for a resolved config.

    Shared by both transports so failure-classification policy lives in
    one place.
    """
    treat_5xx_as_failure = resolved.circuit_breaker.treat_5xx_as_failure

    def classify(response: httpx.Response) -> bool:
        return not (treat_5xx_as_failure and response.status_code >= 500)

    return classify


async def _aclose_response(response: httpx.Response) -> None:
    await response.aclose()


class _SchedulerLike(Protocol):
    def latency_quantile(self, key: str, q: float) -> float | None: ...


class _HedgedTransportCore:
    """Scheduler-independent surface shared by ``HedgedTransport`` and
    ``SyncHedgedTransport``: route registration and the stats / health /
    latency read APIs. Subclasses call ``_init_core()`` from ``__init__``,
    then attach their scheduler and inner transport.
    """

    _scheduler: _SchedulerLike

    def _init_core(
        self,
        default_config: HedgeConfig | None,
        on_circuit_open: Callable[[str, str], None] | None,
        routes: list[Route] | None,
    ) -> None:
        self._default_config = default_config or HedgeConfig()
        self._matcher = EndpointMatcher()
        self._stats = StatsRegistry()
        self._health = HealthRegistry(on_circuit_open=on_circuit_open)
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

        Must be called before request traffic starts: route registration
        has no locking (unlike hedge state), so it's one-time setup, not
        safe to call concurrently with in-flight requests on either
        transport.

        ``path_pattern`` segments may contain ``{name}`` placeholders or a
        bare ``*`` to match any single path segment (e.g.
        ``/api/v1/users/{id}``). Routes are matched in registration order,
        first match wins, so register more specific patterns first.

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

    def latency_quantile(self, key: str, q: float) -> float | None:
        """Return the current estimated latency (seconds) at quantile ``q``
        for a tracked key, or None if nothing has been recorded for that
        key yet.

        ``key`` uses the same ``"endpoint:<name>"`` / ``"host:<hostname>"``
        format as ``stats`` and ``health``. For example, after
        ``name = transport.register("GET", "/search", ...)``, query it with
        ``transport.latency_quantile(f"endpoint:{name}", 0.9)`` for the
        current learned p90::

            p90 = transport.latency_quantile(f"endpoint:{name}", 0.9)
            if p90 is not None:
                print(f"{name} p90: {p90 * 1000:.1f}ms")
        """
        return self._scheduler.latency_quantile(key, q)


class HedgedTransport(_HedgedTransportCore, httpx.AsyncBaseTransport):
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

    _scheduler: HedgeScheduler

    def __init__(
        self,
        inner: httpx.AsyncBaseTransport | None = None,
        default_config: HedgeConfig | None = None,
        routes: list[Route] | None = None,
        on_hedge_fired: Callable[[str], None] | None = None,
        on_circuit_open: Callable[[str, str], None] | None = None,
    ) -> None:
        self._inner = inner or httpx.AsyncHTTPTransport()
        self._init_core(default_config, on_circuit_open, routes)
        self._scheduler = HedgeScheduler(
            self._health,
            self._stats,
            self._default_config.circuit_breaker,
            on_hedge_fired=on_hedge_fired,
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Handle an outgoing request with adaptive, per-endpoint hedging."""
        key, host, resolved, can_hedge = _resolve_request(
            self._matcher, self._default_config, request
        )

        async def do_request() -> httpx.Response:
            return await self._inner.handle_async_request(request)

        return await self._scheduler.execute_with_hedge(
            key=key,
            host=host,
            config=resolved,
            primary_func=do_request,
            hedge_func=do_request,
            classify=_classify_for(resolved),
            can_hedge=can_hedge,
            discard=_aclose_response,
        )

    async def aclose(self) -> None:
        """Close the transport and the wrapped inner transport."""
        self._scheduler.close()
        await self._inner.aclose()
