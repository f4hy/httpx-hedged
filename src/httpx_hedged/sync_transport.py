"""Hedged transport for httpx.Client (sync).

Usage::

    import httpx
    from httpx_hedged import EndpointConfig, HedgeConfig, SyncHedgedTransport

    transport = SyncHedgedTransport()
    transport.register("GET", "/api/v1/fast-lookup", EndpointConfig(percentile=0.90))
    transport.register("GET", "/api/v1/bulk-export", EndpointConfig(percentile=0.90))

    with httpx.Client(transport=transport) as client:
        resp = client.get("https://api.example.com/api/v1/fast-lookup")

Same adaptive per-endpoint hedging as ``HedgedTransport``, but races the
primary and hedge requests on a thread pool instead of ``asyncio`` tasks,
since a sync client has no event loop. See ``_scheduler_sync``'s module
docstring for what that implies about loser-thread cleanup and why a
request timeout on the inner transport is load-bearing here.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import httpx

from httpx_hedged._config import EndpointConfig, HedgeConfig
from httpx_hedged._health import HealthRegistry
from httpx_hedged._matcher import EndpointMatcher, Route
from httpx_hedged._scheduler_sync import SyncHedgeScheduler
from httpx_hedged._stats import StatsRegistry
from httpx_hedged.transport import _resolve_request


class SyncHedgedTransport(httpx.BaseTransport):
    """An httpx sync transport that adds adaptive, per-endpoint hedged requests.

    Wraps a single inner transport (default: ``httpx.HTTPTransport()``, one
    connection pool) and races a backup request when the primary exceeds
    its estimated latency percentile, or a hardcoded delay, for endpoints
    registered with ``EndpointConfig(hedge_delay=...)``. Functionally
    equivalent to ``HedgedTransport`` (same config, matching, stats, and
    circuit-breaker semantics) for use with ``httpx.Client`` instead of
    ``httpx.AsyncClient`` — it's a fully independent instance with its own
    state, not one that shares hedge state with an async ``HedgedTransport``
    hitting the same backend.

    Endpoints are identified by registering method + path patterns via
    ``register()``; a request can also be tagged directly with
    ``extensions={"hedge_endpoint": "name"}`` to bypass pattern matching.
    Unmatched requests fall back to a default config scoped per host.

    A circuit breaker independently tracks host-level and endpoint-level
    health; when either trips, hedging is suppressed for that scope (the
    primary request is always still sent and its result/exception is always
    returned normally).

    Since a sync ``httpx.Client`` is commonly shared and called from
    multiple worker threads, unlike ``HedgedTransport``, this races the
    primary and hedge requests on an internal ``ThreadPoolExecutor`` rather
    than ``asyncio`` tasks. See the module docstring for what that changes
    about loser-cleanup timing and why a request timeout matters here.

    Args:
        inner: The underlying transport to wrap. Defaults to a new
            ``httpx.HTTPTransport()``.
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
        max_workers: Size of the internal thread pool used to race primary
            and hedge requests. Ignored if ``executor`` is given. Defaults
            to 200 (2x httpx's own default connection-pool ceiling), to
            leave headroom for orphaned loser requests piling up alongside
            legitimate concurrent primary/hedge work.
        executor: A pre-built ``ThreadPoolExecutor`` to use instead of
            letting this transport create one. Still shut down by
            ``close()`` regardless of who constructed it, matching how
            ``close()`` always closes ``inner`` too.
    """

    def __init__(
        self,
        inner: httpx.BaseTransport | None = None,
        default_config: HedgeConfig | None = None,
        routes: list[Route] | None = None,
        on_hedge_fired: Callable[[str], None] | None = None,
        on_circuit_open: Callable[[str, str], None] | None = None,
        max_workers: int | None = None,
        executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self._inner = inner or httpx.HTTPTransport()
        self._default_config = default_config or HedgeConfig()
        self._matcher = EndpointMatcher()
        self._stats = StatsRegistry()
        self._health = HealthRegistry(on_circuit_open=on_circuit_open)
        self._scheduler = SyncHedgeScheduler(
            self._health,
            self._stats,
            self._default_config.circuit_breaker,
            on_hedge_fired=on_hedge_fired,
            max_workers=max_workers,
            executor=executor,
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

        Must be called before request traffic starts — not safe to call
        concurrently with ``handle_request`` (unlike hedge state, which is
        safe for concurrent multi-threaded access, route registration
        itself has no locking, matching the async transport's same
        one-time-setup assumption).

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
        format as ``stats`` and ``health``.
        """
        return self._scheduler.latency_quantile(key, q)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Handle an outgoing request with adaptive, per-endpoint hedging."""
        key, host, resolved, can_hedge, treat_5xx_as_failure = _resolve_request(
            self._matcher, self._default_config, request
        )

        def do_request() -> httpx.Response:
            return self._inner.handle_request(request)

        def classify(response: httpx.Response) -> bool:
            return not (treat_5xx_as_failure and response.status_code >= 500)

        def discard(response: httpx.Response) -> None:
            response.close()

        return self._scheduler.execute_with_hedge(
            key=key,
            host=host,
            config=resolved,
            primary_func=do_request,
            hedge_func=do_request,
            classify=classify,
            can_hedge=can_hedge,
            discard=discard,
        )

    def close(self) -> None:
        """Close the transport, its thread pool, and the wrapped inner transport.

        Blocks until any orphaned loser thread finishes — see the module
        docstring: this can hang if a loser is blocked on a socket with no
        timeout configured on the inner transport.
        """
        self._scheduler.close()
        self._inner.close()
