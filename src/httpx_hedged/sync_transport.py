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

from httpx_hedged._config import HedgeConfig
from httpx_hedged._matcher import Route
from httpx_hedged._scheduler_sync import SyncHedgeScheduler
from httpx_hedged.transport import (
    _classify_for,
    _HedgedTransportCore,
    _resolve_request,
)


def _close_response(response: httpx.Response) -> None:
    response.close()


class SyncHedgedTransport(_HedgedTransportCore, httpx.BaseTransport):
    """An httpx sync transport that adds adaptive, per-endpoint hedged requests.

    Wraps a single inner transport (default: ``httpx.HTTPTransport()``, one
    connection pool) and races a backup request when the primary exceeds
    its estimated latency percentile, or a hardcoded delay, for endpoints
    registered with ``EndpointConfig(hedge_delay=...)``. Functionally
    equivalent to ``HedgedTransport`` (same config, matching, stats, and
    circuit-breaker semantics) for use with ``httpx.Client`` instead of
    ``httpx.AsyncClient``; it's a fully independent instance with its own
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

    _scheduler: SyncHedgeScheduler

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
        self._init_core(default_config, on_circuit_open, routes)
        self._scheduler = SyncHedgeScheduler(
            self._health,
            self._stats,
            self._default_config.circuit_breaker,
            on_hedge_fired=on_hedge_fired,
            max_workers=max_workers,
            executor=executor,
        )

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Handle an outgoing request with adaptive, per-endpoint hedging."""
        key, host, resolved, can_hedge = _resolve_request(
            self._matcher, self._default_config, request
        )

        def do_request() -> httpx.Response:
            return self._inner.handle_request(request)

        return self._scheduler.execute_with_hedge(
            key=key,
            host=host,
            config=resolved,
            primary_func=do_request,
            hedge_func=do_request,
            classify=_classify_for(resolved),
            can_hedge=can_hedge,
            discard=_close_response,
        )

    def close(self) -> None:
        """Close the transport, its thread pool, and the wrapped inner transport.

        Blocks until any orphaned loser thread finishes; see the module
        docstring: this can hang if a loser is blocked on a socket with no
        timeout configured on the inner transport.
        """
        self._scheduler.close()
        self._inner.close()
