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

import httpx

from httpx_hedged._core import SyncHedgingCore


class SyncHedgedTransport(
    SyncHedgingCore[httpx.Request, httpx.Response], httpx.BaseTransport
):
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

    def _default_inner(self) -> httpx.BaseTransport:
        return httpx.HTTPTransport()
