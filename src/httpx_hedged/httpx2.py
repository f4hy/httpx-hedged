"""Hedged transports for httpx2 clients.

Same behavior and API as ``httpx_hedged.HedgedTransport`` /
``httpx_hedged.SyncHedgedTransport``, built on
`httpx2 <https://github.com/pydantic/httpx2>`_ instead of ``httpx``. The
two libraries' classes are distinct (an ``httpx2.AsyncClient`` rejects
responses from an ``httpx`` connection pool), so each gets its own shells
over the shared hedging cores. Install with ``pip install httpx-hedged[httpx2]``.

Usage::

    import httpx2
    from httpx_hedged import EndpointConfig
    from httpx_hedged.httpx2 import HedgedTransport

    transport = HedgedTransport()
    transport.register("GET", "/api/v1/fast-lookup", EndpointConfig(percentile=0.90))

    async with httpx2.AsyncClient(transport=transport) as client:
        resp = await client.get("https://api.example.com/api/v1/fast-lookup")
"""

from __future__ import annotations

try:
    import httpx2
except ImportError as exc:  # pragma: no cover - exercised via subprocess test
    raise ImportError(
        "httpx_hedged.httpx2 requires httpx2; install it with "
        "`pip install httpx-hedged[httpx2]`"
    ) from exc

from httpx_hedged._core import AsyncHedgingCore, SyncHedgingCore

__all__ = ["HedgedTransport", "SyncHedgedTransport"]


class HedgedTransport(
    AsyncHedgingCore[httpx2.Request, httpx2.Response], httpx2.AsyncBaseTransport
):
    """An httpx2 async transport that adds adaptive, per-endpoint hedged requests.

    Identical to ``httpx_hedged.HedgedTransport`` apart from the HTTP
    library: ``inner`` defaults to a new ``httpx2.AsyncHTTPTransport()``
    and must be an httpx2 transport. See that class for the full
    description and arguments.
    """

    def _default_inner(self) -> httpx2.AsyncBaseTransport:
        return httpx2.AsyncHTTPTransport()


class SyncHedgedTransport(
    SyncHedgingCore[httpx2.Request, httpx2.Response], httpx2.BaseTransport
):
    """An httpx2 sync transport that adds adaptive, per-endpoint hedged requests.

    Identical to ``httpx_hedged.SyncHedgedTransport`` apart from the HTTP
    library: ``inner`` defaults to a new ``httpx2.HTTPTransport()`` and
    must be an httpx2 transport. See that class for the full description,
    arguments, and the thread-pool/timeout caveats.
    """

    def _default_inner(self) -> httpx2.BaseTransport:
        return httpx2.HTTPTransport()
