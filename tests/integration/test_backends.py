"""Behavior that must hold for each supported HTTP library (httpx and httpx2).

The full behavior suites (``test_httpx_transport.py``,
``test_sync_httpx_transport.py``) run against httpx only, since the hedging
logic is shared. These tests drive each library's own client, request, and
response objects through its transports, covering what actually differs per
library: the default connection pool, and the request/response surface the
shared core reads (method, headers, extensions, status code, closing a loser).

The missing-library tests run in a subprocess, since blocking an import
affects the whole process.
"""

from __future__ import annotations

import asyncio
import importlib
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any

import pytest

from httpx_hedged import EndpointConfig, HedgeConfig

_SHELLS = {"httpx": "httpx_hedged", "httpx2": "httpx_hedged.httpx2"}

# A hedge would fire almost immediately for any request allowed to hedge.
_HEDGE_EAGERLY = HedgeConfig(min_delay=0.0, warmup_delay=0.001, warmup_requests=5)


@pytest.fixture(params=sorted(_SHELLS))
def backend(request: pytest.FixtureRequest) -> SimpleNamespace:
    """An HTTP library plus its matching transports."""
    shells = importlib.import_module(_SHELLS[request.param])
    return SimpleNamespace(
        lib=importlib.import_module(request.param),
        HedgedTransport=shells.HedgedTransport,
        SyncHedgedTransport=shells.SyncHedgedTransport,
    )


@pytest.fixture
def server_url() -> Iterator[str]:
    """A local HTTP server that answers every GET with ``200 ok``."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/"
    server.shutdown()
    server.server_close()


def _slow_first_call(lib: Any, delay: float, *, sync: bool) -> Any:
    """A ``lib.MockTransport`` whose first response takes ``delay`` seconds."""
    calls = []

    if sync:

        def handler(request: Any) -> Any:
            calls.append(request)
            if len(calls) == 1:
                time.sleep(delay)
            return lib.Response(200)

    else:

        async def handler(request: Any) -> Any:  # type: ignore[misc]
            calls.append(request)
            if len(calls) == 1:
                await asyncio.sleep(delay)
            return lib.Response(200)

    transport = lib.MockTransport(handler)
    transport.calls = calls
    return transport


async def test_default_pool_serves_the_librarys_async_client(
    backend: SimpleNamespace, server_url: str
) -> None:
    async with backend.lib.AsyncClient(transport=backend.HedgedTransport()) as client:
        resp = await client.get(server_url)
    assert resp.status_code == 200
    assert resp.text == "ok"


def test_default_pool_serves_the_librarys_sync_client(
    backend: SimpleNamespace, server_url: str
) -> None:
    with backend.lib.Client(transport=backend.SyncHedgedTransport()) as client:
        resp = client.get(server_url)
    assert resp.status_code == 200
    assert resp.text == "ok"


async def test_slow_primary_is_hedged_and_hedge_wins(
    backend: SimpleNamespace,
) -> None:
    inner = _slow_first_call(backend.lib, 0.5, sync=False)
    transport = backend.HedgedTransport(inner=inner)
    name = transport.register("GET", "/slow", EndpointConfig(hedge_delay=0.01))
    async with backend.lib.AsyncClient(transport=transport) as client:
        resp = await client.get("https://api.example.com/slow")
    assert resp.status_code == 200
    snap = transport.stats.snapshot(f"endpoint:{name}")
    assert snap is not None
    assert (snap.hedged_requests, snap.hedge_wins) == (1, 1)


def test_slow_primary_is_hedged_and_hedge_wins_sync(
    backend: SimpleNamespace,
) -> None:
    inner = _slow_first_call(backend.lib, 0.2, sync=True)
    transport = backend.SyncHedgedTransport(inner=inner)
    name = transport.register("GET", "/slow", EndpointConfig(hedge_delay=0.01))
    with backend.lib.Client(transport=transport) as client:
        resp = client.get("https://api.example.com/slow")
    assert resp.status_code == 200
    snap = transport.stats.snapshot(f"endpoint:{name}")
    assert snap is not None
    assert (snap.hedged_requests, snap.hedge_wins) == (1, 1)


async def _streamed_body() -> AsyncIterator[bytes]:
    yield b"streamed"


# Each case builds fresh request kwargs, since a streamed body is one-shot.
@pytest.mark.parametrize(
    ("method", "make_kwargs"),
    [
        ("POST", lambda: {"json": {}}),
        ("GET", lambda: {"headers": {"Connection": "Upgrade", "Upgrade": "ws"}}),
        ("GET", lambda: {"content": _streamed_body()}),
    ],
    ids=["post", "upgrade", "streamed-body"],
)
async def test_unsafe_requests_are_never_hedged(
    backend: SimpleNamespace, method: str, make_kwargs: Callable[[], dict[str, Any]]
) -> None:
    inner = _slow_first_call(backend.lib, 0.05, sync=False)
    transport = backend.HedgedTransport(inner=inner, default_config=_HEDGE_EAGERLY)
    async with backend.lib.AsyncClient(transport=transport) as client:
        resp = await client.request(
            method, "https://api.example.com/x", **make_kwargs()
        )
    assert resp.status_code == 200
    assert len(inner.calls) == 1
    snap = transport.stats.snapshot("host:api.example.com")
    assert snap is not None
    assert snap.hedged_requests == 0


async def test_hedge_endpoint_extension_selects_the_named_endpoint(
    backend: SimpleNamespace,
) -> None:
    transport = backend.HedgedTransport(
        inner=backend.lib.MockTransport(lambda request: backend.lib.Response(200))
    )
    transport.register("GET", "/does/not/match", EndpointConfig(), name="pinned")
    request = backend.lib.Request(
        "GET", "https://api.example.com/x", extensions={"hedge_endpoint": "pinned"}
    )
    async with backend.lib.AsyncClient(transport=transport) as client:
        await client.send(request)
    snap = transport.stats.snapshot("endpoint:pinned")
    assert snap is not None
    assert snap.total_requests == 1


def _run(code: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_httpx2_only_install() -> None:
    _run(
        """
        import sys

        sys.modules["httpx"] = None  # simulate httpx not being installed

        from httpx_hedged import *  # noqa: F403
        import httpx_hedged
        from httpx_hedged.httpx2 import HedgedTransport, SyncHedgedTransport

        HedgedTransport()
        SyncHedgedTransport().close()
        for name in ("HedgedTransport", "SyncHedgedTransport"):
            try:
                getattr(httpx_hedged, name)
            except ImportError as exc:
                assert "httpx-hedged[httpx]" in str(exc), exc
            else:
                raise AssertionError(f"expected ImportError for {name}")
        """
    )


def test_httpx_only_install() -> None:
    _run(
        """
        import sys

        sys.modules["httpx2"] = None  # simulate httpx2 not being installed

        from httpx_hedged import *  # noqa: F403

        HedgedTransport()
        SyncHedgedTransport().close()
        try:
            import httpx_hedged.httpx2
        except ImportError as exc:
            assert "httpx-hedged[httpx2]" in str(exc), exc
        else:
            raise AssertionError("expected ImportError")
        """
    )
