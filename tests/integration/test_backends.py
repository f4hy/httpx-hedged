"""Tests for the per-library transport shells and optional-dependency handling.

The import-isolation tests run in a subprocess: blocking an import (or
calling ``httpx2.alias_httpx()``) affects the whole process, and both
libraries are already imported by the rest of the suite.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from types import SimpleNamespace

import pytest


def _run(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        check=False,
    )


async def test_async_shell_subclasses_its_library_base_and_defaults_its_pool(
    backend: SimpleNamespace,
) -> None:
    transport = backend.HedgedTransport()
    assert isinstance(transport, backend.lib.AsyncBaseTransport)
    # The inner pool must come from the same library as the client: an
    # httpx2.AsyncClient rejects responses from an httpx pool.
    assert isinstance(transport._inner, backend.lib.AsyncHTTPTransport)
    await transport.aclose()


def test_sync_shell_subclasses_its_library_base_and_defaults_its_pool(
    backend: SimpleNamespace,
) -> None:
    transport = backend.SyncHedgedTransport()
    assert isinstance(transport, backend.lib.BaseTransport)
    assert isinstance(transport._inner, backend.lib.HTTPTransport)
    transport.close()


def test_alias_httpx_lets_the_httpx_shells_serve_httpx2_clients() -> None:
    result = _run(
        """
        import asyncio
        import httpx2

        httpx2.alias_httpx()

        import httpx_hedged

        def ok(request):
            return httpx2.Response(200)

        async def main():
            transport = httpx_hedged.HedgedTransport()
            assert isinstance(transport._inner, httpx2.AsyncHTTPTransport)
            transport = httpx_hedged.HedgedTransport(inner=httpx2.MockTransport(ok))
            async with httpx2.AsyncClient(transport=transport) as client:
                assert (await client.get("https://api.example.com/")).status_code == 200

        asyncio.run(main())

        transport = httpx_hedged.SyncHedgedTransport(inner=httpx2.MockTransport(ok))
        with httpx2.Client(transport=transport) as client:
            assert client.get("https://api.example.com/").status_code == 200
        """
    )
    assert result.returncode == 0, result.stderr


def test_without_httpx_installed_httpx2_shells_still_work() -> None:
    result = _run(
        """
        import sys

        sys.modules["httpx"] = None  # simulate httpx not being installed

        import httpx_hedged
        from httpx_hedged import EndpointConfig, HedgeConfig
        from httpx_hedged.httpx2 import HedgedTransport, SyncHedgedTransport

        HedgedTransport(default_config=HedgeConfig()).register(
            "GET", "/x", EndpointConfig()
        )
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
    assert result.returncode == 0, result.stderr


def test_without_httpx2_installed_httpx2_shell_import_explains_extra() -> None:
    result = _run(
        """
        import sys

        sys.modules["httpx2"] = None  # simulate httpx2 not being installed

        import httpx_hedged

        httpx_hedged.HedgedTransport()
        httpx_hedged.SyncHedgedTransport().close()
        try:
            import httpx_hedged.httpx2
        except ImportError as exc:
            assert "httpx-hedged[httpx2]" in str(exc), exc
        else:
            raise AssertionError("expected ImportError")
        """
    )
    assert result.returncode == 0, result.stderr


def test_unknown_attribute_still_raises_attribute_error() -> None:
    import httpx_hedged

    with pytest.raises(AttributeError):
        _ = httpx_hedged.NotAThing  # type: ignore[attr-defined]
