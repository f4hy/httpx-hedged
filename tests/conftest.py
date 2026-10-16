"""Shared test fixtures: scripted stub transports, the HTTP-library backend,
and a fake monotonic clock."""

from __future__ import annotations

import asyncio
import functools
import importlib
import threading
import time
from collections.abc import Awaitable, Callable
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

# A behavior returns the status code to respond with, or raises. The stub
# transports build the response from their ``lib`` (httpx or httpx2).
Behavior = Callable[[Any], Awaitable[int]]
SyncBehavior = Callable[[Any], int]


def delayed_response(delay: float, status_code: int = 200) -> Behavior:
    """A behavior that sleeps ``delay`` seconds then returns a response."""

    async def behavior(request: Any) -> int:
        if delay:
            await asyncio.sleep(delay)
        return status_code

    return behavior


def failing(delay: float = 0.0, exc: type[Exception] = RuntimeError) -> Behavior:
    """A behavior that sleeps ``delay`` seconds then raises."""

    async def behavior(request: Any) -> int:
        if delay:
            await asyncio.sleep(delay)
        raise exc("simulated failure")

    return behavior


class ScriptedTransport:
    """Stub transport that pops the next behavior from a script on each call.

    Once the script is exhausted, the last behavior repeats. Duck-typed
    rather than subclassing either library's ``AsyncBaseTransport`` (clients
    don't type-check their transport); responses are built from ``lib``, so
    it serves both backends.
    """

    def __init__(self, script: list[Behavior], lib: ModuleType) -> None:
        self.script = script
        self.lib = lib
        self.calls = 0

    async def handle_async_request(self, request: Any) -> Any:
        index = min(self.calls, len(self.script) - 1)
        behavior = self.script[index]
        self.calls += 1
        status_code = await behavior(request)
        return self.lib.Response(status_code, request=request)

    async def aclose(self) -> None:
        return None


def sync_delayed_response(delay: float, status_code: int = 200) -> SyncBehavior:
    """A behavior that sleeps ``delay`` seconds then returns a response."""

    def behavior(request: Any) -> int:
        if delay:
            time.sleep(delay)
        return status_code

    return behavior


def sync_failing(
    delay: float = 0.0, exc: type[Exception] = RuntimeError
) -> SyncBehavior:
    """A behavior that sleeps ``delay`` seconds then raises."""

    def behavior(request: Any) -> int:
        if delay:
            time.sleep(delay)
        raise exc("simulated failure")

    return behavior


class SyncScriptedTransport:
    """Sync counterpart to ``ScriptedTransport``.

    Unlike the async version, ``handle_request`` is genuinely called from
    concurrent OS threads (primary + hedge), so bookkeeping needs its own
    lock; the async version gets that for free from the single-threaded
    event loop, and copying its ``self.calls += 1`` here verbatim would be
    a real data race.
    """

    def __init__(self, script: list[SyncBehavior], lib: ModuleType) -> None:
        self.script = script
        self.lib = lib
        self.calls = 0
        self._lock = threading.Lock()

    def handle_request(self, request: Any) -> Any:
        with self._lock:
            index = min(self.calls, len(self.script) - 1)
            behavior = self.script[index]
            self.calls += 1
        return self.lib.Response(behavior(request), request=request)

    def close(self) -> None:
        return None


# Library name -> module holding its HedgedTransport / SyncHedgedTransport.
_BACKENDS = {
    "httpx": "httpx_hedged",
    "httpx2": "httpx_hedged.httpx2",
}


@pytest.fixture(params=sorted(_BACKENDS))
def backend(request: pytest.FixtureRequest) -> SimpleNamespace:
    """The HTTP library under test plus its matching transports and stubs.

    Integration tests take this fixture so each runs once against httpx and
    once against httpx2.
    """
    lib = importlib.import_module(request.param)
    shells = importlib.import_module(_BACKENDS[request.param])
    return SimpleNamespace(
        lib=lib,
        HedgedTransport=shells.HedgedTransport,
        SyncHedgedTransport=shells.SyncHedgedTransport,
        ScriptedTransport=functools.partial(ScriptedTransport, lib=lib),
        SyncScriptedTransport=functools.partial(SyncScriptedTransport, lib=lib),
    )


@pytest.fixture
def fake_clock(monkeypatch: pytest.MonkeyPatch) -> Callable[[float], None]:
    """Monkeypatch ``time.monotonic`` to a controllable fake clock.

    Returns a callable that advances the fake clock by N seconds. All
    modules that call ``time.monotonic()`` share this patched clock, since
    they reference the same ``time`` module object.
    """
    state = {"now": 1_000_000.0}
    monkeypatch.setattr(time, "monotonic", lambda: state["now"])

    def advance(seconds: float) -> None:
        state["now"] += seconds

    return advance
