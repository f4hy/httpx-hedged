"""Shared test fixtures: scripted stub transports and a fake monotonic clock."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

import httpx
import pytest

Behavior = Callable[[httpx.Request], Awaitable[httpx.Response]]


def delayed_response(delay: float, status_code: int = 200) -> Behavior:
    """A behavior that sleeps ``delay`` seconds then returns a response."""

    async def behavior(request: httpx.Request) -> httpx.Response:
        if delay:
            await asyncio.sleep(delay)
        return httpx.Response(status_code, request=request)

    return behavior


def failing(delay: float = 0.0, exc: type[Exception] = RuntimeError) -> Behavior:
    """A behavior that sleeps ``delay`` seconds then raises."""

    async def behavior(request: httpx.Request) -> httpx.Response:
        if delay:
            await asyncio.sleep(delay)
        raise exc("simulated failure")

    return behavior


class ScriptedTransport(httpx.AsyncBaseTransport):
    """Stub transport that pops the next behavior from a script on each call.

    Once the script is exhausted, the last behavior repeats.
    """

    def __init__(self, script: list[Behavior]) -> None:
        self.script = script
        self.calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        index = min(self.calls, len(self.script) - 1)
        behavior = self.script[index]
        self.calls += 1
        return await behavior(request)

    async def aclose(self) -> None:
        return None


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
