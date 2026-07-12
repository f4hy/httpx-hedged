"""Endpoint pattern registry: matches requests to per-endpoint hedge config.

hedge-python keys its latency sketch per *host* only. A service with many
routes of very different latency/RPS profiles (e.g. ``GET /api/v1/foo`` vs.
``GET /api/v1/bar``) would have all of their latencies mixed into one
sketch. This module lets callers register per-route ``EndpointConfig``
objects up front, matched against every request's method and path -- all
still funneled through a single ``httpx.AsyncBaseTransport`` / connection
pool, so supporting many endpoints does not multiply connection pools the
way ``httpx`` ``mounts={}`` would.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import httpx

from httpx_hedged._config import EndpointConfig

_SEGMENT_PARAM_RE = re.compile(r"^\{[A-Za-z_]\w*\}$")


class UnknownHedgeEndpointError(KeyError):
    """Raised when a request's ``hedge_endpoint`` extension names an unknown route."""


@dataclass(frozen=True, slots=True)
class Route:
    """A registered method + path pattern and the config it maps to.

    ``path_pattern`` segments may contain ``{name}`` placeholders or a bare
    ``*`` to match any single path segment (e.g. ``/api/v1/users/{id}``).
    Multi-segment globs are not supported.
    """

    method: str
    path_pattern: str
    config: EndpointConfig
    name: str | None = None


def _compile_pattern(path_pattern: str) -> re.Pattern[str]:
    segments = path_pattern.split("/")
    compiled_segments = []
    for segment in segments:
        if segment == "*" or _SEGMENT_PARAM_RE.match(segment):
            compiled_segments.append("[^/]+")
        else:
            compiled_segments.append(re.escape(segment))
    return re.compile("/".join(compiled_segments))


@dataclass(frozen=True, slots=True)
class _CompiledRoute:
    method: str
    regex: re.Pattern[str]
    config: EndpointConfig
    name: str


class EndpointMatcher:
    """Registry of routes, matched in registration order (first match wins)."""

    def __init__(self) -> None:
        self._routes: list[_CompiledRoute] = []
        self._by_name: dict[str, _CompiledRoute] = {}

    def register(
        self,
        method: str,
        path_pattern: str,
        config: EndpointConfig,
        *,
        name: str | None = None,
    ) -> str:
        """Register a route pattern. Returns the resolved endpoint name.

        Raises ValueError if ``name`` (or the default derived name) is
        already registered.
        """
        method = method.upper()
        resolved_name = name if name is not None else f"{method} {path_pattern}"
        if resolved_name in self._by_name:
            raise ValueError(f"endpoint name already registered: {resolved_name!r}")

        compiled = _CompiledRoute(
            method=method,
            regex=_compile_pattern(path_pattern),
            config=config,
            name=resolved_name,
        )
        self._routes.append(compiled)
        self._by_name[resolved_name] = compiled
        return resolved_name

    def match(self, request: httpx.Request) -> Route | None:
        """Match a request to a registered route.

        If the request carries ``extensions["hedge_endpoint"]``, that name
        is looked up directly (bypassing pattern matching); an unknown name
        raises ``UnknownHedgeEndpointError`` rather than silently falling
        back, so typos fail loudly. Otherwise, routes are tried in
        registration order and the first method+path match wins. Returns
        None if nothing matches.
        """
        override = request.extensions.get("hedge_endpoint")
        if override is not None:
            compiled = self._by_name.get(override)
            if compiled is None:
                raise UnknownHedgeEndpointError(override)
            return Route(
                compiled.method, compiled.regex.pattern, compiled.config, compiled.name
            )

        method = request.method.upper()
        path = request.url.path
        for compiled in self._routes:
            if compiled.method not in ("*", method):
                continue
            if compiled.regex.fullmatch(path):
                return Route(
                    compiled.method,
                    compiled.regex.pattern,
                    compiled.config,
                    compiled.name,
                )
        return None
