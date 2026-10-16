"""Library-neutral hedging core shared by every public transport.

Each public transport is a thin shell that subclasses its own library's
``AsyncBaseTransport`` / ``BaseTransport`` plus ``AsyncHedgingCore`` /
``SyncHedgingCore``, and overrides ``_default_inner`` to supply that
library's default connection pool. The cores do everything else: owning the
inner transport, endpoint registration, the hedge gates, request handling,
and stats/health access. Nothing here imports an HTTP library.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Generic, Protocol, TypeVar

from httpx_hedged._config import EffectiveConfig, EndpointConfig, HedgeConfig, resolve
from httpx_hedged._health import HealthRegistry
from httpx_hedged._matcher import EndpointMatcher, Route
from httpx_hedged._protocols import (
    AsyncInnerTransport,
    RequestLike,
    ResponseLike,
    SyncInnerTransport,
)
from httpx_hedged._scheduler import HedgeScheduler, extract_host
from httpx_hedged._scheduler_sync import SyncHedgeScheduler
from httpx_hedged._stats import StatsRegistry

RequestT = TypeVar("RequestT", bound=RequestLike)
ResponseT = TypeVar("ResponseT", bound=ResponseLike)

_IDEMPOTENT_METHODS = ("GET", "HEAD", "OPTIONS")


def _has_body(request: RequestLike) -> bool:
    """Whether a request carries a body that a hedge can't safely re-send.

    The primary and hedge both send the same request object; a body backed
    by a one-shot stream (e.g. ``content=some_generator``) would be consumed
    by whichever of the two reads it first, corrupting or failing the other.
    Idempotent methods essentially never carry a body in normal use, so this
    only ever changes behavior for that edge case.
    """
    content_length = request.headers.get("content-length")
    if content_length not in (None, "0"):
        return True
    return request.headers.get("transfer-encoding", "").lower() == "chunked"


def _is_upgrade(request: RequestLike) -> bool:
    """Whether a request asks to switch protocols (e.g. a WebSocket handshake).

    A handshake is a plain ``GET``, so it passes the idempotency gate, but a
    hedge would open a second upgraded connection rather than retry a
    response. WebSocket clients such as ``httpx-ws`` send the handshake
    through the client's transport, so it reaches this transport too.
    """
    return "upgrade" in request.headers


def _safe_to_hedge(request: RequestLike) -> bool:
    """Whether sending ``request`` a second time is safe: an idempotent
    method, no body that can't be re-sent, and no protocol upgrade."""
    return (
        request.method.upper() in _IDEMPOTENT_METHODS
        and not _has_body(request)
        and not _is_upgrade(request)
    )


def _classify_for(resolved: EffectiveConfig) -> Callable[[ResponseLike], bool]:
    """Build the circuit-breaker success classifier for a resolved config."""
    treat_5xx_as_failure = resolved.circuit_breaker.treat_5xx_as_failure

    def classify(response: ResponseLike) -> bool:
        return not (treat_5xx_as_failure and response.status_code >= 500)

    return classify


async def _aclose_response(response: ResponseLike) -> None:
    await response.aclose()


def _close_response(response: ResponseLike) -> None:
    response.close()


class _SchedulerLike(Protocol):
    def latency_quantile(self, key: str, q: float) -> float | None: ...


class _HedgingBase(Generic[RequestT, ResponseT]):
    """Scheduler-independent surface shared by the async and sync cores:
    route registration, request resolution, and the stats / health /
    latency read APIs. Subclasses call ``_init_core()`` from ``__init__``,
    then attach their scheduler.
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

    def _resolve(self, request: RequestLike) -> tuple[str, str, EffectiveConfig]:
        """Resolve a request to its hedge key, host, and effective config."""
        host = extract_host(str(request.url))
        route = self._matcher.match(request)
        key = f"endpoint:{route.name}" if route is not None else f"host:{host}"
        resolved = resolve(
            route.config if route is not None else None, self._default_config
        )
        return key, host, resolved


class AsyncHedgingCore(_HedgingBase[RequestT, ResponseT]):
    """Async core: a shell subclasses ``AsyncHedgingCore[<lib>.Request,
    <lib>.Response]`` plus ``<lib>.AsyncBaseTransport`` and overrides
    ``_default_inner``."""

    _scheduler: HedgeScheduler

    def __init__(
        self,
        inner: AsyncInnerTransport[RequestT, ResponseT] | None = None,
        default_config: HedgeConfig | None = None,
        routes: list[Route] | None = None,
        on_hedge_fired: Callable[[str], None] | None = None,
        on_circuit_open: Callable[[str, str], None] | None = None,
    ) -> None:
        self._inner = inner or self._default_inner()
        self._init_core(default_config, on_circuit_open, routes)
        self._scheduler = HedgeScheduler(
            self._health,
            self._stats,
            self._default_config.circuit_breaker,
            on_hedge_fired=on_hedge_fired,
        )

    def _default_inner(self) -> AsyncInnerTransport[RequestT, ResponseT]:
        """The inner transport used when none is passed: the shell's
        library's ``AsyncHTTPTransport()``."""
        raise NotImplementedError

    async def handle_async_request(self, request: RequestT) -> ResponseT:
        """Handle an outgoing request with adaptive, per-endpoint hedging."""
        key, host, resolved = self._resolve(request)

        async def send() -> ResponseT:
            return await self._inner.handle_async_request(request)

        return await self._scheduler.execute_with_hedge(
            key=key,
            host=host,
            config=resolved,
            primary_func=send,
            hedge_func=send,
            classify=_classify_for(resolved),
            can_hedge=_safe_to_hedge(request),
            discard=_aclose_response,
        )

    async def aclose(self) -> None:
        """Close the transport and the wrapped inner transport."""
        self._scheduler.close()
        await self._inner.aclose()


class SyncHedgingCore(_HedgingBase[RequestT, ResponseT]):
    """Sync core: a shell subclasses ``SyncHedgingCore[<lib>.Request,
    <lib>.Response]`` plus ``<lib>.BaseTransport`` and overrides
    ``_default_inner``."""

    _scheduler: SyncHedgeScheduler

    def __init__(
        self,
        inner: SyncInnerTransport[RequestT, ResponseT] | None = None,
        default_config: HedgeConfig | None = None,
        routes: list[Route] | None = None,
        on_hedge_fired: Callable[[str], None] | None = None,
        on_circuit_open: Callable[[str, str], None] | None = None,
        max_workers: int | None = None,
        executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self._inner = inner or self._default_inner()
        self._init_core(default_config, on_circuit_open, routes)
        self._scheduler = SyncHedgeScheduler(
            self._health,
            self._stats,
            self._default_config.circuit_breaker,
            on_hedge_fired=on_hedge_fired,
            max_workers=max_workers,
            executor=executor,
        )

    def _default_inner(self) -> SyncInnerTransport[RequestT, ResponseT]:
        """The inner transport used when none is passed: the shell's
        library's ``HTTPTransport()``."""
        raise NotImplementedError

    def handle_request(self, request: RequestT) -> ResponseT:
        """Handle an outgoing request with adaptive, per-endpoint hedging."""
        key, host, resolved = self._resolve(request)

        def send() -> ResponseT:
            return self._inner.handle_request(request)

        return self._scheduler.execute_with_hedge(
            key=key,
            host=host,
            config=resolved,
            primary_func=send,
            hedge_func=send,
            classify=_classify_for(resolved),
            can_hedge=_safe_to_hedge(request),
            discard=_close_response,
        )

    def close(self) -> None:
        """Close the transport, its thread pool, and the wrapped inner transport.

        Blocks until any orphaned loser thread finishes; see
        ``_scheduler_sync``'s module docstring: this can hang if a loser is
        blocked on a socket with no timeout configured on the inner
        transport.
        """
        self._scheduler.close()
        self._inner.close()
