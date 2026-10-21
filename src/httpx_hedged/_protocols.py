"""Structural types for the parts of a request/response/transport the
hedging core uses.

The core (``_core.py``, ``_matcher.py``) never imports an HTTP library, so
the same logic serves both ``httpx`` and ``httpx2``, whose classes are
distinct but share this shape.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, TypeVar


class URLLike(Protocol):
    @property
    def path(self) -> str: ...


class RequestLike(Protocol):
    @property
    def method(self) -> str: ...

    @property
    def url(self) -> URLLike: ...

    @property
    def headers(self) -> Mapping[str, str]: ...

    @property
    def extensions(self) -> Mapping[str, Any]: ...


class ResponseLike(Protocol):
    @property
    def status_code(self) -> int: ...

    def close(self) -> None: ...

    async def aclose(self) -> None: ...


RequestT_contra = TypeVar("RequestT_contra", bound=RequestLike, contravariant=True)
ResponseT_co = TypeVar("ResponseT_co", bound=ResponseLike, covariant=True)


class AsyncInnerTransport(Protocol[RequestT_contra, ResponseT_co]):
    """An async inner transport (its library's ``AsyncBaseTransport``)."""

    async def handle_async_request(self, request: RequestT_contra) -> ResponseT_co: ...

    async def aclose(self) -> None: ...


class SyncInnerTransport(Protocol[RequestT_contra, ResponseT_co]):
    """A sync inner transport (its library's ``BaseTransport``)."""

    def handle_request(self, request: RequestT_contra) -> ResponseT_co: ...

    def close(self) -> None: ...
