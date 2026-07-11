"""Unit tests for httpx_hedged._matcher: endpoint pattern registration and matching."""

from __future__ import annotations

import httpx
import pytest

from httpx_hedged._matcher import EndpointMatcher, UnknownHedgeEndpointError
from httpx_hedged._options import EndpointConfig


def make_request(
    method: str, path: str, extensions: dict | None = None
) -> httpx.Request:
    return httpx.Request(
        method, f"https://api.example.com{path}", extensions=extensions or {}
    )


def test_exact_path_match() -> None:
    matcher = EndpointMatcher()
    config = EndpointConfig(percentile=0.9)
    matcher.register("GET", "/api/v1/foo", config)

    route = matcher.match(make_request("GET", "/api/v1/foo"))
    assert route is not None
    assert route.config is config
    assert route.name == "GET /api/v1/foo"


def test_no_match_returns_none() -> None:
    matcher = EndpointMatcher()
    matcher.register("GET", "/api/v1/foo", EndpointConfig())
    assert matcher.match(make_request("GET", "/api/v1/bar")) is None


def test_method_must_match() -> None:
    matcher = EndpointMatcher()
    matcher.register("GET", "/api/v1/foo", EndpointConfig())
    assert matcher.match(make_request("POST", "/api/v1/foo")) is None


def test_wildcard_method_matches_any() -> None:
    matcher = EndpointMatcher()
    config = EndpointConfig()
    matcher.register("*", "/api/v1/foo", config)
    assert matcher.match(make_request("POST", "/api/v1/foo")) is not None
    assert matcher.match(make_request("GET", "/api/v1/foo")) is not None


@pytest.mark.parametrize("pattern", ["/api/v1/users/{id}", "/api/v1/users/*"])
def test_single_segment_placeholder_matches_one_segment(pattern: str) -> None:
    matcher = EndpointMatcher()
    matcher.register("GET", pattern, EndpointConfig())
    assert matcher.match(make_request("GET", "/api/v1/users/123")) is not None
    assert matcher.match(make_request("GET", "/api/v1/users/abc")) is not None
    # placeholder is single-segment only
    assert matcher.match(make_request("GET", "/api/v1/users/123/orders")) is None


def test_trailing_slash_is_significant() -> None:
    matcher = EndpointMatcher()
    matcher.register("GET", "/api/v1/foo", EndpointConfig())
    assert matcher.match(make_request("GET", "/api/v1/foo/")) is None


def test_literal_segments_are_escaped() -> None:
    matcher = EndpointMatcher()
    matcher.register("GET", "/api/v1/a.b", EndpointConfig())
    assert matcher.match(make_request("GET", "/api/v1/aXb")) is None
    assert matcher.match(make_request("GET", "/api/v1/a.b")) is not None


def test_first_registered_match_wins() -> None:
    matcher = EndpointMatcher()
    specific = EndpointConfig(percentile=0.5)
    general = EndpointConfig(percentile=0.9)
    matcher.register("GET", "/api/v1/users/self", specific)
    matcher.register("GET", "/api/v1/users/{id}", general)

    route = matcher.match(make_request("GET", "/api/v1/users/self"))
    assert route is not None
    assert route.config is specific


def test_duplicate_name_raises() -> None:
    matcher = EndpointMatcher()
    matcher.register("GET", "/api/v1/foo", EndpointConfig(), name="foo")
    with pytest.raises(ValueError):
        matcher.register("GET", "/api/v1/foo2", EndpointConfig(), name="foo")


def test_extension_override_bypasses_pattern_matching() -> None:
    matcher = EndpointMatcher()
    bulk = EndpointConfig(percentile=0.5)
    matcher.register("GET", "/api/v1/bulk-export", bulk, name="bulk")

    request = make_request(
        "GET", "/some/unrelated/path", extensions={"hedge_endpoint": "bulk"}
    )
    route = matcher.match(request)
    assert route is not None
    assert route.config is bulk


def test_unknown_override_name_raises() -> None:
    matcher = EndpointMatcher()
    request = make_request(
        "GET", "/foo", extensions={"hedge_endpoint": "does-not-exist"}
    )
    with pytest.raises(UnknownHedgeEndpointError):
        matcher.match(request)
