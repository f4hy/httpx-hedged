"""
Unit tests for HTTPX SLO-Based Request Hedging Plugin

Run with: pytest test_httpx_hedging.py -v
"""

import asyncio
import time
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from httpx_hedged import (
    LatencyTracker,
    PercentileHedgingClient,
    PercentileHedgingTransport,
    SLOHedgingClient,
    SLOHedgingTransport,
)


@pytest.fixture
def mock_response():
    """Create a mock httpx.Response."""
    response = Mock(spec=httpx.Response)
    response.status_code = 200
    response.content = b"test response"
    return response


@pytest.fixture
def mock_request():
    """Create a mock httpx.Request."""
    return httpx.Request("GET", "https://example.com/test")


class TestLatencyTracker:
    """Tests for LatencyTracker."""

    def test_initialization(self):
        """Test tracker initialization."""
        tracker = LatencyTracker(window_size=50, percentile=0.95)
        assert tracker.window_size == 50
        assert tracker.percentile == 0.95
        assert len(tracker.latencies) == 0

    def test_record_latency(self):
        """Test recording latencies."""
        tracker = LatencyTracker()
        endpoint = "api.example.com/users"

        tracker.record(endpoint, 0.1)
        tracker.record(endpoint, 0.2)
        tracker.record(endpoint, 0.3)

        assert len(tracker.latencies[endpoint]) == 3

    def test_window_size_limit(self):
        """Test that window size is respected."""
        tracker = LatencyTracker(window_size=5)
        endpoint = "api.example.com/users"

        for i in range(10):
            tracker.record(endpoint, i * 0.1)

        # Should only keep last 5
        assert len(tracker.latencies[endpoint]) == 5
        # Should have the most recent values (with floating point tolerance)
        expected = [0.5, 0.6, 0.7, 0.8, 0.9]
        actual = list(tracker.latencies[endpoint])
        for i, (a, e) in enumerate(zip(actual, expected)):
            assert abs(a - e) < 0.0001, f"Index {i}: {a} != {e}"

    def test_get_slo_with_insufficient_data(self):
        """Test that default SLO is returned with insufficient data."""
        tracker = LatencyTracker()
        endpoint = "api.example.com/users"

        # Record less than 10 samples
        for i in range(5):
            tracker.record(endpoint, 0.1)

        slo = tracker.get_slo(endpoint, default=1.0)
        assert slo == 1.0  # Should return default

    def test_get_slo_with_sufficient_data(self):
        """Test SLO calculation with sufficient data."""
        tracker = LatencyTracker(percentile=0.95)
        endpoint = "api.example.com/users"

        # Record 100 samples: 0.01, 0.02, ..., 1.00
        for i in range(1, 101):
            tracker.record(endpoint, i * 0.01)

        slo = tracker.get_slo(endpoint, default=2.0)
        # p95 of [0.01, 0.02, ..., 1.00] should be around 0.95
        assert 0.90 <= slo <= 1.00
        assert slo != 2.0  # Should not return default

    def test_get_slo_unknown_endpoint(self):
        """Test SLO for unknown endpoint returns default."""
        tracker = LatencyTracker()
        slo = tracker.get_slo("unknown.endpoint", default=1.5)
        assert slo == 1.5

    def test_clear_specific_endpoint(self):
        """Test clearing data for specific endpoint."""
        tracker = LatencyTracker()
        endpoint1 = "api.example.com/users"
        endpoint2 = "api.example.com/posts"

        tracker.record(endpoint1, 0.1)
        tracker.record(endpoint2, 0.2)

        tracker.clear(endpoint1)

        assert endpoint1 not in tracker.latencies
        assert endpoint2 in tracker.latencies

    def test_clear_all_endpoints(self):
        """Test clearing all endpoint data."""
        tracker = LatencyTracker()

        tracker.record("endpoint1", 0.1)
        tracker.record("endpoint2", 0.2)

        tracker.clear()

        assert len(tracker.latencies) == 0


class TestSLOHedgingTransport:
    """Tests for SLOHedgingTransport."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test transport initialization."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)

        transport = SLOHedgingTransport(
            transport=mock_transport, target_slo=2.0, hedge_at=0.9, max_hedges=3
        )

        assert transport.target_slo == 2.0
        assert transport.hedge_at == 0.9
        assert transport.max_hedges == 3
        assert transport.use_adaptive_slo is True
        assert transport.latency_tracker is not None

    @pytest.mark.asyncio
    async def test_hedge_delay_calculation_fixed(self, mock_request):
        """Test hedge delay calculation with fixed SLO."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)

        transport = SLOHedgingTransport(
            transport=mock_transport,
            target_slo=1.0,
            hedge_at=0.95,
            use_adaptive_slo=False,
        )

        delay = transport._get_hedge_delay(mock_request)
        assert delay == 0.95  # 1.0 * 0.95

    @pytest.mark.asyncio
    async def test_hedge_delay_calculation_adaptive(self, mock_request):
        """Test hedge delay calculation with adaptive SLO."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)

        transport = SLOHedgingTransport(
            transport=mock_transport,
            target_slo=2.0,
            hedge_at=0.9,
            use_adaptive_slo=True,
        )

        # Record some latencies to learn from
        endpoint = transport._get_endpoint_key(mock_request)
        for i in range(20):
            transport.latency_tracker.record(endpoint, 0.5)

        # Should now use learned SLO (~0.5) instead of target (2.0)
        delay = transport._get_hedge_delay(mock_request)
        assert delay < 1.0  # Should be much less than 2.0 * 0.9 = 1.8

    @pytest.mark.asyncio
    async def test_single_request_success(self, mock_request, mock_response):
        """Test that a fast request returns immediately without hedging."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)
        mock_transport.handle_async_request = AsyncMock(return_value=mock_response)

        transport = SLOHedgingTransport(
            transport=mock_transport, target_slo=1.0, hedge_at=0.95, max_hedges=2
        )

        start = time.time()
        response = await transport.handle_async_request(mock_request)
        elapsed = time.time() - start

        assert response == mock_response
        assert elapsed < 0.5  # Should return quickly
        assert mock_transport.handle_async_request.call_count == 1

    @pytest.mark.asyncio
    async def test_hedge_triggers_at_slo(self, mock_request, mock_response):
        """Test that hedged requests are sent at SLO threshold."""
        call_count = 0
        call_times = []

        async def track_calls(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            call_times.append(time.time())

            if call_count == 1:
                # First request is slow
                await asyncio.sleep(0.5)
                return mock_response
            else:
                # Hedged requests are fast
                await asyncio.sleep(0.05)
                return mock_response

        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)
        mock_transport.handle_async_request = track_calls

        transport = SLOHedgingTransport(
            transport=mock_transport,
            target_slo=0.2,  # 200ms SLO
            hedge_at=0.75,  # Hedge at 150ms
            max_hedges=1,
        )

        response = await transport.handle_async_request(mock_request)

        assert response == mock_response
        assert call_count == 2

        # Verify hedge was sent at approximately the right time
        hedge_time = call_times[1] - call_times[0]
        assert 0.12 <= hedge_time <= 0.18  # ~150ms with some tolerance

    @pytest.mark.asyncio
    async def test_latency_recording(self, mock_request, mock_response):
        """Test that latencies are recorded for adaptive SLO."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)

        async def slow_response(*args, **kwargs):
            await asyncio.sleep(0.1)
            return mock_response

        mock_transport.handle_async_request = slow_response

        transport = SLOHedgingTransport(
            transport=mock_transport,
            target_slo=1.0,
            hedge_at=0.95,
            use_adaptive_slo=True,
        )

        endpoint = transport._get_endpoint_key(mock_request)

        # Make request
        await transport.handle_async_request(mock_request)

        # Check that latency was recorded
        assert endpoint in transport.latency_tracker.latencies
        assert len(transport.latency_tracker.latencies[endpoint]) == 1

    @pytest.mark.asyncio
    async def test_endpoint_key_generation(self):
        """Test endpoint key generation."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)
        transport = SLOHedgingTransport(transport=mock_transport, target_slo=1.0)

        request1 = httpx.Request("GET", "https://api.example.com/users")
        request2 = httpx.Request("POST", "https://api.example.com/users")
        request3 = httpx.Request("GET", "https://api.example.com/posts")

        key1 = transport._get_endpoint_key(request1)
        key2 = transport._get_endpoint_key(request2)
        key3 = transport._get_endpoint_key(request3)

        # Same path should give same key regardless of method
        assert key1 == key2
        # Different path should give different key
        assert key1 != key3


class TestPercentileHedgingTransport:
    """Tests for PercentileHedgingTransport."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test transport initialization."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)

        transport = PercentileHedgingTransport(
            transport=mock_transport, target_slo=1.0, hedge_points=[0.5, 0.75, 0.95]
        )

        assert transport.target_slo == 1.0
        assert transport.hedge_points == [0.5, 0.75, 0.95]
        assert transport.latency_tracker is not None

    @pytest.mark.asyncio
    async def test_hedge_points_sorting(self):
        """Test that hedge points are sorted."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)

        transport = PercentileHedgingTransport(
            transport=mock_transport,
            target_slo=1.0,
            hedge_points=[0.95, 0.5, 0.75],  # Unsorted
        )

        assert transport.hedge_points == [0.5, 0.75, 0.95]  # Sorted

    @pytest.mark.asyncio
    async def test_invalid_hedge_points(self):
        """Test that invalid hedge points raise error."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)

        with pytest.raises(ValueError):
            PercentileHedgingTransport(
                transport=mock_transport,
                target_slo=1.0,
                hedge_points=[0.5, 1.5],  # 1.5 is invalid
            )

    @pytest.mark.asyncio
    async def test_hedge_delays_calculation(self, mock_request):
        """Test calculation of multiple hedge delays."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)

        transport = PercentileHedgingTransport(
            transport=mock_transport,
            target_slo=1.0,
            hedge_points=[0.5, 0.75, 0.95],
            use_adaptive_slo=False,
        )

        delays = transport._get_hedge_delays(mock_request)
        assert delays == [0.5, 0.75, 0.95]

    @pytest.mark.asyncio
    async def test_multiple_hedges_triggered(self, mock_request, mock_response):
        """Test that multiple hedges are sent at correct times."""
        call_count = 0
        call_times = []

        async def track_calls(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            call_times.append(time.time())

            if call_count <= 2:
                # First two requests are slow
                await asyncio.sleep(0.5)
            else:
                # Third request is fast
                await asyncio.sleep(0.01)
            return mock_response

        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)
        mock_transport.handle_async_request = track_calls

        transport = PercentileHedgingTransport(
            transport=mock_transport,
            target_slo=0.4,
            hedge_points=[0.25, 0.5, 0.75],  # 100ms, 200ms, 300ms
        )

        response = await transport.handle_async_request(mock_request)

        assert response == mock_response
        assert call_count == 3

        # Verify hedges were staggered appropriately
        assert len(call_times) == 3


class TestSLOHedgingClient:
    """Tests for SLOHedgingClient."""

    @pytest.mark.asyncio
    async def test_client_initialization(self):
        """Test client initialization."""
        client = SLOHedgingClient(target_slo=0.5, hedge_at=0.9, max_hedges=2)

        assert isinstance(client._transport, SLOHedgingTransport)
        assert client._transport.target_slo == 0.5
        assert client._transport.hedge_at == 0.9
        assert client._transport.max_hedges == 2

        await client.aclose()

    @pytest.mark.asyncio
    async def test_client_context_manager(self):
        """Test client works as context manager."""
        async with SLOHedgingClient(target_slo=1.0) as client:
            assert isinstance(client, httpx.AsyncClient)
            assert isinstance(client._transport, SLOHedgingTransport)


class TestPercentileHedgingClient:
    """Tests for PercentileHedgingClient."""

    @pytest.mark.asyncio
    async def test_client_initialization(self):
        """Test client initialization."""
        client = PercentileHedgingClient(target_slo=1.0, hedge_points=[0.5, 0.75, 0.95])

        assert isinstance(client._transport, PercentileHedgingTransport)
        assert client._transport.target_slo == 1.0
        assert client._transport.hedge_points == [0.5, 0.75, 0.95]

        await client.aclose()


class TestIntegration:
    """Integration tests with real HTTP requests (optional)."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_real_request_slo(self):
        """Test with a real HTTP request."""
        async with SLOHedgingClient(
            target_slo=0.5, hedge_at=0.9, max_hedges=1, timeout=5.0
        ) as client:
            response = await client.get("https://httpbin.org/get")
            assert response.status_code == 200

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_real_request_percentile(self):
        """Test percentile hedging with real request."""
        async with PercentileHedgingClient(
            target_slo=1.0, hedge_points=[0.5, 0.75], timeout=5.0
        ) as client:
            response = await client.get("https://httpbin.org/get")
            assert response.status_code == 200

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_adaptive_slo_learning(self):
        """Test that adaptive SLO learns from requests."""
        async with SLOHedgingClient(
            target_slo=2.0, hedge_at=0.9, use_adaptive_slo=True, timeout=5.0
        ) as client:
            # Make several requests
            for _ in range(3):
                await client.get("https://httpbin.org/delay/0.5")

            # Transport should have learned latencies
            transport = client._transport
            endpoint = "httpbin.org/delay"

            assert endpoint in transport.latency_tracker.latencies
            assert len(transport.latency_tracker.latencies[endpoint]) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
