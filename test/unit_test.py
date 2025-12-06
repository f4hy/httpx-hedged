"""
Unit tests for HTTPX SLO-Based Request Hedging Plugin

Run with: pytest test_httpx_hedging.py -v
"""

import asyncio
from os import wait
import time
from unittest.mock import AsyncMock, Mock
from typing import Iterable

import httpx
import pytest

from httpx_hedged import (
    HedgingTransport,
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


class TestHedgingTransport:
    """Tests for HedgingTransport."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test transport initialization."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)

        transport = HedgingTransport(
            transport=mock_transport, hedging_delay=2.0,
        )

        assert transport.hedging_delay == 2.0

    @pytest.mark.parametrize("hedging_delay", [-1.0, [-1.0], [0.0, -1.0]])
    @pytest.mark.asyncio
    async def test_initialization_invalid_hedging_delays(self, hedging_delay):
        """Test transport initialization."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)

        with pytest.raises(AssertionError):
            transport = HedgingTransport(
                transport=mock_transport, hedging_delay=hedging_delay,
            )

    @pytest.mark.asyncio
    async def test_single_request_success(self, mock_request, mock_response):
        """Test that a fast request returns immediately without hedging."""
        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)
        mock_transport.handle_async_request = AsyncMock(return_value=mock_response)

        transport = HedgingTransport(
            transport=mock_transport, hedging_delay=0.95,
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

        transport = HedgingTransport(
            transport=mock_transport,
            hedging_delay=0.150,  # Hedge at 150ms
        )

        response = await transport.handle_async_request(mock_request)

        assert response == mock_response
        assert call_count == 2

        # Verify hedge was sent at approximately the right time
        assert_call_times(call_times, call_times[0], [0.00, 0.150])

    @pytest.mark.asyncio
    async def test_send_request_multiple_hedges(self, mock_request, mock_response):
        """Test an instance with multiple hedges"""
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
            elif call_count == 2:
                # First Hedged request is slow
                await asyncio.sleep(0.5)
                return mock_response
            else:
                # Hedged requests are fast
                await asyncio.sleep(0.05)
                return mock_response

        mock_transport = AsyncMock(spec=httpx.AsyncBaseTransport)
        mock_transport.handle_async_request = track_calls

        transport = HedgingTransport(
            transport=mock_transport,
            hedging_delay=[0.150, 0.200],  # Hedge at 150ms
        )

        response = await transport.handle_async_request(mock_request)

        assert response == mock_response
        assert call_count == 3

        # Verify hedge was sent at approximately the right time
        assert_call_times(call_times, call_times[0], [0.00, 0.150, 0.200])


def assert_call_times(call_times: list[float], start_time: float, expected_call_times: list[float]):
    """Assert call times are within 10% of the expected call time"""
    # assert the number of calls is the same
    expected_call_count = len(expected_call_times)
    assert len(call_times) == expected_call_count
    for actual_call_time, expected_call_duration in zip(call_times, expected_call_times):
        actual_duration = actual_call_time - start_time
        lower_bound = expected_call_duration * 0.9
        upper_bound = expected_call_duration * 1.1
        assert lower_bound <= actual_duration <= upper_bound
