"""HTTPX Request Hedging Plugin.

Usage:
    import httpx
    from httpx_hedging import SLOHedgingTransport

    transport = SLOHedgingTransport(
        httpx.AsyncHTTPTransport(),
        target_slo=1.0,      # Target 1 second SLO
        hedge_at=0.95,       # Hedge at 95% of SLO (950ms)
        max_hedges=2         # Maximum 2 additional hedged requests
    )

    client = httpx.AsyncClient(transport=transport)
    response = await client.get("https://example.com")
"""

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)


class HedgingTransport(httpx.AsyncBaseTransport):
    """Asynchronous transport wrapper that implements request hedging.

    Args:
        transport: The underlying async httpx transport to wrap
        target_slo: Target latency SLO in seconds (default: 1.0)
        hedge_at: Hedge when request reaches this fraction of SLO (default: 0.95)
    """

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport,
        target_slo: float = 1.0,
        hedge_at: float = 0.95,
    ):
        self.transport = transport
        self.target_slo = target_slo
        self.hedge_at = hedge_at
        self.hedge_delay = self.target_slo * self.hedge_at

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """
        Handle request with SLO-based hedging strategy.

        Sends initial request, then sends additional hedged requests when
        approaching the SLO threshold if the original hasn't completed yet.
        """
        tasks = []

        async def send_request(
            request: httpx.Request,
        ) -> tuple[httpx.Response, int, float]:
            """Send a request and return response with hedge number and start time."""
            response = await self.transport.handle_async_request(request)
            return response

        initial_task = asyncio.create_task(send_request(request, 0))
        tasks.append(initial_task)

        # Create hedging tasks that will be started after delays
        async def delayed_hedge(delay: float):
            """Wait for delay, then send hedged request."""
            await asyncio.sleep(delay)
            return await send_request(request)

        hedge_task = asyncio.create_task(delayed_hedge(self.hedge_delay))
        tasks.append(hedge_task)

        try:
            # Wait for first request to complete
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )

            # Get the first completed response
            completed_task = done.pop()
            response = await completed_task

            # Cancel remaining tasks if configured
            if pending:
                for task in pending:
                    task.cancel()

                # Wait for cancellation to complete
                await asyncio.gather(*pending, return_exceptions=True)

            return response

        except Exception:
            # If error occurs, cancel all pending tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def aclose(self) -> None:
        await self.transport.aclose()


class PercentileHedgingTransport(httpx.AsyncBaseTransport):
    """
    Advanced hedging transport that uses multiple percentile-based hedging points.

    Instead of a single hedge point, this transport hedges at multiple percentiles
    of the target SLO (e.g., at p50, p75, p95) for maximum tail latency reduction.

    Args:
        transport: The underlying async httpx transport to wrap
        target_slo: Target latency SLO in seconds (default: 1.0)
        hedge_points: List of percentiles at which to hedge
    """

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport,
        target_slo: float,
        hedge_points: list[float],
    ):
        self.transport = transport
        self.target_slo = target_slo
        self.hedge_points = hedge_points

        # Validate hedge points
        for point in self.hedge_points:
            if not 0 < point < 1:
                raise ValueError(f"Hedge points must be between 0 and 1, got {point}")

        self.hedge_points = sorted(self.hedge_points)

    def _get_hedge_delays(self, request: httpx.Request) -> list[float]:
        """Calculate hedge delays for a request based on SLO and percentiles.

        Returns a list of delays, one for each hedge point.
        """
        slo = self.target_slo
        return [slo * point for point in self.hedge_points]

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Handle request with multi-percentile hedging strategy."""

        # Calculate hedge delays based on SLO
        hedge_delays = self._get_hedge_delays(request)

        # Track all request tasks
        tasks = []

        async def send_request(
            request: httpx.Request, hedge_number: int
        ) -> tuple[httpx.Response, int]:
            """Send a request and return response with hedge number."""
            response = await self.transport.handle_async_request(request)
            return response, hedge_number

        # Send initial request
        initial_task = asyncio.create_task(send_request(request, 0))
        tasks.append(initial_task)

        # Create hedging tasks for each hedge point
        async def delayed_hedge(delay: float, hedge_num: int):
            """Wait for delay, then send hedged request."""
            await asyncio.sleep(delay)
            return await send_request(request, hedge_num)

        # Schedule hedged requests at each percentile point
        for i, delay in enumerate(hedge_delays, start=1):
            hedge_task = asyncio.create_task(delayed_hedge(delay, i))
            tasks.append(hedge_task)

        try:
            # Wait for first request to complete
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )

            # Get the first completed response
            completed_task = done.pop()
            response, _hedge_number = await completed_task

            if pending:
                for task in pending:
                    task.cancel()

                # Wait for cancellation to complete
                await asyncio.gather(*pending, return_exceptions=True)

            return response

        except Exception:
            # If error occurs, cancel all pending tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def aclose(self) -> None:
        await self.transport.aclose()


class SLOHedgingClient(httpx.AsyncClient):
    """
    Convenience wrapper for httpx.AsyncClient with SLO-based hedging support.

    Example:
        async with SLOHedgingClient(
            target_slo=0.5,
            hedge_at=0.95
        ) as client:
            response = await client.get("https://example.com")
    """

    def __init__(
        self,
        target_slo: float = 1.0,
        hedge_at: float = 0.95,
        max_hedges: int = 1,
        **kwargs,
    ):
        transport = kwargs.pop("transport", httpx.AsyncHTTPTransport())
        hedging_transport = HedgingTransport(
            transport=transport,
            target_slo=target_slo,
            hedge_at=hedge_at,
            max_hedges=max_hedges,
        )
        super().__init__(transport=hedging_transport, **kwargs)


class PercentileHedgingClient(httpx.AsyncClient):
    """Wrapper for httpx.AsyncClient with percentile-based hedging.

    Example:
        async with PercentileHedgingClient(
            target_slo=1.0,
            hedge_points=[0.5, 0.75, 0.95]
        ) as client:
            response = await client.get("https://example.com")
    """

    def __init__(
        self,
        target_slo: float,
        hedge_points: list[float],
        **kwargs,
    ):
        transport = kwargs.pop("transport", httpx.AsyncHTTPTransport())
        hedging_transport = PercentileHedgingTransport(
            transport=transport,
            target_slo=target_slo,
            hedge_points=hedge_points,
        )
        super().__init__(transport=hedging_transport, **kwargs)
