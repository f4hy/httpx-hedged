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
import time

import httpx

logger = logging.getLogger(__name__)


class SLOHedgingTransport(httpx.AsyncBaseTransport):
    """
    Asynchronous transport wrapper that implements SLO-based request hedging.

    Hedges requests when they approach their target latency SLO rather than
    using fixed delays. This provides intelligent, adaptive hedging.

    Args:
        transport: The underlying async httpx transport to wrap
        target_slo: Target latency SLO in seconds (default: 1.0)
        hedge_at: Hedge when request reaches this fraction of SLO (default: 0.95)
        max_hedges: Maximum number of hedged requests to send (default: 1)
    """

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport,
        target_slo: float = 1.0,
        hedge_at: float = 0.95,
        max_hedges: int = 1,
    ):
        self.transport = transport
        self.target_slo = target_slo
        self.hedge_at = hedge_at
        self.max_hedges = max_hedges

    def _get_hedge_delay(self, request: httpx.Request) -> float:
        """
        Calculate the hedge delay for a request based on SLO.

        Returns the time to wait before sending a hedged request.
        """
        return self.target_slo * self.hedge_at

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """
        Handle request with SLO-based hedging strategy.

        Sends initial request, then sends additional hedged requests when
        approaching the SLO threshold if the original hasn't completed yet.
        """
        hedge_delay = self._get_hedge_delay(request)

        # Track all request tasks
        tasks = []

        async def send_request(
            request: httpx.Request, hedge_number: int
        ) -> tuple[httpx.Response, int, float]:
            """Send a request and return response with hedge number and start time."""
            req_start = time.time()
            response = await self.transport.handle_async_request(request)
            return response, hedge_number, req_start

        # Send initial request
        initial_task = asyncio.create_task(send_request(request, 0))
        tasks.append(initial_task)

        # Create hedging tasks that will be started after delays
        async def delayed_hedge(delay: float, hedge_num: int):
            """Wait for delay, then send hedged request."""
            await asyncio.sleep(delay)
            return await send_request(request, hedge_num)

        # Schedule hedged requests at multiples of hedge_delay
        for i in range(1, self.max_hedges + 1):
            hedge_task = asyncio.create_task(delayed_hedge(hedge_delay * i, i))
            tasks.append(hedge_task)

        try:
            # Wait for first request to complete
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )

            # Get the first completed response
            completed_task = done.pop()
            response, _hedge_number, _req_start = await completed_task

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
        hedge_points: List of percentiles at which to hedge (default: [0.5, 0.75, 0.95])
    """

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport,
        target_slo: float,
        hedge_points: list[float],
    ):
        self.transport = transport
        self.target_slo = target_slo
        self.hedge_points = hedge_points or [0.5, 0.75, 0.95]

        # Validate hedge points
        for point in self.hedge_points:
            if not 0 < point < 1:
                raise ValueError(f"Hedge points must be between 0 and 1, got {point}")

        # Sort hedge points
        self.hedge_points = sorted(self.hedge_points)

    def _get_endpoint_key(self, request: httpx.Request) -> str:
        """Get a unique key for an endpoint (host + path)."""
        return f"{request.url.host}{request.url.path}"

    def _get_hedge_delays(self, request: httpx.Request) -> list[float]:
        """
        Calculate hedge delays for a request based on SLO and percentiles.

        Returns a list of delays, one for each hedge point.
        """
        slo = self.target_slo
        return [slo * point for point in self.hedge_points]

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """
        Handle request with multi-percentile hedging strategy.
        """
        start_time = time.time()
        endpoint = self._get_endpoint_key(request)

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

            # Record latency for adaptive SLO
            if self.use_adaptive_slo and self.latency_tracker:
                total_latency = time.time() - start_time
                self.latency_tracker.record(endpoint, total_latency)

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
        hedging_transport = SLOHedgingTransport(
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
        use_adaptive_slo: bool = True,
        **kwargs,
    ):
        transport = kwargs.pop("transport", httpx.AsyncHTTPTransport())
        hedging_transport = PercentileHedgingTransport(
            transport=transport,
            target_slo=target_slo,
            hedge_points=hedge_points,
            use_adaptive_slo=use_adaptive_slo,
        )
        super().__init__(transport=hedging_transport, **kwargs)


# Example usage and testing
if __name__ == "__main__":
    import asyncio

    async def test_slo_hedging():
        """Test the SLO-based hedging transport."""
        logger.info("Testing SLO-based hedging client...")

        async with SLOHedgingClient(
            target_slo=0.5,  # 500ms SLO
            hedge_at=0.95,  # Hedge at 475ms
            max_hedges=2,
            timeout=10.0,
        ) as client:
            start = time.time()

            # Test with httpbin delay endpoint (2 second delay)
            response = await client.get("https://httpbin.org/delay/2")

            elapsed = time.time() - start
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Time elapsed: {elapsed:.2f}s")
            logger.info("Hedging triggered at ~0.475s and ~0.950s")

    async def test_percentile_hedging():
        """Test the percentile-based hedging transport."""
        logger.info("\nTesting percentile-based hedging client...")

        async with PercentileHedgingClient(
            target_slo=1.0, hedge_points=[0.5, 0.75, 0.95], timeout=10.0
        ) as client:
            start = time.time()

            response = await client.get("https://httpbin.org/delay/2")

            elapsed = time.time() - start
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Time elapsed: {elapsed:.2f}s")
            logger.info("Hedging triggered at p50, p75, and p95 of 1s SLO")

    # Run the tests
    async def main():
        await test_slo_hedging()
        await test_percentile_hedging()

    asyncio.run(main())
