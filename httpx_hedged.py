"""HTTPX Request Hedging Plugin.

Usage:
    import httpx
    from httpx_hedging import SLOHedgingTransport

    transport = HedgingTransport(
        httpx.AsyncHTTPTransport(),
        hedging_delay=0.95,       # Hedge at 950ms
    )

    client = httpx.AsyncClient(transport=transport)
    response = await client.get("https://example.com")
"""

import asyncio
import logging
from typing import Union

import httpx

logger = logging.getLogger(__name__)




class HedgingTransport(httpx.AsyncBaseTransport):
    """Asynchronous transport wrapper that implements request hedging.

    Args:
        transport: The underlying async httpx transport to wrap
        hedging_delay: Hedge when request is passed the delay(s) (default: 0.95)
    """

    # TODO: Add a way for users to check the response
    # TODO: This class should check the details of the underlying Transport to ensure its not retrying

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport,
        hedging_delay: Union[float, list[float]] = 0.95,
    ):
        self.transport = transport
        # TODO: verify hedging delay is a valid time (greater than 0)
        if isinstance(hedging_delay, float):
            assert hedging_delay > 0.00
            self.hedging_delay = hedging_delay
        else:
            # TODO: assert we have valid heding_delays
            for delay in hedging_delay:
                assert delay > 0.00
            self.hedging_delay = hedging_delay

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

        initial_task = asyncio.create_task(send_request(request))
        tasks.append(initial_task)

        # Create hedging tasks that will be started after delays
        async def delayed_hedge(delay: float):
            """Wait for delay, then send hedged request."""
            await asyncio.sleep(delay)
            return await send_request(request)

        if isinstance(self.hedging_delay, float):
            hedge_task = asyncio.create_task(delayed_hedge(self.hedging_delay))
            tasks.append(hedge_task)
        else:
            for delay in self.hedging_delay:
                hedge_task = asyncio.create_task(delayed_hedge(delay))
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

