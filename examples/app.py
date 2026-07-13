"""Tiny FastAPI backend with three latency profiles, for the hedging demo.

    GET /fast   always responds in ~5-15ms
    GET /slow   always takes 800ms
    GET /flaky  90% of requests are fast (~5-15ms), 10% take ~900ms

Run directly with uvicorn, e.g.:

    uv run uvicorn examples.app:app --host 127.0.0.1 --port 8000

See run_example.sh for the full demo (starts this server, runs
example_usage.py against it, prints hedge stats, tears the server down).
"""

from __future__ import annotations

import asyncio
import random

from fastapi import FastAPI

app = FastAPI()

FAST_DELAY_RANGE = (0.005, 0.015)
SLOW_DELAY = 0.8
FLAKY_SLOW_PROBABILITY = 0.10
FLAKY_SLOW_DELAY = 0.9


@app.get("/fast")
async def fast() -> dict[str, str]:
    await asyncio.sleep(random.uniform(*FAST_DELAY_RANGE))
    return {"endpoint": "fast"}


@app.get("/slow")
async def slow() -> dict[str, str]:
    await asyncio.sleep(SLOW_DELAY)
    return {"endpoint": "slow"}


@app.get("/flaky")
async def flaky() -> dict[str, str]:
    if random.random() < FLAKY_SLOW_PROBABILITY:
        await asyncio.sleep(FLAKY_SLOW_DELAY)
        return {"endpoint": "flaky", "path": "slow"}
    await asyncio.sleep(random.uniform(*FAST_DELAY_RANGE))
    return {"endpoint": "flaky", "path": "fast"}
