"""Drives load through a HedgedTransport against the demo backend (app.py).

Registers one endpoint config per route on app.py, each with a different
latency profile:

    /fast   always fast          -> the hedge trigger should rarely fire
    /slow   always slow (800ms)  -> hedging fires but rarely wins (the
                                    backup is just as slow as the primary)
    /flaky  fast 90% of the time -> the interesting case: after warmup the
                                    learned p90 sits near the fast latency,
                                    so the rare slow primary gets hedged
                                    against a fresh (usually fast) request
                                    and the hedge usually wins the race

Every hedge fire is logged to stdout as it happens (via on_hedge_fired),
and a stats/circuit-breaker report is printed at the end.

Runs against the async HedgedTransport (httpx.AsyncClient) by default;
pass --sync to drive the same load through SyncHedgedTransport
(httpx.Client on a thread pool) instead. The report at the end is the
same either way.

Usage: start examples/app.py first (see its docstring), then:

    uv run python examples/example_usage.py [--sync]

Or just run run_example.sh, which does both (and forwards any flags).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from httpx_hedged import EndpointConfig, HedgedTransport, SyncHedgedTransport

BASE_URL = "http://127.0.0.1:8000"
ROUTES = ["fast", "slow", "flaky"]
REQUESTS_PER_ROUTE = 60
CONCURRENCY = 6

# The hedge token bucket auto-estimates RPS over a rolling ~10s window by
# default, which under-counts traffic sent in a shorter burst like this
# demo's, so budget starves and hedges rarely fire. Pinning estimated_rps
# (roughly concurrency / expected latency) avoids that ramp-up and keeps
# the demo's hedge behavior visible run over run. /fast is left on the
# default auto-estimate since it barely ever needs to hedge anyway.
SLOW_ESTIMATED_RPS = 7.5  # concurrency 6 / ~0.8s latency
FLAKY_ESTIMATED_RPS = 60.0  # concurrency 6 / ~0.1s average latency


def on_hedge_fired(key: str) -> None:
    print(f"  [hedge fired] {key}")


def on_circuit_open(scope: str, key: str) -> None:
    print(f"  [circuit OPEN] {scope}={key}")


async def send_one(
    client: httpx.AsyncClient, route: str, sem: asyncio.Semaphore
) -> None:
    async with sem:
        response = await client.get(f"{BASE_URL}/{route}")
        response.raise_for_status()


async def drive_route(client: httpx.AsyncClient, route: str) -> None:
    print(f"\nSending {REQUESTS_PER_ROUTE} requests to /{route} ...")
    sem = asyncio.Semaphore(CONCURRENCY)
    start = time.perf_counter()
    await asyncio.gather(
        *(send_one(client, route, sem) for _ in range(REQUESTS_PER_ROUTE))
    )
    print(f"  done in {time.perf_counter() - start:.2f}s")


def register_routes(transport: HedgedTransport | SyncHedgedTransport) -> None:
    transport.register("GET", "/fast", EndpointConfig(percentile=0.90), name="fast")
    transport.register(
        "GET",
        "/slow",
        EndpointConfig(percentile=0.90, estimated_rps=SLOW_ESTIMATED_RPS),
        name="slow",
    )
    transport.register(
        "GET",
        "/flaky",
        EndpointConfig(percentile=0.90, estimated_rps=FLAKY_ESTIMATED_RPS),
        name="flaky",
    )


def print_report(transport: HedgedTransport | SyncHedgedTransport) -> None:
    print("\n=== per-endpoint stats ===")
    header = (
        f"{'key':<16} {'total':>6} {'hedged':>7} {'hedge_wins':>11} "
        f"{'primary_wins':>13} {'budget_ex':>10} {'circuit_blk':>12} {'errors':>7}"
    )
    print(header)
    for key, snap in sorted(transport.stats.all_snapshots().items()):
        print(
            f"{key:<16} {snap.total_requests:>6} {snap.hedged_requests:>7} "
            f"{snap.hedge_wins:>11} {snap.primary_wins:>13} "
            f"{snap.budget_exhausted:>10} {snap.circuit_blocked:>12} {snap.errors:>7}"
        )

    global_snap = transport.stats.global_snapshot()
    print(f"\ntotal requests sent: {global_snap.total_requests}")
    print(f"total hedges fired:  {global_snap.hedged_requests}")

    print("\n=== learned p90 latency estimates ===")
    for route in ROUTES:
        key = f"endpoint:{route}"
        p90 = transport.latency_quantile(key, 0.9)
        print(f"{key:<16} {f'{p90 * 1000:.1f}ms' if p90 is not None else 'n/a'}")

    print("\n=== circuit breaker state ===")
    for route in ROUTES:
        key = f"endpoint:{route}"
        state = transport.health.endpoint_state(key)
        print(f"{key:<16} {state.name if state else 'n/a'}")


async def wait_for_server(client: httpx.AsyncClient) -> None:
    try:
        response = await client.get(f"{BASE_URL}/fast", timeout=5.0)
        response.raise_for_status()
    except httpx.HTTPError:
        print(
            f"Could not reach {BASE_URL}: is examples/app.py running?",
            file=sys.stderr,
        )
        sys.exit(1)


async def main() -> None:
    async with httpx.AsyncClient() as plain_client:
        await wait_for_server(plain_client)

    transport = HedgedTransport(
        on_hedge_fired=on_hedge_fired,
        on_circuit_open=on_circuit_open,
    )
    register_routes(transport)

    async with httpx.AsyncClient(transport=transport) as client:
        for route in ROUTES:
            await drive_route(client, route)

    print_report(transport)


def send_one_sync(client: httpx.Client, route: str) -> None:
    response = client.get(f"{BASE_URL}/{route}")
    response.raise_for_status()


def drive_route_sync(client: httpx.Client, route: str) -> None:
    print(f"\nSending {REQUESTS_PER_ROUTE} requests to /{route} ...")
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = [
            pool.submit(send_one_sync, client, route) for _ in range(REQUESTS_PER_ROUTE)
        ]
        for future in futures:
            future.result()
    print(f"  done in {time.perf_counter() - start:.2f}s")


def wait_for_server_sync(client: httpx.Client) -> None:
    try:
        response = client.get(f"{BASE_URL}/fast", timeout=5.0)
        response.raise_for_status()
    except httpx.HTTPError:
        print(
            f"Could not reach {BASE_URL}: is examples/app.py running?",
            file=sys.stderr,
        )
        sys.exit(1)


def main_sync() -> None:
    with httpx.Client() as plain_client:
        wait_for_server_sync(plain_client)

    transport = SyncHedgedTransport(
        on_hedge_fired=on_hedge_fired,
        on_circuit_open=on_circuit_open,
    )
    register_routes(transport)

    with httpx.Client(transport=transport) as client:
        for route in ROUTES:
            drive_route_sync(client, route)

    print_report(transport)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sync",
        action="store_true",
        help="use SyncHedgedTransport with httpx.Client instead of the async transport",
    )
    args = parser.parse_args()
    if args.sync:
        main_sync()
    else:
        asyncio.run(main())
