# Examples

A runnable demo of `HedgedTransport` against a small local backend with
three different latency profiles.

| File | What it is |
|---|---|
| `app.py` | A tiny FastAPI backend exposing `/fast`, `/slow`, and `/flaky` routes, each with a different latency profile. |
| `example_usage.py` | A client that registers per-endpoint hedge config, drives load through `HedgedTransport` (or `SyncHedgedTransport` with `--sync`) against `app.py`, and prints a stats/latency/circuit-breaker report. |
| `run_example.sh` | Installs deps, starts `app.py`, runs `example_usage.py` against it, and tears the server down. The easiest way to see the whole thing end to end. |

## Running it

From the repo root:

```bash
./examples/run_example.sh          # async client (HedgedTransport)
./examples/run_example.sh --sync   # sync client (SyncHedgedTransport)
```

Or run the pieces yourself:

```bash
uv sync --group examples
uv run uvicorn examples.app:app --host 127.0.0.1 --port 8000 &
uv run python examples/example_usage.py          # or --sync
```

Both modes drive identical load (same routes, request counts, and
concurrency — the sync mode uses a thread pool where the async mode uses
`asyncio.gather`) and print the same report at the end.

## What to look for in the output

- `/fast` rarely triggers a hedge, since the primary is almost always
  faster than the learned delay.
- `/slow` hedges often, but the hedge rarely *wins*, because the backup is
  just as slow as the primary on this uniformly slow endpoint.
- `/flaky` is the interesting case: after warmup, the learned p90 sits near
  the fast latency, so the rare slow primary gets hedged against a fresh
  (usually fast) request and the hedge usually wins the race.

The final report prints per-endpoint stats (`total`, `hedged`,
`hedge_wins`, `primary_wins`, ...), each endpoint's learned p90 latency
estimate (via `transport.latency_quantile()`), and each endpoint's circuit
breaker state.
