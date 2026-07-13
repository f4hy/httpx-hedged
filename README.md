# httpx-hedged

An [httpx](https://www.python-httpx.org/) transport that adds
adaptive, per-endpoint request hedging: fire a backup request when the
primary is running slow, take whichever finishes first, cancel the
loser. Based on Google's [The Tail at
Scale](https://research.google/pubs/pub40801/) and modeled heavily on
[hedge-python](https://github.com/sunhailin-Leo/hedge-python).

## Quick start

```python
import asyncio
import httpx
from httpx_hedged import HedgedTransport

async def main():
    transport = HedgedTransport()
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get("https://api.example.com/data")
        print(response.json())

asyncio.run(main())
```

For a runnable, end-to-end demo (a small backend with different latency
profiles per route, driven by a client that prints a hedge/latency/circuit
breaker report), see [`examples/`](examples/README.md).

With no configuration, `HedgedTransport` learns a p90 latency estimate per
host (via a [DDSketch](https://arxiv.org/abs/2004.08604) quantile sketch)
and fires a hedge request whenever the primary exceeds it. 

## Why per-endpoint?

A single host can host wildly different endpoints. Learning one latency
distribution per *host*, rather than per endpoint, means a handful of
calls to a slow endpoint skew the hedge trigger for a fast one sharing the
same host:

```
GET /api/v1/fast-lookup   median 10ms,  high RPS
GET /api/v1/bulk-export   median 900ms, low RPS
```

`HedgedTransport` lets you register per-endpoint config up front. Each
registered endpoint gets its own latency sketch, rate estimate, and hedge
budget, all still funneled through a **single inner transport and
connection pool**, unlike using `httpx` `mounts={}`, which would mean one
connection pool per pattern:

```python
from httpx_hedged import EndpointConfig, HedgedTransport

transport = HedgedTransport()
transport.register("GET", "/api/v1/fast-lookup", EndpointConfig(percentile=0.90))
transport.register("GET", "/api/v1/bulk-export", EndpointConfig(percentile=0.90))
```

Requests that don't match a registered pattern fall back to a default
config, tracked per host (the same behavior as hedging with no registered
endpoints at all).

Route patterns may contain `{name}` placeholders or a bare `*` for a single
path segment, e.g. `/api/v1/users/{id}`. Patterns are matched in
registration order, so register more specific patterns first.

## Hardcoded vs. adaptive delay

Most endpoints should hedge adaptively, against their own learned
percentile:

```python
transport.register("GET", "/api/v1/search", EndpointConfig(percentile=0.95))
```

For an endpoint where you already know the right delay, or want
deterministic behavior without a warmup period, hardcode it instead:

```python
transport.register("GET", "/api/v1/health", EndpointConfig(hedge_delay=0.05))
```

A hardcoded endpoint still records latency into its sketch for
observability; it just isn't consulted for the hedge-delay decision.

## Explicit endpoint override

Auto-matching not precise enough for a particular call site (or you'd
rather not register a pattern)? Tag the request directly; this bypasses
pattern matching entirely:

```python
await client.get(
    "https://api.example.com/some/path",
    extensions={"hedge_endpoint": "pinned-name"},
)
```

The name must already be registered (`register(..., name="pinned-name")`);
an unknown name raises `UnknownHedgeEndpointError` rather than silently
falling back, so typos fail loudly.

## Configuration

### `HedgeConfig` (transport-wide default)

| Parameter | Type | Default | Description |
|---|---|---|---|
| `percentile` | `float` | `0.90` | Sketch quantile used as the hedge trigger |
| `budget_percent` | `float` | `10.0` | Max hedge rate as percent of total traffic |
| `estimated_rps` | `float \| None` | `None` | Pin the expected requests/sec, or leave `None` to auto-estimate from observed traffic |
| `rps_window_duration` | `float` | `10.0` | Rolling window (seconds) for RPS auto-estimation |
| `min_delay` | `float` | `0.001` | Floor on the hedge delay in seconds |
| `warmup_requests` | `int` | `20` | Requests using a fixed delay before the sketch is trusted |
| `warmup_delay` | `float` | `0.01` | Fixed hedge delay during warmup, in seconds |
| `window_duration` | `float` | `30.0` | Latency sketch rotation interval in seconds |
| `circuit_breaker` | `CircuitBreakerConfig` | see below | Health circuit-breaker configuration |

### `EndpointConfig` (per-registered-endpoint override)

Every field mirrors `HedgeConfig` and defaults to `None`, meaning "inherit
the transport default." One extra field:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `hedge_delay` | `float \| None` | `None` | Hardcode the hedge delay for this endpoint, skipping the sketch for the decision |

### `CircuitBreakerConfig`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `error_rate_threshold` | `float` | `0.5` | Failure fraction that trips the breaker open |
| `min_samples` | `int` | `20` | Minimum samples in-window before the breaker can trip |
| `window_duration` | `float` | `30.0` | Rolling window (seconds) for the error-rate estimate |
| `cooldown` | `float` | `30.0` | Seconds the breaker stays open before a half-open trial |
| `half_open_max_trial` | `int` | `5` | Trial requests allowed through while half-open |
| `treat_5xx_as_failure` | `bool` | `True` | Whether an HTTP 5xx response counts as a failure |

All three config classes validate their fields at construction time (e.g.
`percentile` must be strictly between 0 and 1, delays and windows must be
non-negative/positive) and raise `ValueError` immediately on a bad value,
rather than silently misbehaving later.

## How it works

### Race and cancel

```
              ┌─ primary  ─────────── ✓ (fast) ──→ return
request ──────┤
              └─ hedge fires after estimated delay ─── ✗ (cancelled)
```

Only idempotent methods (`GET`, `HEAD`, `OPTIONS`) are ever hedged, to avoid
duplicating side effects. A request with a body is also never hedged, even
if the method is idempotent: the primary and hedge send the same
`httpx.Request` object, and a body backed by a one-shot stream can't be
safely read twice.

### DDSketch quantile estimator

Each tracked key (an endpoint, or the per-host fallback) gets its own
sliding-window DDSketch pair that rotates every `window_duration` seconds.
DDSketch gives relative-error quantile guarantees regardless of the
underlying latency distribution's shape. The sketch itself comes from the
[`ddsketch`](https://github.com/DataDog/sketches-py) package (DataDog's
reference implementation of the paper below) by default. If the optional
[`rddsketch`](https://github.com/f4hy/rddsketch) package (a Rust-backed,
near-drop-in reimplementation, Python 3.11+) is installed — e.g. via
`pip install httpx-hedged[rust]` — it's used instead, transparently, for
faster inserts and lower memory use per sketch.

### Token bucket budget

Hedges are rate-limited by a token bucket refilling at
`estimated_rps * budget_percent / 100` tokens/second, per key. During a
genuine outage the bucket drains and hedging stops automatically,
preventing the load-doubling spiral that would deepen the incident. By
default the RPS feeding this calculation is estimated automatically from
observed traffic per key, rather than requiring a manual guess per
endpoint.

## Circuit breaker

A closed / open / half-open circuit breaker tracks request success/failure
at **two independent tiers**: one breaker per host, one breaker per
endpoint key. Either tripping open suppresses hedging for its scope: a
host-level trip disables hedging for every endpoint on that host, while an
endpoint-level trip disables hedging only for that one endpoint.

Crucially, the breaker **only ever suppresses the hedge request**. The
primary request is always sent, and its result or exception is always
returned to the caller normally. This is not a request-blocking circuit
breaker, so hedging can't pile extra load onto a backend that's already
struggling.

```
CLOSED ──(error rate ≥ threshold, samples ≥ min_samples)──▶ OPEN
OPEN ──(cooldown elapsed)──▶ HALF_OPEN
HALF_OPEN ──(trial requests mostly succeed)──▶ CLOSED
HALF_OPEN ──(trial requests mostly fail)────▶ OPEN
```

Note: health is recorded from the *winning* task's outcome only. A
cancelled loser's real outcome is unknowable, and losers are cancelled
deliberately (not doing so would defeat the point of reducing load on a
struggling backend).

## Observability

### Polling stats and health snapshots

```python
transport = HedgedTransport()

# ... after running some traffic ...
for key, snap in transport.stats.all_snapshots().items():
    print(key, snap)

print(transport.stats.global_snapshot())
print(transport.health.host_state("api.example.com"))
```

`StatsSnapshot` reports `total_requests`, `hedged_requests`, `hedge_wins`,
`primary_wins`, `budget_exhausted`, `warmup_requests`, `circuit_blocked`,
and `errors` per key, plus a global aggregate.

To see the learned latency estimate itself (e.g. the current p90 driving
the hedge trigger), query `latency_quantile()` with the same key format:

```python
name = transport.register("GET", "/api/v1/search", EndpointConfig(percentile=0.90))
# ... after running some traffic ...
p90 = transport.latency_quantile(f"endpoint:{name}", 0.9)
if p90 is not None:
    print(f"p90: {p90 * 1000:.1f}ms")
```

Returns `None` if the key hasn't recorded any samples yet. You can query
any quantile, not just the one driving the hedge decision.

### Push-based hooks: metrics and alerting

Polling snapshots works for dashboards, but alerting on a circuit-breaker
trip and emitting a metric on every hedge fire are both things you want to
happen *at the moment they occur*, not on the next poll. `HedgedTransport`
takes two optional callbacks for exactly this:

```python
import logging

import httpx
from httpx_hedged import EndpointConfig, HedgeConfig, HedgedTransport

logger = logging.getLogger("myapp.hedging")


def emit_hedge_fired_metric(key: str) -> None:
    statsd_client.incr("http.hedge.fired", tags=[f"endpoint:{key}"])


def alert_on_circuit_open(scope: str, key: str) -> None:
    # scope is "host" or "endpoint"; key is the host name or endpoint key
    # that tripped. Fires exactly once per OPEN transition.
    logger.error(
        "hedging circuit breaker OPEN: %s=%s is unhealthy, hedging suspended", scope, key
    )


transport = HedgedTransport(
    default_config=HedgeConfig(),
    on_hedge_fired=emit_hedge_fired_metric,
    on_circuit_open=alert_on_circuit_open,
)

transport.register("GET", "/api/v1/fast-lookup", EndpointConfig(percentile=0.90))
transport.register("GET", "/api/v1/bulk-export", EndpointConfig(percentile=0.90))

async with httpx.AsyncClient(transport=transport) as client:
    ...
```

`on_hedge_fired` is called with the key each time a hedge request is
actually launched, after the idempotency, circuit-breaker, and budget
gates have all passed, so it only fires for hedges that were genuinely
sent. `on_circuit_open` is called once per OPEN transition (not on every
suppressed hedge while it stays open), so it's safe to wire straight into
an alerting/paging pipeline without flooding it.

Both callbacks run synchronously on the request path, so keep them fast
(increment a counter, log a line); don't do network I/O in them directly.

## Relationship to hedge-python

This library is modeled heavily on
[hedge-python](https://github.com/sunhailin-Leo/hedge-python), which
pioneered the DDSketch-based adaptive-hedging approach this project
borrows. hedge-python keys its sketch per host, which works well for a
single-endpoint-per-host use case; this project exists to add per-endpoint
tracking on top of the same core idea, plus a health circuit breaker,
for services that expose many differently-shaped endpoints on one host. See
the filed [upstream issue](https://github.com/sunhailin-Leo/hedge-python/issues/2)
for the motivating scenario.

## References

- [The Tail at Scale](https://research.google/pubs/pub40801/): Google's paper on tail latency
- [DDSketch: A fast and fully-mergeable quantile sketch with relative-error guarantees](https://arxiv.org/abs/2004.08604): Masson et al., VLDB 2019
- [hedge-python](https://github.com/sunhailin-Leo/hedge-python): the project this one is modeled after
- [httpx documentation](https://www.python-httpx.org/)

## License

MIT License. See [LICENSE](LICENSE) file for details.
