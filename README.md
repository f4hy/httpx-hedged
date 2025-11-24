# HTTPX Request Hedging Plugin

An intelligent [httpx](https://www.python-httpx.org/) plugin that implements request hedging. Based on [The Tail at Scale](https://research.google/pubs/pub40801/)


### Basic SLO-Based Hedging

```python
import asyncio
from httpx_hedging_slo import SLOHedgingClient

async def main():
    async with SLOHedgingClient(
        target_slo=1.0,   # 1 second SLO
        hedge_at=0.95,    # Hedge at 95% of SLO (950ms)
        max_hedges=1      # Send 1 additional hedged request
    ) as client:
        response = await client.get("https://api.example.com/data")
        print(response.json())

asyncio.run(main())
```

### Percentile-Based Hedging

For maximum tail latency reduction, hedge at multiple percentiles:

```python
from httpx_hedging_slo import PercentileHedgingClient

async with PercentileHedgingClient(
    target_slo=1.0,
    hedge_points=[0.5, 0.75, 0.95],  # Hedge at p50, p75, p95
) as client:
    response = await client.get("https://api.example.com/data")
```

This sends hedged requests at:
- 500ms (p50 of 1s SLO)
- 750ms (p75 of 1s SLO)
- 950ms (p95 of 1s SLO)

## Configuration

### SLOHedgingTransport / SLOHedgingClient

```python
SLOHedgingClient(
    target_slo=1.0,           # Target latency SLO in seconds
    hedge_at=0.95,            # Hedge at this fraction of SLO (0-1)
    max_hedges=1,             # Maximum hedged requests
    use_adaptive_slo=True,    # Learn per-endpoint SLOs
    cancel_on_success=True,   # Cancel pending requests on success
    **kwargs                  # Standard httpx.AsyncClient args
)
```

### PercentileHedgingTransport / PercentileHedgingClient

```python
PercentileHedgingClient(
    target_slo=1.0,              # Target latency SLO in seconds
    hedge_points=[0.5, 0.75, 0.95],  # Hedge at these percentiles
    use_adaptive_slo=True,       # Learn per-endpoint SLOs
    cancel_on_success=True,      # Cancel pending requests on success
    **kwargs                     # Standard httpx.AsyncClient args
)
```

## How It Works

### Timeline Example

With `target_slo=1.0` and `hedge_at=0.95`:

```
t=0.0s:   Initial request sent
t=0.95s:  Request hasn't completed yet → Hedge #1 sent
t=1.1s:   Hedge #1 completes ✓
          Initial request cancelled
          Response returned to client
```
## Best Practices

### Choosing Your SLO

Set `target_slo` based on your actual service level objectives:

```python
# API with 1s p99 latency requirement
SLOHedgingClient(target_slo=1.0, hedge_at=0.95)

# Low-latency service with 100ms requirement
SLOHedgingClient(target_slo=0.1, hedge_at=0.9)

# Backend service with 500ms target
SLOHedgingClient(target_slo=0.5, hedge_at=0.95)
```

### Choosing hedge_at

The `hedge_at` parameter controls when to hedge as a fraction of the SLO:

- **0.95 (recommended)**: Conservative, hedges near SLO deadline
- **0.90**: Balanced approach
- **0.75**: Aggressive, hedges earlier for maximum protection
- **0.50**: Very aggressive, effectively doubles request rate

### When to Use Adaptive SLO

**Use adaptive SLO (default) when:**
- ✅ Calling multiple different endpoints
- ✅ Latencies vary significantly between endpoints
- ✅ You want automatic optimization
- ✅ You're okay with a learning period

**Disable adaptive SLO when:**
- ❌ You need predictable, consistent hedge timing
- ❌ Making requests to a single endpoint
- ❌ Latencies are very stable
- ❌ You want explicit control

```python
# Disable adaptive for consistent behavior
SLOHedgingClient(
    target_slo=1.0,
    hedge_at=0.95,
    use_adaptive_slo=False
)
```

### Choosing Between Single and Percentile Hedging

**Single Hedge Point** (`SLOHedgingClient`):
- Simpler configuration
- Lower overhead
- Good for most use cases
- Use when: You want protection against tail latency with minimal load increase

**Percentile Hedging** (`PercentileHedgingClient`):
- Maximum tail latency reduction
- Higher request volume
- More complex behavior
- Use when: Tail latency is critical and you can afford the load

## Advanced Usage

### Custom Transport Configuration

```python
import httpx
from httpx_hedging_slo import SLOHedgingTransport

# Configure base transport
base_transport = httpx.AsyncHTTPTransport(
    limits=httpx.Limits(max_connections=100),
    http2=True,
    retries=2
)

# Wrap with SLO hedging
hedging_transport = SLOHedgingTransport(
    transport=base_transport,
    target_slo=0.5,
    hedge_at=0.95,
    max_hedges=2
)

# Use with client
async with httpx.AsyncClient(transport=hedging_transport) as client:
    response = await client.get("https://api.example.com")
```

### Monitoring Learned SLOs

```python
client = SLOHedgingClient(target_slo=1.0, use_adaptive_slo=True)

async with client:
    # Make some requests
    await client.get("https://api.example.com/users")
    await client.get("https://api.example.com/posts")

    # Inspect learned SLOs
    transport = client._transport
    tracker = transport.latency_tracker

    for endpoint, latencies in tracker.latencies.items():
        slo = tracker.get_slo(endpoint, default=1.0)
        print(f"{endpoint}: learned SLO = {slo:.3f}s")
```

### Conditional Hedging

```python
from httpx_hedging_slo import SLOHedgingTransport

class ConditionalSLOHedging(SLOHedgingTransport):
    async def handle_async_request(self, request):
        # Only hedge read operations
        if request.method in ["GET", "HEAD"]:
            return await super().handle_async_request(request)
        else:
            # Skip hedging for writes
            return await self.transport.handle_async_request(request)
```

## References

- [The Tail at Scale](https://research.google/pubs/pub40801/) - Google's paper on tail latency
- [SLO Design Principles](https://sre.google/workbook/implementing-slos/) - Google SRE
- [httpx documentation](https://www.python-httpx.org/)

## License

BSD 3-Clause License. See [LICENSE](LICENSE) file for details.
