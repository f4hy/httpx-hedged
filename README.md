# HTTPX Request Hedging

A [httpx](https://www.python-httpx.org/) plugin that implements request hedging. Based on [The Tail at Scale](https://research.google/pubs/pub40801/)

### Basic SLO-Based Hedging

```python
import asyncio
from httpx_hedged import HedgingClient

async def main():
    async with HedgingClient(
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
from httpx_hedged import PercentileHedgingClient

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

### HedgingTransport / HedgingClient

```python
HedgingClient(
    target_slo=1.0,           # Target latency SLO in seconds
    hedge_at=0.95,            # Hedge at this fraction of SLO (0-1)
    max_hedges=1,             # Maximum hedged requests
    **kwargs                  # Standard httpx.AsyncClient args
)
```

### PercentileHedgingTransport / PercentileHedgingClient

```python
PercentileHedgingClient(
    target_slo=1.0,              # Target latency SLO in seconds
    hedge_points=[0.5, 0.75, 0.95],  # Hedge at these percentiles
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
## References

- [The Tail at Scale](https://research.google/pubs/pub40801/) - Google's paper on tail latency
- [SLO Design Principles](https://sre.google/workbook/implementing-slos/) - Google SRE
- [httpx documentation](https://www.python-httpx.org/)

## License

BSD 3-Clause License. See [LICENSE](LICENSE) file for details.
