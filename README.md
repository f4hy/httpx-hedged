# HTTPX Request Hedging

A [httpx](https://www.python-httpx.org/) plugin that implements request hedging. Based on [The Tail at Scale](https://research.google/pubs/pub40801/)

Example:
```python
import asyncio
import httpx
from httpx_hedged import HedgingTransport

async def main():
    # construct the hedging transport
    transport = HedgingTransport(
        transport=httpx.AsyncHTTPTransport()
        hedging_delay=1.0 # hedge at 1 second
    )
    async with httpx.Client(transport=transport) as client:
        response = await client.get("https://www.httpbin.org/delay/2")
        print(response.json())

asyncio.run(main())
```


## Hedging Transport

Use the `HedgingTransport` to hedge requests at the specified `hedging_delay`.

```python
transport = HedgingTransport(
    transport=httpx.AsyncHTTPTransport()
    hedging_delay=1.0 # hedge at 1 second
)
```

`hedging_delay` also accepts a list of delays if multiple are needed.

```python
transport = HedgingTransport(
    transport = httpx.AsyncHTTPTransport(),
    hedging_delay=[0.250, 0.500, 0.750] # hedge at 250ms, 500ms and 750ms
)
```

## How It Works
Coming Soon

## References

- [The Tail at Scale](https://research.google/pubs/pub40801/) - Google's paper on tail latency
- [SLO Design Principles](https://sre.google/workbook/implementing-slos/) - Google SRE
- [httpx documentation](https://www.python-httpx.org/)
- [gRPC request hedging](https://grpc.io/docs/guides/request-hedging/)

## License

BSD 3-Clause License. See [LICENSE](LICENSE) file for details.

## Develop

Run tests using tox.
```
uv run tox
```
