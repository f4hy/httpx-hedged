"""A get-or-create map bounded to a max size, evicting least-recently-used keys.

Per-key state (sketches, breakers, stats) is created lazily, keyed by
endpoint name or a per-host fallback. Nothing ever evicted those keys, so a
caller that talks to high-cardinality hosts (per-tenant subdomains, or -- in
the pathological case -- one key per unique URL when a request has no
parseable host) would grow these maps forever. Bounding them with LRU
eviction keeps memory bounded without requiring callers to know about it.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable

DEFAULT_MAX_SIZE = 10_000


class BoundedRegistry[V]:
    """Not thread-safe by itself -- callers hold their own lock, as with the
    plain dicts this replaces."""

    def __init__(self, max_size: int = DEFAULT_MAX_SIZE) -> None:
        self._max_size = max_size
        self._items: OrderedDict[str, V] = OrderedDict()

    def get_or_create(self, key: str, factory: Callable[[], V]) -> V:
        value = self._items.get(key)
        if value is not None:
            self._items.move_to_end(key)
            return value
        value = factory()
        self._items[key] = value
        if len(self._items) > self._max_size:
            self._items.popitem(last=False)
        return value

    def get(self, key: str) -> V | None:
        value = self._items.get(key)
        if value is not None:
            self._items.move_to_end(key)
        return value

    def items(self) -> list[tuple[str, V]]:
        return list(self._items.items())
