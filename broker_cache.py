from __future__ import annotations

import threading
import time
from typing import Callable, TypeVar

try:
    from alpaca.trading.requests import GetOrdersRequest as _GetOrdersRequest
    _HAS_NESTED = True
except ImportError:
    _HAS_NESTED = False

T = TypeVar("T")


class BrokerSnapshotCache:
    """REST snapshot cache TTL-lel és egyszerű single-flight védelemmel."""

    def __init__(self, trading_client):
        self.client = trading_client
        self._lock = threading.Lock()
        self._cache: dict[str, tuple[float, object]] = {}
        self._inflight: dict[str, threading.Event] = {}

    def _get(self, key: str, max_age: float, fetch_fn: Callable[[], T]) -> T:
        while True:
            now = time.monotonic()
            with self._lock:
                entry = self._cache.get(key)
                if entry and (now - entry[0]) <= max_age:
                    return entry[1]  # type: ignore[return-value]

                if key in self._inflight:
                    waiter = self._inflight[key]
                else:
                    waiter = threading.Event()
                    self._inflight[key] = waiter
                    do_fetch = True
                    break

            waiter.wait(timeout=max_age if max_age > 0 else 1.0)

        try:
            value = fetch_fn()
        finally:
            with self._lock:
                waiter = self._inflight.pop(key, None)
                if waiter is not None:
                    waiter.set()

        with self._lock:
            self._cache[key] = (time.monotonic(), value)
        return value

    def get_account(self, max_age_sec: float = 5):
        return self._get("account", max_age_sec, self.client.get_account)

    def get_clock(self, max_age_sec: float = 30):
        return self._get("clock", max_age_sec, self.client.get_clock)

    def get_open_orders(self, max_age_sec: float = 3):
        # [#2-fix] nested=True: bracket child leg-ek a parent .legs mezőjében
        # jelennek meg, nem önálló top-level entitásként.
        def _fetch():
            if _HAS_NESTED:
                try:
                    return self.client.get_orders(_GetOrdersRequest(nested=True))
                except TypeError:
                    pass
            return self.client.get_orders()
        return self._get("orders", max_age_sec, _fetch)

    def get_positions(self, max_age_sec: float = 3):
        return self._get("positions", max_age_sec, self.client.get_all_positions)

    def invalidate_orders(self) -> None:
        with self._lock:
            self._cache.pop("orders", None)

    def invalidate_positions(self) -> None:
        with self._lock:
            self._cache.pop("positions", None)

    def invalidate_account(self) -> None:
        with self._lock:
            self._cache.pop("account", None)

    def invalidate_clock(self) -> None:
        """[#1-fix] Clock cache törlése — hiányzó metódus pótlása.

        A main.py on_bar() handlerből hívja, hogy a következő get_clock()
        biztosan friss adatot kérjen a brokertől, ne a cache-ből.
        """
        with self._lock:
            self._cache.pop("clock", None)
