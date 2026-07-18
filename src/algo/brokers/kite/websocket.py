"""KiteTicker wrapper for the broker's order-update push connection.

Scope: this wraps KiteTicker specifically for ``on_order_update`` -- the
real-time order-fill notifications ``BrokerBase``'s websocket lifecycle
exposes. Market-data *tick* streaming (which KiteTicker physically multiplexes
over the same socket) is a separate concern owned by
``market_data/websocket_manager.py``; keeping the two apart at this layer is
deliberate, matching the BrokerBase contract.

Concurrency: KiteTicker runs its socket on its own background thread
(``connect(threaded=True)``) and invokes ``on_order_update`` from that thread.
This wrapper therefore protects its callback list with a lock, and never holds
that lock while invoking a callback (callback code may re-enter the broker).
The KiteTicker itself is dependency-injected via a factory, so the whole thing
is unit-testable -- a fake ticker drives the same callback path a real one
would, with no network.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

from algo.brokers.kite.mapper import KiteMappingError, to_broker_order

if TYPE_CHECKING:
    from algo.brokers.broker_base import BrokerOrder, OrderUpdateCallback


class KiteTickerProtocol(Protocol):
    """The subset of KiteTicker this wrapper drives. Declared so a fake ticker
    can be injected in tests without importing the real one."""

    on_order_update: Any
    on_connect: Any
    on_close: Any
    on_error: Any

    def connect(self, threaded: bool = ..., **kwargs: Any) -> None: ...

    def close(self, *args: Any, **kwargs: Any) -> None: ...

    def is_connected(self) -> bool: ...


# A factory that builds a fresh KiteTicker (Kite requires a new ticker per
# connection); injected so tests supply a fake-ticker factory.
KiteTickerFactory = Callable[[], KiteTickerProtocol]


class KiteOrderUpdateStream:
    """Manages the order-update websocket: connect/disconnect lifecycle,
    connection status, and fan-out of mapped ``BrokerOrder`` updates to every
    registered callback.
    """

    def __init__(
        self, *, ticker_factory: KiteTickerFactory, logger: logging.Logger | None = None
    ) -> None:
        self._ticker_factory = ticker_factory
        self._logger = logger if logger is not None else logging.getLogger("algo.brokers.kite.ws")
        self._ticker: KiteTickerProtocol | None = None
        self._connected = False
        self._lock = threading.Lock()
        self._callbacks_lock = threading.Lock()
        self._callbacks: list[OrderUpdateCallback] = []

    # -- Lifecycle -----------------------------------------------------

    def connect(self) -> None:
        """Open the order-update connection. Idempotent: a no-op if already
        connected. Builds a fresh ticker, wires its callbacks, and starts it on
        its own background thread."""
        with self._lock:
            if self._ticker is not None:
                return
            ticker = self._ticker_factory()
            ticker.on_order_update = self._on_order_update
            ticker.on_connect = self._on_connect
            ticker.on_close = self._on_close
            ticker.on_error = self._on_error
            self._ticker = ticker
            ticker.connect(threaded=True)

    def disconnect(self) -> None:
        """Close the order-update connection. Idempotent: a no-op if not
        connected."""
        with self._lock:
            ticker = self._ticker
            self._ticker = None
            self._connected = False
        if ticker is not None:
            try:
                ticker.close()
            except Exception:  # noqa: BLE001 -- teardown must not raise
                self._logger.warning("error closing Kite order-update ticker", exc_info=True)

    def is_connected(self) -> bool:
        """Cheap local view of connection status -- no I/O."""
        return self._connected

    def register_callback(self, callback: OrderUpdateCallback) -> None:
        """Add a callback invoked (fan-out) with every order update. Multiple
        callbacks are supported; registration appends, never replaces."""
        with self._callbacks_lock:
            self._callbacks.append(callback)

    # -- KiteTicker event handlers (called from the ticker's thread) ---

    def _on_order_update(self, ws: Any, data: dict[str, Any]) -> None:
        """Map a Kite order postback to a BrokerOrder and fan it out. A single
        malformed update is logged and dropped rather than allowed to kill the
        ticker thread."""
        try:
            order = to_broker_order(data)
        except KiteMappingError:
            self._logger.warning("dropping unmappable Kite order update: %r", data, exc_info=True)
            return
        self._dispatch(order)

    def _on_connect(self, ws: Any, response: Any) -> None:
        self._connected = True
        self._logger.info("Kite order-update websocket connected")

    def _on_close(self, ws: Any, code: Any, reason: Any) -> None:
        self._connected = False
        self._logger.info("Kite order-update websocket closed: %s %s", code, reason)

    def _on_error(self, ws: Any, code: Any, reason: Any) -> None:
        self._logger.warning("Kite order-update websocket error: %s %s", code, reason)

    def _dispatch(self, order: BrokerOrder) -> None:
        """Invoke every registered callback with the order. Never holds the
        callbacks lock while calling out, and never lets one callback's failure
        stop the others."""
        with self._callbacks_lock:
            callbacks = list(self._callbacks)
        for callback in callbacks:
            try:
                callback(order)
            except Exception:  # noqa: BLE001 -- one bad subscriber must not break the rest
                self._logger.error(
                    "order-update callback raised for order %s", order.broker_order_id, exc_info=True
                )
