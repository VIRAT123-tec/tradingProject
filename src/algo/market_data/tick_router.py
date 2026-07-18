"""Fan-out of accepted ticks to every registered consumer, with a sanity gate.

Two jobs, deliberately narrow:

1. ``is_plausible`` -- a cheap sanity check that rejects a tick before it reaches
   exit-logic evaluation, where a bad price (zero, negative) could otherwise
   trigger a spurious stop-loss or target exit on real money. This is a floor,
   not a full outlier detector -- an implausible-*jump* filter would need
   per-instrument history and is deliberately out of scope here.
2. ``broadcast`` -- fan the tick out to every registered consumer. One consumer
   raising must never stop the others (or kill the websocket callback thread),
   and the consumer list lock is never held while a consumer runs (a consumer
   may re-enter market-data code).

This class holds no market state itself; the latest-price cache and staleness
timestamps live in ``MarketCache``.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from algo.strategy_engine.strategy_context import Tick

    TickConsumer = Callable[[Tick], None]


def is_plausible(tick: Tick) -> bool:
    """Reject a tick whose last price is not strictly positive. An option or
    index LTP is always > 0; a zero/negative value is corrupt data that must not
    reach premium/exit evaluation."""
    return tick.last_price > 0


class TickRouter:
    """Broadcasts accepted ticks to registered consumers."""

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger if logger is not None else logging.getLogger("algo.market_data.router")
        self._lock = threading.Lock()
        self._consumers: list[TickConsumer] = []

    def register(self, consumer: TickConsumer) -> None:
        """Add a consumer invoked (fan-out) for every accepted tick. Multiple
        consumers are supported; registration appends, never replaces."""
        with self._lock:
            self._consumers.append(consumer)

    @property
    def consumer_count(self) -> int:
        with self._lock:
            return len(self._consumers)

    def broadcast(self, tick: Tick) -> None:
        """Deliver ``tick`` to every registered consumer. Never holds the
        consumer lock while calling out; isolates a failing consumer so it
        cannot stop the others."""
        with self._lock:
            consumers = list(self._consumers)
        for consumer in consumers:
            try:
                consumer(tick)
            except Exception:  # noqa: BLE001 -- one bad subscriber must not break the rest
                self._logger.error(
                    "tick consumer raised for %s", tick.instrument, exc_info=True
                )
