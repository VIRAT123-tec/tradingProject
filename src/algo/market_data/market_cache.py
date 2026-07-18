"""Thread-safe in-memory cache of the latest price per instrument.

Derived and ephemeral, exactly as the module's original contract states: it is
rebuilt from live ticks (and polled refreshes) after a restart and is NEVER the
source of truth for anything that matters -- the database is authoritative for
position and strategy state. The cache exists only to answer "what is the most
recent price we have for this instrument, and how old is it?" cheaply and
without a broker round-trip on the hot path.

Thread-safe because it is written from the websocket's callback thread and read
from strategy threads concurrently; every access is guarded by one lock.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from algo.brokers.broker_base import InstrumentIdentifier


@dataclass(frozen=True, slots=True)
class CachedPrice:
    """A cached last-traded price and when it was recorded (the receipt/refresh
    time, used for staleness -- not the exchange tick time, which can lag)."""

    price: Decimal
    updated_at: datetime


class MarketCache:
    """Latest-price cache keyed by ``InstrumentIdentifier``."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._prices: dict[InstrumentIdentifier, CachedPrice] = {}

    def update(self, instrument: InstrumentIdentifier, price: Decimal, updated_at: datetime) -> None:
        """Record a fresh price for an instrument (overwrites any prior value)."""
        with self._lock:
            self._prices[instrument] = CachedPrice(price=price, updated_at=updated_at)

    def get(self, instrument: InstrumentIdentifier) -> CachedPrice | None:
        """The cached price entry for an instrument, or None if never seen."""
        with self._lock:
            return self._prices.get(instrument)

    def get_price(self, instrument: InstrumentIdentifier) -> Decimal | None:
        """The cached last price, or None if never seen."""
        entry = self.get(instrument)
        return entry.price if entry is not None else None

    def is_fresh(
        self, instrument: InstrumentIdentifier, *, now: datetime, max_age_seconds: float
    ) -> bool:
        """Whether the cached price for an instrument exists and is younger than
        ``max_age_seconds``. A missing entry is never fresh."""
        entry = self.get(instrument)
        if entry is None:
            return False
        return (now - entry.updated_at).total_seconds() <= max_age_seconds

    def age_seconds(self, instrument: InstrumentIdentifier, *, now: datetime) -> float | None:
        """Seconds since the cached price was recorded, or None if never seen."""
        entry = self.get(instrument)
        if entry is None:
            return None
        return (now - entry.updated_at).total_seconds()

    def remove(self, instrument: InstrumentIdentifier) -> None:
        """Drop an instrument's cached price (e.g. on final unsubscribe)."""
        with self._lock:
            self._prices.pop(instrument, None)

    def clear(self) -> None:
        """Empty the cache (e.g. on a full reconnect from scratch)."""
        with self._lock:
            self._prices.clear()

    def snapshot(self) -> dict[InstrumentIdentifier, CachedPrice]:
        """A point-in-time copy of the whole cache, for monitoring/debugging."""
        with self._lock:
            return dict(self._prices)
