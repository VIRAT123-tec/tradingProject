"""Market price source for SimulationBroker.

SimulationBroker never invents prices itself -- it asks an injected
PriceSource, so a test can pin exact, deterministic prices (StaticPriceSource)
while a paper-trading run can use something that actually moves
(RandomWalkPriceSource), without SimulationBroker's fill/quote logic knowing
or caring which.
"""

from __future__ import annotations

import random
import threading
from decimal import Decimal
from typing import Protocol

from algo.brokers.broker_base import InstrumentIdentifier


class PriceSource(Protocol):
    """Everything SimulationBroker needs to price a fill or answer a quote:
    the current last-traded price of one instrument."""

    def get_ltp(self, instrument: InstrumentIdentifier) -> Decimal: ...


class StaticPriceSource:
    """Fixed prices that only change when the test explicitly calls
    set_price() -- the simplest, most deterministic source, for tests that
    want to dictate exact fill/exit prices (e.g. to force a target or
    stop-loss to trigger)."""

    def __init__(self, prices: dict[InstrumentIdentifier, Decimal]) -> None:
        self._lock = threading.Lock()
        self._prices = dict(prices)

    def get_ltp(self, instrument: InstrumentIdentifier) -> Decimal:
        with self._lock:
            try:
                return self._prices[instrument]
            except KeyError:
                raise KeyError(f"No price configured for {instrument}") from None

    def set_price(self, instrument: InstrumentIdentifier, price: Decimal) -> None:
        with self._lock:
            self._prices[instrument] = price


class RandomWalkPriceSource:
    """Seeded random-walk prices: deterministic given the same rng and call
    sequence, but varies enough to exercise slippage/partial-fill code paths
    a StaticPriceSource can't. Each get_ltp() call both reads and advances
    the price, modeling "the market moved while we were looking at it."
    """

    def __init__(
        self,
        initial_prices: dict[InstrumentIdentifier, Decimal],
        *,
        rng: random.Random,
        volatility: Decimal = Decimal("0.001"),
    ) -> None:
        self._lock = threading.Lock()
        self._prices = dict(initial_prices)
        self._rng = rng
        self._volatility = volatility

    def get_ltp(self, instrument: InstrumentIdentifier) -> Decimal:
        with self._lock:
            try:
                current = self._prices[instrument]
            except KeyError:
                raise KeyError(f"No price configured for {instrument}") from None
            step = Decimal(str(self._rng.uniform(-1.0, 1.0))) * current * self._volatility
            updated = max(current + step, Decimal("0.05"))
            self._prices[instrument] = updated
            return updated
