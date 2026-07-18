"""PaperTradingBroker: composes a real, read-only Kite broker with the fake
SimulationBroker, so paper trading resolves option contracts and reads
prices exactly the way live trading does, while every order it places
remains fully simulated -- no real order ever reaches an exchange.

Why this exists: routing a strategy's *reads* (which contract is today's ATM
strike, what is it trading at) through the real Kite broker and its *writes*
(place/modify/cancel an order) through ``SimulationBroker`` used to be two
disconnected worlds -- paper mode had its own synthetic, fabricated option
chain (``services/paper_seed_data.py``, now removed) with tradingsymbols that
could never exist on the real exchange. That meant the live Kite tick
websocket could never resolve an instrument token for a paper position's
legs (they were never real Kite instruments), so ``PositionMonitor`` fell
back to polling the fake broker for its own fake prices -- functionally
fine, but not what "paper trading" should mean: watching the real market
and simulating what a real trade against it would have done.

This module closes that gap with a single composed ``BrokerBase``:

* Contract/price *reads* (``find_option_contract``, ``get_instrument``,
  ``get_quote``, ``get_ltp``) delegate to a real ``KiteBroker`` -- the exact
  same method, on the exact same class, live trading uses. A resolved
  contract is also mirrored into the shared ``InstrumentCatalog`` so the
  Simulation broker (which validates a placed order's tradingsymbol against
  its own catalog) recognizes it too.
* Order *writes* and portfolio state (``place_order``, ``modify_order``,
  ``cancel_order``, ``get_order(s)``, ``find_order_by_tag``,
  ``get_positions``, ``get_holdings``, ``get_margins``, the order-update
  websocket) delegate entirely to ``SimulationBroker`` -- no real order is
  ever placed, no real position is ever opened.
* ``SimulationBroker``'s own fill price source is ``KiteLtpPriceSource``
  (below), which reads the *real* live Kite LTP for the fill price -- so a
  simulated fill happens at a real, current market price, not a fabricated
  one.

The net effect: the real Kite websocket (already wired into both
entrypoints) can now genuinely subscribe to and receive live ticks for a
paper position's legs, because they are real Kite instruments -- closing the
``InstrumentNotFoundError`` gap this module was built to fix.
"""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

from algo.brokers.broker_base import (
    BrokerBase,
    BrokerHolding,
    BrokerInstrument,
    BrokerMargins,
    BrokerOrder,
    BrokerPosition,
    BrokerQuote,
    HealthStatus,
    InstrumentIdentifier,
    ModifyOrderRequest,
    OrderUpdateCallback,
    PlaceOrderRequest,
    PlaceOrderResult,
)
from algo.common.enums import BrokerName, Exchange, OptionType

if TYPE_CHECKING:
    from algo.brokers.simulation import InstrumentCatalog, SimulationBroker

__all__ = ["KiteLtpPriceSource", "PaperTradingBroker"]


class KiteLtpPriceSource:
    """``PriceSource`` (see ``brokers/simulation/price_source.py``) that reads
    the real, live Kite last-traded-price for whatever instrument
    ``SimulationBroker`` needs a price for -- entry/exit fills and live P&L
    all become grounded in a real, current market price, even though the
    order that "fills" at that price is entirely simulated.
    """

    def __init__(self, kite: BrokerBase) -> None:
        self._kite = kite

    def get_ltp(self, instrument: InstrumentIdentifier) -> Decimal:
        result = self._kite.get_ltp([instrument])
        try:
            return result[instrument]
        except KeyError:
            raise KeyError(
                f"no live Kite price available for {instrument.exchange.value}:"
                f"{instrument.tradingsymbol}"
            ) from None


class PaperTradingBroker(BrokerBase):
    """A ``BrokerBase`` that reads real Kite market data and instrument
    metadata, but simulates every order/position/margin -- see the module
    docstring for the full rationale and the exact split of responsibility.
    """

    def __init__(
        self,
        *,
        kite: BrokerBase,
        simulation: SimulationBroker,
        catalog: InstrumentCatalog,
        logger: logging.Logger | None = None,
    ) -> None:
        self._kite = kite
        self._simulation = simulation
        self._catalog = catalog
        self._logger = logger if logger is not None else logging.getLogger("algo.brokers.paper")

    # -- Lifecycle -------------------------------------------------------

    @property
    def broker_name(self) -> BrokerName:
        # Always SIMULATION: order execution is what this identifies, and
        # every order this broker places is simulated, regardless of how
        # real its market-data reads are.
        return BrokerName.SIMULATION

    def authenticate(self, *, timeout: float | None = None) -> None:
        # The real Kite session must be valid for contract resolution and
        # price reads to work at all -- authenticate it first so a bad/expired
        # token fails loudly here, not on the first entry attempt.
        self._kite.authenticate(timeout=timeout)
        self._simulation.authenticate(timeout=timeout)

    def is_authenticated(self) -> bool:
        return self._kite.is_authenticated() and self._simulation.is_authenticated()

    def close(self) -> None:
        try:
            self._kite.close()
        except Exception:  # noqa: BLE001 -- a close-time hiccup on the read-only Kite delegate must not block shutdown
            self._logger.warning("error closing the read-only Kite delegate", exc_info=True)
        self._simulation.close()

    def health_check(self, *, timeout: float | None = None) -> HealthStatus:
        # The real Kite connection is the piece with genuine external
        # dependency risk; the in-memory Simulation broker is never
        # meaningfully "unhealthy" on its own.
        return self._kite.health_check(timeout=timeout)

    # -- Orders (simulated) ----------------------------------------------

    def place_order(
        self, request: PlaceOrderRequest, *, timeout: float | None = None
    ) -> PlaceOrderResult:
        return self._simulation.place_order(request, timeout=timeout)

    def modify_order(
        self, request: ModifyOrderRequest, *, timeout: float | None = None
    ) -> None:
        self._simulation.modify_order(request, timeout=timeout)

    def cancel_order(self, broker_order_id: str, *, timeout: float | None = None) -> None:
        self._simulation.cancel_order(broker_order_id, timeout=timeout)

    def get_order(self, broker_order_id: str, *, timeout: float | None = None) -> BrokerOrder:
        return self._simulation.get_order(broker_order_id, timeout=timeout)

    def get_orders(self, *, timeout: float | None = None) -> list[BrokerOrder]:
        return self._simulation.get_orders(timeout=timeout)

    def find_order_by_tag(
        self, tag: str, *, timeout: float | None = None
    ) -> BrokerOrder | None:
        return self._simulation.find_order_by_tag(tag, timeout=timeout)

    # -- Portfolio state (simulated) --------------------------------------

    def get_positions(self, *, timeout: float | None = None) -> list[BrokerPosition]:
        return self._simulation.get_positions(timeout=timeout)

    def get_holdings(self, *, timeout: float | None = None) -> list[BrokerHolding]:
        return self._simulation.get_holdings(timeout=timeout)

    def get_margins(self, *, timeout: float | None = None) -> BrokerMargins:
        return self._simulation.get_margins(timeout=timeout)

    # -- Market data (real) ------------------------------------------------

    def get_quote(
        self, instruments: list[InstrumentIdentifier], *, timeout: float | None = None
    ) -> dict[InstrumentIdentifier, BrokerQuote]:
        return self._kite.get_quote(instruments, timeout=timeout)

    def get_ltp(
        self, instruments: list[InstrumentIdentifier], *, timeout: float | None = None
    ) -> dict[InstrumentIdentifier, Decimal]:
        return self._kite.get_ltp(instruments, timeout=timeout)

    # -- Instrument lookup (real, mirrored into the simulation catalog) ---

    def get_instrument(
        self, exchange: Exchange, tradingsymbol: str, *, timeout: float | None = None
    ) -> BrokerInstrument:
        resolved = self._kite.get_instrument(exchange, tradingsymbol, timeout=timeout)
        self._catalog.add(resolved)
        return resolved

    def find_option_contract(
        self,
        *,
        underlying: str,
        expiry: date,
        strike: Decimal,
        option_type: OptionType,
        exchange: Exchange,
        timeout: float | None = None,
    ) -> BrokerInstrument:
        resolved = self._kite.find_option_contract(
            underlying=underlying, expiry=expiry, strike=strike,
            option_type=option_type, exchange=exchange, timeout=timeout,
        )
        # Mirror into the Simulation broker's own catalog -- place_order()
        # and get_margins() both validate a tradingsymbol against it, and it
        # would otherwise never have heard of a contract this broker only
        # ever resolved through the real Kite side.
        self._catalog.add_option(underlying=underlying, instrument=resolved)
        return resolved

    # -- Websocket / order-update push (simulated) ------------------------

    def connect_websocket(self, *, timeout: float | None = None) -> None:
        self._simulation.connect_websocket(timeout=timeout)

    def disconnect_websocket(self) -> None:
        self._simulation.disconnect_websocket()

    def is_websocket_connected(self) -> bool:
        return self._simulation.is_websocket_connected()

    def register_order_update_callback(self, callback: OrderUpdateCallback) -> None:
        self._simulation.register_order_update_callback(callback)
