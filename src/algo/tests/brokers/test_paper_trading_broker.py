"""Tests for PaperTradingBroker / KiteLtpPriceSource -- the composition that
routes reads to a real Kite broker and writes to the fake SimulationBroker.

The two delegates are mocked (``MagicMock(spec=BrokerBase)``, the same
pattern ``test_kite_broker_wiring.py`` uses at the SDK boundary) since what
this module owns is purely *which delegate a call goes to*, not either
delegate's own behavior (already covered by test_kite_broker.py and
test_simulation_broker.py respectively). The instrument catalog is real,
not mocked, to verify the mirroring behavior for real.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from algo.brokers.broker_base import (
    BrokerBase,
    BrokerInstrument,
    BrokerOrder,
    HealthStatus,
    InstrumentIdentifier,
    ModifyOrderRequest,
    PlaceOrderRequest,
    PlaceOrderResult,
)
from algo.brokers.exceptions import InstrumentNotFoundError
from algo.brokers.paper_trading_broker import KiteLtpPriceSource, PaperTradingBroker
from algo.brokers.simulation import InstrumentCatalog
from algo.common.enums import BrokerName, Exchange, OptionType, OrderStatus, OrderType, ProductType, TransactionType


def _instrument(**overrides) -> BrokerInstrument:
    base = dict(
        instrument_token=123, exchange=Exchange.NFO, tradingsymbol="NIFTY2672125000CE",
        name="NIFTY", lot_size=75, tick_size=Decimal("0.05"),
        expiry=date(2026, 7, 21), strike=Decimal("25000"), option_type=OptionType.CE,
    )
    base.update(overrides)
    return BrokerInstrument(**base)


@pytest.fixture
def kite() -> MagicMock:
    return MagicMock(spec=BrokerBase)


@pytest.fixture
def simulation() -> MagicMock:
    return MagicMock(spec=BrokerBase)


@pytest.fixture
def catalog() -> InstrumentCatalog:
    return InstrumentCatalog()


@pytest.fixture
def broker(kite, simulation, catalog) -> PaperTradingBroker:
    return PaperTradingBroker(kite=kite, simulation=simulation, catalog=catalog)


class TestLifecycle:
    def test_broker_name_is_always_simulation(self, broker):
        assert broker.broker_name is BrokerName.SIMULATION

    def test_authenticate_authenticates_both_delegates(self, broker, kite, simulation):
        broker.authenticate(timeout=5.0)
        kite.authenticate.assert_called_once_with(timeout=5.0)
        simulation.authenticate.assert_called_once_with(timeout=5.0)

    def test_is_authenticated_requires_both(self, broker, kite, simulation):
        kite.is_authenticated.return_value = True
        simulation.is_authenticated.return_value = False
        assert broker.is_authenticated() is False

        simulation.is_authenticated.return_value = True
        assert broker.is_authenticated() is True

    def test_close_closes_both_delegates(self, broker, kite, simulation):
        broker.close()
        kite.close.assert_called_once()
        simulation.close.assert_called_once()

    def test_close_tolerates_kite_delegate_failure(self, broker, kite, simulation):
        kite.close.side_effect = RuntimeError("boom")
        broker.close()  # must not raise
        simulation.close.assert_called_once()

    def test_health_check_delegates_to_kite_only(self, broker, kite, simulation):
        expected = HealthStatus(healthy=True, checked_at=datetime(2026, 7, 10, tzinfo=timezone.utc))
        kite.health_check.return_value = expected
        result = broker.health_check(timeout=3.0)
        assert result is expected
        kite.health_check.assert_called_once_with(timeout=3.0)
        simulation.health_check.assert_not_called()


class TestOrdersDelegateToSimulation:
    def test_place_order(self, broker, kite, simulation):
        request = PlaceOrderRequest(
            exchange=Exchange.NFO, tradingsymbol="NIFTY2672125000CE",
            transaction_type=TransactionType.SELL, quantity=75,
            product=ProductType.INTRADAY, order_type=OrderType.MARKET, tag="E1-CE",
        )
        expected = PlaceOrderResult(broker_order_id="SIM1")
        simulation.place_order.return_value = expected
        result = broker.place_order(request, timeout=2.0)
        assert result is expected
        simulation.place_order.assert_called_once_with(request, timeout=2.0)
        kite.place_order.assert_not_called()

    def test_modify_order(self, broker, kite, simulation):
        request = ModifyOrderRequest(broker_order_id="SIM1", quantity=150)
        broker.modify_order(request, timeout=1.0)
        simulation.modify_order.assert_called_once_with(request, timeout=1.0)
        kite.modify_order.assert_not_called()

    def test_cancel_order(self, broker, kite, simulation):
        broker.cancel_order("SIM1", timeout=1.0)
        simulation.cancel_order.assert_called_once_with("SIM1", timeout=1.0)
        kite.cancel_order.assert_not_called()

    def test_get_order(self, broker, kite, simulation):
        order = MagicMock(spec=BrokerOrder)
        simulation.get_order.return_value = order
        assert broker.get_order("SIM1", timeout=1.0) is order
        simulation.get_order.assert_called_once_with("SIM1", timeout=1.0)
        kite.get_order.assert_not_called()

    def test_get_orders(self, broker, kite, simulation):
        simulation.get_orders.return_value = []
        broker.get_orders(timeout=1.0)
        simulation.get_orders.assert_called_once_with(timeout=1.0)
        kite.get_orders.assert_not_called()

    def test_find_order_by_tag(self, broker, kite, simulation):
        simulation.find_order_by_tag.return_value = None
        broker.find_order_by_tag("E1-CE", timeout=1.0)
        simulation.find_order_by_tag.assert_called_once_with("E1-CE", timeout=1.0)
        kite.find_order_by_tag.assert_not_called()


class TestPortfolioDelegatesToSimulation:
    def test_get_positions(self, broker, kite, simulation):
        simulation.get_positions.return_value = []
        broker.get_positions(timeout=1.0)
        simulation.get_positions.assert_called_once_with(timeout=1.0)
        kite.get_positions.assert_not_called()

    def test_get_holdings(self, broker, kite, simulation):
        simulation.get_holdings.return_value = []
        broker.get_holdings(timeout=1.0)
        simulation.get_holdings.assert_called_once_with(timeout=1.0)
        kite.get_holdings.assert_not_called()

    def test_get_margins(self, broker, kite, simulation):
        broker.get_margins(timeout=1.0)
        simulation.get_margins.assert_called_once_with(timeout=1.0)
        kite.get_margins.assert_not_called()


class TestMarketDataDelegatesToKite:
    def test_get_quote(self, broker, kite, simulation):
        broker.get_quote([], timeout=1.0)
        kite.get_quote.assert_called_once_with([], timeout=1.0)
        simulation.get_quote.assert_not_called()

    def test_get_ltp(self, broker, kite, simulation):
        broker.get_ltp([], timeout=1.0)
        kite.get_ltp.assert_called_once_with([], timeout=1.0)
        simulation.get_ltp.assert_not_called()


class TestInstrumentLookupDelegatesToKiteAndMirrorsIntoCatalog:
    def test_get_instrument_resolves_via_kite_and_registers_in_catalog(self, broker, kite, catalog):
        resolved = _instrument(option_type=None, expiry=None, strike=None)
        kite.get_instrument.return_value = resolved

        result = broker.get_instrument(Exchange.NFO, "NIFTY2672125000CE", timeout=1.0)

        assert result is resolved
        kite.get_instrument.assert_called_once_with(Exchange.NFO, "NIFTY2672125000CE", timeout=1.0)
        # Mirrored: the Simulation broker's own catalog now knows this symbol.
        assert catalog.get_by_symbol(Exchange.NFO, "NIFTY2672125000CE") is resolved

    def test_find_option_contract_resolves_via_kite_and_registers_in_catalog(self, broker, kite, catalog):
        resolved = _instrument()
        kite.find_option_contract.return_value = resolved

        result = broker.find_option_contract(
            underlying="NIFTY", expiry=date(2026, 7, 21), strike=Decimal("25000"),
            option_type=OptionType.CE, exchange=Exchange.NFO, timeout=1.0,
        )

        assert result is resolved
        kite.find_option_contract.assert_called_once_with(
            underlying="NIFTY", expiry=date(2026, 7, 21), strike=Decimal("25000"),
            option_type=OptionType.CE, exchange=Exchange.NFO, timeout=1.0,
        )
        # Mirrored into the catalog by both lookup keys.
        assert catalog.get_by_symbol(Exchange.NFO, "NIFTY2672125000CE") is resolved
        assert catalog.find_option(
            underlying="NIFTY", expiry=date(2026, 7, 21), strike=Decimal("25000"),
            option_type=OptionType.CE, exchange=Exchange.NFO,
        ) is resolved

    def test_find_option_contract_not_found_propagates_and_registers_nothing(self, broker, kite, catalog):
        kite.find_option_contract.side_effect = InstrumentNotFoundError("no such contract")
        with pytest.raises(InstrumentNotFoundError):
            broker.find_option_contract(
                underlying="NIFTY", expiry=date(2026, 7, 21), strike=Decimal("25000"),
                option_type=OptionType.CE, exchange=Exchange.NFO,
            )
        with pytest.raises(InstrumentNotFoundError):
            catalog.get_by_symbol(Exchange.NFO, "NIFTY2672125000CE")


class TestWebsocketDelegatesToSimulation:
    def test_connect_and_disconnect(self, broker, kite, simulation):
        broker.connect_websocket(timeout=1.0)
        simulation.connect_websocket.assert_called_once_with(timeout=1.0)
        kite.connect_websocket.assert_not_called()

        broker.disconnect_websocket()
        simulation.disconnect_websocket.assert_called_once()
        kite.disconnect_websocket.assert_not_called()

    def test_is_connected(self, broker, kite, simulation):
        simulation.is_websocket_connected.return_value = True
        assert broker.is_websocket_connected() is True
        kite.is_websocket_connected.assert_not_called()

    def test_register_callback(self, broker, kite, simulation):
        callback = MagicMock()
        broker.register_order_update_callback(callback)
        simulation.register_order_update_callback.assert_called_once_with(callback)
        kite.register_order_update_callback.assert_not_called()


class TestKiteLtpPriceSource:
    def test_returns_the_real_kite_ltp(self):
        kite = MagicMock(spec=BrokerBase)
        ident = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTY2672125000CE")
        kite.get_ltp.return_value = {ident: Decimal("142.50")}

        source = KiteLtpPriceSource(kite)
        assert source.get_ltp(ident) == Decimal("142.50")
        kite.get_ltp.assert_called_once_with([ident])

    def test_missing_price_raises_a_clear_key_error(self):
        kite = MagicMock(spec=BrokerBase)
        ident = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTY2672125000CE")
        kite.get_ltp.return_value = {}

        source = KiteLtpPriceSource(kite)
        with pytest.raises(KeyError, match="NIFTY2672125000CE"):
            source.get_ltp(ident)
