"""Unit tests for the Kite mapper -- the pure request/response/exception
mapping that is the only place Kite string constants live. Exhaustive because a
mapping bug here (a mis-mapped order status, a timeout classified as
non-ambiguous) is exactly the kind of silent error that moves real money the
wrong way.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
import requests.exceptions as req_exc
from kiteconnect import KiteConnect
from kiteconnect import exceptions as kite_exc

from algo.brokers.broker_base import InstrumentIdentifier, PlaceOrderRequest
from algo.brokers.exceptions import (
    BrokerAuthenticationError,
    BrokerConnectionError,
    BrokerTimeoutError,
    InvalidOrderRequestError,
    OrderRejectedError,
)
from algo.brokers.kite import mapper
from algo.common.enums import (
    Exchange,
    OptionType,
    OrderStatus,
    OrderType,
    ProductType,
    TransactionType,
)


# --------------------------------------------------------------------------
# Enum mapping
# --------------------------------------------------------------------------


class TestEnumMappingToKite:
    def test_exchange(self):
        assert mapper.to_kite_exchange(Exchange.NFO) == KiteConnect.EXCHANGE_NFO
        assert mapper.to_kite_exchange(Exchange.BFO) == KiteConnect.EXCHANGE_BFO

    def test_exchange_cash_segments(self):
        # NSE/BSE are the cash segments used only for reading an underlying
        # index's spot LTP (SpotPriceProvider) -- never for a persisted
        # order/position, but get_ltp() must still be able to map them.
        assert mapper.to_kite_exchange(Exchange.NSE) == KiteConnect.EXCHANGE_NSE
        assert mapper.to_kite_exchange(Exchange.BSE) == KiteConnect.EXCHANGE_BSE

    def test_transaction_type(self):
        assert mapper.to_kite_transaction_type(TransactionType.BUY) == "BUY"
        assert mapper.to_kite_transaction_type(TransactionType.SELL) == "SELL"

    def test_product(self):
        assert mapper.to_kite_product(ProductType.INTRADAY) == KiteConnect.PRODUCT_MIS
        assert mapper.to_kite_product(ProductType.NORMAL) == KiteConnect.PRODUCT_NRML

    def test_order_type(self):
        assert mapper.to_kite_order_type(OrderType.MARKET) == "MARKET"
        assert mapper.to_kite_order_type(OrderType.LIMIT) == "LIMIT"


class TestStatusMappingFromKite:
    @pytest.mark.parametrize(
        "kite_status,expected",
        [
            ("COMPLETE", OrderStatus.COMPLETE),
            ("REJECTED", OrderStatus.REJECTED),
            ("CANCELLED", OrderStatus.CANCELLED),
            ("OPEN", OrderStatus.OPEN),
        ],
    )
    def test_terminal_and_open(self, kite_status, expected):
        assert mapper.from_kite_status(kite_status) is expected

    @pytest.mark.parametrize(
        "transient",
        ["PENDING", "TRIGGER PENDING", "OPEN PENDING", "VALIDATION PENDING",
         "PUT ORDER REQ RECEIVED", "MODIFY PENDING", "CANCEL PENDING", ""],
    )
    def test_transient_statuses_map_to_pending(self, transient):
        assert mapper.from_kite_status(transient) is OrderStatus.PENDING

    def test_unknown_status_maps_to_pending_not_terminal(self):
        # The safe direction to err: never wrongly treat an order as finished.
        assert mapper.from_kite_status("SOME_NEW_KITE_STATUS") is OrderStatus.PENDING


class TestOtherFromKite:
    def test_from_kite_exchange(self):
        assert mapper.from_kite_exchange("NFO") is Exchange.NFO
        assert mapper.from_kite_exchange("BFO") is Exchange.BFO
        assert mapper.from_kite_exchange("NSE") is Exchange.NSE
        assert mapper.from_kite_exchange("BSE") is Exchange.BSE

    def test_from_kite_exchange_unknown_raises(self):
        with pytest.raises(mapper.KiteMappingError):
            mapper.from_kite_exchange("MCX")

    def test_from_kite_product(self):
        assert mapper.from_kite_product("MIS") is ProductType.INTRADAY
        assert mapper.from_kite_product("NRML") is ProductType.NORMAL
        assert mapper.from_kite_product("CNC") is ProductType.NORMAL  # foreign default

    def test_from_kite_option_type(self):
        assert mapper.from_kite_option_type("CE") is OptionType.CE
        assert mapper.from_kite_option_type("PE") is OptionType.PE
        assert mapper.from_kite_option_type("FUT") is None


# --------------------------------------------------------------------------
# Request building
# --------------------------------------------------------------------------


class TestPlaceOrderKwargs:
    def test_market_sell(self):
        request = PlaceOrderRequest(
            exchange=Exchange.NFO, tradingsymbol="NIFTY24000CE", transaction_type=TransactionType.SELL,
            quantity=75, product=ProductType.INTRADAY, order_type=OrderType.MARKET, tag="E1-CE",
        )
        kwargs = mapper.place_order_kwargs(request)
        assert kwargs == {
            "variety": KiteConnect.VARIETY_REGULAR, "exchange": "NFO", "tradingsymbol": "NIFTY24000CE",
            "transaction_type": "SELL", "quantity": 75, "product": "MIS", "order_type": "MARKET", "tag": "E1-CE",
        }
        assert "price" not in kwargs  # market order carries no price

    def test_limit_includes_price_as_float(self):
        request = PlaceOrderRequest(
            exchange=Exchange.NFO, tradingsymbol="X", transaction_type=TransactionType.BUY,
            quantity=75, product=ProductType.NORMAL, order_type=OrderType.LIMIT, price=Decimal("101.5"), tag="t",
        )
        kwargs = mapper.place_order_kwargs(request)
        assert kwargs["price"] == 101.5
        assert isinstance(kwargs["price"], float)
        assert kwargs["product"] == "NRML"

    def test_modify_kwargs_only_includes_provided_fields(self):
        kwargs = mapper.modify_order_kwargs(
            order_id="123", quantity=150, price=None, trigger_price=None, order_type=None,
        )
        assert kwargs == {"variety": KiteConnect.VARIETY_REGULAR, "order_id": "123", "quantity": 150}

    def test_quote_key_format(self):
        ident = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTY24000CE")
        assert mapper.quote_key(ident) == "NFO:NIFTY24000CE"


# --------------------------------------------------------------------------
# Response mapping
# --------------------------------------------------------------------------


def _kite_order(**overrides):
    base = {
        "order_id": "151220000000001", "status": "COMPLETE", "exchange": "NFO",
        "tradingsymbol": "NIFTY24000CE", "transaction_type": "SELL", "product": "MIS",
        "order_type": "MARKET", "quantity": 75, "filled_quantity": 75, "average_price": 120.5,
        "trigger_price": 0, "tag": "E1-CE",
        "order_timestamp": datetime(2026, 7, 7, 9, 20, 5), "exchange_timestamp": datetime(2026, 7, 7, 9, 20, 6),
        "status_message": None,
    }
    base.update(overrides)
    return base


class TestToBrokerOrder:
    def test_complete_order(self):
        order = mapper.to_broker_order(_kite_order())
        assert order.broker_order_id == "151220000000001"
        assert order.status is OrderStatus.COMPLETE
        assert order.exchange is Exchange.NFO
        assert order.transaction_type is TransactionType.SELL
        assert order.product is ProductType.INTRADAY
        assert order.quantity == 75
        assert order.filled_quantity == 75
        assert order.average_price == Decimal("120.5")
        assert order.tag == "E1-CE"

    def test_timestamps_are_localized_to_ist(self):
        order = mapper.to_broker_order(_kite_order())
        assert order.placed_at is not None and order.placed_at.tzinfo is not None
        assert order.filled_at is not None and order.filled_at.tzinfo is not None

    def test_average_price_zero_becomes_none(self):
        order = mapper.to_broker_order(_kite_order(status="OPEN", filled_quantity=0, average_price=0))
        assert order.average_price is None
        assert order.filled_at is None

    def test_rejected_order_carries_message(self):
        order = mapper.to_broker_order(
            _kite_order(status="REJECTED", filled_quantity=0, average_price=0, status_message="insufficient funds")
        )
        assert order.status is OrderStatus.REJECTED
        assert order.status_message == "insufficient funds"

    def test_missing_required_field_raises_mapping_error(self):
        bad = _kite_order()
        del bad["exchange"]
        with pytest.raises(mapper.KiteMappingError):
            mapper.to_broker_order(bad)


class TestToBrokerPositionHoldingMargins:
    def test_position(self):
        pos = mapper.to_broker_position({
            "exchange": "NFO", "tradingsymbol": "NIFTY24000CE", "product": "MIS",
            "quantity": -75, "average_price": 120.5, "last_price": 118.0, "pnl": 187.5,
        })
        assert pos.quantity == -75
        assert pos.average_price == Decimal("120.5")
        assert pos.pnl == Decimal("187.5")

    def test_holding(self):
        h = mapper.to_broker_holding({
            "exchange": "NFO", "tradingsymbol": "X", "quantity": 10, "average_price": 100, "last_price": 110,
        })
        assert h.quantity == 10
        assert h.last_price == Decimal("110")

    def test_margins_equity_block(self):
        m = mapper.to_broker_margins("equity", {
            "available": {"cash": 100000, "opening_balance": 120000}, "utilised": {"debits": 20000},
        })
        assert m.segment == "equity"
        assert m.available_cash == Decimal("100000")
        assert m.used_margin == Decimal("20000")
        assert m.opening_balance == Decimal("120000")


class TestToBrokerInstrument:
    def test_option_instrument(self):
        inst = mapper.to_broker_instrument({
            "instrument_token": 12345, "exchange": "NFO", "tradingsymbol": "NIFTY26JUL24000CE",
            "name": "NIFTY", "lot_size": 75, "tick_size": 0.05, "expiry": date(2026, 7, 30),
            "strike": 24000.0, "instrument_type": "CE",
        })
        assert inst.instrument_token == 12345
        assert inst.lot_size == 75
        assert inst.strike == Decimal("24000")
        assert inst.expiry == date(2026, 7, 30)
        assert inst.option_type is OptionType.CE


class TestToBrokerQuote:
    def test_quote(self):
        ident = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="X")
        q = mapper.to_broker_quote(ident, {
            "last_price": 120.5, "ohlc": {"open": 118, "high": 125, "low": 117, "close": 119},
            "volume": 10000, "timestamp": datetime(2026, 7, 7, 10, 0),
        })
        assert q.last_price == Decimal("120.5")
        assert q.ohlc_high == Decimal("125")
        assert q.volume == 10000
        assert q.timestamp.tzinfo is not None


# --------------------------------------------------------------------------
# Exception translation -- the safety-critical part
# --------------------------------------------------------------------------


class TestExceptionTranslation:
    def test_token_exception_is_auth_error(self):
        result = mapper.translate_kite_exception(kite_exc.TokenException("expired"), mutating=False)
        assert isinstance(result, BrokerAuthenticationError)

    def test_input_exception_is_invalid_request(self):
        result = mapper.translate_kite_exception(kite_exc.InputException("bad"), mutating=True)
        assert isinstance(result, InvalidOrderRequestError)

    def test_order_exception_is_rejection(self):
        result = mapper.translate_kite_exception(kite_exc.OrderException("nope"), mutating=True)
        assert isinstance(result, OrderRejectedError)

    def test_read_timeout_is_ambiguous_timeout(self):
        # A read timeout means the request was sent but no response arrived --
        # ambiguous for a mutation, and BrokerTimeoutError in both cases.
        for mutating in (True, False):
            result = mapper.translate_kite_exception(req_exc.ReadTimeout("slow"), mutating=mutating)
            assert isinstance(result, BrokerTimeoutError)

    def test_connect_timeout_is_connection_error_not_sent(self):
        # Never connected -> never sent -> safe (BrokerConnectionError).
        result = mapper.translate_kite_exception(req_exc.ConnectTimeout("no route"), mutating=True)
        assert isinstance(result, BrokerConnectionError)

    def test_connection_error_is_connection_error(self):
        result = mapper.translate_kite_exception(req_exc.ConnectionError("reset"), mutating=True)
        assert isinstance(result, BrokerConnectionError)

    def test_kite_network_exception_is_ambiguous_on_mutation(self):
        # Kite-reported OMS/network trouble: the order MAY have reached the
        # exchange -> ambiguous timeout on a mutation ...
        result = mapper.translate_kite_exception(kite_exc.NetworkException("oms down"), mutating=True)
        assert isinstance(result, BrokerTimeoutError)

    def test_kite_network_exception_is_retryable_on_read(self):
        # ... but merely a retryable connection blip on a read.
        result = mapper.translate_kite_exception(kite_exc.NetworkException("oms down"), mutating=False)
        assert isinstance(result, BrokerConnectionError)
