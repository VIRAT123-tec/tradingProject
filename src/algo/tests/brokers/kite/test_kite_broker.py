"""Tests for KiteBroker, KiteSession, and KiteOrderUpdateStream.

Everything is driven through injected fakes -- a FakeKiteClient (canned
responses / configurable raises) and a fake ticker -- so the whole broker is
exercised with no network and no real Kite credentials, which is the only way
it can be tested in development. This verifies the orchestration, retry policy,
exception handling, and lifecycle; the pure Kite<->platform mapping is covered
by test_mapper.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

import pytest
import requests.exceptions as req_exc
from kiteconnect import exceptions as kite_exc

from algo.brokers.broker_base import (
    InstrumentIdentifier,
    ModifyOrderRequest,
    PlaceOrderRequest,
)
from algo.brokers.exceptions import (
    BrokerAuthenticationError,
    BrokerConnectionError,
    BrokerTimeoutError,
    InstrumentNotFoundError,
    InvalidOrderRequestError,
    OrderNotCancellableError,
    OrderNotFoundError,
)
from algo.brokers.kite.kite_auth import KiteSession
from algo.brokers.kite.kite_broker import KiteBroker, KiteBrokerConfig
from algo.brokers.kite.websocket import KiteOrderUpdateStream
from algo.common.enums import (
    BrokerName,
    Exchange,
    OptionType,
    OrderStatus,
    OrderType,
    ProductType,
    TransactionType,
)


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


class _Raise:
    """Sentinel wrapping an exception to raise when a fake method is called."""

    def __init__(self, exc):
        self.exc = exc


@dataclass
class FakeKiteClient:
    """Configurable stand-in for kiteconnect.KiteConnect. Each attribute holds
    either a canned return value or a _Raise(exc)."""

    access_token: str | None = None
    profile_result: object = field(default_factory=dict)
    place_order_result: object = "ORDER123"
    modify_order_result: object = "ORDER123"
    cancel_order_result: object = "ORDER123"
    orders_result: object = field(default_factory=list)
    order_history_result: object = field(default_factory=list)
    positions_result: object = field(default_factory=lambda: {"net": []})
    holdings_result: object = field(default_factory=list)
    margins_result: object = field(default_factory=dict)
    quote_result: object = field(default_factory=dict)
    ltp_result: object = field(default_factory=dict)
    instruments_result: object = field(default_factory=list)
    generate_session_result: object = field(default_factory=lambda: {"access_token": "NEW_TOKEN"})
    instruments_call_count: int = 0

    def _resolve(self, value):
        if isinstance(value, _Raise):
            raise value.exc
        return value

    def set_access_token(self, access_token):
        self.access_token = access_token

    def generate_session(self, request_token, api_secret):
        return self._resolve(self.generate_session_result)

    def profile(self):
        return self._resolve(self.profile_result)

    def place_order(self, **kwargs):
        return self._resolve(self.place_order_result)

    def modify_order(self, **kwargs):
        return self._resolve(self.modify_order_result)

    def cancel_order(self, variety, order_id, **kwargs):
        return self._resolve(self.cancel_order_result)

    def orders(self):
        return self._resolve(self.orders_result)

    def order_history(self, order_id):
        return self._resolve(self.order_history_result)

    def positions(self):
        return self._resolve(self.positions_result)

    def holdings(self):
        return self._resolve(self.holdings_result)

    def margins(self, segment=None):
        return self._resolve(self.margins_result)

    def quote(self, *instruments):
        return self._resolve(self.quote_result)

    def ltp(self, *instruments):
        return self._resolve(self.ltp_result)

    def instruments(self, exchange=None):
        self.instruments_call_count += 1
        return self._resolve(self.instruments_result)


@dataclass
class DictTokenStore:
    token: str | None = "TODAY_TOKEN"

    def get_access_token(self):
        return self.token

    def set_access_token(self, access_token):
        self.token = access_token


@dataclass
class FakeTicker:
    on_order_update: object = None
    on_connect: object = None
    on_close: object = None
    on_error: object = None
    connected: bool = False
    connect_calls: int = 0
    close_calls: int = 0

    def connect(self, threaded=True, **kwargs):
        self.connect_calls += 1

    def close(self, *args, **kwargs):
        self.close_calls += 1

    def is_connected(self):
        return self.connected


def _kite_order(**overrides):
    base = {
        "order_id": "ORDER123", "status": "COMPLETE", "exchange": "NFO", "tradingsymbol": "NIFTY24000CE",
        "transaction_type": "SELL", "product": "MIS", "order_type": "MARKET", "quantity": 75,
        "filled_quantity": 75, "average_price": 120.5, "trigger_price": 0, "tag": "E1-CE",
        "order_timestamp": datetime(2026, 7, 7, 9, 20), "exchange_timestamp": datetime(2026, 7, 7, 9, 20),
    }
    base.update(overrides)
    return base


def build_broker(client: FakeKiteClient | None = None, *, token: str | None = "TODAY_TOKEN"):
    client = client or FakeKiteClient()
    session = KiteSession(client=client, api_secret="secret", token_store=DictTokenStore(token=token))
    ticker = FakeTicker()
    stream = KiteOrderUpdateStream(ticker_factory=lambda: ticker)
    broker = KiteBroker(
        client=client, session=session, order_stream=stream,
        config=KiteBrokerConfig(read_retry_attempts=3, read_retry_delay_seconds=0.0),
        sleep=lambda s: None,
    )
    return broker, client, ticker


# --------------------------------------------------------------------------
# Session / auth
# --------------------------------------------------------------------------


class TestSession:
    def test_activate_applies_token(self):
        client = FakeKiteClient()
        session = KiteSession(client=client, api_secret="s", token_store=DictTokenStore(token="TKN"))
        session.activate()
        assert client.access_token == "TKN"
        assert session.is_active

    def test_activate_without_token_raises(self):
        session = KiteSession(client=FakeKiteClient(), api_secret="s", token_store=DictTokenStore(token=None))
        with pytest.raises(BrokerAuthenticationError):
            session.activate()

    def test_generate_session_stores_and_applies_token(self):
        client = FakeKiteClient(generate_session_result={"access_token": "FRESH"})
        store = DictTokenStore(token=None)
        session = KiteSession(client=client, api_secret="s", token_store=store)
        token = session.generate_session("request_token_abc")
        assert token == "FRESH"
        assert client.access_token == "FRESH"
        assert store.token == "FRESH"


class TestAuthenticate:
    def test_authenticate_success(self):
        broker, client, _ = build_broker()
        broker.authenticate()
        assert broker.is_authenticated()
        assert client.access_token == "TODAY_TOKEN"

    def test_authenticate_no_token_raises(self):
        broker, _, _ = build_broker(token=None)
        with pytest.raises(BrokerAuthenticationError):
            broker.authenticate()

    def test_expired_token_on_validate_is_auth_error(self):
        client = FakeKiteClient(profile_result=_Raise(kite_exc.TokenException("expired")))
        broker, _, _ = build_broker(client)
        with pytest.raises(BrokerAuthenticationError):
            broker.authenticate()
        assert not broker.is_authenticated()

    def test_broker_name(self):
        broker, _, _ = build_broker()
        assert broker.broker_name is BrokerName.KITE


# --------------------------------------------------------------------------
# Order mutations (never retried)
# --------------------------------------------------------------------------


class TestPlaceOrder:
    def test_success_returns_broker_order_id(self):
        broker, _, _ = build_broker(FakeKiteClient(place_order_result="ORDER999"))
        result = broker.place_order(_place_request())
        assert result.broker_order_id == "ORDER999"

    def test_rejection_is_order_rejected(self):
        from algo.brokers.exceptions import OrderRejectedError

        broker, _, _ = build_broker(FakeKiteClient(place_order_result=_Raise(kite_exc.OrderException("margin"))))
        with pytest.raises(OrderRejectedError):
            broker.place_order(_place_request())

    def test_input_error_is_invalid_request(self):
        broker, _, _ = build_broker(FakeKiteClient(place_order_result=_Raise(kite_exc.InputException("bad symbol"))))
        with pytest.raises(InvalidOrderRequestError):
            broker.place_order(_place_request())

    def test_read_timeout_is_ambiguous_and_not_retried(self):
        # A timeout on a mutation must surface as ambiguous, and must NOT be
        # retried internally (single call).
        calls = {"n": 0}

        class CountingClient(FakeKiteClient):
            def place_order(self, **kwargs):
                calls["n"] += 1
                raise req_exc.ReadTimeout("slow")

        broker, _, _ = build_broker(CountingClient())
        with pytest.raises(BrokerTimeoutError):
            broker.place_order(_place_request())
        assert calls["n"] == 1  # exactly one attempt -- never retried

    def test_connect_timeout_is_connection_error_not_sent(self):
        broker, _, _ = build_broker(FakeKiteClient(place_order_result=_Raise(req_exc.ConnectTimeout("x"))))
        with pytest.raises(BrokerConnectionError):
            broker.place_order(_place_request())


class TestModifyCancel:
    def test_modify_success(self):
        broker, _, _ = build_broker()
        broker.modify_order(ModifyOrderRequest(broker_order_id="ORDER123", quantity=150))  # no raise

    def test_cancel_terminal_order_is_not_cancellable(self):
        broker, _, _ = build_broker(FakeKiteClient(cancel_order_result=_Raise(kite_exc.OrderException("already complete"))))
        with pytest.raises(OrderNotCancellableError):
            broker.cancel_order("ORDER123")


# --------------------------------------------------------------------------
# Reads
# --------------------------------------------------------------------------


class TestGetOrder:
    def test_returns_latest_history_state(self):
        client = FakeKiteClient(order_history_result=[
            _kite_order(status="OPEN", filled_quantity=0, average_price=0),
            _kite_order(status="COMPLETE"),
        ])
        broker, _, _ = build_broker(client)
        order = broker.get_order("ORDER123")
        assert order.status is OrderStatus.COMPLETE  # last state

    def test_empty_history_is_not_found(self):
        broker, _, _ = build_broker(FakeKiteClient(order_history_result=[]))
        with pytest.raises(OrderNotFoundError):
            broker.get_order("NOPE")

    def test_kite_order_not_found_raises_not_found(self):
        broker, _, _ = build_broker(
            FakeKiteClient(order_history_result=_Raise(kite_exc.InputException("order not found")))
        )
        with pytest.raises(OrderNotFoundError):
            broker.get_order("NOPE")


class TestGetOrdersAndFindByTag:
    def test_get_orders_maps_all(self):
        broker, _, _ = build_broker(FakeKiteClient(orders_result=[_kite_order(order_id="A"), _kite_order(order_id="B")]))
        orders = broker.get_orders()
        assert {o.broker_order_id for o in orders} == {"A", "B"}

    def test_find_order_by_tag_matches(self):
        broker, _, _ = build_broker(FakeKiteClient(orders_result=[
            _kite_order(order_id="A", tag="E1-CE"), _kite_order(order_id="B", tag="E1-PE"),
        ]))
        found = broker.find_order_by_tag("E1-PE")
        assert found is not None and found.broker_order_id == "B"

    def test_find_order_by_tag_none_when_absent(self):
        broker, _, _ = build_broker(FakeKiteClient(orders_result=[_kite_order(tag="E1-CE")]))
        assert broker.find_order_by_tag("NOPE") is None

    def test_find_order_by_tag_returns_most_recent_on_duplicate(self):
        broker, _, _ = build_broker(FakeKiteClient(orders_result=[
            _kite_order(order_id="OLD", tag="DUP", order_timestamp=datetime(2026, 7, 7, 9, 20)),
            _kite_order(order_id="NEW", tag="DUP", order_timestamp=datetime(2026, 7, 7, 9, 25)),
        ]))
        assert broker.find_order_by_tag("DUP").broker_order_id == "NEW"


class TestPortfolio:
    def test_positions(self):
        broker, _, _ = build_broker(FakeKiteClient(positions_result={"net": [
            {"exchange": "NFO", "tradingsymbol": "X", "product": "MIS", "quantity": -75,
             "average_price": 120, "last_price": 118, "pnl": 150},
        ], "day": []}))
        positions = broker.get_positions()
        assert len(positions) == 1 and positions[0].quantity == -75

    def test_margins_reads_equity_block(self):
        broker, _, _ = build_broker(FakeKiteClient(margins_result={
            "equity": {"available": {"cash": 500000, "opening_balance": 500000}, "utilised": {"debits": 0}},
            "commodity": {},
        }))
        margins = broker.get_margins()
        assert margins.available_cash == Decimal("500000")


class TestQuoteBatching:
    def test_quotes_chunked_and_merged(self):
        # batch size 2, 3 instruments -> 2 quote() calls, merged result.
        instruments = [InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=f"S{i}") for i in range(3)]
        quote_data = {f"NFO:S{i}": {"last_price": 100 + i, "ohlc": {}, "volume": 0, "timestamp": None} for i in range(3)}

        call_keys = []

        class ChunkClient(FakeKiteClient):
            def quote(self, *keys):
                call_keys.append(keys)
                return {k: quote_data[k] for k in keys}

        client = ChunkClient()
        session = KiteSession(client=client, api_secret="s", token_store=DictTokenStore())
        stream = KiteOrderUpdateStream(ticker_factory=lambda: FakeTicker())
        broker = KiteBroker(client=client, session=session, order_stream=stream,
                            config=KiteBrokerConfig(quote_batch_size=2), sleep=lambda s: None)

        result = broker.get_quote(instruments)
        assert len(result) == 3
        assert len(call_keys) == 2  # 2 + 1 -> two calls

    def test_ltp(self):
        instruments = [InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="X")]
        broker, _, _ = build_broker(FakeKiteClient(ltp_result={"NFO:X": {"last_price": 120.5}}))
        result = broker.get_ltp(instruments)
        assert result[instruments[0]] == Decimal("120.5")


class TestInstrumentLookup:
    def _instruments(self):
        return [
            {"instrument_token": 1, "exchange": "NFO", "tradingsymbol": "NIFTY26JUL24000CE", "name": "NIFTY",
             "lot_size": 75, "tick_size": 0.05, "expiry": date(2026, 7, 30), "strike": 24000.0, "instrument_type": "CE"},
            {"instrument_token": 2, "exchange": "NFO", "tradingsymbol": "NIFTY26JUL24000PE", "name": "NIFTY",
             "lot_size": 75, "tick_size": 0.05, "expiry": date(2026, 7, 30), "strike": 24000.0, "instrument_type": "PE"},
        ]

    def test_get_instrument_by_symbol(self):
        broker, _, _ = build_broker(FakeKiteClient(instruments_result=self._instruments()))
        inst = broker.get_instrument(Exchange.NFO, "NIFTY26JUL24000CE")
        assert inst.option_type is OptionType.CE

    def test_get_instrument_not_found(self):
        broker, _, _ = build_broker(FakeKiteClient(instruments_result=self._instruments()))
        with pytest.raises(InstrumentNotFoundError):
            broker.get_instrument(Exchange.NFO, "MISSING")

    def test_find_option_contract(self):
        broker, _, _ = build_broker(FakeKiteClient(instruments_result=self._instruments()))
        inst = broker.find_option_contract(
            underlying="NIFTY", expiry=date(2026, 7, 30), strike=Decimal("24000"),
            option_type=OptionType.PE, exchange=Exchange.NFO,
        )
        assert inst.tradingsymbol == "NIFTY26JUL24000PE"

    def test_instrument_dump_is_cached(self):
        client = FakeKiteClient(instruments_result=self._instruments())
        broker, _, _ = build_broker(client)
        broker.get_instrument(Exchange.NFO, "NIFTY26JUL24000CE")
        broker.get_instrument(Exchange.NFO, "NIFTY26JUL24000PE")
        assert client.instruments_call_count == 1  # loaded once, then cached


# --------------------------------------------------------------------------
# Read retry policy
# --------------------------------------------------------------------------


class TestReadRetry:
    def test_read_retries_on_transient_then_succeeds(self):
        attempts = {"n": 0}

        class FlakyClient(FakeKiteClient):
            def orders(self):
                attempts["n"] += 1
                if attempts["n"] < 3:
                    raise req_exc.ConnectionError("blip")
                return [_kite_order()]

        broker, _, _ = build_broker(FlakyClient())
        orders = broker.get_orders()
        assert len(orders) == 1
        assert attempts["n"] == 3  # retried twice, succeeded on the third

    def test_read_gives_up_after_max_attempts(self):
        class AlwaysDownClient(FakeKiteClient):
            def orders(self):
                raise req_exc.ConnectionError("down")

        broker, _, _ = build_broker(AlwaysDownClient())
        with pytest.raises(BrokerConnectionError):
            broker.get_orders()

    def test_read_does_not_retry_non_retryable(self):
        attempts = {"n": 0}

        class AuthFailClient(FakeKiteClient):
            def orders(self):
                attempts["n"] += 1
                raise kite_exc.TokenException("expired")

        broker, _, _ = build_broker(AuthFailClient())
        with pytest.raises(BrokerAuthenticationError):
            broker.get_orders()
        assert attempts["n"] == 1  # auth failure is not retried


# --------------------------------------------------------------------------
# Websocket
# --------------------------------------------------------------------------


class TestWebsocket:
    def test_connect_is_idempotent(self):
        broker, _, ticker = build_broker()
        broker.connect_websocket()
        broker.connect_websocket()
        assert ticker.connect_calls == 1

    def test_order_update_dispatched_to_callback(self):
        broker, _, ticker = build_broker()
        received = []
        broker.register_order_update_callback(received.append)
        broker.connect_websocket()
        # Simulate Kite pushing an order update through the ticker's callback.
        ticker.on_order_update(ticker, _kite_order(status="COMPLETE"))
        assert len(received) == 1 and received[0].status is OrderStatus.COMPLETE

    def test_malformed_update_is_dropped_not_raised(self):
        broker, _, ticker = build_broker()
        received = []
        broker.register_order_update_callback(received.append)
        broker.connect_websocket()
        ticker.on_order_update(ticker, {"garbage": True})  # missing required fields
        assert received == []  # dropped, no exception

    def test_callback_error_isolated(self):
        broker, _, ticker = build_broker()
        good = []

        def bad_cb(order):
            raise RuntimeError("subscriber bug")

        broker.register_order_update_callback(bad_cb)
        broker.register_order_update_callback(good.append)
        broker.connect_websocket()
        ticker.on_order_update(ticker, _kite_order())  # must not raise
        assert len(good) == 1  # the good callback still ran

    def test_connect_status_tracks_ticker_events(self):
        broker, _, ticker = build_broker()
        broker.connect_websocket()
        assert broker.is_websocket_connected() is False
        ticker.on_connect(ticker, {})
        assert broker.is_websocket_connected() is True
        broker.disconnect_websocket()
        assert broker.is_websocket_connected() is False
        assert ticker.close_calls == 1


def _place_request():
    return PlaceOrderRequest(
        exchange=Exchange.NFO, tradingsymbol="NIFTY24000CE", transaction_type=TransactionType.SELL,
        quantity=75, product=ProductType.INTRADAY, order_type=OrderType.MARKET, tag="E1-CE",
    )
