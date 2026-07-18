"""Tests for the TickStream implementations (H4): the polling-only default and
the Kite live adapter (driven by a fake ticker)."""

from __future__ import annotations

from decimal import Decimal

from algo.brokers.broker_base import InstrumentIdentifier
from algo.brokers.kite.market_ticker import KiteTickStream
from algo.common.enums import Exchange
from algo.market_data.polling_tick_stream import PollingTickStream
from algo.strategy_engine.strategy_context import Tick

CE = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTY26JUL0925000CE")
PE = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTY26JUL0925000PE")


class TestPollingTickStream:
    def test_reports_disconnected_so_reads_poll(self):
        s = PollingTickStream()
        assert s.is_connected() is False

    def test_lifecycle_and_subscribe_are_safe_noops(self):
        s = PollingTickStream()
        s.set_handlers(on_tick=lambda t: None, on_connect=lambda: None,
                       on_disconnect=lambda: None, on_reconnect=lambda: None)
        s.start()
        s.subscribe([CE, PE])
        s.unsubscribe([CE])
        s.stop()  # none of these raise


class FakeTicker:
    MODE_LTP = "ltp"

    def __init__(self):
        self.on_ticks = self.on_connect = self.on_close = self.on_error = self.on_reconnect = None
        self.subscribed: list[int] = []
        self.modes: list = []
        self.connected_called = False

    def connect(self, threaded=True, **kw):  # noqa: ANN001, ANN003, FBT002
        self.connected_called = True

    def close(self, *a, **k):
        pass

    def subscribe(self, tokens):
        self.subscribed.extend(tokens)

    def unsubscribe(self, tokens):
        for t in tokens:
            if t in self.subscribed:
                self.subscribed.remove(t)

    def set_mode(self, mode, tokens):
        self.modes.append((mode, tokens))


def _build(fake):
    tokens = {CE: 111, PE: 222}
    reverse = {111: CE, 222: PE}
    stream = KiteTickStream(
        ticker_factory=lambda: fake,
        token_for_instrument=lambda ident: tokens[ident],
        instrument_for_token=lambda tok: reverse.get(tok),
    )
    return stream


class TestKiteTickStream:
    def test_start_connects_and_wires_callbacks(self):
        fake = FakeTicker()
        stream = _build(fake)
        stream.start()
        assert fake.connected_called is True
        assert fake.on_ticks is not None

    def test_subscribe_resolves_tokens_and_sets_mode(self):
        fake = FakeTicker()
        stream = _build(fake)
        stream.start()
        stream.subscribe([CE, PE])
        assert sorted(fake.subscribed) == [111, 222]
        assert fake.modes  # LTP mode was set

    def test_incoming_tick_is_mapped_and_delivered(self):
        fake = FakeTicker()
        received: list[Tick] = []
        stream = _build(fake)
        stream.set_handlers(on_tick=received.append, on_connect=lambda: None,
                            on_disconnect=lambda: None, on_reconnect=lambda: None)
        stream.start()

        fake.on_ticks(None, [{"instrument_token": 111, "last_price": 123.45}])

        assert len(received) == 1
        assert received[0].instrument == CE
        assert received[0].last_price == Decimal("123.45")

    def test_unknown_token_tick_is_dropped(self):
        fake = FakeTicker()
        received: list[Tick] = []
        stream = _build(fake)
        stream.set_handlers(on_tick=received.append, on_connect=lambda: None,
                            on_disconnect=lambda: None, on_reconnect=lambda: None)
        stream.start()

        fake.on_ticks(None, [{"instrument_token": 999, "last_price": 1.0}])  # unknown token

        assert received == []

    def test_connect_callback_sets_connected_and_resubscribes(self):
        fake = FakeTicker()
        connects: list[int] = []
        stream = _build(fake)
        stream.set_handlers(on_tick=lambda t: None, on_connect=lambda: connects.append(1),
                            on_disconnect=lambda: None, on_reconnect=lambda: None)
        stream.start()
        stream.subscribe([CE])
        fake.subscribed.clear()  # simulate the socket forgetting on (re)connect

        fake.on_connect(None, {})

        assert stream.is_connected() is True
        assert connects == [1]
        assert 111 in fake.subscribed  # re-subscribed on connect

    def test_unsubscribe_removes_tokens(self):
        fake = FakeTicker()
        stream = _build(fake)
        stream.start()
        stream.subscribe([CE, PE])
        stream.unsubscribe([CE])
        assert fake.subscribed == [222]
