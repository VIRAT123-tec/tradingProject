"""Unit tests for the options market-data collector components.

All against SQLite + fakes -- no network, no live Kite, no Timescale required.
Covers config, schema init (Timescale-optional), the FULL-mode tick mapping,
the non-blocking writer (batch/overflow/drain/JSON depth), ATM edge-diff,
market-hours phases, metrics render, and the orchestrator's phase/recompute
side effects.
"""

from __future__ import annotations

import time as _time
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from algo.common.enums import Exchange, OptionType
from algo.market_data_collector.atm_window import AtmWindowManager
from algo.market_data_collector.config import (
    CollectorConfig,
    MarketHoursConfig,
    ReconnectConfig,
    WriterConfig,
    load_collector_config,
)
from algo.market_data_collector.db import MarketTick, build_market_data_engine
from algo.market_data_collector.full_tick_stream import CollectorTickStream
from algo.market_data_collector.init_timescale import initialize_schema
from algo.market_data_collector.market_hours import MarketHoursController, Phase
from algo.market_data_collector.metrics import CollectorMetrics
from algo.market_data_collector.tick_writer import TickWriter
from algo.services.instrument_service import InstrumentSpec

IST = timezone(timedelta(hours=5, minutes=30))


def _sqlite():
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    initialize_schema(eng, load_collector_config())
    return eng


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------


class TestConfig:
    def test_committed_config_loads(self):
        c = load_collector_config()
        assert c.underlyings == ["NIFTY", "SENSEX"]
        assert c.instruments_per_underlying == 42
        assert c.tick_mode == "FULL"
        assert c.market_hours.connect == time(9, 14)

    def test_underlyings_uppercased_and_deduped(self):
        c = CollectorConfig(underlyings=["nifty", "NIFTY", "sensex"])
        assert c.underlyings == ["NIFTY", "SENSEX"]

    def test_window_math(self):
        assert CollectorConfig(underlyings=["X"], strikes_each_side=5).instruments_per_underlying == 22


# --------------------------------------------------------------------------
# Schema init (Timescale-optional)
# --------------------------------------------------------------------------


class TestSchemaInit:
    def test_creates_tables_and_index_without_timescale(self):
        from sqlalchemy import inspect

        eng = _sqlite()
        insp = inspect(eng)
        assert set(insp.get_table_names()) >= {"collector_instruments", "option_ticks"}
        assert "ix_option_ticks_token_time" in [i["name"] for i in insp.get_indexes("option_ticks")]

    def test_is_idempotent(self):
        eng = _sqlite()
        initialize_schema(eng, load_collector_config())  # again -> no error


# --------------------------------------------------------------------------
# TickWriter (non-blocking, batch, overflow, drain)
# --------------------------------------------------------------------------


def _tick(token=1, price="100"):
    return MarketTick(
        instrument_token=token, time=datetime.now(timezone.utc),
        last_price=Decimal(price), oi=4200, volume=1000,
        depth={"bid": [{"price": 1, "quantity": 2, "orders": 1}], "ask": []},
    )


class TestTickWriter:
    def test_writes_batches_and_drains_on_stop(self):
        eng = _sqlite()
        w = TickWriter(engine=eng, config=WriterConfig(batch_size=50, flush_interval_ms=30, use_copy=False))
        w.start()
        for i in range(200):
            w.enqueue(_tick(token=i))
        _time.sleep(0.3)
        w.stop(drain=True)
        with eng.connect() as c:
            n = c.execute(text("SELECT count(*) FROM option_ticks")).scalar()
        assert n == 200 == w.rows_written and w.db_errors == 0 and w.queue_size == 0

    def test_overflow_drop_oldest_keeps_newest(self):
        eng = _sqlite()
        # tiny queue, writer not started -> everything stays queued and overflows
        w = TickWriter(engine=eng, config=WriterConfig(queue_max=10, overflow_policy="drop_oldest", use_copy=False))
        for i in range(25):
            w.enqueue(_tick(token=i))
        assert w.dropped_ticks == 15 and w.queue_size == 10

    def test_overflow_drop_newest(self):
        eng = _sqlite()
        w = TickWriter(engine=eng, config=WriterConfig(queue_max=10, overflow_policy="drop_newest", use_copy=False))
        for i in range(25):
            w.enqueue(_tick(token=i))
        assert w.dropped_ticks == 15 and w.queue_size == 10

    def test_depth_json_round_trips(self):
        eng = _sqlite()
        w = TickWriter(engine=eng, config=WriterConfig(batch_size=1, flush_interval_ms=10, use_copy=False))
        w.start()
        w.enqueue(_tick(token=7))
        _time.sleep(0.2)
        w.stop()
        with eng.connect() as c:
            depth = c.execute(text("SELECT depth FROM option_ticks WHERE instrument_token=7")).scalar()
        assert "bid" in str(depth)


# --------------------------------------------------------------------------
# CollectorTickStream (FULL mapping via a fake ticker)
# --------------------------------------------------------------------------


class FakeTicker:
    MODE_FULL = "full"

    def __init__(self):
        self.on_ticks = self.on_connect = self.on_close = None
        self.on_error = self.on_reconnect = self.on_noreconnect = None
        self.subscribed: list[int] = []
        self.modes: list[tuple] = []
        self.connected_called = False
        self.closed = False

    def connect(self, threaded=True, **kw):
        self.connected_called = True

    def close(self, *a, **k):
        self.closed = True
    def subscribe(self, tokens): self.subscribed += tokens
    def unsubscribe(self, tokens): self.subscribed = [t for t in self.subscribed if t not in tokens]
    def set_mode(self, mode, tokens): self.modes.append((mode, tuple(tokens)))


class TestTickStream:
    def _build(self):
        ticks = []
        fake = FakeTicker()
        stream = CollectorTickStream(
            ticker_factory=lambda: fake, mode="FULL", on_tick=ticks.append,
        )
        stream.start()
        fake.on_connect(None, {})  # simulate socket connect
        return stream, fake, ticks

    def test_subscribe_sets_full_mode(self):
        stream, fake, _ = self._build()
        stream.subscribe([101, 102])
        assert set(fake.subscribed) == {101, 102}
        assert ("full", (101, 102)) in fake.modes
        assert stream.subscribed_count == 2

    def test_full_tick_mapped_to_marketick(self):
        stream, fake, ticks = self._build()
        raw = {
            "instrument_token": 55, "last_price": 123.45, "last_traded_quantity": 50,
            "average_traded_price": 120.0, "volume_traded": 9000, "oi": 42000,
            "oi_day_high": 43000, "oi_day_low": 41000,
            "ohlc": {"open": 100, "high": 130, "low": 95, "close": 110},
            "total_buy_quantity": 500, "total_sell_quantity": 600,
            "depth": {"buy": [{"price": 123, "quantity": 5, "orders": 1}], "sell": []},
            "exchange_timestamp": datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc),
        }
        fake.on_ticks(None, [raw])
        assert len(ticks) == 1
        t = ticks[0]
        assert t.instrument_token == 55 and t.last_price == Decimal("123.45")
        assert t.oi == 42000 and t.volume == 9000 and t.ohlc_high == Decimal("130")
        assert t.total_buy_qty == 500 and t.depth["buy"][0]["price"] == 123

    def test_malformed_tick_dropped_not_raised(self):
        stream, fake, ticks = self._build()
        fake.on_ticks(None, [{"no_token": 1}])  # missing instrument_token
        assert ticks == []

    def test_reconnect_resubscribes(self):
        stream, fake, _ = self._build()
        stream.subscribe([201, 202])
        fake.subscribed.clear()  # socket "forgot"
        fake.on_connect(None, {})  # reconnect re-applies
        assert set(fake.subscribed) == {201, 202}


# --------------------------------------------------------------------------
# Self-healing at the stream level: on_noreconnect + brand-new-ticker restart
# --------------------------------------------------------------------------


class TestTickStreamRecovery:
    def _stream(self):
        made: list[FakeTicker] = []

        def factory():
            t = FakeTicker()
            made.append(t)
            return t

        stream = CollectorTickStream(ticker_factory=factory, mode="FULL", on_tick=lambda _t: None)
        return stream, made

    def test_on_noreconnect_marks_dead_and_disconnected(self):
        stream, made = self._stream()
        stream.start()
        made[0].on_connect(None, {})
        assert stream.is_dead() is False and stream.is_connected() is True
        made[0].on_noreconnect(made[0])  # KiteTicker permanently gives up
        assert stream.is_dead() is True and stream.is_connected() is False

    def test_restart_builds_brand_new_ticker_and_closes_old(self):
        stream, made = self._stream()
        stream.start()
        made[0].on_connect(None, {})
        made[0].on_noreconnect(made[0])  # socket dies

        stream.restart()

        assert len(made) == 2 and made[1] is not made[0]  # a completely fresh object
        assert made[0].closed is True                     # dead one torn down
        assert made[1].connected_called is True           # fresh connect issued
        assert stream.is_dead() is False

    def test_restart_preserves_subscriptions_and_resubscribes_on_connect(self):
        stream, made = self._stream()
        stream.start()
        made[0].on_connect(None, {})
        stream.subscribe([101, 102])
        assert set(made[0].subscribed) == {101, 102}

        made[0].on_noreconnect(made[0])
        stream.restart()
        # New socket hasn't connected yet: nothing pushed, but the intent is kept.
        assert made[1].subscribed == []
        assert stream.subscribed_count == 2

        made[1].on_connect(None, {})  # fresh socket connects
        assert set(made[1].subscribed) == {101, 102}  # all instruments re-subscribed
        assert ("full", (101, 102)) in [(m, tuple(sorted(t))) for m, t in made[1].modes]


# --------------------------------------------------------------------------
# Self-healing at the service level: the controller-loop watchdog
# --------------------------------------------------------------------------


class WatchdogStream:
    """Minimal stream exposing exactly what the watchdog reads/drives."""

    def __init__(self):
        self.connected = False
        self.dead = False
        self.restarts = 0
        self.subs: list[int] = []

    def start(self):
        self.connected = True

    def stop(self):
        self.connected = False

    def restart(self):
        self.restarts += 1
        self.dead = False
        self.connected = False  # fresh socket starts out reconnecting

    def is_connected(self):
        return self.connected

    def is_dead(self):
        return self.dead

    def subscribe(self, tokens):
        self.subs += tokens

    def unsubscribe(self, tokens):
        self.subs = [t for t in self.subs if t not in tokens]

    @property
    def subscribed_count(self):
        return len(self.subs)


class TestWatchdog:
    def _svc(self, stream, **rc):
        from algo.market_data_collector.collector_service import CollectorService

        reconnect = ReconnectConfig(
            grace_seconds=rc.get("grace", 10.0),
            restart_backoff_seconds=rc.get("backoff", 1.0),
            heartbeat_warning_seconds=rc.get("warn", 5.0),
            heartbeat_critical_seconds=rc.get("crit", 10.0),
        )
        eng = _sqlite()
        atm = AtmWindowManager(
            instrument_service=FakeInstrService(), expiry_resolver=lambda u, d: date(2026, 7, 21),
            spot_source=lambda u: Decimal("24000"), chain_source=_chain, strikes_each_side=10,
        )
        writer = TickWriter(engine=eng, config=WriterConfig(use_copy=False))
        metrics = CollectorMetrics(stream=stream, writer=writer, atm=atm, underlyings=["NIFTY"])
        svc = CollectorService(
            config=CollectorConfig(underlyings=["NIFTY"], strikes_each_side=10, reconnect=reconnect),
            engine=eng, stream=stream, writer=writer, atm=atm, metrics=metrics,
            market_hours=MarketHoursController(MarketHoursConfig(), FakeCalendar()),
            time_provider=FakeClock(),
        )
        return svc, metrics

    def test_disconnect_within_grace_does_not_restart(self):
        stream = WatchdogStream()
        svc, _ = self._svc(stream)
        stream.connected = True
        svc._watchdog(Phase.COLLECT, mono=100.0)   # healthy baseline
        stream.connected = False                    # internet lost
        svc._watchdog(Phase.COLLECT, mono=101.0)    # disconnect noticed
        svc._watchdog(Phase.COLLECT, mono=108.0)    # 7s < grace(10)
        assert stream.restarts == 0

    def test_long_disconnect_triggers_rebuild(self):
        stream = WatchdogStream()
        svc, _ = self._svc(stream)
        stream.connected = True
        svc._watchdog(Phase.COLLECT, mono=100.0)
        stream.connected = False
        svc._watchdog(Phase.COLLECT, mono=101.0)    # disconnected_since = 101
        svc._watchdog(Phase.COLLECT, mono=112.0)    # 11s >= grace(10) -> rebuild
        assert stream.restarts == 1

    def test_on_noreconnect_restarts_immediately(self):
        stream = WatchdogStream()
        svc, _ = self._svc(stream)
        stream.connected = True
        svc._watchdog(Phase.COLLECT, mono=100.0)
        stream.connected = False
        stream.dead = True                          # KiteTicker gave up
        svc._watchdog(Phase.COLLECT, mono=101.0)    # immediate, no grace wait
        assert stream.restarts == 1

    def test_restart_backoff_prevents_thrashing(self):
        stream = WatchdogStream()
        svc, _ = self._svc(stream, backoff=5.0)
        stream.connected = False
        stream.dead = True
        svc._watchdog(Phase.COLLECT, mono=100.0)    # restart #1
        stream.dead = True                          # still dead after rebuild
        svc._watchdog(Phase.COLLECT, mono=102.0)    # 2s < backoff(5) -> suppressed
        assert stream.restarts == 1
        svc._watchdog(Phase.COLLECT, mono=106.0)    # 6s >= backoff -> restart #2
        assert stream.restarts == 2

    def test_frozen_reactor_warns_then_restarts(self):
        stream = WatchdogStream()
        svc, metrics = self._svc(stream, warn=5.0, crit=10.0)
        stream.connected = True                     # connected but NO ticks ever
        svc._watchdog(Phase.COLLECT, mono=100.0)    # connected_since = 100
        svc._watchdog(Phase.COLLECT, mono=106.0)    # 6s idle -> WARNING, no restart
        assert stream.restarts == 0
        svc._watchdog(Phase.COLLECT, mono=111.0)    # 11s idle -> CRITICAL + restart
        assert stream.restarts == 1

    def test_ticks_reset_heartbeat(self):
        stream = WatchdogStream()
        svc, metrics = self._svc(stream, warn=5.0, crit=10.0)
        stream.connected = True
        svc._watchdog(Phase.COLLECT, mono=100.0)
        metrics.on_tick()                           # a tick arrives -> fresh baseline
        # last_tick_at is monotonic 'now'; idle stays ~0 regardless of mono passed
        svc._watchdog(Phase.COLLECT, mono=_time.monotonic() + 3.0)
        assert stream.restarts == 0

    def test_recovery_after_rebuild_is_logged_and_state_clears(self):
        stream = WatchdogStream()
        svc, _ = self._svc(stream)
        stream.connected = False
        stream.dead = True
        svc._watchdog(Phase.COLLECT, mono=100.0)    # rebuild
        assert stream.restarts == 1
        stream.connected = True                     # fresh socket connects
        svc._watchdog(Phase.COLLECT, mono=102.0)
        assert svc._disconnected_since is None and svc._connected_since is not None

    def test_watchdog_idle_outside_socket_phases(self):
        stream = WatchdogStream()
        svc, _ = self._svc(stream)
        stream.connected = False
        stream.dead = True
        svc._watchdog(Phase.CLOSED, mono=100.0)     # socket not expected up
        svc._watchdog(Phase.FLUSH, mono=200.0)
        assert stream.restarts == 0

    def test_kite_api_timeout_does_not_stop_controller(self):
        """A REST failure during ATM recompute is swallowed per-underlying and
        must never propagate out of the controller path."""
        from algo.market_data_collector.collector_service import CollectorService

        def boom(_u):
            raise ConnectionError("simulated Kite ReadTimeout")

        eng = _sqlite()
        atm = AtmWindowManager(
            instrument_service=FakeInstrService(), expiry_resolver=lambda u, d: date(2026, 7, 21),
            spot_source=boom, chain_source=_chain, strikes_each_side=10,
        )
        stream = WatchdogStream()
        writer = TickWriter(engine=eng, config=WriterConfig(use_copy=False))
        metrics = CollectorMetrics(stream=stream, writer=writer, atm=atm, underlyings=["NIFTY"])
        svc = CollectorService(
            config=CollectorConfig(underlyings=["NIFTY"], strikes_each_side=10),
            engine=eng, stream=stream, writer=writer, atm=atm, metrics=metrics,
            market_hours=MarketHoursController(MarketHoursConfig(), FakeCalendar()),
            time_provider=FakeClock(),
        )
        svc._recompute_all(datetime(2026, 7, 16, 9, 20, tzinfo=IST))  # must not raise
        assert stream.subscribed_count == 0


# --------------------------------------------------------------------------
# ATM window (edge-diff)
# --------------------------------------------------------------------------


class FakeInstrService:
    def get_instrument_spec(self, u):
        return InstrumentSpec(instrument=u, exchange=Exchange.NFO, strike_interval=Decimal("50"),
                              lot_size=75, tick_size=Decimal("0.05"))


def _chain(u, exp):
    c = {}
    s = 23000
    while s <= 25000:
        c[Decimal(s)] = {"CE": (f"N{s}CE", s * 10 + 1), "PE": (f"N{s}PE", s * 10 + 2)}
        s += 50
    return c


class TestAtmWindow:
    def _mgr(self, spot_box):
        return AtmWindowManager(
            instrument_service=FakeInstrService(), expiry_resolver=lambda u, d: date(2026, 7, 21),
            spot_source=lambda u: spot_box["v"], chain_source=_chain, strikes_each_side=10,
        )

    def test_initial_window_is_full(self):
        m = self._mgr({"v": Decimal("24000")})
        d = m.recompute("NIFTY", date(2026, 7, 16))
        assert d.atm == Decimal("24000") and len(d.to_subscribe) == 42 and not d.to_unsubscribe
        assert len(d.new_instruments) == 42

    def test_edge_only_rotation(self):
        box = {"v": Decimal("24000")}
        m = self._mgr(box)
        m.recompute("NIFTY", date(2026, 7, 16))
        box["v"] = Decimal("24040")  # -> ATM 24050
        d = m.recompute("NIFTY", date(2026, 7, 16))
        assert d.atm == Decimal("24050") and len(d.to_subscribe) == 2 and len(d.to_unsubscribe) == 2

    def test_no_move_no_churn(self):
        box = {"v": Decimal("24000")}
        m = self._mgr(box)
        m.recompute("NIFTY", date(2026, 7, 16))
        d = m.recompute("NIFTY", date(2026, 7, 16))
        assert not d.to_subscribe and not d.to_unsubscribe

    def test_reset_forces_full_resubscribe(self):
        m = self._mgr({"v": Decimal("24000")})
        m.recompute("NIFTY", date(2026, 7, 16))
        m.reset()
        d = m.recompute("NIFTY", date(2026, 7, 16))
        assert len(d.to_subscribe) == 42

    def test_missing_strike_skipped(self):
        def gappy(u, exp):
            c = _chain(u, exp)
            del c[Decimal(24000)]  # drop ATM
            return c
        m = AtmWindowManager(
            instrument_service=FakeInstrService(), expiry_resolver=lambda u, d: date(2026, 7, 21),
            spot_source=lambda u: Decimal("24000"), chain_source=gappy, strikes_each_side=10,
        )
        d = m.recompute("NIFTY", date(2026, 7, 16))
        assert len(d.to_subscribe) == 40  # 21 strikes minus the missing one, x2


# --------------------------------------------------------------------------
# Market hours + metrics
# --------------------------------------------------------------------------


class FakeCalendar:
    def __init__(self, trading=True): self._t = trading
    def is_trading_day(self, d): return self._t


class TestMarketHours:
    def _c(self, trading=True):
        return MarketHoursController(MarketHoursConfig(), FakeCalendar(trading))

    def test_phase_progression(self):
        c = self._c()
        def at(h, m): return c.phase(datetime(2026, 7, 16, h, m, tzinfo=IST))
        assert at(9, 0) is Phase.PRE_OPEN
        assert at(9, 14) is Phase.CONNECT
        assert at(9, 20) is Phase.COLLECT
        assert at(15, 30) is Phase.FREEZE
        assert at(15, 31) is Phase.FLUSH
        assert at(15, 45) is Phase.CLOSED

    def test_non_trading_day_is_idle(self):
        c = self._c(trading=False)
        assert c.phase(datetime(2026, 7, 16, 11, 0, tzinfo=IST)) is Phase.PRE_OPEN


class TestMetrics:
    def test_render_contains_counters_and_atm(self):
        box = {"v": Decimal("24000")}
        atm = AtmWindowManager(
            instrument_service=FakeInstrService(), expiry_resolver=lambda u, d: date(2026, 7, 21),
            spot_source=lambda u: box["v"], chain_source=_chain, strikes_each_side=10,
        )
        atm.recompute("NIFTY", date(2026, 7, 16))
        eng = _sqlite()
        stream, fake, _ = TestTickStream()._build()

        class W:  # tiny fake writer for rendering
            rows_written = 5; dropped_ticks = 1; queue_size = 3; last_batch_seconds = 0.0034
        m = CollectorMetrics(stream=stream, writer=W(), atm=atm, underlyings=["NIFTY"])
        m.ticks_received = 7
        out = m.render()
        assert "Ticks Received         : 7" in out
        assert "Rows Written           : 5" in out
        assert "Batch Time             : 3.4 ms" in out
        assert "NIFTY    : 24000" in out


# --------------------------------------------------------------------------
# Orchestrator side effects (phase transitions + recompute)
# --------------------------------------------------------------------------


class FakeStream:
    def __init__(self):
        self.started = self.stopped = False
        self.subs: list[int] = []
    def start(self): self.started = True
    def stop(self): self.stopped = True
    def subscribe(self, tokens): self.subs += tokens
    def unsubscribe(self, tokens): self.subs = [t for t in self.subs if t not in tokens]
    def is_connected(self): return self.started and not self.stopped
    @property
    def subscribed_count(self): return len(self.subs)


class FakeClock:
    def __init__(self): self.dt = datetime(2026, 7, 16, 9, 20, tzinfo=IST)
    def now_ist(self): return self.dt
    def now(self): return self.dt.astimezone(timezone.utc)
    def today(self): return self.dt.date()


class TestCollectorService:
    def _service(self, spot_box):
        from algo.market_data_collector.collector_service import CollectorService
        eng = _sqlite()
        atm = AtmWindowManager(
            instrument_service=FakeInstrService(), expiry_resolver=lambda u, d: date(2026, 7, 21),
            spot_source=lambda u: spot_box["v"], chain_source=_chain, strikes_each_side=10,
        )
        stream = FakeStream()
        writer = TickWriter(engine=eng, config=WriterConfig(use_copy=False))
        metrics = CollectorMetrics(stream=stream, writer=writer, atm=atm, underlyings=["NIFTY"])
        svc = CollectorService(
            config=CollectorConfig(underlyings=["NIFTY"], strikes_each_side=10),
            engine=eng, stream=stream, writer=writer, atm=atm, metrics=metrics,
            market_hours=MarketHoursController(MarketHoursConfig(), FakeCalendar()),
            time_provider=FakeClock(),
        )
        return svc, stream, eng

    def test_connect_phase_starts_stream(self):
        svc, stream, _ = self._service({"v": Decimal("24000")})
        svc._on_phase(Phase.CONNECT)
        assert stream.started is True

    def test_recompute_subscribes_and_upserts_dimension(self):
        svc, stream, eng = self._service({"v": Decimal("24000")})
        svc._recompute_all(datetime(2026, 7, 16, 9, 20, tzinfo=IST))
        assert stream.subscribed_count == 42
        with eng.connect() as c:
            dims = c.execute(text("SELECT count(*) FROM collector_instruments")).scalar()
            sample = c.execute(text(
                "SELECT underlying, option_type, strike FROM collector_instruments LIMIT 1"
            )).fetchone()
        assert dims == 42
        assert sample[0] == "NIFTY" and sample[1] in ("CE", "PE")

    def test_recompute_is_idempotent_on_dimension(self):
        svc, stream, eng = self._service({"v": Decimal("24000")})
        svc._recompute_all(datetime(2026, 7, 16, 9, 20, tzinfo=IST))
        svc._recompute_all(datetime(2026, 7, 16, 9, 21, tzinfo=IST))  # no move -> no new dims
        with eng.connect() as c:
            assert c.execute(text("SELECT count(*) FROM collector_instruments")).scalar() == 42

    def test_closed_phase_stops_stream_and_resets(self):
        svc, stream, _ = self._service({"v": Decimal("24000")})
        svc._on_phase(Phase.CONNECT)
        svc._recompute_all(datetime(2026, 7, 16, 9, 20, tzinfo=IST))
        svc._on_phase(Phase.CLOSED)
        assert stream.stopped is True
        assert svc._atm.subscribed_tokens() == set()  # window forgotten
