"""Reproduces, with real overlapping threads, the exact concurrency bug
reported against live paper trading: PositionMonitor's own price/time-based
exit trigger and the scheduler's independent cutoff trigger (both of which
route through ``PositionMonitor.poll_and_check``), or any other combination of
callers (websocket ticks, duplicate triggers, recovery), attempting to execute
``ExitLogic.exit()`` for the same position at the same time.

Before the fix, two such callers would race unlocked reads and optimistically-
locked writes of the same Position/Order rows: the loser's write was correctly
rejected by the version check, but nothing caught that rejection, so it
surfaced as an unhandled StaleDataError/ConcurrentModificationError that froze
the strategy instance -- or, worse, both callers could independently place a
duplicate BUY close order for the same leg.

``ExitLogic.exit()`` now claims a non-blocking, non-reentrant lock as its very
first action, before any database read. These tests force two real threads to
overlap inside a live ``exit()`` call (via a broker double that blocks the
first close order until released) and assert: no exception of any kind
propagates, exactly one call reaches ``ExitOutcome.EXITED``, every other
concurrent caller returns ``SKIPPED_EXIT_IN_PROGRESS`` synchronously without
touching the broker or the database, and exactly one close order per leg is
ever placed.
"""

from __future__ import annotations

import logging
import random
import threading
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal

from sqlalchemy import BigInteger, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


@compiles(BigInteger, "sqlite")
def _bigint_sqlite(type_, compiler, **kw):  # noqa: ANN001, ANN202
    return "INTEGER"


@compiles(JSONB, "sqlite")
def _jsonb_sqlite(type_, compiler, **kw):  # noqa: ANN001, ANN202
    return "JSON"


from algo.brokers.broker_base import InstrumentIdentifier, PlaceOrderResult
from algo.brokers.simulation import (
    InstrumentCatalog,
    SimulationBroker,
    SimulationConfig,
    StaticPriceSource,
)
from algo.common.enums import (
    Exchange,
    ExitReason,
    OptionType,
    PositionState,
    ProductType,
    TransactionType,
)
from algo.database.models import Account, Base, StrategyInstance
from algo.database.repositories.order_repository import OrderRepository
from algo.database.repositories.position_repository import PositionRepository
from algo.services.instrument_service import InstrumentSpec
from algo.strategy_engine.strategies.strategy_1.config import RetrySettings, Strategy1Config
from algo.strategy_engine.strategies.strategy_1.entry_logic import EntryLogic, EntryOutcome
from algo.strategy_engine.strategies.strategy_1.exit_logic import ExitLogic, ExitOutcome
from algo.strategy_engine.strategies.strategy_1.monitor import PositionMonitor
from algo.strategy_engine.strategies.strategy_1.strike_selector import StrikeSelector
from algo.strategy_engine.strategy_context import RiskDecision, StrategyContext, StrategyIdentity, Tick

INSTRUMENT = "NIFTY"
EXPIRY = date(2026, 7, 30)
TODAY = date(2026, 7, 7)
ATM = Decimal("24000")
SPOT = Decimal("24010")
LOT_SIZE = 75
CE_ENTRY = Decimal("120")
PE_ENTRY = Decimal("110")
CUTOFF = time(15, 15)
PAST_CUTOFF = time(15, 20)
BEFORE_CUTOFF = time(10, 0)
TARGET = Decimal("207")  # entry 230 * 0.90
STOPLOSS = Decimal("253")  # entry 230 * 1.10


@dataclass
class FakeTime:
    _now_ist_time: time = BEFORE_CUTOFF

    def now(self) -> datetime:
        return datetime(2026, 7, 7, 5, 0, tzinfo=timezone.utc)

    def now_ist(self) -> datetime:
        return datetime.combine(TODAY, self._now_ist_time, tzinfo=timezone.utc)

    def today(self) -> date:
        return TODAY


@dataclass
class FakeRisk:
    halted: bool = False

    def is_halted(self, identity) -> bool:
        return self.halted

    def approve_entry(self, identity, *, quantity: int) -> RiskDecision:
        return RiskDecision(approved=True)


@dataclass
class FakeSpot:
    def get_spot_ltp(self, instrument: str) -> Decimal:
        return SPOT


@dataclass
class FakeExpiry:
    def get_current_weekly_expiry(self, instrument: str, as_of: date) -> date:
        return EXPIRY


@dataclass
class FakeInstrumentSvc:
    def get_instrument_spec(self, instrument: str) -> InstrumentSpec:
        return InstrumentSpec(
            instrument=INSTRUMENT, exchange=Exchange.NFO, strike_interval=Decimal("50"),
            lot_size=LOT_SIZE, tick_size=Decimal("0.05"),
        )


class FakeMarketData:
    def subscribe(self, instruments): ...
    def unsubscribe(self, instruments): ...
    def get_ltp(self, instrument): return Decimal("0")
    def get_ltps(self, instruments): return {}
    def is_connected(self): return True


class BlockingCloseBroker(SimulationBroker):
    """A SimulationBroker whose first BUY (close) order placement blocks until
    released, so a test can deterministically force a second exit trigger to
    arrive while the first exit is genuinely still in flight -- proving the
    guard, not luck of thread scheduling.

    Also records every placed order's (tradingsymbol, transaction_type) so
    tests can assert no duplicate close was ever sent.
    """

    started_event: threading.Event | None = None
    release_event: threading.Event | None = None
    placed: list[tuple[str, TransactionType]] | None = None
    _blocked_once: bool = False

    def place_order(self, request, *, timeout=None) -> PlaceOrderResult:
        if self.placed is not None:
            self.placed.append((request.tradingsymbol, request.transaction_type))
        if not self._blocked_once and request.transaction_type == TransactionType.BUY:
            self._blocked_once = True
            assert self.started_event is not None and self.release_event is not None
            self.started_event.set()
            released = self.release_event.wait(timeout=5.0)
            assert released, "test setup bug: release_event was never set"
        return super().place_order(request, timeout=timeout)


@dataclass
class Harness:
    context: StrategyContext
    exit_logic: ExitLogic
    broker: BlockingCloseBroker
    session_factory: sessionmaker
    instance_id: int
    ce_symbol: str
    pe_symbol: str
    ce_id: InstrumentIdentifier
    pe_id: InstrumentIdentifier
    fake_time: FakeTime


def _config() -> Strategy1Config:
    return Strategy1Config(
        entry_time=time(9, 20),
        hard_cutoff_time=CUTOFF,
        target_pct=Decimal("0.10"),
        sl_pct=Decimal("0.10"),
        lots=1,
        product_type=ProductType.INTRADAY,
        skip_on_expiry_day=False,
        monitoring_interval_seconds=5.0,
        polling_interval_seconds=2.0,
        retry=RetrySettings(
            order_timeout_seconds=None,
            fill_confirmation_attempts=20,
            fill_confirmation_delay_seconds=0.001,
            close_retry_attempts=3,
            close_retry_delay_seconds=0.001,
        ),
    )


def build_open_position() -> Harness:
    # StaticPool + check_same_thread=False: a single shared in-memory database
    # usable safely from multiple real threads. Only the thread that wins the
    # exit lock ever reaches the database concurrently with itself -- every
    # loser returns before touching it -- so no genuine concurrent-write
    # contention is exercised here, only the guard.
    engine = create_engine(
        "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)

    with session_factory() as s:
        account = Account(broker="SIMULATION", display_name="test")
        s.add(account)
        s.flush()
        instance = StrategyInstance(
            strategy_id="strategy_1", instrument=INSTRUMENT, account_id=account.id, exchange=Exchange.NFO,
        )
        s.add(instance)
        s.commit()
        instance_id = instance.id

    catalog = InstrumentCatalog.build_option_chain(
        underlying=INSTRUMENT, exchange=Exchange.NFO, expiry=EXPIRY, atm_strike=ATM,
        strike_interval=Decimal("50"), num_strikes_each_side=5, lot_size=LOT_SIZE,
    )
    ce = catalog.find_option(underlying=INSTRUMENT, expiry=EXPIRY, strike=ATM, option_type=OptionType.CE, exchange=Exchange.NFO)
    pe = catalog.find_option(underlying=INSTRUMENT, expiry=EXPIRY, strike=ATM, option_type=OptionType.PE, exchange=Exchange.NFO)
    ce_id = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=ce.tradingsymbol)
    pe_id = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=pe.tradingsymbol)
    price_source = StaticPriceSource({ce_id: CE_ENTRY, pe_id: PE_ENTRY})

    broker = BlockingCloseBroker(
        instrument_catalog=catalog, price_source=price_source,
        config=SimulationConfig(synchronous=True), rng=random.Random(0),
    )
    broker.placed = []
    broker.authenticate()

    fake_time = FakeTime()
    identity = StrategyIdentity(
        instance_id=instance_id, strategy_id="strategy_1", instrument=INSTRUMENT,
        account_id=1, exchange=Exchange.NFO,
    )
    context = StrategyContext(
        identity=identity, config=_config(), session_factory=session_factory, broker=broker,
        market_data=FakeMarketData(), risk=FakeRisk(), time=fake_time,
        logger=logging.LoggerAdapter(logging.getLogger("test.exit.concurrency"), {}),
    )
    strike_selector = StrikeSelector(
        instrument_service=FakeInstrumentSvc(), expiry_service=FakeExpiry(), broker=broker
    )
    entry = EntryLogic(
        context=context, strike_selector=strike_selector, expiry_service=FakeExpiry(),
        spot_price_provider=FakeSpot(), fill_confirmation_attempts=3, fill_confirmation_delay=0.0,
    )
    result = entry.enter()
    assert result.outcome is EntryOutcome.ENTERED

    exit_logic = ExitLogic(
        context=context, fill_confirmation_attempts=3, fill_confirmation_delay=0.0,
        close_retry_attempts=2, close_retry_delay=0.0,
    )
    return Harness(
        context=context, exit_logic=exit_logic, broker=broker, session_factory=session_factory,
        instance_id=instance_id, ce_symbol=ce.tradingsymbol, pe_symbol=pe.tradingsymbol,
        ce_id=ce_id, pe_id=pe_id, fake_time=fake_time,
    )


def _position(h: Harness):
    with h.session_factory() as s:
        return PositionRepository(s).get_by_instance_and_date(h.instance_id, TODAY)


def _prime_blocking(h: Harness) -> None:
    h.broker.started_event = threading.Event()
    h.broker.release_event = threading.Event()


def _close_orders_placed(h: Harness) -> list[tuple[str, TransactionType]]:
    assert h.broker.placed is not None
    return [p for p in h.broker.placed if p[1] == TransactionType.BUY]


class TestConcurrentExitsAreMutuallyExclusive:
    def test_monitor_and_cutoff_trigger_at_the_same_time(self):
        """The literal reported scenario: PositionMonitor's own evaluation and
        the scheduler's cutoff trigger both call poll_and_check() -- the exact
        same entry point on_time_trigger("cutoff") and on_monitor_cycle both
        use -- for the same position at (effectively) the same instant."""
        h = build_open_position()
        h.fake_time._now_ist_time = PAST_CUTOFF  # both callers see TIMEOUT due
        monitor = PositionMonitor(context=h.context, exit_logic=h.exit_logic)
        assert monitor.attach().value == "MONITORING"
        _prime_blocking(h)

        results: list[Exception | None] = [None, None]

        def call_a():
            try:
                monitor.poll_and_check()
            except Exception as exc:  # noqa: BLE001
                results[0] = exc

        def call_b():
            h.broker.started_event.wait(timeout=5.0)
            try:
                monitor.poll_and_check()
            except Exception as exc:  # noqa: BLE001
                results[1] = exc
            finally:
                h.broker.release_event.set()

        t_a = threading.Thread(target=call_a)
        t_b = threading.Thread(target=call_b)
        t_a.start()
        t_b.start()
        t_a.join(timeout=10)
        t_b.join(timeout=10)

        assert results == [None, None], f"an exception propagated: {results}"
        position = _position(h)
        assert position.state is PositionState.CLOSED
        assert position.exit_reason is ExitReason.TIMEOUT
        # Exactly one close order per leg -- no duplicate from the second caller.
        assert len(_close_orders_placed(h)) == 2

    def test_monitor_price_trigger_and_websocket_tick_at_the_same_time(self):
        """A pushed websocket tick (on_tick) and the monitor's own poll both
        decide to exit concurrently -- push vs. pull racing exactly as the
        push-vs-poll order-fill race did on the entry side."""
        h = build_open_position()
        # Stoploss-triggering price, evaluated pre-cutoff via a tick.
        monitor = PositionMonitor(context=h.context, exit_logic=h.exit_logic)
        assert monitor.attach().value == "MONITORING"
        # Feed both legs once so on_tick's combined premium is known and above
        # stoploss, then arrange for poll_and_check to independently observe
        # the identical stale-but-triggering state.
        now = h.context.time.now()
        monitor.on_tick(Tick(instrument=h.ce_id, last_price=Decimal("160"), timestamp=now))
        _prime_blocking(h)

        results: list[Exception | None] = [None, None]

        def call_a():
            # The tick that pushes PE's price over stoploss and fires the exit.
            try:
                monitor.on_tick(Tick(instrument=h.pe_id, last_price=Decimal("150"), timestamp=now))
            except Exception as exc:  # noqa: BLE001
                results[0] = exc

        def call_b():
            h.broker.started_event.wait(timeout=5.0)
            try:
                monitor.poll_and_check()
            except Exception as exc:  # noqa: BLE001
                results[1] = exc
            finally:
                h.broker.release_event.set()

        t_a = threading.Thread(target=call_a)
        t_b = threading.Thread(target=call_b)
        t_a.start()
        t_b.start()
        t_a.join(timeout=10)
        t_b.join(timeout=10)

        assert results == [None, None], f"an exception propagated: {results}"
        position = _position(h)
        assert position.state is PositionState.CLOSED
        assert len(_close_orders_placed(h)) == 2

    def test_double_timeout_direct_calls_race(self):
        """Two threads both call exit_logic.exit(TIMEOUT) directly, bypassing
        the monitor entirely -- proves the guard lives in ExitLogic itself,
        not something the monitor layers on top."""
        h = build_open_position()
        _prime_blocking(h)
        outcomes: list[ExitOutcome | None] = [None, None]
        errors: list[Exception | None] = [None, None]

        def call(idx: int, wait_for_start: bool):
            if wait_for_start:
                h.broker.started_event.wait(timeout=5.0)
            try:
                outcomes[idx] = h.exit_logic.exit(ExitReason.TIMEOUT).outcome
            except Exception as exc:  # noqa: BLE001
                errors[idx] = exc
            finally:
                if wait_for_start:
                    h.broker.release_event.set()

        t_a = threading.Thread(target=call, args=(0, False))
        t_b = threading.Thread(target=call, args=(1, True))
        t_a.start()
        t_b.start()
        t_a.join(timeout=10)
        t_b.join(timeout=10)

        assert errors == [None, None], f"an exception propagated: {errors}"
        assert sorted(o.value for o in outcomes) == sorted(
            [ExitOutcome.EXITED.value, ExitOutcome.SKIPPED_EXIT_IN_PROGRESS.value]
        )
        assert len(_close_orders_placed(h)) == 2

    def test_duplicate_exit_requests_with_different_reasons(self):
        """Two independent subsystems request an exit for *different* reasons
        (e.g. TARGET vs. STOPLOSS) at the same instant -- only one may win,
        and the other must be told to stand down, not silently double-execute."""
        h = build_open_position()
        _prime_blocking(h)
        outcomes: list[ExitOutcome | None] = [None, None]
        errors: list[Exception | None] = [None, None]

        def call(idx: int, reason: ExitReason, wait_for_start: bool):
            if wait_for_start:
                h.broker.started_event.wait(timeout=5.0)
            try:
                outcomes[idx] = h.exit_logic.exit(reason).outcome
            except Exception as exc:  # noqa: BLE001
                errors[idx] = exc
            finally:
                if wait_for_start:
                    h.broker.release_event.set()

        t_a = threading.Thread(target=call, args=(0, ExitReason.TARGET, False))
        t_b = threading.Thread(target=call, args=(1, ExitReason.STOPLOSS, True))
        t_a.start()
        t_b.start()
        t_a.join(timeout=10)
        t_b.join(timeout=10)

        assert errors == [None, None], f"an exception propagated: {errors}"
        assert sorted(o.value for o in outcomes) == sorted(
            [ExitOutcome.EXITED.value, ExitOutcome.SKIPPED_EXIT_IN_PROGRESS.value]
        )
        position = _position(h)
        assert position.state is PositionState.CLOSED
        # The winning reason -- whichever thread actually acquired the guard --
        # is the one persisted; exactly one, never overwritten by the loser.
        assert position.exit_reason in (ExitReason.TARGET, ExitReason.STOPLOSS)
        assert len(_close_orders_placed(h)) == 2

    def test_repeated_market_ticks_during_an_in_flight_exit(self):
        """A burst of live ticks arrives, each independently able to trigger an
        exit, while an exit for this same position is already executing.
        None of them may start a second exit, and none may raise."""
        h = build_open_position()
        monitor = PositionMonitor(context=h.context, exit_logic=h.exit_logic)
        assert monitor.attach().value == "MONITORING"
        now = h.context.time.now()
        _prime_blocking(h)
        errors: list[Exception] = []
        errors_lock = threading.Lock()

        def start_exit():
            try:
                # Stoploss-triggering price kicks off the in-flight exit.
                monitor.on_tick(Tick(instrument=h.ce_id, last_price=Decimal("160"), timestamp=now))
                monitor.on_tick(Tick(instrument=h.pe_id, last_price=Decimal("150"), timestamp=now))
            except Exception as exc:  # noqa: BLE001
                with errors_lock:
                    errors.append(exc)

        t_start = threading.Thread(target=start_exit)
        t_start.start()
        assert h.broker.started_event.wait(timeout=5.0), "exit never reached the broker call"

        def repeated_tick(i: int):
            try:
                monitor.on_tick(
                    Tick(instrument=h.ce_id, last_price=Decimal("160") + i, timestamp=now)
                )
            except Exception as exc:  # noqa: BLE001
                with errors_lock:
                    errors.append(exc)

        tick_threads = [threading.Thread(target=repeated_tick, args=(i,)) for i in range(20)]
        for th in tick_threads:
            th.start()
        for th in tick_threads:
            th.join(timeout=10)

        h.broker.release_event.set()
        t_start.join(timeout=10)

        assert errors == [], f"exceptions propagated from concurrent ticks: {errors}"
        position = _position(h)
        assert position.state is PositionState.CLOSED
        # Still exactly one close per leg despite 20 concurrent tick arrivals.
        assert len(_close_orders_placed(h)) == 2

    def test_exit_lock_returns_immediately_without_touching_the_database(self):
        """Direct proof the guard fires before any DB read: hold the lock
        manually (simulating an in-flight exit on another thread) and confirm
        exit() returns SKIPPED_EXIT_IN_PROGRESS synchronously, and neither the
        position nor its orders were touched (still OPEN, no exit intent/order
        beyond the two entry orders already placed by build_open_position)."""
        h = build_open_position()
        with h.session_factory() as s:
            orders_before = [o.id for o in OrderRepository(s).list_for_trade(1)]

        assert h.exit_logic._exit_lock.acquire(blocking=False)  # noqa: SLF001
        try:
            result = h.exit_logic.exit(ExitReason.MANUAL)
        finally:
            h.exit_logic._exit_lock.release()  # noqa: SLF001

        assert result.outcome is ExitOutcome.SKIPPED_EXIT_IN_PROGRESS
        position = _position(h)
        assert position.state is PositionState.OPEN
        with h.session_factory() as s:
            orders_after = [o.id for o in OrderRepository(s).list_for_trade(1)]
        assert orders_after == orders_before
