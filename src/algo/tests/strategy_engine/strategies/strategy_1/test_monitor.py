"""Tests for the Strategy-1 position monitor.

The monitor's job is to compute the live premium, delegate the exit *decision*
to exit_logic's pure evaluator, and delegate exit *execution* to exit_logic --
so it is tested with a FakeExitLogic (recording evaluate/exit calls) over a
minimal seeded OPEN position on in-memory SQLite. The exit mechanics themselves
are covered by test_exit_logic; here we verify the monitoring behaviour:
attach/recovery, tick-driven premium recompute, the polling fallback, the
price-independent triggers, single-fire, and graceful shutdown.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import BigInteger, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker


@compiles(BigInteger, "sqlite")
def _bigint_sqlite(type_, compiler, **kw):  # noqa: ANN001, ANN202
    return "INTEGER"


@compiles(JSONB, "sqlite")
def _jsonb_sqlite(type_, compiler, **kw):  # noqa: ANN001, ANN202
    return "JSON"


from algo.brokers.broker_base import InstrumentIdentifier
from algo.common.enums import (
    Exchange,
    ExitReason,
    OptionType,
    PositionState,
    ProductType,
    TradeLegStatus,
)
from algo.database.models import Account, Base, StrategyInstance
from algo.database.models.position import Position
from algo.database.models.trade import Trade
from algo.strategy_engine.strategies.strategy_1.config import RetrySettings, Strategy1Config
from algo.strategy_engine.strategies.strategy_1.exit_logic import ExitOutcome, ExitResult, evaluate_exit
from algo.strategy_engine.strategies.strategy_1.monitor import AttachOutcome, PositionMonitor
from algo.strategy_engine.strategy_context import StrategyContext, StrategyIdentity, Tick

INSTRUMENT = "NIFTY"
TODAY = date(2026, 7, 7)
CE_SYMBOL = "NIFTY26JUL3024000CE"
PE_SYMBOL = "NIFTY26JUL3024000PE"
CE_ID = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=CE_SYMBOL)
PE_ID = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=PE_SYMBOL)
TARGET = Decimal("207")
STOPLOSS = Decimal("253")


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


@dataclass
class MutableTime:
    ist: datetime = datetime(2026, 7, 7, 10, 0, tzinfo=timezone.utc)
    wall: datetime = datetime(2026, 7, 7, 4, 30, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self.wall

    def now_ist(self) -> datetime:
        return self.ist

    def today(self) -> date:
        return TODAY


@dataclass
class FakeRisk:
    halted: bool = False

    def is_halted(self, identity) -> bool:
        return self.halted

    def approve_entry(self, identity, *, quantity):
        from algo.strategy_engine.strategy_context import RiskDecision

        return RiskDecision(approved=True)


@dataclass
class FakeMarketData:
    connected: bool = True
    ltps: dict = field(default_factory=dict)
    subscribed: list = field(default_factory=list)
    unsubscribed: list = field(default_factory=list)
    poll_count: int = 0

    def subscribe(self, instruments):
        self.subscribed.append(list(instruments))

    def unsubscribe(self, instruments):
        self.unsubscribed.append(list(instruments))

    def get_ltp(self, instrument):
        return self.ltps[instrument]

    def get_ltps(self, instruments):
        self.poll_count += 1
        return {i: self.ltps[i] for i in instruments if i in self.ltps}

    def is_connected(self):
        return self.connected


@dataclass
class FakeExitLogic:
    """Records evaluate() inputs and exit() calls; uses the *real* pure
    evaluate_exit so priority behaviour is exercised faithfully."""

    hard_cutoff_time: time = time(15, 15)
    exit_calls: list = field(default_factory=list)
    exit_result: ExitResult = field(
        default_factory=lambda: ExitResult(ExitOutcome.EXITED, "closed", position_id=1, realized_pnl=Decimal("0"))
    )

    def evaluate(self, *, now_ist_time, halted, combined_premium, target_premium, stoploss_premium):
        return evaluate_exit(
            now_ist_time=now_ist_time,
            hard_cutoff_time=self.hard_cutoff_time,
            halted=halted,
            combined_premium=combined_premium,
            target_premium=target_premium,
            stoploss_premium=stoploss_premium,
        )

    def exit(self, reason: ExitReason) -> ExitResult:
        self.exit_calls.append(reason)
        return self.exit_result


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------


@dataclass
class Harness:
    monitor: PositionMonitor
    market_data: FakeMarketData
    exit_logic: FakeExitLogic
    time: MutableTime
    risk: FakeRisk
    session_factory: sessionmaker
    position_id: int


def _config() -> Strategy1Config:
    return Strategy1Config(
        entry_time=time(9, 20),
        hard_cutoff_time=time(15, 15),
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
            fill_confirmation_delay_seconds=0.25,
            close_retry_attempts=3,
            close_retry_delay_seconds=0.5,
        ),
    )


def build_monitor(*, state: PositionState = PositionState.OPEN, halted: bool = False, connected: bool = True) -> Harness:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)

    with session_factory() as s:
        account = Account(broker="SIMULATION", display_name="test")
        s.add(account)
        s.flush()
        instance = StrategyInstance(
            strategy_id="strategy_1", instrument=INSTRUMENT, account_id=account.id, exchange=Exchange.NFO
        )
        s.add(instance)
        s.flush()
        position = Position(
            strategy_instance_id=instance.id,
            trade_date=TODAY,
            state=state,
            strike=Decimal("24000"),
            lots=1,
            lot_size=75,
            quantity=75,
            target_pct=Decimal("0.10"),
            sl_pct=Decimal("0.10"),
            combined_entry_premium=Decimal("230"),
            target_premium=TARGET,
            stoploss_premium=STOPLOSS,
        )
        s.add(position)
        s.flush()
        for opt, sym in ((OptionType.CE, CE_SYMBOL), (OptionType.PE, PE_SYMBOL)):
            s.add(
                Trade(
                    position_id=position.id,
                    option_type=opt,
                    trading_symbol=sym,
                    exchange=Exchange.NFO,
                    strike=Decimal("24000"),
                    quantity=75,
                    entry_price=Decimal("120") if opt is OptionType.CE else Decimal("110"),
                    status=TradeLegStatus.OPEN,
                )
            )
        s.commit()
        instance_id = instance.id
        position_id = position.id

    time_provider = MutableTime()
    market_data = FakeMarketData(connected=connected)
    risk = FakeRisk(halted=halted)
    identity = StrategyIdentity(
        instance_id=instance_id, strategy_id="strategy_1", instrument=INSTRUMENT,
        account_id=1, exchange=Exchange.NFO,
    )
    context = StrategyContext(
        identity=identity, config=_config(), session_factory=session_factory,
        broker=object(), market_data=market_data, risk=risk, time=time_provider,
        logger=logging.LoggerAdapter(logging.getLogger("test.monitor"), {}),
    )
    exit_logic = FakeExitLogic()
    monitor = PositionMonitor(context=context, exit_logic=exit_logic, max_tick_staleness_seconds=5.0)
    return Harness(
        monitor=monitor, market_data=market_data, exit_logic=exit_logic, time=time_provider,
        risk=risk, session_factory=session_factory, position_id=position_id,
    )


def _tick(instrument, price, ts=None):
    return Tick(instrument=instrument, last_price=Decimal(str(price)), timestamp=ts or datetime(2026, 7, 7, 4, 30, tzinfo=timezone.utc))


# --------------------------------------------------------------------------
# Attach / recovery
# --------------------------------------------------------------------------


class TestAttach:
    def test_attach_open_position_starts_monitoring_and_subscribes(self):
        h = build_monitor(state=PositionState.OPEN)
        outcome = h.monitor.attach()
        assert outcome is AttachOutcome.MONITORING
        assert h.monitor.is_active
        assert h.market_data.subscribed == [[CE_ID, PE_ID]]

    def test_attach_closed_position_is_nothing_to_monitor(self):
        h = build_monitor(state=PositionState.CLOSED)
        assert h.monitor.attach() is AttachOutcome.NOTHING_TO_MONITOR
        assert not h.monitor.is_active

    def test_attach_exit_pending_reports_pending_exit(self):
        h = build_monitor(state=PositionState.EXIT_PENDING)
        assert h.monitor.attach() is AttachOutcome.PENDING_EXIT
        assert not h.monitor.is_active


# --------------------------------------------------------------------------
# Tick-driven evaluation
# --------------------------------------------------------------------------


class TestOnTick:
    def test_premium_recomputed_after_both_legs_tick(self):
        h = build_monitor()
        h.monitor.attach()
        h.monitor.on_tick(_tick(CE_ID, 120))
        assert h.monitor.last_combined_premium is None  # only one leg yet
        h.monitor.on_tick(_tick(PE_ID, 110))
        assert h.monitor.last_combined_premium == Decimal("230")
        assert h.exit_logic.exit_calls == []  # 230 is between target/stoploss

    def test_target_hit_triggers_exit(self):
        h = build_monitor()
        h.monitor.attach()
        h.monitor.on_tick(_tick(CE_ID, 100))
        h.monitor.on_tick(_tick(PE_ID, 100))  # combined 200 <= target 207
        assert h.exit_logic.exit_calls == [ExitReason.TARGET]
        assert not h.monitor.is_active  # deactivated after firing

    def test_stoploss_hit_triggers_exit(self):
        h = build_monitor()
        h.monitor.attach()
        h.monitor.on_tick(_tick(CE_ID, 140))
        h.monitor.on_tick(_tick(PE_ID, 120))  # combined 260 >= stoploss 253
        assert h.exit_logic.exit_calls == [ExitReason.STOPLOSS]

    def test_single_fire_only(self):
        h = build_monitor()
        h.monitor.attach()
        h.monitor.on_tick(_tick(CE_ID, 100))
        h.monitor.on_tick(_tick(PE_ID, 100))
        # further ticks after the exit fired must not re-trigger
        h.monitor.on_tick(_tick(CE_ID, 90))
        h.monitor.on_tick(_tick(PE_ID, 90))
        assert h.exit_logic.exit_calls == [ExitReason.TARGET]

    def test_ticks_ignored_when_not_active(self):
        h = build_monitor()
        # not attached
        h.monitor.on_tick(_tick(CE_ID, 100))
        assert h.exit_logic.exit_calls == []


# --------------------------------------------------------------------------
# Price-independent triggers + polling fallback
# --------------------------------------------------------------------------


class TestPollAndCheck:
    def test_time_cutoff_fires_without_any_premium(self):
        h = build_monitor()
        h.monitor.attach()
        h.time.ist = datetime(2026, 7, 7, 15, 20, tzinfo=timezone.utc)  # past 15:15 cutoff
        h.monitor.poll_and_check()
        assert h.exit_logic.exit_calls == [ExitReason.TIMEOUT]

    def test_kill_switch_fires_without_any_premium(self):
        h = build_monitor()
        h.monitor.attach()
        h.risk.halted = True
        h.monitor.poll_and_check()
        assert h.exit_logic.exit_calls == [ExitReason.KILL_SWITCH]

    def test_polls_when_websocket_disconnected(self):
        h = build_monitor(connected=False)
        h.market_data.ltps = {CE_ID: Decimal("100"), PE_ID: Decimal("100")}
        h.monitor.attach()
        h.monitor.poll_and_check()
        assert h.market_data.poll_count == 1
        # polled premium 200 <= target -> exit
        assert h.exit_logic.exit_calls == [ExitReason.TARGET]

    def test_polls_when_ticks_are_stale(self):
        h = build_monitor(connected=True)
        h.market_data.ltps = {CE_ID: Decimal("100"), PE_ID: Decimal("100")}
        h.monitor.attach()
        # a fresh tick sets the freshness clock
        h.monitor.on_tick(_tick(CE_ID, 120))
        h.monitor.on_tick(_tick(PE_ID, 110))
        # advance wall clock beyond staleness budget
        h.time.wall = h.time.wall + timedelta(seconds=10)
        h.monitor.poll_and_check()
        assert h.market_data.poll_count == 1

    def test_does_not_poll_when_ticks_fresh(self):
        h = build_monitor(connected=True)
        h.market_data.ltps = {CE_ID: Decimal("120"), PE_ID: Decimal("110")}
        h.monitor.attach()
        h.monitor.on_tick(_tick(CE_ID, 120))
        h.monitor.on_tick(_tick(PE_ID, 110))
        h.monitor.poll_and_check()  # fresh -> no poll
        assert h.market_data.poll_count == 0

    def test_poll_failure_still_evaluates_time_cutoff(self):
        h = build_monitor(connected=False)
        h.market_data.ltps = {}  # get_ltps returns empty -> no premium
        h.monitor.attach()
        h.time.ist = datetime(2026, 7, 7, 15, 30, tzinfo=timezone.utc)
        h.monitor.poll_and_check()
        # cutoff still fires despite no usable market data
        assert h.exit_logic.exit_calls == [ExitReason.TIMEOUT]


# --------------------------------------------------------------------------
# Shutdown
# --------------------------------------------------------------------------


class TestShutdown:
    def test_stop_unsubscribes_and_deactivates(self):
        h = build_monitor()
        h.monitor.attach()
        h.monitor.stop()
        assert not h.monitor.is_active
        assert h.market_data.unsubscribed == [[CE_ID, PE_ID]]

    def test_stop_does_not_close_position(self):
        h = build_monitor()
        h.monitor.attach()
        h.monitor.stop()
        assert h.exit_logic.exit_calls == []  # stopping is not exiting

    def test_stop_is_idempotent(self):
        h = build_monitor()
        h.monitor.attach()
        h.monitor.stop()
        h.monitor.stop()  # no error, no double unsubscribe
        assert len(h.market_data.unsubscribed) == 1
