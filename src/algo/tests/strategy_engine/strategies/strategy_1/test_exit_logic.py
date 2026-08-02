"""Tests for Strategy-1 exit logic.

Pure decision and P&L functions are tested exhaustively with no I/O. Execution
is tested end-to-end against the real SimulationBroker + repositories on
in-memory SQLite (with the JSONB/BigInteger shims), reusing entry_logic to
establish a real OPEN position first, then exiting it.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
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
from algo.brokers.exceptions import OrderRejectedError
from algo.brokers.simulation import (
    InstrumentCatalog,
    SimulationBroker,
    SimulationConfig,
    StaticPriceSource,
)
from algo.common.enums import (
    Exchange,
    ExitReason,
    InstanceStatus,
    OptionType,
    PositionState,
    ProductType,
    TradeLegStatus,
)
from algo.database.models import Account, Base, StrategyInstance
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.strategy_instance_repository import (
    StrategyInstanceRepository,
)
from algo.services.instrument_service import InstrumentSpec
from algo.strategy_engine.strategies.strategy_1.config import RetrySettings, Strategy1Config
from algo.strategy_engine.strategies.strategy_1.entry_logic import EntryLogic, EntryOutcome
from algo.strategy_engine.strategies.strategy_1.exit_logic import (
    ExitLogic,
    ExitOutcome,
    compute_leg_realized_pnl,
    evaluate_exit,
)
from algo.strategy_engine.strategies.strategy_1.strike_selector import StrikeSelector
from algo.strategy_engine.strategy_context import RiskDecision, StrategyContext, StrategyIdentity

INSTRUMENT = "NIFTY"
EXPIRY = date(2026, 7, 30)
TODAY = date(2026, 7, 7)
ATM = Decimal("24000")
SPOT = Decimal("24010")
LOT_SIZE = 75
CE_ENTRY = Decimal("120")
PE_ENTRY = Decimal("110")


# --------------------------------------------------------------------------
# Pure: evaluate_exit priority
# --------------------------------------------------------------------------

CUTOFF = time(15, 15)
BEFORE_CUTOFF = time(10, 0)
AT_CUTOFF = time(15, 15)
AFTER_CUTOFF = time(15, 30)
TARGET = Decimal("207")  # entry 230 * 0.90
STOPLOSS = Decimal("253")  # entry 230 * 1.10


class TestEvaluateExitPriority:
    def _eval(self, **kw):
        base = dict(
            now_ist_time=BEFORE_CUTOFF,
            hard_cutoff_time=CUTOFF,
            halted=False,
            combined_premium=Decimal("230"),
            target_premium=TARGET,
            stoploss_premium=STOPLOSS,
        )
        base.update(kw)
        return evaluate_exit(**base)

    def test_no_trigger_when_premium_between_levels(self):
        d = self._eval(combined_premium=Decimal("230"))
        assert d.should_exit is False and d.reason is None

    def test_target_fires_when_premium_at_or_below_target(self):
        assert self._eval(combined_premium=TARGET).reason is ExitReason.TARGET
        assert self._eval(combined_premium=Decimal("200")).reason is ExitReason.TARGET

    def test_stoploss_fires_when_premium_at_or_above_stoploss(self):
        assert self._eval(combined_premium=STOPLOSS).reason is ExitReason.STOPLOSS
        assert self._eval(combined_premium=Decimal("300")).reason is ExitReason.STOPLOSS

    def test_cutoff_beats_everything_including_stoploss(self):
        # Past cutoff AND premium above stoploss -> TIMEOUT wins (highest).
        d = self._eval(now_ist_time=AFTER_CUTOFF, combined_premium=Decimal("300"))
        assert d.reason is ExitReason.TIMEOUT

    def test_cutoff_fires_at_exact_cutoff_time(self):
        assert self._eval(now_ist_time=AT_CUTOFF).reason is ExitReason.TIMEOUT

    def test_kill_switch_beats_stoploss_and_target(self):
        d = self._eval(halted=True, combined_premium=Decimal("300"))  # would be stoploss
        assert d.reason is ExitReason.KILL_SWITCH

    def test_cutoff_beats_kill_switch(self):
        d = self._eval(now_ist_time=AFTER_CUTOFF, halted=True)
        assert d.reason is ExitReason.TIMEOUT

    def test_cutoff_fires_even_without_a_premium(self):
        d = self._eval(now_ist_time=AFTER_CUTOFF, combined_premium=None)
        assert d.reason is ExitReason.TIMEOUT

    def test_kill_switch_fires_even_without_a_premium(self):
        d = self._eval(halted=True, combined_premium=None)
        assert d.reason is ExitReason.KILL_SWITCH

    def test_no_premium_means_no_stoploss_or_target(self):
        d = self._eval(combined_premium=None)
        assert d.should_exit is False


class TestComputeLegRealizedPnl:
    def test_short_leg_profits_when_exit_below_entry(self):
        pnl = compute_leg_realized_pnl(entry_price=Decimal("120"), exit_price=Decimal("100"), quantity=75)
        assert pnl == Decimal("1500")  # (120 - 100) * 75

    def test_short_leg_loses_when_exit_above_entry(self):
        pnl = compute_leg_realized_pnl(entry_price=Decimal("120"), exit_price=Decimal("150"), quantity=75)
        assert pnl == Decimal("-2250")  # (120 - 150) * 75

    def test_flat_when_exit_equals_entry(self):
        assert compute_leg_realized_pnl(entry_price=Decimal("120"), exit_price=Decimal("120"), quantity=75) == Decimal("0")


# --------------------------------------------------------------------------
# Fakes + harness (open a real position via entry, then exit it)
# --------------------------------------------------------------------------


@dataclass
class FakeTime:
    _today: date = TODAY

    def now(self) -> datetime:
        return datetime(2026, 7, 7, 5, 0, tzinfo=timezone.utc)

    def now_ist(self) -> datetime:
        return datetime(2026, 7, 7, 10, 30, tzinfo=timezone.utc)

    def today(self) -> date:
        return self._today


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
            instrument=INSTRUMENT,
            exchange=Exchange.NFO,
            strike_interval=Decimal("50"),
            lot_size=LOT_SIZE,
            tick_size=Decimal("0.05"),
        )


class FakeMarketData:
    def subscribe(self, instruments): ...
    def unsubscribe(self, instruments): ...
    def get_ltp(self, instrument): return Decimal("0")
    def get_ltps(self, instruments): return {}
    def is_connected(self): return True


class RejectBuyBroker(SimulationBroker):
    """Force-rejects a BUY for one configured symbol -- to make one exit leg
    fail deterministically."""

    reject_buy_symbol: str | None = None

    def place_order(self, request, *, timeout=None):
        from algo.common.enums import TransactionType

        if (
            request.tradingsymbol == self.reject_buy_symbol
            and request.transaction_type == TransactionType.BUY
        ):
            raise OrderRejectedError(f"forced BUY rejection for {request.tradingsymbol}")
        return super().place_order(request, timeout=timeout)


@dataclass
class Harness:
    exit_logic: ExitLogic
    broker: SimulationBroker
    session_factory: sessionmaker
    instance_id: int
    ce_symbol: str
    pe_symbol: str
    price_source: StaticPriceSource
    ce_id: InstrumentIdentifier
    pe_id: InstrumentIdentifier


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


def build_open_position(*, reject_buy_symbol_selector=None, halted: bool = False) -> Harness:
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
        s.commit()
        instance_id = instance.id

    catalog = InstrumentCatalog.build_option_chain(
        underlying=INSTRUMENT, exchange=Exchange.NFO, expiry=EXPIRY,
        atm_strike=ATM, strike_interval=Decimal("50"), num_strikes_each_side=5, lot_size=LOT_SIZE,
    )
    ce = catalog.find_option(underlying=INSTRUMENT, expiry=EXPIRY, strike=ATM, option_type=OptionType.CE, exchange=Exchange.NFO)
    pe = catalog.find_option(underlying=INSTRUMENT, expiry=EXPIRY, strike=ATM, option_type=OptionType.PE, exchange=Exchange.NFO)
    ce_id = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=ce.tradingsymbol)
    pe_id = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=pe.tradingsymbol)
    price_source = StaticPriceSource({ce_id: CE_ENTRY, pe_id: PE_ENTRY})

    broker = RejectBuyBroker(
        instrument_catalog=catalog, price_source=price_source,
        config=SimulationConfig(synchronous=True), rng=random.Random(0),
    )
    broker.authenticate()

    identity = StrategyIdentity(
        instance_id=instance_id, strategy_id="strategy_1", instrument=INSTRUMENT,
        account_id=1, exchange=Exchange.NFO,
    )
    context = StrategyContext(
        identity=identity, config=_config(), session_factory=session_factory, broker=broker,
        market_data=FakeMarketData(), risk=FakeRisk(halted=halted), time=FakeTime(),
        logger=logging.LoggerAdapter(logging.getLogger("test.exit"), {}),
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

    if reject_buy_symbol_selector is not None:
        broker.reject_buy_symbol = reject_buy_symbol_selector(ce.tradingsymbol, pe.tradingsymbol)

    exit_logic = ExitLogic(
        context=context, fill_confirmation_attempts=3, fill_confirmation_delay=0.0,
        close_retry_attempts=2, close_retry_delay=0.0,
    )
    return Harness(
        exit_logic=exit_logic, broker=broker, session_factory=session_factory,
        instance_id=instance_id, ce_symbol=ce.tradingsymbol, pe_symbol=pe.tradingsymbol,
        price_source=price_source, ce_id=ce_id, pe_id=pe_id,
    )


def _position(h: Harness):
    with h.session_factory() as s:
        return PositionRepository(s).get_by_instance_and_date(h.instance_id, TODAY)


def _instance_status(h: Harness) -> InstanceStatus:
    with h.session_factory() as s:
        return StrategyInstanceRepository(s).get_by_id_or_raise(h.instance_id).status


# --------------------------------------------------------------------------
# Execution tests
# --------------------------------------------------------------------------


class TestHappyExit:
    def test_target_exit_closes_both_legs_with_profit(self):
        h = build_open_position()
        # Premium decays: buy back both legs cheaper than entry -> profit.
        h.price_source.set_price(h.ce_id, Decimal("60"))
        h.price_source.set_price(h.pe_id, Decimal("55"))

        result = h.exit_logic.exit(ExitReason.TARGET)

        assert result.outcome is ExitOutcome.EXITED
        position = _position(h)
        assert position.state is PositionState.CLOSED
        assert position.exit_reason is ExitReason.TARGET
        assert position.combined_exit_premium == Decimal("115")  # 60 + 55
        # entry 230, exit 115, qty 75 -> (230-115)*75 = 8625
        assert position.realized_pnl == Decimal("8625")
        assert result.realized_pnl == Decimal("8625")

    def test_stoploss_exit_records_loss(self):
        h = build_open_position()
        h.price_source.set_price(h.ce_id, Decimal("160"))
        h.price_source.set_price(h.pe_id, Decimal("150"))

        result = h.exit_logic.exit(ExitReason.STOPLOSS)

        assert result.outcome is ExitOutcome.EXITED
        # entry 230, exit 310, qty 75 -> (230-310)*75 = -6000
        assert result.realized_pnl == Decimal("-6000")
        assert _position(h).exit_reason is ExitReason.STOPLOSS

    def test_trades_marked_closed_with_per_leg_pnl(self):
        h = build_open_position()
        h.price_source.set_price(h.ce_id, Decimal("60"))
        h.price_source.set_price(h.pe_id, Decimal("55"))
        h.exit_logic.exit(ExitReason.TARGET)

        with h.session_factory() as s:
            repo = PositionRepository(s)
            position = repo.get_by_instance_and_date(h.instance_id, TODAY)
            trades = {t.option_type: t for t in repo.list_trades_for_position(position.id)}
        assert all(t.status is TradeLegStatus.CLOSED for t in trades.values())
        assert trades[OptionType.CE].realized_pnl == (CE_ENTRY - Decimal("60")) * LOT_SIZE
        assert trades[OptionType.PE].realized_pnl == (PE_ENTRY - Decimal("55")) * LOT_SIZE

    def test_broker_is_flat_after_exit(self):
        h = build_open_position()
        h.price_source.set_price(h.ce_id, Decimal("60"))
        h.price_source.set_price(h.pe_id, Decimal("55"))
        h.exit_logic.exit(ExitReason.TARGET)
        assert h.broker.get_positions() == []

    def test_kill_switch_exit_uses_kill_switch_reason_and_actor(self):
        h = build_open_position()
        h.exit_logic.exit(ExitReason.KILL_SWITCH)

        with h.session_factory() as s:
            repo = PositionRepository(s)
            position = repo.get_by_instance_and_date(h.instance_id, TODAY)
            transitions = repo.list_state_transitions(position.id)
        assert position.exit_reason is ExitReason.KILL_SWITCH
        # the EXIT_PENDING and CLOSED transitions were attributed to KILL_SWITCH
        from algo.common.enums import StateTransitionActor
        exit_transitions = [t for t in transitions if t.to_state in (PositionState.EXIT_PENDING, PositionState.CLOSED)]
        assert all(t.actor is StateTransitionActor.KILL_SWITCH for t in exit_transitions)


class TestIdempotency:
    def test_second_exit_is_skipped(self):
        h = build_open_position()
        h.price_source.set_price(h.ce_id, Decimal("60"))
        h.price_source.set_price(h.pe_id, Decimal("55"))

        first = h.exit_logic.exit(ExitReason.TARGET)
        second = h.exit_logic.exit(ExitReason.TARGET)

        assert first.outcome is ExitOutcome.EXITED
        assert second.outcome is ExitOutcome.SKIPPED_ALREADY_CLOSED

    def test_exit_when_no_position_returns_not_open(self):
        h = build_open_position()
        # Advance the clock to a day with no position; exit() finds nothing open.
        h.exit_logic._context.time._today = date(2026, 7, 8)  # type: ignore[attr-defined]
        result = h.exit_logic.exit(ExitReason.MANUAL)
        assert result.outcome is ExitOutcome.NOT_OPEN


class TestPartialExit:
    def test_one_leg_fails_to_close_errors_without_leaving_it_silently_open(self):
        # PE BUY (the close) is rejected -> PE stays open -> ERROR.
        h = build_open_position(reject_buy_symbol_selector=lambda ce, pe: pe)
        h.price_source.set_price(h.ce_id, Decimal("60"))
        h.price_source.set_price(h.pe_id, Decimal("55"))

        result = h.exit_logic.exit(ExitReason.TARGET)

        assert result.outcome is ExitOutcome.PARTIAL_EXIT_ERROR
        position = _position(h)
        assert position.state is PositionState.ERROR
        assert _instance_status(h) is InstanceStatus.FROZEN

        with h.session_factory() as s:
            repo = PositionRepository(s)
            trades = {t.option_type: t for t in repo.list_trades_for_position(position.id)}
        # CE closed, PE still open (the leg we could not flatten).
        assert trades[OptionType.CE].status is TradeLegStatus.CLOSED
        assert trades[OptionType.PE].status is TradeLegStatus.OPEN

        # Broker reflects the still-open PE short.
        broker_positions = h.broker.get_positions()
        assert len(broker_positions) == 1
        assert broker_positions[0].tradingsymbol == h.pe_symbol


class TestMoneyPathCloseFillPriceGuard:
    """Money-path integrity: a COMPLETE close with no usable fill price must not
    be recorded as a 0 exit (which would corrupt realized P&L / trade history);
    the position is left for reconciliation instead of finalized with wrong
    numbers."""

    def test_case_c_complete_close_without_price_does_not_finalize(self):
        from algo.common.enums import OrderStatus, TransactionType
        from algo.database.repositories.reconciliation_break_repository import (
            ReconciliationBreakRepository,
        )

        h = build_open_position()
        h.price_source.set_price(h.ce_id, Decimal("60"))
        h.price_source.set_price(h.pe_id, Decimal("55"))

        # After the real entry, make every COMPLETE close report no fill price.
        orig = h.broker.get_order

        def _no_close_price(broker_order_id, *, timeout=None):
            bo = orig(broker_order_id, timeout=timeout)
            if (
                bo is not None
                and bo.status is OrderStatus.COMPLETE
                and bo.transaction_type is TransactionType.BUY
            ):
                return bo.model_copy(update={"average_price": None})
            return bo

        h.broker.get_order = _no_close_price

        result = h.exit_logic.exit(ExitReason.TARGET)

        # Not exited: the position is frozen for reconciliation, NOT finalized
        # with a wrong (0-based) realized P&L or exit premium.
        assert result.outcome is not ExitOutcome.EXITED
        position = _position(h)
        assert position.state is PositionState.ERROR
        assert position.realized_pnl is None            # no wrong P&L written
        assert position.combined_exit_premium is None   # no wrong exit premium written
        assert _instance_status(h) is InstanceStatus.FROZEN
        with h.session_factory() as s:
            breaks = ReconciliationBreakRepository(s).list_for_position(position.id)
        assert len(breaks) >= 1                          # reconciliation path preserved

    def test_case_d_valid_close_price_exits_with_unchanged_math(self):
        h = build_open_position()
        h.price_source.set_price(h.ce_id, Decimal("60"))
        h.price_source.set_price(h.pe_id, Decimal("55"))

        result = h.exit_logic.exit(ExitReason.TARGET)

        assert result.outcome is ExitOutcome.EXITED
        position = _position(h)
        assert position.state is PositionState.CLOSED
        assert position.combined_exit_premium == Decimal("115")  # 60 + 55, unchanged
        assert position.realized_pnl == Decimal("8625")          # (230-115)*75, unchanged
