"""Integration tests for Strategy-1 entry logic.

Exercises the full orchestration end-to-end against the real, already-verified
SimulationBroker and real repositories on an in-memory SQLite database (with the
JSONB/BigInteger compile shims the DB layer uses for SQLite), plus small fakes
for the injected service seams. Covers the money-critical paths: happy entry,
idempotency, risk blocking, expiry-day skip, entry rejection, and the
partial-entry auto-unwind.
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
    InstanceStatus,
    OptionType,
    OrderStatus,
    PositionState,
    ProductType,
    TradeLegStatus,
)
from algo.database.models import Account, Base, Position, StrategyInstance
from algo.database.repositories.order_repository import OrderRepository
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.reconciliation_break_repository import (
    ReconciliationBreakRepository,
)
from algo.database.repositories.strategy_instance_repository import (
    StrategyInstanceRepository,
)
from algo.services.instrument_service import InstrumentSpec
from algo.strategy_engine.strategies.strategy_1.config import RetrySettings, Strategy1Config
from algo.strategy_engine.strategies.strategy_1.entry_logic import EntryLogic, EntryOutcome
from algo.strategy_engine.strategies.strategy_1.strike_selector import StrikeSelector
from algo.strategy_engine.strategy_context import RiskDecision, StrategyContext, StrategyIdentity

INSTRUMENT = "NIFTY"
EXPIRY = date(2026, 7, 30)
TODAY = date(2026, 7, 7)
ATM = Decimal("24000")
SPOT = Decimal("24010")  # rounds to ATM 24000
LOT_SIZE = 75
CE_PRICE = Decimal("120")
PE_PRICE = Decimal("110")


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


@dataclass
class FakeTime:
    _today: date = TODAY

    def now(self) -> datetime:
        return datetime(2026, 7, 7, 3, 50, tzinfo=timezone.utc)

    def now_ist(self) -> datetime:
        return datetime(2026, 7, 7, 9, 20, tzinfo=timezone.utc)

    def today(self) -> date:
        return self._today


@dataclass
class FakeRisk:
    halted: bool = False
    approved: bool = True
    reject_reason: str = "daily loss limit breached"

    def is_halted(self, identity) -> bool:
        return self.halted

    def approve_entry(self, identity, *, quantity: int) -> RiskDecision:
        if self.approved:
            return RiskDecision(approved=True)
        return RiskDecision(approved=False, reason=self.reject_reason)


@dataclass
class FakeSpot:
    price: Decimal = SPOT

    def get_spot_ltp(self, instrument: str) -> Decimal:
        return self.price


@dataclass
class FakeExpiry:
    expiry: date = EXPIRY

    def get_current_weekly_expiry(self, instrument: str, as_of: date) -> date:
        return self.expiry


@dataclass
class FakeInstrumentSvc:
    spec: InstrumentSpec

    def get_instrument_spec(self, instrument: str) -> InstrumentSpec:
        return self.spec


class FakeMarketData:
    def subscribe(self, instruments): ...
    def unsubscribe(self, instruments): ...
    def get_ltp(self, instrument): return Decimal("0")
    def get_ltps(self, instruments): return {}
    def is_connected(self): return True


class RejectLegBroker(SimulationBroker):
    """SimulationBroker that force-rejects a SELL for one configured symbol,
    to deterministically produce first-leg-rejection and partial-entry cases.
    A BUY (the auto-unwind) is never rejected."""

    reject_sell_symbol: str | None = None

    def place_order(self, request, *, timeout=None):
        from algo.common.enums import TransactionType

        if (
            request.tradingsymbol == self.reject_sell_symbol
            and request.transaction_type == TransactionType.SELL
        ):
            raise OrderRejectedError(f"forced rejection for {request.tradingsymbol}")
        return super().place_order(request, timeout=timeout)


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------


@dataclass
class Harness:
    entry_logic: EntryLogic
    broker: SimulationBroker
    session_factory: sessionmaker
    instance_id: int
    ce_symbol: str
    pe_symbol: str


def _config(**overrides) -> Strategy1Config:
    base = dict(
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
    base.update(overrides)
    return Strategy1Config(**base)


def build_harness(
    *,
    config: Strategy1Config | None = None,
    halted: bool = False,
    approved: bool = True,
    today: date = TODAY,
    reject_sell_symbol_selector=None,
) -> Harness:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)

    with session_factory() as s:
        account = Account(broker="SIMULATION", display_name="test")
        s.add(account)
        s.flush()
        instance = StrategyInstance(
            strategy_id="strategy_1",
            instrument=INSTRUMENT,
            account_id=account.id,
            exchange=Exchange.NFO,
        )
        s.add(instance)
        s.commit()
        instance_id = instance.id

    catalog = InstrumentCatalog.build_option_chain(
        underlying=INSTRUMENT,
        exchange=Exchange.NFO,
        expiry=EXPIRY,
        atm_strike=ATM,
        strike_interval=Decimal("50"),
        num_strikes_each_side=5,
        lot_size=LOT_SIZE,
    )
    ce = catalog.find_option(
        underlying=INSTRUMENT, expiry=EXPIRY, strike=ATM, option_type=OptionType.CE, exchange=Exchange.NFO
    )
    pe = catalog.find_option(
        underlying=INSTRUMENT, expiry=EXPIRY, strike=ATM, option_type=OptionType.PE, exchange=Exchange.NFO
    )
    ce_id = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=ce.tradingsymbol)
    pe_id = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=pe.tradingsymbol)
    price_source = StaticPriceSource({ce_id: CE_PRICE, pe_id: PE_PRICE})

    broker = RejectLegBroker(
        instrument_catalog=catalog,
        price_source=price_source,
        config=SimulationConfig(synchronous=True),
        rng=random.Random(0),
    )
    broker.authenticate()
    if reject_sell_symbol_selector is not None:
        broker.reject_sell_symbol = reject_sell_symbol_selector(ce.tradingsymbol, pe.tradingsymbol)

    identity = StrategyIdentity(
        instance_id=instance_id,
        strategy_id="strategy_1",
        instrument=INSTRUMENT,
        account_id=1,
        exchange=Exchange.NFO,
    )
    context = StrategyContext(
        identity=identity,
        config=config or _config(),
        session_factory=session_factory,
        broker=broker,
        market_data=FakeMarketData(),
        risk=FakeRisk(halted=halted, approved=approved),
        time=FakeTime(_today=today),
        logger=logging.LoggerAdapter(logging.getLogger("test.entry"), {}),
    )
    strike_selector = StrikeSelector(
        instrument_service=FakeInstrumentSvc(
            InstrumentSpec(
                instrument=INSTRUMENT,
                exchange=Exchange.NFO,
                strike_interval=Decimal("50"),
                lot_size=LOT_SIZE,
                tick_size=Decimal("0.05"),
            )
        ),
        expiry_service=FakeExpiry(),
        broker=broker,
    )
    entry_logic = EntryLogic(
        context=context,
        strike_selector=strike_selector,
        expiry_service=FakeExpiry(),
        spot_price_provider=FakeSpot(),
        fill_confirmation_attempts=3,
        fill_confirmation_delay=0.0,
    )
    return Harness(
        entry_logic=entry_logic,
        broker=broker,
        session_factory=session_factory,
        instance_id=instance_id,
        ce_symbol=ce.tradingsymbol,
        pe_symbol=pe.tradingsymbol,
    )


def _position(h: Harness):
    with h.session_factory() as s:
        return PositionRepository(s).get_by_instance_and_date(h.instance_id, TODAY)


def _instance_status(h: Harness) -> InstanceStatus:
    with h.session_factory() as s:
        return StrategyInstanceRepository(s).get_by_id_or_raise(h.instance_id).status


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------


class TestHappyPath:
    def test_both_legs_fill_and_position_opens(self):
        h = build_harness()
        result = h.entry_logic.enter()

        assert result.outcome is EntryOutcome.ENTERED
        position = _position(h)
        assert position.state is PositionState.OPEN
        assert position.combined_entry_premium == CE_PRICE + PE_PRICE  # 230
        assert position.target_premium == Decimal("207.00")  # 230 * 0.90
        assert position.stoploss_premium == Decimal("253.00")  # 230 * 1.10
        assert position.entry_completed_at is not None

    def test_trades_and_orders_and_intents_recorded(self):
        h = build_harness()
        h.entry_logic.enter()

        with h.session_factory() as s:
            repo = PositionRepository(s)
            position = repo.get_by_instance_and_date(h.instance_id, TODAY)
            trades = repo.list_trades_for_position(position.id)
            assert len(trades) == 2
            assert all(t.status is TradeLegStatus.OPEN for t in trades)
            assert {t.entry_price for t in trades} == {CE_PRICE, PE_PRICE}
            for t in trades:
                orders = OrderRepository(s).list_for_trade(t.id)
                assert len(orders) == 1
                assert orders[0].status is OrderStatus.COMPLETE
                assert orders[0].broker_order_id is not None
            transitions = repo.list_state_transitions(position.id)
            assert transitions[-1].to_state is PositionState.OPEN

    def test_broker_shows_two_short_legs(self):
        h = build_harness()
        h.entry_logic.enter()
        positions = h.broker.get_positions()
        assert len(positions) == 2
        assert all(p.quantity == -LOT_SIZE for p in positions)


class TestIdempotency:
    def test_second_enter_is_a_noop(self):
        h = build_harness()
        first = h.entry_logic.enter()
        second = h.entry_logic.enter()

        assert first.outcome is EntryOutcome.ENTERED
        assert second.outcome is EntryOutcome.SKIPPED_ALREADY_EXISTS

        # Still exactly one position, and no duplicate broker orders
        # (2 legs = exactly 2 orders, not 4).
        with h.session_factory() as s:
            positions = s.query(Position).all()
        assert len(positions) == 1
        assert len(h.broker.get_orders()) == 2


class TestRiskBlocking:
    def test_kill_switch_halt_blocks_entry(self):
        h = build_harness(halted=True)
        result = h.entry_logic.enter()

        assert result.outcome is EntryOutcome.BLOCKED_BY_RISK
        assert _position(h) is None  # no position created
        assert h.broker.get_orders() == []  # no orders placed

    def test_risk_rejection_blocks_entry(self):
        h = build_harness(approved=False)
        result = h.entry_logic.enter()

        assert result.outcome is EntryOutcome.BLOCKED_BY_RISK
        assert _position(h) is None
        assert h.broker.get_orders() == []


class TestExpiryDaySkip:
    def test_skips_when_configured_and_today_is_expiry(self):
        h = build_harness(config=_config(skip_on_expiry_day=True), today=EXPIRY)
        result = h.entry_logic.enter()

        assert result.outcome is EntryOutcome.SKIPPED_EXPIRY_DAY
        assert h.broker.get_orders() == []

    def test_does_not_skip_when_flag_false(self):
        h = build_harness(config=_config(skip_on_expiry_day=False), today=EXPIRY)
        result = h.entry_logic.enter()
        # today == expiry but skip disabled -> normal entry
        assert result.outcome is EntryOutcome.ENTERED


class TestEntryRejection:
    def test_first_leg_rejection_errors_without_naked_exposure(self):
        h = build_harness(reject_sell_symbol_selector=lambda ce, pe: ce)
        result = h.entry_logic.enter()

        assert result.outcome is EntryOutcome.ENTRY_REJECTED
        assert _position(h).state is PositionState.ERROR
        assert _instance_status(h) is InstanceStatus.FROZEN
        assert h.broker.get_positions() == []  # nothing live


class TestPartialEntryAutoUnwind:
    def test_ce_fills_pe_rejects_triggers_unwind_and_error(self):
        h = build_harness(reject_sell_symbol_selector=lambda ce, pe: pe)
        result = h.entry_logic.enter()

        assert result.outcome is EntryOutcome.PARTIAL_ENTRY_ERROR
        position = _position(h)
        assert position.state is PositionState.ERROR
        assert _instance_status(h) is InstanceStatus.FROZEN

        with h.session_factory() as s:
            repo = PositionRepository(s)
            trades = {t.option_type: t for t in repo.list_trades_for_position(position.id)}
            assert trades[OptionType.CE].status is TradeLegStatus.UNWOUND
            breaks = ReconciliationBreakRepository(s).list_for_position(position.id)
            assert len(breaks) >= 1

        # Net flat at the broker: CE sold then bought back.
        assert h.broker.get_positions() == []
