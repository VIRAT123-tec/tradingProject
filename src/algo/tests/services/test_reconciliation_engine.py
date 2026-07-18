"""Tests for the ReconciliationEngine.

Reconciliation is a read-only *consumer* of broker truth (get_orders /
get_positions), so it is driven here with a FakeBroker returning precisely
constructed BrokerOrder / BrokerPosition DTOs -- exact control over "what the
broker says" per scenario. The database side is a real in-memory SQLite
instance (with the JSONB/BigInteger compile shims the DB layer's own tests
use), seeded to represent each crashed/in-doubt state, so the actual queries
and writes reconciliation performs are exercised, not stand-ins.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
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


from algo.brokers.broker_base import BrokerOrder, BrokerPosition
from algo.brokers.exceptions import BrokerConnectionError
from algo.common.enums import (
    Exchange,
    InstanceStatus,
    IntentStatus,
    OptionType,
    OrderPurpose,
    OrderStatus,
    OrderType,
    PositionState,
    ProductType,
    ReconciliationBreakType,
    StateTransitionActor,
    TradeLegStatus,
    TransactionType,
)
from algo.database.models import Account, Base, StrategyInstance
from algo.database.models.order import Order
from algo.database.models.order_intent import OrderIntent
from algo.database.models.position import Position
from algo.database.models.trade import Trade
from algo.database.repositories.order_intent_repository import OrderIntentRepository
from algo.database.repositories.order_repository import OrderRepository
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.reconciliation_break_repository import ReconciliationBreakRepository
from algo.database.repositories.strategy_instance_repository import StrategyInstanceRepository
from algo.services.reconciliation_engine import (
    ReconciliationEngine,
    ReconciliationOutcome,
)

TODAY = date(2026, 7, 7)
CE_SYMBOL = "NIFTYCE"
PE_SYMBOL = "NIFTYPE"
QTY = 75


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


@dataclass
class FakeTime:
    def now(self) -> datetime:
        return datetime(2026, 7, 7, 5, 0, tzinfo=timezone.utc)

    def now_ist(self) -> datetime:
        return datetime(2026, 7, 7, 10, 30, tzinfo=timezone.utc)

    def today(self) -> date:
        return TODAY


@dataclass
class FakeBroker:
    orders: list = field(default_factory=list)
    positions: list = field(default_factory=list)
    raise_orders: bool = False
    raise_positions: bool = False

    def get_orders(self, *, timeout=None):
        if self.raise_orders:
            raise BrokerConnectionError("simulated orderbook fetch failure")
        return list(self.orders)

    def get_positions(self, *, timeout=None):
        if self.raise_positions:
            raise BrokerConnectionError("simulated positions fetch failure")
        return list(self.positions)


@dataclass
class FakeReconciler:
    """Records finalize calls and actually completes the transition, so
    idempotency (a second reconcile finding the position already terminal) can
    be verified."""

    entry_calls: list = field(default_factory=list)
    exit_calls: list = field(default_factory=list)

    def finalize_entry(self, session, position):
        self.entry_calls.append(position.id)
        PositionRepository(session).transition_state(
            position, to_state=PositionState.OPEN, actor=StateTransitionActor.RECOVERY,
            reason="test finalize entry",
        )

    def finalize_exit(self, session, position):
        self.exit_calls.append(position.id)
        PositionRepository(session).transition_state(
            position, to_state=PositionState.CLOSED, actor=StateTransitionActor.RECOVERY,
            reason="test finalize exit",
        )


def broker_order(
    *,
    tag,
    status,
    broker_order_id,
    tradingsymbol=CE_SYMBOL,
    transaction_type=TransactionType.SELL,
    quantity=QTY,
    filled_quantity=None,
    average_price=Decimal("120"),
) -> BrokerOrder:
    if filled_quantity is None:
        filled_quantity = quantity if status is OrderStatus.COMPLETE else 0
    return BrokerOrder(
        broker_order_id=broker_order_id, tag=tag, status=status, exchange=Exchange.NFO,
        tradingsymbol=tradingsymbol, transaction_type=transaction_type, product=ProductType.INTRADAY,
        order_type=OrderType.MARKET, quantity=quantity, filled_quantity=filled_quantity,
        average_price=average_price if filled_quantity > 0 else None,
        placed_at=datetime(2026, 7, 7, 3, 50, tzinfo=timezone.utc),
        filled_at=datetime(2026, 7, 7, 3, 51, tzinfo=timezone.utc) if filled_quantity > 0 else None,
    )


def broker_position(tradingsymbol, quantity) -> BrokerPosition:
    return BrokerPosition(
        exchange=Exchange.NFO, tradingsymbol=tradingsymbol, product=ProductType.INTRADAY,
        quantity=quantity, average_price=Decimal("120"), last_price=Decimal("120"), pnl=Decimal("0"),
    )


# --------------------------------------------------------------------------
# Harness + seeding
# --------------------------------------------------------------------------


@dataclass
class Harness:
    engine: ReconciliationEngine
    broker: FakeBroker
    reconciler: FakeReconciler
    session_factory: sessionmaker
    account_id: int
    instance_id: int


def build_harness(*, with_reconciler: bool = True) -> Harness:
    engine_sql = create_engine("sqlite://")
    Base.metadata.create_all(engine_sql)
    session_factory = sessionmaker(bind=engine_sql, expire_on_commit=False)

    with session_factory() as s:
        account = Account(broker="SIMULATION", display_name="test")
        s.add(account)
        s.flush()
        instance = StrategyInstance(
            strategy_id="strategy_1", instrument="NIFTY", account_id=account.id, exchange=Exchange.NFO
        )
        s.add(instance)
        s.commit()
        account_id = account.id
        instance_id = instance.id

    broker = FakeBroker()
    reconciler = FakeReconciler()
    engine = ReconciliationEngine(
        broker=broker, session_factory=session_factory, time_provider=FakeTime(),
        position_reconciler=reconciler if with_reconciler else None,
    )
    return Harness(
        engine=engine, broker=broker, reconciler=reconciler, session_factory=session_factory,
        account_id=account_id, instance_id=instance_id,
    )


def seed_position(h: Harness, *, state: PositionState) -> int:
    with h.session_factory() as s:
        position = Position(
            strategy_instance_id=h.instance_id, trade_date=TODAY, state=state,
            strike=Decimal("24000"), lots=1, lot_size=QTY, quantity=QTY,
            target_pct=Decimal("0.10"), sl_pct=Decimal("0.10"),
        )
        s.add(position)
        s.commit()
        return position.id


def seed_leg(
    h: Harness,
    position_id: int,
    *,
    option_type: OptionType,
    symbol: str,
    trade_status: TradeLegStatus,
    purpose: OrderPurpose,
    intent_status: IntentStatus,
    tag: str,
    entry_price: Decimal | None = None,
) -> tuple[int, int]:
    """Seed one Trade + its in-doubt OrderIntent. Returns (trade_id, intent_id)."""
    transaction_type = TransactionType.SELL if purpose is OrderPurpose.ENTRY else TransactionType.BUY
    with h.session_factory() as s:
        trade = Trade(
            position_id=position_id, option_type=option_type, trading_symbol=symbol,
            exchange=Exchange.NFO, strike=Decimal("24000"), quantity=QTY, status=trade_status,
            entry_price=entry_price,
        )
        s.add(trade)
        s.flush()
        intent = OrderIntent(
            trade_id=trade.id, purpose=purpose, transaction_type=transaction_type, quantity=QTY,
            idempotency_key=f"key-{tag}", broker_tag=tag, status=intent_status,
            broker_call_started_at=datetime(2026, 7, 7, 3, 50, tzinfo=timezone.utc)
            if intent_status is not IntentStatus.PENDING else None,
        )
        s.add(intent)
        s.commit()
        return trade.id, intent.id


def _position(h: Harness, position_id: int) -> Position:
    with h.session_factory() as s:
        return PositionRepository(s).get_by_id_or_raise(position_id)


def _intent(h: Harness, intent_id: int) -> OrderIntent:
    with h.session_factory() as s:
        return OrderIntentRepository(s).get_by_id_or_raise(intent_id)


def _trade(h: Harness, trade_id: int) -> Trade:
    with h.session_factory() as s:
        return PositionRepository(s).get_trade_by_id(trade_id)


def _breaks(h: Harness) -> list:
    with h.session_factory() as s:
        return ReconciliationBreakRepository(s).list_pending()


def _instance_status(h: Harness) -> InstanceStatus:
    with h.session_factory() as s:
        return StrategyInstanceRepository(s).get_by_id_or_raise(h.instance_id).status


# --------------------------------------------------------------------------
# Broker availability
# --------------------------------------------------------------------------


class TestBrokerAvailability:
    def test_broker_orderbook_unavailable_aborts_without_repair(self):
        h = build_harness()
        h.broker.raise_orders = True
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        seed_leg(h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.SUBMITTED_UNCONFIRMED, tag="E1-CE")

        report = h.engine.reconcile()

        assert report.broker_unavailable
        assert report.breaks_recorded == 0
        # Nothing was touched.
        assert _position(h, pos_id).state is PositionState.ENTRY_PENDING
        assert _breaks(h) == []


# --------------------------------------------------------------------------
# Intent resolution
# --------------------------------------------------------------------------


class TestIntentResolution:
    def test_intent_not_at_broker_is_failed(self):
        # DB thinks we may have placed it, but broker has no such order.
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        _, intent_id = seed_leg(
            h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
            purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.SUBMITTED_UNCONFIRMED, tag="E1-CE",
        )
        seed_leg(h, pos_id, option_type=OptionType.PE, symbol=PE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.SUBMITTED_UNCONFIRMED, tag="E1-PE")
        h.broker.orders = []  # nothing at broker

        report = h.engine.reconcile()

        assert _intent(h, intent_id).status is IntentStatus.FAILED
        assert report.count(ReconciliationOutcome.INTENT_FAILED_NOT_AT_BROKER) == 2

    def test_broker_filled_but_db_behind_records_fill(self):
        # Broker success, DB failed to record it: intent PLACED, no order row,
        # broker order COMPLETE.
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        ce_trade, ce_intent = seed_leg(
            h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
            purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-CE",
        )
        pe_trade, pe_intent = seed_leg(
            h, pos_id, option_type=OptionType.PE, symbol=PE_SYMBOL, trade_status=TradeLegStatus.PENDING,
            purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-PE",
        )
        h.broker.orders = [
            broker_order(tag="E1-CE", status=OrderStatus.COMPLETE, broker_order_id="B-CE", tradingsymbol=CE_SYMBOL, average_price=Decimal("120")),
            broker_order(tag="E1-PE", status=OrderStatus.COMPLETE, broker_order_id="B-PE", tradingsymbol=PE_SYMBOL, average_price=Decimal("110")),
        ]

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.FILL_RECORDED) == 2
        assert _intent(h, ce_intent).status is IntentStatus.CONFIRMED
        assert _intent(h, pe_intent).status is IntentStatus.CONFIRMED
        assert _trade(h, ce_trade).status is TradeLegStatus.OPEN
        assert _trade(h, ce_trade).entry_price == Decimal("120")
        assert _trade(h, pe_trade).entry_price == Decimal("110")
        # An order row was created for each leg from broker truth.
        with h.session_factory() as s:
            assert OrderRepository(s).get_by_broker_order_id("B-CE") is not None

    def test_rejected_at_broker_fails_intent(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        _, intent_id = seed_leg(
            h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
            purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-CE",
        )
        h.broker.orders = [broker_order(tag="E1-CE", status=OrderStatus.REJECTED, broker_order_id="B-CE")]

        report = h.engine.reconcile()

        assert _intent(h, intent_id).status is IntentStatus.FAILED
        assert report.count(ReconciliationOutcome.REJECTION_RECORDED) == 1

    def test_partial_fill_records_break(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        ce_trade, _ = seed_leg(
            h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
            purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-CE",
        )
        h.broker.orders = [
            broker_order(tag="E1-CE", status=OrderStatus.COMPLETE, broker_order_id="B-CE", filled_quantity=50),
        ]

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.PARTIAL_FILL_RECORDED) == 1
        assert report.breaks_recorded >= 1
        any_break = _breaks(h)
        assert any(b.break_type is ReconciliationBreakType.QUANTITY_MISMATCH for b in any_break)

    def test_still_in_flight_left_alone(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        _, intent_id = seed_leg(
            h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
            purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-CE",
        )
        # Seed a matching order row so it isn't "created".
        with h.session_factory() as s:
            trade = PositionRepository(s).list_trades_for_position(pos_id)[0]
            OrderRepository(s).create(
                trade_id=trade.id, intent_id=intent_id, purpose=OrderPurpose.ENTRY,
                transaction_type=TransactionType.SELL, order_type=OrderType.MARKET, quantity=QTY, broker_tag="E1-CE",
            )
            s.commit()
        h.broker.orders = [broker_order(tag="E1-CE", status=OrderStatus.OPEN, broker_order_id="B-CE")]

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.ORDER_STILL_IN_FLIGHT) == 1
        assert _intent(h, intent_id).status is IntentStatus.PLACED  # unchanged


# --------------------------------------------------------------------------
# Duplicates and orphans
# --------------------------------------------------------------------------


class TestDuplicatesAndOrphans:
    def test_duplicate_orders_under_one_tag_error_the_position(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        seed_leg(h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.SUBMITTED_UNCONFIRMED, tag="E1-CE")
        h.broker.orders = [
            broker_order(tag="E1-CE", status=OrderStatus.COMPLETE, broker_order_id="B-CE-1"),
            broker_order(tag="E1-CE", status=OrderStatus.COMPLETE, broker_order_id="B-CE-2"),
        ]

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.DUPLICATE_ORDERS_DETECTED) == 1
        assert _position(h, pos_id).state is PositionState.ERROR
        assert _instance_status(h) is InstanceStatus.FROZEN
        assert any(b.break_type is ReconciliationBreakType.QUANTITY_MISMATCH for b in _breaks(h))

    def test_orphan_order_recorded_as_break(self):
        h = build_harness()
        # A broker order under a tag we have no intent for.
        h.broker.orders = [broker_order(tag="UNKNOWN-TAG", status=OrderStatus.COMPLETE, broker_order_id="B-X")]

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.ORPHAN_ORDER_DETECTED) == 1
        assert any(b.break_type is ReconciliationBreakType.ORPHAN_FILL for b in _breaks(h))


# --------------------------------------------------------------------------
# Position reconciliation
# --------------------------------------------------------------------------


class TestEntryPositionReconciliation:
    def test_both_legs_filled_finalizes_entry(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        seed_leg(h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-CE")
        seed_leg(h, pos_id, option_type=OptionType.PE, symbol=PE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-PE")
        h.broker.orders = [
            broker_order(tag="E1-CE", status=OrderStatus.COMPLETE, broker_order_id="B-CE", tradingsymbol=CE_SYMBOL),
            broker_order(tag="E1-PE", status=OrderStatus.COMPLETE, broker_order_id="B-PE", tradingsymbol=PE_SYMBOL),
        ]

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.ENTRY_FINALIZED) == 1
        assert h.reconciler.entry_calls == [pos_id]
        assert _position(h, pos_id).state is PositionState.OPEN

    def test_partial_entry_errors_and_freezes(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        seed_leg(h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-CE")
        seed_leg(h, pos_id, option_type=OptionType.PE, symbol=PE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.SUBMITTED_UNCONFIRMED, tag="E1-PE")
        # CE filled, PE never landed at broker.
        h.broker.orders = [
            broker_order(tag="E1-CE", status=OrderStatus.COMPLETE, broker_order_id="B-CE", tradingsymbol=CE_SYMBOL),
        ]

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.POSITION_ERRORED) == 1
        assert _position(h, pos_id).state is PositionState.ERROR
        assert _instance_status(h) is InstanceStatus.FROZEN
        assert any(b.break_type is ReconciliationBreakType.PARTIAL_ENTRY for b in _breaks(h))
        assert h.reconciler.entry_calls == []  # not finalized

    def test_no_fills_aborts_entry_to_closed(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        seed_leg(h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.SUBMITTED_UNCONFIRMED, tag="E1-CE")
        seed_leg(h, pos_id, option_type=OptionType.PE, symbol=PE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.SUBMITTED_UNCONFIRMED, tag="E1-PE")
        h.broker.orders = []  # nothing filled

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.ENTRY_ABORTED_NO_FILLS) == 1
        assert _position(h, pos_id).state is PositionState.CLOSED

    def test_completable_entry_without_reconciler_records_break(self):
        h = build_harness(with_reconciler=False)
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        seed_leg(h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-CE")
        seed_leg(h, pos_id, option_type=OptionType.PE, symbol=PE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-PE")
        h.broker.orders = [
            broker_order(tag="E1-CE", status=OrderStatus.COMPLETE, broker_order_id="B-CE", tradingsymbol=CE_SYMBOL),
            broker_order(tag="E1-PE", status=OrderStatus.COMPLETE, broker_order_id="B-PE", tradingsymbol=PE_SYMBOL),
        ]

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.NEEDS_FINALIZATION_NO_RECONCILER) == 1
        assert _position(h, pos_id).needs_reconciliation is True
        assert _position(h, pos_id).state is PositionState.ENTRY_PENDING  # untouched otherwise


class TestExitPositionReconciliation:
    def test_both_legs_closed_finalizes_exit(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.EXIT_PENDING)
        seed_leg(h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.OPEN,
                 purpose=OrderPurpose.EXIT, intent_status=IntentStatus.PLACED, tag="X1-CE", entry_price=Decimal("120"))
        seed_leg(h, pos_id, option_type=OptionType.PE, symbol=PE_SYMBOL, trade_status=TradeLegStatus.OPEN,
                 purpose=OrderPurpose.EXIT, intent_status=IntentStatus.PLACED, tag="X1-PE", entry_price=Decimal("110"))
        h.broker.orders = [
            broker_order(tag="X1-CE", status=OrderStatus.COMPLETE, broker_order_id="B-CE", tradingsymbol=CE_SYMBOL, transaction_type=TransactionType.BUY),
            broker_order(tag="X1-PE", status=OrderStatus.COMPLETE, broker_order_id="B-PE", tradingsymbol=PE_SYMBOL, transaction_type=TransactionType.BUY),
        ]

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.EXIT_FINALIZED) == 1
        assert h.reconciler.exit_calls == [pos_id]
        assert _position(h, pos_id).state is PositionState.CLOSED

    def test_partial_exit_errors(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.EXIT_PENDING)
        seed_leg(h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.OPEN,
                 purpose=OrderPurpose.EXIT, intent_status=IntentStatus.PLACED, tag="X1-CE", entry_price=Decimal("120"))
        seed_leg(h, pos_id, option_type=OptionType.PE, symbol=PE_SYMBOL, trade_status=TradeLegStatus.OPEN,
                 purpose=OrderPurpose.EXIT, intent_status=IntentStatus.SUBMITTED_UNCONFIRMED, tag="X1-PE", entry_price=Decimal("110"))
        # CE close filled, PE close never landed.
        h.broker.orders = [
            broker_order(tag="X1-CE", status=OrderStatus.COMPLETE, broker_order_id="B-CE", tradingsymbol=CE_SYMBOL, transaction_type=TransactionType.BUY),
        ]

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.POSITION_ERRORED) == 1
        assert _position(h, pos_id).state is PositionState.ERROR

    def test_exit_pending_with_no_legs_closed_is_left_for_strategy_recovery(self):
        """Neither leg has a close order at the broker yet -- the exit was
        decided (EXIT_PENDING recorded) but never completed, whether because
        it was never attempted before a crash or every attempt failed to
        land. This is exactly the state Strategy.recover()'s own EXIT_PENDING
        branch exists to safely resume via ExitLogic.exit()'s idempotent
        per-leg logic -- reconciliation must not freeze the instance first
        and make that documented recovery path unreachable (a real,
        previously-uncaught gap: this exact scenario had no prior test)."""
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.EXIT_PENDING)
        seed_leg(h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.OPEN,
                 purpose=OrderPurpose.EXIT, intent_status=IntentStatus.PENDING, tag="X1-CE", entry_price=Decimal("120"))
        seed_leg(h, pos_id, option_type=OptionType.PE, symbol=PE_SYMBOL, trade_status=TradeLegStatus.OPEN,
                 purpose=OrderPurpose.EXIT, intent_status=IntentStatus.PENDING, tag="X1-PE", entry_price=Decimal("110"))
        h.broker.orders = []  # neither close ever reached the broker

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.EXIT_PENDING_LEFT_FOR_RECOVERY) == 1
        assert _position(h, pos_id).state is PositionState.EXIT_PENDING  # untouched, not errored


class TestOpenPositionReconciliation:
    def test_open_consistent_with_broker_is_no_action(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.OPEN)
        with h.session_factory() as s:
            for opt, sym in ((OptionType.CE, CE_SYMBOL), (OptionType.PE, PE_SYMBOL)):
                s.add(Trade(position_id=pos_id, option_type=opt, trading_symbol=sym, exchange=Exchange.NFO,
                            strike=Decimal("24000"), quantity=QTY, status=TradeLegStatus.OPEN, entry_price=Decimal("120")))
            s.commit()
        h.broker.positions = [broker_position(CE_SYMBOL, -QTY), broker_position(PE_SYMBOL, -QTY)]

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.CONSISTENT_NO_ACTION) >= 1
        assert _position(h, pos_id).state is PositionState.OPEN

    def test_open_but_broker_flat_errors(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.OPEN)
        with h.session_factory() as s:
            for opt, sym in ((OptionType.CE, CE_SYMBOL), (OptionType.PE, PE_SYMBOL)):
                s.add(Trade(position_id=pos_id, option_type=opt, trading_symbol=sym, exchange=Exchange.NFO,
                            strike=Decimal("24000"), quantity=QTY, status=TradeLegStatus.OPEN, entry_price=Decimal("120")))
            s.commit()
        h.broker.positions = []  # broker shows flat -> RMS squared us off

        report = h.engine.reconcile()

        assert report.count(ReconciliationOutcome.POSITION_ERRORED) == 1
        assert _position(h, pos_id).state is PositionState.ERROR
        assert any(b.break_type is ReconciliationBreakType.BROKER_FLAT_DB_OPEN for b in _breaks(h))


# --------------------------------------------------------------------------
# Idempotency
# --------------------------------------------------------------------------


class TestIdempotency:
    def test_second_run_is_a_noop(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        seed_leg(h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-CE")
        seed_leg(h, pos_id, option_type=OptionType.PE, symbol=PE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-PE")
        h.broker.orders = [
            broker_order(tag="E1-CE", status=OrderStatus.COMPLETE, broker_order_id="B-CE", tradingsymbol=CE_SYMBOL),
            broker_order(tag="E1-PE", status=OrderStatus.COMPLETE, broker_order_id="B-PE", tradingsymbol=PE_SYMBOL),
        ]
        # First run resolves everything and finalizes to OPEN.
        first = h.engine.reconcile()
        assert first.count(ReconciliationOutcome.ENTRY_FINALIZED) == 1
        breaks_after_first = len(_breaks(h))

        # After the position is OPEN, the broker still shows the legs live.
        h.broker.positions = [broker_position(CE_SYMBOL, -QTY), broker_position(PE_SYMBOL, -QTY)]
        h.reconciler.entry_calls.clear()

        second = h.engine.reconcile()

        # No intents in-doubt, position OPEN & consistent -> nothing new.
        assert second.count(ReconciliationOutcome.ENTRY_FINALIZED) == 0
        assert second.count(ReconciliationOutcome.FILL_RECORDED) == 0
        assert h.reconciler.entry_calls == []
        assert len(_breaks(h)) == breaks_after_first  # no duplicate breaks

    def test_errored_position_not_re_touched(self):
        h = build_harness()
        pos_id = seed_position(h, state=PositionState.ENTRY_PENDING)
        seed_leg(h, pos_id, option_type=OptionType.CE, symbol=CE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.PLACED, tag="E1-CE")
        seed_leg(h, pos_id, option_type=OptionType.PE, symbol=PE_SYMBOL, trade_status=TradeLegStatus.PENDING,
                 purpose=OrderPurpose.ENTRY, intent_status=IntentStatus.SUBMITTED_UNCONFIRMED, tag="E1-PE")
        h.broker.orders = [broker_order(tag="E1-CE", status=OrderStatus.COMPLETE, broker_order_id="B-CE", tradingsymbol=CE_SYMBOL)]

        h.engine.reconcile()  # -> ERROR + break
        breaks_after_first = len(_breaks(h))

        second = h.engine.reconcile()

        # ERROR positions are skipped -> no new breaks, no new actions.
        assert len(_breaks(h)) == breaks_after_first
        assert second.count(ReconciliationOutcome.POSITION_ERRORED) == 0
