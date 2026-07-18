"""Tests for OrderUpdateProcessor (H3).

Drives the processor against a real in-memory SQLite database with a seeded
Order row, feeding it BrokerOrder DTOs that represent fills, partial fills,
rejections, and cancellations -- verifying correct propagation, idempotency,
and advance-only behaviour. A final integration-style test pushes a real update
through the simulation broker's order-update websocket to confirm the wiring.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

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


from algo.brokers.broker_base import BrokerOrder
from algo.common.enums import (
    Exchange,
    OptionType,
    OrderPurpose,
    OrderStatus,
    OrderType,
    PositionState,
    ProductType,
    TransactionType,
)
from algo.database.models import Account, Base, Position, StrategyInstance, Trade
from algo.database.models.order import Order
from algo.database.repositories.order_repository import OrderRepository
from algo.services.order_update_processor import OrderUpdateOutcome, OrderUpdateProcessor

CE_SYMBOL = "NIFTY26JUL0925000CE"


class FakeTime:
    def now(self):
        return datetime(2026, 7, 9, 10, 0, tzinfo=timezone.utc)

    def now_ist(self):
        return self.now()

    def today(self) -> date:
        return date(2026, 7, 9)


def build_env(*, order_status=OrderStatus.OPEN, filled=0, broker_order_id="B-CE", tag="E1-CE"):
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    sf = sessionmaker(bind=engine, expire_on_commit=False)
    with sf() as s:
        account = Account(broker="SIMULATION", display_name="t")
        s.add(account)
        s.flush()
        inst = StrategyInstance(strategy_id="strategy_1", instrument="NIFTY",
                                account_id=account.id, exchange=Exchange.NFO)
        s.add(inst)
        s.flush()
        pos = Position(strategy_instance_id=inst.id, trade_date=date(2026, 7, 9),
                       state=PositionState.ENTRY_PENDING)
        s.add(pos)
        s.flush()
        trade = Trade(position_id=pos.id, option_type=OptionType.CE, trading_symbol=CE_SYMBOL,
                      exchange=Exchange.NFO, strike=Decimal("25000"), quantity=75)
        s.add(trade)
        s.flush()
        order = Order(trade_id=trade.id, intent_id=None, purpose=OrderPurpose.ENTRY,
                      transaction_type=TransactionType.SELL, order_type=OrderType.MARKET,
                      quantity=75, filled_quantity=filled, status=order_status,
                      broker_order_id=broker_order_id, broker_tag=tag)
        s.add(order)
        s.commit()
        order_id = order.id
    return sf, order_id


def _broker_order(*, status, filled_quantity, broker_order_id="B-CE", tag="E1-CE",
                  average_price=Decimal("100"), quantity=75, status_message=None):
    return BrokerOrder(
        broker_order_id=broker_order_id, tag=tag, status=status, exchange=Exchange.NFO,
        tradingsymbol=CE_SYMBOL, transaction_type=TransactionType.SELL, product=ProductType.INTRADAY,
        order_type=OrderType.MARKET, quantity=quantity, filled_quantity=filled_quantity,
        average_price=average_price, status_message=status_message,
    )


def _order(sf, order_id) -> Order:
    with sf() as s:
        return OrderRepository(s).get_by_id_or_raise(order_id)


class TestPropagation:
    def test_complete_fill_is_recorded(self):
        sf, oid = build_env()
        proc = OrderUpdateProcessor(session_factory=sf, time_provider=FakeTime())

        outcome = proc.process(_broker_order(status=OrderStatus.COMPLETE, filled_quantity=75))

        assert outcome is OrderUpdateOutcome.APPLIED_FILL
        o = _order(sf, oid)
        assert o.status is OrderStatus.COMPLETE
        assert o.filled_quantity == 75
        assert o.average_price == Decimal("100")

    def test_partial_fill_is_recorded_and_stays_open(self):
        sf, oid = build_env()
        proc = OrderUpdateProcessor(session_factory=sf, time_provider=FakeTime())

        outcome = proc.process(_broker_order(status=OrderStatus.OPEN, filled_quantity=25))

        assert outcome is OrderUpdateOutcome.APPLIED_PARTIAL_FILL
        o = _order(sf, oid)
        assert o.status is OrderStatus.OPEN
        assert o.filled_quantity == 25

    def test_rejection_is_recorded(self):
        sf, oid = build_env()
        proc = OrderUpdateProcessor(session_factory=sf, time_provider=FakeTime())

        outcome = proc.process(_broker_order(
            status=OrderStatus.REJECTED, filled_quantity=0, status_message="margin"))

        assert outcome is OrderUpdateOutcome.APPLIED_REJECTION
        o = _order(sf, oid)
        assert o.status is OrderStatus.REJECTED
        assert "margin" in o.error_message

    def test_cancellation_is_recorded(self):
        sf, oid = build_env()
        proc = OrderUpdateProcessor(session_factory=sf, time_provider=FakeTime())

        outcome = proc.process(_broker_order(status=OrderStatus.CANCELLED, filled_quantity=0))

        assert outcome is OrderUpdateOutcome.APPLIED_CANCELLATION
        assert _order(sf, oid).status is OrderStatus.CANCELLED


class TestIdempotencyAndAdvanceOnly:
    def test_reprocessing_the_same_fill_is_a_noop(self):
        sf, oid = build_env()
        proc = OrderUpdateProcessor(session_factory=sf, time_provider=FakeTime())
        fill = _broker_order(status=OrderStatus.COMPLETE, filled_quantity=75)

        assert proc.process(fill) is OrderUpdateOutcome.APPLIED_FILL
        v_after_first = _order_version(sf, oid)
        assert proc.process(fill) is OrderUpdateOutcome.ALREADY_TERMINAL
        assert _order_version(sf, oid) == v_after_first  # no second write

    def test_late_open_after_complete_is_ignored(self):
        # A stale OPEN snapshot arriving after the order already COMPLETE must
        # not regress it.
        sf, oid = build_env(order_status=OrderStatus.COMPLETE, filled=75)
        proc = OrderUpdateProcessor(session_factory=sf, time_provider=FakeTime())

        outcome = proc.process(_broker_order(status=OrderStatus.OPEN, filled_quantity=25))

        assert outcome is OrderUpdateOutcome.ALREADY_TERMINAL
        assert _order(sf, oid).status is OrderStatus.COMPLETE

    def test_non_advancing_partial_is_noop(self):
        sf, oid = build_env(filled=50)
        proc = OrderUpdateProcessor(session_factory=sf, time_provider=FakeTime())

        outcome = proc.process(_broker_order(status=OrderStatus.OPEN, filled_quantity=50))

        assert outcome is OrderUpdateOutcome.NO_CHANGE


class TestLocationAndSafety:
    def test_unknown_order_is_not_found_not_created(self):
        sf, _oid = build_env()
        proc = OrderUpdateProcessor(session_factory=sf, time_provider=FakeTime())

        outcome = proc.process(_broker_order(
            status=OrderStatus.COMPLETE, filled_quantity=75, broker_order_id="UNKNOWN", tag="NOPE"))

        assert outcome is OrderUpdateOutcome.NOT_FOUND
        with sf() as s:
            assert len(list(s.query(Order))) == 1  # no orphan row created

    def test_located_by_tag_when_broker_id_not_yet_set(self):
        # Row created (by tag) before acknowledgement: broker_order_id is None.
        sf, oid = build_env(broker_order_id=None, tag="E1-CE")
        proc = OrderUpdateProcessor(session_factory=sf, time_provider=FakeTime())

        outcome = proc.process(_broker_order(status=OrderStatus.COMPLETE, filled_quantity=75,
                                             broker_order_id="B-NEW", tag="E1-CE"))

        assert outcome is OrderUpdateOutcome.APPLIED_FILL
        o = _order(sf, oid)
        assert o.status is OrderStatus.COMPLETE
        assert o.broker_order_id == "B-NEW"  # learned the id from the update


def _order_version(sf, order_id) -> int:
    return _order(sf, order_id).version


class TestThroughSimulationWebsocket:
    def test_update_pushed_through_broker_ws_reaches_the_db(self):
        """End-to-end wiring: register the processor as the broker's order-update
        callback, connect the ws, and place an order -- the resulting fill
        notification must flow through the processor. (The order row is created
        here to stand in for what entry_logic would create.)"""
        import random

        from algo.brokers.broker_base import InstrumentIdentifier, PlaceOrderRequest
        from algo.brokers.simulation import InstrumentCatalog, SimulationBroker, SimulationConfig, StaticPriceSource
        from algo.brokers.simulation.instrument_catalog import InstrumentCatalog as Cat

        sf, _oid = build_env(broker_order_id=None, tag="WS-CE", order_status=OrderStatus.OPEN)

        catalog = Cat.build_option_chain(
            underlying="NIFTY", exchange=Exchange.NFO, expiry=date(2026, 7, 9),
            atm_strike=Decimal("25000"), strike_interval=Decimal("50"),
            num_strikes_each_side=1, lot_size=75,
        )
        call = catalog.find_option(underlying="NIFTY", expiry=date(2026, 7, 9),
                                   strike=Decimal("25000"), option_type=OptionType.CE, exchange=Exchange.NFO)
        sym = call.tradingsymbol
        # Re-seed the order's symbol/tag to match what we'll place.
        with sf() as s:
            o = s.query(Order).one()
            t = s.query(Trade).one()
            t.trading_symbol = sym
            o.broker_tag = "WS-CE"
            s.commit()

        price = StaticPriceSource({InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=sym): Decimal("100")})
        broker = SimulationBroker(
            instrument_catalog=catalog, price_source=price,
            config=SimulationConfig(synchronous=True), rng=random.Random(0),
        )
        broker.authenticate()
        proc = OrderUpdateProcessor(session_factory=sf, time_provider=FakeTime())
        broker.register_order_update_callback(proc.process)
        broker.connect_websocket()

        broker.place_order(PlaceOrderRequest(
            exchange=Exchange.NFO, tradingsymbol=sym, transaction_type=TransactionType.SELL,
            quantity=75, product=ProductType.INTRADAY, order_type=OrderType.MARKET, tag="WS-CE",
        ))

        # The synchronous fill dispatched an order update through the processor.
        o = s = None
        with sf() as session:
            order = OrderRepository(session).get_by_broker_tag("WS-CE")
            assert order.status is OrderStatus.COMPLETE
            assert order.filled_quantity == 75
