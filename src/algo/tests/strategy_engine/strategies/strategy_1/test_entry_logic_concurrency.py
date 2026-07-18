"""Reproduces, deterministically (no real thread timing), the exact
concurrency bug reported against paper trading: the broker's push-based
order-update pipeline (``OrderUpdateProcessor``) and Strategy-1's own
poll-based fill confirmation (``EntryLogic._confirm_and_record``) both
attempting to apply the *same* terminal fill to the *same* ``Order`` row.

Before the fix, the poll path's own unconditional ``record_fill()`` call
would raise ``ConcurrentModificationError`` (StaleDataError under the hood)
whenever the push path had already committed that row's fill first -- an
unhandled exception that froze the whole strategy instance. This test forces
that exact ordering (push commits first, poll discovers the row already
terminal) via a broker subclass, not luck of real scheduling, so it fails
reliably pre-fix and passes reliably post-fix.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
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


from algo.brokers.broker_base import InstrumentIdentifier
from algo.brokers.simulation import (
    InstrumentCatalog,
    SimulationBroker,
    SimulationConfig,
    StaticPriceSource,
)
from algo.common.enums import Exchange, OptionType, OrderStatus, PositionState, ProductType
from algo.database.models import Account, Base, StrategyInstance
from algo.database.repositories.order_repository import OrderRepository
from algo.database.repositories.position_repository import PositionRepository
from algo.services.instrument_service import InstrumentSpec
from algo.services.order_update_processor import OrderUpdateProcessor
from algo.strategy_engine.strategies.strategy_1.config import RetrySettings, Strategy1Config
from algo.strategy_engine.strategies.strategy_1.entry_logic import EntryLogic, EntryOutcome
from algo.strategy_engine.strategies.strategy_1.strike_selector import StrikeSelector
from algo.strategy_engine.strategy_context import RiskDecision, StrategyContext, StrategyIdentity

INSTRUMENT = "NIFTY"
EXPIRY = date(2026, 7, 30)
TODAY = date(2026, 7, 7)
ATM = Decimal("24000")
SPOT = Decimal("24010")
LOT_SIZE = 75
CE_PRICE = Decimal("120")
PE_PRICE = Decimal("110")


@dataclass
class FakeTime:
    def now(self) -> datetime:
        return datetime(2026, 7, 7, 3, 50, tzinfo=timezone.utc)

    def now_ist(self) -> datetime:
        return datetime(2026, 7, 7, 9, 20, tzinfo=timezone.utc)

    def today(self) -> date:
        return TODAY


@dataclass
class FakeRisk:
    def is_halted(self, identity) -> bool:
        return False

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
    spec: InstrumentSpec

    def get_instrument_spec(self, instrument: str) -> InstrumentSpec:
        return self.spec


class FakeMarketData:
    def subscribe(self, instruments): ...
    def unsubscribe(self, instruments): ...
    def get_ltp(self, instrument): return Decimal("0")
    def get_ltps(self, instruments): return {}
    def is_connected(self): return True


class RacyBroker(SimulationBroker):
    """A SimulationBroker whose ``get_order`` deterministically injects a
    concurrent writer: the first time it reports an order as COMPLETE, it
    first drives that exact fill through a real ``OrderUpdateProcessor``
    (simulating the broker's own push callback winning the race to commit),
    *then* returns the completed status to the caller -- so the caller's own
    subsequent write attempt finds the row already terminal, exactly as a
    real push-vs-poll race would produce, deterministically instead of by
    thread-timing luck.
    """

    session_factory: sessionmaker | None = None
    _race_fired: bool = False

    def get_order(self, broker_order_id, *, timeout=None):
        result = super().get_order(broker_order_id, timeout=timeout)
        if not self._race_fired and result.status is OrderStatus.COMPLETE:
            self._race_fired = True
            assert self.session_factory is not None
            processor = OrderUpdateProcessor(session_factory=self.session_factory, time_provider=FakeTime())
            outcome = processor.process(result)
            assert outcome.value == "APPLIED_FILL", (
                f"test setup bug: the simulated push write did not apply (got {outcome})"
            )
        return result


def _build_harness():
    engine = create_engine("sqlite://")
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
    price_source = StaticPriceSource({ce_id: CE_PRICE, pe_id: PE_PRICE})

    broker = RacyBroker(
        instrument_catalog=catalog, price_source=price_source,
        config=SimulationConfig(synchronous=True), rng=random.Random(0),
    )
    broker.session_factory = session_factory
    broker.authenticate()

    identity = StrategyIdentity(
        instance_id=instance_id, strategy_id="strategy_1", instrument=INSTRUMENT,
        account_id=1, exchange=Exchange.NFO,
    )
    context = StrategyContext(
        identity=identity,
        config=Strategy1Config(
            entry_time=dt_time(9, 20),
            hard_cutoff_time=dt_time(15, 15),
            target_pct=Decimal("0.10"), sl_pct=Decimal("0.10"), lots=1,
            product_type=ProductType.INTRADAY, skip_on_expiry_day=False,
            monitoring_interval_seconds=5.0, polling_interval_seconds=2.0,
            retry=RetrySettings(
                order_timeout_seconds=None, fill_confirmation_attempts=3,
                fill_confirmation_delay_seconds=0.001, close_retry_attempts=3, close_retry_delay_seconds=0.001,
            ),
        ),
        session_factory=session_factory,
        broker=broker,
        market_data=FakeMarketData(),
        risk=FakeRisk(),
        time=FakeTime(),
        logger=logging.LoggerAdapter(logging.getLogger("test.entry.concurrency"), {}),
    )
    strike_selector = StrikeSelector(
        instrument_service=FakeInstrumentSvc(
            InstrumentSpec(instrument=INSTRUMENT, exchange=Exchange.NFO, strike_interval=Decimal("50"),
                           lot_size=LOT_SIZE, tick_size=Decimal("0.05"))
        ),
        expiry_service=FakeExpiry(), broker=broker,
    )
    entry_logic = EntryLogic(
        context=context, strike_selector=strike_selector, expiry_service=FakeExpiry(),
        spot_price_provider=FakeSpot(), fill_confirmation_attempts=3, fill_confirmation_delay=0.0,
    )
    return entry_logic, session_factory, instance_id


class TestConcurrentPushAndPollDoNotRace:
    def test_entry_survives_the_push_path_winning_the_fill_race(self):
        """The literal reported bug: push commits the fill first, poll's own
        confirmation write must not raise ConcurrentModificationError, and
        the entry must still complete correctly (position OPEN, not FROZEN).
        """
        entry_logic, session_factory, instance_id = _build_harness()

        result = entry_logic.enter()

        assert result.outcome is EntryOutcome.ENTERED, (
            f"expected ENTERED, got {result.outcome.value}: {result.message}"
        )

        with session_factory() as s:
            position = PositionRepository(s).get_by_instance_and_date(instance_id, TODAY)
            assert position is not None
            assert position.state is PositionState.OPEN
            assert position.combined_entry_premium == CE_PRICE + PE_PRICE

            orders = OrderRepository(s)
            trades = PositionRepository(s).list_trades_for_position(position.id)
            for trade in trades:
                leg_orders = orders.list_for_trade(trade.id)
                # Written exactly once by whichever writer got there first --
                # no duplicate row for this leg.
                assert len(leg_orders) == 1
                assert leg_orders[0].status is OrderStatus.COMPLETE

    def test_the_race_actually_fired_during_this_test(self):
        """Guards against a silently-broken test: confirms the injected race
        (push applying before poll) genuinely happened, not that the broker
        just never reached COMPLETE in time."""
        entry_logic, _session_factory, _instance_id = _build_harness()
        broker: RacyBroker = entry_logic._context.broker  # noqa: SLF001 -- test-only introspection
        entry_logic.enter()
        assert broker._race_fired is True  # noqa: SLF001
