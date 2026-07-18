"""Tests for the Strategy-1 orchestrator (strategy.py).

Strategy1 is tested with fake EntryLogic/ExitLogic/Monitor injected directly
(recording calls, no real broker/DB order flow), plus a real in-memory SQLite
database for the position-state reads/writes strategy.py itself performs
(recovery routing, ENTRY_PENDING freeze). This isolates orchestration behavior
from the already-separately-tested execution mechanics of entry_logic.py,
exit_logic.py, and monitor.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
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


from algo.common.enums import (
    Exchange,
    ExitReason,
    InstanceStatus,
    OptionType,
    PositionState,
    ProductType,
    StateTransitionActor,
    TradeLegStatus,
)
from algo.database.models import Account, Base, StrategyInstance
from algo.database.models.position import Position
from algo.database.models.trade import Trade
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.strategy_instance_repository import (
    StrategyInstanceRepository,
)
from algo.strategy_engine.strategies.strategy_1.config import RetrySettings, Strategy1Config
from algo.strategy_engine.strategies.strategy_1.entry_logic import EntryOutcome, EntryResult
from algo.strategy_engine.strategies.strategy_1.exit_logic import ExitOutcome, ExitResult
from algo.strategy_engine.strategies.strategy_1.monitor import AttachOutcome
from algo.strategy_engine.strategies.strategy_1.strategy import Strategy1
from algo.strategy_engine.strategy_context import StrategyContext, StrategyIdentity
from algo.strategy_engine.strategy_registry import StrategyRegistry, default_registry

INSTRUMENT = "NIFTY"
TODAY = date(2026, 7, 7)


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


@dataclass
class FakeTime:
    def now(self) -> datetime:
        return datetime(2026, 7, 7, 4, 30, tzinfo=timezone.utc)

    def now_ist(self) -> datetime:
        return datetime(2026, 7, 7, 10, 0, tzinfo=timezone.utc)

    def today(self) -> date:
        return TODAY


class FakeMarketData:
    def subscribe(self, instruments): ...
    def unsubscribe(self, instruments): ...
    def get_ltp(self, instrument): return Decimal("0")
    def get_ltps(self, instruments): return {}
    def is_connected(self): return True


class FakeRisk:
    def is_halted(self, identity): return False
    def approve_entry(self, identity, *, quantity):
        from algo.strategy_engine.strategy_context import RiskDecision
        return RiskDecision(approved=True)


@dataclass
class FakeEntryLogic:
    result: EntryResult = field(
        default_factory=lambda: EntryResult(EntryOutcome.ENTERED, "entered", position_id=1)
    )
    calls: int = 0

    def enter(self) -> EntryResult:
        self.calls += 1
        return self.result


@dataclass
class FakeExitLogic:
    result: ExitResult = field(
        default_factory=lambda: ExitResult(ExitOutcome.EXITED, "exited", position_id=1)
    )
    exit_calls: list = field(default_factory=list)

    def evaluate(self, **kw):
        raise NotImplementedError("not used by strategy.py directly")

    def exit(self, reason: ExitReason) -> ExitResult:
        self.exit_calls.append(reason)
        return self.result


@dataclass
class FakeMonitor:
    attach_outcome: AttachOutcome = AttachOutcome.MONITORING
    attach_calls: int = 0
    tick_calls: int = 0
    poll_calls: int = 0
    stop_calls: int = 0
    is_active: bool = False
    last_combined_premium: Decimal | None = None

    def attach(self) -> AttachOutcome:
        self.attach_calls += 1
        self.is_active = True
        return self.attach_outcome

    def on_tick(self, tick) -> None:
        self.tick_calls += 1

    def poll_and_check(self) -> None:
        self.poll_calls += 1

    def stop(self) -> None:
        self.stop_calls += 1
        self.is_active = False


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------


@dataclass
class Harness:
    strategy: Strategy1
    entry_logic: FakeEntryLogic
    exit_logic: FakeExitLogic
    monitor: FakeMonitor
    session_factory: sessionmaker
    instance_id: int


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


def build_strategy(*, position_state: PositionState | None = None) -> Harness:
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
        instance_id = instance.id

        if position_state is not None:
            position = Position(
                strategy_instance_id=instance_id,
                trade_date=TODAY,
                state=position_state,
                strike=Decimal("24000"),
                lots=1,
                lot_size=75,
                quantity=75,
                target_pct=Decimal("0.10"),
                sl_pct=Decimal("0.10"),
            )
            s.add(position)
            s.flush()
            for opt, sym in ((OptionType.CE, "NIFTYCE"), (OptionType.PE, "NIFTYPE")):
                s.add(
                    Trade(
                        position_id=position.id,
                        option_type=opt,
                        trading_symbol=sym,
                        exchange=Exchange.NFO,
                        strike=Decimal("24000"),
                        quantity=75,
                        status=TradeLegStatus.OPEN if position_state == PositionState.OPEN else TradeLegStatus.PENDING,
                    )
                )
            s.commit()
        else:
            s.commit()

    identity = StrategyIdentity(
        instance_id=instance_id, strategy_id="strategy_1", instrument=INSTRUMENT,
        account_id=1, exchange=Exchange.NFO,
    )
    context = StrategyContext(
        identity=identity, config=_config(), session_factory=session_factory,
        broker=object(), market_data=FakeMarketData(), risk=FakeRisk(), time=FakeTime(),
        logger=logging.LoggerAdapter(logging.getLogger("test.strategy"), {}),
    )
    entry_logic = FakeEntryLogic()
    exit_logic = FakeExitLogic()
    monitor = FakeMonitor()
    strategy = Strategy1(context, entry_logic=entry_logic, exit_logic=exit_logic, monitor=monitor)
    return Harness(
        strategy=strategy, entry_logic=entry_logic, exit_logic=exit_logic, monitor=monitor,
        session_factory=session_factory, instance_id=instance_id,
    )


# --------------------------------------------------------------------------
# Registration
# --------------------------------------------------------------------------


class TestRegistration:
    def test_strategy1_is_registered_under_strategy_1(self):
        assert default_registry.is_registered("strategy_1")
        assert default_registry.get("strategy_1") is Strategy1

    def test_config_schema_is_strategy1config(self):
        assert Strategy1.config_schema() is Strategy1Config


# --------------------------------------------------------------------------
# Constructor validation / DI
# --------------------------------------------------------------------------


class TestConstruction:
    def test_wrong_config_type_raises(self):
        from pydantic import BaseModel

        class WrongConfig(BaseModel):
            pass

        identity = StrategyIdentity(
            instance_id=1, strategy_id="strategy_1", instrument=INSTRUMENT, account_id=1, exchange=Exchange.NFO
        )
        context = StrategyContext(
            identity=identity, config=WrongConfig(), session_factory=lambda: None,  # type: ignore[arg-type]
            broker=object(), market_data=FakeMarketData(), risk=FakeRisk(), time=FakeTime(),
            logger=logging.LoggerAdapter(logging.getLogger("t"), {}),
        )
        with pytest.raises(TypeError, match="Strategy1Config"):
            Strategy1(context)

    def test_missing_seams_raise_without_direct_injection(self):
        identity = StrategyIdentity(
            instance_id=1, strategy_id="strategy_1", instrument=INSTRUMENT, account_id=1, exchange=Exchange.NFO
        )
        context = StrategyContext(
            identity=identity, config=_config(), session_factory=lambda: None,  # type: ignore[arg-type]
            broker=object(), market_data=FakeMarketData(), risk=FakeRisk(), time=FakeTime(),
            logger=logging.LoggerAdapter(logging.getLogger("t"), {}),
            # instrument_service / expiry_service / spot_price_provider all None
        )
        with pytest.raises(TypeError, match="instrument_service"):
            Strategy1(context)

    def test_direct_injection_bypasses_seam_requirement(self):
        h = build_strategy()  # uses direct entry_logic/exit_logic/monitor injection
        assert h.strategy is not None  # constructed without raising


# --------------------------------------------------------------------------
# Scheduling
# --------------------------------------------------------------------------


class TestScheduledTriggers:
    def test_declares_entry_and_cutoff_triggers_from_config(self):
        h = build_strategy()
        triggers = h.strategy.scheduled_triggers()
        by_name = {t.name: t for t in triggers}
        assert by_name["entry"].trigger_time == time(9, 20)
        assert by_name["cutoff"].trigger_time == time(15, 15)

    def test_entry_trigger_is_skip_on_catchup(self):
        from algo.strategy_engine.strategy_base import TriggerCatchUpPolicy

        h = build_strategy()
        by_name = {t.name: t for t in h.strategy.scheduled_triggers()}
        assert by_name["entry"].catch_up is TriggerCatchUpPolicy.SKIP

    def test_cutoff_trigger_fires_on_catchup(self):
        from algo.strategy_engine.strategy_base import TriggerCatchUpPolicy

        h = build_strategy()
        by_name = {t.name: t for t in h.strategy.scheduled_triggers()}
        assert by_name["cutoff"].catch_up is TriggerCatchUpPolicy.FIRE_ON_STARTUP


# --------------------------------------------------------------------------
# on_time_trigger routing
# --------------------------------------------------------------------------


class TestOnTimeTrigger:
    def test_entry_trigger_calls_entry_logic(self):
        h = build_strategy()
        h.strategy.on_time_trigger("entry")
        assert h.entry_logic.calls == 1

    def test_entry_trigger_attaches_monitor_when_entered(self):
        h = build_strategy()
        h.entry_logic.result = EntryResult(EntryOutcome.ENTERED, "entered", position_id=1)
        h.strategy.on_time_trigger("entry")
        assert h.monitor.attach_calls == 1

    def test_entry_trigger_does_not_attach_monitor_when_skipped(self):
        h = build_strategy()
        h.entry_logic.result = EntryResult(EntryOutcome.SKIPPED_ALREADY_EXISTS, "skip")
        h.strategy.on_time_trigger("entry")
        assert h.monitor.attach_calls == 0

    def test_entry_trigger_does_not_attach_monitor_on_error(self):
        h = build_strategy()
        h.entry_logic.result = EntryResult(EntryOutcome.ENTRY_REJECTED, "rejected", position_id=1)
        h.strategy.on_time_trigger("entry")
        assert h.monitor.attach_calls == 0

    def test_cutoff_trigger_calls_monitor_poll_and_check(self):
        h = build_strategy()
        h.strategy.on_time_trigger("cutoff")
        assert h.monitor.poll_calls == 1
        assert h.entry_logic.calls == 0

    def test_unknown_trigger_is_ignored_not_raised(self):
        h = build_strategy()
        h.strategy.on_time_trigger("something_else")  # must not raise
        assert h.entry_logic.calls == 0
        assert h.monitor.poll_calls == 0


class TestOnMarketTick:
    def test_tick_delegates_to_monitor(self):
        from algo.brokers.broker_base import InstrumentIdentifier
        from algo.strategy_engine.strategy_context import Tick

        h = build_strategy()
        tick = Tick(
            instrument=InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="X"),
            last_price=Decimal("1"),
            timestamp=datetime.now(timezone.utc),
        )
        h.strategy.on_market_tick(tick)
        assert h.monitor.tick_calls == 1


class TestOnShutdown:
    def test_shutdown_stops_monitor(self):
        h = build_strategy()
        h.strategy.on_shutdown()
        assert h.monitor.stop_calls == 1

    def test_shutdown_does_not_call_exit_logic(self):
        h = build_strategy()
        h.strategy.on_shutdown()
        assert h.exit_logic.exit_calls == []


# --------------------------------------------------------------------------
# health()
# --------------------------------------------------------------------------


class TestHealth:
    def test_healthy_with_no_position(self):
        h = build_strategy()
        health = h.strategy.health()
        assert health.healthy is True
        assert health.state == "NO_POSITION"

    def test_unhealthy_when_position_is_error(self):
        h = build_strategy(position_state=PositionState.ERROR)
        health = h.strategy.health()
        assert health.healthy is False
        assert health.state == "ERROR"

    def test_healthy_open_reports_state(self):
        h = build_strategy(position_state=PositionState.OPEN)
        health = h.strategy.health()
        assert health.healthy is True
        assert health.state == "OPEN"

    def test_health_never_raises_even_if_session_factory_broken(self):
        identity = StrategyIdentity(
            instance_id=1, strategy_id="strategy_1", instrument=INSTRUMENT, account_id=1, exchange=Exchange.NFO
        )

        def broken_session_factory():
            raise RuntimeError("db is down")

        context = StrategyContext(
            identity=identity, config=_config(), session_factory=broken_session_factory,  # type: ignore[arg-type]
            broker=object(), market_data=FakeMarketData(), risk=FakeRisk(), time=FakeTime(),
            logger=logging.LoggerAdapter(logging.getLogger("t"), {}),
        )
        strategy = Strategy1(
            context, entry_logic=FakeEntryLogic(), exit_logic=FakeExitLogic(), monitor=FakeMonitor()
        )
        health = strategy.health()  # must not raise
        assert health.healthy is False


# --------------------------------------------------------------------------
# recover() -- restart recovery routing
# --------------------------------------------------------------------------


class TestRecoverNoPosition:
    def test_no_position_logs_fresh_start_and_does_not_touch_components(self):
        h = build_strategy()
        h.strategy.recover()
        assert h.entry_logic.calls == 0
        assert h.exit_logic.exit_calls == []
        assert h.monitor.attach_calls == 0


class TestRecoverClosed:
    def test_closed_position_is_a_noop(self):
        h = build_strategy(position_state=PositionState.CLOSED)
        h.strategy.recover()
        assert h.monitor.attach_calls == 0
        assert h.exit_logic.exit_calls == []


class TestRecoverError:
    def test_error_position_is_a_noop(self):
        h = build_strategy(position_state=PositionState.ERROR)
        h.strategy.recover()
        assert h.monitor.attach_calls == 0
        assert h.exit_logic.exit_calls == []


class TestRecoverOpen:
    def test_open_position_resumes_monitoring(self):
        h = build_strategy(position_state=PositionState.OPEN)
        h.strategy.recover()
        assert h.monitor.attach_calls == 1
        assert h.exit_logic.exit_calls == []


class TestRecoverExitPending:
    def test_exit_pending_completes_via_exit_logic_with_inferred_reason(self):
        h = build_strategy(position_state=PositionState.EXIT_PENDING)
        # Seed the audit trail exit_logic would have written before a crash.
        with h.session_factory() as s:
            position = PositionRepository(s).get_by_instance_and_date(h.instance_id, TODAY)
            from algo.database.models.position_state_transition import PositionStateTransition

            s.add(
                PositionStateTransition(
                    position_id=position.id,
                    from_state=PositionState.OPEN,
                    to_state=PositionState.EXIT_PENDING,
                    reason="exit triggered: STOPLOSS",
                    actor=StateTransitionActor.STRATEGY,
                )
            )
            s.commit()

        h.strategy.recover()

        assert h.exit_logic.exit_calls == [ExitReason.STOPLOSS]
        assert h.monitor.attach_calls == 0

    def test_exit_pending_defaults_to_manual_when_reason_unknown(self):
        h = build_strategy(position_state=PositionState.EXIT_PENDING)
        # No transition audit row seeded -> cannot infer.
        h.strategy.recover()
        assert h.exit_logic.exit_calls == [ExitReason.MANUAL]


class TestRecoverEntryPending:
    def test_entry_pending_moves_to_error_and_freezes_instance(self):
        h = build_strategy(position_state=PositionState.ENTRY_PENDING)
        h.strategy.recover()

        with h.session_factory() as s:
            position = PositionRepository(s).get_by_instance_and_date(h.instance_id, TODAY)
            assert position.state is PositionState.ERROR
            instance = StrategyInstanceRepository(s).get_by_id_or_raise(h.instance_id)
            assert instance.status is InstanceStatus.FROZEN

        # No component tried to act on the un-resumable entry.
        assert h.entry_logic.calls == 0
        assert h.monitor.attach_calls == 0
        assert h.exit_logic.exit_calls == []

    def test_entry_pending_records_error_reason(self):
        h = build_strategy(position_state=PositionState.ENTRY_PENDING)
        h.strategy.recover()
        with h.session_factory() as s:
            position = PositionRepository(s).get_by_instance_and_date(h.instance_id, TODAY)
            assert position.error_message is not None
            assert "ENTRY_PENDING" in position.error_message
