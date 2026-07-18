"""Tests for PlatformScheduler.

Uses a real StrategyRunner wrapping a minimal DummyStrategy (configurable
triggers, recover/trigger-handler failure injection) and a real in-memory
SQLite database (StrategyRunner's own fault-isolation path writes a FROZEN
status there on an unhandled hook exception, exactly as in production), so the
integration with StrategyRunner's actual lifecycle and RunnerStatus is
exercised, not a stand-in for it.

Most tests call ``_tick()`` directly against a controllable clock for fast,
deterministic assertions; a small set of threading tests exercise the real
background loop with a short poll interval.
"""

from __future__ import annotations

import logging
import time as real_time
from dataclasses import dataclass
from datetime import date, datetime, time, timezone

import pytest
from pydantic import BaseModel
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


from algo.common.enums import Exchange, InstanceStatus
from algo.database.models import Account, Base, StrategyInstance
from algo.database.repositories.strategy_instance_repository import StrategyInstanceRepository
from algo.scheduler import PlatformScheduler, SchedulerConfig, WeekdayTradingCalendar
from algo.strategy_engine.strategy_base import Strategy, StrategyHealth, TimeTrigger, TriggerCatchUpPolicy
from algo.strategy_engine.strategy_context import RiskDecision, StrategyContext, StrategyIdentity, Tick
from algo.strategy_engine.strategy_runner import RunnerStatus, StrategyRunner

INSTRUMENT = "NIFTY"


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


@dataclass
class MutableTime:
    ist: datetime
    _today: date

    def now(self) -> datetime:
        return self.ist

    def now_ist(self) -> datetime:
        return self.ist

    def today(self) -> date:
        return self._today


class FakeMarketData:
    def subscribe(self, instruments): ...
    def unsubscribe(self, instruments): ...
    def get_ltp(self, instrument): return None
    def get_ltps(self, instruments): return {}
    def is_connected(self): return True


class FakeRisk:
    def is_halted(self, identity): return False
    def approve_entry(self, identity, *, quantity): return RiskDecision(approved=True)


class DummyConfig(BaseModel):
    """Trivial config -- DummyStrategy has no real parameters to validate."""


class DummyStrategy(Strategy):
    """Minimal Strategy recording every lifecycle call; triggers and failure
    injection are configurable per instance so tests can shape exact scenarios."""

    def __init__(
        self,
        context: StrategyContext,
        *,
        triggers: list[TimeTrigger] | None = None,
        fail_recover: bool = False,
        fail_on_trigger: str | None = None,
    ) -> None:
        super().__init__(context)
        self.calls: list[str] = []
        self._triggers = triggers or []
        self._fail_recover = fail_recover
        self._fail_on_trigger = fail_on_trigger

    @classmethod
    def config_schema(cls) -> type[BaseModel]:
        return DummyConfig

    def scheduled_triggers(self):
        return self._triggers

    def initialize(self) -> None:
        self.calls.append("initialize")

    def recover(self) -> None:
        self.calls.append("recover")
        if self._fail_recover:
            raise RuntimeError("recover failed")

    def on_time_trigger(self, trigger_name: str) -> None:
        self.calls.append(f"trigger:{trigger_name}")
        if trigger_name == self._fail_on_trigger:
            raise RuntimeError(f"handler for {trigger_name} failed")

    def on_market_tick(self, tick: Tick) -> None:
        self.calls.append("tick")

    def on_shutdown(self) -> None:
        self.calls.append("shutdown")

    def health(self) -> StrategyHealth:
        return StrategyHealth(healthy=True, state="OK", checked_at=datetime.now(timezone.utc))


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------


def make_engine_and_session_factory():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def seed_instance(session_factory, *, instrument: str = INSTRUMENT) -> int:
    with session_factory() as s:
        account = Account(broker="SIMULATION", display_name="test")
        s.add(account)
        s.flush()
        instance = StrategyInstance(
            strategy_id="dummy", instrument=instrument, account_id=account.id, exchange=Exchange.NFO
        )
        s.add(instance)
        s.commit()
        return instance.id


def build_runner(
    session_factory,
    instance_id: int,
    *,
    triggers: list[TimeTrigger],
    clock: MutableTime,
    fail_recover: bool = False,
    fail_on_trigger: str | None = None,
    instrument: str = INSTRUMENT,
) -> StrategyRunner:
    identity = StrategyIdentity(
        instance_id=instance_id, strategy_id="dummy", instrument=instrument,
        account_id=1, exchange=Exchange.NFO,
    )
    context = StrategyContext(
        identity=identity, config=DummyConfig(), session_factory=session_factory,
        broker=object(), market_data=FakeMarketData(), risk=FakeRisk(), time=clock,
        logger=logging.LoggerAdapter(logging.getLogger("test.scheduler"), {}),
    )
    strategy = DummyStrategy(
        context, triggers=triggers, fail_recover=fail_recover, fail_on_trigger=fail_on_trigger
    )
    return StrategyRunner(strategy)


def _wall_clock(hour: int, minute: int, day: date = date(2026, 7, 7)) -> MutableTime:
    # Wall-clock convention matching the rest of this suite: the stored
    # datetime's hour/minute IS the intended IST value; tzinfo is a placeholder
    # since only .time()/.date() are ever read from it.
    return MutableTime(ist=datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc), _today=day)


def instance_status(session_factory, instance_id: int) -> InstanceStatus:
    with session_factory() as s:
        return StrategyInstanceRepository(s).get_by_id_or_raise(instance_id).status


# --------------------------------------------------------------------------
# Registration
# --------------------------------------------------------------------------


class TestRegistration:
    def test_register_starts_the_runner(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(8, 0)
        runner = build_runner(sf, iid, triggers=[], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)

        scheduler.register(runner)

        assert runner.status is RunnerStatus.RUNNING
        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert "recover" in strategy.calls

    def test_double_registration_raises(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(8, 0)
        runner = build_runner(sf, iid, triggers=[], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.register(runner)
        with pytest.raises(ValueError):
            scheduler.register(runner)

    def test_unregister_stops_runner_by_default(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(8, 0)
        runner = build_runner(sf, iid, triggers=[], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.register(runner)

        scheduler.unregister(runner)

        assert runner.status is RunnerStatus.STOPPED
        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert "shutdown" in strategy.calls
        assert scheduler.registered_identities() == []

    def test_unregister_can_skip_stopping(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(8, 0)
        runner = build_runner(sf, iid, triggers=[], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.register(runner)

        scheduler.unregister(runner, stop_runner=False)

        assert runner.status is RunnerStatus.RUNNING  # not stopped by scheduler

    def test_unregister_unknown_runner_is_noop(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(8, 0)
        runner = build_runner(sf, iid, triggers=[], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.unregister(runner)  # never registered -- must not raise


# --------------------------------------------------------------------------
# Missed-trigger seeding at registration
# --------------------------------------------------------------------------


class TestMissedTriggerSeeding:
    def test_future_trigger_not_seeded(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(8, 0)  # before 09:20
        entry = TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.SKIP)
        runner = build_runner(sf, iid, triggers=[entry], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)

        scheduler.register(runner)
        scheduler._tick()  # still before 09:20 -> must not fire

        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert "trigger:entry" not in strategy.calls

    def test_missed_skip_trigger_is_skipped_for_today(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(10, 0)  # after 09:20 already
        entry = TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.SKIP)
        runner = build_runner(sf, iid, triggers=[entry], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)

        scheduler.register(runner)
        scheduler._tick()

        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert "trigger:entry" not in strategy.calls  # skipped, never fired today

    def test_missed_fire_on_startup_trigger_fires_immediately(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(16, 0)  # after 15:15 cutoff
        cutoff = TimeTrigger("cutoff", time(15, 15), TriggerCatchUpPolicy.FIRE_ON_STARTUP)
        runner = build_runner(sf, iid, triggers=[cutoff], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)

        scheduler.register(runner)
        scheduler._tick()

        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert "trigger:cutoff" in strategy.calls

    def test_weekend_registration_seeds_nothing_and_does_not_fire(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        saturday = date(2026, 7, 4)  # confirmed Saturday
        clock = _wall_clock(10, 0, day=saturday)
        entry = TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.FIRE_ON_STARTUP)
        runner = build_runner(sf, iid, triggers=[entry], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)

        scheduler.register(runner)
        scheduler._tick()

        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert "trigger:entry" not in strategy.calls  # non-trading day suppresses it


# --------------------------------------------------------------------------
# _tick firing behaviour
# --------------------------------------------------------------------------


class TestTickFiring:
    def test_trigger_fires_once_due(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(9, 0)
        entry = TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.SKIP)
        runner = build_runner(sf, iid, triggers=[entry], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.register(runner)

        scheduler._tick()  # 09:00 -- not due yet
        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert "trigger:entry" not in strategy.calls

        clock.ist = clock.ist.replace(hour=9, minute=20)
        scheduler._tick()  # now due
        assert "trigger:entry" in strategy.calls

    def test_trigger_does_not_refire_same_day(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(9, 0)  # register before due, so the normal tick path fires it
        entry = TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.SKIP)
        runner = build_runner(sf, iid, triggers=[entry], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.register(runner)

        clock.ist = clock.ist.replace(hour=9, minute=20)
        scheduler._tick()
        scheduler._tick()
        scheduler._tick()

        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert strategy.calls.count("trigger:entry") == 1

    def test_trigger_fires_again_next_day(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(9, 0)
        entry = TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.SKIP)
        runner = build_runner(sf, iid, triggers=[entry], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.register(runner)
        clock.ist = clock.ist.replace(hour=9, minute=20)
        scheduler._tick()

        # Next day, same time.
        clock._today = date(2026, 7, 8)
        clock.ist = clock.ist.replace(day=8)
        scheduler._tick()

        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert strategy.calls.count("trigger:entry") == 2

    def test_frozen_runner_is_skipped(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(9, 0)
        entry = TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.SKIP)
        cutoff = TimeTrigger("cutoff", time(15, 15), TriggerCatchUpPolicy.SKIP)
        runner = build_runner(sf, iid, triggers=[entry, cutoff], clock=clock, fail_on_trigger="entry")
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.register(runner)

        clock.ist = clock.ist.replace(hour=9, minute=20)
        scheduler._tick()  # entry fires and fails -> runner freezes
        assert runner.status is RunnerStatus.FROZEN
        assert instance_status(sf, iid) is InstanceStatus.FROZEN

        clock.ist = clock.ist.replace(hour=15, minute=15)
        scheduler._tick()  # cutoff would be due, but runner is FROZEN

        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert "trigger:cutoff" not in strategy.calls

    def test_multiple_runners_scheduled_independently(self):
        sf = make_engine_and_session_factory()
        iid1 = seed_instance(sf, instrument="NIFTY")
        iid2 = seed_instance(sf, instrument="SENSEX")
        clock = _wall_clock(9, 0)
        trigger = TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.SKIP)
        runner1 = build_runner(sf, iid1, triggers=[trigger], clock=clock, instrument="NIFTY")
        runner2 = build_runner(sf, iid2, triggers=[trigger], clock=clock, instrument="SENSEX")
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.register(runner1)
        scheduler.register(runner2)

        clock.ist = clock.ist.replace(hour=9, minute=20)
        scheduler._tick()

        s1: DummyStrategy = runner1._strategy  # type: ignore[attr-defined]
        s2: DummyStrategy = runner2._strategy  # type: ignore[attr-defined]
        assert "trigger:entry" in s1.calls
        assert "trigger:entry" in s2.calls

    def test_failing_trigger_does_not_block_other_runners(self):
        sf = make_engine_and_session_factory()
        iid1 = seed_instance(sf, instrument="NIFTY")
        iid2 = seed_instance(sf, instrument="SENSEX")
        clock = _wall_clock(9, 0)
        trigger = TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.SKIP)
        bad_runner = build_runner(sf, iid1, triggers=[trigger], clock=clock, fail_on_trigger="entry", instrument="NIFTY")
        good_runner = build_runner(sf, iid2, triggers=[trigger], clock=clock, instrument="SENSEX")
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.register(bad_runner)
        scheduler.register(good_runner)

        clock.ist = clock.ist.replace(hour=9, minute=20)
        scheduler._tick()

        good: DummyStrategy = good_runner._strategy  # type: ignore[attr-defined]
        assert "trigger:entry" in good.calls
        assert bad_runner.status is RunnerStatus.FROZEN

    def test_non_trading_day_suppresses_firing(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        saturday = date(2026, 7, 4)
        clock = _wall_clock(6, 0, day=saturday)  # before 09:20 at registration, so nothing seeded
        entry = TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.SKIP)
        runner = build_runner(sf, iid, triggers=[entry], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.register(runner)

        clock.ist = clock.ist.replace(hour=9, minute=20)  # now due, still Saturday
        scheduler._tick()

        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert "trigger:entry" not in strategy.calls


# --------------------------------------------------------------------------
# Real background thread
# --------------------------------------------------------------------------


class TestBackgroundThread:
    def test_thread_fires_due_trigger(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(9, 19, day=date(2026, 7, 8))  # Wednesday, just before due
        entry = TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.SKIP)
        runner = build_runner(sf, iid, triggers=[entry], clock=clock)
        scheduler = PlatformScheduler(
            time_provider=clock, config=SchedulerConfig(poll_interval_seconds=0.05)
        )
        scheduler.register(runner)
        scheduler.start()
        try:
            clock.ist = clock.ist.replace(minute=20)  # becomes due
            deadline = real_time.monotonic() + 2.0
            strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
            while "trigger:entry" not in strategy.calls and real_time.monotonic() < deadline:
                real_time.sleep(0.02)
            assert "trigger:entry" in strategy.calls
        finally:
            scheduler.stop()

    def test_start_is_idempotent(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(9, 0)
        scheduler = PlatformScheduler(time_provider=clock, config=SchedulerConfig(poll_interval_seconds=0.05))
        scheduler.start()
        first_thread = scheduler._thread
        scheduler.start()
        assert scheduler._thread is first_thread
        scheduler.stop()

    def test_stop_joins_thread_and_stops_runners(self):
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(9, 0)
        runner = build_runner(sf, iid, triggers=[], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock, config=SchedulerConfig(poll_interval_seconds=0.05))
        scheduler.register(runner)
        scheduler.start()

        scheduler.stop()

        assert scheduler._thread is None
        assert runner.status is RunnerStatus.STOPPED
        strategy: DummyStrategy = runner._strategy  # type: ignore[attr-defined]
        assert "shutdown" in strategy.calls

    def test_stop_is_idempotent(self):
        clock = _wall_clock(9, 0)
        scheduler = PlatformScheduler(time_provider=clock, config=SchedulerConfig(poll_interval_seconds=0.05))
        scheduler.start()
        scheduler.stop()
        scheduler.stop()  # must not raise

    def test_stop_clears_schedules_allowing_reregistration(self):
        """A scheduler instance must be reusable for a subsequent start()/
        register() cycle (e.g. the dependency container restarting the
        platform without discarding the scheduler object) -- stop() must not
        leave stale entries that make register() raise 'already registered'.

        A StrategyRunner itself is single-use (start() only works from
        CREATED, per its own contract), so a real restart always registers a
        *new* runner object built fresh from the same instance id -- exactly
        what this test does, and what DependencyContainer.start() does on a
        second call.
        """
        sf = make_engine_and_session_factory()
        iid = seed_instance(sf)
        clock = _wall_clock(9, 0)
        first_runner = build_runner(sf, iid, triggers=[], clock=clock)
        scheduler = PlatformScheduler(time_provider=clock)
        scheduler.register(first_runner)

        scheduler.stop()

        assert scheduler.registered_identities() == []
        second_runner = build_runner(sf, iid, triggers=[], clock=clock)
        scheduler.register(second_runner)  # must not raise "already registered"
        assert scheduler.registered_identities() == [second_runner.identity_str]


# --------------------------------------------------------------------------
# WeekdayTradingCalendar
# --------------------------------------------------------------------------


class TestWeekdayTradingCalendar:
    def test_weekdays_are_trading_days(self):
        calendar = WeekdayTradingCalendar()
        monday = date(2026, 7, 6)
        for offset in range(5):
            assert calendar.is_trading_day(date.fromordinal(monday.toordinal() + offset))

    def test_weekend_is_not_a_trading_day(self):
        calendar = WeekdayTradingCalendar()
        assert not calendar.is_trading_day(date(2026, 7, 4))  # Saturday
        assert not calendar.is_trading_day(date(2026, 7, 5))  # Sunday
