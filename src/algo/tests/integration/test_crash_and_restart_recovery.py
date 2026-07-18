"""Restart and crash recovery: a real DependencyContainer "crashes" (is
stopped, mid-lifecycle) and a fresh, independent container is built against
the same persistent (file-backed) database -- exactly how a real process
restart works against Postgres. See conftest.py's module docstring for why
file-backed SQLite (not in-memory) is required for this to be meaningful.

What this file does NOT re-test (already covered by each module's own unit
suite): every branch of Strategy1.recover()'s own logic in isolation
(test_strategy.py), every ReconciliationEngine break-classification case
(test_reconciliation_engine.py), every TriggerCatchUpPolicy permutation in
isolation (test_platform_scheduler.py). This file tests that a second,
independently-constructed container correctly reconstructs a first
container's in-flight state through the real, wired restart path -- register()
-> runner.start() -> Strategy.recover() -- not that each piece's own logic is
correct.
"""

from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path

import yaml

from algo.brokers.simulation import StaticPriceSource
from algo.common.enums import ExitReason, InstanceStatus, PositionState, StateTransitionActor
from algo.database.models import Position
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.strategy_instance_repository import StrategyInstanceRepository
from algo.scheduler import SchedulerConfig
from algo.strategy_engine.strategies.strategy_1.state_machine import PositionStateMachine
from algo.tests.integration.conftest import (
    atm_legs,
    build_container,
    build_nifty_option_chain,
    make_clock,
)


def _instance_id(container) -> int:
    with container.session_factory() as session:
        instance = StrategyInstanceRepository(session).get_by_strategy_instrument_account(
            "strategy_1", "NIFTY", container._account_ids["primary"]  # noqa: SLF001
        )
        return instance.id


def _position(container, clock):
    instance_id = _instance_id(container)
    with container.session_factory() as session:
        return PositionRepository(session).get_by_instance_and_date(instance_id, clock.today())


def _wait_for_state(container, clock, expected: PositionState, *, timeout: float = 5.0):
    """Poll for a background-thread-driven state change, bounded -- used only
    where a real PlatformScheduler background thread (not a manual _tick()
    call) is what's expected to make the change, per this module's own
    determinism notes."""
    deadline = time.monotonic() + timeout
    position = _position(container, clock)
    while position is None or position.state is not expected:
        if time.monotonic() > deadline:
            raise AssertionError(
                f"position did not reach {expected} within {timeout}s "
                f"(last seen: {None if position is None else position.state})"
            )
        time.sleep(0.02)
        position = _position(container, clock)
    return position


def _instance_status(container) -> InstanceStatus:
    with container.session_factory() as session:
        instance = StrategyInstanceRepository(session).get_by_strategy_instrument_account(
            "strategy_1", "NIFTY", container._account_ids["primary"]  # noqa: SLF001
        )
        return instance.status


class TestFreshStartRestart:
    def test_restart_with_no_prior_state_is_a_fresh_start(self, tmp_path):
        clock = make_clock(hour=9, minute=0)
        db_path = tmp_path / "db.sqlite"
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})

        first = build_container(
            tmp_path, clock=clock, db_path=db_path, instrument_catalog=catalog, price_source=prices,
        )
        first.start()
        first.stop()  # "crash" before the entry trigger ever fires

        second = build_container(
            tmp_path, clock=clock, db_path=db_path, instrument_catalog=catalog, price_source=prices,
        )
        second.start()
        try:
            assert second.runners[0].status.name == "RUNNING"
            assert _position(second, clock) is None
        finally:
            second.stop()


class TestOpenPositionRestart:
    def test_restart_resumes_monitoring_and_can_still_exit(self, tmp_path):
        """Crash while a position is OPEN (the common case: process dies
        overnight, or is redeployed mid-day) -- a fresh container's recover()
        must re-attach monitoring via PositionMonitor.attach(), and the
        resumed monitor must still correctly close the position on the next
        price move, exactly as if the process had never restarted.

        The second container reuses the first's own ``.broker`` object
        (rather than letting DependencyContainer build a fresh one) -- a real
        broker's (Kite's, or the exchange behind it) position/order state
        persists independently of this process restarting; a brand new
        SimulationBroker's does not (it is purely in-memory), so building one
        fresh here would make reconciliation correctly, but unhelpfully for
        *this* test, flag a broker/database mismatch that only exists because
        nothing told the new broker about the first one's fills.
        """
        clock = make_clock(hour=9, minute=0)
        db_path = tmp_path / "db.sqlite"
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})

        first = build_container(
            tmp_path, clock=clock, db_path=db_path, instrument_catalog=catalog, price_source=prices,
        )
        first.start()
        clock.set_time(hour=9, minute=20)
        first.runners[0].dispatch_time_trigger("entry")
        assert _position(first, clock).state is PositionState.OPEN
        first.stop()  # "crash": process dies with the position still OPEN

        second = build_container(
            tmp_path, clock=clock, db_path=db_path, instrument_catalog=catalog, price_source=prices,
            broker=first.broker,
        )
        second.start()
        try:
            # recover() ran inside start() -> register(); the position must
            # still be OPEN (recovery re-attached monitoring, not re-entered).
            assert _position(second, clock).state is PositionState.OPEN

            prices.set_price(call, Decimal("80"))
            prices.set_price(put, Decimal("80"))
            clock.set_time(hour=11, minute=0)
            second.runners[0].dispatch_time_trigger("cutoff")

            position = _position(second, clock)
            assert position.state is PositionState.CLOSED
            assert position.exit_reason is ExitReason.TARGET
        finally:
            second.stop()


class TestEntryPendingCrash:
    def test_restart_with_entry_pending_and_no_broker_orders_cleanly_aborts(self, tmp_path):
        """A crash right after the durable ENTRY_PENDING record was written
        but before any broker order was placed leaves no exposure at all --
        reconciliation (which runs before any runner's recover(), broker
        reachable) resolves this deterministically to CLOSED on its own, a
        clean abort needing no manual intervention. This is
        ReconciliationEngine's primary, broker-aware recovery path taking
        over; Strategy.recover()'s own "ENTRY_PENDING has no resume path ->
        freeze" branch is reached only when reconciliation itself cannot run
        (broker unavailable at startup) -- a materially different, narrower
        scenario this test does not attempt to reproduce.

        The account and strategy-instance rows are created by a first,
        normal (idle -- clock stays before entry_time) container start, then
        the crash state is seeded directly: a faithful stand-in for what a
        real crash leaves behind (a row stuck mid-transition), without racing
        a background thread to reproduce the exact interruption point.
        """
        clock = make_clock(hour=9, minute=0)
        db_path = tmp_path / "db.sqlite"
        catalog = build_nifty_option_chain()
        prices = StaticPriceSource({})

        first = build_container(
            tmp_path, clock=clock, db_path=db_path, instrument_catalog=catalog, price_source=prices,
        )
        first.start()  # creates the Account + StrategyInstance rows; no entry yet
        instance_id = _instance_id(first)
        first.stop()

        with first.session_factory() as session:
            session.add(
                Position(strategy_instance_id=instance_id, trade_date=clock.today(), state=PositionState.ENTRY_PENDING)
            )
            session.commit()

        second = build_container(
            tmp_path, clock=clock, db_path=db_path, instrument_catalog=catalog, price_source=prices,
            broker=first.broker,
        )
        second.start()
        try:
            assert second.runners[0].status.name == "RUNNING"  # not frozen
            assert _position(second, clock).state is PositionState.CLOSED
            assert _instance_status(second) is InstanceStatus.ACTIVE
        finally:
            second.stop()


class TestExitPendingCrash:
    def test_restart_with_exit_pending_completes_the_interrupted_exit(self, tmp_path):
        """Crash after an exit was decided (state machine moved OPEN ->
        EXIT_PENDING, recorded with exit_logic's own transition-reason format)
        but before ExitLogic actually closed the legs. recover() must infer
        the original reason from the audit trail and complete the exit --
        driven here from a REAL entry's real, filled legs (not synthetic
        ones), so ExitLogic.exit() on restart has genuine open positions to
        close, exactly as a real crash would leave them. The second container
        reuses the first's ``.broker`` -- see the equivalent note in
        TestOpenPositionRestart for why a fresh SimulationBroker would
        incorrectly make reconciliation see a broker/database mismatch here.
        """
        clock = make_clock(hour=9, minute=0)
        db_path = tmp_path / "db.sqlite"
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})

        first = build_container(
            tmp_path, clock=clock, db_path=db_path, instrument_catalog=catalog, price_source=prices,
        )
        first.start()
        clock.set_time(hour=9, minute=20)
        first.runners[0].dispatch_time_trigger("entry")
        assert _position(first, clock).state is PositionState.OPEN

        with first.session_factory() as session:
            repo = PositionRepository(session)
            position = repo.get_by_instance_and_date(_instance_id(first), clock.today())
            PositionStateMachine(repo).transition(
                position, to_state=PositionState.EXIT_PENDING,
                actor=StateTransitionActor.STRATEGY,
                reason=f"exit triggered: {ExitReason.TARGET.value}",
            )
            session.commit()
        first.stop()  # "crash": process dies after deciding to exit, before closing legs

        second = build_container(
            tmp_path, clock=clock, db_path=db_path, instrument_catalog=catalog, price_source=prices,
            broker=first.broker,
        )
        second.start()
        try:
            position = _position(second, clock)
            assert position.state is PositionState.CLOSED
            assert position.exit_reason is ExitReason.TARGET
        finally:
            second.stop()


_STRATEGY_1_NIFTY_CONFIG = Path("configs/strategies/strategy_1/nifty.yaml")


def _real_nifty_entry_hour_minute() -> tuple[int, int]:
    """The current, real, committed entry_time for NIFTY strategy_1, as
    (hour, minute).

    Read live rather than hardcoded: ``build_container``'s
    ``make_config_root`` helper copies this real, user-editable file
    verbatim, so a test that hardcodes "a clock time past entry_time" goes
    silently stale -- and starts testing a different scenario than it claims
    to -- the moment someone edits ``entry_time`` in this file (as happened
    once already: it moved from 09:20 to 11:25 mid-project). Reading it here
    keeps the test correct regardless of what it currently says.
    """
    raw = yaml.safe_load(_STRATEGY_1_NIFTY_CONFIG.read_text(encoding="utf-8"))
    hour_str, minute_str, *_ = raw["entry_time"].split(":")
    return int(hour_str), int(minute_str)


def _add_minutes(hour: int, minute: int, delta_minutes: int) -> tuple[int, int]:
    total = hour * 60 + minute + delta_minutes
    return (total // 60) % 24, total % 60


class TestMissedTriggerCatchUpOnRestart:
    def test_missed_entry_trigger_is_skipped_not_fired_late(self, tmp_path):
        """entry uses TriggerCatchUpPolicy.SKIP -- a restart past the
        configured entry time with no position yet must NOT force a late
        entry (a missed entry window is a deliberate no-trade, per
        strategy.py's own documented design)."""
        entry_hour, entry_minute = _real_nifty_entry_hour_minute()
        # Comfortably after today's configured entry time -- computed from
        # the real config, not hardcoded, so this stays "already well past
        # entry" no matter what entry_time currently is.
        past_hour, past_minute = _add_minutes(entry_hour, entry_minute, 40)
        clock = make_clock(hour=past_hour, minute=past_minute)
        db_path = tmp_path / "db.sqlite"
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})

        container = build_container(
            tmp_path, clock=clock, db_path=db_path, instrument_catalog=catalog, price_source=prices,
        )
        container.start()
        try:
            assert _position(container, clock) is None
            # Advancing the clock further and ticking again must still not
            # enter -- it was seeded SKIPPED for today, not merely "not yet due".
            later_hour, later_minute = _add_minutes(entry_hour, entry_minute, 280)
            clock.set_time(hour=later_hour, minute=later_minute)
            container.scheduler._tick()  # noqa: SLF001
            assert _position(container, clock) is None
        finally:
            container.stop()

    def test_missed_cutoff_trigger_fires_on_restart_and_closes_open_position(self, tmp_path):
        """cutoff uses TriggerCatchUpPolicy.FIRE_ON_STARTUP -- a restart past
        15:15 with an OPEN position left over from before the crash must force
        the exit immediately, via the scheduler's own catch-up + tick path
        (not a manually-dispatched trigger), the moment the scheduler is
        ticked. The second container reuses the first's ``.broker`` -- see
        the equivalent note in TestOpenPositionRestart."""
        clock = make_clock(hour=9, minute=0)
        db_path = tmp_path / "db.sqlite"
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})

        first = build_container(
            tmp_path, clock=clock, db_path=db_path, instrument_catalog=catalog, price_source=prices,
        )
        first.start()
        clock.set_time(hour=9, minute=20)
        first.runners[0].dispatch_time_trigger("entry")
        assert _position(first, clock).state is PositionState.OPEN
        first.stop()

        clock.set_time(hour=15, minute=30)  # past the 15:15 hard cutoff
        second = build_container(
            tmp_path, clock=clock, db_path=db_path, instrument_catalog=catalog, price_source=prices,
            broker=first.broker,
        )
        second.start()
        try:
            # No manual _tick() call here: PlatformScheduler's background
            # loop (started by container.start()) evaluates a tick
            # immediately, before its first wait -- calling _tick() again
            # manually would race that same first automatic tick against the
            # same due trigger for the same runner. Instead, wait (briefly,
            # boundedly) for that one automatic tick to complete, the same
            # pattern test_platform_scheduler.py's own background-thread
            # tests use.
            position = _wait_for_state(second, clock, PositionState.CLOSED)
            assert position.exit_reason is ExitReason.TIMEOUT
        finally:
            second.stop()
