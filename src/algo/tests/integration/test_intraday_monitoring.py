"""Proof that the intraday stop-loss/target evaluation is actually wired
(resolves review finding C1).

Before this, a position was only ever evaluated at the 15:15 hard cutoff --
nothing drove the monitor during the day. These tests verify, end to end with a
real DependencyContainer / SimulationBroker / Strategy1, that:

* the **pull path** (the MonitoringScheduler heartbeat) closes a position on
  target/stop-loss on its own background cadence, with NO manual trigger and
  well before the cutoff time; and
* the **push path** (a live tick delivered through MarketDataService to the
  runner) closes a position on the same conditions.

Both are exercised through the real wiring the container sets up in start(),
not by calling the monitor directly.
"""

from __future__ import annotations

import time
from decimal import Decimal

from algo.brokers.simulation import StaticPriceSource
from algo.common.enums import ExitReason, PositionState
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.strategy_instance_repository import StrategyInstanceRepository
from algo.strategy_engine.strategy_context import Tick
from algo.strategy_engine.strategy_scheduler import MonitoringSchedulerConfig
from algo.tests.integration.conftest import (
    NIFTY,
    atm_legs,
    build_container,
    build_nifty_option_chain,
    make_clock,
)


def _position(container, clock):
    with container.session_factory() as session:
        instance = StrategyInstanceRepository(session).get_by_strategy_instrument_account(
            "strategy_1", NIFTY, container._account_ids["primary"]  # noqa: SLF001
        )
        return PositionRepository(session).get_by_instance_and_date(instance.id, clock.today())


def _wait_for_state(container, clock, expected, *, timeout=5.0):
    deadline = time.monotonic() + timeout
    pos = _position(container, clock)
    while pos is None or pos.state is not expected:
        if time.monotonic() > deadline:
            raise AssertionError(
                f"position did not reach {expected} within {timeout}s "
                f"(last: {None if pos is None else pos.state})"
            )
        time.sleep(0.02)
        pos = _position(container, clock)
    return pos


class PushableTickStream:
    """A TickStream a test can push ticks through by hand, to exercise the
    push path (MarketDataService -> consumer -> runner.dispatch_tick)."""

    def __init__(self) -> None:
        self._on_tick = None

    def set_handlers(self, *, on_tick, on_connect, on_disconnect, on_reconnect) -> None:  # noqa: ANN001
        self._on_tick = on_tick

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    def subscribe(self, instruments) -> None:  # noqa: ANN001
        pass

    def unsubscribe(self, instruments) -> None:  # noqa: ANN001
        pass

    def push(self, tick: Tick) -> None:
        assert self._on_tick is not None, "handlers not registered"
        self._on_tick(tick)


class TestHeartbeatDrivesIntradayExit:
    """The pull path: the monitoring heartbeat, on its own, closes a position
    intraday -- no manual trigger, clock held well before the 15:15 cutoff."""

    def test_target_hit_intraday_closes_position(self, tmp_path):
        clock = make_clock(hour=9, minute=0)
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite",
            instrument_catalog=catalog, price_source=prices,
            monitoring_scheduler_config=MonitoringSchedulerConfig(interval_seconds=0.05),
        )
        container.start()
        try:
            # Enter at 09:20 (manual, since the platform scheduler is idled in tests).
            clock.set_time(hour=9, minute=20)
            container.runners[0].dispatch_time_trigger("entry")
            assert _position(container, clock).state is PositionState.OPEN

            # Move to mid-morning (well before cutoff) and drop the premium to the target.
            clock.set_time(hour=11, minute=0)
            prices.set_price(call, Decimal("80"))
            prices.set_price(put, Decimal("80"))  # combined 160 <= target 180

            # No manual trigger: the background heartbeat must close it.
            position = _wait_for_state(container, clock, PositionState.CLOSED)
            assert position.exit_reason is ExitReason.TARGET
        finally:
            container.stop()

    def test_stoploss_hit_intraday_closes_position(self, tmp_path):
        clock = make_clock(hour=9, minute=0)
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite",
            instrument_catalog=catalog, price_source=prices,
            monitoring_scheduler_config=MonitoringSchedulerConfig(interval_seconds=0.05),
        )
        container.start()
        try:
            clock.set_time(hour=9, minute=20)
            container.runners[0].dispatch_time_trigger("entry")
            assert _position(container, clock).state is PositionState.OPEN

            clock.set_time(hour=11, minute=0)
            prices.set_price(call, Decimal("120"))
            prices.set_price(put, Decimal("120"))  # combined 240 >= stoploss 220

            position = _wait_for_state(container, clock, PositionState.CLOSED)
            assert position.exit_reason is ExitReason.STOPLOSS
        finally:
            container.stop()

    def test_heartbeat_is_harmless_when_flat(self, tmp_path):
        """Before any entry, the heartbeat runs but must do nothing -- no
        position, no error."""
        clock = make_clock(hour=9, minute=0)
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite",
            instrument_catalog=catalog, price_source=prices,
            monitoring_scheduler_config=MonitoringSchedulerConfig(interval_seconds=0.02),
        )
        container.start()
        try:
            time.sleep(0.2)  # let several heartbeats fire against a flat book
            assert _position(container, clock) is None
            assert container.runners[0].status.name == "RUNNING"  # not frozen
        finally:
            container.stop()


class TestPushedTickDrivesExit:
    """The push path: a live tick delivered through the market-data feed closes
    the position, proving the tick->consumer->runner wiring is live."""

    def test_pushed_ticks_crossing_target_close_position(self, tmp_path):
        clock = make_clock(hour=9, minute=0)
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})
        stream = PushableTickStream()
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite",
            instrument_catalog=catalog, price_source=prices,
            tick_stream=stream,  # override the never-pushing default
        )
        container.start()
        try:
            clock.set_time(hour=9, minute=20)
            container.runners[0].dispatch_time_trigger("entry")
            assert _position(container, clock).state is PositionState.OPEN

            clock.set_time(hour=11, minute=0)
            now = clock.now()
            # Deliver a fresh tick for each leg, low enough to cross the target.
            stream.push(Tick(instrument=call, last_price=Decimal("80"), timestamp=now))
            stream.push(Tick(instrument=put, last_price=Decimal("80"), timestamp=now))

            # Synchronous path: the second tick's evaluation should have closed it.
            position = _position(container, clock)
            assert position.state is PositionState.CLOSED
            assert position.exit_reason is ExitReason.TARGET
        finally:
            container.stop()


class TestKillSwitchFlattensOpenPosition:
    """H2 end-to-end: engaging the kill switch against a running platform
    causes the monitoring heartbeat to flatten an open position via the normal
    exit path -- even though the price is flat (no target/stop-loss trigger)."""

    def test_engaged_kill_switch_closes_open_position(self, tmp_path):
        clock = make_clock(hour=9, minute=0)
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite",
            instrument_catalog=catalog, price_source=prices,
            monitoring_scheduler_config=MonitoringSchedulerConfig(interval_seconds=0.05),
        )
        container.start()
        try:
            clock.set_time(hour=9, minute=20)
            container.runners[0].dispatch_time_trigger("entry")
            assert _position(container, clock).state is PositionState.OPEN

            # Price stays flat (combined 200, between target 180 and stop 220):
            # nothing price-based would exit. The kill switch is the only reason.
            clock.set_time(hour=11, minute=0)
            engaged = container.kill_switch.engage(reason="integration test halt", activated_by="test")
            assert engaged is True

            position = _wait_for_state(container, clock, PositionState.CLOSED)
            assert position.exit_reason is ExitReason.KILL_SWITCH
        finally:
            container.stop()

    def test_kill_switch_blocks_a_new_entry(self, tmp_path):
        clock = make_clock(hour=9, minute=0)
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite",
            instrument_catalog=catalog, price_source=prices,
        )
        container.start()
        try:
            # Engage BEFORE the entry trigger fires.
            container.kill_switch.engage(reason="halt before entry", activated_by="test")
            clock.set_time(hour=9, minute=20)
            container.runners[0].dispatch_time_trigger("entry")
            # No position should have been opened.
            assert _position(container, clock) is None
        finally:
            container.stop()
