"""H4 end-to-end: the startup flow with the REAL seams.

Two proofs:

* the connectivity-verification gate fails fast (the container refuses to start
  if the broker reports unhealthy, before any strategy is built); and
* a container wired exactly the way ``start_paper``'s ``build_seams`` wires it
  -- the real ``ConfigInstrumentService`` / ``ConfigExpiryService`` /
  ``PollingTickStream`` and the container-built ``BrokerSpotPriceProvider`` --
  starts, verifies connectivity, enters a trade, and exits it via the
  monitoring heartbeat. This exercises the real seams end to end, not the test
  fakes the other integration files use.
"""

from __future__ import annotations

import random
import time
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from algo.brokers.broker_base import HealthStatus, InstrumentIdentifier
from algo.brokers.exceptions import BrokerConnectionError
from algo.brokers.simulation import InstrumentCatalog, SimulationBroker, SimulationConfig, StaticPriceSource
from algo.common.enums import ExitReason, Exchange, OptionType, PositionState
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.strategy_instance_repository import StrategyInstanceRepository
from algo.market_data.polling_tick_stream import PollingTickStream
from algo.scheduler.trading_calendar import WeekdayTradingCalendar
from algo.services.live_seams import ConfigExpiryService, ConfigInstrumentService
from algo.strategy_engine.strategy_scheduler import MonitoringSchedulerConfig
from algo.tests.integration.conftest import (
    NIFTY,
    NIFTY_ATM_STRIKE,
    NIFTY_LOT_SIZE,
    NIFTY_STRIKE_INTERVAL,
    build_container,
    make_clock,
)


def _position(container, clock):
    with container.session_factory() as session:
        inst = StrategyInstanceRepository(session).get_by_strategy_instrument_account(
            "strategy_1", NIFTY, container._account_ids["primary"]  # noqa: SLF001
        )
        return PositionRepository(session).get_by_instance_and_date(inst.id, clock.today())


def _wait_for_state(container, clock, expected, *, timeout=5.0):
    deadline = time.monotonic() + timeout
    pos = _position(container, clock)
    while pos is None or pos.state is not expected:
        if time.monotonic() > deadline:
            raise AssertionError(f"not {expected} within {timeout}s (last={None if pos is None else pos.state})")
        time.sleep(0.02)
        pos = _position(container, clock)
    return pos


class _UnhealthyBroker(SimulationBroker):
    """Authenticates fine but reports unhealthy -- to exercise the gate."""

    def health_check(self, *, timeout=None) -> HealthStatus:
        return HealthStatus(healthy=False, checked_at=self._clock.now(), latency_ms=1.0,
                            detail="simulated broker outage")


class TestConnectivityGate:
    def test_container_fails_fast_when_broker_unhealthy(self, tmp_path):
        clock = make_clock(hour=9, minute=0)
        unhealthy = _UnhealthyBroker(
            instrument_catalog=InstrumentCatalog(), price_source=StaticPriceSource({}),
            config=SimulationConfig(synchronous=True), rng=random.Random(0),
        )
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite", broker=unhealthy,
        )
        with pytest.raises(BrokerConnectionError, match="connectivity"):
            container.start()
        assert container.is_started is False


class TestRealSeamsStartupFlow:
    def test_starts_enters_and_exits_with_real_seams(self, tmp_path):
        # Clock on a Monday; ConfigExpiryService reads the real, committed
        # configs/instruments/nifty.yaml (expiry_weekday: 1 = Tuesday, per
        # NSE's current weekly-expiry day) and resolves the current weekly
        # expiry to Tuesday 2026-07-07 -- matching the chain built below.
        # (Not the fake NIFTY_EXPIRY constant other integration tests use --
        # this is the one test in the suite that exercises the *real*
        # ConfigExpiryService against the real config, so its own local
        # scenario must actually agree with what that config now says.)
        #
        # Deliberately in the past relative to the real calendar, not the
        # future: DependencyContainer._build_broker() constructs
        # SimulationBroker with no clock= override, so it always timestamps
        # fills with the REAL wall clock (SystemClock) regardless of this
        # test's mocked `clock` -- a separate, pre-existing gap between
        # build_container()'s strategy-side TimeProvider and the broker's
        # own Clock. A future-dated scenario makes that gap produce an
        # exit_time earlier than entry_time (a real CHECK constraint
        # violation); a past-dated one does not. Left as a known test-infra
        # gap, not fixed here -- out of scope for this change.
        expiry = date(2026, 7, 7)
        clock = make_clock(hour=9, minute=0, day=date(2026, 7, 6))
        catalog = InstrumentCatalog.build_option_chain(
            underlying=NIFTY, exchange=Exchange.NFO, expiry=expiry,
            atm_strike=NIFTY_ATM_STRIKE, strike_interval=NIFTY_STRIKE_INTERVAL,
            num_strikes_each_side=5, lot_size=NIFTY_LOT_SIZE,
        )
        call_instrument = catalog.find_option(
            underlying=NIFTY, expiry=expiry, strike=NIFTY_ATM_STRIKE,
            option_type=OptionType.CE, exchange=Exchange.NFO,
        )
        put_instrument = catalog.find_option(
            underlying=NIFTY, expiry=expiry, strike=NIFTY_ATM_STRIKE,
            option_type=OptionType.PE, exchange=Exchange.NFO,
        )
        call = InstrumentIdentifier(exchange=call_instrument.exchange, tradingsymbol=call_instrument.tradingsymbol)
        put = InstrumentIdentifier(exchange=put_instrument.exchange, tradingsymbol=put_instrument.tradingsymbol)
        spot = InstrumentIdentifier(exchange=Exchange.NSE, tradingsymbol="NIFTY 50")
        prices = StaticPriceSource({
            spot: NIFTY_ATM_STRIKE,     # BrokerSpotPriceProvider reads this
            call: Decimal("100"),
            put: Decimal("100"),
        })

        instruments = ConfigInstrumentService(Path("configs/instruments"))
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite",
            instrument_catalog=catalog, price_source=prices,
            # The real seams, exactly as start_paper.build_seams() supplies them:
            instrument_service=instruments,
            expiry_service=ConfigExpiryService(instrument_service=instruments,
                                               trading_calendar=WeekdayTradingCalendar()),
            tick_stream=PollingTickStream(),
            spot_price_provider=None,  # let the container build the broker-backed one
            monitoring_scheduler_config=MonitoringSchedulerConfig(interval_seconds=0.05),
        )
        container.start()
        try:
            # Startup flow completed: connectivity verified, strategies armed.
            assert container.is_started is True
            assert len(container.runners) == 1

            clock.set_time(hour=9, minute=20)
            container.runners[0].dispatch_time_trigger("entry")
            assert _position(container, clock).state is PositionState.OPEN

            # Drop the premium to the target; the heartbeat (polling, no ws) exits.
            clock.set_time(hour=11, minute=0)
            prices.set_price(call, Decimal("80"))
            prices.set_price(put, Decimal("80"))
            position = _wait_for_state(container, clock, PositionState.CLOSED)
            assert position.exit_reason is ExitReason.TARGET
        finally:
            container.stop()
