"""Application startup and graceful shutdown, end to end: a real
``app.Application`` wrapping a real ``DependencyContainer`` (Simulation
broker), run through its full lifecycle -- not the fake-container unit tests
already covering ``Application``'s own logic in isolation (``test_app.py``)
or the container's own construction/lifecycle in isolation
(``test_dependency_container.py``). This file checks that the two work
correctly *together*: a real scheduler thread actually starts, real strategy
instances actually get registered and reach RUNNING, and a real shutdown
(via ``request_shutdown()``, standing in for a delivered OS signal) actually
stops everything -- no orphaned threads, no half-torn-down state.
"""

from __future__ import annotations

import threading
import time
from decimal import Decimal

from algo.app import Application, BrokerModeMismatchError
from algo.brokers.simulation import StaticPriceSource
from algo.common.enums import BrokerName
from algo.strategy_engine.strategy_runner import RunnerStatus
from algo.tests.integration.conftest import (
    atm_legs,
    build_container,
    build_nifty_option_chain,
    make_clock,
)


class TestFullLifecycle:
    def test_run_starts_and_gracefully_shuts_down(self, tmp_path):
        clock = make_clock(hour=9, minute=0)
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite",
            instrument_catalog=catalog, price_source=prices,
        )
        application = Application(
            container, expected_broker=BrokerName.SIMULATION, install_signal_handlers=False,
        )

        thread = threading.Thread(target=application.run, daemon=True)
        thread.start()

        deadline = time.monotonic() + 5.0
        while not container.is_started:
            assert time.monotonic() < deadline, "container never reached started"
            time.sleep(0.01)

        assert len(container.runners) == 1
        assert container.runners[0].status is RunnerStatus.RUNNING
        assert container.scheduler.registered_identities() == [container.runners[0].identity_str]

        application.request_shutdown()
        thread.join(timeout=5.0)

        assert not thread.is_alive()
        assert container.is_started is False
        assert container.runners[0].status is RunnerStatus.STOPPED

    def test_broker_mode_mismatch_never_starts_the_container(self, tmp_path):
        clock = make_clock(hour=9, minute=0)
        catalog = build_nifty_option_chain()
        prices = StaticPriceSource({})
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite",
            instrument_catalog=catalog, price_source=prices,
        )
        # brokers.yaml in this suite's temp config always selects SIMULATION.
        application = Application(
            container, expected_broker=BrokerName.KITE, install_signal_handlers=False,
        )

        try:
            application.start()
            raised = False
        except BrokerModeMismatchError:
            raised = True

        assert raised is True
        assert container.is_started is False
        assert container.runners == []
