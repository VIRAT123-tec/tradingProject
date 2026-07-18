"""End-to-end trade lifecycle: a real DependencyContainer, real
SimulationBroker, and real Strategy1 driven through an entire trade -- entry,
monitoring, exit -- with success and failure scenarios.

This is the suite that found and drove the fix for a real, previously-latent
integration bug: ``configs/app.yaml``'s instrument identity casing didn't
match ``risk.yaml``'s lookup keys (fixed), and ``LtpPoller.get_ltps`` didn't
match ``BrokerBase.get_ltp`` (fixed) -- neither was reachable from any
existing unit test, because each one supplied its own same-named fake. See
conftest.py's module docstring for the full rationale.

What this file does NOT re-test (already covered by each module's own unit
suite): every ``evaluate_exit`` priority-ordering permutation, every
``PositionStateMachine`` transition-legality case, ATM-strike rounding edge
cases, every individual risk-check rejection reason. This file tests that the
real wiring between those modules produces the right end-to-end outcome for
one representative case of each scenario.
"""

from __future__ import annotations

from decimal import Decimal

from algo.brokers.simulation import StaticPriceSource
from algo.common.enums import ExitReason, PositionState
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.strategy_instance_repository import StrategyInstanceRepository
from algo.tests.integration.conftest import (
    atm_legs,
    build_container,
    build_nifty_option_chain,
    make_clock,
)


def _entry_and_get_position(container, runner, clock):
    clock.set_time(hour=9, minute=20)
    runner.dispatch_time_trigger("entry")
    instance_id = runner._strategy.context.identity.instance_id  # noqa: SLF001
    with container.session_factory() as session:
        return PositionRepository(session).get_by_instance_and_date(instance_id, clock.today())


def _current_position(container, runner, clock):
    instance_id = runner._strategy.context.identity.instance_id  # noqa: SLF001
    with container.session_factory() as session:
        return PositionRepository(session).get_by_instance_and_date(instance_id, clock.today())


class TestEntry:
    def test_entry_opens_a_position_with_correct_premiums(self, tmp_path):
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
            runner = container.runners[0]
            position = _entry_and_get_position(container, runner, clock)

            assert position is not None
            assert position.state is PositionState.OPEN
            assert position.combined_entry_premium == Decimal("200.0000")
            assert position.target_premium == Decimal("180.0000")  # entry * (1 - 0.10)
            assert position.stoploss_premium == Decimal("220.0000")  # entry * (1 + 0.10)
            assert position.strike == Decimal("25000.00")
        finally:
            container.stop()

    def test_entry_before_configured_time_does_nothing(self, tmp_path):
        clock = make_clock(hour=9, minute=0)  # before 09:20 entry_time
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite",
            instrument_catalog=catalog, price_source=prices,
        )
        container.start()
        try:
            runner = container.runners[0]
            # The scheduler itself would not fire yet; calling the strategy
            # hook directly still goes through entry_logic, which enters
            # unconditionally once dispatched -- what's actually under test
            # here is the *scheduler* not dispatching before the trigger
            # time, not entry_logic refusing a too-early entry.
            container.scheduler._tick()  # noqa: SLF001
            position = _current_position(container, runner, clock)
            assert position is None
        finally:
            container.stop()


class TestExitViaTarget:
    def test_price_drop_to_target_closes_the_position(self, tmp_path):
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
            runner = container.runners[0]
            _entry_and_get_position(container, runner, clock)

            prices.set_price(call, Decimal("80"))
            prices.set_price(put, Decimal("80"))  # combined 160 <= target 180
            clock.set_time(hour=11, minute=0)  # well before the 15:15 hard cutoff
            runner.dispatch_time_trigger("cutoff")  # forces poll_and_check()

            position = _current_position(container, runner, clock)
            assert position.state is PositionState.CLOSED
            assert position.exit_reason is ExitReason.TARGET
            assert position.combined_exit_premium == Decimal("160.0000")
            assert position.realized_pnl == Decimal("3000.0000")  # (200-160) * 75
        finally:
            container.stop()


class TestExitViaStopLoss:
    def test_price_rise_to_stoploss_closes_the_position(self, tmp_path):
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
            runner = container.runners[0]
            _entry_and_get_position(container, runner, clock)

            prices.set_price(call, Decimal("120"))
            prices.set_price(put, Decimal("120"))  # combined 240 >= stoploss 220
            clock.set_time(hour=11, minute=0)
            runner.dispatch_time_trigger("cutoff")

            position = _current_position(container, runner, clock)
            assert position.state is PositionState.CLOSED
            assert position.exit_reason is ExitReason.STOPLOSS
            assert position.combined_exit_premium == Decimal("240.0000")
            assert position.realized_pnl == Decimal("-3000.0000")  # (200-240) * 75
        finally:
            container.stop()


class TestExitViaHardCutoff:
    def test_cutoff_time_forces_exit_even_mid_range(self, tmp_path):
        """Price stays between target and stoploss (no P&L trigger), but the
        configured hard_cutoff_time has been reached -- exit_logic's priority
        order (time cutoff beats stoploss/target) must still force the exit
        through the real wired path, not just in exit_logic's own isolated
        unit tests.

        The cutoff instant is read from the strategy config the container
        actually loaded, not a literal, so this stays valid whatever
        hard_cutoff_time the YAML is set to."""
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
            runner = container.runners[0]
            _entry_and_get_position(container, runner, clock)

            prices.set_price(call, Decimal("100"))
            prices.set_price(put, Decimal("100"))  # combined 200 -- unchanged, no P&L trigger
            # Advance to exactly the configured hard cutoff (>= fires the
            # timeout), read from the loaded config rather than hardcoded.
            cutoff = runner._strategy.context.config.hard_cutoff_time  # noqa: SLF001
            clock.set_time(hour=cutoff.hour, minute=cutoff.minute, second=cutoff.second)
            runner.dispatch_time_trigger("cutoff")

            position = _current_position(container, runner, clock)
            assert position.state is PositionState.CLOSED
            assert position.exit_reason is ExitReason.TIMEOUT
            assert position.realized_pnl == Decimal("0.0000")
        finally:
            container.stop()


class TestRiskBlocksEntry:
    def test_daily_entry_limit_blocks_a_new_entry(self, tmp_path):
        """risk.yaml's committed max_daily_entries_per_account counts Position
        rows dated today across every strategy instance under one account
        (PositionRepository.count_for_account_on_date -- a StrategyInstance
        join, since Position itself carries no account_id). Seed exactly the
        configured limit of other instances (each with a Position dated today)
        under the same account, then verify RiskCore -- called for real through
        entry_logic -- blocks this instance's own (would-be over-limit) entry.
        Reads the limit from config so it stays correct as the committed value
        changes."""
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
            runner = container.runners[0]
            account_id = container._account_ids["primary"]  # noqa: SLF001
            limit = container.risk_config.max_daily_entries_per_account

            from algo.common.enums import Exchange
            from algo.database.models import Position

            with container.session_factory() as session:
                repo = StrategyInstanceRepository(session)
                for i in range(limit):  # fill the account's daily quota exactly
                    other_instance, _ = repo.get_or_create(
                        strategy_id="strategy_1", instrument=f"NIFTY_FAKE{i}",
                        account_id=account_id, exchange=Exchange.NFO,
                    )
                    session.flush()
                    session.add(
                        Position(
                            strategy_instance_id=other_instance.id,
                            trade_date=clock.today(),
                            state=PositionState.CLOSED,
                        )
                    )
                session.commit()

            position = _entry_and_get_position(container, runner, clock)
            assert position is None  # blocked before any Position row was created
        finally:
            container.stop()


class TestBrokerRejectsEntry:
    def test_rejected_order_leaves_no_open_position(self, tmp_path):
        clock = make_clock(hour=9, minute=0)
        catalog = build_nifty_option_chain()
        call, put = atm_legs(catalog)
        prices = StaticPriceSource({call: Decimal("100"), put: Decimal("100")})
        container = build_container(
            tmp_path, clock=clock, db_path=tmp_path / "db.sqlite",
            instrument_catalog=catalog, price_source=prices,
        )
        # Force every simulated order to be rejected by the broker.
        container.broker._inner._config = container.broker._inner._config.model_copy(  # noqa: SLF001
            update={"rejection_probability": 1.0}
        )
        container.start()
        try:
            runner = container.runners[0]
            position = _entry_and_get_position(container, runner, clock)
            assert position is None or position.state in (PositionState.ERROR, PositionState.IDLE)
        finally:
            container.stop()
