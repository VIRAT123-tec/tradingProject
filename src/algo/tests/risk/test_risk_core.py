"""Tests for RiskCore: the concrete pre-trade risk gate.

Uses a real in-memory SQLite database (with the JSONB/BigInteger compile shims
the DB layer's own tests use) and the real, already-verified SimulationBroker,
rather than mocking the persistence or broker layers -- so these tests exercise
the actual queries risk_core.py runs, not a stand-in for them.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError
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


from algo.brokers.exceptions import BrokerConnectionError
from algo.brokers.simulation import InstrumentCatalog, SimulationBroker, SimulationConfig, StaticPriceSource
from algo.common.enums import (
    Exchange,
    InstanceStatus,
    PositionState,
    RiskFlagScope,
    RiskFlagType,
    TradeLegStatus,
)
from algo.database.models import Account, Base, StrategyInstance
from algo.database.models.position import Position
from algo.database.models.risk_control_flag import RiskControlFlag
from algo.database.repositories.daily_risk_state_repository import DailyRiskStateRepository
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.risk_control_flag_repository import RiskControlFlagRepository
from algo.database.repositories.strategy_instance_repository import StrategyInstanceRepository
from algo.risk.risk_core import RiskCheckStatus, RiskCore, RiskCoreConfig
from algo.strategy_engine.parameter_loader import ParameterLoader
from algo.strategy_engine.strategy_context import StrategyIdentity

INSTRUMENT = "NIFTY"
TODAY = date(2026, 7, 7)
DURING_HOURS = time(11, 0)


@dataclass
class FakeTime:
    ist: datetime = datetime(2026, 7, 7, 11, 0, tzinfo=timezone.utc)  # wall-clock 11:00 (tzinfo is a placeholder -- only .time() is ever read)
    _today: date = TODAY

    def now(self) -> datetime:
        return datetime(2026, 7, 7, 5, 30, tzinfo=timezone.utc)

    def now_ist(self) -> datetime:
        return self.ist

    def today(self) -> date:
        return self._today


class RaisingBroker(SimulationBroker):
    """A broker double that raises on get_margins()/health_check() to test
    risk_core's own exception handling around broker calls."""

    fail_margins: bool = False
    fail_health: bool = False

    def get_margins(self, *, timeout=None):
        if self.fail_margins:
            raise BrokerConnectionError("simulated margin fetch failure")
        return super().get_margins(timeout=timeout)

    def health_check(self, *, timeout=None):
        if self.fail_health:
            raise BrokerConnectionError("simulated health check failure")
        return super().health_check(timeout=timeout)


def _config(**overrides) -> RiskCoreConfig:
    base = dict(
        market_open_time=time(9, 15),
        market_close_time=time(15, 30),
        max_daily_entries_per_account=2,
        legs_per_entry=2,
        margin_per_lot_by_instrument={"NIFTY": Decimal("50000"), "SENSEX": Decimal("70000")},
        daily_loss_limit_by_account=Decimal("25000"),
    )
    base.update(overrides)
    return RiskCoreConfig(**base)


@dataclass
class Harness:
    risk_core: RiskCore
    session_factory: sessionmaker
    identity: StrategyIdentity
    account_id: int
    broker: SimulationBroker


def build_harness(*, config: RiskCoreConfig | None = None, initial_cash: Decimal = Decimal("1000000")) -> Harness:
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
        s.commit()
        account_id = account.id
        instance_id = instance.id

    catalog = InstrumentCatalog()  # empty -- not needed for these checks
    broker = RaisingBroker(
        instrument_catalog=catalog,
        price_source=StaticPriceSource({}),
        config=SimulationConfig(synchronous=True, initial_cash=initial_cash),
        rng=random.Random(0),
    )
    broker.authenticate()

    identity = StrategyIdentity(
        instance_id=instance_id, strategy_id="strategy_1", instrument=INSTRUMENT,
        account_id=account_id, exchange=Exchange.NFO,
    )
    risk_core = RiskCore(
        config=config or _config(),
        broker=broker,
        session_factory=session_factory,
        time_provider=FakeTime(),
    )
    return Harness(risk_core=risk_core, session_factory=session_factory, identity=identity, account_id=account_id, broker=broker)


def _add_flag(
    session_factory: sessionmaker,
    *,
    flag_type: RiskFlagType,
    scope: RiskFlagScope,
    account_id: int | None = None,
    strategy_instance_id: int | None = None,
    active: bool = True,
) -> None:
    with session_factory() as s:
        RiskControlFlagRepository(s).activate(
            flag_type=flag_type, scope=scope, reason="test", activated_by="test",
            activated_at=datetime.now(timezone.utc), account_id=account_id,
            strategy_instance_id=strategy_instance_id,
        )
        s.commit()


# --------------------------------------------------------------------------
# RiskCoreConfig validation
# --------------------------------------------------------------------------


class TestRiskCoreConfig:
    def test_valid_config_constructs(self):
        config = _config()
        assert config.market_open_time == time(9, 15)
        assert config.daily_loss_limit_by_account == Decimal("25000")

    def test_market_open_after_close_raises(self):
        with pytest.raises(ValidationError, match="market_open_time"):
            _config(market_open_time=time(16, 0), market_close_time=time(9, 0))

    def test_market_open_equal_close_raises(self):
        with pytest.raises(ValidationError):
            _config(market_open_time=time(9, 15), market_close_time=time(9, 15))

    def test_negative_margin_per_lot_raises(self):
        with pytest.raises(ValidationError, match="margin_per_lot_by_instrument"):
            _config(margin_per_lot_by_instrument={"NIFTY": Decimal("-1")})

    def test_zero_margin_per_lot_raises(self):
        with pytest.raises(ValidationError):
            _config(margin_per_lot_by_instrument={"NIFTY": Decimal("0")})

    def test_daily_loss_limit_none_is_valid(self):
        config = _config(daily_loss_limit_by_account=None)
        assert config.daily_loss_limit_by_account is None

    def test_daily_loss_limit_zero_raises(self):
        with pytest.raises(ValidationError, match="daily_loss_limit_by_account"):
            _config(daily_loss_limit_by_account=Decimal("0"))

    def test_daily_loss_limit_negative_raises(self):
        with pytest.raises(ValidationError):
            _config(daily_loss_limit_by_account=Decimal("-100"))

    def test_missing_field_raises(self):
        with pytest.raises(ValidationError):
            RiskCoreConfig(market_open_time=time(9, 15))

    def test_real_risk_yaml_loads_and_validates(self):
        loader = ParameterLoader()
        config = loader.load(loader.config_root / "risk.yaml", RiskCoreConfig)
        assert isinstance(config, RiskCoreConfig)
        assert config.market_open_time < config.market_close_time


# --------------------------------------------------------------------------
# is_halted (the cheap, per-tick check)
# --------------------------------------------------------------------------


class TestIsHalted:
    def test_no_flags_is_not_halted(self):
        h = build_harness()
        assert h.risk_core.is_halted(h.identity) is False

    def test_active_global_kill_switch_halts(self):
        h = build_harness()
        _add_flag(h.session_factory, flag_type=RiskFlagType.KILL_SWITCH, scope=RiskFlagScope.GLOBAL)
        assert h.risk_core.is_halted(h.identity) is True

    def test_active_account_scoped_emergency_exit_halts(self):
        h = build_harness()
        _add_flag(
            h.session_factory, flag_type=RiskFlagType.EMERGENCY_EXIT, scope=RiskFlagScope.ACCOUNT,
            account_id=h.account_id,
        )
        assert h.risk_core.is_halted(h.identity) is True

    def test_active_instance_scoped_freeze_halts(self):
        h = build_harness()
        _add_flag(
            h.session_factory, flag_type=RiskFlagType.FREEZE, scope=RiskFlagScope.STRATEGY_INSTANCE,
            strategy_instance_id=h.identity.instance_id,
        )
        assert h.risk_core.is_halted(h.identity) is True

    def test_inactive_flag_does_not_halt(self):
        h = build_harness()
        with h.session_factory() as s:
            flag = RiskControlFlagRepository(s).activate(
                flag_type=RiskFlagType.KILL_SWITCH, scope=RiskFlagScope.GLOBAL, reason=None,
                activated_by="test", activated_at=datetime.now(timezone.utc),
            )
            s.flush()
            RiskControlFlagRepository(s).clear(flag, cleared_by="test", cleared_at=datetime.now(timezone.utc))
            s.commit()
        assert h.risk_core.is_halted(h.identity) is False

    def test_flag_scoped_to_different_account_does_not_halt(self):
        h = build_harness()
        _add_flag(
            h.session_factory, flag_type=RiskFlagType.KILL_SWITCH, scope=RiskFlagScope.ACCOUNT,
            account_id=h.account_id + 999,
        )
        assert h.risk_core.is_halted(h.identity) is False


# --------------------------------------------------------------------------
# Individual checks
# --------------------------------------------------------------------------


class TestTradingHoursCheck:
    def test_within_hours_passes(self):
        h = build_harness()
        result = h.risk_core._check_trading_hours()
        assert result.status is RiskCheckStatus.PASSED

    def test_before_open_fails(self):
        h = build_harness()
        h.risk_core._time.ist = datetime(2026, 7, 7, 8, 30, tzinfo=timezone.utc)  # wall-clock 08:30
        result = h.risk_core._check_trading_hours()
        assert result.status is RiskCheckStatus.FAILED

    def test_after_close_fails(self):
        h = build_harness()
        h.risk_core._time.ist = datetime(2026, 7, 7, 16, 0, tzinfo=timezone.utc)  # wall-clock 16:00
        result = h.risk_core._check_trading_hours()
        assert result.status is RiskCheckStatus.FAILED

    def test_exactly_at_open_passes(self):
        h = build_harness()
        h.risk_core._time.ist = datetime(2026, 7, 7, 9, 15, tzinfo=timezone.utc)  # wall-clock 09:15
        result = h.risk_core._check_trading_hours()
        assert result.status is RiskCheckStatus.PASSED


class TestStrategyStateCheck:
    def test_active_instance_passes(self):
        h = build_harness()
        result = h.risk_core._check_strategy_state(h.identity)
        assert result.status is RiskCheckStatus.PASSED

    def test_frozen_instance_fails(self):
        h = build_harness()
        with h.session_factory() as s:
            repo = StrategyInstanceRepository(s)
            instance = repo.get_by_id_or_raise(h.identity.instance_id)
            repo.set_status(instance, InstanceStatus.FROZEN)
            s.commit()
        result = h.risk_core._check_strategy_state(h.identity)
        assert result.status is RiskCheckStatus.FAILED

    def test_disabled_instance_fails(self):
        h = build_harness()
        with h.session_factory() as s:
            repo = StrategyInstanceRepository(s)
            instance = repo.get_by_id_or_raise(h.identity.instance_id)
            repo.set_status(instance, InstanceStatus.DISABLED)
            s.commit()
        result = h.risk_core._check_strategy_state(h.identity)
        assert result.status is RiskCheckStatus.FAILED

    def test_active_instance_with_freeze_flag_fails(self):
        h = build_harness()
        _add_flag(
            h.session_factory, flag_type=RiskFlagType.FREEZE, scope=RiskFlagScope.STRATEGY_INSTANCE,
            strategy_instance_id=h.identity.instance_id,
        )
        result = h.risk_core._check_strategy_state(h.identity)
        assert result.status is RiskCheckStatus.FAILED

    def test_missing_instance_fails(self):
        h = build_harness()
        bad_identity = StrategyIdentity(
            instance_id=999999, strategy_id="strategy_1", instrument=INSTRUMENT,
            account_id=h.account_id, exchange=Exchange.NFO,
        )
        result = h.risk_core._check_strategy_state(bad_identity)
        assert result.status is RiskCheckStatus.FAILED


class TestKillSwitchAndEmergencyStopChecks:
    def test_kill_switch_flag_fails_kill_switch_check_only(self):
        h = build_harness()
        _add_flag(h.session_factory, flag_type=RiskFlagType.KILL_SWITCH, scope=RiskFlagScope.GLOBAL)
        assert h.risk_core._check_kill_switch(h.identity).status is RiskCheckStatus.FAILED
        assert h.risk_core._check_emergency_stop(h.identity).status is RiskCheckStatus.PASSED

    def test_emergency_exit_flag_fails_emergency_check_only(self):
        h = build_harness()
        _add_flag(h.session_factory, flag_type=RiskFlagType.EMERGENCY_EXIT, scope=RiskFlagScope.GLOBAL)
        assert h.risk_core._check_emergency_stop(h.identity).status is RiskCheckStatus.FAILED
        assert h.risk_core._check_kill_switch(h.identity).status is RiskCheckStatus.PASSED


class TestDailyLossLimitCheck:
    def test_disabled_when_limit_is_none(self):
        h = build_harness(config=_config(daily_loss_limit_by_account=None))
        result = h.risk_core._check_daily_loss_limit(h.identity)
        assert result.status is RiskCheckStatus.PASSED
        assert "no daily loss limit" in result.detail

    def test_no_prior_row_creates_one_and_passes(self):
        h = build_harness()
        result = h.risk_core._check_daily_loss_limit(h.identity)
        assert result.status is RiskCheckStatus.PASSED
        with h.session_factory() as s:
            row = DailyRiskStateRepository(s).get_portfolio_row(h.account_id, TODAY)
            assert row is not None
            assert row.loss_limit == Decimal("25000")

    def test_pnl_breaching_limit_fails(self):
        # Realized P&L is now recomputed from the source-of-truth Position rows,
        # so a breach is expressed by a CLOSED position carrying the loss, not
        # by writing the DailyRiskState row directly.
        h = build_harness()
        with h.session_factory() as s:
            s.add(
                Position(
                    strategy_instance_id=h.identity.instance_id,
                    trade_date=TODAY,
                    state=PositionState.CLOSED,
                    realized_pnl=Decimal("-30000"),
                )
            )
            s.commit()
        result = h.risk_core._check_daily_loss_limit(h.identity)
        assert result.status is RiskCheckStatus.FAILED
        # And the breach is latched onto the persisted row.
        with h.session_factory() as s:
            row = DailyRiskStateRepository(s).get_portfolio_row(h.account_id, TODAY)
            assert row.breached is True
            assert row.realized_pnl == Decimal("-30000")

    def test_realized_pnl_within_limit_passes(self):
        h = build_harness()
        with h.session_factory() as s:
            s.add(
                Position(
                    strategy_instance_id=h.identity.instance_id,
                    trade_date=TODAY,
                    state=PositionState.CLOSED,
                    realized_pnl=Decimal("-10000"),  # loss, but within the -25000 limit
                )
            )
            s.commit()
        result = h.risk_core._check_daily_loss_limit(h.identity)
        assert result.status is RiskCheckStatus.PASSED

    def test_realized_pnl_sums_across_instances_under_the_account(self):
        # Two instruments under the same account, each closing at a loss that is
        # individually within the limit but together breaches it.
        h = build_harness()
        with h.session_factory() as s:
            second = StrategyInstance(
                strategy_id="strategy_1", instrument="SENSEX", account_id=h.account_id, exchange=Exchange.BFO
            )
            s.add(second)
            s.flush()
            s.add(Position(strategy_instance_id=h.identity.instance_id, trade_date=TODAY,
                           state=PositionState.CLOSED, realized_pnl=Decimal("-15000")))
            s.add(Position(strategy_instance_id=second.id, trade_date=TODAY,
                           state=PositionState.CLOSED, realized_pnl=Decimal("-15000")))
            s.commit()
        result = h.risk_core._check_daily_loss_limit(h.identity)
        assert result.status is RiskCheckStatus.FAILED  # -30000 total breaches -25000

    def test_breach_blocks_full_entry_validation(self):
        """The whole point of H1: a breached daily loss limit prevents a new
        entry through the full validate_entry sequence, not just the isolated
        check."""
        h = build_harness()
        with h.session_factory() as s:
            s.add(Position(strategy_instance_id=h.identity.instance_id, trade_date=TODAY,
                           state=PositionState.CLOSED, realized_pnl=Decimal("-30000")))
            s.commit()
        result = h.risk_core.validate_entry(h.identity, quantity=1)
        assert result.approved is False
        assert result.failed_check.name == "daily_loss_limit"

    def test_breach_latch_persists_across_restart(self):
        """Once breached, the block survives a process restart even if the
        underlying positions were somehow cleared -- the latch is persisted."""
        h = build_harness()
        with h.session_factory() as s:
            s.add(Position(strategy_instance_id=h.identity.instance_id, trade_date=TODAY,
                           state=PositionState.CLOSED, realized_pnl=Decimal("-30000")))
            s.commit()
        # First check latches the breach.
        assert h.risk_core._check_daily_loss_limit(h.identity).status is RiskCheckStatus.FAILED

        # Simulate a restart: a brand-new RiskCore against the same database,
        # and remove the position so a recompute alone would read 0.
        with h.session_factory() as s:
            s.query(Position).delete()
            s.commit()
        fresh = RiskCore(
            config=_config(), broker=h.broker, session_factory=h.session_factory, time_provider=FakeTime()
        )
        assert fresh._check_daily_loss_limit(h.identity).status is RiskCheckStatus.FAILED

    def test_persist_conflict_does_not_change_the_decision(self, monkeypatch):
        """M1b: with coincident same-account entries now dispatched
        concurrently, a lost optimistic-lock race on the shared daily-risk row
        must NOT fail the check -- the decision is computed read-only, and the
        persist is best-effort. Simulate the race by making the persist write
        always raise ConcurrentModificationError."""
        from algo.database.repositories.daily_risk_state_repository import (
            DailyRiskStateRepository as _Repo,
        )
        from algo.database.repositories.exceptions import ConcurrentModificationError

        def _boom(self, row, **kwargs):  # noqa: ANN001, ANN003
            raise ConcurrentModificationError("simulated concurrent update")

        monkeypatch.setattr(_Repo, "update_pnl", _boom)

        # Breaching position: the read-only decision still blocks entry despite
        # the persist conflict.
        h = build_harness()
        with h.session_factory() as s:
            s.add(Position(strategy_instance_id=h.identity.instance_id, trade_date=TODAY,
                           state=PositionState.CLOSED, realized_pnl=Decimal("-30000")))
            s.commit()
        result = h.risk_core._check_daily_loss_limit(h.identity)  # must not raise
        assert result.status is RiskCheckStatus.FAILED

        # Within-limit position: still PASSED despite the persist conflict.
        h2 = build_harness()
        with h2.session_factory() as s:
            s.add(Position(strategy_instance_id=h2.identity.instance_id, trade_date=TODAY,
                           state=PositionState.CLOSED, realized_pnl=Decimal("-1000")))
            s.commit()
        assert h2.risk_core._check_daily_loss_limit(h2.identity).status is RiskCheckStatus.PASSED

    def test_explicitly_breached_flag_fails_even_if_pnl_currently_fine(self):
        h = build_harness()
        with h.session_factory() as s:
            repo = DailyRiskStateRepository(s)
            row, _ = repo.get_or_create_portfolio_row(account_id=h.account_id, trade_date=TODAY, loss_limit=Decimal("25000"))
            repo.mark_breached(row, breached_at=datetime.now(timezone.utc))
            s.commit()
        result = h.risk_core._check_daily_loss_limit(h.identity)
        assert result.status is RiskCheckStatus.FAILED

    def test_snapshot_is_not_retroactively_changed_by_later_config(self):
        h = build_harness(config=_config(daily_loss_limit_by_account=Decimal("25000")))
        h.risk_core._check_daily_loss_limit(h.identity)  # creates row with limit=25000

        # A later RiskCore with a different configured limit must not alter
        # the already-snapshotted row.
        h2 = Harness(
            risk_core=RiskCore(
                config=_config(daily_loss_limit_by_account=Decimal("5000")),
                broker=h.broker, session_factory=h.session_factory, time_provider=FakeTime(),
            ),
            session_factory=h.session_factory, identity=h.identity, account_id=h.account_id, broker=h.broker,
        )
        h2.risk_core._check_daily_loss_limit(h.identity)
        with h.session_factory() as s:
            row = DailyRiskStateRepository(s).get_portfolio_row(h.account_id, TODAY)
            assert row.loss_limit == Decimal("25000")  # unchanged


class TestDuplicatePositionCheck:
    def test_no_position_passes(self):
        h = build_harness()
        result = h.risk_core._check_duplicate_position(h.identity)
        assert result.status is RiskCheckStatus.PASSED

    def test_existing_position_fails(self):
        h = build_harness()
        with h.session_factory() as s:
            s.add(Position(strategy_instance_id=h.identity.instance_id, trade_date=TODAY, state=PositionState.OPEN))
            s.commit()
        result = h.risk_core._check_duplicate_position(h.identity)
        assert result.status is RiskCheckStatus.FAILED


class TestMaxDailyEntriesCheck:
    def test_under_limit_passes(self):
        h = build_harness(config=_config(max_daily_entries_per_account=2))
        result = h.risk_core._check_max_daily_entries(h.identity)
        assert result.status is RiskCheckStatus.PASSED

    def test_at_limit_fails(self):
        h = build_harness(config=_config(max_daily_entries_per_account=1))
        with h.session_factory() as s:
            s.add(Position(strategy_instance_id=h.identity.instance_id, trade_date=TODAY, state=PositionState.OPEN))
            s.commit()
        result = h.risk_core._check_max_daily_entries(h.identity)
        assert result.status is RiskCheckStatus.FAILED

    def test_counts_across_multiple_instances_under_same_account(self):
        h = build_harness(config=_config(max_daily_entries_per_account=2))
        with h.session_factory() as s:
            second_instance = StrategyInstance(
                strategy_id="strategy_1", instrument="SENSEX", account_id=h.account_id, exchange=Exchange.BFO
            )
            s.add(second_instance)
            s.flush()
            s.add(Position(strategy_instance_id=h.identity.instance_id, trade_date=TODAY, state=PositionState.OPEN))
            s.add(Position(strategy_instance_id=second_instance.id, trade_date=TODAY, state=PositionState.OPEN))
            s.commit()
        result = h.risk_core._check_max_daily_entries(h.identity)
        assert result.status is RiskCheckStatus.FAILED  # 2 positions, limit 2 -> at limit


class TestBrokerConnectivityCheck:
    def test_healthy_broker_passes(self):
        h = build_harness()
        result = h.risk_core._check_broker_connectivity()
        assert result.status is RiskCheckStatus.PASSED

    def test_unauthenticated_broker_fails(self):
        h = build_harness()
        # A fresh, never-authenticated broker reports unhealthy via health_check().
        fresh_broker = RaisingBroker(
            instrument_catalog=InstrumentCatalog(), price_source=StaticPriceSource({}),
            config=SimulationConfig(synchronous=True), rng=random.Random(0),
        )
        h.risk_core._broker = fresh_broker
        result = h.risk_core._check_broker_connectivity()
        assert result.status is RiskCheckStatus.FAILED

    def test_broker_raising_fails_gracefully(self):
        h = build_harness()
        h.broker.fail_health = True
        result = h.risk_core._check_broker_connectivity()
        assert result.status is RiskCheckStatus.FAILED
        assert "raised" in result.detail


class TestAvailableMarginCheck:
    def test_sufficient_margin_passes(self):
        h = build_harness(initial_cash=Decimal("1000000"))
        result = h.risk_core._check_available_margin(h.identity, quantity=1)
        # required = 50000 (per lot) * 1 (qty) * 2 (legs) = 100000 <= 1,000,000
        assert result.status is RiskCheckStatus.PASSED

    def test_insufficient_margin_fails(self):
        h = build_harness(initial_cash=Decimal("50000"))
        result = h.risk_core._check_available_margin(h.identity, quantity=5)
        # required = 50000 * 5 * 2 = 500000 > 50000 available
        assert result.status is RiskCheckStatus.FAILED

    def test_unknown_instrument_fails(self):
        h = build_harness()
        bad_identity = StrategyIdentity(
            instance_id=h.identity.instance_id, strategy_id="strategy_1", instrument="BANKNIFTY",
            account_id=h.account_id, exchange=Exchange.NFO,
        )
        result = h.risk_core._check_available_margin(bad_identity, quantity=1)
        assert result.status is RiskCheckStatus.FAILED
        assert "no margin_per_lot_by_instrument" in result.detail

    def test_broker_raising_fails_gracefully(self):
        h = build_harness()
        h.broker.fail_margins = True
        result = h.risk_core._check_available_margin(h.identity, quantity=1)
        assert result.status is RiskCheckStatus.FAILED
        assert "raised" in result.detail


# --------------------------------------------------------------------------
# Full validate_entry: ordering and fail-fast
# --------------------------------------------------------------------------


class TestValidateEntryFailFast:
    def test_all_pass_approves_with_full_check_list(self):
        h = build_harness()
        result = h.risk_core.validate_entry(h.identity, quantity=1)
        assert result.approved is True
        assert [c.status for c in result.checks] == [RiskCheckStatus.PASSED] * 9
        assert [c.name for c in result.checks] == [
            "trading_hours", "strategy_state", "kill_switch", "emergency_stop",
            "daily_loss_limit", "duplicate_position", "max_daily_entries",
            "broker_connectivity", "available_margin",
        ]

    def test_first_check_failure_skips_all_the_rest(self):
        h = build_harness()
        h.risk_core._time.ist = datetime(2026, 7, 7, 16, 0, tzinfo=timezone.utc)  # wall-clock 16:00  # outside hours
        result = h.risk_core.validate_entry(h.identity, quantity=1)

        assert result.approved is False
        assert result.checks[0].name == "trading_hours"
        assert result.checks[0].status is RiskCheckStatus.FAILED
        assert all(c.status is RiskCheckStatus.SKIPPED for c in result.checks[1:])

    def test_middle_check_failure_leaves_earlier_passed_and_later_skipped(self):
        h = build_harness()
        with h.session_factory() as s:
            s.add(Position(strategy_instance_id=h.identity.instance_id, trade_date=TODAY, state=PositionState.OPEN))
            s.commit()

        result = h.risk_core.validate_entry(h.identity, quantity=1)

        by_name = {c.name: c for c in result.checks}
        assert result.approved is False
        assert by_name["trading_hours"].status is RiskCheckStatus.PASSED
        assert by_name["strategy_state"].status is RiskCheckStatus.PASSED
        assert by_name["duplicate_position"].status is RiskCheckStatus.FAILED
        assert by_name["max_daily_entries"].status is RiskCheckStatus.SKIPPED
        assert by_name["broker_connectivity"].status is RiskCheckStatus.SKIPPED
        assert by_name["available_margin"].status is RiskCheckStatus.SKIPPED

    def test_failed_check_property(self):
        h = build_harness()
        h.risk_core._time.ist = datetime(2026, 7, 7, 16, 0, tzinfo=timezone.utc)  # wall-clock 16:00
        result = h.risk_core.validate_entry(h.identity, quantity=1)
        assert result.failed_check is not None
        assert result.failed_check.name == "trading_hours"

    def test_failed_check_is_none_when_approved(self):
        h = build_harness()
        result = h.risk_core.validate_entry(h.identity, quantity=1)
        assert result.failed_check is None


class TestApproveEntry:
    def test_approved_decision(self):
        h = build_harness()
        decision = h.risk_core.approve_entry(h.identity, quantity=1)
        assert decision.approved is True
        assert decision.reason is None

    def test_rejected_decision_carries_reason(self):
        h = build_harness()
        h.risk_core._time.ist = datetime(2026, 7, 7, 16, 0, tzinfo=timezone.utc)  # wall-clock 16:00
        decision = h.risk_core.approve_entry(h.identity, quantity=1)
        assert decision.approved is False
        assert decision.reason is not None
        assert "trading_hours" in decision.reason
