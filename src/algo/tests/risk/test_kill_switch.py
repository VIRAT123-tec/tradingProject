"""Tests for the KillSwitch producer and its operator CLI (H2).

The service is exercised against a real in-memory SQLite database (the flag is
a real row), and its interaction with the read side is confirmed by checking
that RiskCore's own kill-switch/emergency checks then fail. The CLI's command
logic is driven directly through ``_run`` with the real service.
"""

from __future__ import annotations

import argparse
import io
from datetime import date, datetime, timezone

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


from algo import killswitch as killswitch_cli
from algo.common.enums import Exchange, RiskFlagScope, RiskFlagType
from algo.database.models import Account, Base, StrategyInstance
from algo.risk.kill_switch import KillSwitch


class FakeTime:
    def now(self):
        return datetime(2026, 7, 9, 10, 0, tzinfo=timezone.utc)

    def now_ist(self):
        return datetime(2026, 7, 9, 10, 0, tzinfo=timezone.utc)

    def today(self) -> date:
        return date(2026, 7, 9)


def build_env():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    sf = sessionmaker(bind=engine, expire_on_commit=False)
    with sf() as s:
        account = Account(broker="SIMULATION", display_name="test")
        s.add(account)
        s.flush()
        instance = StrategyInstance(
            strategy_id="strategy_1", instrument="NIFTY", account_id=account.id, exchange=Exchange.NFO
        )
        s.add(instance)
        s.commit()
        account_id, instance_id = account.id, instance.id
    return sf, account_id, instance_id


class TestEngageDisengage:
    def test_engage_creates_active_global_flag(self):
        sf, _acct, _inst = build_env()
        ks = KillSwitch(session_factory=sf, time_provider=FakeTime())

        created = ks.engage(reason="test halt", activated_by="tester")

        assert created is True
        assert ks.is_engaged() is True
        assert ks.is_engaged(flag_type=RiskFlagType.KILL_SWITCH) is True

    def test_engage_is_idempotent(self):
        sf, _acct, _inst = build_env()
        ks = KillSwitch(session_factory=sf, time_provider=FakeTime())

        assert ks.engage(reason="r", activated_by="a") is True
        assert ks.engage(reason="r again", activated_by="a") is False  # no duplicate
        assert len(ks.list_active()) == 1

    def test_disengage_clears_the_flag(self):
        sf, _acct, _inst = build_env()
        ks = KillSwitch(session_factory=sf, time_provider=FakeTime())
        ks.engage(reason="r", activated_by="a")

        cleared = ks.disengage(cleared_by="a")

        assert cleared == 1
        assert ks.is_engaged() is False

    def test_disengage_when_none_active_is_zero(self):
        sf, _acct, _inst = build_env()
        ks = KillSwitch(session_factory=sf, time_provider=FakeTime())
        assert ks.disengage(cleared_by="a") == 0

    def test_account_scope_targets_only_that_account(self):
        sf, acct, _inst = build_env()
        ks = KillSwitch(session_factory=sf, time_provider=FakeTime())
        ks.engage(reason="r", activated_by="a", scope=RiskFlagScope.ACCOUNT, account_id=acct)
        assert ks.is_engaged(scope=RiskFlagScope.ACCOUNT) is True
        assert ks.is_engaged(scope=RiskFlagScope.GLOBAL) is False

    def test_persists_across_a_fresh_kill_switch(self):
        sf, _acct, _inst = build_env()
        KillSwitch(session_factory=sf, time_provider=FakeTime()).engage(reason="r", activated_by="a")
        # A brand-new KillSwitch (simulating a restart) sees the persisted flag.
        assert KillSwitch(session_factory=sf, time_provider=FakeTime()).is_engaged() is True


class TestScopeValidation:
    def test_global_with_account_id_rejected(self):
        sf, acct, _inst = build_env()
        ks = KillSwitch(session_factory=sf, time_provider=FakeTime())
        with pytest.raises(ValueError, match="GLOBAL"):
            ks.engage(reason="r", activated_by="a", scope=RiskFlagScope.GLOBAL, account_id=acct)

    def test_account_scope_without_account_id_rejected(self):
        sf, _acct, _inst = build_env()
        ks = KillSwitch(session_factory=sf, time_provider=FakeTime())
        with pytest.raises(ValueError, match="ACCOUNT"):
            ks.engage(reason="r", activated_by="a", scope=RiskFlagScope.ACCOUNT)


class TestRiskCoreSeesTheFlag:
    def test_engaged_kill_switch_makes_risk_core_block_entry(self):
        """Ties the producer to the reader: after engaging, RiskCore's own
        kill-switch check fails for an instance the flag covers."""
        from decimal import Decimal
        from time import time as _t  # noqa: F401  (unused; kept import minimal)

        from algo.risk.risk_core import RiskCheckStatus, RiskCore, RiskCoreConfig
        from algo.strategy_engine.strategy_context import StrategyIdentity

        sf, acct, inst = build_env()
        ks = KillSwitch(session_factory=sf, time_provider=FakeTime())
        identity = StrategyIdentity(
            instance_id=inst, strategy_id="strategy_1", instrument="NIFTY",
            account_id=acct, exchange=Exchange.NFO,
        )
        config = RiskCoreConfig(
            market_open_time=datetime(2026, 7, 9, 9, 15).time(),
            market_close_time=datetime(2026, 7, 9, 15, 30).time(),
            max_daily_entries_per_account=2, legs_per_entry=2,
            margin_per_lot_by_instrument={"NIFTY": Decimal("50000")},
            daily_loss_limit_by_account=Decimal("25000"),
        )
        # No broker needed for the kill-switch check specifically.
        risk = RiskCore(config=config, broker=None, session_factory=sf, time_provider=FakeTime())

        assert risk._check_kill_switch(identity).status is RiskCheckStatus.PASSED  # before
        ks.engage(reason="halt", activated_by="ops")
        assert risk._check_kill_switch(identity).status is RiskCheckStatus.FAILED  # after


class TestCli:
    def _parser(self):
        return killswitch_cli._build_parser()

    def test_status_when_clear(self):
        sf, _acct, _inst = build_env()
        ks = KillSwitch(session_factory=sf, time_provider=FakeTime())
        out = io.StringIO()
        args = self._parser().parse_args(["status"])
        rc = killswitch_cli._run(ks, args, out=out)
        assert rc == 0
        assert "NOT halted" in out.getvalue()

    def test_engage_then_status_then_disengage(self):
        sf, _acct, _inst = build_env()
        ks = KillSwitch(session_factory=sf, time_provider=FakeTime())

        out = io.StringIO()
        rc = killswitch_cli._run(ks, self._parser().parse_args(
            ["engage", "--reason", "manual", "--by", "ops"]), out=out)
        assert rc == 0 and "engaged" in out.getvalue().lower()

        out = io.StringIO()
        killswitch_cli._run(ks, self._parser().parse_args(["status"]), out=out)
        assert "IS halted" in out.getvalue()

        out = io.StringIO()
        killswitch_cli._run(ks, self._parser().parse_args(["disengage", "--by", "ops"]), out=out)
        assert "Cleared 1" in out.getvalue()
        assert ks.is_engaged() is False
