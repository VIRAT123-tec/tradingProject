"""Tests for the instance_admin operator CLI (clearing a stale FROZEN
StrategyInstance status).

Exercised against a real in-memory SQLite database (the status is a real
column on a real row), driving the CLI's command logic directly through
``_run`` with an injected session factory -- the same pattern
``test_kill_switch.py`` uses for its own operator CLI.
"""

from __future__ import annotations

import io

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


from algo import instance_admin
from algo.common.enums import Exchange, InstanceStatus
from algo.database.models import Account, Base, StrategyInstance


def build_env():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    sf = sessionmaker(bind=engine, expire_on_commit=False)
    with sf() as s:
        account = Account(broker="SIMULATION", display_name="test")
        s.add(account)
        s.flush()
        nifty = StrategyInstance(
            strategy_id="strategy_1", instrument="NIFTY", account_id=account.id,
            exchange=Exchange.NFO, status=InstanceStatus.FROZEN,
        )
        sensex = StrategyInstance(
            strategy_id="strategy_1", instrument="SENSEX", account_id=account.id,
            exchange=Exchange.BFO, status=InstanceStatus.ACTIVE,
        )
        s.add_all([nifty, sensex])
        s.commit()
        nifty_id, sensex_id = nifty.id, sensex.id
    return sf, nifty_id, sensex_id


class TestList:
    def test_lists_every_active_and_frozen_instance(self):
        sf, nifty_id, sensex_id = build_env()
        out = io.StringIO()
        rc = instance_admin._run(sf, instance_admin._build_parser().parse_args(["list"]), out=out)
        assert rc == 0
        text = out.getvalue()
        assert f"id={nifty_id} strategy_1/NIFTY" in text
        assert "status=FROZEN" in text
        assert f"id={sensex_id} strategy_1/SENSEX" in text
        assert "status=ACTIVE" in text

    def test_empty_database_reports_nothing_found(self):
        engine = create_engine("sqlite://")
        Base.metadata.create_all(engine)
        sf = sessionmaker(bind=engine, expire_on_commit=False)
        out = io.StringIO()
        rc = instance_admin._run(sf, instance_admin._build_parser().parse_args(["list"]), out=out)
        assert rc == 0
        assert "No ACTIVE or FROZEN" in out.getvalue()


class TestUnfreeze:
    def test_unfreeze_clears_a_frozen_instance(self):
        sf, nifty_id, _sensex_id = build_env()
        out = io.StringIO()
        args = instance_admin._build_parser().parse_args(
            ["unfreeze", "--instance-id", str(nifty_id), "--reason", "confirmed stale", "--by", "ops"]
        )
        rc = instance_admin._run(sf, args, out=out)
        assert rc == 0
        assert "FROZEN -> ACTIVE" in out.getvalue()
        assert "confirmed stale" in out.getvalue()

        with sf() as s:
            refreshed = s.get(StrategyInstance, nifty_id)
            assert refreshed.status is InstanceStatus.ACTIVE

    def test_unfreeze_on_an_already_active_instance_is_a_no_op(self):
        sf, _nifty_id, sensex_id = build_env()
        out = io.StringIO()
        args = instance_admin._build_parser().parse_args(
            ["unfreeze", "--instance-id", str(sensex_id), "--reason", "n/a", "--by", "ops"]
        )
        rc = instance_admin._run(sf, args, out=out)
        assert rc == 0
        assert "not FROZEN -- no change" in out.getvalue()

        with sf() as s:
            refreshed = s.get(StrategyInstance, sensex_id)
            assert refreshed.status is InstanceStatus.ACTIVE

    def test_unfreeze_unknown_instance_id_errors(self):
        sf, _nifty_id, _sensex_id = build_env()
        out = io.StringIO()
        args = instance_admin._build_parser().parse_args(
            ["unfreeze", "--instance-id", "999999", "--reason", "n/a", "--by", "ops"]
        )
        rc = instance_admin._run(sf, args, out=out)
        assert rc == 1
        assert "no StrategyInstance with id=999999" in out.getvalue()
