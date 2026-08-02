"""Tests for the startup migration guard (algo.database.migration_guard).

Cases A-C run on in-memory SQLite -- the guard only compares the ``alembic_version``
table against the migration scripts' declared head, which is backend-agnostic, so
these are fast and need no Postgres. The revision ids are derived from the real
migration chain (never hardcoded), so adding a migration cannot silently rot them.

Case D (requirement 8) exercises the REAL Alembic migration chain against a clean
PostgreSQL database and then round-trips an ORM model -- the one test that would
have caught the ``pnl_per_share`` drift. It is opt-in via ``MIGRATION_TEST_DATABASE_URL``
(a disposable Postgres) so the default suite stays fast and Postgres-free.

Why this matters (the CI-drift note, requirement 8): the rest of the suite builds
its schema with ``Base.metadata.create_all()``, which generates tables directly
from the ORM and BYPASSES Alembic entirely. So a model/migration divergence -- a
column added to the model without a migration, OR a migration never applied -- is
invisible to those tests. That is exactly how ``pnl_per_share`` reached production.
The guard closes the runtime hole; Case D closes the test hole.
"""

from __future__ import annotations

import os

import pytest
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text

from algo.database.migration_guard import (
    SchemaOutOfDateError,
    alembic_head_revisions,
    build_alembic_config,
    verify_database_is_current,
)


def _head_and_previous() -> tuple[str, str]:
    """The current head revision and its immediate predecessor, read from the
    real migration chain (so this never hardcodes revision ids)."""
    script = ScriptDirectory.from_config(build_alembic_config())
    head = script.get_current_head()
    previous = script.get_revision(head).down_revision
    assert isinstance(head, str) and isinstance(previous, str)
    return head, previous


def _stamp(engine, revision: str | None) -> None:
    """Simulate a database stamped at ``revision`` (or, if None, a database that
    was never migrated -- no alembic_version table at all)."""
    if revision is None:
        return
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)"))
        conn.execute(
            text("INSERT INTO alembic_version (version_num) VALUES (:r)"), {"r": revision}
        )


class TestVerifyDatabaseIsCurrent:
    def test_case_a_at_head_passes(self):
        head, _ = _head_and_previous()
        engine = create_engine("sqlite://")
        _stamp(engine, head)
        # Must not raise.
        verify_database_is_current(engine)

    def test_case_b_one_migration_behind_fails_with_clear_message(self):
        head, previous = _head_and_previous()
        engine = create_engine("sqlite://")
        _stamp(engine, previous)

        with pytest.raises(SchemaOutOfDateError) as exc:
            verify_database_is_current(engine)

        msg = str(exc.value)
        assert previous in msg          # current revision reported
        assert head in msg              # expected revision reported
        assert "alembic upgrade head" in msg  # remediation command
        assert "not up to date" in msg

    def test_case_c_database_never_migrated_fails(self):
        engine = create_engine("sqlite://")  # no alembic_version table
        _stamp(engine, None)

        with pytest.raises(SchemaOutOfDateError) as exc:
            verify_database_is_current(engine)

        msg = str(exc.value)
        assert "never been migrated" in msg
        assert "alembic upgrade head" in msg

    def test_head_helper_matches_chain(self):
        head, _ = _head_and_previous()
        assert alembic_head_revisions() == {head}


POSTGRES_URL = os.environ.get("MIGRATION_TEST_DATABASE_URL")


@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set MIGRATION_TEST_DATABASE_URL to a clean, disposable Postgres to run "
    "the real Alembic upgrade round-trip (Case D)",
)
class TestPostgresMigrationRoundtrip:
    """Case D + requirement 8: clean Postgres -> alembic upgrade head -> ORM works.

    Opt-in only, so the default (SQLite) suite is unaffected.
    """

    def test_fresh_db_upgrade_head_then_orm_and_guard_agree(self, monkeypatch):
        from decimal import Decimal

        from alembic import command

        from algo.database.models.base import Base
        from algo.database.models.trade_history import TradeHistory

        monkeypatch.setenv("DATABASE_URL", POSTGRES_URL)
        engine = create_engine(POSTGRES_URL)

        # Start clean: drop anything left over so this is a true fresh database.
        Base.metadata.drop_all(engine)
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS alembic_version"))

        # Before migrating, the guard must refuse (Case C on real Postgres).
        with pytest.raises(SchemaOutOfDateError):
            verify_database_is_current(engine)

        # Run the REAL migration chain.
        command.upgrade(build_alembic_config(), "head")

        # Now the guard passes...
        verify_database_is_current(engine)

        # ...and the ORM (including pnl_per_share) round-trips against the
        # migration-built schema -- the exact check the create_all suite skips.
        cols = {c["name"] for c in __import__("sqlalchemy").inspect(engine).get_columns("trade_history")}
        assert "pnl_per_share" in cols
        assert set(TradeHistory.__table__.columns.keys()) <= cols
