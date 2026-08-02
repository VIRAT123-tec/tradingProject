"""Startup migration guard: refuse to start if the database schema is behind
the code's Alembic head.

This exists to permanently kill the ORM<->database drift class of bug -- e.g.
the ORM expecting ``trade_history.pnl_per_share`` while the physical table
predates that migration. Instead of starting and blowing up later *inside a
completed trade*, the platform fails fast at boot with a clear operator message.

Two modes (requirement 4):
* **strict** (default) -- verify only; raise ``SchemaOutOfDateError`` if behind.
* **auto-migrate** (opt-in via ``DB_AUTO_MIGRATE=true``) -- run
  ``alembic upgrade head`` first, then re-verify. Deliberately OFF by default:
  applying migrations is an operator decision, never an implicit side effect of
  starting a trading process.

Scope: this guards the Alembic-managed TRADING database (``DATABASE_URL``). The
market-data collector uses a separate database whose schema is (idempotently)
created on every start, so it cannot drift and is intentionally not guarded here.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory

if TYPE_CHECKING:
    from sqlalchemy import Engine

_logger = logging.getLogger("algo.migration_guard")

#: Env var that opts into auto-migrate mode (Mode B). Anything falsey/unset =
#: strict mode (Mode A, the safe default).
AUTO_MIGRATE_ENV = "DB_AUTO_MIGRATE"

_TRUE = {"1", "true", "yes", "on"}


class SchemaOutOfDateError(RuntimeError):
    """The database schema revision does not match the code's Alembic head.

    Raised by the startup guard so the process aborts (non-zero exit) instead of
    starting a scheduler/collector/broker against a stale schema.
    """


def build_alembic_config(alembic_ini: str | Path | None = None) -> Config:
    """Load the project's ``alembic.ini``. Resolved from this file's location
    (repo root) so it works regardless of the process's working directory."""
    if alembic_ini is not None:
        ini = Path(alembic_ini)
    else:
        # src/algo/database/migration_guard.py -> parents[3] == repo root.
        ini = Path(__file__).resolve().parents[3] / "alembic.ini"
    if not ini.exists():
        raise SchemaOutOfDateError(f"alembic.ini not found at {ini}; cannot verify schema")
    return Config(str(ini))


def alembic_head_revisions(alembic_ini: str | Path | None = None) -> set[str]:
    """The revision id(s) the migration scripts declare as head."""
    return set(ScriptDirectory.from_config(build_alembic_config(alembic_ini)).get_heads())


def database_current_revisions(engine: Engine) -> set[str]:
    """The revision(s) the database is stamped at, read from its
    ``alembic_version`` table. Empty if the database has never been migrated
    (no such table) -- which the guard treats as 'behind head'."""
    with engine.connect() as conn:
        return set(MigrationContext.configure(conn).get_current_heads())


def _fmt(revs: set[str]) -> str:
    return ", ".join(sorted(revs)) if revs else "<none> (database has never been migrated)"


def verify_database_is_current(
    engine: Engine,
    *,
    auto_migrate: bool = False,
    alembic_ini: str | Path | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Ensure ``engine``'s database is at the code's Alembic head.

    Strict mode (``auto_migrate=False``): raise ``SchemaOutOfDateError`` -- after
    a CRITICAL log naming the current and expected revisions and the remediation
    command -- if the database is behind. Read-only and idempotent in this mode.

    Auto-migrate mode: run ``alembic upgrade head`` first, then re-verify.

    Raises ``SchemaOutOfDateError`` if the schema is (or stays) behind. A DB
    connection error propagates unchanged so the caller aborts startup too.
    """
    log = logger if logger is not None else _logger
    cfg = build_alembic_config(alembic_ini)
    heads = set(ScriptDirectory.from_config(cfg).get_heads())
    current = database_current_revisions(engine)

    if current == heads:
        log.info("database schema is up to date (revision %s)", _fmt(heads))
        return

    if auto_migrate:
        log.warning(
            "database schema is behind (current=%s, head=%s); %s is set -- running "
            "'alembic upgrade head'",
            _fmt(current), _fmt(heads), AUTO_MIGRATE_ENV,
        )
        from alembic import command

        command.upgrade(cfg, "head")
        current = database_current_revisions(engine)
        if current != heads:
            raise SchemaOutOfDateError(
                f"auto-migration did not reach head (current={_fmt(current)}, "
                f"head={_fmt(heads)})"
            )
        log.info("auto-migration complete; database schema now at %s", _fmt(heads))
        return

    message = (
        "Database schema is not up to date.\n"
        f"  Current revision : {_fmt(current)}\n"
        f"  Expected revision: {_fmt(heads)}\n"
        "  Run: alembic upgrade head\n"
        "Platform startup aborted -- no scheduler, no collector, no trading."
    )
    log.critical(message)
    raise SchemaOutOfDateError(message)


def guard_database_schema(
    *, auto_migrate: bool | None = None, logger: logging.Logger | None = None
) -> None:
    """Entrypoint-facing guard. Build a short-lived engine from ``DATABASE_URL``,
    verify it is at Alembic head, then dispose it.

    Call this at the very top of a trading process's ``main()`` -- BEFORE the
    dependency container, broker login, scheduler, websocket, or any strategy
    code. Raises ``SchemaOutOfDateError`` (fail-fast) if the schema is behind.

    ``auto_migrate`` defaults to the ``DB_AUTO_MIGRATE`` env var (off unless
    explicitly set), so strict verification is the out-of-the-box behaviour.
    """
    from algo.database.database import build_engine, load_database_settings

    if auto_migrate is None:
        auto_migrate = os.environ.get(AUTO_MIGRATE_ENV, "").strip().lower() in _TRUE

    engine = build_engine(load_database_settings())
    try:
        verify_database_is_current(engine, auto_migrate=auto_migrate, logger=logger)
    finally:
        engine.dispose()
