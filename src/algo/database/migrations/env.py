"""Alembic environment script.

Scope decision: this reads DATABASE_URL directly from the environment (via
.env, loaded with python-dotenv) rather than going through
database/database.py's engine factory, because that module is still an
unimplemented stub (a later delivery step). When it is built, this file
should be updated to reuse its URL-construction logic instead of duplicating
it, so there is exactly one place that assembles a connection string from
configs/database.yaml + environment variables.

Supports both of Alembic's standard modes:

- Offline (`alembic upgrade head --sql`): renders the SQL that *would* run,
  without connecting to any database. Useful for a DBA or operator to review
  the exact DDL before it ever touches the production database with real
  capital behind it, and was how this initial migration was verified in this
  environment (no live Postgres instance was available to connect to).
- Online (`alembic upgrade head`): connects and actually applies the migration.

compare_type=True is set in both modes so that a *future* autogenerate run
(once a real Postgres instance is available to diff against) detects column
type changes, not just added/removed tables and columns -- the default is
False, which would silently miss e.g. a Numeric precision change.
"""

import os
from logging.config import fileConfig

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool

from algo.database.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _get_database_url() -> str:
    """Resolve the database URL to migrate against.

    Priority: an explicit `-x db_url=...` command-line override (for offline
    SQL generation/review without needing real credentials present), then the
    DATABASE_URL environment variable (loaded from .env if present). Raises
    rather than silently falling back to any default -- an ambiguous target
    database is not an acceptable failure mode for a system that will manage
    real capital.
    """
    x_args = context.get_x_argument(as_dictionary=True)
    if "db_url" in x_args:
        return x_args["db_url"]

    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set (checked environment and .env). "
            "Refusing to guess a target database -- set DATABASE_URL, or "
            "pass -x db_url=... for offline SQL generation."
        )
    return url


def run_migrations_offline() -> None:
    """Emit SQL to stdout without connecting to a database."""
    url = _get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Connect and apply migrations against a live database."""
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = _get_database_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
