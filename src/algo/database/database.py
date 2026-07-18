"""Database engine construction.

This module's only job is "build me a correctly configured SQLAlchemy
Engine." It knows nothing about accounts, positions, or trades -- no
business logic lives here, only connection/pool configuration.

Configuration is deliberately split across two sources:

- ``DATABASE_URL`` (environment variable, loaded from ``.env`` if present) --
  the connection string, including credentials. Never read from YAML, never
  hardcoded, per the platform's "no hardcoded secrets" requirement. This
  mirrors exactly how database/migrations/env.py resolves its own connection
  string, so the running application and Alembic never disagree about where
  credentials come from.
- ``configs/database.yaml`` -- non-secret pool/connection tuning (pool size,
  timeouts, pre-ping, statement timeout, ...), validated through a Pydantic
  model so a malformed value fails immediately at import/startup time rather
  than at 09:20 AM when the first order is about to be placed.

The connect_args used here (``connect_timeout``, ``application_name``,
``options``) are psycopg2/libpq parameter names, matching the psycopg2-binary
driver this project depends on -- if the driver is ever swapped, this
function is the one place that would need to change.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from sqlalchemy import Engine, create_engine
from sqlalchemy.pool import QueuePool


class DatabasePoolSettings(BaseModel):
    """Validated, non-secret connection-pool tuning, sourced from
    configs/database.yaml. Every field has a production-sane default, so the
    YAML file may be empty, partial, or absent -- only the values it does
    specify override these defaults.
    """

    pool_size: int = Field(
        default=5, gt=0, description="Persistent connections kept open per process."
    )
    max_overflow: int = Field(
        default=10,
        ge=0,
        description="Extra connections allowed beyond pool_size under burst load.",
    )
    pool_timeout: int = Field(
        default=30,
        gt=0,
        description="Seconds to wait for a free pooled connection before raising.",
    )
    pool_recycle: int = Field(
        default=1800,
        gt=0,
        description="Seconds before a pooled connection is discarded and replaced -- "
        "guards against silently using a connection that has gone stale in a "
        "long-running process (e.g. dropped by a firewall/load balancer idle timeout).",
    )
    pool_pre_ping: bool = Field(
        default=True,
        description="Test a connection with a lightweight query before handing it "
        "out, transparently reconnecting if it has gone stale. Kept True by default: "
        "a silently-dead connection at the moment an entry/exit order is about to be "
        "persisted is exactly the kind of failure this platform cannot tolerate.",
    )
    echo: bool = Field(
        default=False, description="Log every emitted SQL statement -- local debugging only."
    )
    connect_timeout: int = Field(
        default=10, gt=0, description="Seconds to wait for the initial TCP connection."
    )
    statement_timeout_ms: int = Field(
        default=30_000,
        gt=0,
        description="Postgres-side statement_timeout: aborts a runaway query rather "
        "than letting it hang a thread indefinitely.",
    )
    application_name: str = Field(
        default="algo_platform",
        description="Reported to Postgres as application_name, for identifying this "
        "process's connections in pg_stat_activity.",
    )


class DatabaseSettings(BaseModel):
    """Fully resolved database configuration: the secret URL plus the
    validated, non-secret pool settings."""

    url: str
    pool: DatabasePoolSettings


def _config_dir() -> Path:
    """Resolve the configs/ directory: defaults to "configs" relative to the
    current working directory (matching how alembic.ini and pyproject.toml
    already assume the process runs from the algo_platform/ root), overridable
    via the CONFIG_DIR environment variable for other layouts."""
    return Path(os.environ.get("CONFIG_DIR", "configs"))


def load_database_settings() -> DatabaseSettings:
    """Load and validate database configuration from DATABASE_URL (env/.env)
    and configs/database.yaml.

    Raises RuntimeError if DATABASE_URL is unset, and pydantic.ValidationError
    if database.yaml contains an invalid value -- both deliberately fail loud
    immediately rather than silently falling back to a guessed default.
    """
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set (checked environment and .env). Refusing "
            "to guess a target database."
        )

    yaml_path = _config_dir() / "database.yaml"
    raw: dict[str, Any] = {}
    if yaml_path.exists():
        with yaml_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

    return DatabaseSettings(url=url, pool=DatabasePoolSettings(**raw))


def build_engine(settings: DatabaseSettings) -> Engine:
    """Construct a new Engine from resolved settings.

    Pure function: no caching, no global state. Exercised directly in tests
    against arbitrary settings, independent of the process-wide singleton
    below.
    """
    return create_engine(
        settings.url,
        poolclass=QueuePool,
        pool_size=settings.pool.pool_size,
        max_overflow=settings.pool.max_overflow,
        pool_timeout=settings.pool.pool_timeout,
        pool_recycle=settings.pool.pool_recycle,
        pool_pre_ping=settings.pool.pool_pre_ping,
        echo=settings.pool.echo,
        connect_args={
            "connect_timeout": settings.pool.connect_timeout,
            "application_name": settings.pool.application_name,
            "options": f"-c statement_timeout={settings.pool.statement_timeout_ms}",
        },
    )


_engine: Engine | None = None
_engine_lock = threading.Lock()


def get_engine() -> Engine:
    """Return the process-wide Engine singleton, building it on first call.

    Thread-safe via double-checked locking: concurrent first-callers (e.g.
    the market-data thread and the main scheduler thread both starting up at
    the same time) cannot each construct a separate Engine, which would
    silently double the real connection pool held open against Postgres.
    """
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                _engine = build_engine(load_database_settings())
    return _engine


def dispose_engine() -> None:
    """Dispose of the singleton Engine's connection pool and clear the cache.

    Call this during graceful process shutdown so pooled connections are
    closed cleanly. Also useful in test teardown: the next get_engine() call
    after this builds a fresh Engine rather than reusing a disposed one.
    """
    global _engine
    with _engine_lock:
        if _engine is not None:
            _engine.dispose()
            _engine = None
