"""Idempotent schema initialisation for the market-data database.

Creates the plain tables, then -- if the TimescaleDB extension is available --
upgrades ``option_ticks`` to a hypertable with an efficient per-instrument
index, compression, a compression policy, and continuous aggregates. If
Timescale is NOT installed, everything except the plain tables + index is
skipped with a clear warning, and the collector still works (plain Postgres).

All DDL is ``IF NOT EXISTS`` / ``if_not_exists => TRUE``, so this is safe to run
on every startup. Config-driven (chunk interval, compression window, aggregate
intervals) -- no static SQL file to drift out of sync.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import text

from algo.market_data_collector.db import create_all_tables

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from algo.market_data_collector.config import CollectorConfig

_logger = logging.getLogger("algo.collector.init")

# Human interval -> (view suffix, timescale time_bucket literal).
_AGG_SUFFIX = {"1 minute": "1m", "5 minutes": "5m", "1 hour": "1h", "1 day": "1d"}


def initialize_schema(engine: Engine, config: CollectorConfig) -> dict[str, bool]:
    """Create/upgrade the market-data schema. Returns a summary of what was
    applied (for logging), never raises on a missing Timescale extension."""
    create_all_tables(engine)
    applied = {"tables": True, "index": False, "timescale": False,
               "hypertable": False, "compression": False, "aggregates": False}

    with engine.begin() as conn:
        # Per-instrument replay index -- useful with or without Timescale.
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_option_ticks_token_time "
            "ON option_ticks (instrument_token, time DESC)"
        ))
        applied["index"] = True

        if not _try_enable_timescale(conn):
            _logger.warning(
                "TimescaleDB extension not available on this database -- option_ticks "
                "will remain a plain table (fully functional; no compression/hypertable/"
                "continuous-aggregates). Install timescaledb and re-run to upgrade."
            )
            return applied
        applied["timescale"] = True

    # Hypertable + policies each in their own transaction (Timescale requires it).
    applied["hypertable"] = _create_hypertable(engine, config)
    applied["compression"] = _apply_compression(engine, config)
    applied["aggregates"] = _create_continuous_aggregates(engine, config)
    _logger.info("market-data schema initialised: %s", applied)
    return applied


def _try_enable_timescale(conn) -> bool:
    try:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
        row = conn.execute(text(
            "SELECT 1 FROM pg_extension WHERE extname='timescaledb'"
        )).fetchone()
        return row is not None
    except Exception:  # noqa: BLE001 -- not installed / no permission; degrade gracefully
        return False


def _create_hypertable(engine: Engine, config: CollectorConfig) -> bool:
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "SELECT create_hypertable('option_ticks', 'time', "
                "chunk_time_interval => INTERVAL :ci, if_not_exists => TRUE, "
                "migrate_data => TRUE)"
            ), {"ci": config.timescale.chunk_interval})
        return True
    except Exception:  # noqa: BLE001
        _logger.error("failed to create hypertable on option_ticks", exc_info=True)
        return False


def _apply_compression(engine: Engine, config: CollectorConfig) -> bool:
    if not config.timescale.compression_after:
        return False
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE option_ticks SET ("
                "timescaledb.compress, "
                "timescaledb.compress_segmentby = 'instrument_token', "
                "timescaledb.compress_orderby = 'time DESC')"
            ))
            conn.execute(text(
                "SELECT add_compression_policy('option_ticks', INTERVAL :after, "
                "if_not_exists => TRUE)"
            ), {"after": config.timescale.compression_after})
        return True
    except Exception:  # noqa: BLE001
        _logger.error("failed to configure compression on option_ticks", exc_info=True)
        return False


def _create_continuous_aggregates(engine: Engine, config: CollectorConfig) -> bool:
    ok = True
    for interval in config.timescale.continuous_aggregates:
        suffix = _AGG_SUFFIX.get(interval)
        if suffix is None:
            _logger.warning("unknown continuous-aggregate interval %r; skipping", interval)
            continue
        view = f"option_ticks_{suffix}"
        try:
            # A continuous aggregate CREATE cannot run inside a txn block.
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text(
                    f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view} "
                    "WITH (timescaledb.continuous) AS "
                    f"SELECT time_bucket(INTERVAL '{interval}', time) AS bucket, instrument_token, "
                    "first(last_price, time) AS open, max(last_price) AS high, "
                    "min(last_price) AS low, last(last_price, time) AS close, "
                    "last(volume, time) AS volume, last(oi, time) AS oi "
                    "FROM option_ticks GROUP BY bucket, instrument_token "
                    "WITH NO DATA"
                ))
        except Exception:  # noqa: BLE001
            _logger.error("failed to create continuous aggregate %s", view, exc_info=True)
            ok = False
    return ok
