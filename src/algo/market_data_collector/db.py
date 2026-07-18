"""Market-data database layer: a SEPARATE engine, metadata, and tables for the
collector, entirely isolated from the trading execution database.

Uses its own SQLAlchemy ``MetaData`` (not the trading ``Base``), its own engine
built from ``MARKET_DATA_DATABASE_URL`` (via the reused ``build_engine``), and
Core ``Table`` definitions (not ORM models) -- the right shape for a
high-throughput append-only store and for COPY bulk inserts.

The ``option_ticks`` table is a plain table here; ``init_timescale`` upgrades it
to a TimescaleDB hypertable (with compression + continuous aggregates) when the
extension is present, and leaves it a perfectly functional plain table when it
is not. So the collector runs on any Postgres today and gains Timescale's
storage/query benefits the moment the extension is installed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    Date,
    DateTime,
    MetaData,
    Numeric,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import JSONB

from algo.database.database import DatabasePoolSettings, DatabaseSettings, build_engine

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from algo.market_data_collector.config import CollectorConfig

# JSONB on Postgres, plain JSON on SQLite (tests) -- one definition, both backends.
_JSON = JSON().with_variant(JSONB, "postgresql")

metadata = MetaData()

# Dimension: token -> instrument identity. Small, upserted when a strike first
# enters a window, so the fact table can stay narrow (token + market fields).
collector_instruments = Table(
    "collector_instruments",
    metadata,
    Column("instrument_token", BigInteger, primary_key=True),
    Column("underlying", String(32), nullable=False, index=True),
    Column("exchange", String(8), nullable=False),
    Column("expiry_date", Date, nullable=True, index=True),
    Column("strike", Numeric(12, 2), nullable=True),
    Column("option_type", String(2), nullable=True),   # CE / PE
    Column("tradingsymbol", String(64), nullable=False),
    Column("first_seen", DateTime(timezone=True), nullable=False),
)

# Fact (hypertable once Timescale is enabled): one row per received tick.
option_ticks = Table(
    "option_ticks",
    metadata,
    Column("time", DateTime(timezone=True), nullable=False),
    Column("instrument_token", BigInteger, nullable=False),
    Column("last_price", Numeric(18, 4), nullable=True),
    Column("last_traded_qty", BigInteger, nullable=True),
    Column("avg_traded_price", Numeric(18, 4), nullable=True),
    Column("volume", BigInteger, nullable=True),
    Column("oi", BigInteger, nullable=True),
    Column("oi_day_high", BigInteger, nullable=True),
    Column("oi_day_low", BigInteger, nullable=True),
    Column("ohlc_open", Numeric(18, 4), nullable=True),
    Column("ohlc_high", Numeric(18, 4), nullable=True),
    Column("ohlc_low", Numeric(18, 4), nullable=True),
    Column("ohlc_close", Numeric(18, 4), nullable=True),
    Column("total_buy_qty", BigInteger, nullable=True),
    Column("total_sell_qty", BigInteger, nullable=True),
    Column("depth", _JSON, nullable=True),  # 5-level bid/ask (FULL mode only)
    # No primary key: append-only fact table. The (instrument_token, time DESC)
    # index and the hypertable conversion are added by init_timescale.
)

# The physical column order used for COPY / batched INSERT (excludes the _id
# placeholder). Single source of truth so the writer and the table never drift.
TICK_COLUMNS: tuple[str, ...] = (
    "time", "instrument_token", "last_price", "last_traded_qty", "avg_traded_price",
    "volume", "oi", "oi_day_high", "oi_day_low",
    "ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close",
    "total_buy_qty", "total_sell_qty", "depth",
)


@dataclass(slots=True)
class MarketTick:
    """One rich market-data tick (FULL/QUOTE/LTP). Only ``time`` and
    ``instrument_token`` are always present; the rest depend on the mode and on
    what the exchange sent."""

    instrument_token: int
    time: datetime
    last_price: Decimal | None = None
    last_traded_qty: int | None = None
    avg_traded_price: Decimal | None = None
    volume: int | None = None
    oi: int | None = None
    oi_day_high: int | None = None
    oi_day_low: int | None = None
    ohlc_open: Decimal | None = None
    ohlc_high: Decimal | None = None
    ohlc_low: Decimal | None = None
    ohlc_close: Decimal | None = None
    total_buy_qty: int | None = None
    total_sell_qty: int | None = None
    depth: dict[str, Any] | None = field(default=None)

    def as_row(self) -> tuple:
        """Values in ``TICK_COLUMNS`` order, for COPY / executemany."""
        return (
            self.time, self.instrument_token, self.last_price, self.last_traded_qty,
            self.avg_traded_price, self.volume, self.oi, self.oi_day_high, self.oi_day_low,
            self.ohlc_open, self.ohlc_high, self.ohlc_low, self.ohlc_close,
            self.total_buy_qty, self.total_sell_qty, self.depth,
        )


def resolve_market_data_url(config: CollectorConfig) -> str:
    """The market-data DB connection string, from the env var named in config.
    Raises with clear guidance if unset -- never guesses a target database."""
    from dotenv import load_dotenv

    load_dotenv()  # same .env-loading contract as database.load_database_settings
    url = os.environ.get(config.db_url_env)
    if not url:
        raise RuntimeError(
            f"{config.db_url_env} is not set. Point it at the SEPARATE market-data "
            "database (its own Postgres/TimescaleDB), e.g. "
            f"{config.db_url_env}=postgresql+psycopg2://user:pass@host:5432/algo_market_data"
        )
    return url


def build_market_data_engine(config: CollectorConfig, *, url: str | None = None) -> Engine:
    """Build the collector's own engine (separate pool), reusing the platform's
    ``build_engine``. ``url`` overrides the env var (tests)."""
    resolved = url if url is not None else resolve_market_data_url(config)
    settings = DatabaseSettings(
        url=resolved,
        pool=DatabasePoolSettings(application_name="algo_market_data_collector"),
    )
    return build_engine(settings)


def create_all_tables(engine: Engine) -> None:
    """Create the collector tables if absent (plain tables; Timescale upgrade is
    separate). Safe/idempotent, works on Postgres and SQLite."""
    metadata.create_all(engine)
