"""TradeHistory model: one permanent, append-only row per completed trade.

This is a dedicated *analytics* table -- a denormalized fact row written once,
after a Position reaches CLOSED, purely for future backtesting, dashboards,
performance analysis, and ad-hoc SQL. It is NOT part of the execution path:

* The execution tables (positions/trades/orders) remain the single source of
  truth. This table is a derived copy; if a write here ever fails, nothing in
  the trading workflow is affected and the row can be rebuilt from the source
  tables (see scripts/backfill_trade_history.py).
* One completed trade (one straddle Position) = one row. ``position_id`` is a
  real foreign key back to the position (traceability) and is UNIQUE, which
  also makes writing idempotent: recovery re-completing an exit, or a retried
  write, can never create a duplicate.
* Human/identity fields (strategy_id, instrument, account_name, mode, ...) are
  *denormalized* onto the row on purpose, so analytics queries need no joins.
  This is standard fact-table design, not accidental duplication.
* Append-only: rows are inserted once and never updated (except a deliberate
  integrity backfill) and never deleted.

A handful of derived columns (holding_seconds/minutes, profit_percent,
day_of_week, month) are computed once at write time from values the strategy
already produced -- they add no trading logic, they just make common analytics
queries trivial and fast over millions of rows.

``max_profit_seen``/``max_loss_seen``/``exit_spot_ltp`` are intentionally
nullable and left NULL for now: they are not currently persisted by the
execution/monitoring layer, and populating them will be a later, isolated phase
that does not change today's monitoring logic.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from algo.common.enums import Exchange, ExitReason
from algo.database.models.base import Base, TimestampMixin
from algo.database.models.enum_column import enum_column

if TYPE_CHECKING:
    pass


class TradeHistory(Base, TimestampMixin):
    """One completed trade, permanently recorded for analytics."""

    __tablename__ = "trade_history"
    __table_args__ = (
        # Indexes sized for millions of rows: the common analytics axes are a
        # date range, an instrument, a strategy, an expiry, and an exit reason;
        # the composite serves the frequent "one strategy+instrument over a
        # date range" query without a table scan.
        Index("ix_trade_history_trade_date", "trade_date"),
        Index("ix_trade_history_instrument", "instrument"),
        Index("ix_trade_history_strategy_id", "strategy_id"),
        Index("ix_trade_history_exit_reason", "exit_reason"),
        Index("ix_trade_history_expiry_date", "expiry_date"),
        Index("ix_trade_history_strategy_instrument_date", "strategy_id", "instrument", "trade_date"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # --- Traceability (real FK) + denormalized identity for join-free analytics ---
    position_id: Mapped[int] = mapped_column(
        ForeignKey("positions.id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
        doc="The completed position this row records. UNIQUE -> one history row "
        "per trade, and the idempotency key that makes writing safe to retry.",
    )
    strategy_instance_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    account_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(64), nullable=False)
    instrument: Mapped[str] = mapped_column(String(32), nullable=False)
    account_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    mode: Mapped[str | None] = mapped_column(
        String(16), nullable=True, doc="PAPER or LIVE -- the active broker at the time of the trade."
    )

    # --- Contract ---
    exchange: Mapped[Exchange | None] = mapped_column(enum_column(Exchange), nullable=True)
    strike: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    expiry_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    call_symbol: Mapped[str | None] = mapped_column(String(64), nullable=True)
    put_symbol: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # --- Timing (+ derived holding durations) ---
    entry_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    exit_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    entry_signal_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    exit_signal_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    holding_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    holding_minutes: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)

    # --- Sizing ---
    lots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lot_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # --- Premiums / thresholds (combined + per leg) ---
    combined_entry_premium: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    combined_exit_premium: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    target_premium: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    stoploss_premium: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    target_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), nullable=True)
    sl_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), nullable=True)
    call_entry_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    call_exit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    put_entry_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    put_exit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)

    # --- Underlying spot ---
    entry_spot_ltp: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    exit_spot_ltp: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 4), nullable=True, doc="Not persisted by execution yet -- NULL until a later phase."
    )

    # --- Outcome (+ derived profit_percent) ---
    exit_reason: Mapped[ExitReason | None] = mapped_column(enum_column(ExitReason), nullable=True)
    realized_pnl: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    pnl_per_share: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 4), nullable=True,
        doc="realized_pnl / lot_size, rounded to two decimals -- additive analytics "
        "alongside (never replacing) realized_pnl. NULL when lot_size is unknown/zero.",
    )
    profit_percent: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 4), nullable=True,
        doc="realized_pnl as a percent of premium collected "
        "(realized_pnl / (combined_entry_premium * quantity) * 100).",
    )
    max_profit_seen: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 4), nullable=True, doc="Not persisted by monitoring yet -- NULL until a later phase."
    )
    max_loss_seen: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 4), nullable=True, doc="Not persisted by monitoring yet -- NULL until a later phase."
    )

    # --- Derived calendar buckets (from trade_date), for fast group-bys ---
    day_of_week: Mapped[str | None] = mapped_column(String(9), nullable=True, doc="e.g. 'Tuesday'.")
    month: Mapped[int | None] = mapped_column(Integer, nullable=True, doc="Calendar month 1-12.")

    # --- Broker references + extensibility ---
    broker_order_ids: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True,
        doc="Structured broker order ids, e.g. {'entry': {'CE': ..., 'PE': ...}, 'exit': {...}}.",
    )
    extra: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True, doc="Reserved for future analytics fields without a schema change."
    )

    def __repr__(self) -> str:
        return (
            f"TradeHistory(id={self.id}, position_id={self.position_id}, "
            f"instrument={self.instrument!r}, trade_date={self.trade_date}, "
            f"exit_reason={self.exit_reason}, realized_pnl={self.realized_pnl})"
        )
