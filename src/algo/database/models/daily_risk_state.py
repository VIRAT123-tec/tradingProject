"""DailyRiskState model: durable per-day P&L/loss-limit tracking that the
kill switch reads and that survives a restart.

Design resolution for the open "portfolio-level vs per-instrument loss limit"
question: this table supports both grains simultaneously via a nullable
``instrument`` column rather than forcing a single choice. A row with
instrument=NULL is the portfolio-level aggregate for that account/day; a row
with instrument set is that specific instrument's daily figure for the same
account/day. risk/daily_loss_limit.py can check either or both depending on
how configs/risk.yaml ultimately configures the limit.

Important Postgres subtlety this schema relies on: a plain
UNIQUE(account_id, instrument, trade_date) constraint would NOT prevent two
portfolio-level rows (instrument=NULL) for the same account/day, because SQL
treats every NULL as distinct from every other NULL in a unique constraint.
This is solved with two separate partial unique indexes instead of one
three-column constraint.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from algo.database.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from algo.database.models.account import Account


class DailyRiskState(Base, TimestampMixin):
    """Per-account (and optionally per-instrument) daily realized/unrealized
    P&L against the configured loss limit.
    """

    __tablename__ = "daily_risk_state"
    __table_args__ = (
        # These two indexes carry the table's actual data-integrity guarantee
        # (the dual portfolio/per-instrument grain), not just a performance
        # tuning -- so both sqlite_where and postgresql_where are set with the
        # same predicate. Without sqlite_where, SQLite silently drops the
        # unrecognized postgresql_where kwarg and creates a plain (non-partial)
        # unique index instead, which would make the two indexes collide with
        # each other on SQLite even though the schema is correct on the actual
        # Postgres target -- confirmed by hitting exactly this during testing.
        # Every other partial index in this schema is a pure performance aid
        # (a dropped predicate costs speed, not correctness) and is
        # deliberately left Postgres-only.
        Index(
            "uq_daily_risk_state_portfolio_row",
            "account_id",
            "trade_date",
            unique=True,
            postgresql_where=text("instrument IS NULL"),
            sqlite_where=text("instrument IS NULL"),
        ),
        Index(
            "uq_daily_risk_state_instrument_row",
            "account_id",
            "instrument",
            "trade_date",
            unique=True,
            postgresql_where=text("instrument IS NOT NULL"),
            sqlite_where=text("instrument IS NOT NULL"),
        ),
        Index("ix_daily_risk_state_trade_date", "trade_date"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="RESTRICT"), nullable=False
    )
    instrument: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        doc="NULL = this row is the portfolio-level aggregate for the "
        "account/day. Set (e.g. 'NIFTY') = this row is that instrument's "
        "own daily figure for the same account/day. Both kinds of row can "
        "coexist for the same account/day.",
    )
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)

    realized_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=0)
    unrealized_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=0)
    loss_limit: Mapped[Decimal] = mapped_column(
        Numeric(18, 4),
        nullable=False,
        doc="Snapshotted from configs/risk.yaml at the start of the day, "
        "for the same reason position config values are snapshotted: a "
        "mid-day config edit must not retroactively change what already "
        "happened.",
    )
    breached: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    breached_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    version: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=1,
        doc="Optimistic-lock version -- multiple positions closing "
        "concurrently can update the same day's aggregate row.",
    )

    account: Mapped["Account"] = relationship(back_populates="daily_risk_states")

    __mapper_args__ = {"version_id_col": version}

    def __repr__(self) -> str:
        return (
            f"DailyRiskState(id={self.id}, account_id={self.account_id}, "
            f"instrument={self.instrument!r}, trade_date={self.trade_date}, "
            f"breached={self.breached})"
        )
