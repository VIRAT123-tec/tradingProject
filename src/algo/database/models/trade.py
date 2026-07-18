"""Trade model: one option leg (CE or PE) belonging to a Position.

Normalized as one row per leg rather than flat ce_*/pe_* columns on Position --
this is what lets a future multi-leg strategy (e.g. an iron condor with two
CEs and two PEs) reuse this table without a schema change, and lets one leg
legitimately have a richer order history (retries, partial fills) than its
sibling without contorting the parent row.

There is intentionally no trades.entry_order_id / exit_order_id foreign key
back to Order -- that would create a circular FK (a trade needs an order to
exist, an order needs a trade to exist). Instead Order.trade_id points here,
and entry_price/exit_price are the winning fill copied onto this row: Trade is
the authoritative leg record, Order is the execution audit trail underneath it.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from algo.common.enums import Exchange, OptionType, TradeLegStatus
from algo.database.models.base import Base, TimestampMixin
from algo.database.models.enum_column import enum_column

if TYPE_CHECKING:
    from algo.database.models.order import Order
    from algo.database.models.order_intent import OrderIntent
    from algo.database.models.position import Position


class Trade(Base, TimestampMixin):
    """One CE or PE leg of a Position's straddle.

    UNIQUE(position_id, option_type, strike) includes strike (not just
    option_type) so that a future strategy selling two CEs at different
    strikes under the same position still fits this table without a schema
    change, even though strategy-1 always uses a single shared ATM strike for
    both legs.
    """

    __tablename__ = "trades"
    __table_args__ = (
        UniqueConstraint(
            "position_id", "option_type", "strike", name="uq_trades_position_type_strike"
        ),
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint(
            "exit_time IS NULL OR entry_time IS NULL OR exit_time >= entry_time",
            name="exit_after_entry",
        ),
        CheckConstraint(
            "entry_price IS NULL OR entry_price >= 0", name="entry_price_non_negative"
        ),
        CheckConstraint(
            "exit_price IS NULL OR exit_price >= 0", name="exit_price_non_negative"
        ),
        Index(
            "ix_trades_open_status", "status", postgresql_where="status = 'OPEN'"
        ),
        Index("ix_trades_position_id", "position_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    position_id: Mapped[int] = mapped_column(
        ForeignKey("positions.id", ondelete="RESTRICT"), nullable=False
    )
    option_type: Mapped[OptionType] = mapped_column(enum_column(OptionType), nullable=False)
    trading_symbol: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        doc="Broker trading symbol resolved by strike_selector.py, e.g. "
        "'NIFTY24JUL24500CE'.",
    )
    exchange: Mapped[Exchange] = mapped_column(enum_column(Exchange), nullable=False)
    strike: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)

    entry_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    exit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    entry_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    exit_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    realized_pnl: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)

    status: Mapped[TradeLegStatus] = mapped_column(
        enum_column(TradeLegStatus),
        nullable=False,
        default=TradeLegStatus.PENDING,
        doc="PENDING until this leg's entry fill is confirmed, then OPEN, "
        "then CLOSED (normal exit) or UNWOUND (bought back immediately after "
        "a partial-entry failure on the sibling leg) or ERROR.",
    )
    version: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=1,
        doc="Optimistic-lock version -- fill_manager.py and recovery can both "
        "attempt to update this row.",
    )

    position: Mapped["Position"] = relationship(back_populates="trades")
    orders: Mapped[list["Order"]] = relationship(back_populates="trade")
    order_intents: Mapped[list["OrderIntent"]] = relationship(back_populates="trade")

    __mapper_args__ = {"version_id_col": version}

    def __repr__(self) -> str:
        return (
            f"Trade(id={self.id}, position_id={self.position_id}, "
            f"option_type={self.option_type}, status={self.status})"
        )
