"""Order model: one broker order placement attempt.

A Trade leg can have multiple Order rows over its life -- a rejected attempt
followed by a retry, or an entry order followed later by an exit order. This
is exactly why Order is its own table rather than a scalar order-id column on
Trade: a single FK cannot represent "N attempts", and the full attempt history
(including rejected/cancelled ones) is precisely what reconciliation.py needs
to compare against the broker's orderbook.

raw_response is stored as JSONB (Postgres) so the full broker payload is
available for audit/reconciliation without having to have anticipated every
field Kite might return in a strongly-typed column.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from algo.common.enums import OrderPurpose, OrderStatus, OrderType, TransactionType
from algo.database.models.base import Base, TimestampMixin
from algo.database.models.enum_column import enum_column

if TYPE_CHECKING:
    from algo.database.models.order_intent import OrderIntent
    from algo.database.models.trade import Trade


class Order(Base, TimestampMixin):
    """One broker order placement attempt, tied to the Trade leg it belongs to
    and, where known, the OrderIntent that preceded it.

    intent_id is nullable to tolerate the (should-be-rare) case where an order
    row is reconstructed during recovery from broker truth alone, without a
    matching intent ever having been found (e.g. an intent's broker_tag was
    lost or a genuinely out-of-band order was reconciled).
    """

    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("broker_order_id", name="uq_orders_broker_order_id"),
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint(
            "filled_quantity >= 0 AND filled_quantity <= quantity",
            name="filled_quantity_within_bounds",
        ),
        CheckConstraint("retry_count >= 0", name="retry_count_non_negative"),
        Index(
            "ix_orders_in_flight", "status", postgresql_where="status IN ('PENDING', 'OPEN')"
        ),
        Index("ix_orders_trade_id", "trade_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    trade_id: Mapped[int] = mapped_column(
        ForeignKey("trades.id", ondelete="RESTRICT"), nullable=False
    )
    intent_id: Mapped[int | None] = mapped_column(
        ForeignKey("order_intents.id", ondelete="RESTRICT"), nullable=True
    )

    purpose: Mapped[OrderPurpose] = mapped_column(enum_column(OrderPurpose), nullable=False)
    transaction_type: Mapped[TransactionType] = mapped_column(
        enum_column(TransactionType), nullable=False
    )
    order_type: Mapped[OrderType] = mapped_column(
        enum_column(OrderType), nullable=False, default=OrderType.MARKET
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    filled_quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    average_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)

    status: Mapped[OrderStatus] = mapped_column(
        enum_column(OrderStatus), nullable=False, default=OrderStatus.PENDING
    )
    broker_order_id: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        doc="Broker's order id, set once the placement call returns an ack. "
        "Null until then -- see OrderIntent.status=SUBMITTED_UNCONFIRMED for "
        "the window where we called the broker but don't have this yet.",
    )
    broker_tag: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        doc="Copy of the OrderIntent.broker_tag actually sent with this "
        "order, denormalized here so reconciliation can match a broker "
        "orderbook row to this table directly without a join.",
    )
    placed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    filled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_response: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        doc="Full broker payload (order placement ack and/or status query "
        "response) for audit and reconciliation.",
    )
    retry_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of retry attempts consumed placing this specific order, "
        "distinguishing a retryable network blip from a non-retryable "
        "rejection per the retry/error-classification requirement.",
    )
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)

    trade: Mapped["Trade"] = relationship(back_populates="orders")
    intent: Mapped["OrderIntent | None"] = relationship(back_populates="orders")

    __mapper_args__ = {"version_id_col": version}

    def __repr__(self) -> str:
        return (
            f"Order(id={self.id}, trade_id={self.trade_id}, "
            f"broker_order_id={self.broker_order_id!r}, status={self.status})"
        )
