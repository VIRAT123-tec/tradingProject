"""OrderIntent model: the durable "we are about to place this order" record,
committed *before* any broker call is made.

This table is the single most important piece of the crash-recovery design
(see the Task 2.1 scenario analysis). Its existence on disk before the broker
is ever called is what allows a restart to distinguish "never attempted",
"attempted, ack lost" (SUBMITTED_UNCONFIRMED), and "attempted and confirmed" --
without it, a crash between placing an order and recording its result is
unrecoverable and can lead to duplicate/naked exposure on restart.

idempotency_key and broker_tag are both unique: idempotency_key is the
internal, human-readable dedupe key (e.g. an application-generated string);
broker_tag is the compact (<=20 char) derivative actually stamped on the Kite
order via its `tag` field, which is what lets reconciliation find an orphan
fill at the broker and join it back to this intent. Kite does not enforce tag
uniqueness itself -- the uniqueness guarantee here is entirely DB-side.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from algo.common.enums import IntentStatus, OrderPurpose, TransactionType
from algo.database.models.base import Base, TimestampMixin
from algo.database.models.enum_column import enum_column

if TYPE_CHECKING:
    from algo.database.models.order import Order
    from algo.database.models.trade import Trade


class OrderIntent(Base, TimestampMixin):
    """A pre-committed record of an intended broker order, written before the
    broker is called so a crash mid-placement is always recoverable.
    """

    __tablename__ = "order_intents"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="quantity_positive"),
        Index(
            "ix_order_intents_in_doubt",
            "status",
            postgresql_where="status IN ('PENDING', 'SUBMITTED_UNCONFIRMED', 'PLACED')",
        ),
        Index("ix_order_intents_trade_id", "trade_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    trade_id: Mapped[int] = mapped_column(
        ForeignKey("trades.id", ondelete="RESTRICT"), nullable=False
    )
    purpose: Mapped[OrderPurpose] = mapped_column(enum_column(OrderPurpose), nullable=False)
    transaction_type: Mapped[TransactionType] = mapped_column(
        enum_column(TransactionType), nullable=False
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)

    idempotency_key: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        unique=True,
        doc="Deterministic internal dedupe key, e.g. "
        "'{strategy_instance_id}:{trade_date}:{option_type}:{purpose}'. A "
        "retry of the same logical action reuses this key rather than "
        "minting a new intent.",
    )
    broker_tag: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        unique=True,
        doc="Compact tag actually sent to Kite (<=20 chars per Kite's tag "
        "limit) -- a hashed/truncated derivative of idempotency_key. Must be "
        "collision-resistant within a trading day; recovery's ability to "
        "find an orphan fill depends entirely on this value.",
    )
    status: Mapped[IntentStatus] = mapped_column(
        enum_column(IntentStatus),
        nullable=False,
        default=IntentStatus.PENDING,
        doc="PENDING (committed, broker not yet called) -> "
        "SUBMITTED_UNCONFIRMED (broker call made, response lost -- e.g. "
        "network timeout) or PLACED (broker_order_id received) -> CONFIRMED "
        "(fill recorded) or FAILED/CANCELLED.",
    )
    broker_call_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Set immediately before the broker call is made. Distinguishes "
        "'never attempted' (this is null, status=PENDING) from 'attempted, "
        "ack lost' (this is set, status=SUBMITTED_UNCONFIRMED) during "
        "crash recovery.",
    )
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)

    trade: Mapped["Trade"] = relationship(back_populates="order_intents")
    orders: Mapped[list["Order"]] = relationship(back_populates="intent")

    __mapper_args__ = {"version_id_col": version}

    def __repr__(self) -> str:
        return (
            f"OrderIntent(id={self.id}, trade_id={self.trade_id}, "
            f"purpose={self.purpose}, status={self.status})"
        )
