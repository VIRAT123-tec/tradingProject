"""ReconciliationBreak model: an append-only record of every detected
discrepancy between broker truth and our database state, and how it was
resolved.

This is the durable trail behind the "critical reconciliation alert" the
functional spec requires -- what an operator actually reviews after an
auto-repair, and the audit record proving a discrepancy was not silently
dropped. All three foreign keys are nullable because a break does not always
cleanly attach to all three: an orphan fill attaches to a position/order/
intent, but a stray broker order under no recognizable tag may only be
describable by its broker_snapshot.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from algo.common.enums import ReconciliationBreakType, ReconciliationResolution
from algo.database.models.base import Base, TimestampMixin
from algo.database.models.enum_column import enum_column

if TYPE_CHECKING:
    from algo.database.models.order import Order
    from algo.database.models.order_intent import OrderIntent
    from algo.database.models.position import Position


class ReconciliationBreak(Base, TimestampMixin):
    """One detected broker-vs-DB discrepancy and its resolution history."""

    __tablename__ = "reconciliation_breaks"
    __table_args__ = (
        Index(
            "ix_reconciliation_breaks_pending",
            "resolution",
            postgresql_where="resolution = 'PENDING'",
        ),
        Index("ix_reconciliation_breaks_position_id", "position_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    position_id: Mapped[int | None] = mapped_column(
        ForeignKey("positions.id", ondelete="RESTRICT"), nullable=True
    )
    order_id: Mapped[int | None] = mapped_column(
        ForeignKey("orders.id", ondelete="RESTRICT"), nullable=True
    )
    intent_id: Mapped[int | None] = mapped_column(
        ForeignKey("order_intents.id", ondelete="RESTRICT"), nullable=True
    )

    break_type: Mapped[ReconciliationBreakType] = mapped_column(
        enum_column(ReconciliationBreakType), nullable=False
    )
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    broker_snapshot: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        doc="Raw broker state (orderbook/tradebook/positions entries) "
        "captured at the moment this break was detected, for audit.",
    )
    resolution: Mapped[ReconciliationResolution] = mapped_column(
        enum_column(ReconciliationResolution),
        nullable=False,
        default=ReconciliationResolution.PENDING,
    )
    resolution_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    resolved_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)

    position: Mapped["Position | None"] = relationship(back_populates="reconciliation_breaks")
    order: Mapped["Order | None"] = relationship()
    intent: Mapped["OrderIntent | None"] = relationship()

    __mapper_args__ = {"version_id_col": version}

    def __repr__(self) -> str:
        return (
            f"ReconciliationBreak(id={self.id}, break_type={self.break_type}, "
            f"resolution={self.resolution})"
        )
