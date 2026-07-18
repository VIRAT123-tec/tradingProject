"""PositionStateTransition model: an append-only audit log of every state
change a Position goes through.

This table has no updated_at and no version column -- rows are written once
and never modified, so optimistic locking is meaningless here (there's
nothing to race over). Every transition is written in the same database
transaction that effects the actual state change, giving a complete,
replayable history that both reconciliation.py and any future compliance
review would rely on.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from algo.common.enums import PositionState, StateTransitionActor
from algo.database.models.base import Base
from algo.database.models.enum_column import enum_column

if TYPE_CHECKING:
    from algo.database.models.position import Position


class PositionStateTransition(Base):
    """One recorded state-machine transition for a Position.

    from_state is nullable to represent the row created alongside a
    Position's initial creation (no prior state to record), rather than
    forcing an artificial "NONE" enum member.
    """

    __tablename__ = "position_state_transitions"
    __table_args__ = (
        Index("ix_position_state_transitions_position_created", "position_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    position_id: Mapped[int] = mapped_column(
        ForeignKey("positions.id", ondelete="RESTRICT"), nullable=False
    )
    from_state: Mapped[PositionState | None] = mapped_column(
        enum_column(PositionState, name="positionstate_from_enum"), nullable=True
    )
    to_state: Mapped[PositionState] = mapped_column(
        enum_column(PositionState, name="positionstate_to_enum"), nullable=False
    )
    reason: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        doc="Free-text explanation, e.g. 'stoploss hit: combined premium "
        "142.50 >= 140.00' or 'recovered filled entry after crash'.",
    )
    actor: Mapped[StateTransitionActor] = mapped_column(
        enum_column(StateTransitionActor),
        nullable=False,
        doc="What caused this transition -- the strategy's own logic, the "
        "independent kill switch, crash recovery, or a manual operator action.",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="Append-only: this is the only timestamp this table needs.",
    )

    position: Mapped["Position"] = relationship(back_populates="state_transitions")

    def __repr__(self) -> str:
        return (
            f"PositionStateTransition(id={self.id}, position_id={self.position_id}, "
            f"{self.from_state} -> {self.to_state}, actor={self.actor})"
        )
