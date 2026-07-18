"""RiskControlFlag model: the durable kill-switch / emergency-exit / freeze
record.

This table must survive a restart -- a kill switch that lives only in memory
is worse than no kill switch at all, because it lulls: it "worked" until the
one time a crash happened to coincide with it being active. On startup,
recovery loads active flags here before arming any strategy instance.

The three-way scope CHECK constraint prevents a structurally nonsensical row
(e.g. scope=GLOBAL but strategy_instance_id set) from ever being written --
correctness enforced at the schema level rather than relying on every writer
to get it right.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from algo.common.enums import RiskFlagScope, RiskFlagType
from algo.database.models.base import Base, TimestampMixin
from algo.database.models.enum_column import enum_column

if TYPE_CHECKING:
    from algo.database.models.account import Account
    from algo.database.models.strategy_instance import StrategyInstance


class RiskControlFlag(Base, TimestampMixin):
    """A kill-switch/emergency-exit/freeze flag, scoped globally, to one
    account, or to one strategy instance.
    """

    __tablename__ = "risk_control_flags"
    __table_args__ = (
        CheckConstraint(
            "(scope = 'GLOBAL' AND account_id IS NULL AND strategy_instance_id IS NULL) "
            "OR (scope = 'ACCOUNT' AND account_id IS NOT NULL AND strategy_instance_id IS NULL) "
            "OR (scope = 'STRATEGY_INSTANCE' AND strategy_instance_id IS NOT NULL)",
            name="scope_matches_populated_fk",
        ),
        Index(
            "ix_risk_control_flags_active", "active", postgresql_where="active = true"
        ),
        Index("ix_risk_control_flags_account_id", "account_id"),
        Index("ix_risk_control_flags_strategy_instance_id", "strategy_instance_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    flag_type: Mapped[RiskFlagType] = mapped_column(enum_column(RiskFlagType), nullable=False)
    scope: Mapped[RiskFlagScope] = mapped_column(enum_column(RiskFlagScope), nullable=False)
    account_id: Mapped[int | None] = mapped_column(
        ForeignKey("accounts.id", ondelete="RESTRICT"), nullable=True
    )
    strategy_instance_id: Mapped[int | None] = mapped_column(
        ForeignKey("strategy_instances.id", ondelete="RESTRICT"), nullable=True
    )
    active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        doc="The independent kill-switch watcher polls WHERE active=true on "
        "a short interval -- this column and its partial index are the hot "
        "path for that check.",
    )
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    activated_by: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        doc="e.g. 'system:daily_loss_limit' or 'manual:operator_name'.",
    )
    activated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, doc="When this flag became active."
    )
    cleared_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    cleared_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)

    account: Mapped["Account | None"] = relationship(back_populates="risk_control_flags")
    strategy_instance: Mapped["StrategyInstance | None"] = relationship(
        back_populates="risk_control_flags"
    )

    __mapper_args__ = {"version_id_col": version}

    def __repr__(self) -> str:
        return (
            f"RiskControlFlag(id={self.id}, flag_type={self.flag_type}, "
            f"scope={self.scope}, active={self.active})"
        )
