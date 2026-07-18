"""StrategyInstance model: the persistent identity of "strategy X running on
instrument Y under account Z".

Created once by strategy_engine/instance_factory.py and reused forever after --
a process restart re-attaches to the same instance row rather than recreating
it. This is what makes "reconstruct current state from the database rather
than assuming IDLE" possible: the instance's identity survives independently
of any single day's Position.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from algo.common.enums import Exchange, InstanceStatus
from algo.database.models.base import Base, TimestampMixin
from algo.database.models.enum_column import enum_column

if TYPE_CHECKING:
    from algo.database.models.account import Account
    from algo.database.models.position import Position
    from algo.database.models.risk_control_flag import RiskControlFlag


class StrategyInstance(Base, TimestampMixin):
    """One persistent (strategy_id, instrument, account_id) combination.

    ``instrument`` is deliberately a plain validated string, not an enum or a
    foreign key to an instruments table -- instrument names are config-driven
    (configs/instruments/*.yaml) and onboarding a new one must never require a
    schema migration. ``exchange``, by contrast, is a true fixed-vocabulary
    enum: NFO/BFO are structural exchange segments, not strategy parameters.
    """

    __tablename__ = "strategy_instances"
    __table_args__ = (
        UniqueConstraint(
            "strategy_id",
            "instrument",
            "account_id",
            name="uq_strategy_instances_strategy_instrument_account",
        ),
        Index(
            "ix_strategy_instances_frozen",
            "status",
            postgresql_where="status = 'FROZEN'",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    strategy_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        doc="Identifier matching strategy_registry.py's registration key, "
        "e.g. 'strategy_1'. Not a foreign key: the registry is code, not a "
        "database table, for this first pass.",
    )
    instrument: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        doc="Instrument name, e.g. 'NIFTY' or 'SENSEX'. Config-driven, "
        "deliberately not an enum or lookup table.",
    )
    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="RESTRICT"), nullable=False
    )
    exchange: Mapped[Exchange] = mapped_column(
        enum_column(Exchange),
        nullable=False,
        doc="Exchange segment this instrument's derivatives trade on (NFO/BFO).",
    )
    status: Mapped[InstanceStatus] = mapped_column(
        enum_column(InstanceStatus),
        nullable=False,
        default=InstanceStatus.ACTIVE,
        doc="ACTIVE=schedulable, FROZEN=this instrument's instance is paused "
        "pending manual review after an unrecoverable error (the rest of the "
        "platform keeps running), DISABLED=administratively turned off.",
    )
    version: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=1,
        doc="Optimistic-lock version, incremented on every update. Guards "
        "against a lost update if, e.g., a manual unfreeze races a fresh "
        "ERROR transition from the strategy runner.",
    )

    account: Mapped["Account"] = relationship(back_populates="strategy_instances")
    positions: Mapped[list["Position"]] = relationship(back_populates="strategy_instance")
    risk_control_flags: Mapped[list["RiskControlFlag"]] = relationship(
        back_populates="strategy_instance"
    )

    __mapper_args__ = {"version_id_col": version}

    def __repr__(self) -> str:
        return (
            f"StrategyInstance(id={self.id}, strategy_id={self.strategy_id!r}, "
            f"instrument={self.instrument!r}, status={self.status})"
        )
