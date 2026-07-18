"""Account model: a single broker trading account.

Deliberately minimal for this first pass (single-account scope) -- holds only
identity and status, never credentials. API keys/tokens live in environment
variables / secrets, referenced by brokers/kite/kite_auth.py, never persisted
to this table. Keeping credentials out of the database means account rotation,
backups, and read replicas never carry secret material.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from algo.common.enums import BrokerName
from algo.database.models.base import Base, TimestampMixin
from algo.database.models.enum_column import enum_column

if TYPE_CHECKING:
    from algo.database.models.daily_risk_state import DailyRiskState
    from algo.database.models.risk_control_flag import RiskControlFlag
    from algo.database.models.strategy_instance import StrategyInstance


class Account(Base, TimestampMixin):
    """A broker trading account that one or more StrategyInstances trade under.

    Single account is assumed for this first version (per project scope), but
    account_id is a real foreign key on every dependent table from day one, so
    adding a second account later is new rows, not a migration of every query.
    """

    __tablename__ = "accounts"
    __table_args__ = (
        UniqueConstraint("broker", "broker_client_id", name="uq_accounts_broker_client"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    broker: Mapped[BrokerName] = mapped_column(
        enum_column(BrokerName),
        nullable=False,
        doc="Which broker backend this account is served by (KITE or SIMULATION).",
    )
    broker_client_id: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        doc="Broker's own client/user id for this account. Nullable for "
        "SIMULATION accounts, which have no real broker-side identity.",
    )
    display_name: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        doc="Human-readable label shown in logs, alerts, and any admin tooling.",
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        doc="False disables this account platform-wide without deleting its "
        "history -- no dependent row is ever deleted.",
    )

    strategy_instances: Mapped[list["StrategyInstance"]] = relationship(
        back_populates="account"
    )
    daily_risk_states: Mapped[list["DailyRiskState"]] = relationship(
        back_populates="account"
    )
    risk_control_flags: Mapped[list["RiskControlFlag"]] = relationship(
        back_populates="account"
    )

    def __repr__(self) -> str:
        return f"Account(id={self.id}, broker={self.broker}, display_name={self.display_name!r})"
