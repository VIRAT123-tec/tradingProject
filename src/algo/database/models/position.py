"""Position model: one straddle cycle for one strategy instance on one trading day.

This is the aggregate root of the daily lifecycle -- the explicit, persisted
state machine (IDLE -> ENTRY_PENDING -> OPEN -> EXIT_PENDING -> CLOSED, with
ERROR reachable from any state) lives here, along with the config values
snapshotted at entry time (lots, lot_size, target_pct, sl_pct, ...) so a
historical trade's target/SL always reflects what was configured *when it was
placed*, never what today's YAML happens to say.

The UNIQUE(strategy_instance_id, trade_date) constraint is the single
mechanism behind three requirements at once: entry idempotency (insert-and-
catch-violation, not read-then-write), "one cycle per day," and "no re-entry
same day once CLOSED."
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from algo.common.enums import ExitReason, PositionState
from algo.database.models.base import Base, TimestampMixin
from algo.database.models.enum_column import enum_column

if TYPE_CHECKING:
    from algo.database.models.position_state_transition import PositionStateTransition
    from algo.database.models.reconciliation_break import ReconciliationBreak
    from algo.database.models.strategy_instance import StrategyInstance
    from algo.database.models.trade import Trade


class Position(Base, TimestampMixin):
    """A single trading day's straddle attempt for one StrategyInstance.

    Money/premium columns use Numeric(18, 4) to comfortably hold sub-paisa
    broker precision without float rounding error. Percentage columns
    (target_pct, sl_pct) use Numeric(6, 4), storing a 0-1 fraction (e.g. 0.10
    for 10%) with headroom far beyond any sane configured value.
    """

    __tablename__ = "positions"
    __table_args__ = (
        UniqueConstraint(
            "strategy_instance_id", "trade_date", name="uq_positions_instance_trade_date"
        ),
        CheckConstraint("lots IS NULL OR lots > 0", name="lots_positive"),
        CheckConstraint("lot_size IS NULL OR lot_size > 0", name="lot_size_positive"),
        CheckConstraint("quantity IS NULL OR quantity > 0", name="quantity_positive"),
        CheckConstraint(
            "lots IS NULL OR lot_size IS NULL OR quantity IS NULL "
            "OR quantity = lots * lot_size",
            name="quantity_matches_lots_times_lot_size",
        ),
        CheckConstraint(
            "target_pct IS NULL OR (target_pct > 0 AND target_pct < 1)",
            name="target_pct_in_range",
        ),
        CheckConstraint(
            "sl_pct IS NULL OR (sl_pct > 0 AND sl_pct < 1)", name="sl_pct_in_range"
        ),
        CheckConstraint(
            "exit_completed_at IS NULL OR entry_completed_at IS NULL "
            "OR exit_completed_at >= entry_completed_at",
            name="exit_after_entry",
        ),
        Index(
            "ix_positions_active_states",
            "state",
            postgresql_where="state IN ('ENTRY_PENDING', 'OPEN', 'EXIT_PENDING', 'ERROR')",
        ),
        Index("ix_positions_trade_date", "trade_date"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    strategy_instance_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_instances.id", ondelete="RESTRICT"), nullable=False
    )
    trade_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        doc="IST calendar trading date this cycle belongs to, sourced from "
        "services/time_service.py. This value is the idempotency key together "
        "with strategy_instance_id -- a timezone bug in time_service would "
        "corrupt this column's correctness, not just display.",
    )
    state: Mapped[PositionState] = mapped_column(
        enum_column(PositionState),
        nullable=False,
        default=PositionState.IDLE,
        doc="Current state-machine state. Authoritative -- never inferred "
        "from in-memory strategy runner state.",
    )

    # --- Entry-time snapshot: copied from config/market at entry, never
    # re-derived from current config, so edits to YAML after entry cannot
    # retroactively change a historical trade's thresholds. ---
    expiry_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    strike: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    strike_interval: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    lots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lot_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    entry_spot_ltp: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    target_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), nullable=True)
    sl_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), nullable=True)

    # --- Computed at/after entry ---
    combined_entry_premium: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 4), nullable=True
    )
    target_premium: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    stoploss_premium: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)

    # --- Exit ---
    combined_exit_premium: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 4), nullable=True
    )
    exit_reason: Mapped[ExitReason | None] = mapped_column(
        enum_column(ExitReason), nullable=True
    )
    realized_pnl: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)

    # --- Timing ---
    entry_signal_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="When entry_logic.py decided to fire, before any broker call.",
    )
    entry_completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="When both legs were confirmed filled and the DB write succeeded "
        "-- this is the moment the trade is 'open' in the system's eyes, per "
        "the functional spec, even though the broker order existed earlier.",
    )
    exit_signal_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    exit_completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        doc="Human-readable narrative for ERROR/frozen states, e.g. 'partial "
        "entry: PE rejected, CE leg auto-unwound'.",
    )
    needs_reconciliation: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        doc="Set when recovery or reconciliation had to repair this position "
        "from broker truth. Stays true until an operator (or a clean "
        "subsequent reconcile) clears it -- surfaces auto-repaired positions "
        "to monitoring even though they were not left in ERROR.",
    )
    version: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=1,
        doc="Optimistic-lock version. Mandatory here: the independent "
        "kill-switch watcher and the strategy's own exit_logic can both "
        "attempt to transition this row concurrently.",
    )

    strategy_instance: Mapped["StrategyInstance"] = relationship(back_populates="positions")
    trades: Mapped[list["Trade"]] = relationship(back_populates="position")
    state_transitions: Mapped[list["PositionStateTransition"]] = relationship(
        back_populates="position"
    )
    reconciliation_breaks: Mapped[list["ReconciliationBreak"]] = relationship(
        back_populates="position"
    )

    __mapper_args__ = {"version_id_col": version}

    def __repr__(self) -> str:
        return (
            f"Position(id={self.id}, strategy_instance_id={self.strategy_instance_id}, "
            f"trade_date={self.trade_date}, state={self.state})"
        )
