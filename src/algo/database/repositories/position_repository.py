"""PositionRepository: persistence for the Position aggregate, which includes
its child Trade legs and its PositionStateTransition audit log.

Trade and PositionStateTransition are handled here rather than in their own
repositories: neither has a meaningful lifecycle independent of its parent
Position (a Trade cannot exist without a Position; a transition record is
meaningless without the Position it describes), so keeping them inside this
one repository matches the actual aggregate boundary.

Nothing here decides *what* state a Position should move to, *when* an entry
should fire, or *how* combined premium/target/SL are computed -- that is
strategy_engine/strategies/strategy_1/*.py's job. This repository only
persists decisions the caller has already made.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import func, select

from algo.common.enums import PositionState, StateTransitionActor
from algo.database.models.position import Position
from algo.database.models.position_state_transition import PositionStateTransition
from algo.database.models.strategy_instance import StrategyInstance
from algo.database.models.trade import Trade
from algo.database.repositories.base import BaseRepository


class PositionRepository(BaseRepository[Position]):
    """Persistence operations for the Position aggregate (Position + Trade +
    PositionStateTransition)."""

    model = Position

    # --- Position ---

    def get_by_instance_and_date(
        self, strategy_instance_id: int, trade_date: date
    ) -> Position | None:
        """Look up the position for one instance's one trading day -- the
        same (strategy_instance_id, trade_date) key the unique constraint
        enforces, and the idempotency check entry_logic.py runs before
        deciding whether to enter."""
        stmt = select(Position).where(
            Position.strategy_instance_id == strategy_instance_id,
            Position.trade_date == trade_date,
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_or_create(
        self,
        *,
        strategy_instance_id: int,
        trade_date: date,
        initial_state: PositionState,
        **snapshot_fields: object,
    ) -> tuple[Position, bool]:
        """Idempotently ensure today's Position row exists for this instance.

        Returns (position, created) -- created=False means a row already
        existed for this (strategy_instance_id, trade_date), which is the
        expected outcome of a duplicate scheduler fire, a process restart
        mid-cycle, or a genuine same-day re-check; in that case
        initial_state/snapshot_fields are ignored and the existing row is
        returned as-is. The caller is responsible for deciding what to do
        next based on the returned row's actual state.

        initial_state is an explicit required argument rather than a
        hardcoded default: deciding "we are entering, so start this row at
        ENTRY_PENDING" is entry_logic.py's decision, not this repository's.
        """

        def lookup() -> Position | None:
            return self.get_by_instance_and_date(strategy_instance_id, trade_date)

        def factory() -> Position:
            return Position(
                strategy_instance_id=strategy_instance_id,
                trade_date=trade_date,
                state=initial_state,
                **snapshot_fields,
            )

        return self._get_or_create(lookup=lookup, factory=factory)

    def list_non_terminal(self) -> list[Position]:
        """List every position not yet CLOSED (and including ERROR) -- the
        startup crash-recovery scan, and the ongoing "what needs attention"
        monitoring query. Backed by the partial index on non-terminal states."""
        stmt = select(Position).where(
            Position.state.in_(
                (
                    PositionState.ENTRY_PENDING,
                    PositionState.OPEN,
                    PositionState.EXIT_PENDING,
                    PositionState.ERROR,
                )
            )
        )
        return list(self.session.execute(stmt).scalars())

    def count_for_account_on_date(self, account_id: int, trade_date: date) -> int:
        """Count of Position rows opened today across every strategy instance
        under one account -- the account-wide "how many new positions today"
        figure a max-daily-entries risk check needs, distinct from
        ``get_by_instance_and_date``'s single-instance duplicate-prevention
        check (which asks the same question for one instance only)."""
        stmt = (
            select(func.count(Position.id))
            .join(StrategyInstance, StrategyInstance.id == Position.strategy_instance_id)
            .where(
                StrategyInstance.account_id == account_id,
                Position.trade_date == trade_date,
            )
        )
        return self.session.execute(stmt).scalar_one()

    def realized_pnl_for_account_on_date(self, account_id: int, trade_date: date) -> Decimal:
        """Sum of realized P&L across every position under one account for one
        trading day -- the account-wide realized loss/profit figure the daily
        loss-limit check compares against its configured limit.

        Realized P&L is only populated on CLOSED positions (it is NULL while a
        position is open); ``coalesce`` treats those NULLs (and an account with
        no positions yet today) as zero, so this returns a well-defined
        ``Decimal`` in every case. Recomputed from the source-of-truth rows on
        each call, so it is inherently correct across restarts and cannot drift
        from a separately-maintained running total.
        """
        stmt = (
            select(func.coalesce(func.sum(Position.realized_pnl), 0))
            .join(StrategyInstance, StrategyInstance.id == Position.strategy_instance_id)
            .where(
                StrategyInstance.account_id == account_id,
                Position.trade_date == trade_date,
            )
        )
        return Decimal(str(self.session.execute(stmt).scalar_one()))

    def list_closed_ids(self) -> list[int]:
        """Ids of every CLOSED position -- the set the trade-history backfill
        walks to record any completed trade not yet in ``trade_history``."""
        stmt = select(Position.id).where(Position.state == PositionState.CLOSED)
        return list(self.session.execute(stmt).scalars())

    def list_needing_reconciliation(self) -> list[Position]:
        """List positions flagged during recovery/reconciliation as having
        been repaired from broker truth -- surfaced to monitoring until an
        operator (or a clean subsequent reconcile) clears the flag."""
        stmt = select(Position).where(Position.needs_reconciliation.is_(True))
        return list(self.session.execute(stmt).scalars())

    def transition_state(
        self,
        position: Position,
        *,
        to_state: PositionState,
        actor: StateTransitionActor,
        reason: str | None = None,
    ) -> PositionStateTransition:
        """Move a Position to a new state and append the corresponding audit
        row, as one persistence operation.

        Does not validate whether from_state -> to_state is a legal
        transition -- that is state_machine.py's responsibility. This method
        only records a transition the caller has already decided on, and
        guarantees the state change and its audit entry are never written
        one without the other.
        """
        from_state = position.state
        position.state = to_state
        transition = PositionStateTransition(
            position_id=position.id,
            from_state=from_state,
            to_state=to_state,
            reason=reason,
            actor=actor,
        )
        self.session.add(transition)
        self.flush()
        return transition

    def list_state_transitions(self, position_id: int) -> list[PositionStateTransition]:
        """Full ordered transition history for one position -- the audit
        trail reconciliation.py and any future compliance review rely on."""
        stmt = (
            select(PositionStateTransition)
            .where(PositionStateTransition.position_id == position_id)
            .order_by(PositionStateTransition.created_at)
        )
        return list(self.session.execute(stmt).scalars())

    # --- Trade (child of Position) ---

    def add_trade(self, trade: Trade) -> Trade:
        """Persist a new Trade leg belonging to an already-persisted Position."""
        return self.add(trade)

    def get_trade_by_id(self, trade_id: int) -> Trade | None:
        """Fetch a single leg by id -- used by fill_manager.py/order_tracker.py,
        which act on a specific leg without needing the whole Position graph."""
        return self.session.get(Trade, trade_id)

    def list_trades_for_position(self, position_id: int) -> list[Trade]:
        """Both legs (or however many a future multi-leg strategy has) for
        one position."""
        stmt = select(Trade).where(Trade.position_id == position_id)
        return list(self.session.execute(stmt).scalars())
