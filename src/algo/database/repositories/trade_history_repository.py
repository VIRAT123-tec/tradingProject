"""TradeHistoryRepository: persistence for the append-only trade_history table.

Deliberately tiny -- a completed trade is written exactly once and never
updated or deleted. The only real operation is an idempotent insert keyed on
``position_id`` (its UNIQUE constraint), so a retried write or a recovery
re-completing an exit can never create a duplicate historical row.
"""

from __future__ import annotations

from sqlalchemy import select

from algo.database.models.trade_history import TradeHistory
from algo.database.repositories.base import BaseRepository


class TradeHistoryRepository(BaseRepository[TradeHistory]):
    """Persistence for completed-trade history rows."""

    model = TradeHistory

    def get_by_position_id(self, position_id: int) -> TradeHistory | None:
        """The history row for a position, or None if not yet recorded."""
        stmt = select(TradeHistory).where(TradeHistory.position_id == position_id)
        return self.session.execute(stmt).scalar_one_or_none()

    def add_if_absent(self, row: TradeHistory) -> tuple[TradeHistory, bool]:
        """Insert ``row`` unless a history row already exists for its
        ``position_id``. Returns (row, created); created=False means one was
        already there (idempotent -- the passed row is discarded)."""
        return self._get_or_create(
            lookup=lambda: self.get_by_position_id(row.position_id),
            factory=lambda: row,
        )

    def recorded_position_ids(self, position_ids: list[int]) -> set[int]:
        """Subset of ``position_ids`` that already have a history row -- used by
        the backfill to skip trades already recorded."""
        if not position_ids:
            return set()
        stmt = select(TradeHistory.position_id).where(
            TradeHistory.position_id.in_(position_ids)
        )
        return set(self.session.execute(stmt).scalars())
