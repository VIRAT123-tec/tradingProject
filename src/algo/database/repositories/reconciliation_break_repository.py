"""ReconciliationBreakRepository: persistence for detected broker-vs-database
discrepancies and their resolution history.

This is the durable trail behind the "critical reconciliation alert" the
functional spec requires -- what an operator reviews after an auto-repair,
and the audit record proving a discrepancy was never silently dropped.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select

from algo.common.enums import ReconciliationBreakType, ReconciliationResolution
from algo.database.models.reconciliation_break import ReconciliationBreak
from algo.database.repositories.base import BaseRepository


class ReconciliationBreakRepository(BaseRepository[ReconciliationBreak]):
    """Persistence operations for ReconciliationBreak rows."""

    model = ReconciliationBreak

    def create(
        self,
        *,
        break_type: ReconciliationBreakType,
        detected_at: datetime,
        position_id: int | None = None,
        order_id: int | None = None,
        intent_id: int | None = None,
        broker_snapshot: dict[str, Any] | None = None,
    ) -> ReconciliationBreak:
        """Record a newly detected discrepancy. Classifying *what kind* of
        break this is (orphan fill, broker-flat-db-open, ...) is
        reconciliation.py's job -- this only persists the classification and
        evidence it has already produced."""
        break_ = ReconciliationBreak(
            position_id=position_id,
            order_id=order_id,
            intent_id=intent_id,
            break_type=break_type,
            detected_at=detected_at,
            broker_snapshot=broker_snapshot,
        )
        return self.add(break_)

    def list_pending(self) -> list[ReconciliationBreak]:
        """The operator's open-issues queue -- backed by the partial index
        on resolution='PENDING'."""
        stmt = select(ReconciliationBreak).where(
            ReconciliationBreak.resolution == ReconciliationResolution.PENDING
        )
        return list(self.session.execute(stmt).scalars())

    def list_for_position(self, position_id: int) -> list[ReconciliationBreak]:
        """Every break ever recorded against one position."""
        stmt = select(ReconciliationBreak).where(
            ReconciliationBreak.position_id == position_id
        )
        return list(self.session.execute(stmt).scalars())

    def resolve(
        self,
        break_: ReconciliationBreak,
        *,
        resolution: ReconciliationResolution,
        resolved_by: str,
        resolved_at: datetime,
        resolution_notes: str | None = None,
    ) -> None:
        """Record how a break was resolved (auto-repaired or manually)."""
        break_.resolution = resolution
        break_.resolved_by = resolved_by
        break_.resolved_at = resolved_at
        if resolution_notes is not None:
            break_.resolution_notes = resolution_notes
        self.flush()
