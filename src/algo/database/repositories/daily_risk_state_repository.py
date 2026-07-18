"""DailyRiskStateRepository: persistence for the DailyRiskState aggregate.

Exposes the portfolio-level (instrument=NULL) and per-instrument grains as
distinct, explicit method pairs rather than one method with an ambiguous
optional parameter -- callers in risk/daily_loss_limit.py should not have to
remember that passing instrument=None means something structurally different
(a different partial unique index, a different row) from passing an instrument.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import select

from algo.database.models.daily_risk_state import DailyRiskState
from algo.database.repositories.base import BaseRepository


class DailyRiskStateRepository(BaseRepository[DailyRiskState]):
    """Persistence operations for DailyRiskState rows."""

    model = DailyRiskState

    def get_portfolio_row(self, account_id: int, trade_date: date) -> DailyRiskState | None:
        """The account-wide aggregate row for one day (instrument IS NULL)."""
        stmt = select(DailyRiskState).where(
            DailyRiskState.account_id == account_id,
            DailyRiskState.trade_date == trade_date,
            DailyRiskState.instrument.is_(None),
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_instrument_row(
        self, account_id: int, instrument: str, trade_date: date
    ) -> DailyRiskState | None:
        """One instrument's own row for one day."""
        stmt = select(DailyRiskState).where(
            DailyRiskState.account_id == account_id,
            DailyRiskState.instrument == instrument,
            DailyRiskState.trade_date == trade_date,
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_or_create_portfolio_row(
        self, *, account_id: int, trade_date: date, loss_limit: Decimal
    ) -> tuple[DailyRiskState, bool]:
        """Idempotently ensure today's portfolio-level row exists."""

        def lookup() -> DailyRiskState | None:
            return self.get_portfolio_row(account_id, trade_date)

        def factory() -> DailyRiskState:
            return DailyRiskState(
                account_id=account_id,
                instrument=None,
                trade_date=trade_date,
                loss_limit=loss_limit,
            )

        return self._get_or_create(lookup=lookup, factory=factory)

    def get_or_create_instrument_row(
        self, *, account_id: int, instrument: str, trade_date: date, loss_limit: Decimal
    ) -> tuple[DailyRiskState, bool]:
        """Idempotently ensure today's per-instrument row exists."""

        def lookup() -> DailyRiskState | None:
            return self.get_instrument_row(account_id, instrument, trade_date)

        def factory() -> DailyRiskState:
            return DailyRiskState(
                account_id=account_id,
                instrument=instrument,
                trade_date=trade_date,
                loss_limit=loss_limit,
            )

        return self._get_or_create(lookup=lookup, factory=factory)

    def update_pnl(
        self,
        row: DailyRiskState,
        *,
        realized_pnl: Decimal,
        unrealized_pnl: Decimal,
    ) -> None:
        """Persist a recomputed P&L snapshot. The caller computes the
        figures; this only writes them."""
        row.realized_pnl = realized_pnl
        row.unrealized_pnl = unrealized_pnl
        self.flush()

    def mark_breached(self, row: DailyRiskState, *, breached_at: datetime) -> None:
        """Record that the loss limit was breached. Deciding that it *was*
        breached (comparing pnl to loss_limit) is daily_loss_limit.py's job."""
        row.breached = True
        row.breached_at = breached_at
        self.flush()
