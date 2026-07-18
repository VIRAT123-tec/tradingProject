"""RiskControlFlagRepository: persistence for the durable kill-switch /
emergency-exit / freeze record.

list_active is the hot-path query the independent kill-switch watcher polls
on a short interval, per the non-negotiable requirement that the kill switch
be checked independently of strategy logic -- it must stay fast, which is
exactly what the partial index on active=true backs.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import or_, select

from algo.common.enums import RiskFlagScope, RiskFlagType
from algo.database.models.risk_control_flag import RiskControlFlag
from algo.database.repositories.base import BaseRepository


class RiskControlFlagRepository(BaseRepository[RiskControlFlag]):
    """Persistence operations for RiskControlFlag rows."""

    model = RiskControlFlag

    def list_active(self) -> list[RiskControlFlag]:
        """Every currently active flag, regardless of scope."""
        stmt = select(RiskControlFlag).where(RiskControlFlag.active.is_(True))
        return list(self.session.execute(stmt).scalars())

    def list_active_relevant_to(
        self, *, account_id: int, strategy_instance_id: int
    ) -> list[RiskControlFlag]:
        """Active flags whose scope covers a given strategy instance: GLOBAL
        flags, ACCOUNT flags for its account, or STRATEGY_INSTANCE flags for
        it specifically. This is a filter matching the scope semantics the
        schema's own CHECK constraint already encodes -- not a decision about
        what to do once a relevant flag is found."""
        stmt = select(RiskControlFlag).where(
            RiskControlFlag.active.is_(True),
            or_(
                RiskControlFlag.scope == RiskFlagScope.GLOBAL,
                (RiskControlFlag.scope == RiskFlagScope.ACCOUNT)
                & (RiskControlFlag.account_id == account_id),
                (RiskControlFlag.scope == RiskFlagScope.STRATEGY_INSTANCE)
                & (RiskControlFlag.strategy_instance_id == strategy_instance_id),
            ),
        )
        return list(self.session.execute(stmt).scalars())

    def activate(
        self,
        *,
        flag_type: RiskFlagType,
        scope: RiskFlagScope,
        reason: str | None,
        activated_by: str,
        activated_at: datetime,
        account_id: int | None = None,
        strategy_instance_id: int | None = None,
    ) -> RiskControlFlag:
        """Create and persist a new active flag. The caller has already
        decided the flag_type/scope/target -- this only writes the row (the
        model's scope-consistency CHECK constraint is the actual guard
        against a structurally invalid combination)."""
        flag = RiskControlFlag(
            flag_type=flag_type,
            scope=scope,
            account_id=account_id,
            strategy_instance_id=strategy_instance_id,
            reason=reason,
            activated_by=activated_by,
            activated_at=activated_at,
        )
        return self.add(flag)

    def clear(
        self, flag: RiskControlFlag, *, cleared_by: str, cleared_at: datetime
    ) -> None:
        """Deactivate a flag. Deciding it is safe to clear (e.g. an operator
        confirming broker state is sane again) is not this repository's
        concern -- it only persists the clearance."""
        flag.active = False
        flag.cleared_by = cleared_by
        flag.cleared_at = cleared_at
        self.flush()
