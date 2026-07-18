"""StrategyInstanceRepository: persistence for the StrategyInstance aggregate.

get_or_create is what backs instance_factory.py's "create once, reuse
forever" contract -- the first call for a given (strategy_id, instrument,
account_id) creates the persistent identity; every later call (including
after a process restart) finds the same row.
"""

from __future__ import annotations

from sqlalchemy import select

from algo.common.enums import Exchange, InstanceStatus
from algo.database.models.strategy_instance import StrategyInstance
from algo.database.repositories.base import BaseRepository


class StrategyInstanceRepository(BaseRepository[StrategyInstance]):
    """Persistence operations for StrategyInstance rows."""

    model = StrategyInstance

    def get_by_strategy_instrument_account(
        self, strategy_id: str, instrument: str, account_id: int
    ) -> StrategyInstance | None:
        """Look up an instance by its (strategy_id, instrument, account_id)
        natural key -- the same key the unique constraint enforces."""
        stmt = select(StrategyInstance).where(
            StrategyInstance.strategy_id == strategy_id,
            StrategyInstance.instrument == instrument,
            StrategyInstance.account_id == account_id,
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_or_create(
        self,
        *,
        strategy_id: str,
        instrument: str,
        account_id: int,
        exchange: Exchange,
    ) -> tuple[StrategyInstance, bool]:
        """Idempotently ensure the persistent instance row exists. Returns
        (instance, created) -- created=False means this instance already
        existed (the normal case on every restart after the first run).

        The new row's status is left to the model's own column default
        (ACTIVE) rather than being decided here, since a fresh instance
        being active-by-default is a schema-level fact, not a repository
        decision.
        """

        def lookup() -> StrategyInstance | None:
            return self.get_by_strategy_instrument_account(strategy_id, instrument, account_id)

        def factory() -> StrategyInstance:
            return StrategyInstance(
                strategy_id=strategy_id,
                instrument=instrument,
                account_id=account_id,
                exchange=exchange,
            )

        return self._get_or_create(lookup=lookup, factory=factory)

    def list_active(self) -> list[StrategyInstance]:
        """List instances eligible to be armed by the scheduler."""
        stmt = select(StrategyInstance).where(StrategyInstance.status == InstanceStatus.ACTIVE)
        return list(self.session.execute(stmt).scalars())

    def list_frozen(self) -> list[StrategyInstance]:
        """List instances currently frozen pending manual review."""
        stmt = select(StrategyInstance).where(StrategyInstance.status == InstanceStatus.FROZEN)
        return list(self.session.execute(stmt).scalars())

    def set_status(self, instance: StrategyInstance, status: InstanceStatus) -> None:
        """Persist a status change on an already-loaded instance.

        Callers deciding to freeze an instance should load it via
        get_by_id_for_update first, within the same transaction as whatever
        else caused the freeze (e.g. the owning Position's ERROR transition),
        so the two writes land together.
        """
        instance.status = status
        self.flush()
