"""Repository layer: one repository per aggregate, giving strategy_engine,
execution, and risk a persistence interface that never leaks raw SQLAlchemy
query construction or exceptions into their business logic.

Transaction-ownership contract (applies to every repository below): methods
flush(), never commit(). The Session is injected by the caller, which owns
the transaction boundary and decides when multiple repository calls should
land in one atomic commit.
"""

from algo.database.repositories.account_repository import AccountRepository
from algo.database.repositories.base import BaseRepository
from algo.database.repositories.daily_risk_state_repository import DailyRiskStateRepository
from algo.database.repositories.exceptions import (
    ConcurrentModificationError,
    NotFoundError,
    RepositoryError,
)
from algo.database.repositories.order_intent_repository import OrderIntentRepository
from algo.database.repositories.order_repository import OrderRepository
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.reconciliation_break_repository import (
    ReconciliationBreakRepository,
)
from algo.database.repositories.risk_control_flag_repository import RiskControlFlagRepository
from algo.database.repositories.strategy_instance_repository import StrategyInstanceRepository

__all__ = [
    "BaseRepository",
    "RepositoryError",
    "NotFoundError",
    "ConcurrentModificationError",
    "AccountRepository",
    "StrategyInstanceRepository",
    "PositionRepository",
    "OrderIntentRepository",
    "OrderRepository",
    "DailyRiskStateRepository",
    "RiskControlFlagRepository",
    "ReconciliationBreakRepository",
]
