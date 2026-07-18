"""SQLAlchemy ORM model definitions for algo_platform.

Every model must be imported here so that a single
``from algo.database.models import Base`` (as Alembic's env.py will do) causes
every table to register itself on ``Base.metadata`` -- Alembic's autogenerate
only sees tables whose model classes have actually been imported somewhere in
the process, so this module is the one place that import must happen.

Tables implemented (Tier 1 -- core daily lifecycle, Tier 2 -- production-
critical recoverability, per the approved database architecture):

    accounts, strategy_instances, positions, trades, orders, order_intents,
    position_state_transitions, daily_risk_state, risk_control_flags,
    reconciliation_breaks

Deferred (not yet needed): instrument_master, event_log -- these belong to
later delivery steps (strike/expiry resolution, reporting) and will be added
when those steps are built, not speculatively now.
"""

from algo.database.models.account import Account
from algo.database.models.base import Base, TimestampMixin
from algo.database.models.daily_risk_state import DailyRiskState
from algo.database.models.order import Order
from algo.database.models.order_intent import OrderIntent
from algo.database.models.position import Position
from algo.database.models.position_state_transition import PositionStateTransition
from algo.database.models.reconciliation_break import ReconciliationBreak
from algo.database.models.risk_control_flag import RiskControlFlag
from algo.database.models.strategy_instance import StrategyInstance
from algo.database.models.trade import Trade
from algo.database.models.trade_history import TradeHistory

__all__ = [
    "Base",
    "TimestampMixin",
    "Account",
    "StrategyInstance",
    "Position",
    "Trade",
    "Order",
    "OrderIntent",
    "PositionStateTransition",
    "DailyRiskState",
    "RiskControlFlag",
    "ReconciliationBreak",
    "TradeHistory",
]
