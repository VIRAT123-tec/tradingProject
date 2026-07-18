"""Top-level daily job scheduler: fires the platform-wide time-based triggers
(entry, hard cutoff, and any other trigger a strategy declares) each registered
StrategyRunner needs, at the right wall-clock time.

Distinct from ``strategy_engine/strategy_scheduler.py``, which handles per-
strategy-instance tick-monitoring cadence -- this package owns "what time is
it, what trigger fires now."
"""

from algo.scheduler.platform_scheduler import PlatformScheduler, SchedulerConfig
from algo.scheduler.trading_calendar import TradingCalendar, WeekdayTradingCalendar

__all__ = [
    "PlatformScheduler",
    "SchedulerConfig",
    "TradingCalendar",
    "WeekdayTradingCalendar",
]
