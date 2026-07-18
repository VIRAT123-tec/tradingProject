"""MarketHoursController: the automatic, holiday-aware trading-session clock.

It maps the current IST time to a session ``Phase`` from the configured
checkpoints (connect / start / stop_subscribe / flush / disconnect). It is a
*pure* function of (now, calendar) -- it owns no sockets or threads; the
orchestrator polls ``phase()`` and drives the one-time side effects on each
transition. On a non-trading day (holiday/weekend, via the reused
``WeekdayTradingCalendar``) every time maps to ``PRE_OPEN`` so nothing connects.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

    from algo.market_data_collector.config import MarketHoursConfig
    from algo.scheduler.trading_calendar import TradingCalendar


class Phase(str, Enum):
    """Session phase, ordered by time of day within a trading day."""

    PRE_OPEN = "PRE_OPEN"      # before connect (or a non-trading day): idle
    CONNECT = "CONNECT"        # connect .. start: socket open, not yet subscribing
    COLLECT = "COLLECT"        # start .. stop_subscribe: subscribing + rotating ATM
    FREEZE = "FREEZE"          # stop_subscribe .. flush: window frozen, still receiving
    FLUSH = "FLUSH"            # flush .. disconnect: drain the queue to the DB
    CLOSED = "CLOSED"          # after disconnect: socket closed, idle till tomorrow


class MarketHoursController:
    def __init__(self, config: MarketHoursConfig, calendar: TradingCalendar) -> None:
        self._cfg = config
        self._calendar = calendar

    def phase(self, now_ist: datetime) -> Phase:
        if not self._calendar.is_trading_day(now_ist.date()):
            return Phase.PRE_OPEN
        t = now_ist.time()
        c = self._cfg
        if t < c.connect:
            return Phase.PRE_OPEN
        if t < c.start:
            return Phase.CONNECT
        if t < c.stop_subscribe:
            return Phase.COLLECT
        if t < c.flush:
            return Phase.FREEZE
        if t < c.disconnect:
            return Phase.FLUSH
        return Phase.CLOSED
