"""Trading-day seam for the scheduler.

A separate concern from *what time it is* (``TimeProvider``): this answers
*whether today is a day to fire triggers at all*. Split out as its own
Protocol so a real, holiday-aware implementation (backed by
``configs/holidays.yaml`` via ``services/holiday_service.py``, not yet built)
can be injected later without touching the scheduler itself.

The default, ``WeekdayTradingCalendar``, deliberately covers only the
Monday-Friday check -- no holiday awareness. Without even this much, a
scheduler process running continuously (as this platform's is meant to) would
try to fire an entry trigger on a Saturday, which is a real correctness gap,
not a hypothetical one. Full exchange-holiday awareness is a known, explicit
gap this default does not attempt to close (see this module's summary).
"""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable


@runtime_checkable
class TradingCalendar(Protocol):
    """Seam for "is this date a trading day"."""

    def is_trading_day(self, day: date) -> bool: ...


class WeekdayTradingCalendar:
    """Every Monday-Friday is a trading day; Saturday/Sunday are not. No
    exchange-holiday awareness -- see the module docstring."""

    def is_trading_day(self, day: date) -> bool:
        return day.weekday() < 5  # Monday=0 .. Sunday=6
