"""HolidayService: the single source of truth for exchange trading holidays.

Loads ``configs/holidays.yaml`` (per-exchange date lists) once and answers the
exchange-aware trading-calendar questions the rest of the platform needs:

    is_holiday(day, exchange)
    is_trading_day(day, exchange)          # weekday AND not a holiday
    next_trading_day(day, exchange)
    previous_trading_day(day, exchange)

The exchange->calendar mapping lives ONLY here (NFO follows the NSE calendar,
BFO follows BSE), so no holiday/exchange logic is duplicated anywhere else. Add
or remove a holiday by editing ``holidays.yaml`` -- no code change.

``HolidayAwareTradingCalendar`` is a thin adapter that satisfies the existing
``scheduler.trading_calendar.TradingCalendar`` Protocol (``is_trading_day(day)``)
by consulting this service, so ``PlatformScheduler`` and the collector's
``MarketHoursController`` become holiday-aware purely by dependency injection --
they need no code change. With an empty calendar the service is byte-for-byte
equivalent to ``WeekdayTradingCalendar`` (weekday check only), so behaviour on a
day with no holidays is unchanged.
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from pathlib import Path

import yaml

from algo.common.enums import Exchange

_logger = logging.getLogger("algo.holidays")

# The one and only place the exchange->holiday-calendar mapping lives. The F&O
# segments follow their cash exchange's holiday list (NIFTY & co. on NFO follow
# NSE; SENSEX & co. on BFO follow BSE).
_CALENDAR_KEY: dict[Exchange, str] = {
    Exchange.NFO: "NSE",
    Exchange.NSE: "NSE",
    Exchange.BFO: "BSE",
    Exchange.BSE: "BSE",
}

# How far a next/previous-trading-day walk will search before giving up -- a
# guard against a misconfigured calendar, never hit in normal operation.
_MAX_WALK_DAYS = 3650


def _default_holidays_path() -> Path:
    """``configs/holidays.yaml``, resolved the same way the rest of the platform
    resolves ``configs/`` (overridable via ``CONFIG_DIR``)."""
    return Path(os.environ.get("CONFIG_DIR", "configs")) / "holidays.yaml"


def load_holidays(path: str | Path | None = None) -> dict[str, frozenset[date]]:
    """Load and parse ``holidays.yaml`` into ``{calendar_key: frozenset[date]}``.

    Accepts YAML dates (already ``date``) or ISO strings. Fails loud on a
    malformed date -- a bad calendar must not be silently ignored."""
    resolved = Path(path) if path is not None else _default_holidays_path()
    raw = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    by_exchange = raw.get("holidays") or {}
    result: dict[str, frozenset[date]] = {}
    for key, dates in by_exchange.items():
        parsed: set[date] = set()
        for d in dates or []:
            parsed.add(d if isinstance(d, date) else date.fromisoformat(str(d)))
        result[str(key).upper()] = frozenset(parsed)
    return result


class HolidayService:
    """Exchange-aware trading-calendar oracle backed by ``holidays.yaml``."""

    def __init__(
        self,
        holidays: dict[str, frozenset[date]] | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._holidays: dict[str, frozenset[date]] = {
            str(k).upper(): frozenset(v) for k, v in (holidays or {}).items()
        }
        self._logger = logger if logger is not None else _logger
        self._logger.info(
            "holiday calendar loaded: %s",
            {k: len(v) for k, v in self._holidays.items()} or "no holidays configured",
        )

    @classmethod
    def from_config(
        cls, path: str | Path | None = None, *, logger: logging.Logger | None = None
    ) -> "HolidayService":
        """Build from ``configs/holidays.yaml`` (or ``path``)."""
        return cls(load_holidays(path), logger=logger)

    # -- Exchange-aware queries ----------------------------------------

    def _dates_for(self, exchange: Exchange) -> frozenset[date]:
        key = _CALENDAR_KEY.get(exchange, exchange.value)
        return self._holidays.get(key, frozenset())

    def is_holiday(self, day: date, exchange: Exchange) -> bool:
        """True if ``day`` is a declared exchange holiday (weekends are not
        'holidays' -- see ``is_trading_day``)."""
        return day in self._dates_for(exchange)

    def is_trading_day(self, day: date, exchange: Exchange) -> bool:
        """True if the exchange trades on ``day``: a weekday that is not a
        declared holiday. With no holidays configured this reduces exactly to the
        weekday check, so normal-day behaviour is unchanged."""
        return day.weekday() < 5 and day not in self._dates_for(exchange)

    def previous_trading_day(self, day: date, exchange: Exchange) -> date:
        """The nearest trading day strictly before ``day`` (skips weekends and
        consecutive holidays)."""
        d = day - timedelta(days=1)
        for _ in range(_MAX_WALK_DAYS):
            if self.is_trading_day(d, exchange):
                return d
            d -= timedelta(days=1)
        raise ValueError(f"no trading day found within {_MAX_WALK_DAYS} days before {day}")

    def next_trading_day(self, day: date, exchange: Exchange) -> date:
        """The nearest trading day strictly after ``day``."""
        d = day + timedelta(days=1)
        for _ in range(_MAX_WALK_DAYS):
            if self.is_trading_day(d, exchange):
                return d
            d += timedelta(days=1)
        raise ValueError(f"no trading day found within {_MAX_WALK_DAYS} days after {day}")


class HolidayAwareTradingCalendar:
    """Platform-wide ``TradingCalendar`` (``is_trading_day(day)``) backed by
    ``HolidayService``.

    A day is a platform trading day only if it is a trading day on EVERY bound
    exchange, so any exchange holiday closes the platform for that day and no
    order is ever placed on a holiday. (Indian exchanges share their holiday
    calendar, so this is exact in practice.) Logs a holiday at most once per date
    -- the scheduler polls this every second, so the guard keeps the log clean.
    Consumed only by PlatformScheduler and MarketHoursController, both of which
    query it with the current date.
    """

    def __init__(
        self,
        holiday_service: HolidayService,
        *,
        exchanges: tuple[Exchange, ...] = (Exchange.NSE, Exchange.BSE),
        logger: logging.Logger | None = None,
    ) -> None:
        self._service = holiday_service
        self._exchanges = exchanges
        self._logger = logger if logger is not None else _logger
        self._last_holiday_logged: date | None = None

    def is_trading_day(self, day: date) -> bool:
        if day.weekday() >= 5:
            return False  # weekend -- normal, unchanged, not logged
        closed_on = [e for e in self._exchanges if self._service.is_holiday(day, e)]
        if closed_on:
            if self._last_holiday_logged != day:
                self._logger.info(
                    "Holiday detected: exchange(s)=%s date=%s; trading skipped",
                    ",".join(e.value for e in closed_on),
                    day.isoformat(),
                )
                self._last_holiday_logged = day
            return False
        return True
