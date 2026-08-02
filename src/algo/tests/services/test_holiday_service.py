"""Unit tests for HolidayService + HolidayAwareTradingCalendar.

Covers the exchange-aware trading-calendar primitives and the platform-wide
adapter that makes the scheduler/collector holiday-aware. Cases A-E from the
holiday-awareness spec (Case D -- holiday expiry shift -- lives in
test_live_seams.py, where the expiry service is exercised).

Anchor dates (2026):
  Mon 2026-01-26  Republic Day -- a real committed holiday, and a MONDAY
                  (the spec's "Monday is a holiday" example).
  Tue 2026-01-27  fabricated consecutive holiday (Case E only).
  Wed 2026-01-28  normal weekday.
  Fri 2026-01-23  the trading day before that weekend+holiday run.
  Sat/Sun 2026-01-24/25  weekend.
"""

from __future__ import annotations

import logging
from datetime import date

from algo.common.enums import Exchange
from algo.services.holiday_service import (
    HolidayAwareTradingCalendar,
    HolidayService,
    load_holidays,
)

_MON_HOLIDAY = date(2026, 1, 26)
_TUE_HOLIDAY = date(2026, 1, 27)
_WED = date(2026, 1, 28)
_FRI_BEFORE = date(2026, 1, 23)
_SAT = date(2026, 1, 24)
_SUN = date(2026, 1, 25)


def _svc(nse=(_MON_HOLIDAY,), bse=()) -> HolidayService:
    return HolidayService(
        {"NSE": frozenset(nse), "BSE": frozenset(bse)},
        logger=logging.getLogger("test.holidays"),
    )


class TestTradingDayAndHoliday:
    def test_case_a_normal_weekday_is_a_trading_day(self):
        s = _svc()
        assert s.is_trading_day(_WED, Exchange.NFO) is True
        assert s.is_holiday(_WED, Exchange.NFO) is False

    def test_case_b_weekend_is_not_a_trading_day(self):
        s = _svc()
        assert s.is_trading_day(_SAT, Exchange.NFO) is False
        assert s.is_trading_day(_SUN, Exchange.NFO) is False
        # A weekend is not a *declared* holiday.
        assert s.is_holiday(_SAT, Exchange.NFO) is False

    def test_case_c_holiday_is_not_a_trading_day(self):
        s = _svc()
        assert s.is_holiday(_MON_HOLIDAY, Exchange.NFO) is True
        assert s.is_trading_day(_MON_HOLIDAY, Exchange.NFO) is False


class TestExchangeMapping:
    def test_nfo_follows_nse_and_bfo_follows_bse(self):
        # Holiday declared on NSE only.
        s = _svc(nse=(_MON_HOLIDAY,), bse=())
        assert s.is_trading_day(_MON_HOLIDAY, Exchange.NFO) is False  # NIFTY & co.
        assert s.is_trading_day(_MON_HOLIDAY, Exchange.BFO) is True   # SENSEX & co.
        assert s.is_holiday(_MON_HOLIDAY, Exchange.NSE) is True
        assert s.is_holiday(_MON_HOLIDAY, Exchange.BSE) is False


class TestTradingDayWalks:
    def test_previous_and_next_trading_day(self):
        s = _svc(nse=(_MON_HOLIDAY,))
        # Before the Monday holiday, the previous trading day skips the weekend.
        assert s.previous_trading_day(_MON_HOLIDAY, Exchange.NFO) == _FRI_BEFORE
        # After Friday: weekend, then the Monday holiday, land on Tuesday.
        assert s.next_trading_day(_FRI_BEFORE, Exchange.NFO) == _TUE_HOLIDAY

    def test_case_e_consecutive_holidays(self):
        # Monday AND Tuesday are holidays; walking back from Wednesday must skip
        # both holidays and the weekend to reach the prior Friday.
        s = _svc(nse=(_MON_HOLIDAY, _TUE_HOLIDAY))
        assert s.previous_trading_day(_WED, Exchange.NFO) == _FRI_BEFORE
        assert s.is_trading_day(_TUE_HOLIDAY, Exchange.NFO) is False


class TestHolidayAwareTradingCalendar:
    def test_case_c_at_scheduler_level_holiday_closes_platform(self):
        cal = HolidayAwareTradingCalendar(_svc(nse=(_MON_HOLIDAY,)))
        assert cal.is_trading_day(_MON_HOLIDAY) is False  # holiday -> no triggers fire
        assert cal.is_trading_day(_WED) is True           # normal weekday -> unchanged
        assert cal.is_trading_day(_SAT) is False          # weekend -> unchanged

    def test_case_a_b_empty_service_equals_weekday_only(self):
        # No holidays configured -> identical to WeekdayTradingCalendar, proving
        # normal-day behaviour is unchanged.
        cal = HolidayAwareTradingCalendar(HolidayService())
        assert cal.is_trading_day(_WED) is True
        assert cal.is_trading_day(_MON_HOLIDAY) is True   # not a holiday when unconfigured
        assert cal.is_trading_day(_SAT) is False

    def test_holiday_logged_once_per_date(self, caplog):
        cal = HolidayAwareTradingCalendar(_svc(nse=(_MON_HOLIDAY,)))
        with caplog.at_level(logging.INFO, logger="algo.holidays"):
            cal.is_trading_day(_MON_HOLIDAY)
            cal.is_trading_day(_MON_HOLIDAY)  # scheduler polls again the same day
        logged = [r for r in caplog.records if "Holiday detected" in r.getMessage()]
        assert len(logged) == 1


class TestConfigLoading:
    def test_committed_holidays_yaml_loads(self):
        cals = load_holidays()  # the real configs/holidays.yaml
        assert "NSE" in cals and "BSE" in cals
        assert date(2026, 8, 15) in cals["NSE"]  # Independence Day
        assert date(2026, 1, 26) in cals["BSE"]  # Republic Day
