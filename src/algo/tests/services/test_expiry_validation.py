"""Tests for ValidatingExpiryService / ExpiryNotListedError (live_seams.py):
the instrument-master validation that eliminates the possibility of *using* a
computed expiry the exchange does not actually list.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from algo.common.enums import Exchange
from algo.services.instrument_service import InstrumentSpec
from algo.services.live_seams import ExpiryNotListedError, ValidatingExpiryService


class _FakeInner:
    """Stand-in ExpiryService that returns a fixed computed expiry."""

    def __init__(self, expiry: date) -> None:
        self._expiry = expiry
        self.calls: list[tuple[str, date]] = []

    def get_current_weekly_expiry(self, instrument: str, as_of: date) -> date:
        self.calls.append((instrument, as_of))
        return self._expiry


class _FakeInstruments:
    def __init__(self, *, exchange: Exchange = Exchange.NFO, underlying_symbol: str | None = None) -> None:
        self._exchange = exchange
        self._underlying_symbol = underlying_symbol

    def get_instrument_spec(self, instrument: str) -> InstrumentSpec:
        return InstrumentSpec(
            instrument=instrument,
            exchange=self._exchange,
            strike_interval=Decimal("50"),
            lot_size=75,
            tick_size=Decimal("0.05"),
            underlying_symbol=self._underlying_symbol,
        )


class _FakeProvider:
    """Records how it was queried and returns a canned listed-expiry answer."""

    def __init__(self, listed: list[date] | None) -> None:
        self._listed = listed
        self.queried_underlying: str | None = None
        self.queried_exchange: Exchange | None = None

    def list_option_expiries(self, *, underlying, exchange, timeout=None):
        self.queried_underlying = underlying
        self.queried_exchange = exchange
        return self._listed


def _service(*, computed: date, listed, underlying_symbol=None, exchange=Exchange.NFO):
    inner = _FakeInner(computed)
    provider = _FakeProvider(listed)
    svc = ValidatingExpiryService(
        inner=inner,
        instrument_service=_FakeInstruments(exchange=exchange, underlying_symbol=underlying_symbol),
        listed_provider=provider,
    )
    return svc, inner, provider


class TestValidatingExpiryService:
    def test_returns_computed_when_listed(self):
        svc, _, provider = _service(
            computed=date(2026, 8, 6),
            listed=[date(2026, 7, 30), date(2026, 8, 6), date(2026, 8, 13)],
        )
        assert svc.get_current_weekly_expiry("NIFTY", date(2026, 8, 3)) == date(2026, 8, 6)
        # Validation used the display identity as the underlying (no override).
        assert provider.queried_underlying == "NIFTY"
        assert provider.queried_exchange is Exchange.NFO

    def test_passthrough_when_provider_cannot_enumerate(self):
        # None = "unsupported" (e.g. simulation broker) -> behave exactly as before.
        svc, _, _ = _service(computed=date(2026, 8, 6), listed=None)
        assert svc.get_current_weekly_expiry("NIFTY", date(2026, 8, 3)) == date(2026, 8, 6)

    def test_raises_when_computed_not_listed_nearby(self):
        # Computed Thursday, exchange actually lists the Tuesday two days earlier.
        svc, _, _ = _service(
            computed=date(2026, 8, 6),  # Thu
            listed=[date(2026, 8, 4), date(2026, 8, 11)],  # Tue
        )
        with pytest.raises(ExpiryNotListedError) as exc:
            svc.get_current_weekly_expiry("NIFTY", date(2026, 8, 3))
        msg = str(exc.value)
        assert "2026-08-06" in msg  # computed
        assert "2026-08-04" in msg  # nearest listed
        assert "expiry_weekday is likely stale" in msg
        assert exc.value.computed_expiry == date(2026, 8, 6)
        assert exc.value.listed_expiries == [date(2026, 8, 4), date(2026, 8, 11)]

    def test_raises_with_no_listed_contracts(self):
        # Enumerable but nothing listed -> wrong underlying_symbol / stale-dump reason.
        svc, _, _ = _service(computed=date(2026, 8, 6), listed=[])
        with pytest.raises(ExpiryNotListedError) as exc:
            svc.get_current_weekly_expiry("SENSEXBANK", date(2026, 8, 3))
        msg = str(exc.value)
        assert "NO option contracts" in msg
        assert "underlying_symbol" in msg

    def test_uses_underlying_symbol_override_for_lookup(self):
        # SENSEXBANK display identity -> BANKEX contract name in the dump.
        svc, _, provider = _service(
            computed=date(2026, 8, 27),
            listed=[date(2026, 8, 27)],
            underlying_symbol="BANKEX",
            exchange=Exchange.BFO,
        )
        assert svc.get_current_weekly_expiry("SENSEXBANK", date(2026, 8, 3)) == date(2026, 8, 27)
        assert provider.queried_underlying == "BANKEX"
        assert provider.queried_exchange is Exchange.BFO

    def test_far_computed_expiry_reason(self):
        svc, _, _ = _service(
            computed=date(2026, 12, 31),
            listed=[date(2026, 8, 6), date(2026, 8, 13)],
        )
        with pytest.raises(ExpiryNotListedError) as exc:
            svc.get_current_weekly_expiry("NIFTY", date(2026, 8, 3))
        assert "far from every listed expiry" in str(exc.value)
