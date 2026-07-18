"""Tests for the concrete instrument/expiry/spot seams (H4), plus the shared
live Kite tick-stream wiring (KiteInstrumentTokenMap / build_kite_tick_stream)
both start_paper.py and start_live.py now use."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from algo.brokers.broker_base import InstrumentIdentifier
from algo.brokers.exceptions import BrokerAuthenticationError, InstrumentNotFoundError
from algo.brokers.kite import mapper as kite_mapper
from algo.brokers.kite.market_ticker import KiteTickStream
from algo.common.enums import Exchange
from algo.services.live_seams import (
    BrokerSpotPriceProvider,
    ConfigExpiryService,
    ConfigInstrumentService,
    KiteInstrumentTokenMap,
    build_kite_tick_stream,
)


def _write_instruments(tmp_path: Path) -> Path:
    d = tmp_path / "instruments"
    d.mkdir()
    (d / "nifty.yaml").write_text(
        "exchange: NFO\nstrike_interval: '50'\nlot_size: 75\ntick_size: '0.05'\n"
        "spot_exchange: NSE\nspot_symbol: 'NIFTY 50'\nexpiry_weekday: 3\n",
        encoding="utf-8",
    )
    (d / "sensex.yaml").write_text(
        "exchange: BFO\nstrike_interval: '100'\nlot_size: 20\ntick_size: '0.05'\n"
        "spot_exchange: BSE\nspot_symbol: 'SENSEX'\nexpiry_weekday: 4\n",
        encoding="utf-8",
    )
    return d


class TestConfigInstrumentService:
    def test_reads_specs(self, tmp_path):
        svc = ConfigInstrumentService(_write_instruments(tmp_path))
        spec = svc.get_instrument_spec("NIFTY")
        assert spec.exchange is Exchange.NFO
        assert spec.strike_interval == Decimal("50")
        assert spec.lot_size == 75
        assert svc.get_instrument_spec("SENSEX").exchange is Exchange.BFO

    def test_unknown_instrument_raises(self, tmp_path):
        svc = ConfigInstrumentService(_write_instruments(tmp_path))
        with pytest.raises(KeyError, match="BANKNIFTY"):
            svc.get_instrument_spec("BANKNIFTY")

    def test_spot_reference_and_expiry_weekday(self, tmp_path):
        svc = ConfigInstrumentService(_write_instruments(tmp_path))
        assert svc.spot_reference("NIFTY") == (Exchange.NSE, "NIFTY 50")
        assert svc.expiry_weekday("NIFTY") == 3

    def test_reads_the_real_committed_configs(self):
        # Validates the shipped configs/instruments/*.yaml actually parse.
        svc = ConfigInstrumentService(Path("configs/instruments"))
        assert "NIFTY" in svc.known_instruments
        assert svc.get_instrument_spec("NIFTY").lot_size > 0


class FakeBroker:
    def __init__(self, ltps):
        self._ltps = ltps

    def get_ltp(self, instruments, *, timeout=None):
        return {i: self._ltps[i] for i in instruments if i in self._ltps}


class TestBrokerSpotPriceProvider:
    def test_reads_spot_via_broker(self, tmp_path):
        svc = ConfigInstrumentService(_write_instruments(tmp_path))
        ident = InstrumentIdentifier(exchange=Exchange.NSE, tradingsymbol="NIFTY 50")
        provider = BrokerSpotPriceProvider(broker=FakeBroker({ident: Decimal("25000")}), instrument_service=svc)
        assert provider.get_spot_ltp("NIFTY") == Decimal("25000")

    def test_missing_spot_raises(self, tmp_path):
        svc = ConfigInstrumentService(_write_instruments(tmp_path))
        provider = BrokerSpotPriceProvider(broker=FakeBroker({}), instrument_service=svc)
        with pytest.raises(LookupError):
            provider.get_spot_ltp("NIFTY")


class _AllWeekdaysCalendar:
    def is_trading_day(self, day):
        return True


class _HolidayCalendar:
    def __init__(self, holidays):
        self._holidays = set(holidays)

    def is_trading_day(self, day):
        return day.weekday() < 5 and day not in self._holidays


class TestConfigExpiryService:
    def test_nearest_expiry_weekday_on_or_after(self, tmp_path):
        svc = ConfigInstrumentService(_write_instruments(tmp_path))
        exp = ConfigExpiryService(instrument_service=svc, trading_calendar=_AllWeekdaysCalendar())
        # 2026-07-06 is a Monday; nifty expiry weekday 3 = Thursday -> 2026-07-09.
        assert exp.get_current_weekly_expiry("NIFTY", date(2026, 7, 6)) == date(2026, 7, 9)

    def test_same_day_when_as_of_is_expiry_day(self, tmp_path):
        svc = ConfigInstrumentService(_write_instruments(tmp_path))
        exp = ConfigExpiryService(instrument_service=svc, trading_calendar=_AllWeekdaysCalendar())
        assert exp.get_current_weekly_expiry("NIFTY", date(2026, 7, 9)) == date(2026, 7, 9)

    def test_holiday_shifts_expiry_earlier(self, tmp_path):
        svc = ConfigInstrumentService(_write_instruments(tmp_path))
        # Thursday 2026-07-09 is a holiday -> expiry shifts to Wednesday 07-08.
        cal = _HolidayCalendar([date(2026, 7, 9)])
        exp = ConfigExpiryService(instrument_service=svc, trading_calendar=cal)
        assert exp.get_current_weekly_expiry("NIFTY", date(2026, 7, 6)) == date(2026, 7, 8)


# ---------------------------------------------------------------------------
# KiteInstrumentTokenMap / build_kite_tick_stream
# ---------------------------------------------------------------------------

_NFO = kite_mapper.to_kite_exchange(Exchange.NFO)


@dataclass
class FakeKiteInstrumentClient:
    """Stand-in for the KiteClientProtocol subset KiteInstrumentTokenMap uses:
    ``instruments(exchange)`` and ``set_access_token``."""

    rows_by_exchange: dict[str, list[dict]] = field(default_factory=dict)
    calls: list[str] = field(default_factory=list)
    access_token: str | None = None

    def instruments(self, exchange=None):
        self.calls.append(exchange)
        return self.rows_by_exchange.get(exchange, [])

    def set_access_token(self, access_token):
        self.access_token = access_token


class TestKiteInstrumentTokenMap:
    def test_resolves_token_for_known_instrument(self):
        client = FakeKiteInstrumentClient(
            rows_by_exchange={_NFO: [{"instrument_token": 111, "tradingsymbol": "NIFTY25000CE"}]}
        )
        token_map = KiteInstrumentTokenMap(client_factory=lambda: client)
        ident = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTY25000CE")
        assert token_map.token_for_instrument(ident) == 111

    def test_reverse_lookup_works_after_a_forward_lookup(self):
        client = FakeKiteInstrumentClient(
            rows_by_exchange={_NFO: [{"instrument_token": 111, "tradingsymbol": "NIFTY25000CE"}]}
        )
        token_map = KiteInstrumentTokenMap(client_factory=lambda: client)
        ident = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTY25000CE")
        token_map.token_for_instrument(ident)
        assert token_map.instrument_for_token(111) == ident

    def test_unknown_symbol_raises_instrument_not_found(self):
        client = FakeKiteInstrumentClient(rows_by_exchange={_NFO: []})
        token_map = KiteInstrumentTokenMap(client_factory=lambda: client)
        ident = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="MISSING")
        with pytest.raises(InstrumentNotFoundError):
            token_map.token_for_instrument(ident)

    def test_unknown_token_returns_none_rather_than_raising(self):
        client = FakeKiteInstrumentClient(rows_by_exchange={_NFO: []})
        token_map = KiteInstrumentTokenMap(client_factory=lambda: client)
        assert token_map.instrument_for_token(999) is None

    def test_instrument_dump_is_fetched_once_per_exchange_and_cached(self):
        client = FakeKiteInstrumentClient(
            rows_by_exchange={_NFO: [{"instrument_token": 111, "tradingsymbol": "NIFTY25000CE"}]}
        )
        token_map = KiteInstrumentTokenMap(client_factory=lambda: client)
        ident = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTY25000CE")
        token_map.token_for_instrument(ident)
        token_map.token_for_instrument(ident)
        assert client.calls == [_NFO]

    def test_client_is_constructed_lazily_not_at_init(self):
        built: list[int] = []

        def factory():
            built.append(1)
            return FakeKiteInstrumentClient()

        KiteInstrumentTokenMap(client_factory=factory)
        assert built == []  # no lookup performed yet -> client never built


class _FakeAccessTokenStore:
    def __init__(self, token: str | None = None) -> None:
        self._token = token

    def get_access_token(self):
        return self._token

    def set_access_token(self, access_token):
        self._token = access_token


class TestBuildKiteTickStream:
    def test_returns_a_kite_tick_stream_with_no_network_io(self, monkeypatch):
        # No KITE_API_KEY set at all -- construction itself must still succeed,
        # since everything is wired lazily (build_seams() runs before .env is
        # loaded by the container).
        monkeypatch.delenv("KITE_API_KEY", raising=False)
        stream = build_kite_tick_stream(access_token_store=_FakeAccessTokenStore())
        assert isinstance(stream, KiteTickStream)

    def test_starting_without_api_key_raises_a_clear_error(self, monkeypatch):
        monkeypatch.delenv("KITE_API_KEY", raising=False)
        stream = build_kite_tick_stream(access_token_store=_FakeAccessTokenStore(token="tok"))
        with pytest.raises(BrokerAuthenticationError, match="KITE_API_KEY"):
            stream.start()

    def test_starting_without_access_token_raises_a_clear_error(self, monkeypatch):
        monkeypatch.setenv("KITE_API_KEY", "key123")
        stream = build_kite_tick_stream(access_token_store=_FakeAccessTokenStore(token=None))
        with pytest.raises(BrokerAuthenticationError, match="access token"):
            stream.start()
