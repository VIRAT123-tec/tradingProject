"""Concrete implementations of the instrument/expiry/spot seams, sourced from
``configs/instruments/*.yaml`` and the broker (part of resolving H4).

These make the four-seam set that ``build_seams()`` needs constructible, so the
platform can actually launch, while keeping every value config-driven rather
than hardcoded in code:

* ``ConfigInstrumentService`` -- reads ``configs/instruments/<name>.yaml`` for
  each instrument's static spec (exchange, strike interval, lot size, tick
  size) plus its spot reference and expiry weekday. This is exactly the
  YAML-backed implementation the ``InstrumentService`` seam's docstring
  anticipated.
* ``BrokerSpotPriceProvider`` -- reads the underlying index's spot LTP through
  the broker's own (broker-agnostic) ``get_ltp`` on the cash segment. No
  broker-specific logic here.
* ``ConfigExpiryService`` -- computes the current weekly expiry from a
  configured expiry weekday, shifting earlier off an exchange holiday via the
  injected ``TradingCalendar``.

  ⚠️ **Confirm before live use.** The ``ExpiryService`` seam docstring warns
  that weekly-expiry weekdays have changed for both Nifty and Sensex and that
  holiday shifts do not follow a fixed formula. This config-weekday computation
  is a serviceable default whose ``expiry_weekday`` value MUST be validated
  against the exchange's current rules (ideally replaced by an instrument-master
  / exchange-calendar source) before real-money use. It is honest about being
  config-driven; it is not authoritative.
* ``KiteInstrumentTokenMap`` / ``build_kite_tick_stream`` -- wiring, not a new
  websocket client: they construct the already-built, already-tested
  ``brokers/kite/market_ticker.KiteTickStream`` adapter (which itself wraps
  ``kiteconnect.KiteTicker`` exactly as ``scripts/test_websocket.py`` does) so
  ``start_paper.py`` and ``start_live.py`` can share one real live-tick source
  regardless of which broker is placing orders. See ``build_kite_tick_stream``'s
  own docstring for the important operational consequence (both modes now need
  a valid daily Kite login).
"""

from __future__ import annotations

import threading
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

from algo.brokers.broker_base import InstrumentIdentifier
from algo.brokers.exceptions import InstrumentNotFoundError
from algo.brokers.kite import mapper as kite_mapper
from algo.common.enums import Exchange
from algo.services.instrument_service import InstrumentSpec

if TYPE_CHECKING:
    from algo.brokers.broker_base import BrokerBase
    from algo.brokers.kite.kite_auth import AccessTokenStore
    from algo.brokers.kite.kite_broker import KiteClientProtocol
    from algo.brokers.kite.market_ticker import KiteTickStream
    from algo.scheduler.trading_calendar import TradingCalendar


class InstrumentConfig(BaseModel):
    """One instrument's static configuration (``configs/instruments/<name>.yaml``)."""

    model_config = ConfigDict(frozen=True)

    exchange: Exchange = Field(description="F&O segment the options trade on (NFO/BFO).")
    strike_interval: Decimal = Field(gt=0)
    lot_size: int = Field(gt=0)
    tick_size: Decimal = Field(gt=0)
    spot_exchange: Exchange = Field(description="Cash segment for the underlying index spot (NSE/BSE).")
    spot_symbol: str = Field(description="The underlying index's spot tradingsymbol, e.g. 'NIFTY 50'.")
    expiry_weekday: int = Field(
        ge=0, le=6,
        description="Expiry weekday, 0=Mon..6=Sun. For weekly cadence this is the "
        "weekly-expiry weekday; for monthly cadence it is the weekday the "
        "monthly expiry falls on (the last such weekday of the month). CONFIRM "
        "against current exchange rules before live use -- see the module docstring.",
    )
    expiry_cadence: Literal["weekly", "monthly"] = Field(
        default="weekly",
        description="How this instrument's options expire. 'weekly' (default, e.g. "
        "NIFTY/SENSEX) resolves the nearest expiry_weekday on or after today; "
        "'monthly' (e.g. BANKNIFTY/FINNIFTY/MIDCPNIFTY/BANKEX, which no longer "
        "list weekly options) resolves the last expiry_weekday of the expiry "
        "month. Absent = weekly, so existing configs are unaffected.",
    )
    underlying_symbol: str | None = Field(
        default=None,
        description="Contract name in the broker's instrument dump when it differs "
        "from the config's display identity (the filename stem). None = same as "
        "the identity (the common case). Example: identity 'SENSEXBANK' whose "
        "contracts are named 'BANKEX'. Used only for contract lookup; the "
        "identity is what appears in the database, logs, and reports.",
    )


def _default_instruments_dir() -> Path:
    import os
    return Path(os.environ.get("CONFIG_DIR", "configs")) / "instruments"


class ConfigInstrumentService:
    """``InstrumentService`` backed by ``configs/instruments/*.yaml``.

    Loads every ``<name>.yaml`` under the instruments directory eagerly at
    construction (fail fast on a malformed file), keying each by the UPPERCASE
    stem of its filename (``nifty.yaml`` -> ``"NIFTY"``), matching the
    instrument-identity casing used across ``app.yaml`` and ``risk.yaml``.
    """

    def __init__(self, instruments_dir: Path | None = None) -> None:
        self._dir = instruments_dir if instruments_dir is not None else _default_instruments_dir()
        self._configs: dict[str, InstrumentConfig] = {}
        for path in sorted(self._dir.glob("*.yaml")):
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            if not raw:
                continue  # skip an empty placeholder file rather than fail
            self._configs[path.stem.upper()] = InstrumentConfig.model_validate(raw)

    def get_instrument_spec(self, instrument: str) -> InstrumentSpec:
        cfg = self._require(instrument)
        return InstrumentSpec(
            instrument=instrument, exchange=cfg.exchange, strike_interval=cfg.strike_interval,
            lot_size=cfg.lot_size, tick_size=cfg.tick_size,
            underlying_symbol=cfg.underlying_symbol,
        )

    def spot_reference(self, instrument: str) -> tuple[Exchange, str]:
        """The (cash-segment exchange, spot tradingsymbol) for an instrument's
        underlying index -- what ``BrokerSpotPriceProvider`` needs to read the
        spot LTP."""
        cfg = self._require(instrument)
        return cfg.spot_exchange, cfg.spot_symbol

    def expiry_weekday(self, instrument: str) -> int:
        return self._require(instrument).expiry_weekday

    def expiry_cadence(self, instrument: str) -> str:
        """'weekly' or 'monthly' -- how this instrument's options expire (see
        ``InstrumentConfig.expiry_cadence``)."""
        return self._require(instrument).expiry_cadence

    @property
    def known_instruments(self) -> list[str]:
        return sorted(self._configs)

    def _require(self, instrument: str) -> InstrumentConfig:
        try:
            return self._configs[instrument]
        except KeyError:
            raise KeyError(
                f"no instrument config for {instrument!r} in {self._dir} "
                f"(known: {self.known_instruments})"
            ) from None


class BrokerSpotPriceProvider:
    """``SpotPriceProvider`` that reads the underlying index spot LTP through
    the broker's cash-segment ``get_ltp``.

    Broker-agnostic: it depends only on ``BrokerBase`` and the instrument
    service's spot reference. Raises ``LookupError`` (never returns a guessed or
    stale price) if the broker cannot price the spot -- a wrong spot selects the
    wrong ATM strike, so a silent fallback here would be dangerous.
    """

    def __init__(self, *, broker: BrokerBase, instrument_service: ConfigInstrumentService) -> None:
        self._broker = broker
        self._instruments = instrument_service

    def get_spot_ltp(self, instrument: str) -> Decimal:
        exchange, symbol = self._instruments.spot_reference(instrument)
        ident = InstrumentIdentifier(exchange=exchange, tradingsymbol=symbol)
        ltps = self._broker.get_ltp([ident])
        try:
            return ltps[ident]
        except KeyError:
            raise LookupError(
                f"broker returned no spot LTP for {instrument} ({exchange.value}:{symbol})"
            ) from None


class ConfigExpiryService:
    """``ExpiryService`` computing the current weekly expiry from a configured
    weekday, shifting earlier off an exchange holiday.

    See the module docstring's warning: the configured ``expiry_weekday`` must
    be validated against current exchange rules before live use.
    """

    def __init__(
        self,
        *,
        instrument_service: ConfigInstrumentService,
        trading_calendar: TradingCalendar,
    ) -> None:
        self._instruments = instrument_service
        self._calendar = trading_calendar

    def get_current_weekly_expiry(self, instrument: str, as_of: date) -> date:
        """The current expiry to trade for ``instrument`` as of ``as_of``.

        (Named ``..._weekly_expiry`` for the seam's history; it actually returns
        the current expiry for whichever cadence the instrument is configured
        with.) Weekly cadence -> nearest ``expiry_weekday`` on or after today.
        Monthly cadence -> the last ``expiry_weekday`` of the expiry month (this
        month's if still upcoming, else next month's) -- for indices that no
        longer list weekly options. Either result is then shifted earlier off an
        exchange holiday to the previous trading day.
        """
        weekday = self._instruments.expiry_weekday(instrument)
        if self._instruments.expiry_cadence(instrument) == "monthly":
            expiry = self._monthly_expiry_on_or_after(as_of, weekday)
        else:
            # Nearest occurrence of the expiry weekday on or after as_of
            # (same-day if as_of itself is the expiry weekday -- the 0DTE
            # convention the seam documents).
            days_ahead = (weekday - as_of.weekday()) % 7
            expiry = as_of + timedelta(days=days_ahead)
        # Holiday shift: if the expiry day is not a trading day, the exchange
        # convention is to move the expiry to the previous trading day.
        guard = 0
        while not self._calendar.is_trading_day(expiry) and guard < 14:
            expiry -= timedelta(days=1)
            guard += 1
        return expiry

    @staticmethod
    def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
        """The last date in (year, month) that falls on ``weekday`` (0=Mon)."""
        next_month = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
        last_day = next_month - timedelta(days=1)
        return last_day - timedelta(days=(last_day.weekday() - weekday) % 7)

    def _monthly_expiry_on_or_after(self, as_of: date, weekday: int) -> date:
        """The monthly expiry (last ``weekday`` of the month) on or after
        ``as_of`` -- this month's if it has not yet passed, otherwise next
        month's."""
        expiry = self._last_weekday_of_month(as_of.year, as_of.month, weekday)
        if expiry < as_of:
            year = as_of.year + 1 if as_of.month == 12 else as_of.year
            month = 1 if as_of.month == 12 else as_of.month + 1
            expiry = self._last_weekday_of_month(year, month, weekday)
        return expiry


# ---------------------------------------------------------------------------
# Shared live Kite tick stream (wiring only -- see build_kite_tick_stream)
# ---------------------------------------------------------------------------


class KiteInstrumentTokenMap:
    """Resolves ``InstrumentIdentifier`` <-> Kite's numeric ``instrument_token``
    for the shared live tick stream (``build_kite_tick_stream`` below).

    ``KiteBroker`` already performs an equivalent per-exchange instrument-dump
    lookup internally (for ``find_option_contract``), but that cache is
    private to the container's own broker instance -- which either does not
    exist at all (paper mode never builds a ``KiteBroker``) or is constructed
    too late (``build_seams()`` runs *before* the container exists) for this
    seam to reach into. Duplicating the lookup costs one extra per-exchange
    HTTP call at most (the dump is cached after the first fetch, same as
    ``KiteBroker``'s own cache); it does not duplicate any decision logic.

    Lazy and cached per exchange: the dump for an exchange is fetched only the
    first time a lookup actually needs it (typically the moment a position
    opens and the monitor subscribes to its two option legs), then reused for
    every later lookup on that exchange in both directions.
    """

    def __init__(self, *, client_factory: Callable[[], KiteClientProtocol]) -> None:
        self._client_factory = client_factory
        self._client: KiteClientProtocol | None = None
        self._by_symbol: dict[Exchange, dict[str, int]] = {}
        self._by_token: dict[int, InstrumentIdentifier] = {}
        self._lock = threading.Lock()

    def token_for_instrument(self, ident: InstrumentIdentifier) -> int:
        """``TokenForInstrument`` callable for ``KiteTickStream``."""
        symbols = self._ensure_loaded(ident.exchange)
        try:
            return symbols[ident.tradingsymbol]
        except KeyError:
            raise InstrumentNotFoundError(
                f"no Kite instrument_token found for "
                f"{ident.exchange.value}:{ident.tradingsymbol}"
            ) from None

    def instrument_for_token(self, token: int) -> InstrumentIdentifier | None:
        """``InstrumentForToken`` callable for ``KiteTickStream``."""
        return self._by_token.get(token)

    def _ensure_loaded(self, exchange: Exchange) -> dict[str, int]:
        with self._lock:
            cached = self._by_symbol.get(exchange)
            if cached is not None:
                return cached
            if self._client is None:
                self._client = self._client_factory()
            kite_exchange = kite_mapper.to_kite_exchange(exchange)
            rows = self._client.instruments(kite_exchange)
            symbols: dict[str, int] = {}
            for row in rows:
                raw_token = row.get("instrument_token")
                symbol = row.get("tradingsymbol")
                if raw_token is None or not symbol:
                    continue
                token = int(raw_token)
                symbols[symbol] = token
                self._by_token[token] = InstrumentIdentifier(
                    exchange=exchange, tradingsymbol=symbol
                )
            self._by_symbol[exchange] = symbols
            return symbols


def build_kite_tick_stream(*, access_token_store: AccessTokenStore) -> KiteTickStream:
    """Build the one live Kite market-data websocket both ``start_paper.py``
    and ``start_live.py`` wire up as their ``tick_stream`` seam, so every
    trading mode watches option-leg prices from the same real-time source --
    only order *placement* differs between the Simulation and Kite brokers,
    never the live prices the strategy monitors against.

    No new websocket client is written here. This wires together two pieces
    that already exist and are already tested: ``kiteconnect.KiteTicker``,
    constructed exactly as ``scripts/test_websocket.py`` does (positional
    ``KiteTicker(api_key, access_token)``), and
    ``brokers/kite/market_ticker.KiteTickStream`` (the adapter that already
    satisfies the platform's broker-agnostic ``TickStream`` seam). Only the
    construction/instrument-lookup wiring below is new.

    ``KITE_API_KEY`` is read from the environment and the access token from
    ``access_token_store``, both lazily -- inside closures invoked only when
    the tick stream actually connects or first subscribes, which happens well
    after ``DependencyContainer.start()`` has already loaded ``.env`` and
    validated the token via ``broker.authenticate()``. Reading either eagerly
    here would be too early: ``build_seams()`` runs *before* the container
    (and its ``.env`` load) exists.

    ⚠️ **Operational consequence:** because this is now the ``tick_stream``
    for *both* entrypoints, ``python -m algo.start_paper`` also requires a
    valid ``KITE_API_KEY``/``KITE_ACCESS_TOKEN`` (i.e. the daily
    ``scripts/generate_token.py`` login) even though paper mode's *order
    placement* still never touches the real broker -- only its *prices* now
    do. Paper mode is no longer zero-Kite-credential.
    """
    import os

    from algo.brokers.exceptions import BrokerAuthenticationError
    from algo.brokers.kite.market_ticker import KiteTickStream

    def _require_api_key() -> str:
        api_key = os.environ.get("KITE_API_KEY")
        if not api_key:
            raise BrokerAuthenticationError(
                "KITE_API_KEY must be set (in .env) to use the live Kite tick stream"
            )
        return api_key

    def _require_access_token() -> str:
        token = access_token_store.get_access_token()
        if not token:
            raise BrokerAuthenticationError(
                "no Kite access token available to use the live Kite tick stream "
                "-- run scripts/generate_token.py"
            )
        return token

    def _ticker_factory():
        from kiteconnect import KiteTicker

        # Same construction scripts/test_websocket.py uses.
        return KiteTicker(_require_api_key(), _require_access_token())

    def _lookup_client_factory() -> KiteClientProtocol:
        from kiteconnect import KiteConnect

        client = KiteConnect(api_key=_require_api_key())
        client.set_access_token(_require_access_token())
        return client

    token_map = KiteInstrumentTokenMap(client_factory=_lookup_client_factory)

    return KiteTickStream(
        ticker_factory=_ticker_factory,
        token_for_instrument=token_map.token_for_instrument,
        instrument_for_token=token_map.instrument_for_token,
    )
