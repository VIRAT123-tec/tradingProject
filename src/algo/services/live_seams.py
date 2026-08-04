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

import logging
import threading
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Literal, Protocol

import yaml
from pydantic import BaseModel, ConfigDict, Field

from algo.brokers.broker_base import InstrumentIdentifier
from algo.brokers.exceptions import InstrumentNotFoundError
from algo.brokers.kite import mapper as kite_mapper
from algo.common.enums import Exchange
from algo.services.instrument_service import InstrumentService, InstrumentSpec

if TYPE_CHECKING:
    from algo.brokers.broker_base import BrokerBase
    from algo.brokers.kite.kite_auth import AccessTokenStore
    from algo.services.expiry_service import ExpiryService
    from algo.brokers.kite.kite_broker import KiteClientProtocol
    from algo.brokers.kite.market_ticker import KiteTickStream
    from algo.scheduler.trading_calendar import TradingCalendar  # noqa: F401 -- kept for reference
    from algo.services.holiday_service import HolidayService

_logger = logging.getLogger("algo.expiry")


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
        holiday_service: HolidayService,
        logger: logging.Logger | None = None,
    ) -> None:
        self._instruments = instrument_service
        self._holidays = holiday_service
        self._logger = logger if logger is not None else _logger

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
        exchange = self._instruments.get_instrument_spec(instrument).exchange
        if self._instruments.expiry_cadence(instrument) == "monthly":
            expiry = self._monthly_expiry_on_or_after(as_of, weekday)
        else:
            # Nearest occurrence of the expiry weekday on or after as_of
            # (same-day if as_of itself is the expiry weekday -- the 0DTE
            # convention the seam documents).
            days_ahead = (weekday - as_of.weekday()) % 7
            expiry = as_of + timedelta(days=days_ahead)
        # Holiday shift: if the computed expiry is not a trading day on THIS
        # instrument's exchange (weekend or an NSE/BSE holiday), the exchange
        # convention is to move the expiry to the previous trading day. Uses the
        # single HolidayService source of truth -- no holiday logic duplicated.
        original = expiry
        guard = 0
        while not self._holidays.is_trading_day(expiry, exchange) and guard < 14:
            expiry -= timedelta(days=1)
            guard += 1
        if expiry != original:
            self._logger.info(
                "Weekly expiry shifted for %s: original=%s adjusted=%s reason=exchange "
                "holiday (%s)",
                instrument, original.isoformat(), expiry.isoformat(), exchange.value,
            )
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
# Instrument-master validation of the computed expiry (source of truth = dump)
# ---------------------------------------------------------------------------


class ListedExpiryProvider(Protocol):
    """Minimal seam over a broker's instrument master: "which option expiries
    does the exchange currently list for this underlying?" Satisfied directly by
    ``BrokerBase.list_option_expiries`` (``None`` = the broker cannot enumerate,
    e.g. the simulation broker)."""

    def list_option_expiries(
        self, *, underlying: str, exchange: Exchange, timeout: float | None = None
    ) -> list[date] | None: ...


class ExpiryNotListedError(Exception):
    """Raised when a *computed* expiry is not among the expiries the exchange's
    instrument master actually lists for the underlying -- i.e. the platform
    computed an expiry that does not exist on the exchange.

    Carries the structured facts (``computed_expiry``, ``listed_expiries``,
    ``instrument``/``underlying``/``exchange``/``as_of``) and, in its message,
    a fully-formed, operator-actionable diagnosis: the computed expiry, the
    nearest expiries the exchange *does* list, and the likely reason. This is
    the loud, precise replacement for the opaque ``InstrumentNotFoundError`` a
    phantom expiry used to produce three layers deeper.
    """

    def __init__(
        self,
        *,
        instrument: str,
        underlying: str,
        exchange: Exchange,
        computed_expiry: date,
        listed: list[date],
        as_of: date,
    ) -> None:
        self.instrument = instrument
        self.underlying = underlying
        self.exchange = exchange
        self.computed_expiry = computed_expiry
        self.listed_expiries = list(listed)
        self.as_of = as_of
        super().__init__(self._build_message())

    def _build_message(self) -> str:
        computed = self.computed_expiry
        if not self.listed_expiries:
            nearest = "<none: the exchange lists NO option contracts for this underlying>"
            reason = (
                f"the underlying symbol {self.underlying!r} matched no option contracts "
                f"in the {self.exchange.value} instrument dump -- likely a wrong "
                "underlying_symbol in configs/instruments/*.yaml, or an empty/stale "
                "instrument dump"
            )
        else:
            closest = min(self.listed_expiries, key=lambda d: abs((d - computed).days))
            nearby = sorted(self.listed_expiries, key=lambda d: abs((d - computed).days))[:5]
            nearest = ", ".join(f"{d.isoformat()} ({d:%a})" for d in sorted(nearby))
            if abs((closest - computed).days) <= 4:
                reason = (
                    f"the exchange lists a nearby expiry on {closest.isoformat()} "
                    f"({closest:%a}) but not the computed {computed:%a} "
                    f"{computed.isoformat()} -- the configured expiry_weekday is likely "
                    "stale, or a holiday shift diverged from the exchange's own adjustment"
                )
            else:
                reason = (
                    "the computed expiry is far from every listed expiry -- the instrument "
                    "dump may be stale, or expiry_weekday/expiry_cadence is misconfigured "
                    "for this instrument"
                )
        return (
            f"computed expiry {computed.isoformat()} ({computed:%a}) for {self.instrument} "
            f"(underlying {self.underlying} on {self.exchange.value}, as_of "
            f"{self.as_of.isoformat()}) is NOT listed on the exchange. Nearest listed "
            f"expiries: {nearest}. Likely reason: {reason}. Refusing to request a contract "
            "for a non-existent expiry."
        )


class ValidatingExpiryService:
    """``ExpiryService`` decorator that validates the inner service's computed
    expiry against the exchange's live instrument master (the source of truth),
    failing loudly if it does not exist rather than letting a phantom expiry
    reach ``find_option_contract`` as an opaque ``InstrumentNotFoundError`` that
    freezes the instance with no explanation.

    Drop-in: it implements the same ``ExpiryService`` seam, so ``strike_selector``
    / ``entry_logic`` need no change. Instrument- and cadence-agnostic --
    validation is a pure set-membership check against the listed dates, identical
    for weekly and monthly. If the provider cannot enumerate expiries
    (``list_option_expiries`` returns ``None`` -- e.g. the simulation broker),
    validation is skipped and behaviour is exactly as before: this is a strict
    safety addition, never a regression.

    It never guesses and never silently substitutes a different expiry: a
    mismatch raises ``ExpiryNotListedError`` (which propagates through
    strike_selector -> entry_logic -> StrategyRunner, preserving the existing
    freeze for a genuine failure, but now with a precise, actionable message).
    """

    def __init__(
        self,
        *,
        inner: "ExpiryService",
        instrument_service: InstrumentService,
        listed_provider: ListedExpiryProvider,
        logger: logging.Logger | None = None,
    ) -> None:
        self._inner = inner
        self._instruments = instrument_service
        self._listed = listed_provider
        self._logger = logger if logger is not None else _logger

    def get_current_weekly_expiry(self, instrument: str, as_of: date) -> date:
        computed = self._inner.get_current_weekly_expiry(instrument, as_of)
        spec = self._instruments.get_instrument_spec(instrument)
        underlying = spec.underlying_symbol or instrument
        listed = self._listed.list_option_expiries(
            underlying=underlying, exchange=spec.exchange
        )
        if listed is None:
            # The broker cannot enumerate its instrument master (simulation):
            # nothing to validate against, so preserve prior behaviour exactly.
            return computed
        if computed in listed:
            return computed
        error = ExpiryNotListedError(
            instrument=instrument,
            underlying=underlying,
            exchange=spec.exchange,
            computed_expiry=computed,
            listed=listed,
            as_of=as_of,
        )
        # Log at the point of detection too, so the cause is visible even if a
        # caller were to translate the exception before the runner logs it.
        self._logger.critical("%s", error)
        raise error


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
