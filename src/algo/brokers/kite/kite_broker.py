"""KiteBroker: the concrete ``BrokerBase`` backed by Kite Connect.

Design in one paragraph. Everything Kite-specific is dependency-injected -- the
KiteConnect client, the ``KiteSession`` (token lifecycle), and the
``KiteOrderUpdateStream`` (order-update websocket) are all constructor
parameters -- so this class carries no hidden global state and is fully
unit-testable against fakes, with no network and no real credentials (which is
the only way it *can* be tested, since there is no live Kite in development).
All request/response and exception mapping lives in ``mapper.py``; this class
never touches a Kite string constant directly. Read-only calls retry on
transient errors within a caller-supplied deadline; the three order-mutating
calls (place/modify/cancel) never retry, honouring the ambiguous-outcome
contract in ``BrokerBase`` -- a timed-out placement is surfaced as an ambiguous
``BrokerTimeoutError`` for the caller (entry_logic + reconciliation) to resolve,
never silently re-sent.
"""

from __future__ import annotations

import logging
import threading
import time as _time_module
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from algo.brokers.broker_base import (
    BrokerBase,
    BrokerHolding,
    BrokerInstrument,
    BrokerMargins,
    BrokerOrder,
    BrokerPosition,
    BrokerQuote,
    HealthStatus,
    InstrumentIdentifier,
    ModifyOrderRequest,
    OrderUpdateCallback,
    PlaceOrderRequest,
    PlaceOrderResult,
)
from algo.brokers.exceptions import (
    BrokerError,
    InstrumentNotFoundError,
    OrderNotCancellableError,
    OrderNotFoundError,
    OrderNotModifiableError,
    RetryableBrokerError,
)
from algo.brokers.kite import mapper
from algo.common.enums import BrokerName, Exchange, OptionType

if TYPE_CHECKING:
    from algo.brokers.kite.kite_auth import KiteSession
    from algo.brokers.kite.websocket import KiteOrderUpdateStream

_T = TypeVar("_T")

# Sort key sentinel: an order with no placed_at sorts before any timestamped one.
_MIN_AWARE = datetime.min.replace(tzinfo=timezone.utc)

# IST has no DST, so a fixed +5:30 offset always yields the correct trading date.
_IST = timezone(timedelta(hours=5, minutes=30))


def _ist_today() -> date:
    """Current IST calendar date -- the notion of "trading day" the instrument
    dump is refreshed once per (see ``KiteBroker._instrument_rows``). Matches
    the timezone ``Position.trade_date`` is stamped in."""
    return datetime.now(_IST).date()


class KiteClientProtocol(Protocol):
    """The subset of ``kiteconnect.KiteConnect`` this broker uses. Declared as a
    Protocol so tests inject a fake with no network, and so this module never
    imports the concrete client type."""

    def login_url(self) -> str: ...
    def set_access_token(self, access_token: str) -> None: ...
    def generate_session(self, request_token: str, api_secret: str) -> dict[str, Any]: ...
    def profile(self) -> dict[str, Any]: ...
    def place_order(self, **kwargs: Any) -> str: ...
    def modify_order(self, **kwargs: Any) -> str: ...
    def cancel_order(self, variety: str, order_id: str, **kwargs: Any) -> str: ...
    def orders(self) -> list[dict[str, Any]]: ...
    def order_history(self, order_id: str) -> list[dict[str, Any]]: ...
    def positions(self) -> dict[str, list[dict[str, Any]]]: ...
    def holdings(self) -> list[dict[str, Any]]: ...
    def margins(self, segment: str | None = ...) -> dict[str, Any]: ...
    def quote(self, *instruments: str) -> dict[str, Any]: ...
    def ltp(self, *instruments: str) -> dict[str, Any]: ...
    def instruments(self, exchange: str | None = ...) -> list[dict[str, Any]]: ...


class KiteBrokerConfig(BaseModel):
    """Retry/batching tuning for the Kite broker. The per-request socket
    timeout is configured on the injected KiteConnect client itself (Kite has
    no per-call timeout); the values here govern only this broker's own
    read-retry loop and quote batching."""

    model_config = ConfigDict(frozen=True)

    read_retry_attempts: int = Field(
        default=3, gt=0,
        description="Max attempts for a read-only call before giving up. Only "
        "read-only calls retry; place/modify/cancel never do.",
    )
    read_retry_delay_seconds: float = Field(
        default=0.5, ge=0,
        description="Delay between read-only retry attempts (0 = retry immediately).",
    )
    quote_batch_size: int = Field(
        default=200, gt=0,
        description="Max instruments per quote()/ltp() call; larger requests are "
        "chunked and merged so callers never see the broker's per-call cap.",
    )


class KiteBroker(BrokerBase):
    """``BrokerBase`` implemented against Kite Connect. See the module docstring
    for the design contract."""

    def __init__(
        self,
        *,
        client: KiteClientProtocol,
        session: KiteSession,
        order_stream: KiteOrderUpdateStream,
        config: KiteBrokerConfig | None = None,
        logger: logging.Logger | None = None,
        sleep: Callable[[float], None] = _time_module.sleep,
        today_provider: Callable[[], date] | None = None,
    ) -> None:
        self._client = client
        self._session = session
        self._order_stream = order_stream
        self._config = config or KiteBrokerConfig()
        self._logger = logger if logger is not None else logging.getLogger("algo.brokers.kite")
        self._sleep = sleep
        # Per-exchange instrument dump, auto-refreshed once per trading day (see
        # _instrument_rows). Kite re-lists weekly contracts (NIFTY/SENSEX) as
        # expiries roll, so a dump held for the whole process lifetime goes stale
        # and makes find_option_contract miss the new weekly contracts.
        self._instrument_cache: dict[str, list[dict[str, Any]]] = {}
        # The trading date each exchange's dump was loaded for; a lookup on a
        # later date triggers exactly one refresh for that exchange.
        self._instrument_cache_date: dict[str, date] = {}
        # Monotonic timestamp of each exchange's last (re)load -- surfaced in the
        # find_option_contract miss log as the dump's age.
        self._instrument_cache_loaded_at: dict[str, float] = {}
        # Serialises cache reads and the once-a-day refresh across the scheduler's
        # concurrent per-instrument dispatch threads (two instruments sharing an
        # exchange can look up simultaneously).
        self._instrument_cache_lock = threading.Lock()
        # "Trading day" source (injectable for tests); IST calendar date by
        # default, matching Position.trade_date's timezone.
        self._today_provider = today_provider if today_provider is not None else _ist_today

    # -- Lifecycle -----------------------------------------------------

    @property
    def broker_name(self) -> BrokerName:
        return BrokerName.KITE

    def authenticate(self, *, timeout: float | None = None) -> None:
        # Apply the day's token to the client, then validate it with a cheap
        # authenticated call. A TokenException here (expired/invalid token) is
        # translated to a non-retryable BrokerAuthenticationError -- recovery is
        # a fresh interactive login, not a retry.
        self._session.activate()
        try:
            self._client.profile()
        except Exception as exc:  # noqa: BLE001 -- translated immediately
            self._session.invalidate()
            raise mapper.translate_kite_exception(exc, mutating=False) from exc

    def is_authenticated(self) -> bool:
        return self._session.is_active

    def close(self) -> None:
        self._order_stream.disconnect()

    def health_check(self, *, timeout: float | None = None) -> HealthStatus:
        started = _time_module.monotonic()
        try:
            self._read(self._client.profile, timeout=timeout)
        except BrokerError as exc:
            return HealthStatus(
                healthy=False, checked_at=datetime.now(timezone.utc),
                latency_ms=(_time_module.monotonic() - started) * 1000, detail=str(exc),
            )
        return HealthStatus(
            healthy=True, checked_at=datetime.now(timezone.utc),
            latency_ms=(_time_module.monotonic() - started) * 1000,
        )

    # -- Orders (mutating: never retried) ------------------------------

    def place_order(
        self, request: PlaceOrderRequest, *, timeout: float | None = None
    ) -> PlaceOrderResult:
        kwargs = mapper.place_order_kwargs(request)
        try:
            order_id = self._client.place_order(**kwargs)
        except Exception as exc:  # noqa: BLE001 -- translated immediately
            raise mapper.translate_kite_exception(exc, mutating=True) from exc
        return PlaceOrderResult(broker_order_id=str(order_id), raw_response={"order_id": order_id})

    def modify_order(self, request: ModifyOrderRequest, *, timeout: float | None = None) -> None:
        kwargs = mapper.modify_order_kwargs(
            order_id=request.broker_order_id, quantity=request.quantity, price=request.price,
            trigger_price=request.trigger_price, order_type=request.order_type,
        )
        try:
            self._client.modify_order(**kwargs)
        except mapper.kite_exc.OrderException as exc:
            # An OrderException on modify most commonly means the order is no
            # longer modifiable (already terminal).
            raise OrderNotModifiableError(f"Kite could not modify order: {exc}") from exc
        except Exception as exc:  # noqa: BLE001
            raise mapper.translate_kite_exception(exc, mutating=True) from exc

    def cancel_order(self, broker_order_id: str, *, timeout: float | None = None) -> None:
        from kiteconnect import KiteConnect

        try:
            self._client.cancel_order(KiteConnect.VARIETY_REGULAR, broker_order_id)
        except mapper.kite_exc.OrderException as exc:
            # Cancelling an already-terminal order (e.g. it filled first) is an
            # expected condition, not a bug.
            raise OrderNotCancellableError(f"Kite could not cancel order: {exc}") from exc
        except Exception as exc:  # noqa: BLE001
            raise mapper.translate_kite_exception(exc, mutating=True) from exc

    # -- Orders (read-only: retried) -----------------------------------

    def get_order(self, broker_order_id: str, *, timeout: float | None = None) -> BrokerOrder:
        def _fetch() -> list[dict[str, Any]]:
            try:
                return self._client.order_history(broker_order_id)
            except Exception as exc:  # noqa: BLE001
                if mapper.is_order_not_found(exc):
                    raise OrderNotFoundError(f"no Kite order {broker_order_id}") from exc
                raise

        history = self._read(_fetch, timeout=timeout)
        if not history:
            raise OrderNotFoundError(f"no Kite order {broker_order_id}")
        # order_history returns states oldest-first; the last is the current one.
        return mapper.to_broker_order(history[-1])

    def get_orders(self, *, timeout: float | None = None) -> list[BrokerOrder]:
        raw = self._read(self._client.orders, timeout=timeout)
        return [mapper.to_broker_order(o) for o in raw]

    def find_order_by_tag(self, tag: str, *, timeout: float | None = None) -> BrokerOrder | None:
        # Kite has no server-side tag lookup, so scan today's orderbook. If more
        # than one order carries the tag (Kite does not enforce tag uniqueness),
        # return the most recently placed -- the ambiguity is surfaced, not
        # hidden, exactly as the simulation broker documents.
        matches = [o for o in self.get_orders(timeout=timeout) if o.tag == tag]
        if not matches:
            return None
        return max(matches, key=lambda o: o.placed_at or _MIN_AWARE)

    # -- Portfolio state -----------------------------------------------

    def get_positions(self, *, timeout: float | None = None) -> list[BrokerPosition]:
        raw = self._read(self._client.positions, timeout=timeout)
        net = raw.get("net", []) if isinstance(raw, dict) else []
        return [mapper.to_broker_position(p) for p in net]

    def get_holdings(self, *, timeout: float | None = None) -> list[BrokerHolding]:
        raw = self._read(self._client.holdings, timeout=timeout)
        return [mapper.to_broker_holding(h) for h in raw]

    def get_margins(self, *, timeout: float | None = None) -> BrokerMargins:
        raw = self._read(self._client.margins, timeout=timeout)
        # F&O margin lives under the 'equity' segment in Kite's model.
        block = raw.get("equity", raw) if isinstance(raw, dict) else {}
        return mapper.to_broker_margins("equity", block)

    # -- Market data (pull) --------------------------------------------

    def get_quote(
        self, instruments: list[InstrumentIdentifier], *, timeout: float | None = None
    ) -> dict[InstrumentIdentifier, BrokerQuote]:
        result: dict[InstrumentIdentifier, BrokerQuote] = {}
        for chunk in self._chunk(instruments):
            keys = [mapper.quote_key(i) for i in chunk]
            key_to_instrument = dict(zip(keys, chunk))
            raw = self._read(lambda keys=keys: self._client.quote(*keys), timeout=timeout)
            for key, data in raw.items():
                instrument = key_to_instrument.get(key)
                if instrument is not None:
                    result[instrument] = mapper.to_broker_quote(instrument, data)
        return result

    def get_ltp(
        self, instruments: list[InstrumentIdentifier], *, timeout: float | None = None
    ) -> dict[InstrumentIdentifier, Decimal]:
        result: dict[InstrumentIdentifier, Decimal] = {}
        for chunk in self._chunk(instruments):
            keys = [mapper.quote_key(i) for i in chunk]
            key_to_instrument = dict(zip(keys, chunk))
            raw = self._read(lambda keys=keys: self._client.ltp(*keys), timeout=timeout)
            for key, data in raw.items():
                instrument = key_to_instrument.get(key)
                price = mapper._decimal(data.get("last_price"))
                if instrument is not None and price is not None:
                    result[instrument] = price
        return result

    # -- Instrument lookup ---------------------------------------------

    def get_instrument(
        self, exchange: Exchange, tradingsymbol: str, *, timeout: float | None = None
    ) -> BrokerInstrument:
        rows = self._instrument_rows(exchange, timeout=timeout)
        for row in rows:
            if row.get("tradingsymbol") == tradingsymbol:
                return mapper.to_broker_instrument(row)
        raise InstrumentNotFoundError(
            f"no instrument {exchange.value}:{tradingsymbol} in Kite dump"
        )

    def find_option_contract(
        self, *, underlying: str, expiry: date, strike: Decimal, option_type: OptionType,
        exchange: Exchange, timeout: float | None = None,
    ) -> BrokerInstrument:
        rows = self._instrument_rows(exchange, timeout=timeout)
        option_value = option_type.value
        for row in rows:
            if (
                row.get("name") == underlying
                and row.get("instrument_type") == option_value
                and row.get("expiry") == expiry
                and mapper._price_or_none(row.get("strike")) == strike
            ):
                return mapper.to_broker_instrument(row)
        # No exact (name, instrument_type, expiry, strike) match was found.
        # Before failing loud (behaviour UNCHANGED -- the raise below always
        # runs), emit a diagnostic showing what the dump DID contain for this
        # underlying. The overwhelmingly common cause is a computed expiry that
        # does not match any LISTED contract -- especially for WEEKLY instruments
        # (NIFTY/SENSEX), whose weekly-expiry weekday the exchange changes and
        # which holiday-shift more often than monthlies. This log distinguishes
        # "requested expiry is not listed at all" (a wrong expiry weekday /
        # holiday-shift / stale dump) from "expiry is listed but the requested
        # strike is not" (an ATM/strike-interval problem), turning the next
        # freeze into an at-a-glance root cause. The try/except guards ONLY the
        # diagnostic so it can never mask or replace the real error below.
        try:
            listed_expiries = sorted(
                d for d in {
                    row.get("expiry")
                    for row in rows
                    if row.get("name") == underlying
                    and row.get("instrument_type") == option_value
                }
                if d is not None
            )
            strikes_at_requested_expiry = sorted(
                s for s in (
                    mapper._price_or_none(row.get("strike"))
                    for row in rows
                    if row.get("name") == underlying
                    and row.get("instrument_type") == option_value
                    and row.get("expiry") == expiry
                )
                if s is not None
            )
            kite_exchange = mapper.to_kite_exchange(exchange)
            loaded_at = self._instrument_cache_loaded_at.get(kite_exchange)
            cache_age_s = None if loaded_at is None else _time_module.monotonic() - loaded_at
            expiry_type = type(expiry).__name__
            listed_type = (
                type(listed_expiries[0]).__name__ if listed_expiries else "n/a"
            )
            self._logger.error(
                "find_option_contract MISS: underlying=%s type=%s exchange=%s "
                "requested expiry=%s (%s) strike=%s | dump_rows=%d cache_age=%s | "
                "listed expiries for %s %s (%s): %s | strikes listed AT requested expiry: %s",
                underlying, option_value, exchange.value, expiry, expiry_type, strike,
                len(rows),
                "unknown" if cache_age_s is None else f"{cache_age_s:.0f}s",
                underlying, option_value, listed_type,
                [d.isoformat() if hasattr(d, "isoformat") else d for d in listed_expiries],
                strikes_at_requested_expiry or "<none -- requested expiry is NOT listed at all>",
            )
        except Exception:  # noqa: BLE001 -- a diagnostic must never mask the real error below
            self._logger.error(
                "find_option_contract MISS: underlying=%s type=%s expiry=%s strike=%s on %s "
                "(diagnostic dump failed)",
                underlying, option_value, expiry, strike, exchange.value, exc_info=True,
            )
        raise InstrumentNotFoundError(
            f"no {option_value} contract for {underlying} strike={strike} expiry={expiry} "
            f"on {exchange.value}"
        )

    def list_option_expiries(
        self, *, underlying: str, exchange: Exchange, timeout: float | None = None
    ) -> list[date]:
        """Distinct, sorted option expiries currently listed for ``underlying``
        on ``exchange``, read straight from the (once-per-trading-day-refreshed)
        instrument dump -- the exchange's own source of truth.

        Used to validate a computed expiry against reality before requesting a
        contract for it (see services.live_seams.ValidatingExpiryService), so a
        stale expiry_weekday / holiday-shift / dump can never silently drive a
        lookup for a non-existent expiry. Scans the same rows and same name/
        instrument_type fields find_option_contract matches on, so the two agree
        by construction."""
        rows = self._instrument_rows(exchange, timeout=timeout)
        expiries = {
            row.get("expiry")
            for row in rows
            if row.get("name") == underlying
            and row.get("instrument_type") in (OptionType.CE.value, OptionType.PE.value)
            and row.get("expiry") is not None
        }
        return sorted(expiries)

    def refresh_instruments(self, exchange: Exchange, *, timeout: float | None = None) -> int:
        """Force a reload of the cached instrument dump for one exchange (Kite's
        instrument master changes daily -- e.g. new weekly expiries). Returns
        the number of instruments loaded. Called by scripts/sync_instruments.py.

        Unconditional (unlike the once-a-day auto-refresh in _instrument_rows),
        but shares the same store logic, so a manual refresh also stamps today's
        trading date and the auto-refresh will not re-fetch again that day."""
        kite_exchange = mapper.to_kite_exchange(exchange)
        with self._instrument_cache_lock:
            rows = self._fetch_and_store(kite_exchange, self._today(), timeout=timeout)
        return len(rows)

    # -- Websocket -----------------------------------------------------

    def connect_websocket(self, *, timeout: float | None = None) -> None:
        self._order_stream.connect()

    def disconnect_websocket(self) -> None:
        self._order_stream.disconnect()

    def is_websocket_connected(self) -> bool:
        return self._order_stream.is_connected()

    def register_order_update_callback(self, callback: OrderUpdateCallback) -> None:
        self._order_stream.register_callback(callback)

    # -- Internal ------------------------------------------------------

    def _instrument_rows(self, exchange: Exchange, *, timeout: float | None) -> list[dict[str, Any]]:
        """Return the cached raw instrument dump for one exchange, refreshing it
        at most once per trading day.

        Kite re-lists weekly option contracts as expiries roll, so a dump loaded
        yesterday no longer contains today's weekly (NIFTY/SENSEX) contracts; the
        first lookup on a new trading date therefore reloads. Every later lookup
        that same day hits the cache -- the refresh is once-a-day, never
        per-lookup. Refreshes are tracked independently per exchange.

        Failure policy: if the daily refresh fails but a previous dump exists,
        the failure is logged and the previous dump is returned (a stale dump is
        strictly better than freezing every instrument on a transient Kite
        outage); the load date is deliberately *not* advanced, so the next lookup
        retries the refresh. Only a failure with no usable cached dump at all
        propagates. The one-time cold load, and each retry, use the normal
        _read retry/timeout path unchanged.
        """
        kite_exchange = mapper.to_kite_exchange(exchange)
        today = self._today()
        with self._instrument_cache_lock:
            cached = self._instrument_cache.get(kite_exchange)
            if cached is not None and self._instrument_cache_date.get(kite_exchange) == today:
                return cached
            # A new trading day (or a never-loaded exchange): (re)load exactly once.
            try:
                return self._fetch_and_store(kite_exchange, today, timeout=timeout)
            except BrokerError:
                if cached is not None:
                    self._logger.warning(
                        "instrument dump refresh failed for %s; continuing with the "
                        "previous dump (loaded for %s) -- newly listed weekly contracts "
                        "may be missing until the next successful refresh",
                        kite_exchange,
                        self._instrument_cache_date.get(kite_exchange),
                        exc_info=True,
                    )
                    return cached
                # No usable dump exists at all: cannot serve lookups -- fail loud.
                raise

    def _fetch_and_store(
        self, kite_exchange: str, today: date, *, timeout: float | None
    ) -> list[dict[str, Any]]:
        """Fetch one exchange's instrument dump and store it with today's trading
        date. Caller must hold ``self._instrument_cache_lock``. Uses the normal
        ``_read`` retry/timeout path; on failure it raises (leaving any existing
        cache untouched) for the caller to decide fallback vs. propagate."""
        rows = list(self._read(lambda: self._client.instruments(kite_exchange), timeout=timeout))
        self._instrument_cache[kite_exchange] = rows
        self._instrument_cache_date[kite_exchange] = today
        self._instrument_cache_loaded_at[kite_exchange] = _time_module.monotonic()
        return rows

    def _today(self) -> date:
        """Current trading date (injected clock; IST calendar date by default)."""
        return self._today_provider()

    def _chunk(self, instruments: list[InstrumentIdentifier]):
        size = self._config.quote_batch_size
        for start in range(0, len(instruments), size):
            yield instruments[start : start + size]

    def _read(self, fn: Callable[[], _T], *, timeout: float | None) -> _T:
        """Execute a read-only Kite call, retrying on retryable (transient)
        errors up to the configured attempt count and within the caller's
        ``timeout`` deadline. Non-retryable errors (and our own already-typed
        BrokerErrors, e.g. OrderNotFoundError raised inside ``fn``) propagate
        immediately."""
        deadline = None if timeout is None else _time_module.monotonic() + timeout
        last_error: BrokerError | None = None
        for attempt in range(self._config.read_retry_attempts):
            try:
                return fn()
            except BrokerError as exc:
                # A semantic BrokerError raised inside fn (e.g. OrderNotFound):
                # do not translate again, and only retry if it is retryable.
                if not isinstance(exc, RetryableBrokerError):
                    raise
                last_error = exc
            except Exception as exc:  # noqa: BLE001 -- translate then decide
                translated = mapper.translate_kite_exception(exc, mutating=False)
                if not isinstance(translated, RetryableBrokerError):
                    raise translated from exc
                last_error = translated
            if attempt < self._config.read_retry_attempts - 1:
                if deadline is not None and _time_module.monotonic() + self._config.read_retry_delay_seconds > deadline:
                    break
                self._sleep(self._config.read_retry_delay_seconds)
        assert last_error is not None
        raise last_error
