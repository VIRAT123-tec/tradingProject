"""KiteTickStream: the live market-data ``TickStream`` backed by KiteTicker.

This is the low-latency push feed for option-leg prices, satisfying the
broker-agnostic ``TickStream`` seam ``MarketDataService`` consumes. All
Kite-specific knowledge (the KiteTicker socket, instrument-token subscription,
raw-tick shape) is confined here, per the "no broker-specific logic outside the
Kite implementation" rule.

Like ``KiteOrderUpdateStream``, the ticker is dependency-injected via a factory
and the token<->instrument mapping via callables, so the whole adapter is
unit-testable with a fake ticker and fake maps -- no network. KiteTicker runs
its socket on its own thread and invokes the callbacks from there; this adapter
holds a lock around its mutable subscription/handler state and never invokes a
service handler while holding it.

⚠️ This adapter has been unit-tested against a fake ticker but not validated
against a live Kite market socket. The platform does not depend on it for
correctness -- ``PollingTickStream`` (the default) keeps stop-loss/target and
the kill switch working via polling -- so this is the opt-in low-latency path,
to be enabled once validated live.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from algo.brokers.broker_base import InstrumentIdentifier
from algo.strategy_engine.strategy_context import Tick


class KiteMarketTickerProtocol(Protocol):
    """The subset of KiteTicker this adapter drives (declared so a fake can be
    injected in tests)."""

    on_ticks: Any
    on_connect: Any
    on_close: Any
    on_error: Any
    on_reconnect: Any

    def connect(self, threaded: bool = ..., **kwargs: Any) -> None: ...
    def close(self, *args: Any, **kwargs: Any) -> None: ...
    def subscribe(self, tokens: list[int]) -> None: ...
    def unsubscribe(self, tokens: list[int]) -> None: ...
    def set_mode(self, mode: Any, tokens: list[int]) -> None: ...


KiteMarketTickerFactory = Callable[[], KiteMarketTickerProtocol]
TokenForInstrument = Callable[[InstrumentIdentifier], int]
InstrumentForToken = Callable[[int], "InstrumentIdentifier | None"]


class KiteTickStream:
    """Live market-tick ``TickStream`` over KiteTicker."""

    def __init__(
        self,
        *,
        ticker_factory: KiteMarketTickerFactory,
        token_for_instrument: TokenForInstrument,
        instrument_for_token: InstrumentForToken,
        logger: logging.Logger | None = None,
    ) -> None:
        self._ticker_factory = ticker_factory
        self._token_for = token_for_instrument
        self._instrument_for = instrument_for_token
        self._logger = logger if logger is not None else logging.getLogger("algo.brokers.kite.ticks")

        self._lock = threading.Lock()
        self._ticker: KiteMarketTickerProtocol | None = None
        self._connected = False
        self._subscribed_tokens: set[int] = set()

        self._on_tick: Callable[[Tick], None] | None = None
        self._on_connect: Callable[[], None] | None = None
        self._on_disconnect: Callable[[], None] | None = None
        self._on_reconnect: Callable[[], None] | None = None

    # -- TickStream seam ------------------------------------------------

    def set_handlers(
        self,
        *,
        on_tick: Callable[[Tick], None],
        on_connect: Callable[[], None],
        on_disconnect: Callable[[], None],
        on_reconnect: Callable[[], None],
    ) -> None:
        self._on_tick = on_tick
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect
        self._on_reconnect = on_reconnect

    def start(self) -> None:
        with self._lock:
            if self._ticker is not None:
                return
            ticker = self._ticker_factory()
            ticker.on_ticks = self._handle_ticks
            ticker.on_connect = self._handle_connect
            ticker.on_close = self._handle_close
            ticker.on_error = self._handle_error
            ticker.on_reconnect = self._handle_reconnect
            self._ticker = ticker
            ticker.connect(threaded=True)

    def stop(self) -> None:
        with self._lock:
            ticker = self._ticker
            self._ticker = None
            self._connected = False
            self._subscribed_tokens.clear()
        if ticker is not None:
            try:
                ticker.close()
            except Exception:  # noqa: BLE001 -- teardown must not raise
                self._logger.warning("error closing Kite market ticker", exc_info=True)

    def is_connected(self) -> bool:
        return self._connected

    def subscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        tokens = self._resolve_tokens(instruments)
        if not tokens:
            return
        with self._lock:
            ticker = self._ticker
            self._subscribed_tokens.update(tokens)
        if ticker is not None:
            ticker.subscribe(tokens)
            ticker.set_mode(self._ltp_mode(ticker), tokens)

    def unsubscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        tokens = self._resolve_tokens(instruments)
        if not tokens:
            return
        with self._lock:
            ticker = self._ticker
            self._subscribed_tokens.difference_update(tokens)
        if ticker is not None:
            ticker.unsubscribe(tokens)

    # -- KiteTicker callbacks (invoked on the ticker's thread) ----------

    def _handle_ticks(self, ws: Any, ticks: list[dict[str, Any]]) -> None:
        handler = self._on_tick
        if handler is None:
            return
        for raw in ticks:
            try:
                tick = self._to_tick(raw)
            except Exception:  # noqa: BLE001 -- one malformed tick must not kill the thread
                self._logger.warning("dropping unmappable Kite tick: %r", raw, exc_info=True)
                continue
            if tick is not None:
                handler(tick)

    def _handle_connect(self, ws: Any, response: Any) -> None:
        self._connected = True
        self._logger.info("Kite market ticker connected")
        # Re-apply subscriptions the socket forgot across a (re)connect.
        with self._lock:
            ticker = self._ticker
            tokens = list(self._subscribed_tokens)
        if ticker is not None and tokens:
            ticker.subscribe(tokens)
            ticker.set_mode(self._ltp_mode(ticker), tokens)
        if self._on_connect is not None:
            self._on_connect()

    def _handle_reconnect(self, ws: Any, attempts: Any) -> None:
        self._logger.info("Kite market ticker reconnecting (attempt %s)", attempts)
        if self._on_reconnect is not None:
            self._on_reconnect()

    def _handle_close(self, ws: Any, code: Any, reason: Any) -> None:
        self._connected = False
        self._logger.info("Kite market ticker closed: %s %s", code, reason)
        if self._on_disconnect is not None:
            self._on_disconnect()

    def _handle_error(self, ws: Any, code: Any, reason: Any) -> None:
        self._logger.warning("Kite market ticker error: %s %s", code, reason)

    # -- Internal ------------------------------------------------------

    def _resolve_tokens(self, instruments: list[InstrumentIdentifier]) -> list[int]:
        tokens: list[int] = []
        for ident in instruments:
            try:
                tokens.append(self._token_for(ident))
            except Exception:  # noqa: BLE001 -- an unmappable instrument is skipped, not fatal
                self._logger.warning("no instrument token for %s; not subscribing", ident, exc_info=True)
        return tokens

    def _to_tick(self, raw: dict[str, Any]) -> Tick | None:
        token = raw.get("instrument_token")
        if token is None:
            return None
        ident = self._instrument_for(int(token))
        if ident is None:
            return None
        last_price = raw.get("last_price")
        if last_price is None:
            return None
        ts = raw.get("exchange_timestamp") or raw.get("last_trade_time")
        timestamp = ts if isinstance(ts, datetime) else datetime.now(timezone.utc)
        return Tick(instrument=ident, last_price=Decimal(str(last_price)), timestamp=timestamp)

    @staticmethod
    def _ltp_mode(ticker: KiteMarketTickerProtocol) -> Any:
        # We only need last-traded price; MODE_LTP is the cheapest subscription.
        return getattr(ticker, "MODE_LTP", "ltp")
