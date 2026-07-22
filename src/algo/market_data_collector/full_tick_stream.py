"""CollectorTickStream: the collector's OWN Kite websocket, in FULL/QUOTE/LTP
mode, mapping every raw tick to a rich ``MarketTick``.

This is a second, independent KiteTicker connection (Kite allows several per
api_key) -- entirely separate from the trading feed's LTP-only ``KiteTickStream``.
It is built with the same construction pattern as
``services/live_seams.build_kite_tick_stream`` (``KiteTicker(api_key, token)``)
but subscribes in the configured mode and captures the full tick shape (OHLC,
volume, OI, buy/sell qty, 5-level depth) instead of just last price.

Subscriptions are managed in **token space** (the ATM window already resolves
tokens), and the socket callback runs on KiteTicker's own thread. Per the hard
requirement, the tick handler this adapter calls does nothing but hand the
``MarketTick`` onward (the orchestrator enqueues it) -- the socket thread never
touches the database.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from algo.market_data_collector.db import MarketTick


class KiteMarketTickerProtocol(Protocol):
    on_ticks: Any
    on_connect: Any
    on_close: Any
    on_error: Any
    on_reconnect: Any
    on_noreconnect: Any

    def connect(self, threaded: bool = ..., **kwargs: Any) -> None: ...
    def close(self, *args: Any, **kwargs: Any) -> None: ...
    def subscribe(self, tokens: list[int]) -> None: ...
    def unsubscribe(self, tokens: list[int]) -> None: ...
    def set_mode(self, mode: Any, tokens: list[int]) -> None: ...


TickerFactory = Callable[[], KiteMarketTickerProtocol]


def _dec(v: Any) -> Decimal | None:
    return None if v is None else Decimal(str(v))


class CollectorTickStream:
    """Independent FULL-mode market-data websocket for the collector."""

    def __init__(
        self,
        *,
        ticker_factory: TickerFactory,
        mode: str = "FULL",
        on_tick: Callable[[MarketTick], None],
        on_connect: Callable[[], None] | None = None,
        on_disconnect: Callable[[], None] | None = None,
        on_reconnect: Callable[[], None] | None = None,
        on_noreconnect: Callable[[], None] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._ticker_factory = ticker_factory
        self._mode = mode.upper()
        self._on_tick = on_tick
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect
        self._on_reconnect = on_reconnect
        self._on_noreconnect = on_noreconnect
        self._logger = logger if logger is not None else logging.getLogger("algo.collector.ticks")

        self._lock = threading.Lock()
        self._ticker: KiteMarketTickerProtocol | None = None
        self._connected = False
        self._subscribed: set[int] = set()
        # Set (on the ticker thread) when KiteTicker permanently gives up
        # reconnecting; read by the watchdog to trigger an immediate rebuild.
        self._dead = threading.Event()

    # -- Lifecycle -----------------------------------------------------

    def start(self) -> None:
        with self._lock:
            if self._ticker is not None:
                return
            self._spawn_locked()

    def restart(self) -> None:
        """Destroy the current (dead/frozen) websocket and build a BRAND-NEW one.

        The old KiteTicker is never reused: it is closed and discarded, then the
        factory constructs a fresh KiteTicker (which re-reads the access token,
        i.e. re-authenticates). The intended subscription set is deliberately
        preserved so the new socket re-subscribes every instrument on connect.
        """
        with self._lock:
            old = self._ticker
            self._ticker = None
            self._connected = False  # never expose the dead socket as connected
        if old is not None:
            try:
                old.close()
            except Exception:  # noqa: BLE001 -- tearing down a dead socket must not raise
                self._logger.warning("error closing dead collector ticker", exc_info=True)
        with self._lock:
            self._logger.info("creating fresh KiteTicker (re-authenticating)")
            self._spawn_locked()  # keeps self._subscribed intact

    def _spawn_locked(self) -> None:
        """Build + wire + connect a new ticker. Caller must hold self._lock."""
        self._dead.clear()
        ticker = self._ticker_factory()
        ticker.on_ticks = self._handle_ticks
        ticker.on_connect = self._handle_connect
        ticker.on_close = self._handle_close
        ticker.on_error = self._handle_error
        ticker.on_reconnect = self._handle_reconnect
        ticker.on_noreconnect = self._handle_noreconnect
        self._ticker = ticker
        ticker.connect(threaded=True)

    def stop(self) -> None:
        with self._lock:
            ticker = self._ticker
            self._ticker = None
            self._connected = False
            self._subscribed.clear()
            self._dead.clear()
        if ticker is not None:
            try:
                ticker.close()
            except Exception:  # noqa: BLE001 -- teardown must not raise
                self._logger.warning("error closing collector ticker", exc_info=True)

    def is_connected(self) -> bool:
        return self._connected

    def is_dead(self) -> bool:
        """True once KiteTicker has permanently stopped reconnecting."""
        return self._dead.is_set()

    @property
    def subscribed_count(self) -> int:
        with self._lock:
            return len(self._subscribed)

    # -- Subscriptions (token space) -----------------------------------

    def subscribe(self, tokens: list[int]) -> None:
        tokens = [int(t) for t in tokens]
        if not tokens:
            return
        with self._lock:
            ticker = self._ticker
            self._subscribed.update(tokens)
        if ticker is not None and self._connected:
            ticker.subscribe(tokens)
            ticker.set_mode(self._mode_const(ticker), tokens)

    def unsubscribe(self, tokens: list[int]) -> None:
        tokens = [int(t) for t in tokens]
        if not tokens:
            return
        with self._lock:
            ticker = self._ticker
            self._subscribed.difference_update(tokens)
        if ticker is not None and self._connected:
            ticker.unsubscribe(tokens)

    # -- KiteTicker callbacks (ticker thread) --------------------------

    def _handle_ticks(self, ws: Any, ticks: list[dict[str, Any]]) -> None:
        handler = self._on_tick
        for raw in ticks:
            try:
                tick = self._to_market_tick(raw)
            except Exception:  # noqa: BLE001 -- one bad tick must not kill the thread
                self._logger.warning("dropping unmappable collector tick: %r", raw, exc_info=True)
                continue
            if tick is not None:
                handler(tick)  # ENQUEUE ONLY -- never blocks on I/O

    def _handle_connect(self, ws: Any, response: Any) -> None:
        self._connected = True
        self._dead.clear()  # a successful connect clears any prior give-up state
        self._logger.info("collector ticker connected")
        with self._lock:
            ticker = self._ticker
            tokens = list(self._subscribed)
        if ticker is not None and tokens:
            ticker.subscribe(tokens)
            ticker.set_mode(self._mode_const(ticker), tokens)
            self._logger.info("collector subscribed %d instruments", len(tokens))
        if self._on_connect is not None:
            self._on_connect()

    def _handle_reconnect(self, ws: Any, attempts: Any) -> None:
        self._logger.info("collector ticker reconnect attempt #%s", attempts)
        if self._on_reconnect is not None:
            self._on_reconnect()

    def _handle_noreconnect(self, ws: Any = None) -> None:
        """KiteTicker has exhausted its reconnect budget and stopped forever.
        Flag the socket dead so the watchdog rebuilds it immediately instead of
        waiting out the grace window."""
        self._connected = False
        self._dead.set()
        self._logger.critical(
            "collector ticker gave up reconnecting (KiteTicker on_noreconnect) -- socket is dead"
        )
        if self._on_noreconnect is not None:
            self._on_noreconnect()

    def _handle_close(self, ws: Any, code: Any, reason: Any) -> None:
        self._connected = False
        self._logger.info("collector ticker closed (network disconnected): %s %s", code, reason)
        if self._on_disconnect is not None:
            self._on_disconnect()

    def _handle_error(self, ws: Any, code: Any, reason: Any) -> None:
        self._logger.warning("collector ticker error: %s %s", code, reason)

    # -- Mapping -------------------------------------------------------

    def _to_market_tick(self, raw: dict[str, Any]) -> MarketTick | None:
        token = raw.get("instrument_token")
        if token is None:
            return None
        ohlc = raw.get("ohlc") or {}
        ts = raw.get("exchange_timestamp") or raw.get("last_trade_time")
        return MarketTick(
            instrument_token=int(token),
            time=ts if isinstance(ts, datetime) else datetime.now(timezone.utc),
            last_price=_dec(raw.get("last_price")),
            last_traded_qty=raw.get("last_traded_quantity"),
            avg_traded_price=_dec(raw.get("average_traded_price")),
            volume=raw.get("volume_traded", raw.get("volume")),
            oi=raw.get("oi"),
            oi_day_high=raw.get("oi_day_high"),
            oi_day_low=raw.get("oi_day_low"),
            ohlc_open=_dec(ohlc.get("open")),
            ohlc_high=_dec(ohlc.get("high")),
            ohlc_low=_dec(ohlc.get("low")),
            ohlc_close=_dec(ohlc.get("close")),
            total_buy_qty=raw.get("total_buy_quantity"),
            total_sell_qty=raw.get("total_sell_quantity"),
            depth=raw.get("depth"),
        )

    def _mode_const(self, ticker: KiteMarketTickerProtocol) -> Any:
        return getattr(ticker, f"MODE_{self._mode}", self._mode.lower())
