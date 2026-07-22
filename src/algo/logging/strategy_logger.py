"""Human-readable observability logging for an open Strategy-1 position: a
periodic, formatted snapshot while a position is being monitored, a final
summary when it closes, and clear websocket connect/disconnect status
changes.

Purely observational -- ``PositionMonitorLogger`` reads already-known data
(plus, for the underlying spot price, a best-effort broker read made only
for display) and never influences any exit decision, order, or state
transition. ``PositionMonitor`` calls these methods strictly *after* its own
decision logic has already run and acted on it, and every method here
swallows its own exceptions -- a logging hiccup can never delay, alter, or
break the actual strategy. See ``monitor.py``'s module docstring for the
decision logic this module only ever describes, never makes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from algo.common.enums import ExitReason
from algo.common.utilities import pnl_per_share

if TYPE_CHECKING:
    import logging

    from algo.strategy_engine.strategy_context import (
        MarketDataGateway,
        SpotPriceProvider,
        StrategyIdentity,
        TimeProvider,
    )

__all__ = ["PositionMonitorLogger"]

_BAR = "=" * 50

#: Display label for each ExitReason -- TIMEOUT is this platform's name for
#: the hard end-of-day cutoff; "CUTOFF" is the human-readable term for it.
_REASON_LABEL = {
    ExitReason.TARGET: "TARGET",
    ExitReason.STOPLOSS: "STOPLOSS",
    ExitReason.TIMEOUT: "CUTOFF",
    ExitReason.KILL_SWITCH: "KILL_SWITCH",
    ExitReason.MANUAL: "MANUAL",
    ExitReason.ERROR: "ERROR",
}


def _fmt_money(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}"


def _fmt_pct(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"


def _fmt_price(value: Decimal | None) -> str:
    return f"{value:.2f}" if value is not None else "n/a"


def _fmt_holding(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"


@dataclass
class _PositionSnapshot:
    """Per-position bookkeeping the logger accumulates between attach() and
    close() -- reset fresh on every ``on_attach`` call."""

    position_id: int
    strike: Decimal | None
    expiry: date | None
    quantity: int | None
    call_symbol: str
    put_symbol: str
    entry_premium: Decimal | None
    entry_time: datetime | None
    lot_size: int | None = None

    cycle: int = 0
    last_log_at: datetime | None = None
    ws_connected_last: bool | None = None
    pnl_high: Decimal | None = None
    pnl_low: Decimal | None = None


class PositionMonitorLogger:
    """Formats and emits the detailed monitoring/close logs described in
    this module's docstring, onto an injected logger (the strategy's own
    tagged ``context.logger`` by convention, so these lines carry the same
    identity context every other strategy log line does).
    """

    def __init__(
        self,
        *,
        identity: StrategyIdentity,
        time_provider: TimeProvider,
        market_data: MarketDataGateway,
        spot_price_provider: SpotPriceProvider | None,
        logger: logging.Logger,
        min_log_interval_seconds: float = 3.0,
    ) -> None:
        self._identity = identity
        self._time = time_provider
        self._market_data = market_data
        self._spot_price_provider = spot_price_provider
        self._logger = logger
        self._min_interval = min_log_interval_seconds
        self._snapshot: _PositionSnapshot | None = None

    # -- Lifecycle -----------------------------------------------------

    def on_attach(
        self,
        *,
        position_id: int,
        strike: Decimal | None,
        expiry: date | None,
        quantity: int | None,
        call_symbol: str,
        put_symbol: str,
        entry_premium: Decimal | None,
        entry_time: datetime | None,
        lot_size: int | None = None,
    ) -> None:
        """Reset per-position tracking -- called once, right after
        ``PositionMonitor.attach()`` confirms it is watching a position."""
        try:
            self._snapshot = _PositionSnapshot(
                position_id=position_id, strike=strike, expiry=expiry, quantity=quantity,
                call_symbol=call_symbol, put_symbol=put_symbol,
                entry_premium=entry_premium, entry_time=entry_time, lot_size=lot_size,
                ws_connected_last=self._safe_is_connected(),
            )
        except Exception:  # noqa: BLE001 -- observability must never break attach()
            self._logger.debug("PositionMonitorLogger.on_attach failed", exc_info=True)

    # -- Per-cycle -------------------------------------------------------

    def on_cycle(
        self,
        *,
        combined_premium: Decimal | None,
        call_price: Decimal | None,
        put_price: Decimal | None,
        target_premium: Decimal | None,
        stoploss_premium: Decimal | None,
        last_tick_at: datetime | None,
        price_source: str,
    ) -> None:
        """Called on every monitor evaluation (tick or poll). Cheap
        websocket-status transitions are checked and logged every call;
        the full detailed block is throttled to at most once per
        ``min_log_interval_seconds`` so a busy tick feed cannot spam the log.
        """
        snap = self._snapshot
        if snap is None:
            return
        try:
            snap.cycle += 1
            self._log_websocket_transition(snap)
            self._track_pnl(snap, self._pnl(snap, combined_premium))

            now = self._time.now_ist()
            if snap.last_log_at is not None:
                elapsed = (now - snap.last_log_at).total_seconds()
                if elapsed < self._min_interval:
                    return
            snap.last_log_at = now

            self._log_cycle_block(
                snap, now=now, combined_premium=combined_premium,
                call_price=call_price, put_price=put_price,
                target_premium=target_premium, stoploss_premium=stoploss_premium,
                last_tick_at=last_tick_at, price_source=price_source,
            )
        except Exception:  # noqa: BLE001 -- observability must never break monitoring
            self._logger.debug("PositionMonitorLogger.on_cycle failed", exc_info=True)

    # -- Close -------------------------------------------------------------

    def on_close(
        self,
        *,
        reason: ExitReason,
        exit_premium: Decimal | None,
        exit_time: datetime | None,
        realized_pnl: Decimal | None,
    ) -> None:
        """Called once, right after ``ExitLogic.exit()`` returns."""
        snap = self._snapshot
        if snap is None:
            return
        try:
            self._track_pnl(snap, self._pnl(snap, exit_premium))
            holding = (
                _fmt_holding(exit_time - snap.entry_time)
                if exit_time is not None and snap.entry_time is not None
                else "n/a"
            )
            lines = [
                "",
                _BAR,
                "POSITION CLOSED",
                "",
                f"Strategy    : {self._identity.strategy_id}/{self._identity.instrument}",
                f"Position ID : {snap.position_id}",
                "",
                f"Reason : {_REASON_LABEL.get(reason, reason.value)}",
                "",
                f"Entry Time : {snap.entry_time}",
                f"Exit Time  : {exit_time}",
                "",
                f"Entry Premium : {_fmt_price(snap.entry_premium)}",
                f"Exit Premium  : {_fmt_price(exit_premium)}",
                "",
                f"Maximum Profit Seen (Rs) : {_fmt_money(snap.pnl_high)}",
                f"Maximum Loss Seen (Rs)   : {_fmt_money(snap.pnl_low)}",
                "",
                f"Total P&L (Rs) : {_fmt_money(realized_pnl)}",
                f"P&L Per Share (Rs) : {_fmt_money(pnl_per_share(realized_pnl, snap.lot_size))}",
                "",
                f"Holding Time : {holding}",
                _BAR,
            ]
            self._logger.info("\n".join(lines))
        except Exception:  # noqa: BLE001 -- observability must never break exit handling
            self._logger.debug("PositionMonitorLogger.on_close failed", exc_info=True)
        finally:
            self._snapshot = None

    # -- Internal --------------------------------------------------------

    def _log_cycle_block(
        self, snap: _PositionSnapshot, *, now: datetime, combined_premium: Decimal | None,
        call_price: Decimal | None, put_price: Decimal | None,
        target_premium: Decimal | None, stoploss_premium: Decimal | None,
        last_tick_at: datetime | None, price_source: str,
    ) -> None:
        spot = self._safe_spot_ltp()
        pnl = self._pnl(snap, combined_premium)
        pnl_pct = self._pnl_pct(snap, pnl)
        target_hit = (
            "YES" if target_premium is not None and combined_premium is not None
            and combined_premium <= target_premium else "NO"
        )
        stoploss_hit = (
            "YES" if stoploss_premium is not None and combined_premium is not None
            and combined_premium >= stoploss_premium else "NO"
        )
        connected = self._safe_is_connected()

        lines = [
            "",
            _BAR,
            f"Strategy : {self._identity.strategy_id}/{self._identity.instrument}",
            f"Time     : {now.strftime('%H:%M:%S')}",
            f"Cycle    : #{snap.cycle}",
            "",
            f"Position ID : {snap.position_id}",
            f"Strike      : {_fmt_price(snap.strike)}",
            f"Expiry      : {snap.expiry}",
            f"Quantity    : {snap.quantity}",
            "",
            f"Underlying Spot : {_fmt_price(spot)}",
            "",
            "CALL",
            f"  Symbol : {snap.call_symbol}",
            f"  LTP    : {_fmt_price(call_price)}",
            "",
            "PUT",
            f"  Symbol : {snap.put_symbol}",
            f"  LTP    : {_fmt_price(put_price)}",
            "",
            f"Combined Premium : {_fmt_price(combined_premium)}",
            f"Entry Premium    : {_fmt_price(snap.entry_premium)}",
            "",
            f"Target Premium   : {_fmt_price(target_premium)}",
            f"Stoploss Premium : {_fmt_price(stoploss_premium)}",
            "",
            f"Current P&L (Rs) : {_fmt_money(pnl)}",
            f"Current P&L (%)  : {_fmt_pct(pnl_pct)}",
            "",
            f"Target Hit       : {target_hit}",
            f"Stoploss Hit     : {stoploss_hit}",
            "",
            f"Last Tick        : {last_tick_at if last_tick_at is not None else 'never'}",
            f"WebSocket Status : {'CONNECTED' if connected else 'DISCONNECTED'}",
            f"Price Source     : {price_source}",
            _BAR,
        ]
        self._logger.info("\n".join(lines))

    def _log_websocket_transition(self, snap: _PositionSnapshot) -> None:
        connected = self._safe_is_connected()
        if snap.ws_connected_last is None:
            snap.ws_connected_last = connected
            return
        if connected == snap.ws_connected_last:
            return
        if connected:
            self._logger.info("LIVE MARKET DATA RESTORED")
        else:
            self._logger.warning("LIVE MARKET DATA LOST\nSwitching to polling fallback...")
        snap.ws_connected_last = connected

    def _track_pnl(self, snap: _PositionSnapshot, pnl: Decimal | None) -> None:
        if pnl is None:
            return
        snap.pnl_high = pnl if snap.pnl_high is None else max(snap.pnl_high, pnl)
        snap.pnl_low = pnl if snap.pnl_low is None else min(snap.pnl_low, pnl)

    def _pnl(self, snap: _PositionSnapshot, combined_premium: Decimal | None) -> Decimal | None:
        # Short straddle: profit realizes as the combined premium falls
        # below the entry premium -- mirrors combined_premium.py's own
        # target/stoploss direction, for display only.
        if snap.entry_premium is None or combined_premium is None or snap.quantity is None:
            return None
        return (snap.entry_premium - combined_premium) * snap.quantity

    def _pnl_pct(self, snap: _PositionSnapshot, pnl: Decimal | None) -> Decimal | None:
        if pnl is None or snap.entry_premium is None or snap.quantity is None or snap.entry_premium == 0:
            return None
        return (pnl / (snap.entry_premium * snap.quantity)) * 100

    def _safe_spot_ltp(self) -> Decimal | None:
        if self._spot_price_provider is None:
            return None
        try:
            return self._spot_price_provider.get_spot_ltp(self._identity.instrument)
        except Exception:  # noqa: BLE001 -- display-only; never worth failing a log line over
            return None

    def _safe_is_connected(self) -> bool:
        try:
            return bool(self._market_data.is_connected())
        except Exception:  # noqa: BLE001
            return False
