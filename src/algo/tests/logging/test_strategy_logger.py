"""Tests for PositionMonitorLogger -- the observability logging described in
strategy_logger.py's own module docstring. Exercised against a real
logging.Logger with a capturing handler (the same pattern
test_alerting.py uses), never mocked, so the actual formatted output is
verified, not just that a call happened.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from algo.common.enums import ExitReason
from algo.logging.strategy_logger import PositionMonitorLogger

_IST = timezone(timedelta(hours=5, minutes=30))


class _CapturingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def captured():
    logger = logging.getLogger("test.strategy_logger")
    logger.setLevel(logging.DEBUG)
    handler = _CapturingHandler()
    logger.addHandler(handler)
    try:
        yield logger, handler
    finally:
        logger.removeHandler(handler)


@dataclass
class _FakeIdentity:
    strategy_id: str = "strategy_1"
    instrument: str = "NIFTY"


@dataclass
class _FakeTime:
    now_value: datetime = field(default_factory=lambda: datetime(2026, 7, 10, 13, 35, 12, tzinfo=_IST))

    def now_ist(self) -> datetime:
        return self.now_value

    def advance(self, seconds: float) -> None:
        self.now_value = self.now_value + timedelta(seconds=seconds)


@dataclass
class _FakeMarketData:
    connected: bool = True

    def is_connected(self) -> bool:
        return self.connected


@dataclass
class _FakeSpotProvider:
    price: Decimal = Decimal("24234.55")
    should_fail: bool = False

    def get_spot_ltp(self, instrument: str) -> Decimal:
        if self.should_fail:
            raise RuntimeError("no spot price available")
        return self.price


def _messages(handler: _CapturingHandler) -> list[str]:
    return [r.getMessage() for r in handler.records]


_DEFAULT_SPOT = object()  # sentinel: distinguishes "use the default fake" from an explicit None


def _build(captured, *, market_data=None, spot=_DEFAULT_SPOT, min_interval=3.0):
    logger, handler = captured
    time_provider = _FakeTime()
    obs = PositionMonitorLogger(
        identity=_FakeIdentity(),
        time_provider=time_provider,
        market_data=market_data or _FakeMarketData(),
        spot_price_provider=_FakeSpotProvider() if spot is _DEFAULT_SPOT else spot,
        logger=logger,
        min_log_interval_seconds=min_interval,
    )
    return obs, time_provider, handler


def _attach(obs, *, entry_premium=Decimal("208.80"), quantity=75, lot_size=75) -> None:
    obs.on_attach(
        position_id=4, strike=Decimal("24200"), expiry=date(2026, 7, 16), quantity=quantity,
        call_symbol="NIFTY2671424200CE", put_symbol="NIFTY2671424200PE",
        entry_premium=entry_premium, entry_time=datetime(2026, 7, 10, 11, 25, 3, tzinfo=_IST),
        lot_size=lot_size,
    )


class TestOnCycleDetailedBlock:
    def test_first_cycle_logs_the_full_block_with_expected_fields(self, captured):
        obs, _time, handler = _build(captured)
        _attach(obs)

        obs.on_cycle(
            combined_premium=Decimal("198.75"), call_price=Decimal("102.45"), put_price=Decimal("96.30"),
            target_premium=Decimal("187.92"), stoploss_premium=Decimal("229.68"),
            last_tick_at=datetime(2026, 7, 10, 13, 35, 10, tzinfo=_IST),
            price_source="LIVE KITE WEBSOCKET",
        )

        assert len(handler.records) == 1
        text = handler.records[0].getMessage()
        assert "Strategy : strategy_1/NIFTY" in text
        assert "Position ID : 4" in text
        assert "Strike      : 24200.00" in text
        assert "Expiry      : 2026-07-16" in text
        assert "Quantity    : 75" in text
        assert "Underlying Spot : 24234.55" in text
        assert "NIFTY2671424200CE" in text
        assert "LTP    : 102.45" in text
        assert "NIFTY2671424200PE" in text
        assert "LTP    : 96.30" in text
        assert "Combined Premium : 198.75" in text
        assert "Entry Premium    : 208.80" in text
        assert "Target Premium   : 187.92" in text
        assert "Stoploss Premium : 229.68" in text
        assert "Target Hit       : NO" in text
        assert "Stoploss Hit     : NO" in text
        assert "WebSocket Status : CONNECTED" in text
        assert "Price Source     : LIVE KITE WEBSOCKET" in text
        assert "Cycle    : #1" in text

    def test_pnl_computed_correctly_for_a_short_straddle(self, captured):
        # Profit realizes as combined premium falls below entry premium.
        obs, _time, handler = _build(captured)
        _attach(obs, entry_premium=Decimal("208.80"), quantity=75)

        obs.on_cycle(
            combined_premium=Decimal("200.13"), call_price=Decimal("100"), put_price=Decimal("100.13"),
            target_premium=Decimal("187.92"), stoploss_premium=Decimal("229.68"),
            last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )
        text = handler.records[0].getMessage()
        # (208.80 - 200.13) * 75 = 650.25
        assert "Current P&L (Rs) : +650.25" in text

    def test_target_hit_flag_matches_exit_logic_comparison(self, captured):
        obs, _time, handler = _build(captured)
        _attach(obs)
        obs.on_cycle(
            combined_premium=Decimal("187.92"),  # exactly at target -- exit_logic uses <=
            call_price=Decimal("90"), put_price=Decimal("97.92"),
            target_premium=Decimal("187.92"), stoploss_premium=Decimal("229.68"),
            last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )
        text = handler.records[0].getMessage()
        assert "Target Hit       : YES" in text
        assert "Stoploss Hit     : NO" in text

    def test_stoploss_hit_flag_matches_exit_logic_comparison(self, captured):
        obs, _time, handler = _build(captured)
        _attach(obs)
        obs.on_cycle(
            combined_premium=Decimal("229.68"),  # exactly at stoploss -- exit_logic uses >=
            call_price=Decimal("120"), put_price=Decimal("109.68"),
            target_premium=Decimal("187.92"), stoploss_premium=Decimal("229.68"),
            last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )
        text = handler.records[0].getMessage()
        assert "Stoploss Hit     : YES" in text

    def test_throttles_rapid_cycles_but_still_counts_them(self, captured):
        obs, time_provider, handler = _build(captured, min_interval=3.0)
        _attach(obs)

        for _ in range(5):
            obs.on_cycle(
                combined_premium=Decimal("198.75"), call_price=Decimal("100"), put_price=Decimal("98.75"),
                target_premium=Decimal("187.92"), stoploss_premium=Decimal("229.68"),
                last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
            )
            time_provider.advance(0.1)  # well under the 3s throttle

        # Only the first cycle actually printed the block.
        assert len(handler.records) == 1
        assert "Cycle    : #1" in handler.records[0].getMessage()

        time_provider.advance(3.0)
        obs.on_cycle(
            combined_premium=Decimal("198.75"), call_price=Decimal("100"), put_price=Decimal("98.75"),
            target_premium=Decimal("187.92"), stoploss_premium=Decimal("229.68"),
            last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )
        assert len(handler.records) == 2
        # The 6th on_cycle() call overall (5 in the loop + this one).
        assert "Cycle    : #6" in handler.records[1].getMessage()

    def test_no_snapshot_is_a_silent_no_op(self, captured):
        obs, _time, handler = _build(captured)
        # No on_attach() call.
        obs.on_cycle(
            combined_premium=Decimal("1"), call_price=Decimal("1"), put_price=Decimal("1"),
            target_premium=None, stoploss_premium=None, last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )
        assert handler.records == []

    def test_spot_price_failure_does_not_break_the_cycle_log(self, captured):
        obs, _time, handler = _build(captured, spot=_FakeSpotProvider(should_fail=True))
        _attach(obs)
        obs.on_cycle(
            combined_premium=Decimal("198.75"), call_price=Decimal("100"), put_price=Decimal("98.75"),
            target_premium=Decimal("187.92"), stoploss_premium=Decimal("229.68"),
            last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )
        assert len(handler.records) == 1
        assert "Underlying Spot : n/a" in handler.records[0].getMessage()

    def test_none_spot_price_provider_shows_not_available(self, captured):
        obs, _time, handler = _build(captured, spot=None)
        _attach(obs)
        obs.on_cycle(
            combined_premium=Decimal("198.75"), call_price=Decimal("100"), put_price=Decimal("98.75"),
            target_premium=Decimal("187.92"), stoploss_premium=Decimal("229.68"),
            last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )
        assert "Underlying Spot : n/a" in handler.records[0].getMessage()


class TestWebsocketTransitions:
    def test_disconnect_logs_lost_message_exactly_once(self, captured):
        market_data = _FakeMarketData(connected=True)
        obs, time_provider, handler = _build(captured, market_data=market_data, min_interval=1000.0)
        _attach(obs)

        obs.on_cycle(
            combined_premium=Decimal("198.75"), call_price=Decimal("100"), put_price=Decimal("98.75"),
            target_premium=None, stoploss_premium=None, last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )
        first_block_count = len(handler.records)

        market_data.connected = False
        obs.on_cycle(
            combined_premium=Decimal("198.75"), call_price=Decimal("100"), put_price=Decimal("98.75"),
            target_premium=None, stoploss_premium=None, last_tick_at=None, price_source="POLLING FALLBACK",
        )
        # Repeated disconnected cycles must not repeat the message.
        obs.on_cycle(
            combined_premium=Decimal("198.75"), call_price=Decimal("100"), put_price=Decimal("98.75"),
            target_premium=None, stoploss_premium=None, last_tick_at=None, price_source="POLLING FALLBACK",
        )

        lost_messages = [m for m in _messages(handler) if "LIVE MARKET DATA LOST" in m]
        assert len(lost_messages) == 1
        assert "Switching to polling fallback" in lost_messages[0]

    def test_reconnect_logs_restored_message_exactly_once(self, captured):
        market_data = _FakeMarketData(connected=False)
        obs, _time, handler = _build(captured, market_data=market_data, min_interval=1000.0)
        _attach(obs)

        obs.on_cycle(
            combined_premium=Decimal("1"), call_price=Decimal("1"), put_price=Decimal("1"),
            target_premium=None, stoploss_premium=None, last_tick_at=None, price_source="POLLING FALLBACK",
        )
        market_data.connected = True
        obs.on_cycle(
            combined_premium=Decimal("1"), call_price=Decimal("1"), put_price=Decimal("1"),
            target_premium=None, stoploss_premium=None, last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )
        obs.on_cycle(
            combined_premium=Decimal("1"), call_price=Decimal("1"), put_price=Decimal("1"),
            target_premium=None, stoploss_premium=None, last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )

        restored = [m for m in _messages(handler) if "LIVE MARKET DATA RESTORED" in m]
        assert len(restored) == 1


class TestOnClose:
    def test_logs_the_closed_summary_with_expected_fields(self, captured):
        obs, _time, handler = _build(captured)
        _attach(obs, entry_premium=Decimal("208.80"), quantity=75)

        obs.on_close(
            reason=ExitReason.TARGET,
            exit_premium=Decimal("187.50"),
            exit_time=datetime(2026, 7, 10, 13, 42, 18, tzinfo=_IST),
            realized_pnl=Decimal("1597.50"),
        )

        assert len(handler.records) == 1
        text = handler.records[0].getMessage()
        assert "POSITION CLOSED" in text
        assert "Position ID : 4" in text
        assert "Reason : TARGET" in text
        assert "Entry Premium : 208.80" in text
        assert "Exit Premium  : 187.50" in text
        assert "Total P&L (Rs) : +1597.50" in text
        # 1597.50 / 75 = 21.30 -- additive line, Total P&L unchanged above.
        assert "P&L Per Share (Rs) : +21.30" in text
        assert "Holding Time : 2h 17m 15s" in text

    def test_pnl_per_share_uses_lot_size(self, captured):
        # Validation example 2: lot_size 50, P&L -2500 -> -50.00.
        obs, _time, handler = _build(captured)
        _attach(obs, lot_size=50)
        obs.on_close(
            reason=ExitReason.STOPLOSS, exit_premium=Decimal("250"),
            exit_time=datetime(2026, 7, 10, 13, 0, 0, tzinfo=_IST), realized_pnl=Decimal("-2500"),
        )
        text = handler.records[-1].getMessage()
        assert "Total P&L (Rs) : -2500.00" in text  # existing line unchanged
        assert "P&L Per Share (Rs) : -50.00" in text

    def test_pnl_per_share_is_na_when_lot_size_unknown(self, captured):
        obs, _time, handler = _build(captured)
        _attach(obs, lot_size=None)  # unknown lot size -> n/a, never a bogus 0
        obs.on_close(
            reason=ExitReason.TARGET, exit_premium=Decimal("100"),
            exit_time=datetime(2026, 7, 10, 13, 0, 0, tzinfo=_IST), realized_pnl=Decimal("1000"),
        )
        assert "P&L Per Share (Rs) : n/a" in handler.records[-1].getMessage()

    def test_cutoff_reason_displays_as_cutoff_not_timeout(self, captured):
        obs, _time, handler = _build(captured)
        _attach(obs)
        obs.on_close(
            reason=ExitReason.TIMEOUT, exit_premium=Decimal("200"),
            exit_time=datetime(2026, 7, 10, 15, 15, 0, tzinfo=_IST), realized_pnl=Decimal("100"),
        )
        assert "Reason : CUTOFF" in handler.records[0].getMessage()

    def test_close_resets_state_so_a_stray_late_cycle_is_a_no_op(self, captured):
        obs, _time, handler = _build(captured)
        _attach(obs)
        obs.on_close(
            reason=ExitReason.STOPLOSS, exit_premium=Decimal("230"),
            exit_time=datetime(2026, 7, 10, 13, 0, 0, tzinfo=_IST), realized_pnl=Decimal("-1500"),
        )
        handler.records.clear()

        obs.on_cycle(
            combined_premium=Decimal("1"), call_price=Decimal("1"), put_price=Decimal("1"),
            target_premium=None, stoploss_premium=None, last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )
        assert handler.records == []

    def test_max_profit_and_loss_seen_reflect_cycles_between_attach_and_close(self, captured):
        obs, time_provider, handler = _build(captured, min_interval=0.0)
        _attach(obs, entry_premium=Decimal("200"), quantity=75)

        # Premium rises (loss) then falls sharply (profit) before close.
        obs.on_cycle(
            combined_premium=Decimal("220"), call_price=Decimal("110"), put_price=Decimal("110"),
            target_premium=Decimal("180"), stoploss_premium=Decimal("230"),
            last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )
        time_provider.advance(1)
        obs.on_cycle(
            combined_premium=Decimal("150"), call_price=Decimal("75"), put_price=Decimal("75"),
            target_premium=Decimal("180"), stoploss_premium=Decimal("230"),
            last_tick_at=None, price_source="LIVE KITE WEBSOCKET",
        )

        obs.on_close(
            reason=ExitReason.TARGET, exit_premium=Decimal("150"),
            exit_time=datetime(2026, 7, 10, 14, 0, 0, tzinfo=_IST), realized_pnl=Decimal("3750"),
        )

        text = handler.records[-1].getMessage()
        # loss cycle: (200-220)*75 = -1500 ; profit cycle: (200-150)*75 = 3750
        assert "Maximum Profit Seen (Rs) : +3750.00" in text
        assert "Maximum Loss Seen (Rs)   : -1500.00" in text
