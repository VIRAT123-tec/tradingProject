"""Unit tests for the Strategy-1 combined premium module.

Flagged in the spec as a piece most likely to have subtle, money-costing bugs.
Pure functions are tested exhaustively with no I/O; ``CombinedPremiumTracker``
is tested with plain Decimal/Tick inputs -- no StrategyContext is constructed
except in the two ``from_context`` tests, which use a minimal fake config
rather than the full DI graph, since testability without a heavy object graph
was an explicit design goal.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from algo.brokers.broker_base import InstrumentIdentifier
from algo.common.enums import Exchange
from algo.strategy_engine.strategies.strategy_1.combined_premium import (
    CombinedPremiumTracker,
    PremiumSnapshot,
    PremiumThresholds,
    compute_combined_premium,
    compute_stoploss_premium,
    compute_target_premium,
    compute_thresholds,
)
from algo.strategy_engine.strategy_context import Tick

CE = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTY24000CE")
PE = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTY24000PE")
OTHER = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTY24050CE")

T0 = datetime(2026, 7, 7, 3, 50, tzinfo=timezone.utc)
T1 = datetime(2026, 7, 7, 3, 51, tzinfo=timezone.utc)
T2 = datetime(2026, 7, 7, 3, 52, tzinfo=timezone.utc)


# --------------------------------------------------------------------------
# Pure computation
# --------------------------------------------------------------------------


class TestComputeCombinedPremium:
    def test_basic_addition(self):
        assert compute_combined_premium(Decimal("71.20"), Decimal("71.30")) == Decimal("142.50")

    def test_zero_is_allowed(self):
        assert compute_combined_premium(Decimal("0"), Decimal("50")) == Decimal("50")
        assert compute_combined_premium(Decimal("0"), Decimal("0")) == Decimal("0")

    def test_negative_ce_raises(self):
        with pytest.raises(ValueError, match="ce_price"):
            compute_combined_premium(Decimal("-1"), Decimal("50"))

    def test_negative_pe_raises(self):
        with pytest.raises(ValueError, match="pe_price"):
            compute_combined_premium(Decimal("50"), Decimal("-1"))

    def test_result_is_exact_decimal(self):
        result = compute_combined_premium(Decimal("71.25"), Decimal("70.15"))
        assert result == Decimal("141.40")
        assert isinstance(result, Decimal)


class TestComputeTargetPremium:
    def test_ten_percent_target(self):
        assert compute_target_premium(Decimal("140"), Decimal("0.10")) == Decimal("126.0")

    def test_target_below_entry_for_any_valid_pct(self):
        target = compute_target_premium(Decimal("200"), Decimal("0.25"))
        assert target < Decimal("200")
        assert target == Decimal("150.00")

    @pytest.mark.parametrize("bad_pct", [Decimal("0"), Decimal("1"), Decimal("1.5"), Decimal("-0.1")])
    def test_out_of_range_pct_raises(self, bad_pct):
        with pytest.raises(ValueError, match="target_pct"):
            compute_target_premium(Decimal("140"), bad_pct)

    @pytest.mark.parametrize("bad_premium", [Decimal("0"), Decimal("-10")])
    def test_non_positive_entry_premium_raises(self, bad_premium):
        with pytest.raises(ValueError, match="entry_premium"):
            compute_target_premium(bad_premium, Decimal("0.10"))


class TestComputeStoplossPremium:
    def test_ten_percent_stoploss(self):
        assert compute_stoploss_premium(Decimal("140"), Decimal("0.10")) == Decimal("154.0")

    def test_stoploss_above_entry_for_any_valid_pct(self):
        sl = compute_stoploss_premium(Decimal("200"), Decimal("0.25"))
        assert sl > Decimal("200")
        assert sl == Decimal("250.00")

    @pytest.mark.parametrize("bad_pct", [Decimal("0"), Decimal("1"), Decimal("2"), Decimal("-0.5")])
    def test_out_of_range_pct_raises(self, bad_pct):
        with pytest.raises(ValueError, match="sl_pct"):
            compute_stoploss_premium(Decimal("140"), bad_pct)

    @pytest.mark.parametrize("bad_premium", [Decimal("0"), Decimal("-10")])
    def test_non_positive_entry_premium_raises(self, bad_premium):
        with pytest.raises(ValueError, match="entry_premium"):
            compute_stoploss_premium(bad_premium, Decimal("0.10"))


class TestComputeThresholds:
    def test_bundles_target_and_stoploss_consistently(self):
        thresholds = compute_thresholds(Decimal("140"), Decimal("0.10"), Decimal("0.15"))

        assert thresholds == PremiumThresholds(
            entry_premium=Decimal("140"),
            target_premium=Decimal("126.0"),
            stoploss_premium=Decimal("161.0"),
            target_pct=Decimal("0.10"),
            sl_pct=Decimal("0.15"),
        )

    def test_target_always_below_and_stoploss_always_above_entry(self):
        for entry, tp, sp in [
            (Decimal("100"), Decimal("0.05"), Decimal("0.05")),
            (Decimal("500.55"), Decimal("0.20"), Decimal("0.30")),
            (Decimal("1"), Decimal("0.99"), Decimal("0.01")),
        ]:
            thresholds = compute_thresholds(entry, tp, sp)
            assert thresholds.target_premium < thresholds.entry_premium
            assert thresholds.stoploss_premium > thresholds.entry_premium


# --------------------------------------------------------------------------
# CombinedPremiumTracker
# --------------------------------------------------------------------------


def _tracker(target_pct=Decimal("0.10"), sl_pct=Decimal("0.10")) -> CombinedPremiumTracker:
    return CombinedPremiumTracker(
        call_instrument=CE, put_instrument=PE, target_pct=target_pct, sl_pct=sl_pct
    )


class TestTrackerConstruction:
    def test_same_instrument_for_both_legs_raises(self):
        with pytest.raises(ValueError, match="different"):
            CombinedPremiumTracker(
                call_instrument=CE, put_instrument=CE, target_pct=Decimal("0.1"), sl_pct=Decimal("0.1")
            )

    @pytest.mark.parametrize("bad_pct", [Decimal("0"), Decimal("1"), Decimal("-0.1")])
    def test_invalid_target_pct_raises(self, bad_pct):
        with pytest.raises(ValueError, match="target_pct"):
            CombinedPremiumTracker(
                call_instrument=CE, put_instrument=PE, target_pct=bad_pct, sl_pct=Decimal("0.1")
            )

    @pytest.mark.parametrize("bad_pct", [Decimal("0"), Decimal("1"), Decimal("-0.1")])
    def test_invalid_sl_pct_raises(self, bad_pct):
        with pytest.raises(ValueError, match="sl_pct"):
            CombinedPremiumTracker(
                call_instrument=CE, put_instrument=PE, target_pct=Decimal("0.1"), sl_pct=bad_pct
            )


class TestRecordEntry:
    def test_computes_and_stores_thresholds(self):
        tracker = _tracker(target_pct=Decimal("0.10"), sl_pct=Decimal("0.10"))

        thresholds = tracker.record_entry(
            call_fill_price=Decimal("71.20"), put_fill_price=Decimal("68.80"), at=T0
        )

        assert thresholds.entry_premium == Decimal("140.00")
        assert thresholds.target_premium == Decimal("126.000")
        assert thresholds.stoploss_premium == Decimal("154.000")
        assert tracker.thresholds == thresholds

    def test_seeds_latest_prices_so_snapshot_available_immediately(self):
        tracker = _tracker()
        tracker.record_entry(call_fill_price=Decimal("71.20"), put_fill_price=Decimal("68.80"), at=T0)

        snapshot = tracker.current_snapshot()
        assert snapshot is not None
        assert snapshot.combined_premium == Decimal("140.00")
        assert snapshot.ce_price == Decimal("71.20")
        assert snapshot.pe_price == Decimal("68.80")
        assert snapshot.as_of == T0

    def test_calling_twice_raises(self):
        tracker = _tracker()
        tracker.record_entry(call_fill_price=Decimal("71.20"), put_fill_price=Decimal("68.80"), at=T0)

        with pytest.raises(ValueError, match="already been called"):
            tracker.record_entry(call_fill_price=Decimal("50"), put_fill_price=Decimal("50"), at=T1)


class TestOnTick:
    def test_no_snapshot_before_both_legs_known(self):
        tracker = _tracker()
        assert tracker.current_snapshot() is None

        result = tracker.on_tick(Tick(instrument=CE, last_price=Decimal("70"), timestamp=T0))

        assert result is None  # PE still unknown
        assert tracker.current_snapshot() is None
        assert tracker.latest_call_price == Decimal("70")
        assert tracker.latest_put_price is None

    def test_snapshot_once_both_legs_have_ticked(self):
        tracker = _tracker()
        tracker.on_tick(Tick(instrument=CE, last_price=Decimal("70"), timestamp=T0))
        result = tracker.on_tick(Tick(instrument=PE, last_price=Decimal("72"), timestamp=T1))

        assert result is not None
        assert result.combined_premium == Decimal("142")
        assert result.as_of == T1

    def test_unrelated_instrument_tick_is_ignored(self):
        tracker = _tracker()
        tracker.on_tick(Tick(instrument=CE, last_price=Decimal("70"), timestamp=T0))
        tracker.on_tick(Tick(instrument=PE, last_price=Decimal("72"), timestamp=T1))

        result = tracker.on_tick(Tick(instrument=OTHER, last_price=Decimal("999"), timestamp=T2))

        assert result is None
        # Unaffected by the unrelated tick.
        assert tracker.latest_call_price == Decimal("70")
        assert tracker.latest_put_price == Decimal("72")

    def test_premium_recomputes_live_as_ticks_arrive(self):
        tracker = _tracker()
        tracker.record_entry(call_fill_price=Decimal("71.20"), put_fill_price=Decimal("68.80"), at=T0)
        assert tracker.current_snapshot().combined_premium == Decimal("140.00")

        # Premium decays -- moving toward target.
        tracker.on_tick(Tick(instrument=CE, last_price=Decimal("60"), timestamp=T1))
        tracker.on_tick(Tick(instrument=PE, last_price=Decimal("60"), timestamp=T1))
        assert tracker.current_snapshot().combined_premium == Decimal("120")

        # Premium spikes -- moving toward stoploss.
        tracker.on_tick(Tick(instrument=CE, last_price=Decimal("90"), timestamp=T2))
        assert tracker.current_snapshot().combined_premium == Decimal("150")  # PE still 60

    def test_uses_tick_timestamp_not_wall_clock(self):
        tracker = _tracker()
        tracker.on_tick(Tick(instrument=CE, last_price=Decimal("70"), timestamp=T0))
        snapshot = tracker.on_tick(Tick(instrument=PE, last_price=Decimal("72"), timestamp=T2))

        assert snapshot.as_of == T2  # the tick's own timestamp, not datetime.now()


class TestFromContext:
    def test_reads_target_and_sl_pct_from_config(self):
        context = _FakeContext(_FakeConfig(target_pct=Decimal("0.12"), sl_pct=Decimal("0.18")))
        tracker = CombinedPremiumTracker.from_context(context, call_instrument=CE, put_instrument=PE)

        thresholds = tracker.record_entry(
            call_fill_price=Decimal("100"), put_fill_price=Decimal("100"), at=T0
        )
        assert thresholds.target_premium == Decimal("176.00")  # 200 * (1 - 0.12)
        assert thresholds.stoploss_premium == Decimal("236.00")  # 200 * (1 + 0.18)

    def test_missing_fields_raise_type_error(self):
        context = _FakeContext(_FakeConfigMissingFields())
        with pytest.raises(TypeError, match="target_pct"):
            CombinedPremiumTracker.from_context(context, call_instrument=CE, put_instrument=PE)

    def test_wrong_type_fields_raise_type_error(self):
        context = _FakeContext(_FakeConfigWrongType())
        with pytest.raises(TypeError):
            CombinedPremiumTracker.from_context(context, call_instrument=CE, put_instrument=PE)


@dataclass
class _FakeConfig:
    target_pct: Decimal
    sl_pct: Decimal


@dataclass
class _FakeConfigMissingFields:
    lots: int = 1


@dataclass
class _FakeConfigWrongType:
    target_pct: float = 0.1  # wrong type -- must be Decimal
    sl_pct: float = 0.1


@dataclass
class _FakeContext:
    config: object
