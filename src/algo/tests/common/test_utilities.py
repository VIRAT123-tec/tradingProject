"""Unit tests for the shared pnl_per_share helper -- the single definition of
the P&L-per-share (per-unit) formula reused by the Excel report, the
trade_history row, and the position-close summary log."""

from __future__ import annotations

from decimal import Decimal

from algo.common.utilities import pnl_per_share


class TestPnlPerShare:
    def test_validation_example_1_positive(self):
        # Lot Size = 65, P&L = 1300 -> 20.00
        assert pnl_per_share(Decimal("1300"), 65) == Decimal("20.00")

    def test_validation_example_2_loss(self):
        # Lot Size = 50, P&L = -2500 -> -50.00
        assert pnl_per_share(Decimal("-2500"), 50) == Decimal("-50.00")

    def test_validation_example_3_zero_pnl(self):
        # Lot Size = 75, P&L = 0 -> 0.00 (a real zero, not None)
        assert pnl_per_share(Decimal("0"), 75) == Decimal("0.00")

    def test_intro_example_rounds_to_two_decimals(self):
        # Lot Size = 65, P&L = 1000 -> 15.38
        assert pnl_per_share(Decimal("1000"), 65) == Decimal("15.38")

    def test_none_pnl_returns_none(self):
        assert pnl_per_share(None, 75) is None

    def test_missing_or_zero_lot_size_returns_none_never_divides(self):
        assert pnl_per_share(Decimal("1000"), None) is None
        assert pnl_per_share(Decimal("1000"), 0) is None

    def test_result_is_quantized_to_two_places(self):
        result = pnl_per_share(Decimal("1000"), 65)
        assert result is not None and result.as_tuple().exponent == -2
