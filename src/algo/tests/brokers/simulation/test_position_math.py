"""Direct unit tests for _update_position, the pure function SimulationBroker
uses to fold every fill into the position ledger. Tested in isolation from
SimulationBroker itself because this is exactly the kind of small, easy-to-
get-the-sign-wrong money arithmetic that deserves its own focused coverage
rather than being verified only indirectly through end-to-end order tests.
"""

from __future__ import annotations

from decimal import Decimal

from algo.brokers.simulation.simulation_broker import _SimPosition, _update_position
from algo.common.enums import TransactionType


class TestOpeningAndAdding:
    def test_opens_new_long_from_flat(self):
        flat = _SimPosition()
        result = _update_position(flat, TransactionType.BUY, 75, Decimal("120"))

        assert result.quantity == 75
        assert result.average_price == Decimal("120")
        assert result.realized_pnl == Decimal("0")

    def test_opens_new_short_from_flat(self):
        flat = _SimPosition()
        result = _update_position(flat, TransactionType.SELL, 75, Decimal("120"))

        assert result.quantity == -75
        assert result.average_price == Decimal("120")

    def test_adds_to_existing_long_with_weighted_average(self):
        position = _SimPosition(quantity=75, average_price=Decimal("100"))
        result = _update_position(position, TransactionType.BUY, 75, Decimal("120"))

        assert result.quantity == 150
        assert result.average_price == Decimal("110")  # (100*75 + 120*75) / 150

    def test_adds_to_existing_short_with_weighted_average(self):
        position = _SimPosition(quantity=-75, average_price=Decimal("100"))
        result = _update_position(position, TransactionType.SELL, 75, Decimal("120"))

        assert result.quantity == -150
        assert result.average_price == Decimal("110")


class TestClosingAndRealizingPnl:
    def test_partial_close_of_long_realizes_pnl_on_closed_portion_only(self):
        position = _SimPosition(quantity=100, average_price=Decimal("100"))
        result = _update_position(position, TransactionType.SELL, 40, Decimal("120"))

        assert result.quantity == 60
        assert result.average_price == Decimal("100")  # unchanged: remaining portion still at entry price
        assert result.realized_pnl == Decimal("800")  # 40 * (120 - 100)

    def test_partial_close_of_short_realizes_pnl_on_closed_portion_only(self):
        position = _SimPosition(quantity=-100, average_price=Decimal("100"))
        result = _update_position(position, TransactionType.BUY, 40, Decimal("80"))

        assert result.quantity == -60
        assert result.average_price == Decimal("100")
        assert result.realized_pnl == Decimal("800")  # 40 * (100 - 80), short profits when price falls

    def test_short_position_loses_when_price_rises(self):
        position = _SimPosition(quantity=-100, average_price=Decimal("100"))
        result = _update_position(position, TransactionType.BUY, 100, Decimal("130"))

        assert result.quantity == 0
        assert result.realized_pnl == Decimal("-3000")  # 100 * (100 - 130)

    def test_exact_full_close_zeroes_quantity_and_average(self):
        position = _SimPosition(quantity=75, average_price=Decimal("100"))
        result = _update_position(position, TransactionType.SELL, 75, Decimal("110"))

        assert result.quantity == 0
        assert result.average_price == Decimal("0")
        assert result.realized_pnl == Decimal("750")  # 75 * (110 - 100)

    def test_realized_pnl_accumulates_across_multiple_fills(self):
        position = _SimPosition(quantity=100, average_price=Decimal("100"))
        position = _update_position(position, TransactionType.SELL, 30, Decimal("110"))  # +300
        position = _update_position(position, TransactionType.SELL, 30, Decimal("120"))  # +600

        assert position.quantity == 40
        assert position.realized_pnl == Decimal("900")


class TestFlipping:
    def test_sell_more_than_long_position_flips_to_short(self):
        position = _SimPosition(quantity=50, average_price=Decimal("100"))
        result = _update_position(position, TransactionType.SELL, 80, Decimal("110"))

        # 50 closed (realizing pnl), remaining 30 opens a fresh short at fill_price.
        assert result.quantity == -30
        assert result.average_price == Decimal("110")
        assert result.realized_pnl == Decimal("500")  # 50 * (110 - 100)

    def test_buy_more_than_short_position_flips_to_long(self):
        position = _SimPosition(quantity=-50, average_price=Decimal("100"))
        result = _update_position(position, TransactionType.BUY, 80, Decimal("90"))

        assert result.quantity == 30
        assert result.average_price == Decimal("90")
        assert result.realized_pnl == Decimal("500")  # 50 * (100 - 90)
