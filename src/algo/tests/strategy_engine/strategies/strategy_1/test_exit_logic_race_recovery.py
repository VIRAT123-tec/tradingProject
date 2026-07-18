"""Regression test for the exit-path optimistic-lock poisoning freeze.

The bug: ExitLogic's close confirmation (_record_close_fill) and the push-path
OrderUpdateProcessor both write the same Order row. When the push path commits
the terminal fill first, ExitLogic's own row write fails the optimistic-lock
version check (StaleDataError -> ConcurrentModificationError). That error was
*caught*, but the failed flush had already deactivated the shared SQLAlchemy
session, so the trade/leg/intent writes and the commit that followed in the
*same* session raised PendingRollbackError -- freezing a position that had, in
fact, exited successfully.

The fix isolates the Order-row reconciliation in its own unit of work
(``reconcile_order_terminal``): a lost race rolls back only that session and is
deferred to the push path's committed result, so it can never poison the
trade/leg/intent writes. This test forces a *real* StaleDataError at exactly the
production point -- a concurrent committed writer bumps the Order's version
between ExitLogic's read and its flush -- and asserts the exit still closes the
position cleanly rather than freezing.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import text

from algo.common.enums import ExitReason, PositionState, TradeLegStatus
from algo.database.models.order import Order
from algo.database.repositories.order_repository import OrderRepository
from algo.database.repositories.position_repository import PositionRepository
from algo.strategy_engine.strategies.strategy_1.exit_logic import ExitOutcome
from algo.tests.strategy_engine.strategies.strategy_1.test_exit_logic import (
    TODAY,
    build_open_position,
)


class TestExitSurvivesLostOptimisticLockRace:
    def test_push_path_winning_the_order_write_does_not_freeze_the_exit(self, monkeypatch):
        """A concurrent writer commits the Order row's terminal fill (bumping its
        version) mid-confirmation, so ExitLogic's own Order write raises a real
        StaleDataError. The exit must still complete: position CLOSED, both legs
        CLOSED, no exception escapes."""
        h = build_open_position()
        # Premium decays -> a clean TARGET exit that buys both legs back.
        h.price_source.set_price(h.ce_id, Decimal("60"))
        h.price_source.set_price(h.pe_id, Decimal("55"))

        original = OrderRepository.get_by_id_for_update
        fired = {"done": False}

        def racy_get_for_update(self, id_):
            # Return the row exactly as ExitLogic's reconcile session loaded it
            # (stale version, still in that session's identity map)...
            order = original(self, id_)
            if not fired["done"] and order is not None:
                fired["done"] = True
                # ...then, from a *separate* committed transaction, advance the
                # version -- reproducing the push path winning the race. The
                # reconcile session's subsequent flush (UPDATE ... WHERE
                # version=<stale>) now matches 0 rows -> real StaleDataError.
                with h.session_factory() as other:
                    other.execute(
                        text("UPDATE orders SET version = version + 1 WHERE id = :id"),
                        {"id": id_},
                    )
                    other.commit()
            return order

        monkeypatch.setattr(OrderRepository, "get_by_id_for_update", racy_get_for_update)

        # Must not raise (previously: PendingRollbackError -> frozen instance).
        result = h.exit_logic.exit(ExitReason.TARGET)

        assert fired["done"], "test did not actually inject the race"
        assert result.outcome is ExitOutcome.EXITED, (
            f"exit should complete despite the lost race, got {result.outcome.value}: {result.message}"
        )

        with h.session_factory() as s:
            repo = PositionRepository(s)
            position = repo.get_by_instance_and_date(h.instance_id, TODAY)
            assert position is not None
            assert position.state is PositionState.CLOSED
            trades = repo.list_trades_for_position(position.id)
            assert trades and all(t.status is TradeLegStatus.CLOSED for t in trades)
            # Each leg still has exactly one Order row -- no duplicate created by
            # the recovery path.
            orders = OrderRepository(s)
            for trade in trades:
                leg_orders = orders.list_for_trade(trade.id)
                assert len([o for o in leg_orders if o.purpose.value == "EXIT"]) == 1
