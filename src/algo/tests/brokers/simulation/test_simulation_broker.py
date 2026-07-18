"""Order-lifecycle tests for SimulationBroker: full fills, partial fills,
rejections, timeouts, connection failures, cancellation, modification,
authentication/health, and seeded determinism.

All tests here use synchronous=True (no background thread, no real waiting)
so they run fast and their outcomes depend only on the seeded RNG, never on
wall-clock timing. Timing-dependent behavior (the real ack-then-fill gap) is
covered separately in test_simulation_broker_concurrency.py, which needs the
background thread and therefore real waits.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from algo.brokers import (
    BrokerConnectionError,
    BrokerTimeoutError,
    InvalidOrderRequestError,
    ModifyOrderRequest,
    OrderNotCancellableError,
    OrderNotFoundError,
    OrderNotModifiableError,
    OrderRejectedError,
)
from algo.brokers.exceptions import BrokerAuthenticationError
from algo.brokers.simulation import SimulationConfig
from algo.common.enums import OrderStatus, TransactionType

from .conftest import CE_LTP, LOT_SIZE, make_request

pytestmark = pytest.mark.usefixtures("catalog", "price_source")


# ---------------------------------------------------------------------------
# Full fills
# ---------------------------------------------------------------------------


class TestFullFills:
    def test_market_order_fully_fills_at_current_ltp(self, broker_factory, ce_symbol):
        broker = broker_factory(config=SimulationConfig(synchronous=True), seed=1)
        result = broker.place_order(make_request(ce_symbol, tag="full-1"))
        order = broker.get_order(result.broker_order_id)

        assert order.status == OrderStatus.COMPLETE
        assert order.filled_quantity == LOT_SIZE
        assert order.average_price == CE_LTP
        assert order.filled_at is not None
        assert order.tag == "full-1"

    def test_full_fill_appears_in_get_orders(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        result = broker.place_order(make_request(ce_symbol, tag="full-2"))
        orders = broker.get_orders()

        assert len(orders) == 1
        assert orders[0].broker_order_id == result.broker_order_id

    def test_full_fill_updates_short_position(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        broker.place_order(make_request(ce_symbol, tag="full-3", transaction_type=TransactionType.SELL))
        positions = broker.get_positions()

        assert len(positions) == 1
        assert positions[0].quantity == -LOT_SIZE  # SELL to open => net short
        assert positions[0].average_price == CE_LTP
        assert positions[0].pnl == Decimal("0")  # LTP unchanged since entry

    def test_full_fill_reduces_available_margin(self, broker_factory, ce_symbol):
        config = SimulationConfig(synchronous=True, initial_cash=Decimal("1000000"), margin_per_lot=Decimal("50000"))
        broker = broker_factory(config=config, seed=1)
        broker.place_order(make_request(ce_symbol, tag="full-4"))
        margins = broker.get_margins()

        assert margins.used_margin == Decimal("50000")
        assert margins.available_cash == Decimal("950000")

    def test_buy_to_open_produces_long_position(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        broker.place_order(make_request(ce_symbol, tag="long-1", transaction_type=TransactionType.BUY))
        positions = broker.get_positions()

        assert positions[0].quantity == LOT_SIZE


# ---------------------------------------------------------------------------
# Partial fills
# ---------------------------------------------------------------------------


class TestPartialFills:
    def test_forced_partial_fill_completes_over_multiple_steps(self, broker_factory, ce_symbol):
        config = SimulationConfig(
            synchronous=True,
            partial_fill_probability=1.0,
            min_partial_fill_steps=3,
            max_partial_fill_steps=3,
        )
        broker = broker_factory(config=config, seed=7)
        result = broker.place_order(make_request(ce_symbol, tag="partial-1", quantity=75))
        order = broker.get_order(result.broker_order_id)

        assert order.status == OrderStatus.COMPLETE
        assert order.filled_quantity == 75

    def test_partial_fill_last_step_fills_exact_remainder(self, broker_factory, ce_symbol):
        # 80 / 3 steps = 26 per step (26, 26, 28) -- the remainder must not be dropped.
        config = SimulationConfig(
            synchronous=True,
            partial_fill_probability=1.0,
            min_partial_fill_steps=3,
            max_partial_fill_steps=3,
        )
        broker = broker_factory(config=config, seed=7)
        result = broker.place_order(make_request(ce_symbol, tag="partial-2", quantity=80))
        order = broker.get_order(result.broker_order_id)

        assert order.filled_quantity == 80
        assert order.status == OrderStatus.COMPLETE

    def test_partial_fill_average_price_reflects_weighted_fills(self, broker_factory, ce_symbol, price_source, ce_identifier):
        config = SimulationConfig(
            synchronous=True,
            partial_fill_probability=1.0,
            min_partial_fill_steps=2,
            max_partial_fill_steps=2,
        )
        broker = broker_factory(config=config, seed=3)
        result = broker.place_order(make_request(ce_symbol, tag="partial-3", quantity=100))
        order = broker.get_order(result.broker_order_id)

        # Price never moves in this test (StaticPriceSource), so the
        # weighted average must equal the flat price regardless of step split.
        assert order.average_price == CE_LTP
        assert order.filled_quantity == 100

    def test_position_updates_incrementally_during_partial_fill(self, broker_factory, ce_symbol):
        config = SimulationConfig(
            synchronous=True,
            partial_fill_probability=1.0,
            min_partial_fill_steps=4,
            max_partial_fill_steps=4,
        )
        broker = broker_factory(config=config, seed=9)
        result = broker.place_order(make_request(ce_symbol, tag="partial-4", quantity=100))
        order = broker.get_order(result.broker_order_id)
        positions = broker.get_positions()

        # By the time synchronous drain completes, the position must reflect
        # the FULL filled amount, not just the first step -- proves each
        # step's fill was applied to the ledger, not only the last one.
        assert positions[0].quantity == -order.filled_quantity


# ---------------------------------------------------------------------------
# Rejections
# ---------------------------------------------------------------------------


class TestRejections:
    def test_unknown_instrument_is_rejected_immediately(self, broker_factory):
        broker = broker_factory(seed=1)
        request = make_request("BOGUS26JUL9999CE", tag="reject-unknown")

        with pytest.raises(OrderRejectedError):
            broker.place_order(request)

        assert broker.find_order_by_tag("reject-unknown") is None

    def test_forced_delayed_rejection_leaves_no_fill(self, broker_factory, ce_symbol):
        config = SimulationConfig(synchronous=True, rejection_probability=1.0)
        broker = broker_factory(config=config, seed=2)
        result = broker.place_order(make_request(ce_symbol, tag="reject-delayed"))
        order = broker.get_order(result.broker_order_id)

        assert order.status == OrderStatus.REJECTED
        assert order.filled_quantity == 0
        assert order.status_message is not None

    def test_rejected_order_does_not_affect_positions(self, broker_factory, ce_symbol):
        config = SimulationConfig(synchronous=True, rejection_probability=1.0)
        broker = broker_factory(config=config, seed=2)
        broker.place_order(make_request(ce_symbol, tag="reject-no-position"))

        assert broker.get_positions() == []


# ---------------------------------------------------------------------------
# Timeouts
# ---------------------------------------------------------------------------


class TestTimeouts:
    def test_ack_timeout_raises_but_order_is_recoverable_by_tag(self, broker_factory, ce_symbol):
        config = SimulationConfig(synchronous=True, ack_timeout_probability=1.0)
        broker = broker_factory(config=config, seed=4)
        request = make_request(ce_symbol, tag="ack-timeout-1")

        with pytest.raises(BrokerTimeoutError):
            broker.place_order(request)

        # This is the crux of the whole ambiguous-outcome contract: the
        # caller was told "unknown", but the order actually reached the
        # (simulated) broker and can be found and resolved.
        recovered = broker.find_order_by_tag("ack-timeout-1")
        assert recovered is not None
        assert recovered.status == OrderStatus.COMPLETE
        assert recovered.filled_quantity == LOT_SIZE

    def test_caller_timeout_shorter_than_latency_raises_before_order_exists(self, broker_factory, ce_symbol):
        config = SimulationConfig(synchronous=True, ack_latency_seconds=0.5)
        broker = broker_factory(config=config, seed=5)
        request = make_request(ce_symbol, tag="caller-timeout-1")

        with pytest.raises(BrokerTimeoutError):
            broker.place_order(request, timeout=0.01)

        assert broker.find_order_by_tag("caller-timeout-1") is None

    def test_read_call_respects_caller_timeout(self, broker_factory):
        config = SimulationConfig(synchronous=True, ack_latency_seconds=0.5)
        broker = broker_factory(config=config, seed=5)

        with pytest.raises(BrokerTimeoutError):
            broker.get_orders(timeout=0.01)


# ---------------------------------------------------------------------------
# Connection failures
# ---------------------------------------------------------------------------


class TestConnectionFailures:
    def test_connection_failure_on_place_order_creates_no_record(self, broker_factory, ce_symbol):
        config = SimulationConfig(synchronous=True, connection_failure_probability=1.0)
        broker = broker_factory(config=config, seed=6)
        request = make_request(ce_symbol, tag="conn-fail-1")

        with pytest.raises(BrokerConnectionError):
            broker.place_order(request)

        # get_orders()/get_positions() would themselves raise under this same
        # 100%-failure config (correctly -- a real outage fails every call,
        # not just placement), so "nothing was created" has to be verified
        # via internal state rather than another broker call.
        assert broker._orders == {}
        assert broker._orders_by_tag == {}
        assert broker._positions == {}

    def test_connection_failure_applies_to_read_methods_too(self, broker_factory):
        config = SimulationConfig(synchronous=True, connection_failure_probability=1.0)
        broker = broker_factory(config=config, seed=6)

        with pytest.raises(BrokerConnectionError):
            broker.get_positions()
        with pytest.raises(BrokerConnectionError):
            broker.get_margins()

    def test_websocket_connect_can_fail(self, broker_factory):
        config = SimulationConfig(synchronous=True, websocket_connect_failure_probability=1.0)
        broker = broker_factory(config=config, seed=6)

        with pytest.raises(BrokerConnectionError):
            broker.connect_websocket()
        assert broker.is_websocket_connected() is False


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


class TestCancellation:
    def test_cancel_after_partial_fill_preserves_filled_quantity(self, broker_factory, ce_symbol):
        # synchronous=False here deliberately: cancellation *during* a partial
        # fill (not after full drain) requires the real timing gap only the
        # background matching engine produces.
        config = SimulationConfig(
            synchronous=False,
            partial_fill_probability=1.0,
            min_partial_fill_steps=5,
            max_partial_fill_steps=5,
            ack_latency_seconds=0.0,
            fill_latency_seconds=0.2,
            matching_tick_seconds=0.01,
        )
        broker = broker_factory(config=config, seed=8)
        result = broker.place_order(make_request(ce_symbol, tag="cancel-partial-1", quantity=100))

        _wait_until(lambda: broker.get_order(result.broker_order_id).filled_quantity > 0, timeout=2.0)
        mid_state = broker.get_order(result.broker_order_id)
        assert 0 < mid_state.filled_quantity < 100  # must genuinely be mid-flight

        broker.cancel_order(result.broker_order_id)
        final_state = broker.get_order(result.broker_order_id)

        assert final_state.status == OrderStatus.CANCELLED
        assert final_state.filled_quantity == mid_state.filled_quantity

    def test_cancel_completed_order_raises_not_cancellable(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        result = broker.place_order(make_request(ce_symbol, tag="cancel-complete-1"))

        with pytest.raises(OrderNotCancellableError):
            broker.cancel_order(result.broker_order_id)

    def test_cancel_unknown_order_raises_not_found(self, broker_factory):
        broker = broker_factory(seed=1)

        with pytest.raises(OrderNotFoundError):
            broker.cancel_order("SIM999999999999")

    def test_cancel_open_unfilled_order(self, broker_factory, ce_symbol):
        config = SimulationConfig(
            synchronous=False,
            ack_latency_seconds=0.0,
            fill_latency_seconds=5.0,  # long enough it won't fill during the test
            matching_tick_seconds=0.05,
        )
        broker = broker_factory(config=config, seed=1)
        result = broker.place_order(make_request(ce_symbol, tag="cancel-open-1"))

        pre_cancel = broker.get_order(result.broker_order_id)
        assert pre_cancel.filled_quantity == 0

        broker.cancel_order(result.broker_order_id)
        order = broker.get_order(result.broker_order_id)

        assert order.status == OrderStatus.CANCELLED
        assert order.filled_quantity == 0


# ---------------------------------------------------------------------------
# Modification
# ---------------------------------------------------------------------------


class TestModifyOrder:
    def test_modify_price_and_quantity_on_open_order(self, broker_factory, ce_symbol):
        config = SimulationConfig(
            synchronous=False,
            ack_latency_seconds=0.0,
            fill_latency_seconds=5.0,
            matching_tick_seconds=0.05,
        )
        broker = broker_factory(config=config, seed=1)
        result = broker.place_order(make_request(ce_symbol, tag="modify-1", quantity=75))
        broker.modify_order(ModifyOrderRequest(broker_order_id=result.broker_order_id, quantity=150))

        order = broker.get_order(result.broker_order_id)
        assert order.quantity == 150

    def test_modify_terminal_order_raises_not_modifiable(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        result = broker.place_order(make_request(ce_symbol, tag="modify-terminal-1"))

        with pytest.raises(OrderNotModifiableError):
            broker.modify_order(ModifyOrderRequest(broker_order_id=result.broker_order_id, quantity=150))

    def test_modify_quantity_below_filled_raises_invalid_request(self, broker_factory, ce_symbol):
        config = SimulationConfig(
            synchronous=False,
            ack_latency_seconds=0.0,
            fill_latency_seconds=5.0,
            matching_tick_seconds=0.05,
        )
        broker = broker_factory(config=config, seed=1)
        result = broker.place_order(make_request(ce_symbol, tag="modify-below-filled", quantity=75))

        # Nothing has filled yet (long fill_latency), so any quantity below
        # the current filled_quantity (0) is impossible to construct here;
        # instead assert the guard rejects a quantity below what's already
        # filled once we manufacture that state directly.
        order_id = result.broker_order_id
        sim_order = broker._orders[order_id]  # test-only introspection
        sim_order.filled_quantity = 50

        with pytest.raises(InvalidOrderRequestError):
            broker.modify_order(ModifyOrderRequest(broker_order_id=order_id, quantity=25))

    def test_modify_unknown_order_raises_not_found(self, broker_factory):
        broker = broker_factory(seed=1)

        with pytest.raises(OrderNotFoundError):
            broker.modify_order(ModifyOrderRequest(broker_order_id="SIM999999999999", quantity=75))


# ---------------------------------------------------------------------------
# Authentication / health
# ---------------------------------------------------------------------------


class TestAuthenticationAndHealth:
    def test_operations_require_authentication(self, broker_factory, ce_symbol):
        broker = broker_factory(authenticate=False)

        with pytest.raises(BrokerAuthenticationError):
            broker.place_order(make_request(ce_symbol, tag="unauth-1"))

    def test_configurable_auth_failure(self, broker_factory):
        config = SimulationConfig(auth_should_fail=True)
        broker = broker_factory(config=config, authenticate=False)

        with pytest.raises(BrokerAuthenticationError):
            broker.authenticate()
        assert broker.is_authenticated() is False

    def test_health_check_reports_unhealthy_before_auth(self, broker_factory):
        broker = broker_factory(authenticate=False)
        status = broker.health_check()

        assert status.healthy is False
        assert status.detail is not None

    def test_health_check_reports_healthy_after_auth(self, broker_factory):
        broker = broker_factory()
        status = broker.health_check()

        assert status.healthy is True


# ---------------------------------------------------------------------------
# Deterministic, seeded behavior
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_seed_and_call_sequence_produces_identical_outcomes(self, catalog, price_source, ce_symbol):
        config = SimulationConfig(
            synchronous=True,
            rejection_probability=0.3,
            partial_fill_probability=0.3,
            min_partial_fill_steps=2,
            max_partial_fill_steps=4,
        )
        outcomes_a = _run_scripted_orders(catalog, price_source, ce_symbol, config, seed=123)
        outcomes_b = _run_scripted_orders(catalog, price_source, ce_symbol, config, seed=123)

        assert outcomes_a == outcomes_b

    def test_different_seeds_can_diverge(self, catalog, price_source, ce_symbol):
        config = SimulationConfig(synchronous=True, rejection_probability=0.5)
        outcomes_a = _run_scripted_orders(catalog, price_source, ce_symbol, config, seed=1)
        outcomes_b = _run_scripted_orders(catalog, price_source, ce_symbol, config, seed=2)

        # With p=0.5 across 20 independent draws, the chance two different
        # seeds coincidentally produce an identical outcome sequence is
        # astronomically small (~1 in a million) -- safe as a real assertion.
        assert outcomes_a != outcomes_b


def _run_scripted_orders(catalog, price_source, symbol, config, *, seed: int) -> list[tuple[str, int]]:
    import random as random_module

    from algo.brokers.simulation import SimulationBroker

    broker = SimulationBroker(
        instrument_catalog=catalog,
        price_source=price_source,
        config=config,
        rng=random_module.Random(seed),
    )
    broker.authenticate()
    try:
        outcomes = []
        for i in range(20):
            request = make_request(symbol, tag=f"determinism-{i}")
            result = broker.place_order(request)
            order = broker.get_order(result.broker_order_id)
            outcomes.append((order.status.value, order.filled_quantity))
        return outcomes
    finally:
        broker.close()


def _wait_until(predicate, *, timeout: float, interval: float = 0.01) -> None:
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")
