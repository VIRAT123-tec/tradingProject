"""WebSocket order-update callback tests for SimulationBroker.

Covers: delivery on fill, non-delivery while disconnected, fan-out to
multiple registered callbacks, one callback per partial-fill step, and that a
callback is free to call back into the broker (e.g. cancel a different order)
without deadlocking -- proving no internal lock is held while callbacks run.
"""

from __future__ import annotations

from algo.brokers import BrokerOrder
from algo.brokers.simulation import SimulationConfig
from algo.common.enums import OrderStatus

from .conftest import make_request


class TestCallbackDelivery:
    def test_callback_invoked_on_full_fill(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        broker.connect_websocket()
        received: list[BrokerOrder] = []
        broker.register_order_update_callback(received.append)

        broker.place_order(make_request(ce_symbol, tag="cb-full-1"))

        assert len(received) == 1
        assert received[0].status == OrderStatus.COMPLETE
        assert received[0].filled_quantity == 75

    def test_no_callback_delivery_when_websocket_disconnected(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        # deliberately never call connect_websocket()
        received: list[BrokerOrder] = []
        broker.register_order_update_callback(received.append)

        broker.place_order(make_request(ce_symbol, tag="cb-no-ws-1"))

        assert received == []

    def test_callback_stops_after_disconnect(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        broker.connect_websocket()
        received: list[BrokerOrder] = []
        broker.register_order_update_callback(received.append)

        broker.disconnect_websocket()
        broker.place_order(make_request(ce_symbol, tag="cb-disconnect-1"))

        assert received == []

    def test_simulate_websocket_disconnect_stops_delivery(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        broker.connect_websocket()
        received: list[BrokerOrder] = []
        broker.register_order_update_callback(received.append)

        broker.simulate_websocket_disconnect()
        assert broker.is_websocket_connected() is False

        broker.place_order(make_request(ce_symbol, tag="cb-force-disc-1"))
        assert received == []

    def test_multiple_callbacks_all_invoked(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        broker.connect_websocket()
        received_a: list[BrokerOrder] = []
        received_b: list[BrokerOrder] = []
        broker.register_order_update_callback(received_a.append)
        broker.register_order_update_callback(received_b.append)

        broker.place_order(make_request(ce_symbol, tag="cb-fanout-1"))

        assert len(received_a) == 1
        assert len(received_b) == 1
        assert received_a[0].broker_order_id == received_b[0].broker_order_id

    def test_one_callback_per_partial_fill_step(self, broker_factory, ce_symbol):
        config = SimulationConfig(
            synchronous=True,
            partial_fill_probability=1.0,
            min_partial_fill_steps=3,
            max_partial_fill_steps=3,
        )
        broker = broker_factory(config=config, seed=7)
        broker.connect_websocket()
        received: list[BrokerOrder] = []
        broker.register_order_update_callback(received.append)

        broker.place_order(make_request(ce_symbol, tag="cb-partial-1", quantity=75))

        assert len(received) == 3
        # Monotonically increasing filled_quantity across the step sequence.
        filled_progression = [snapshot.filled_quantity for snapshot in received]
        assert filled_progression == sorted(filled_progression)
        assert filled_progression[-1] == 75
        assert received[-1].status == OrderStatus.COMPLETE
        assert all(s.status == OrderStatus.OPEN for s in received[:-1])

    def test_callback_receives_correct_order_identity(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        broker.connect_websocket()
        received: list[BrokerOrder] = []
        broker.register_order_update_callback(received.append)

        result = broker.place_order(make_request(ce_symbol, tag="cb-identity-1"))

        assert received[0].broker_order_id == result.broker_order_id
        assert received[0].tag == "cb-identity-1"


class TestCallbackReentrancy:
    def test_callback_can_call_get_order_without_deadlock(self, broker_factory, ce_symbol):
        broker = broker_factory(seed=1)
        broker.connect_websocket()
        reentrant_results = []

        def on_update(order: BrokerOrder) -> None:
            # Calling back into the broker from inside a callback must not
            # deadlock -- proves _dispatch_order_update never holds the
            # broker's internal lock while invoking callbacks.
            reentrant_results.append(broker.get_order(order.broker_order_id))

        broker.register_order_update_callback(on_update)
        broker.place_order(make_request(ce_symbol, tag="cb-reentrant-1"))

        assert len(reentrant_results) == 1
        assert reentrant_results[0].status == OrderStatus.COMPLETE

    def test_callback_can_cancel_a_different_order_without_deadlock(self, broker_factory, ce_symbol):
        # Both orders configured to stay OPEN (long fill_latency) so neither
        # resolves on its own during the test -- only the explicit
        # cancel_order calls below drive any state change.
        config = SimulationConfig(
            synchronous=False,
            ack_latency_seconds=0.0,
            fill_latency_seconds=5.0,
            matching_tick_seconds=0.05,
        )
        broker = broker_factory(config=config, seed=1)
        broker.connect_websocket()

        order_a = broker.place_order(make_request(ce_symbol, tag="reentrant-a", quantity=75))
        order_b = broker.place_order(make_request(ce_symbol, tag="reentrant-b", quantity=75))

        def on_update(order: BrokerOrder) -> None:
            # Only react to A's own cancellation, canceling B from inside
            # the callback -- a real broker-call-from-within-a-broker-call.
            # B's resulting cancellation will re-invoke this callback too,
            # but its tag won't match, so there is no unbounded recursion.
            if order.tag == "reentrant-a" and order.status == OrderStatus.CANCELLED:
                broker.cancel_order(order_b.broker_order_id)

        broker.register_order_update_callback(on_update)

        # Cancelling A, from the main thread, is what triggers the callback
        # that reentrantly cancels B -- if _dispatch_order_update held the
        # broker's lock while calling this callback, cancel_order(B) would
        # deadlock waiting for that same lock. It doesn't.
        broker.cancel_order(order_a.broker_order_id)

        assert broker.get_order(order_a.broker_order_id).status == OrderStatus.CANCELLED
        assert broker.get_order(order_b.broker_order_id).status == OrderStatus.CANCELLED
