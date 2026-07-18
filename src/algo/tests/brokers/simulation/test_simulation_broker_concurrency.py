"""Concurrency and threaded-mode timing tests for SimulationBroker.

These use synchronous=False (the real background matching-engine thread) --
unlike test_simulation_broker.py, which stays in synchronous mode for speed
and determinism. Outcomes here are still made deterministic via
rejection_probability=0 / partial_fill_probability=0 (every order fully
fills) so assertions on aggregate state don't depend on race-sensitive RNG
draw ordering across threads -- only the *thread-safety* of the bookkeeping
is under test, not the fill-decision logic (already covered elsewhere).
"""

from __future__ import annotations

import threading
import time

import pytest

from algo.brokers.simulation import SimulationConfig
from algo.common.enums import OrderStatus, TransactionType

from .conftest import LOT_SIZE, make_request

_TERMINAL = frozenset({OrderStatus.COMPLETE, OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.ERROR})


def _threaded_config(**overrides) -> SimulationConfig:
    defaults = dict(
        synchronous=False,
        ack_latency_seconds=0.0,
        fill_latency_seconds=0.05,
        matching_tick_seconds=0.005,
        rejection_probability=0.0,
        partial_fill_probability=0.0,
    )
    defaults.update(overrides)
    return SimulationConfig(**defaults)


def _wait_until_terminal(broker, broker_order_id: str, *, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if broker.get_order(broker_order_id).status in _TERMINAL:
            return
        time.sleep(0.005)
    raise AssertionError(f"order {broker_order_id} did not reach a terminal state within {timeout}s")


class TestConcurrentOrderPlacement:
    def test_concurrent_placements_from_many_threads_all_succeed(self, broker_factory, ce_symbol):
        broker = broker_factory(config=_threaded_config(), seed=42)
        num_threads = 20
        results: list[str] = []
        errors: list[BaseException] = []
        results_lock = threading.Lock()
        barrier = threading.Barrier(num_threads)

        def worker(index: int) -> None:
            try:
                barrier.wait(timeout=5.0)  # maximize actual overlap
                request = make_request(ce_symbol, tag=f"concurrent-{index}", quantity=LOT_SIZE)
                result = broker.place_order(request)
                with results_lock:
                    results.append(result.broker_order_id)
            except BaseException as exc:  # noqa: BLE001 -- test wants to see everything
                with results_lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        assert errors == [], f"unexpected exceptions from worker threads: {errors}"
        assert len(results) == num_threads
        assert len(set(results)) == num_threads, "broker_order_id must be unique per order, even under contention"

        for broker_order_id in results:
            _wait_until_terminal(broker, broker_order_id)

        orders = broker.get_orders()
        assert len(orders) == num_threads
        assert all(o.status == OrderStatus.COMPLETE for o in orders)
        assert all(o.filled_quantity == LOT_SIZE for o in orders)

    def test_concurrent_placements_produce_correct_aggregate_position(self, broker_factory, ce_symbol):
        broker = broker_factory(config=_threaded_config(), seed=43)
        num_threads = 15

        def worker(index: int) -> None:
            request = make_request(
                ce_symbol,
                tag=f"pos-concurrent-{index}",
                quantity=LOT_SIZE,
                transaction_type=TransactionType.SELL,
            )
            result = broker.place_order(request)
            _wait_until_terminal(broker, result.broker_order_id)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        positions = broker.get_positions()
        assert len(positions) == 1  # all threads traded the same instrument
        assert positions[0].quantity == -LOT_SIZE * num_threads

    def test_all_tags_resolve_correctly_under_concurrency(self, broker_factory, ce_symbol):
        broker = broker_factory(config=_threaded_config(), seed=44)
        num_threads = 15
        placed: dict[int, str] = {}
        lock = threading.Lock()

        def worker(index: int) -> None:
            request = make_request(ce_symbol, tag=f"tag-concurrent-{index}", quantity=LOT_SIZE)
            result = broker.place_order(request)
            with lock:
                placed[index] = result.broker_order_id

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        for index, broker_order_id in placed.items():
            found = broker.find_order_by_tag(f"tag-concurrent-{index}")
            assert found is not None
            assert found.broker_order_id == broker_order_id


class TestConcurrentReadsDuringResolution:
    def test_readers_see_no_corruption_while_orders_resolve_in_background(self, broker_factory, ce_symbol):
        broker = broker_factory(
            config=_threaded_config(fill_latency_seconds=0.1, matching_tick_seconds=0.005), seed=45
        )
        stop = threading.Event()
        read_errors: list[BaseException] = []

        def reader() -> None:
            while not stop.is_set():
                try:
                    broker.get_orders()
                    broker.get_positions()
                    broker.get_margins()
                except BaseException as exc:  # noqa: BLE001
                    read_errors.append(exc)

        reader_threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in reader_threads:
            t.start()

        order_ids = []
        for i in range(10):
            request = make_request(ce_symbol, tag=f"reader-race-{i}", quantity=LOT_SIZE)
            order_ids.append(broker.place_order(request).broker_order_id)
            time.sleep(0.01)

        for order_id in order_ids:
            _wait_until_terminal(broker, order_id)

        stop.set()
        for t in reader_threads:
            t.join(timeout=5.0)

        assert read_errors == []
        orders = broker.get_orders()
        assert len(orders) == 10
        assert all(o.status == OrderStatus.COMPLETE for o in orders)


class TestSynchronousModeIsSingleThreaded:
    def test_synchronous_mode_does_not_start_a_background_thread(self, broker_factory, ce_symbol):
        broker = broker_factory(config=SimulationConfig(synchronous=True), seed=1)
        assert broker._matching_engine is None

    def test_threaded_mode_starts_a_background_thread(self, broker_factory, ce_symbol):
        broker = broker_factory(config=_threaded_config(), seed=1)
        assert broker._matching_engine is not None
        assert broker._matching_engine.is_alive()

    def test_close_stops_the_background_thread(self, catalog, price_source, ce_symbol):
        from algo.brokers.simulation import SimulationBroker
        import random

        broker = SimulationBroker(
            instrument_catalog=catalog,
            price_source=price_source,
            config=_threaded_config(),
            rng=random.Random(1),
        )
        broker.authenticate()
        engine = broker._matching_engine
        assert engine is not None
        assert engine.is_alive()

        broker.close()
        engine.join(timeout=1.0)
        assert not engine.is_alive()
