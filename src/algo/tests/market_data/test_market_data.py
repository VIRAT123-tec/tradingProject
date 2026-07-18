"""Tests for the market data layer: the MarketCache, SubscriptionManager, and
TickRouter components, and the MarketDataService facade driven against a fake
TickStream and fake LtpPoller (no network).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from algo.brokers.broker_base import InstrumentIdentifier
from algo.brokers.exceptions import BrokerConnectionError
from algo.common.enums import Exchange
from algo.market_data import (
    MarketCache,
    MarketDataConfig,
    MarketDataService,
    MarketDataUnavailableError,
    SubscriptionManager,
    TickRouter,
    is_plausible,
)
from algo.strategy_engine.strategy_context import Tick

CE = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTYCE")
PE = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="NIFTYPE")
XX = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="OTHER")

T0 = datetime(2026, 7, 7, 4, 30, tzinfo=timezone.utc)


def _tick(instrument, price, ts=T0):
    return Tick(instrument=instrument, last_price=Decimal(str(price)), timestamp=ts)


# --------------------------------------------------------------------------
# MarketCache
# --------------------------------------------------------------------------


class TestMarketCache:
    def test_update_and_get(self):
        cache = MarketCache()
        cache.update(CE, Decimal("120"), T0)
        assert cache.get_price(CE) == Decimal("120")

    def test_missing_is_none(self):
        assert MarketCache().get_price(CE) is None

    def test_freshness(self):
        cache = MarketCache()
        cache.update(CE, Decimal("120"), T0)
        assert cache.is_fresh(CE, now=T0 + timedelta(seconds=3), max_age_seconds=5)
        assert not cache.is_fresh(CE, now=T0 + timedelta(seconds=10), max_age_seconds=5)

    def test_missing_never_fresh(self):
        assert not MarketCache().is_fresh(CE, now=T0, max_age_seconds=5)

    def test_age_seconds(self):
        cache = MarketCache()
        cache.update(CE, Decimal("120"), T0)
        assert cache.age_seconds(CE, now=T0 + timedelta(seconds=4)) == 4.0
        assert cache.age_seconds(PE, now=T0) is None

    def test_remove_and_clear(self):
        cache = MarketCache()
        cache.update(CE, Decimal("120"), T0)
        cache.remove(CE)
        assert cache.get_price(CE) is None
        cache.update(PE, Decimal("110"), T0)
        cache.clear()
        assert cache.get_price(PE) is None


# --------------------------------------------------------------------------
# SubscriptionManager (reference counting)
# --------------------------------------------------------------------------


class TestSubscriptionManager:
    def test_first_subscribe_is_newly_active(self):
        sm = SubscriptionManager()
        assert sm.subscribe([CE, PE]) == [CE, PE]

    def test_duplicate_subscribe_not_newly_active(self):
        sm = SubscriptionManager()
        sm.subscribe([CE])
        assert sm.subscribe([CE]) == []  # already active -> no new ws subscribe
        assert sm.reference_count(CE) == 2

    def test_unsubscribe_only_removes_at_zero(self):
        sm = SubscriptionManager()
        sm.subscribe([CE])
        sm.subscribe([CE])  # ref count 2
        assert sm.unsubscribe([CE]) == []  # still needed by one consumer
        assert sm.is_subscribed(CE)
        assert sm.unsubscribe([CE]) == [CE]  # now truly gone
        assert not sm.is_subscribed(CE)

    def test_unsubscribe_unknown_is_noop(self):
        assert SubscriptionManager().unsubscribe([CE]) == []

    def test_active_set(self):
        sm = SubscriptionManager()
        sm.subscribe([CE, PE])
        assert set(sm.active()) == {CE, PE}


# --------------------------------------------------------------------------
# TickRouter
# --------------------------------------------------------------------------


class TestTickRouter:
    def test_broadcast_fans_out(self):
        router = TickRouter()
        a, b = [], []
        router.register(a.append)
        router.register(b.append)
        tick = _tick(CE, 120)
        router.broadcast(tick)
        assert a == [tick] and b == [tick]

    def test_failing_consumer_isolated(self):
        router = TickRouter()
        good = []
        router.register(lambda t: (_ for _ in ()).throw(RuntimeError("bug")))
        router.register(good.append)
        router.broadcast(_tick(CE, 120))  # must not raise
        assert len(good) == 1

    def test_is_plausible_rejects_non_positive(self):
        assert is_plausible(_tick(CE, 120))
        assert not is_plausible(_tick(CE, 0))
        assert not is_plausible(_tick(CE, -5))


# --------------------------------------------------------------------------
# Fakes for the service
# --------------------------------------------------------------------------


@dataclass
class MutableTime:
    now_value: datetime = T0

    def now(self) -> datetime:
        return self.now_value

    def now_ist(self) -> datetime:
        return self.now_value

    def today(self) -> date:
        return date(2026, 7, 7)


@dataclass
class FakeTickStream:
    handlers: dict = field(default_factory=dict)
    connected: bool = False
    subscribed: list = field(default_factory=list)
    unsubscribed: list = field(default_factory=list)
    started: bool = False
    subscribe_raises: bool = False

    def set_handlers(self, *, on_tick, on_connect, on_disconnect, on_reconnect):
        self.handlers = {
            "on_tick": on_tick, "on_connect": on_connect,
            "on_disconnect": on_disconnect, "on_reconnect": on_reconnect,
        }

    def start(self):
        self.started = True

    def stop(self):
        self.started = False
        self.connected = False

    def is_connected(self):
        return self.connected

    def subscribe(self, instruments):
        if self.subscribe_raises:
            raise RuntimeError("ws subscribe failed")
        self.subscribed.append(list(instruments))

    def unsubscribe(self, instruments):
        self.unsubscribed.append(list(instruments))

    # test helpers to simulate stream events
    def emit_connect(self):
        self.connected = True
        self.handlers["on_connect"]()

    def emit_reconnect(self):
        self.connected = True
        self.handlers["on_reconnect"]()

    def emit_disconnect(self):
        self.connected = False
        self.handlers["on_disconnect"]()

    def emit_tick(self, tick):
        self.handlers["on_tick"](tick)


@dataclass
class FakePoller:
    prices: dict = field(default_factory=dict)
    calls: int = 0
    raise_error: bool = False

    def get_ltp(self, instruments, *, timeout=None):
        self.calls += 1
        if self.raise_error:
            raise BrokerConnectionError("poll failed")
        return {i: self.prices[i] for i in instruments if i in self.prices}


def build_service(*, freshness=5.0, poller_prices=None, config=None):
    stream = FakeTickStream()
    poller = FakePoller(prices=poller_prices or {})
    clock = MutableTime()
    service = MarketDataService(
        tick_stream=stream, poller=poller, time_provider=clock,
        config=config or MarketDataConfig(freshness_seconds=freshness),
        sleep=lambda _: None,  # no real backoff waits in tests
    )
    return service, stream, poller, clock


# --------------------------------------------------------------------------
# MarketDataService: subscriptions
# --------------------------------------------------------------------------


class TestServiceSubscriptions:
    def test_subscribe_while_connected_hits_stream(self):
        service, stream, _, _ = build_service()
        stream.emit_connect()
        service.subscribe([CE, PE])
        assert stream.subscribed[-1] == [CE, PE]

    def test_subscribe_while_disconnected_deferred_until_connect(self):
        service, stream, _, _ = build_service()
        service.subscribe([CE, PE])  # not connected yet
        assert stream.subscribed == []
        stream.emit_connect()  # on_connect resubscribes the active set
        assert stream.subscribed[-1] == [CE, PE]

    def test_refcount_prevents_premature_unsubscribe(self):
        service, stream, _, _ = build_service()
        stream.emit_connect()
        service.subscribe([CE])
        service.subscribe([CE])  # two consumers
        service.unsubscribe([CE])
        assert stream.unsubscribed == []  # still needed
        service.unsubscribe([CE])
        assert stream.unsubscribed[-1] == [CE]  # now released

    def test_subscribe_failure_is_isolated(self):
        service, stream, _, _ = build_service()
        stream.emit_connect()
        stream.subscribe_raises = True
        service.subscribe([CE])  # must not raise


# --------------------------------------------------------------------------
# MarketDataService: live ticks + cache + broadcast
# --------------------------------------------------------------------------


class TestServiceLiveTicks:
    def test_live_tick_updates_cache_and_broadcasts(self):
        service, stream, _, _ = build_service()
        received = []
        service.register_consumer(received.append)
        stream.emit_tick(_tick(CE, 120))
        assert received[0].last_price == Decimal("120")
        assert service.get_ltp(CE) == Decimal("120")  # served from cache, no poll

    def test_implausible_tick_dropped(self):
        service, stream, poller, _ = build_service()
        received = []
        service.register_consumer(received.append)
        stream.emit_tick(_tick(CE, 0))  # zero price
        assert received == []
        assert service._cache.get_price(CE) is None


# --------------------------------------------------------------------------
# MarketDataService: polling fallback
# --------------------------------------------------------------------------


class TestServicePollingFallback:
    def test_get_ltps_serves_fresh_cache_without_polling(self):
        service, stream, poller, clock = build_service(freshness=5.0)
        stream.emit_tick(_tick(CE, 120))
        result = service.get_ltps([CE])
        assert result[CE] == Decimal("120")
        assert poller.calls == 0  # fresh cache -> no poll

    def test_get_ltps_polls_when_missing(self):
        service, stream, poller, _ = build_service(poller_prices={CE: Decimal("119")})
        result = service.get_ltps([CE])
        assert result[CE] == Decimal("119")
        assert poller.calls == 1

    def test_get_ltps_polls_when_stale(self):
        service, stream, poller, clock = build_service(freshness=5.0, poller_prices={CE: Decimal("118")})
        stream.emit_tick(_tick(CE, 120, ts=T0))
        clock.now_value = T0 + timedelta(seconds=10)  # cache now stale
        result = service.get_ltps([CE])
        assert result[CE] == Decimal("118")  # polled fresh value
        assert poller.calls == 1

    def test_get_ltp_raises_when_unavailable(self):
        service, _, _, _ = build_service(poller_prices={})  # poll returns nothing
        with pytest.raises(MarketDataUnavailableError):
            service.get_ltp(CE)

    def test_poll_failure_returns_empty_not_raise(self):
        service, stream, poller, _ = build_service()
        poller.raise_error = True
        result = service.get_ltps([CE])  # must not raise
        assert result == {}

    def test_poll_active_broadcasts_synthetic_ticks(self):
        service, stream, poller, _ = build_service(poller_prices={CE: Decimal("117"), PE: Decimal("113")})
        stream.emit_connect()
        service.subscribe([CE, PE])
        received = []
        service.register_consumer(received.append)

        priced = service.poll_active()

        assert priced == 2
        assert {r.instrument for r in received} == {CE, PE}
        assert service._cache.get_price(CE) == Decimal("117")


# --------------------------------------------------------------------------
# MarketDataService: reconnect + staleness + lifecycle
# --------------------------------------------------------------------------


class TestServiceReconnectAndLifecycle:
    def test_reconnect_resubscribes_active_set(self):
        service, stream, _, _ = build_service()
        stream.emit_connect()
        service.subscribe([CE, PE])
        stream.subscribed.clear()
        stream.emit_reconnect()  # dropped socket has no memory of subscriptions
        assert stream.subscribed[-1] == [CE, PE]  # resubscribed wholesale

    def test_is_connected_tracks_stream(self):
        service, stream, _, _ = build_service()
        assert not service.is_connected()
        stream.emit_connect()
        assert service.is_connected()
        stream.emit_disconnect()
        assert not service.is_connected()

    def test_start_stop_delegate_to_stream(self):
        service, stream, _, _ = build_service()
        service.start()
        assert stream.started
        service.stop()
        assert not stream.started

    def test_is_stale_reports_freshness(self):
        service, stream, _, clock = build_service(freshness=5.0)
        assert service.is_stale(CE)  # never priced
        stream.emit_tick(_tick(CE, 120, ts=T0))
        assert not service.is_stale(CE)
        clock.now_value = T0 + timedelta(seconds=10)
        assert service.is_stale(CE)

    def test_unsubscribe_drops_cache_entry(self):
        service, stream, _, _ = build_service()
        stream.emit_connect()
        service.subscribe([CE])
        stream.emit_tick(_tick(CE, 120))
        service.unsubscribe([CE])
        assert service._cache.get_price(CE) is None


# --------------------------------------------------------------------------
# MarketDataService: polling resilience (transient Kite REST failures)
# --------------------------------------------------------------------------

import logging

from algo.brokers.exceptions import (
    BrokerError,
    BrokerAuthenticationError,
    NonRetryableBrokerError,
)


@dataclass
class ScriptedPoller:
    """A poller that fails a configurable number of times (with a configurable
    error) before succeeding, records how many times it was called, and records
    every backoff sleep the service asked for."""

    prices: dict = field(default_factory=dict)
    fail_times: int = 0            # how many leading calls raise
    error: Exception = field(default_factory=lambda: BrokerConnectionError("503 transient"))
    always_fail: bool = False
    calls: int = 0

    def get_ltp(self, instruments, *, timeout=None):
        self.calls += 1
        if self.always_fail or self.calls <= self.fail_times:
            raise self.error
        return {i: self.prices[i] for i in instruments if i in self.prices}


def _service_with(poller, *, threshold=5, attempts=3, backoff=0.5, max_backoff=8.0):
    sleeps = []
    service = MarketDataService(
        tick_stream=FakeTickStream(),
        poller=poller,
        time_provider=MutableTime(),
        config=MarketDataConfig(
            poll_retry_attempts=attempts,
            poll_retry_backoff_seconds=backoff,
            poll_retry_max_backoff_seconds=max_backoff,
            poll_failure_escalation_threshold=threshold,
        ),
        sleep=sleeps.append,
    )
    return service, sleeps


class TestPollResilience:
    def test_transient_failure_is_retried_then_succeeds(self):
        # Fails twice (transient), succeeds on the 3rd attempt within one poll.
        poller = ScriptedPoller(prices={CE: Decimal("101")}, fail_times=2)
        service, sleeps = _service_with(poller, attempts=3)
        result = service.get_ltps([CE])
        assert result[CE] == Decimal("101")
        assert poller.calls == 3
        assert sleeps == [0.5, 1.0]  # exponential backoff between the 3 attempts

    def test_backoff_is_capped(self):
        poller = ScriptedPoller(always_fail=True)
        service, sleeps = _service_with(poller, attempts=5, backoff=4.0, max_backoff=8.0)
        service.get_ltps([CE])
        # 4, 8, capped 8, capped 8 (4 waits across 5 attempts)
        assert sleeps == [4.0, 8.0, 8.0, 8.0]

    def test_exhausted_transient_returns_empty_and_stays_quiet(self, caplog):
        poller = ScriptedPoller(always_fail=True)
        service, _ = _service_with(poller, attempts=3, threshold=5)
        with caplog.at_level(logging.DEBUG, logger="algo.market_data"):
            result = service.get_ltps([CE])
        assert result == {}
        assert poller.calls == 3  # retried up to the attempt budget
        # A single failed cycle is quiet: no ERROR/CRITICAL, no stack trace.
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_escalates_to_error_only_after_threshold_consecutive_cycles(self, caplog):
        poller = ScriptedPoller(always_fail=True)
        service, _ = _service_with(poller, attempts=1, threshold=3)
        with caplog.at_level(logging.DEBUG, logger="algo.market_data"):
            service.get_ltps([CE])  # streak 1 -> DEBUG
            service.get_ltps([CE])  # streak 2 -> DEBUG
            assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
            service.get_ltps([CE])  # streak 3 == threshold -> ERROR
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1

    def test_escalates_to_critical_at_double_threshold(self, caplog):
        poller = ScriptedPoller(always_fail=True)
        service, _ = _service_with(poller, attempts=1, threshold=2)
        with caplog.at_level(logging.DEBUG, logger="algo.market_data"):
            for _ in range(4):  # threshold=2 -> ERROR at 2, CRITICAL at 4
                service.get_ltps([CE])
        assert any(r.levelno == logging.CRITICAL for r in caplog.records)

    def test_recovery_resets_the_streak(self, caplog):
        poller = ScriptedPoller(prices={CE: Decimal("99")}, fail_times=3)
        service, _ = _service_with(poller, attempts=1, threshold=3)
        with caplog.at_level(logging.DEBUG, logger="algo.market_data"):
            service.get_ltps([CE])  # streak 1
            service.get_ltps([CE])  # streak 2
            service.get_ltps([CE])  # streak 3 -> ERROR
            recovered = service.get_ltps([CE])  # 4th call succeeds -> reset + INFO
            service.get_ltps([CE])  # would raise again... but prices now return; stays success
        assert recovered[CE] == Decimal("99")
        assert any("recovered" in r.message for r in caplog.records)

    def test_non_retryable_error_is_not_retried(self):
        poller = ScriptedPoller(always_fail=True, error=BrokerAuthenticationError("expired token"))
        service, sleeps = _service_with(poller, attempts=3)
        result = service.get_ltps([CE])
        assert result == {}
        assert poller.calls == 1  # not retried -- retrying an auth failure can't help
        assert sleeps == []

    def test_plain_broker_error_like_json_parse_is_treated_transient(self):
        # A JSON-parse failure reaches the poll layer as a plain BrokerError
        # (KiteMappingError), NOT a RetryableBrokerError -- it must still retry.
        poller = ScriptedPoller(prices={CE: Decimal("100")}, fail_times=1,
                                error=BrokerError("could not parse JSON response"))
        service, _ = _service_with(poller, attempts=3)
        result = service.get_ltps([CE])
        assert result[CE] == Decimal("100")
        assert poller.calls == 2  # retried once, then succeeded

    def test_unexpected_error_is_guarded_logged_and_not_retried(self, caplog):
        poller = ScriptedPoller(always_fail=True, error=ValueError("bug"))
        service, sleeps = _service_with(poller, attempts=3)
        with caplog.at_level(logging.DEBUG, logger="algo.market_data"):
            result = service.get_ltps([CE])  # must not raise
        assert result == {}
        assert poller.calls == 1  # unexpected errors are not retried
        assert any(r.levelno == logging.ERROR for r in caplog.records)
