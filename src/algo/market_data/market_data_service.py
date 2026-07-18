"""MarketDataService: the facade that ties the market-data layer together and
implements the ``MarketDataGateway`` seam the strategy monitor depends on.

It composes the small single-purpose pieces -- the latest-price ``MarketCache``,
the reference-counted ``SubscriptionManager``, the ``TickRouter`` broadcaster --
over two injected broker-agnostic seams: a ``TickStream`` (the live websocket)
and an ``LtpPoller`` (the pull-based fallback, satisfied by ``BrokerBase``).
It contains no broker-specific code; all Kite knowledge stays in the injected
adapters, so the whole service is unit-testable against fakes with no network.

Responsibilities: live ticks in (via the stream) update the cache and fan out to
consumers; ``get_ltp``/``get_ltps`` serve from the cache when fresh and
transparently fall back to polling when the feed is stale or down; subscriptions
are reference-counted and the minimal websocket deltas issued; a reconnect
triggers a wholesale re-subscribe; and staleness is observable per instrument.
It owns no thread and no loop -- the stream runs on its own callback thread, and
the optional periodic ``poll_active`` refresh is driven by an external scheduler.
Trading logic lives entirely elsewhere.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from algo.brokers.exceptions import BrokerError, NonRetryableBrokerError
from algo.market_data.market_cache import MarketCache
from algo.market_data.subscription_manager import SubscriptionManager
from algo.market_data.tick_router import TickRouter, is_plausible
from algo.strategy_engine.strategy_context import Tick

if TYPE_CHECKING:
    from collections.abc import Callable
    from decimal import Decimal

    from algo.brokers.broker_base import InstrumentIdentifier
    from algo.market_data.websocket_manager import LtpPoller, TickStream
    from algo.strategy_engine.strategy_context import TimeProvider

    TickConsumer = Callable[[Tick], None]


class MarketDataUnavailableError(BrokerError):
    """A requested last price could not be obtained -- neither from a fresh
    cache entry nor from a polling fallback (feed down and poll failed/empty)."""


class MarketDataConfig(BaseModel):
    """Tuning for the market-data service."""

    model_config = ConfigDict(frozen=True)

    freshness_seconds: float = Field(
        default=5.0, gt=0,
        description="A cached price younger than this is served directly; an "
        "older (or missing) one triggers a polling fallback on read. Also the "
        "threshold ``is_stale`` reports against.",
    )
    poll_timeout_seconds: float | None = Field(
        default=None,
        description="Per-call timeout passed to the poller's get_ltp on a "
        "fallback poll. None uses the broker's own configured default.",
    )
    poll_retry_attempts: int = Field(
        default=3, ge=1,
        description="Total attempts for a single fallback poll when it hits a "
        "transient error (1 = no retry). Retries use exponential backoff. A poll "
        "is a side-effect-free read, so retrying is always safe.",
    )
    poll_retry_backoff_seconds: float = Field(
        default=0.5, gt=0,
        description="Base wait before the first poll retry; each subsequent wait "
        "doubles (exponential backoff), capped at poll_retry_max_backoff_seconds.",
    )
    poll_retry_max_backoff_seconds: float = Field(
        default=8.0, gt=0,
        description="Upper bound on any single backoff wait between poll retries.",
    )
    poll_failure_escalation_threshold: int = Field(
        default=5, ge=1,
        description="Number of *consecutive* failed poll cycles tolerated quietly "
        "(logged at DEBUG) before the situation is escalated to an ERROR-level "
        "alert -- and to CRITICAL at twice this many. A single transient blip "
        "never produces an ERROR or a stack trace.",
    )


class MarketDataService:
    """Live market data with a transparent polling fallback, implementing the
    ``MarketDataGateway`` contract (subscribe/unsubscribe/get_ltp/get_ltps/
    is_connected) plus a consumer-broadcast and periodic-poll surface."""

    def __init__(
        self,
        *,
        tick_stream: TickStream,
        poller: LtpPoller,
        time_provider: TimeProvider,
        config: MarketDataConfig | None = None,
        cache: MarketCache | None = None,
        subscriptions: SubscriptionManager | None = None,
        router: TickRouter | None = None,
        logger: logging.Logger | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._stream = tick_stream
        self._poller = poller
        self._time = time_provider
        self._config = config or MarketDataConfig()
        self._cache = cache or MarketCache()
        self._subscriptions = subscriptions or SubscriptionManager()
        self._router = router or TickRouter(logger=logger)
        self._logger = logger if logger is not None else logging.getLogger("algo.market_data")
        self._sleep = sleep
        # Counts *consecutive* failed poll cycles so a persistent outage can be
        # escalated once (ERROR/CRITICAL) while single transient blips stay
        # quiet. Guarded because _poll runs from both the strategy read path and
        # the monitoring-scheduler thread.
        self._poll_failure_lock = threading.Lock()
        self._consecutive_poll_failures = 0

        self._stream.set_handlers(
            on_tick=self._on_tick,
            on_connect=self._on_connect,
            on_disconnect=self._on_disconnect,
            on_reconnect=self._on_reconnect,
        )

    # -- Lifecycle -----------------------------------------------------

    def start(self) -> None:
        """Open the websocket feed. Subscriptions already registered (or added
        later) are (re)applied on connect."""
        self._stream.start()

    def stop(self) -> None:
        """Close the websocket feed. Does not clear subscriptions or cache --
        a subsequent ``start`` resumes from the same subscription set."""
        self._stream.stop()

    def is_connected(self) -> bool:
        """Whether the live feed is currently connected (MarketDataGateway)."""
        return self._stream.is_connected()

    # -- Consumers -----------------------------------------------------

    def register_consumer(self, consumer: TickConsumer) -> None:
        """Register a callback invoked (fan-out) with every accepted tick,
        whether it arrived live over the websocket or was synthesised by a
        polling refresh. Multiple consumers supported."""
        self._router.register(consumer)

    # -- Subscriptions (MarketDataGateway) -----------------------------

    def subscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        """Register interest in these instruments. Only instruments not already
        subscribed by another consumer are actually subscribed at the websocket
        (reference counting); a subscribe issued while disconnected is remembered
        and applied on the next connect."""
        newly_active = self._subscriptions.subscribe(instruments)
        if newly_active and self._stream.is_connected():
            self._safe_stream_subscribe(newly_active)

    def unsubscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        """Drop interest in these instruments. Only instruments no consumer
        needs any more are actually unsubscribed at the websocket; their cache
        entries are dropped."""
        newly_inactive = self._subscriptions.unsubscribe(instruments)
        if newly_inactive:
            if self._stream.is_connected():
                self._safe_stream_unsubscribe(newly_inactive)
            for instrument in newly_inactive:
                self._cache.remove(instrument)

    # -- Price reads (MarketDataGateway) -------------------------------

    def get_ltp(self, instrument: InstrumentIdentifier) -> Decimal:
        """Latest price for one instrument, preferring a fresh cached tick and
        falling back to a poll. Raises ``MarketDataUnavailableError`` if neither
        yields a value."""
        result = self.get_ltps([instrument])
        try:
            return result[instrument]
        except KeyError:
            raise MarketDataUnavailableError(
                f"no live or polled price available for {instrument}"
            ) from None

    def get_ltps(
        self, instruments: list[InstrumentIdentifier]
    ) -> dict[InstrumentIdentifier, Decimal]:
        """Latest prices for a batch. Each instrument is served from the cache if
        the cached tick is fresh; every stale/missing one is polled in a single
        batched fallback call, the cache updated, and the merged result returned.
        The result may be partial if a poll cannot price some instrument."""
        now = self._time.now()
        result: dict[InstrumentIdentifier, Decimal] = {}
        to_poll: list[InstrumentIdentifier] = []
        for instrument in instruments:
            entry = self._cache.get(instrument)
            if entry is not None and self._cache.is_fresh(
                instrument, now=now, max_age_seconds=self._config.freshness_seconds
            ):
                result[instrument] = entry.price
            else:
                to_poll.append(instrument)

        if to_poll:
            polled = self._poll(to_poll)
            result.update(polled)
        return result

    # -- Polling fallback ----------------------------------------------

    def poll_active(self) -> int:
        """Poll every currently-subscribed instrument, update the cache, and
        broadcast a synthesised tick for each -- the periodic fallback an
        external scheduler drives while the websocket feed is degraded, so
        consumers that rely on broadcast ticks keep receiving updates even with
        no live feed. Returns the number of instruments successfully priced."""
        active = self._subscriptions.active()
        if not active:
            return 0
        polled = self._poll(active, broadcast=True)
        return len(polled)

    def is_stale(self, instrument: InstrumentIdentifier) -> bool:
        """Whether we have no price for an instrument fresher than the configured
        freshness window -- the staleness signal a watchdog/monitor can act on."""
        return not self._cache.is_fresh(
            instrument, now=self._time.now(), max_age_seconds=self._config.freshness_seconds
        )

    def age_seconds(self, instrument: InstrumentIdentifier) -> float | None:
        """Seconds since the last recorded price for an instrument, or None if
        never priced."""
        return self._cache.age_seconds(instrument, now=self._time.now())

    def _poll(
        self, instruments: list[InstrumentIdentifier], *, broadcast: bool = False
    ) -> dict[InstrumentIdentifier, Decimal]:
        """Poll a batch via the fallback poller, updating the cache (and,
        optionally, broadcasting synthesised ticks). Never raises -- the
        price-independent callers (time cutoff, kill-switch) must keep working
        when the feed is down; a total failure yields an empty result."""
        polled = self._resilient_poll(instruments)
        if not polled:
            return {}
        now = self._time.now()
        for instrument, price in polled.items():
            self._cache.update(instrument, price, now)
            if broadcast and price > 0:
                self._router.broadcast(Tick(instrument=instrument, last_price=price, timestamp=now))
        return polled

    def _resilient_poll(
        self, instruments: list[InstrumentIdentifier]
    ) -> dict[InstrumentIdentifier, Decimal] | None:
        """Call the fallback poller, absorbing transient Kite REST failures.

        A poll is a side-effect-free read, so any failure that is not a
        definitive ``NonRetryableBrokerError`` (an expired session, an unknown
        instrument) is retried with exponential backoff. This covers the whole
        family of transient REST failures uniformly -- HTTP 500/502/503/504,
        read timeouts, dropped connections, and malformed/JSON-parse responses
        -- regardless of whether the broker classified them as retryable or left
        them as a generic ``BrokerError`` (a JSON-parse failure, for instance,
        arrives here as a plain ``BrokerError``, not a ``RetryableBrokerError``).

        Returns the priced dict on success (possibly partial or empty) or
        ``None`` if every attempt failed. Logging severity is driven by how many
        *consecutive* cycles have failed, not by any single failure: a lone blip
        is logged at DEBUG with no stack trace; only a sustained outage crossing
        the configured threshold escalates to ERROR (then CRITICAL). It never
        raises.
        """
        attempts = self._config.poll_retry_attempts
        backoff = self._config.poll_retry_backoff_seconds
        last_error: BaseException | None = None
        unexpected = False
        for attempt in range(attempts):
            try:
                polled = self._poller.get_ltp(
                    instruments, timeout=self._config.poll_timeout_seconds
                )
            except NonRetryableBrokerError as exc:
                # Auth / unknown instrument: retrying cannot help.
                last_error = exc
                break
            except BrokerError as exc:
                # Transient (5xx, timeout, connection drop, malformed response):
                # back off and try again while attempts remain.
                last_error = exc
            except Exception as exc:  # noqa: BLE001 -- last-resort guard; never crash a caller
                # Not even a translated broker error: unexpected. Don't retry a
                # possible bug, but still don't propagate into the exit path.
                last_error, unexpected = exc, True
                break
            else:
                self._note_poll_recovered()
                return polled
            if attempt < attempts - 1:
                self._sleep(min(backoff, self._config.poll_retry_max_backoff_seconds))
                backoff *= 2
        self._note_poll_failure(last_error, len(instruments), unexpected=unexpected)
        return None

    def _note_poll_recovered(self) -> None:
        """Reset the consecutive-failure count after a successful poll, and note
        recovery if we had previously escalated."""
        with self._poll_failure_lock:
            prior = self._consecutive_poll_failures
            self._consecutive_poll_failures = 0
        if prior >= self._config.poll_failure_escalation_threshold:
            self._logger.info(
                "market-data pull fallback recovered after %d consecutive failures", prior
            )

    def _note_poll_failure(
        self, error: BaseException | None, count: int, *, unexpected: bool
    ) -> None:
        """Record one failed poll cycle and log it at a severity that grows only
        with *consecutive* failures -- so single transient blips never produce an
        ERROR or a stack trace, and a sustained outage escalates exactly once."""
        with self._poll_failure_lock:
            self._consecutive_poll_failures += 1
            consecutive = self._consecutive_poll_failures
        threshold = self._config.poll_failure_escalation_threshold

        if unexpected:
            # An untranslated error is a genuine surprise -- always worth an
            # ERROR with a stack trace, whatever the streak.
            self._logger.error(
                "market-data poll hit an unexpected error for %d instruments; "
                "continuing on last-known / websocket data",
                count, exc_info=error,
            )
        elif consecutive == threshold:
            self._logger.error(
                "market-data pull fallback has failed %d consecutive times for %d "
                "instruments; the poll feed appears down. Price-based exits "
                "(stop-loss / target) may lag until it recovers -- price-independent "
                "exits (hard cutoff, kill-switch) are unaffected. Last error: %s",
                consecutive, count, error,
            )
        elif consecutive == threshold * 2:
            self._logger.critical(
                "market-data pull fallback has failed %d consecutive times for %d "
                "instruments; feed persistently down. Last error: %s",
                consecutive, count, error,
            )
        else:
            # Single/brief transient failure (or between escalation points):
            # quiet, no stack trace.
            self._logger.debug(
                "market-data poll transient failure (%d consecutive) for %d "
                "instruments: %s",
                consecutive, count, error,
            )

    # -- Websocket handlers (called from the stream's thread) ----------

    def _on_tick(self, tick: Tick) -> None:
        """A live tick: sanity-gate it, update the cache, fan it out. A rejected
        (implausible) tick is dropped -- never cached, never broadcast."""
        if not is_plausible(tick):
            self._logger.warning("dropping implausible tick for %s: %s", tick.instrument, tick.last_price)
            return
        self._cache.update(tick.instrument, tick.last_price, self._time.now())
        self._router.broadcast(tick)

    def _on_connect(self) -> None:
        self._logger.info("market-data websocket connected")
        self._resubscribe_all()

    def _on_disconnect(self) -> None:
        self._logger.warning("market-data websocket disconnected; reads will poll as fallback")

    def _on_reconnect(self) -> None:
        """A dropped connection was re-established. The socket has no memory of
        prior subscriptions, so re-subscribe the whole active set."""
        self._logger.info("market-data websocket reconnected; re-subscribing active instruments")
        self._resubscribe_all()

    def _resubscribe_all(self) -> None:
        active = self._subscriptions.active()
        if active:
            self._safe_stream_subscribe(active)

    # -- Guards --------------------------------------------------------

    def _safe_stream_subscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        try:
            self._stream.subscribe(instruments)
        except Exception:  # noqa: BLE001 -- a subscribe failure must not crash the caller
            self._logger.error("websocket subscribe failed for %d instruments", len(instruments), exc_info=True)

    def _safe_stream_unsubscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        try:
            self._stream.unsubscribe(instruments)
        except Exception:  # noqa: BLE001
            self._logger.error("websocket unsubscribe failed for %d instruments", len(instruments), exc_info=True)
