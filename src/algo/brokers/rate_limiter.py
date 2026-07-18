"""Rate-limiting decorator for any BrokerBase implementation.

Wraps a concrete broker (Kite or Simulation) and throttles calls per
RateLimitCategory before delegating to it. The point of doing this as a
decorator around BrokerBase, rather than inside each concrete broker, is
structural: the dependency container hands execution/, risk/, and
strategy_engine/ nothing but a BrokerBase reference, so if the container
always wraps the concrete broker in this class, callers have no path to
bypass throttling -- not even by accident.

Self-throttles by sleeping wherever possible, since the goal is to keep the
platform under the broker's documented limits automatically. It only raises
BrokerRateLimitExceededError when waiting would blow the caller's own
`timeout` budget -- at that point the request was never sent, so raising
(rather than sleeping past the deadline) is unambiguously safe to retry.

Limits are supplied via RateLimitConfig, loaded from brokers.yaml -- never
hardcoded here, since a broker's documented limits can change and can differ
between paper and live accounts.
"""

from __future__ import annotations

import threading
import time
from datetime import date
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

from algo.brokers.broker_base import (
    BrokerBase,
    BrokerHolding,
    BrokerInstrument,
    BrokerMargins,
    BrokerOrder,
    BrokerPosition,
    BrokerQuote,
    HealthStatus,
    InstrumentIdentifier,
    ModifyOrderRequest,
    OrderUpdateCallback,
    PlaceOrderRequest,
    PlaceOrderResult,
)
from algo.brokers.exceptions import BrokerRateLimitExceededError
from algo.common.enums import BrokerName, Exchange, OptionType


class RateLimitCategory(str, Enum):
    """Groups BrokerBase methods that share one throttling budget, at the
    granularity that matters operationally: order-mutating calls are the
    ones a runaway loop could turn into a real exchange/compliance problem,
    so they get their own strict budget separate from cheap, high-volume
    reads.
    """

    ORDER_MUTATION = "ORDER_MUTATION"
    ORDER_READ = "ORDER_READ"
    PORTFOLIO_READ = "PORTFOLIO_READ"
    MARKET_DATA = "MARKET_DATA"
    INSTRUMENT_LOOKUP = "INSTRUMENT_LOOKUP"
    GENERAL = "GENERAL"


class RateLimitRule(BaseModel):
    """At most `max_calls` calls per rolling `per_seconds` window."""

    model_config = ConfigDict(frozen=True)

    max_calls: int = Field(gt=0)
    per_seconds: float = Field(gt=0)


class RateLimitConfig(BaseModel):
    """Per-category rules. A category with no rule configured is left
    unthrottled by RateLimitedBroker -- callers should treat an incomplete
    config as a configuration bug, not rely on the fallback."""

    model_config = ConfigDict(frozen=True)

    rules: dict[RateLimitCategory, RateLimitRule]


class _TokenBucket:
    """Thread-safe sliding-window limiter for one category.

    A lock-protected list of call timestamps, pruned on each acquire, is
    enough here: call volumes at this layer (tens per second at most) are far
    too low for the O(n) prune-then-count to matter.
    """

    def __init__(self, rule: RateLimitRule) -> None:
        self._max_calls = rule.max_calls
        self._per_seconds = rule.per_seconds
        self._calls: list[float] = []
        self._lock = threading.Lock()

    def acquire(self, *, deadline: float | None) -> None:
        """Block until a slot is available, or raise
        BrokerRateLimitExceededError if no slot will free before `deadline`
        (a time.monotonic() value)."""
        while True:
            with self._lock:
                now = time.monotonic()
                window_start = now - self._per_seconds
                self._calls = [t for t in self._calls if t > window_start]
                if len(self._calls) < self._max_calls:
                    self._calls.append(now)
                    return
                wait_until = self._calls[0] + self._per_seconds

            if deadline is not None and wait_until > deadline:
                raise BrokerRateLimitExceededError(
                    f"Rate limit exceeded ({self._max_calls} calls / "
                    f"{self._per_seconds}s) and no slot will free within the "
                    "caller's timeout budget."
                )
            time.sleep(max(0.0, wait_until - time.monotonic()))


class RateLimitedBroker(BrokerBase):
    """Delegates every BrokerBase method to `inner`, throttling first.

    Lifecycle methods (authenticate, health_check) are throttled under
    GENERAL; websocket lifecycle and the cheap local state checks
    (is_authenticated, is_websocket_connected) are never throttled -- they
    are not broker API calls with a documented rate limit.
    """

    _METHOD_CATEGORIES: dict[str, RateLimitCategory] = {
        "authenticate": RateLimitCategory.GENERAL,
        "health_check": RateLimitCategory.GENERAL,
        "place_order": RateLimitCategory.ORDER_MUTATION,
        "modify_order": RateLimitCategory.ORDER_MUTATION,
        "cancel_order": RateLimitCategory.ORDER_MUTATION,
        "get_order": RateLimitCategory.ORDER_READ,
        "get_orders": RateLimitCategory.ORDER_READ,
        "find_order_by_tag": RateLimitCategory.ORDER_READ,
        "get_positions": RateLimitCategory.PORTFOLIO_READ,
        "get_holdings": RateLimitCategory.PORTFOLIO_READ,
        "get_margins": RateLimitCategory.PORTFOLIO_READ,
        "get_quote": RateLimitCategory.MARKET_DATA,
        "get_ltp": RateLimitCategory.MARKET_DATA,
        "get_instrument": RateLimitCategory.INSTRUMENT_LOOKUP,
        "find_option_contract": RateLimitCategory.INSTRUMENT_LOOKUP,
    }

    def __init__(self, inner: BrokerBase, config: RateLimitConfig) -> None:
        self._inner = inner
        self._buckets = {
            category: _TokenBucket(rule) for category, rule in config.rules.items()
        }

    def _throttle(self, method_name: str, timeout: float | None) -> None:
        category = self._METHOD_CATEGORIES[method_name]
        bucket = self._buckets.get(category)
        if bucket is None:
            return
        deadline = None if timeout is None else time.monotonic() + timeout
        bucket.acquire(deadline=deadline)

    @property
    def broker_name(self) -> BrokerName:
        return self._inner.broker_name

    def authenticate(self, *, timeout: float | None = None) -> None:
        self._throttle("authenticate", timeout)
        self._inner.authenticate(timeout=timeout)

    def is_authenticated(self) -> bool:
        return self._inner.is_authenticated()

    def close(self) -> None:
        self._inner.close()

    def health_check(self, *, timeout: float | None = None) -> HealthStatus:
        self._throttle("health_check", timeout)
        return self._inner.health_check(timeout=timeout)

    def place_order(
        self, request: PlaceOrderRequest, *, timeout: float | None = None
    ) -> PlaceOrderResult:
        self._throttle("place_order", timeout)
        return self._inner.place_order(request, timeout=timeout)

    def modify_order(
        self, request: ModifyOrderRequest, *, timeout: float | None = None
    ) -> None:
        self._throttle("modify_order", timeout)
        self._inner.modify_order(request, timeout=timeout)

    def cancel_order(self, broker_order_id: str, *, timeout: float | None = None) -> None:
        self._throttle("cancel_order", timeout)
        self._inner.cancel_order(broker_order_id, timeout=timeout)

    def get_order(self, broker_order_id: str, *, timeout: float | None = None) -> BrokerOrder:
        self._throttle("get_order", timeout)
        return self._inner.get_order(broker_order_id, timeout=timeout)

    def get_orders(self, *, timeout: float | None = None) -> list[BrokerOrder]:
        self._throttle("get_orders", timeout)
        return self._inner.get_orders(timeout=timeout)

    def find_order_by_tag(
        self, tag: str, *, timeout: float | None = None
    ) -> BrokerOrder | None:
        self._throttle("find_order_by_tag", timeout)
        return self._inner.find_order_by_tag(tag, timeout=timeout)

    def get_positions(self, *, timeout: float | None = None) -> list[BrokerPosition]:
        self._throttle("get_positions", timeout)
        return self._inner.get_positions(timeout=timeout)

    def get_holdings(self, *, timeout: float | None = None) -> list[BrokerHolding]:
        self._throttle("get_holdings", timeout)
        return self._inner.get_holdings(timeout=timeout)

    def get_margins(self, *, timeout: float | None = None) -> BrokerMargins:
        self._throttle("get_margins", timeout)
        return self._inner.get_margins(timeout=timeout)

    def get_quote(
        self, instruments: list[InstrumentIdentifier], *, timeout: float | None = None
    ) -> dict[InstrumentIdentifier, BrokerQuote]:
        self._throttle("get_quote", timeout)
        return self._inner.get_quote(instruments, timeout=timeout)

    def get_ltp(
        self, instruments: list[InstrumentIdentifier], *, timeout: float | None = None
    ) -> dict[InstrumentIdentifier, Decimal]:
        self._throttle("get_ltp", timeout)
        return self._inner.get_ltp(instruments, timeout=timeout)

    def get_instrument(
        self, exchange: Exchange, tradingsymbol: str, *, timeout: float | None = None
    ) -> BrokerInstrument:
        self._throttle("get_instrument", timeout)
        return self._inner.get_instrument(exchange, tradingsymbol, timeout=timeout)

    def find_option_contract(
        self,
        *,
        underlying: str,
        expiry: date,
        strike: Decimal,
        option_type: OptionType,
        exchange: Exchange,
        timeout: float | None = None,
    ) -> BrokerInstrument:
        self._throttle("find_option_contract", timeout)
        return self._inner.find_option_contract(
            underlying=underlying,
            expiry=expiry,
            strike=strike,
            option_type=option_type,
            exchange=exchange,
            timeout=timeout,
        )

    def connect_websocket(self, *, timeout: float | None = None) -> None:
        self._inner.connect_websocket(timeout=timeout)

    def disconnect_websocket(self) -> None:
        self._inner.disconnect_websocket()

    def is_websocket_connected(self) -> bool:
        return self._inner.is_websocket_connected()

    def register_order_update_callback(self, callback: OrderUpdateCallback) -> None:
        self._inner.register_order_update_callback(callback)
