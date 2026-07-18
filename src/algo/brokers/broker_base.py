"""Abstract broker interface that kite/ and simulation/ both implement identically,
so strategy code never knows which concrete broker it's talking to.

Design contract (read before implementing a concrete broker):

1. Synchronous, deliberately. Every method here is a plain ``def``, never
   ``async def``, matching the platform's existing sync-SQLAlchemy,
   thread-based concurrency model (the websocket already runs in its own
   thread rather than an event loop). A future implementation that wants
   async internals (e.g. an async HTTP client for concurrent requests) must
   bridge internally -- run its own event loop on a background thread and
   cross back via ``asyncio.run_coroutine_threadsafe(...).result(timeout=...)``
   -- and still expose this exact synchronous surface. This interface must
   never become ``async def``; if the platform ever needs async at the call
   site (execution/, risk/ becoming async themselves), that is a new,
   parallel ``AsyncBrokerBase`` sharing the DTOs and exceptions below, not a
   change to this class.

2. DTOs only, never ORM models. Nothing here imports from ``algo.database`` --
   that dependency arrow only ever points one way (database -> ... ->
   execution), and this module must not point it backwards. Translating
   between a ``PlaceOrderResult`` and an ``OrderIntent``/``Order`` row is
   execution/'s job, not this layer's.

3. The broker is not the system of record. get_orders() returns *today's*
   broker orderbook, full stop -- never a historical query. Trading history
   is always read from the database repository layer.

4. Idempotency is supported, not guaranteed. Kite does not enforce tag
   uniqueness server-side, so this interface cannot manufacture a guarantee
   the broker itself doesn't provide. What it does provide: every
   PlaceOrderRequest carries the caller's idempotency tag through to the
   broker, and find_order_by_tag() lets a caller ask "did this already
   happen?" before deciding whether a retry is safe. The actual dedup
   guarantee lives in the database's order_intents unique constraint.

5. Rate limiting is enforced by wrapping a concrete broker in
   ``brokers.rate_limiter.RateLimitedBroker``, not by anything in this class.
   Each abstract method's docstring names its RateLimitCategory so that
   wrapper's per-category throttling lines up with the methods it guards;
   look there for the actual limiter implementation.

6. Partial fills are a normal, expected state, not an edge case -- see
   BrokerOrder's docstring below before writing any code that branches on
   ``status`` without also reading ``filled_quantity``.
"""

from __future__ import annotations

import abc
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from algo.common.enums import (
    BrokerName,
    Exchange,
    OptionType,
    OrderStatus,
    OrderType,
    ProductType,
    TransactionType,
)

# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------


class InstrumentIdentifier(BaseModel):
    """Hashable key identifying one tradable instrument, used to batch quote/
    LTP lookups without relying on a stringly-typed "EXCHANGE:SYMBOL" key
    that a typo could silently turn into a missing dict entry."""

    model_config = ConfigDict(frozen=True)

    exchange: Exchange
    tradingsymbol: str


class BrokerInstrument(BaseModel):
    """Result of an instrument lookup: resolves logical option parameters
    (underlying/expiry/strike/option_type) or a known tradingsymbol into
    everything needed to place an order against it."""

    model_config = ConfigDict(frozen=True)

    instrument_token: int
    exchange: Exchange
    tradingsymbol: str
    name: str
    lot_size: int = Field(gt=0)
    tick_size: Decimal
    expiry: date | None = None
    strike: Decimal | None = None
    option_type: OptionType | None = None


class PlaceOrderRequest(BaseModel):
    """Input to place_order. ``tag`` carries the caller's idempotency key
    (the OrderIntent.broker_tag, <=20 chars) through to the broker so
    find_order_by_tag() can later reconcile it."""

    model_config = ConfigDict(frozen=True)

    exchange: Exchange
    tradingsymbol: str
    transaction_type: TransactionType
    quantity: int = Field(gt=0)
    product: ProductType
    order_type: OrderType
    price: Decimal | None = None
    trigger_price: Decimal | None = None
    tag: str = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def _check_price_matches_order_type(self) -> "PlaceOrderRequest":
        if self.order_type == OrderType.LIMIT and self.price is None:
            raise ValueError("price is required when order_type is LIMIT")
        if self.order_type == OrderType.MARKET and self.price is not None:
            raise ValueError("price must be None when order_type is MARKET")
        return self


class PlaceOrderResult(BaseModel):
    """Result of place_order. Deliberately NOT a full BrokerOrder -- status
    is genuinely unknown synchronously; the broker only ever acks with an id.
    """

    model_config = ConfigDict(frozen=True)

    broker_order_id: str
    raw_response: dict[str, Any] = Field(default_factory=dict)


class ModifyOrderRequest(BaseModel):
    """Input to modify_order. All fields but the target order id are
    optional -- omit whatever the broker's own modify semantics leave
    unchanged."""

    model_config = ConfigDict(frozen=True)

    broker_order_id: str
    quantity: int | None = Field(default=None, gt=0)
    price: Decimal | None = None
    trigger_price: Decimal | None = None
    order_type: OrderType | None = None


class BrokerOrder(BaseModel):
    """A point-in-time snapshot of one broker order.

    Partial fills: there is no separate "PARTIAL" status. An order that is
    still working and has partially filled reports status=OPEN with
    0 < filled_quantity < quantity; a fully filled order reports
    status=COMPLETE with filled_quantity == quantity. average_price is the
    volume-weighted average price of the filled portion only, and is None
    until at least one fill has occurred.

    A terminal status of CANCELLED or REJECTED does NOT imply
    filled_quantity == 0 -- a partially filled order can have its unfilled
    remainder cancelled (by the exchange at session end, or by an explicit
    cancel_order call), leaving the filled portion as a real, standing
    position. Always read filled_quantity to determine what actually
    executed; never infer executed size from status alone.
    """

    model_config = ConfigDict(frozen=True)

    broker_order_id: str
    tag: str | None = None
    status: OrderStatus
    exchange: Exchange
    tradingsymbol: str
    transaction_type: TransactionType
    product: ProductType
    order_type: OrderType
    quantity: int = Field(gt=0)
    filled_quantity: int = Field(ge=0)
    average_price: Decimal | None = None
    trigger_price: Decimal | None = None
    placed_at: datetime | None = None
    filled_at: datetime | None = None
    exchange_updated_at: datetime | None = (
        None  # last update time the exchange reported; use to detect stale/out-of-order callbacks
    )
    status_message: str | None = None
    raw_response: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check_filled_within_bounds(self) -> "BrokerOrder":
        if self.filled_quantity > self.quantity:
            raise ValueError(
                f"filled_quantity ({self.filled_quantity}) cannot exceed "
                f"quantity ({self.quantity})"
            )
        return self


OrderUpdateCallback = Callable[[BrokerOrder], None]


class BrokerPosition(BaseModel):
    """One row of the broker's live net-position feed (today's F&O/equity
    exposure) -- distinct from the database's Position model, which is this
    platform's own persisted per-instrument strategy state machine."""

    model_config = ConfigDict(frozen=True)

    exchange: Exchange
    tradingsymbol: str
    product: ProductType
    quantity: int
    average_price: Decimal
    last_price: Decimal
    pnl: Decimal


class BrokerHolding(BaseModel):
    """One row of demat (T+1 settled) holdings -- a separate concept from
    BrokerPosition in Kite's own model, kept separate here too."""

    model_config = ConfigDict(frozen=True)

    exchange: Exchange
    tradingsymbol: str
    quantity: int
    average_price: Decimal
    last_price: Decimal


class BrokerMargins(BaseModel):
    """Funds/margin snapshot for one segment (e.g. equity or commodity)."""

    model_config = ConfigDict(frozen=True)

    segment: str
    available_cash: Decimal
    used_margin: Decimal
    opening_balance: Decimal


class BrokerQuote(BaseModel):
    """Full quote for one instrument."""

    model_config = ConfigDict(frozen=True)

    instrument: InstrumentIdentifier
    last_price: Decimal
    ohlc_open: Decimal
    ohlc_high: Decimal
    ohlc_low: Decimal
    ohlc_close: Decimal
    volume: int
    timestamp: datetime


class HealthStatus(BaseModel):
    """Result of an active health probe (distinct from the cheap, local,
    no-I/O is_authenticated()/is_websocket_connected() state checks)."""

    model_config = ConfigDict(frozen=True)

    healthy: bool
    checked_at: datetime
    latency_ms: float | None = None
    detail: str | None = None


# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------


class BrokerBase(abc.ABC):
    """Abstract broker interface. See the module docstring for the design
    contract (sync-only, DTOs-only, idempotency, rate-limit categories,
    partial-fill semantics) that every method below is written against.
    """

    @property
    @abc.abstractmethod
    def broker_name(self) -> BrokerName:
        """Which broker this instance talks to. No I/O; used for logging and
        by the DI container to verify it wired up what config asked for."""
        raise NotImplementedError

    @abc.abstractmethod
    def authenticate(self, *, timeout: float | None = None) -> None:
        """Establish/validate this broker session using credentials already
        supplied at construction time.

        This is NOT the interactive login flow (Kite's request_token
        exchange lives in kite/kite_auth.py + scripts/generate_token.py,
        producing an access token this method then activates) -- it must
        never require user interaction.

        Raises BrokerAuthenticationError if the credentials are invalid/
        expired. Rate-limit category: GENERAL.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def is_authenticated(self) -> bool:
        """Cheap local check: was authenticate() successful and has nothing
        since invalidated it. No I/O -- use health_check() to actively probe
        the broker instead."""
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        """Release resources (disconnect the websocket if connected, close
        any HTTP session) for graceful shutdown."""
        raise NotImplementedError

    @abc.abstractmethod
    def health_check(self, *, timeout: float | None = None) -> HealthStatus:
        """Actively probe the broker (e.g. a lightweight profile/margins
        call) and report round-trip health. Safe to poll periodically from a
        monitoring loop. Rate-limit category: GENERAL.
        """
        raise NotImplementedError

    # -- Orders --------------------------------------------------------

    @abc.abstractmethod
    def place_order(
        self, request: PlaceOrderRequest, *, timeout: float | None = None
    ) -> PlaceOrderResult:
        """Place an order.

        AMBIGUOUS-OUTCOME CONTRACT: if this call raises BrokerTimeoutError or
        BrokerConnectionError, whether the order reached the broker is
        UNKNOWN. Callers must never blindly retry this method on those
        exceptions -- resolve the ambiguity first via find_order_by_tag()
        using request.tag (this is exactly the PENDING ->
        SUBMITTED_UNCONFIRMED window order_intents exists to make
        recoverable). A generic retry decorator must not wrap this method.

        Raises OrderRejectedError / InvalidOrderRequestError for definitive,
        non-retryable rejections. Rate-limit category: ORDER_MUTATION.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def modify_order(
        self, request: ModifyOrderRequest, *, timeout: float | None = None
    ) -> None:
        """Modify a live order. Same ambiguous-outcome contract as
        place_order applies to timeouts/connection errors -- re-check via
        get_order(request.broker_order_id) before retrying.

        Raises OrderNotModifiableError if the order is already terminal.
        Rate-limit category: ORDER_MUTATION.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def cancel_order(
        self, broker_order_id: str, *, timeout: float | None = None
    ) -> None:
        """Cancel a live order. Same ambiguous-outcome contract as
        place_order applies to timeouts/connection errors.

        Raises OrderNotCancellableError if the order is already terminal --
        this is an expected condition (e.g. it filled just before the cancel
        arrived), not a bug. Rate-limit category: ORDER_MUTATION.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_order(
        self, broker_order_id: str, *, timeout: float | None = None
    ) -> BrokerOrder:
        """Point lookup of one order's current state. Read-only, freely
        retryable. Raises OrderNotFoundError if the broker has no record of
        this id. Rate-limit category: ORDER_READ.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_orders(self, *, timeout: float | None = None) -> list[BrokerOrder]:
        """Today's full broker orderbook. This is not a historical query --
        trading history is always read from the database, never the broker.
        Read-only, freely retryable. Rate-limit category: ORDER_READ.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def find_order_by_tag(
        self, tag: str, *, timeout: float | None = None
    ) -> BrokerOrder | None:
        """Find today's order carrying the given idempotency tag, or None if
        no such order exists at the broker.

        This is the reconciliation primitive: it is how a caller resolves
        "did this already happen?" after an ambiguous place_order/
        modify_order/cancel_order outcome, without every concrete broker (or
        every caller) re-implementing a linear scan over get_orders().
        Read-only, freely retryable. Rate-limit category: ORDER_READ.
        """
        raise NotImplementedError

    # -- Portfolio state -------------------------------------------------

    @abc.abstractmethod
    def get_positions(self, *, timeout: float | None = None) -> list[BrokerPosition]:
        """Today's live net-position feed. Read-only, freely retryable.
        Rate-limit category: PORTFOLIO_READ.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_holdings(self, *, timeout: float | None = None) -> list[BrokerHolding]:
        """Demat holdings. Read-only, freely retryable. Rate-limit category:
        PORTFOLIO_READ.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_margins(self, *, timeout: float | None = None) -> BrokerMargins:
        """Funds/margin snapshot, for pre-trade margin checks. Read-only,
        freely retryable. Rate-limit category: PORTFOLIO_READ.
        """
        raise NotImplementedError

    # -- Market data (pull) ----------------------------------------------

    @abc.abstractmethod
    def get_quote(
        self, instruments: list[InstrumentIdentifier], *, timeout: float | None = None
    ) -> dict[InstrumentIdentifier, BrokerQuote]:
        """Fetch full quotes for a batch of instruments in as few broker API
        calls as possible.

        Implementations must never degrade to one broker call per
        instrument. This interface imposes no limit on len(instruments); if
        the broker caps how many instruments fit in one request,
        implementations must chunk internally and merge results -- callers
        should not need to know or care what that cap is. Read-only, freely
        retryable. Rate-limit category: MARKET_DATA.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_ltp(
        self, instruments: list[InstrumentIdentifier], *, timeout: float | None = None
    ) -> dict[InstrumentIdentifier, Decimal]:
        """Cheap last-traded-price subset of get_quote, batched the same way
        and subject to the same no-per-instrument-calls rule. Read-only,
        freely retryable. Rate-limit category: MARKET_DATA.
        """
        raise NotImplementedError

    # -- Instrument lookup -------------------------------------------------

    @abc.abstractmethod
    def get_instrument(
        self, exchange: Exchange, tradingsymbol: str, *, timeout: float | None = None
    ) -> BrokerInstrument:
        """Resolve a known tradingsymbol to its full instrument details.
        Raises InstrumentNotFoundError if no match exists. Read-only, freely
        retryable. Rate-limit category: INSTRUMENT_LOOKUP.
        """
        raise NotImplementedError

    @abc.abstractmethod
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
        """Resolve logical option parameters (e.g. "today's ATM straddle
        leg") to a concrete tradingsymbol/token. Raises
        InstrumentNotFoundError if no contract matches. Read-only, freely
        retryable. Rate-limit category: INSTRUMENT_LOOKUP.
        """
        raise NotImplementedError

    # -- Websocket (order-update push) ------------------------------------
    # Tick/LTP streaming is a separate concern owned by
    # market_data/websocket_manager.py; this lifecycle is for real-time
    # order-fill notifications only.

    @abc.abstractmethod
    def connect_websocket(self, *, timeout: float | None = None) -> None:
        """Open the order-update push connection. Idempotent: calling this
        while already connected must be a no-op, not an error."""
        raise NotImplementedError

    @abc.abstractmethod
    def disconnect_websocket(self) -> None:
        """Close the order-update push connection. Idempotent: calling this
        while already disconnected must be a no-op, not an error."""
        raise NotImplementedError

    @abc.abstractmethod
    def is_websocket_connected(self) -> bool:
        """Cheap local check, no I/O."""
        raise NotImplementedError

    @abc.abstractmethod
    def register_order_update_callback(self, callback: OrderUpdateCallback) -> None:
        """Register a callback invoked with a fresh BrokerOrder snapshot on
        every order state change, including intermediate partial-fill
        progress -- not just terminal states.

        A single order can trigger this callback multiple times (e.g.
        OPEN/filled_quantity=25 -> OPEN/filled_quantity=60 ->
        COMPLETE/filled_quantity=75). Each invocation carries the order's
        full current state, not a delta: callers should compare
        filled_quantity / exchange_updated_at against whatever they last
        recorded, not assume exactly-once or in-order delivery -- a
        websocket reconnect can replay or reorder updates.

        Supports multiple registrations: every registered callback is
        invoked for every update (fan-out, not replace-on-register).
        """
        raise NotImplementedError
