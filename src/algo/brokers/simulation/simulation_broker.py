"""Fake/simulation broker implementing BrokerBase identically to kite/, so
start_paper.py exercises the same strategy code as start_live.py.

Architecture, in one paragraph: placing an order rolls a deterministic
outcome (full fill / partial-then-fill / rejected) once, from an injected
random.Random -- everything after that is just executing that plan against
an injected Clock, either inline (synchronous=True, for fast tests) or on a
background "matching engine" thread that ticks periodically (the
paper-trading default, which reproduces the real ack-now/fill-later timing
gap the platform's crash-recovery design depends on being able to observe).
Prices and instruments are never generated here -- both are injected
(PriceSource, InstrumentCatalog) so callers control exactly what a test
scenario sees.
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

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
from algo.brokers.exceptions import (
    BrokerAuthenticationError,
    BrokerConnectionError,
    BrokerTimeoutError,
    InstrumentNotFoundError,
    InvalidOrderRequestError,
    OrderNotCancellableError,
    OrderNotFoundError,
    OrderNotModifiableError,
    OrderRejectedError,
)
from algo.brokers.simulation.clock import Clock, SystemClock
from algo.brokers.simulation.config import FillOutcome, SimulationConfig
from algo.brokers.simulation.instrument_catalog import InstrumentCatalog
from algo.brokers.simulation.price_source import PriceSource
from algo.common.enums import (
    BrokerName,
    Exchange,
    OptionType,
    OrderStatus,
    OrderType,
    ProductType,
    TransactionType,
)

_TERMINAL_STATUSES = frozenset(
    {OrderStatus.COMPLETE, OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.ERROR}
)


@dataclass
class _SimOrder:
    """Mutable internal order state -- BrokerOrder (frozen, returned to
    callers) is derived from this, never the other way around."""

    broker_order_id: str
    tag: str
    exchange: Exchange
    tradingsymbol: str
    transaction_type: TransactionType
    product: ProductType
    quantity: int
    order_type: OrderType
    price: Decimal | None
    trigger_price: Decimal | None
    status: OrderStatus
    filled_quantity: int
    average_price: Decimal | None
    placed_at: datetime
    filled_at: datetime | None
    exchange_updated_at: datetime
    status_message: str | None
    outcome: FillOutcome
    resolve_at: datetime
    remaining_steps: int
    step_quantity: int


@dataclass
class _SimPosition:
    quantity: int = 0
    average_price: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")


def _update_position(
    position: _SimPosition,
    transaction_type: TransactionType,
    filled_qty: int,
    fill_price: Decimal,
) -> _SimPosition:
    """Pure function: fold one fill into a position, realizing PnL on
    whatever portion crosses (reduces or flips) the existing side.

    Quantity sign convention: positive = net long, negative = net short --
    matches Kite's own net-position convention.
    """
    signed_delta = filled_qty if transaction_type == TransactionType.BUY else -filled_qty

    if position.quantity == 0 or (position.quantity > 0) == (signed_delta > 0):
        # Flat, or adding to the existing side: pure weighted average, no PnL realized.
        total_qty = position.quantity + signed_delta
        if total_qty == 0:
            return _SimPosition(quantity=0, average_price=Decimal("0"), realized_pnl=position.realized_pnl)
        total_cost = position.average_price * abs(position.quantity) + fill_price * abs(signed_delta)
        return _SimPosition(
            quantity=total_qty,
            average_price=total_cost / abs(total_qty),
            realized_pnl=position.realized_pnl,
        )

    # Opposite side: this fill closes some or all of the existing position,
    # and may flip it, realizing PnL on the closed portion.
    closing_qty = min(abs(position.quantity), abs(signed_delta))
    if position.quantity > 0:
        realized = closing_qty * (fill_price - position.average_price)
    else:
        realized = closing_qty * (position.average_price - fill_price)
    new_realized = position.realized_pnl + realized
    new_quantity = position.quantity + signed_delta
    remaining_delta = abs(signed_delta) - closing_qty

    if remaining_delta == 0:
        new_avg = position.average_price if new_quantity != 0 else Decimal("0")
    else:
        # Flipped: the remainder opens a fresh position on the new side at fill_price.
        new_avg = fill_price
    return _SimPosition(quantity=new_quantity, average_price=new_avg, realized_pnl=new_realized)


def _decide_outcome(rng: random.Random, config: SimulationConfig) -> FillOutcome:
    """The one place randomness is consulted for a given order -- everything
    downstream just executes this decision over time."""
    if rng.random() < config.rejection_probability:
        return FillOutcome.REJECTED
    if rng.random() < config.partial_fill_probability:
        return FillOutcome.PARTIAL_THEN_FILL
    return FillOutcome.FULL_FILL


class _MatchingEngine(threading.Thread):
    """Background thread that periodically resolves whatever orders are due,
    reproducing the timing gap between "order accepted" and "order filled"
    that a real broker has. Not started at all in synchronous mode."""

    def __init__(self, broker: "SimulationBroker", tick_seconds: float) -> None:
        super().__init__(daemon=True, name="SimulationBroker-MatchingEngine")
        self._broker = broker
        self._tick_seconds = tick_seconds
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.is_set():
            self._broker._resolve_due_orders(force=False)
            self._stop_event.wait(self._tick_seconds)

    def stop(self) -> None:
        self._stop_event.set()


class SimulationBroker(BrokerBase):
    """See the module docstring for the overall architecture."""

    def __init__(
        self,
        *,
        instrument_catalog: InstrumentCatalog,
        price_source: PriceSource,
        config: SimulationConfig | None = None,
        clock: Clock | None = None,
        rng: random.Random | None = None,
        initial_holdings: list[BrokerHolding] | None = None,
    ) -> None:
        self._config = config or SimulationConfig()
        self._instrument_catalog = instrument_catalog
        self._price_source = price_source
        self._clock = clock or SystemClock()
        self._rng = rng or random.Random()
        self._holdings = list(initial_holdings or [])

        self._authenticated = False
        self._ws_connected = False

        self._lock = threading.Lock()
        self._callbacks_lock = threading.Lock()

        self._orders: dict[str, _SimOrder] = {}
        self._orders_by_tag: dict[str, str] = {}
        self._positions: dict[tuple[Exchange, str, ProductType], _SimPosition] = {}
        self._callbacks: list[OrderUpdateCallback] = []

        self._matching_engine: _MatchingEngine | None = None
        if not self._config.synchronous:
            self._matching_engine = _MatchingEngine(self, self._config.matching_tick_seconds)
            self._matching_engine.start()

    # -- Lifecycle ---------------------------------------------------------

    @property
    def broker_name(self) -> BrokerName:
        return BrokerName.SIMULATION

    def authenticate(self, *, timeout: float | None = None) -> None:
        if self._config.auth_should_fail:
            raise BrokerAuthenticationError("Simulated authentication failure")
        self._clock.sleep(self._config.ack_latency_seconds)
        self._authenticated = True

    def is_authenticated(self) -> bool:
        return self._authenticated

    def close(self) -> None:
        if self._matching_engine is not None:
            self._matching_engine.stop()
            self._matching_engine.join(timeout=2.0)
        self._ws_connected = False

    def health_check(self, *, timeout: float | None = None) -> HealthStatus:
        started = time.monotonic()
        self._simulate_read_call(timeout=timeout)
        elapsed_ms = (time.monotonic() - started) * 1000
        return HealthStatus(
            healthy=self._authenticated,
            checked_at=self._clock.now(),
            latency_ms=elapsed_ms,
            detail=None if self._authenticated else "Not authenticated",
        )

    # -- Orders --------------------------------------------------------

    def place_order(
        self, request: PlaceOrderRequest, *, timeout: float | None = None
    ) -> PlaceOrderResult:
        self._require_authenticated()

        if self._rng.random() < self._config.connection_failure_probability:
            raise BrokerConnectionError("Simulated connection failure placing order")

        if timeout is not None and self._config.ack_latency_seconds > timeout:
            raise BrokerTimeoutError("Simulated ack exceeded the caller's timeout budget")
        self._clock.sleep(self._config.ack_latency_seconds)

        try:
            self._instrument_catalog.get_by_symbol(request.exchange, request.tradingsymbol)
        except InstrumentNotFoundError:
            raise OrderRejectedError(
                f"Unknown instrument {request.exchange.value}:{request.tradingsymbol}"
            ) from None

        now = self._clock.now()
        broker_order_id = self._next_order_id()
        outcome = _decide_outcome(self._rng, self._config)
        sim_order = self._build_sim_order(broker_order_id, request, outcome, now)

        with self._lock:
            self._orders[broker_order_id] = sim_order
            # Tag collisions are permitted, exactly like real Kite (it does not
            # enforce tag uniqueness) -- a duplicate tag simply overwrites the
            # index, so find_order_by_tag() returns the most recent order
            # sharing that tag, reproducing the same ambiguity the real
            # broker has rather than hiding it.
            self._orders_by_tag[request.tag] = broker_order_id

        if self._rng.random() < self._config.ack_timeout_probability:
            raise BrokerTimeoutError(
                "Simulated ack timeout -- the order may have reached the broker; "
                "use find_order_by_tag() to check before retrying"
            )

        self._pump()
        return PlaceOrderResult(
            broker_order_id=broker_order_id,
            raw_response={"status": "ack", "order_id": broker_order_id},
        )

    def modify_order(
        self, request: ModifyOrderRequest, *, timeout: float | None = None
    ) -> None:
        self._require_authenticated()
        if self._rng.random() < self._config.connection_failure_probability:
            raise BrokerConnectionError("Simulated connection failure modifying order")
        if timeout is not None and self._config.ack_latency_seconds > timeout:
            raise BrokerTimeoutError("Simulated modify exceeded the caller's timeout budget")
        self._clock.sleep(self._config.ack_latency_seconds)

        with self._lock:
            order = self._orders.get(request.broker_order_id)
            if order is None:
                raise OrderNotFoundError(f"No order {request.broker_order_id}")
            if order.status in _TERMINAL_STATUSES:
                raise OrderNotModifiableError(
                    f"Order {request.broker_order_id} is already {order.status.value}"
                )
            if request.quantity is not None:
                if request.quantity < order.filled_quantity:
                    raise InvalidOrderRequestError(
                        "Cannot modify quantity below what has already filled"
                    )
                order.quantity = request.quantity
            if request.price is not None:
                order.price = request.price
            if request.trigger_price is not None:
                order.trigger_price = request.trigger_price
            if request.order_type is not None:
                order.order_type = request.order_type
            order.exchange_updated_at = self._clock.now()
            updated = self._to_broker_order(order)

        self._dispatch_order_update(updated)

    def cancel_order(self, broker_order_id: str, *, timeout: float | None = None) -> None:
        self._require_authenticated()
        if self._rng.random() < self._config.connection_failure_probability:
            raise BrokerConnectionError("Simulated connection failure cancelling order")
        if timeout is not None and self._config.ack_latency_seconds > timeout:
            raise BrokerTimeoutError("Simulated cancel exceeded the caller's timeout budget")
        self._clock.sleep(self._config.ack_latency_seconds)

        with self._lock:
            order = self._orders.get(broker_order_id)
            if order is None:
                raise OrderNotFoundError(f"No order {broker_order_id}")
            if order.status in _TERMINAL_STATUSES:
                raise OrderNotCancellableError(
                    f"Order {broker_order_id} is already {order.status.value}"
                )
            order.status = OrderStatus.CANCELLED
            order.exchange_updated_at = self._clock.now()
            order.remaining_steps = 0
            updated = self._to_broker_order(order)

        self._dispatch_order_update(updated)

    def get_order(self, broker_order_id: str, *, timeout: float | None = None) -> BrokerOrder:
        self._simulate_read_call(timeout=timeout)
        self._pump()
        with self._lock:
            order = self._orders.get(broker_order_id)
            if order is None:
                raise OrderNotFoundError(f"No order {broker_order_id}")
            return self._to_broker_order(order)

    def get_orders(self, *, timeout: float | None = None) -> list[BrokerOrder]:
        self._simulate_read_call(timeout=timeout)
        self._pump()
        with self._lock:
            return [self._to_broker_order(o) for o in self._orders.values()]

    def find_order_by_tag(
        self, tag: str, *, timeout: float | None = None
    ) -> BrokerOrder | None:
        self._simulate_read_call(timeout=timeout)
        self._pump()
        with self._lock:
            broker_order_id = self._orders_by_tag.get(tag)
            if broker_order_id is None:
                return None
            return self._to_broker_order(self._orders[broker_order_id])

    # -- Portfolio state -------------------------------------------------

    def get_positions(self, *, timeout: float | None = None) -> list[BrokerPosition]:
        self._simulate_read_call(timeout=timeout)
        self._pump()
        with self._lock:
            positions = dict(self._positions)

        result: list[BrokerPosition] = []
        for (exchange, tradingsymbol, product), pos in positions.items():
            if pos.quantity == 0:
                continue
            last_price = self._price_source.get_ltp(
                InstrumentIdentifier(exchange=exchange, tradingsymbol=tradingsymbol)
            )
            pnl = pos.realized_pnl + Decimal(pos.quantity) * (last_price - pos.average_price)
            result.append(
                BrokerPosition(
                    exchange=exchange,
                    tradingsymbol=tradingsymbol,
                    product=product,
                    quantity=pos.quantity,
                    average_price=pos.average_price,
                    last_price=last_price,
                    pnl=pnl,
                )
            )
        return result

    def get_holdings(self, *, timeout: float | None = None) -> list[BrokerHolding]:
        # Holdings/settlement isn't simulated beyond this static, injectable
        # list -- strategy-1 trades intraday MIS, so a full T+1 settlement
        # model would be complexity this platform doesn't currently need.
        self._simulate_read_call(timeout=timeout)
        return list(self._holdings)

    def get_margins(self, *, timeout: float | None = None) -> BrokerMargins:
        self._simulate_read_call(timeout=timeout)
        self._pump()
        with self._lock:
            positions = dict(self._positions)

        used_margin = Decimal("0")
        realized_total = Decimal("0")
        for (exchange, tradingsymbol, _product), pos in positions.items():
            realized_total += pos.realized_pnl
            if pos.quantity == 0:
                continue
            try:
                instrument = self._instrument_catalog.get_by_symbol(exchange, tradingsymbol)
                lots = max(1, abs(pos.quantity) // instrument.lot_size)
            except InstrumentNotFoundError:
                lots = abs(pos.quantity)
            used_margin += self._config.margin_per_lot * lots

        available = self._config.initial_cash + realized_total - used_margin
        return BrokerMargins(
            segment="NFO",
            available_cash=available,
            used_margin=used_margin,
            opening_balance=self._config.initial_cash,
        )

    # -- Market data (pull) ----------------------------------------------

    def get_quote(
        self, instruments: list[InstrumentIdentifier], *, timeout: float | None = None
    ) -> dict[InstrumentIdentifier, BrokerQuote]:
        # Batching has no real broker round-trip to save here (PriceSource is
        # in-memory), so this loop already satisfies BrokerBase's "one
        # logical call" contract without needing internal chunking.
        self._simulate_read_call(timeout=timeout)
        now = self._clock.now()
        result: dict[InstrumentIdentifier, BrokerQuote] = {}
        for instrument in instruments:
            ltp = self._price_source.get_ltp(instrument)
            result[instrument] = BrokerQuote(
                instrument=instrument,
                last_price=ltp,
                ohlc_open=ltp,
                ohlc_high=ltp,
                ohlc_low=ltp,
                ohlc_close=ltp,
                volume=0,
                timestamp=now,
            )
        return result

    def get_ltp(
        self, instruments: list[InstrumentIdentifier], *, timeout: float | None = None
    ) -> dict[InstrumentIdentifier, Decimal]:
        self._simulate_read_call(timeout=timeout)
        return {instrument: self._price_source.get_ltp(instrument) for instrument in instruments}

    # -- Instrument lookup -------------------------------------------------

    def get_instrument(
        self, exchange: Exchange, tradingsymbol: str, *, timeout: float | None = None
    ) -> BrokerInstrument:
        self._simulate_read_call(timeout=timeout)
        return self._instrument_catalog.get_by_symbol(exchange, tradingsymbol)

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
        self._simulate_read_call(timeout=timeout)
        return self._instrument_catalog.find_option(
            underlying=underlying,
            expiry=expiry,
            strike=strike,
            option_type=option_type,
            exchange=exchange,
        )

    # -- Websocket (order-update push) ------------------------------------

    def connect_websocket(self, *, timeout: float | None = None) -> None:
        if self._rng.random() < self._config.websocket_connect_failure_probability:
            raise BrokerConnectionError("Simulated websocket connection failure")
        self._clock.sleep(self._config.ack_latency_seconds)
        self._ws_connected = True

    def disconnect_websocket(self) -> None:
        self._ws_connected = False

    def is_websocket_connected(self) -> bool:
        return self._ws_connected

    def register_order_update_callback(self, callback: OrderUpdateCallback) -> None:
        with self._callbacks_lock:
            self._callbacks.append(callback)

    def simulate_websocket_disconnect(self) -> None:
        """Test-support only, not part of BrokerBase: force the websocket to
        drop mid-session so a test can exercise the platform's reconnect
        handling deterministically."""
        self._ws_connected = False

    # -- Internal: matching engine ------------------------------------------

    def _pump(self) -> None:
        """Advance order resolution as far as this call's mode allows: a
        full drain in synchronous mode (so a query immediately after
        place_order sees the final state), or a single respectful pass in
        threaded mode (only orders actually due -- the background thread
        handles the rest). Called opportunistically by every read method so
        results are current even if the background thread hasn't ticked yet,
        or (in synchronous mode) don't exist at all.
        """
        if self._config.synchronous:
            while self._resolve_due_orders(force=True):
                pass
        else:
            self._resolve_due_orders(force=False)

    def _resolve_due_orders(self, *, force: bool) -> list[BrokerOrder]:
        updates: list[BrokerOrder] = []
        with self._lock:
            for order in list(self._orders.values()):
                updated = self._process_due_order(order, force=force)
                if updated is not None:
                    updates.append(updated)
        for update in updates:
            self._dispatch_order_update(update)
        return updates

    def _process_due_order(self, order: _SimOrder, *, force: bool) -> BrokerOrder | None:
        """Caller must hold self._lock. Advances `order` by exactly one
        resolution step if it is due (or unconditionally if force=True),
        returning the updated snapshot, or None if nothing changed."""
        if order.status in _TERMINAL_STATUSES:
            return None
        now = self._clock.now()
        if not force and now < order.resolve_at:
            return None

        if order.outcome == FillOutcome.REJECTED:
            order.status = OrderStatus.REJECTED
            order.status_message = "Simulated rejection"
            order.exchange_updated_at = now
            order.remaining_steps = 0
            return self._to_broker_order(order)

        instrument = InstrumentIdentifier(exchange=order.exchange, tradingsymbol=order.tradingsymbol)
        fill_price = self._price_source.get_ltp(instrument)
        remaining_to_fill = order.quantity - order.filled_quantity
        step_qty = remaining_to_fill if order.remaining_steps <= 1 else min(
            order.step_quantity, remaining_to_fill
        )

        if step_qty > 0:
            self._apply_fill(order, step_qty, fill_price)

        order.remaining_steps = max(0, order.remaining_steps - 1)
        order.exchange_updated_at = now

        if order.filled_quantity >= order.quantity:
            order.status = OrderStatus.COMPLETE
            order.filled_at = now
            order.remaining_steps = 0
        else:
            order.status = OrderStatus.OPEN
            order.resolve_at = now + timedelta(seconds=self._config.fill_latency_seconds)

        return self._to_broker_order(order)

    def _apply_fill(self, order: _SimOrder, filled_delta: int, fill_price: Decimal) -> None:
        """Caller must hold self._lock. Updates both this order's own
        weighted-average fill price and the broker-wide position ledger."""
        prior_filled = order.filled_quantity
        prior_avg = order.average_price or Decimal("0")
        new_filled = prior_filled + filled_delta
        order.average_price = (prior_avg * prior_filled + fill_price * filled_delta) / new_filled
        order.filled_quantity = new_filled

        key = (order.exchange, order.tradingsymbol, order.product)
        current = self._positions.get(key, _SimPosition())
        self._positions[key] = _update_position(current, order.transaction_type, filled_delta, fill_price)

    def _dispatch_order_update(self, order: BrokerOrder) -> None:
        """Never called while holding self._lock -- callbacks are arbitrary
        caller code (e.g. execution/ may call cancel_order from within one),
        so no internal lock may be held while they run."""
        if not self._ws_connected:
            return
        with self._callbacks_lock:
            callbacks = list(self._callbacks)
        for callback in callbacks:
            callback(order)

    # -- Internal: helpers -------------------------------------------------

    def _require_authenticated(self) -> None:
        if not self._authenticated:
            raise BrokerAuthenticationError(
                "SimulationBroker is not authenticated; call authenticate() first"
            )

    def _simulate_read_call(self, *, timeout: float | None) -> None:
        if self._rng.random() < self._config.connection_failure_probability:
            raise BrokerConnectionError("Simulated connection failure")
        if timeout is not None and self._config.ack_latency_seconds > timeout:
            raise BrokerTimeoutError("Simulated call exceeded the caller's timeout budget")
        self._clock.sleep(self._config.ack_latency_seconds)

    def _next_order_id(self) -> str:
        """Mint a globally-unique broker order id.

        A UUID, deliberately *not* a restart-resettable counter.
        ``broker_order_id`` is persisted under a UNIQUE constraint
        (``uq_orders_broker_order_id``); the previous in-memory counter was
        initialised to 0 in ``__init__``, so every process restart re-minted
        ``SIM000000000001...`` straight back into rows a prior run had already
        written -- the collision that rolled the transaction back and froze the
        strategy. ``uuid4`` is unique across restarts, processes, and threads
        with no shared or persisted state, so the same id can never be handed
        out twice. No lock is needed (``uuid4`` is itself thread-safe); the
        ``SIM`` prefix is kept only so a simulated order stays recognisable in
        logs and the database."""
        return f"SIM{uuid.uuid4().hex}"

    def _build_sim_order(
        self,
        broker_order_id: str,
        request: PlaceOrderRequest,
        outcome: FillOutcome,
        placed_at: datetime,
    ) -> _SimOrder:
        if outcome == FillOutcome.PARTIAL_THEN_FILL:
            steps = self._rng.randint(
                self._config.min_partial_fill_steps, self._config.resolved_max_partial_fill_steps
            )
            step_quantity = max(1, request.quantity // steps)
        else:
            steps = 1
            step_quantity = request.quantity

        return _SimOrder(
            broker_order_id=broker_order_id,
            tag=request.tag,
            exchange=request.exchange,
            tradingsymbol=request.tradingsymbol,
            transaction_type=request.transaction_type,
            product=request.product,
            quantity=request.quantity,
            order_type=request.order_type,
            price=request.price,
            trigger_price=request.trigger_price,
            status=OrderStatus.OPEN,
            filled_quantity=0,
            average_price=None,
            placed_at=placed_at,
            filled_at=None,
            exchange_updated_at=placed_at,
            status_message=None,
            outcome=outcome,
            resolve_at=placed_at + timedelta(seconds=self._config.fill_latency_seconds),
            remaining_steps=steps,
            step_quantity=step_quantity,
        )

    def _to_broker_order(self, order: _SimOrder) -> BrokerOrder:
        return BrokerOrder(
            broker_order_id=order.broker_order_id,
            tag=order.tag,
            status=order.status,
            exchange=order.exchange,
            tradingsymbol=order.tradingsymbol,
            transaction_type=order.transaction_type,
            product=order.product,
            order_type=order.order_type,
            quantity=order.quantity,
            filled_quantity=order.filled_quantity,
            average_price=order.average_price,
            trigger_price=order.trigger_price,
            placed_at=order.placed_at,
            filled_at=order.filled_at,
            exchange_updated_at=order.exchange_updated_at,
            status_message=order.status_message,
            raw_response={},
        )
