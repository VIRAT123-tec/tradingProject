"""OrderRepository: persistence for the Order aggregate -- one row per broker
order placement attempt.

record_fill takes the resulting status as an explicit argument rather than
deriving "COMPLETE vs still OPEN" from filled_quantity internally --
classifying a partial fill is a business decision belonging to
fill_manager.py, not something this repository infers on its own.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from algo.common.enums import OrderPurpose, OrderStatus, OrderType, TransactionType
from algo.database.models.order import Order
from algo.database.repositories.base import BaseRepository


class OrderRepository(BaseRepository[Order]):
    """Persistence operations for Order rows."""

    model = Order

    def get_by_broker_order_id(self, broker_order_id: str) -> Order | None:
        """Look up an order by the broker's own order id -- the primary key
        reconciliation.py matches against the broker's orderbook."""
        stmt = select(Order).where(Order.broker_order_id == broker_order_id)
        return self.session.execute(stmt).scalar_one_or_none()

    def get_by_broker_tag(self, broker_tag: str) -> Order | None:
        """Look up an order by the tag stamped on it at placement time."""
        stmt = select(Order).where(Order.broker_tag == broker_tag)
        return self.session.execute(stmt).scalar_one_or_none()

    def create(
        self,
        *,
        trade_id: int,
        intent_id: int | None,
        purpose: OrderPurpose,
        transaction_type: TransactionType,
        order_type: OrderType,
        quantity: int,
        broker_tag: str | None = None,
    ) -> Order:
        """Create a new order attempt row. Status starts PENDING (the model
        default); callers advance it via the mark_* methods as the broker
        call progresses."""
        order = Order(
            trade_id=trade_id,
            intent_id=intent_id,
            purpose=purpose,
            transaction_type=transaction_type,
            order_type=order_type,
            quantity=quantity,
            broker_tag=broker_tag,
        )
        return self.add(order)

    def list_for_trade(self, trade_id: int) -> list[Order]:
        """Every placement attempt (including rejected/cancelled ones) for
        one leg -- the full history reconciliation.py compares against the
        broker's orderbook."""
        stmt = select(Order).where(Order.trade_id == trade_id)
        return list(self.session.execute(stmt).scalars())

    def list_in_flight(self) -> list[Order]:
        """Orders not yet in a terminal state (PENDING or OPEN at the
        broker) -- backed by the partial index; used by order_tracker.py's
        polling fallback and the startup reconciliation scan."""
        stmt = select(Order).where(Order.status.in_((OrderStatus.PENDING, OrderStatus.OPEN)))
        return list(self.session.execute(stmt).scalars())

    def mark_acknowledged(
        self, order: Order, *, broker_order_id: str, placed_at: datetime
    ) -> None:
        """Record that the broker accepted the order and returned an id."""
        order.broker_order_id = broker_order_id
        order.placed_at = placed_at
        order.status = OrderStatus.OPEN
        self.flush()

    def record_fill(
        self,
        order: Order,
        *,
        status: OrderStatus,
        filled_quantity: int,
        average_price: Decimal,
        filled_at: datetime,
        raw_response: dict[str, Any] | None = None,
    ) -> None:
        """Record a fill event. status is supplied by the caller (e.g.
        fill_manager.py), which is responsible for deciding whether this fill
        completes the order or leaves it partially filled."""
        order.status = status
        order.filled_quantity = filled_quantity
        order.average_price = average_price
        order.filled_at = filled_at
        if raw_response is not None:
            order.raw_response = raw_response
        self.flush()

    def record_rejection(
        self, order: Order, *, error_message: str, raw_response: dict[str, Any] | None = None
    ) -> None:
        """Record that the broker rejected this order (a non-retryable
        outcome -- the caller has already classified it as such before
        calling this)."""
        order.status = OrderStatus.REJECTED
        order.error_message = error_message
        if raw_response is not None:
            order.raw_response = raw_response
        self.flush()

    def record_cancellation(self, order: Order) -> None:
        """Record that this order was cancelled (by us or by the broker)."""
        order.status = OrderStatus.CANCELLED
        self.flush()

    def increment_retry_count(self, order: Order) -> None:
        """Record that another placement/retry attempt was consumed for this
        order row -- the retry count itself is decided by the retry wrapper
        in common/decorators.py, this only persists the resulting count."""
        order.retry_count += 1
        self.flush()
