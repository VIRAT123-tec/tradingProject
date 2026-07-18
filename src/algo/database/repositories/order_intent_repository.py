"""OrderIntentRepository: persistence for the OrderIntent aggregate -- the
durable "about to place this order" record written before any broker call.

The status-transition helpers here (mark_broker_call_started, mark_placed,
mark_confirmed, mark_failed, mark_cancelled) are thin, single-purpose writes:
each persists a transition the caller (order_manager.py) has already decided
happened. None of them decide *when* to fire -- that decision, and the
retry/error-classification logic behind it, belongs to order_manager.py and
common/decorators.py's retry wrapper, not here.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select

from algo.common.enums import IntentStatus, OrderPurpose, TransactionType
from algo.database.models.order_intent import OrderIntent
from algo.database.repositories.base import BaseRepository


class OrderIntentRepository(BaseRepository[OrderIntent]):
    """Persistence operations for OrderIntent rows."""

    model = OrderIntent

    def get_by_idempotency_key(self, idempotency_key: str) -> OrderIntent | None:
        """Look up an intent by its internal dedupe key."""
        stmt = select(OrderIntent).where(OrderIntent.idempotency_key == idempotency_key)
        return self.session.execute(stmt).scalar_one_or_none()

    def get_by_broker_tag(self, broker_tag: str) -> OrderIntent | None:
        """Look up an intent by the compact tag actually sent to the broker --
        the join key reconciliation uses to attribute an orphan broker fill
        back to the intent that caused it."""
        stmt = select(OrderIntent).where(OrderIntent.broker_tag == broker_tag)
        return self.session.execute(stmt).scalar_one_or_none()

    def get_or_create(
        self,
        *,
        trade_id: int,
        purpose: OrderPurpose,
        transaction_type: TransactionType,
        quantity: int,
        idempotency_key: str,
        broker_tag: str,
    ) -> tuple[OrderIntent, bool]:
        """Idempotently ensure an intent exists for this idempotency_key.

        Returns (intent, created) -- created=False means a retry (scheduler
        misfire, process restart) reused an already-recorded intent rather
        than minting a duplicate order attempt.
        """

        def lookup() -> OrderIntent | None:
            return self.get_by_idempotency_key(idempotency_key)

        def factory() -> OrderIntent:
            return OrderIntent(
                trade_id=trade_id,
                purpose=purpose,
                transaction_type=transaction_type,
                quantity=quantity,
                idempotency_key=idempotency_key,
                broker_tag=broker_tag,
            )

        return self._get_or_create(lookup=lookup, factory=factory)

    def list_in_doubt(self) -> list[OrderIntent]:
        """List every intent not yet in a terminal state (PENDING,
        SUBMITTED_UNCONFIRMED, or PLACED) -- the exact startup crash-recovery
        scan: these are the actions whose outcome the database doesn't yet
        know, per the crash-recovery workflow."""
        stmt = select(OrderIntent).where(
            OrderIntent.status.in_(
                (
                    IntentStatus.PENDING,
                    IntentStatus.SUBMITTED_UNCONFIRMED,
                    IntentStatus.PLACED,
                )
            )
        )
        return list(self.session.execute(stmt).scalars())

    def list_for_trade(self, trade_id: int) -> list[OrderIntent]:
        """All intents ever recorded for one leg."""
        stmt = select(OrderIntent).where(OrderIntent.trade_id == trade_id)
        return list(self.session.execute(stmt).scalars())

    def mark_broker_call_started(self, intent: OrderIntent, *, started_at: datetime) -> None:
        """Record that the broker call is being made now, before we know the
        result -- this is the SUBMITTED_UNCONFIRMED window. Set immediately
        before the call, in its own committed transaction if possible, so a
        crash during the call itself is still recoverable."""
        intent.broker_call_started_at = started_at
        intent.status = IntentStatus.SUBMITTED_UNCONFIRMED
        self.flush()

    def mark_placed(self, intent: OrderIntent) -> None:
        """Record that the broker acknowledged the order (a broker_order_id
        was received). The Order row itself is created by OrderRepository;
        this only advances the intent's own status."""
        intent.status = IntentStatus.PLACED
        self.flush()

    def mark_confirmed(self, intent: OrderIntent) -> None:
        """Record that the intended action's outcome (fill, or a resolved
        rejection) has been fully recorded elsewhere."""
        intent.status = IntentStatus.CONFIRMED
        self.flush()

    def mark_failed(self, intent: OrderIntent) -> None:
        """Record that this intent will not be pursued further (e.g. resolved
        during recovery as never having reached the broker)."""
        intent.status = IntentStatus.FAILED
        self.flush()

    def mark_cancelled(self, intent: OrderIntent) -> None:
        """Record that this intent was deliberately abandoned."""
        intent.status = IntentStatus.CANCELLED
        self.flush()
