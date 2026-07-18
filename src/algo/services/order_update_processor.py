"""OrderUpdateProcessor: applies pushed broker order updates to the database
idempotently (resolves review finding H3).

The broker's order-update websocket delivers ``BrokerOrder`` snapshots (fills,
partial fills, cancellations, rejections) asynchronously, on the broker's own
callback thread. This processor turns each one into an idempotent,
**advance-only** update of the matching ``Order`` row, so the push feed keeps
the database current faster than -- and redundantly with -- the entry/exit
polling path, and also catches genuinely out-of-band updates (e.g. a
broker-initiated cancellation after our poller has stopped watching).

It is deliberately broker-agnostic: it consumes the platform's own
``BrokerOrder`` DTO, so no Kite-specific logic lives here (the Kite mapping
already happened in ``brokers/kite``). It can therefore be driven by any broker
that emits order updates, including the simulation broker.

``apply_broker_order_update`` (module-level, below) is the single authoritative
decision for "what should this Order row become, given this BrokerOrder
snapshot" -- it is not private to this class. ``entry_logic.py``/
``exit_logic.py``'s own poll-based fill confirmation call it too (against a row
they lock themselves), so there is exactly one place, not two independently
maintained ones, that decides how to apply a terminal order outcome. See those
modules' own docstrings for why this matters: two writers independently
deciding "what to write" for the same row -- even against the same underlying
truth -- is what previously let one legitimately lose an optimistic-lock race
in a way nothing caught.

Correctness properties:

* **Idempotent** -- applying the same update twice is a no-op. A row already in
  a terminal state is never touched again, and a partial fill that does not
  advance ``filled_quantity`` writes nothing.
* **Advance-only** -- it never regresses a row (e.g. a late OPEN snapshot
  arriving after a COMPLETE is ignored). Combined with the terminal guard, this
  is what makes it safe to run alongside the synchronous entry/exit poller: the
  two write the same truth, and whichever applies first wins while the other
  no-ops.
* **Consistent under concurrency** -- each update is its own committed unit of
  work, loading the row ``FOR UPDATE`` so it serialises with any concurrent
  writer at the database level; an optimistic-lock conflict (the other writer
  won the race) is caught and reported, never raised into the callback thread.
* **Never destabilises the caller** -- every exception is contained; an update
  for an order this process does not know about is logged and ignored
  (reconciliation, not this processor, owns orphan/broker-only orders).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

from algo.common.enums import OrderStatus
from algo.database.repositories.exceptions import ConcurrentModificationError
from algo.database.repositories.order_repository import OrderRepository
from algo.database.session import unit_of_work

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.orm import Session, sessionmaker

    from algo.brokers.broker_base import BrokerOrder
    from algo.database.models.order import Order
    from algo.strategy_engine.strategy_context import TimeProvider


_TERMINAL_STATUSES = frozenset(
    {OrderStatus.COMPLETE, OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.ERROR}
)


class OrderUpdateOutcome(str, Enum):
    """What the processor did with one update -- returned for observability and
    testing (the websocket callback ignores the return value)."""

    APPLIED_FILL = "APPLIED_FILL"
    APPLIED_PARTIAL_FILL = "APPLIED_PARTIAL_FILL"
    APPLIED_REJECTION = "APPLIED_REJECTION"
    APPLIED_CANCELLATION = "APPLIED_CANCELLATION"
    NO_CHANGE = "NO_CHANGE"
    ALREADY_TERMINAL = "ALREADY_TERMINAL"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"


class OrderUpdateProcessor:
    """Applies ``BrokerOrder`` updates to ``Order`` rows idempotently."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        time_provider: TimeProvider,
        logger: logging.Logger | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._time = time_provider
        self._logger = logger if logger is not None else logging.getLogger("algo.order_updates")

    def process(self, broker_order: BrokerOrder) -> OrderUpdateOutcome:
        """Apply one broker order update. Safe to call from the broker's
        websocket callback thread: it never raises, containing every failure as
        a returned outcome plus a log line."""
        try:
            with self._unit_of_work() as session:
                repo = OrderRepository(session)
                located = self._locate(repo, broker_order)
                if located is None:
                    self._logger.info(
                        "order update for unknown order (id=%s tag=%s status=%s); ignoring "
                        "-- reconciliation owns broker-only orders",
                        broker_order.broker_order_id, broker_order.tag, broker_order.status.value,
                    )
                    return OrderUpdateOutcome.NOT_FOUND
                # Re-load under a row lock so we serialise with the poller at the
                # database level rather than racing on the optimistic version.
                order = repo.get_by_id_for_update(located.id)
                if order is None:  # deleted between the two reads -- vanishingly unlikely
                    return OrderUpdateOutcome.NOT_FOUND
                return self._apply(repo, order, broker_order)
        except ConcurrentModificationError:
            # The synchronous poller committed first; its write is the truth.
            self._logger.info(
                "order update for %s lost a write race; deferring to the other writer",
                broker_order.broker_order_id,
            )
            return OrderUpdateOutcome.CONFLICT
        except Exception:  # noqa: BLE001 -- a callback-thread update must never crash the ws
            self._logger.error(
                "failed to process order update for %s", broker_order.broker_order_id, exc_info=True
            )
            return OrderUpdateOutcome.CONFLICT

    # -- Internal ------------------------------------------------------

    @staticmethod
    def _locate(repo: OrderRepository, broker_order: BrokerOrder) -> Order | None:
        """Find the Order row this update refers to: by broker order id first
        (set once the order is acknowledged), then by the tag stamped at
        placement (covers the window before acknowledgement)."""
        if broker_order.broker_order_id:
            found = repo.get_by_broker_order_id(broker_order.broker_order_id)
            if found is not None:
                return found
        if broker_order.tag:
            return repo.get_by_broker_tag(broker_order.tag)
        return None

    def _apply(
        self, repo: OrderRepository, order: Order, bo: BrokerOrder
    ) -> OrderUpdateOutcome:
        """Thin wrapper over the shared, module-level decision function --
        see ``apply_broker_order_update``'s own docstring for why this logic
        lives there and not here."""
        return apply_broker_order_update(repo, order, bo, now=self._time.now())

    @contextmanager
    def _unit_of_work(self) -> Iterator[Session]:
        """One committed transaction against the injected factory (see
        ``database.session.unit_of_work``)."""
        with unit_of_work(self._session_factory) as session:
            yield session


def apply_broker_order_update(
    repo: OrderRepository, order: Order, bo: BrokerOrder, *, now: datetime,
) -> OrderUpdateOutcome:
    """The single, authoritative decision for how a ``BrokerOrder`` snapshot
    should be applied to an already-locked ``Order`` row: terminal-guard
    (never touch a row already in a terminal state) and advance-only (a
    stale/duplicate snapshot writes nothing).

    This is deliberately the *only* place that decides what to write for a
    given ``(order, bo)`` pair. Both the broker's pushed order-update stream
    (``OrderUpdateProcessor.process``, above) and Strategy-1's own poll-based
    fill confirmation (``entry_logic.py``/``exit_logic.py``) call this exact
    function against a row they have each locked themselves
    (``get_by_id_for_update``), so the two writers can never independently
    decide different things about the same order: whichever acquires the row
    lock first serialises the other, which then sees this function's
    terminal-guard correctly recognise "already applied" rather than writing
    -- or racing -- again.

    Pure with respect to *deciding*: the only side effects are the
    ``repo.record_*`` calls this function itself makes (each of which
    flushes). The caller owns the surrounding transaction, the row lock, and
    (for callers where a lock is not held under the same guarantees as
    ``OrderUpdateProcessor``'s) any ``ConcurrentModificationError`` this can
    still raise if the row was modified after it was loaded.
    """
    if order.status in _TERMINAL_STATUSES:
        return OrderUpdateOutcome.ALREADY_TERMINAL  # advance-only: never regress

    # Learn the broker order id if this row was created (by tag) before ack.
    if order.broker_order_id is None and bo.broker_order_id:
        order.broker_order_id = bo.broker_order_id

    if bo.status is OrderStatus.COMPLETE:
        repo.record_fill(
            order, status=OrderStatus.COMPLETE, filled_quantity=bo.filled_quantity,
            average_price=bo.average_price or Decimal("0"),
            filled_at=bo.filled_at or now, raw_response=bo.raw_response,
        )
        return OrderUpdateOutcome.APPLIED_FILL

    if bo.status in (OrderStatus.REJECTED, OrderStatus.ERROR):
        repo.record_rejection(
            order, error_message=bo.status_message or f"broker {bo.status.value.lower()}",
            raw_response=bo.raw_response,
        )
        return OrderUpdateOutcome.APPLIED_REJECTION

    if bo.status is OrderStatus.CANCELLED:
        repo.record_cancellation(order)
        return OrderUpdateOutcome.APPLIED_CANCELLATION

    # Still open: persist only a genuine partial-fill advance.
    if bo.filled_quantity > order.filled_quantity:
        repo.record_fill(
            order, status=OrderStatus.OPEN, filled_quantity=bo.filled_quantity,
            average_price=bo.average_price or order.average_price or Decimal("0"),
            filled_at=bo.filled_at or now, raw_response=bo.raw_response,
        )
        return OrderUpdateOutcome.APPLIED_PARTIAL_FILL

    # Nothing new (may still have learned the broker_order_id above, which
    # the caller's unit of work will commit).
    return OrderUpdateOutcome.NO_CHANGE


def reconcile_order_terminal(
    *,
    session_factory: sessionmaker[Session],
    broker_order_id: str,
    broker_order: BrokerOrder,
    now: datetime,
    logger: logging.Logger | logging.LoggerAdapter,
) -> None:
    """Apply a broker's terminal order snapshot to the matching ``Order`` row
    **in its own committed transaction**, and never raise on a lost
    optimistic-lock race.

    This is the poll path's safe counterpart to ``OrderUpdateProcessor.process``
    (the push path). Strategy-1's entry/exit confirmation calls it after polling
    an order to a terminal state, to bring the ``Order`` row in line with that
    outcome. Two properties make it safe to run alongside the push path:

    * **Self-contained transaction.** The order-row write lives in its own unit
      of work, isolated from the caller's other writes (the trade/leg/intent
      bookkeeping). A ``flush`` that fails the version check deactivates its
      session until rolled back; keeping that write here means a lost race rolls
      back *this* session cleanly (``unit_of_work`` rolls back and closes on the
      exception) and can never poison the caller's session -- which is exactly
      what previously turned a lost race into a ``PendingRollbackError`` that
      froze a strategy whose position had in fact exited successfully.

    * **Idempotent, defer-on-conflict.** If the push path already committed the
      identical terminal outcome first, ``apply_broker_order_update`` either
      no-ops (its terminal guard) or loses the version race; either way the
      committed row is the source of truth, so a ``ConcurrentModificationError``
      here is caught, logged, and swallowed rather than propagated.
    """
    try:
        with unit_of_work(session_factory) as session:
            repo = OrderRepository(session)
            located = repo.get_by_broker_order_id(broker_order_id)
            if located is None:
                return
            order = repo.get_by_id_for_update(located.id)
            if order is None:  # deleted between the two reads -- vanishingly unlikely
                return
            apply_broker_order_update(repo, order, broker_order, now=now)
    except ConcurrentModificationError:
        logger.info(
            "order %s terminal reconciliation lost the write race to the push "
            "path; its committed state is the source of truth",
            broker_order_id,
        )
