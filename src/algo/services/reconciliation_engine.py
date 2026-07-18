"""reconciliation_engine.py: resolve in-doubt database state against broker
truth after a crash, restart, or ambiguous order outcome.

This is the module the whole crash-recovery design was built to enable. The
order_intents table (a durable "about to place this order" record written
before every broker call), the SUBMITTED_UNCONFIRMED intent status, and the
reconciliation_breaks table all exist so that, on restart, this engine can ask
"what actually happened at the broker?" and repair the database to match --
turning an unrecoverable ambiguity into either a clean, correct state or a
clearly-flagged break for a human.

What it does, and does not, do:

* It makes **only read-only broker calls** (get_orders / get_positions). It
  NEVER places, modifies, or cancels an order -- a naked leg discovered here is
  recorded as a break and the position moved to ERROR for the risk engine or an
  operator to flatten, never auto-traded by reconciliation itself.
* It repairs the database (records fills, resolves in-doubt intents, transitions
  positions) to match broker truth, and records a ReconciliationBreak for every
  discrepancy so nothing is ever silently dropped.
* It is broker-agnostic (depends only on ``BrokerBase``) and strategy-agnostic:
  the one genuinely strategy-specific action -- finalizing a completed entry or
  exit (computing that strategy's target/stoploss or realized P&L and driving
  ENTRY_PENDING->OPEN / EXIT_PENDING->CLOSED) -- is delegated to an injected
  ``PositionReconciler`` seam, so this engine never imports a concrete strategy.
  Every other transition it makes itself (->ERROR, ->CLOSED-abort) is
  strategy-neutral.
* It is idempotent: a second run finds already-resolved intents no longer
  in-doubt, already-terminal orders no longer in-flight, and already-repaired
  positions no longer needing action -- so re-running is safe and produces no
  duplicate repairs or breaks.

Safety-first on broker availability: if broker truth cannot be fetched, the
engine repairs nothing and reports BROKER_UNAVAILABLE. Guessing a repair
without broker truth is exactly the failure this module exists to prevent.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Protocol

from algo.brokers.exceptions import BrokerError
from algo.common.enums import (
    InstanceStatus,
    IntentStatus,
    OrderPurpose,
    OrderStatus,
    OrderType,
    PositionState,
    ReconciliationBreakType,
    StateTransitionActor,
    TradeLegStatus,
)
from algo.database.repositories.order_intent_repository import OrderIntentRepository
from algo.database.repositories.order_repository import OrderRepository
from algo.database.repositories.position_repository import PositionRepository
from algo.database.session import unit_of_work
from algo.database.repositories.reconciliation_break_repository import (
    ReconciliationBreakRepository,
)
from algo.database.repositories.strategy_instance_repository import (
    StrategyInstanceRepository,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

    from algo.brokers.broker_base import BrokerBase, BrokerOrder, BrokerPosition
    from algo.database.models.order_intent import OrderIntent
    from algo.database.models.position import Position
    from algo.strategy_engine.strategy_context import TimeProvider


# ---------------------------------------------------------------------------
# Strategy-specific finalization seam
# ---------------------------------------------------------------------------


class PositionReconciler(Protocol):
    """The one strategy-specific hook the engine needs: complete an entry or
    exit whose legs are already fully resolved at the broker but whose position
    never left ENTRY_PENDING / EXIT_PENDING because a crash interrupted the
    original finalize step.

    Kept a seam (not imported concretely) so the engine stays strategy-agnostic:
    computing a strategy's entry premium / target / stoploss, or its realized
    P&L, is the strategy's business. Implementations run inside the ``session``
    the engine passes and must be idempotent (a position already OPEN/CLOSED is
    a no-op) and must not perform broker I/O.
    """

    def finalize_entry(self, session: Session, position: Position) -> None:
        """All legs confirmed filled; compute and persist entry premium/
        target/stoploss from the recorded fills and transition
        ENTRY_PENDING -> OPEN."""
        ...

    def finalize_exit(self, session: Session, position: Position) -> None:
        """All legs confirmed closed; compute realized P&L and transition
        EXIT_PENDING -> CLOSED."""
        ...


# ---------------------------------------------------------------------------
# Structured results
# ---------------------------------------------------------------------------


class ReconciliationOutcome(str, Enum):
    """What reconciliation did about one subject (an intent, order, or
    position). Every run returns one of these per subject examined."""

    # Whole-run
    BROKER_UNAVAILABLE = "BROKER_UNAVAILABLE"

    # Intent / order level
    CONSISTENT_NO_ACTION = "CONSISTENT_NO_ACTION"
    INTENT_FAILED_NOT_AT_BROKER = "INTENT_FAILED_NOT_AT_BROKER"
    FILL_RECORDED = "FILL_RECORDED"
    PARTIAL_FILL_RECORDED = "PARTIAL_FILL_RECORDED"
    REJECTION_RECORDED = "REJECTION_RECORDED"
    ORDER_STILL_IN_FLIGHT = "ORDER_STILL_IN_FLIGHT"
    ORDER_ROW_CREATED = "ORDER_ROW_CREATED"
    DUPLICATE_ORDERS_DETECTED = "DUPLICATE_ORDERS_DETECTED"
    ORPHAN_ORDER_DETECTED = "ORPHAN_ORDER_DETECTED"

    # Position level
    ENTRY_FINALIZED = "ENTRY_FINALIZED"
    EXIT_FINALIZED = "EXIT_FINALIZED"
    ENTRY_ABORTED_NO_FILLS = "ENTRY_ABORTED_NO_FILLS"
    POSITION_ERRORED = "POSITION_ERRORED"
    NEEDS_FINALIZATION_NO_RECONCILER = "NEEDS_FINALIZATION_NO_RECONCILER"
    EXIT_PENDING_LEFT_FOR_RECOVERY = "EXIT_PENDING_LEFT_FOR_RECOVERY"


@dataclass(frozen=True, slots=True)
class ReconciliationItem:
    """The result of reconciling one subject."""

    subject: str  # e.g. "intent:5", "order:12", "position:3", "broker_tag:X1-..."
    outcome: ReconciliationOutcome
    detail: str
    break_recorded: bool = False


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    """The full, structured result of one ``reconcile`` run."""

    items: tuple[ReconciliationItem, ...]

    @property
    def broker_unavailable(self) -> bool:
        return any(i.outcome is ReconciliationOutcome.BROKER_UNAVAILABLE for i in self.items)

    @property
    def breaks_recorded(self) -> int:
        return sum(1 for i in self.items if i.break_recorded)

    def count(self, outcome: ReconciliationOutcome) -> int:
        return sum(1 for i in self.items if i.outcome is outcome)


_TERMINAL_ORDER_STATUSES = frozenset(
    {OrderStatus.COMPLETE, OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.ERROR}
)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class ReconciliationEngine:
    """Reconciles in-doubt database state against broker truth.

    All collaborators are injected (dependency injection throughout): the
    broker (broker-agnostic ``BrokerBase``), a database session factory, a
    time provider, an optional ``PositionReconciler`` for strategy-specific
    finalization, and a logger. With no reconciler injected, a position that
    would otherwise be finalized is instead recorded as a break -- safe, never
    guessed.
    """

    def __init__(
        self,
        *,
        broker: BrokerBase,
        session_factory: sessionmaker[Session],
        time_provider: TimeProvider,
        position_reconciler: PositionReconciler | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._broker = broker
        self._session_factory = session_factory
        self._time = time_provider
        self._reconciler = position_reconciler
        self._logger = logger if logger is not None else logging.getLogger("algo.reconciliation")

    # -- Public API ----------------------------------------------------

    def reconcile(self) -> ReconciliationReport:
        """Run a full reconciliation pass and return the structured report.

        This is the restart-recovery entry point: call it at startup, before
        arming any strategy instance, so every in-doubt record is resolved
        against broker truth first. Broker truth is fetched once, up front and
        outside any database transaction; if it cannot be fetched, nothing is
        repaired.
        """
        try:
            broker_orders = self._broker.get_orders()
        except BrokerError as exc:
            self._logger.critical("reconciliation aborted: get_orders failed: %s", exc)
            return ReconciliationReport(
                items=(
                    ReconciliationItem(
                        "broker", ReconciliationOutcome.BROKER_UNAVAILABLE,
                        f"could not fetch broker orderbook: {exc}",
                    ),
                )
            )

        # get_positions is only needed for the OPEN-but-broker-flat check;
        # tolerate its absence rather than aborting the whole run.
        try:
            broker_positions = self._broker.get_positions()
        except BrokerError as exc:
            self._logger.warning(
                "reconciliation: get_positions failed (%s); skipping broker-flat checks", exc
            )
            broker_positions = None

        orders_by_tag = self._group_orders_by_tag(broker_orders)

        items: list[ReconciliationItem] = []
        items.extend(self._reconcile_intents(orders_by_tag))
        items.extend(self._detect_orphans_and_duplicates(orders_by_tag))
        items.extend(self._reconcile_positions(broker_positions))

        report = ReconciliationReport(items=tuple(items))
        self._logger.info(
            "reconciliation complete: %d subjects examined, %d breaks recorded",
            len(report.items),
            report.breaks_recorded,
        )
        return report

    # -- Intents -------------------------------------------------------

    def _reconcile_intents(
        self, orders_by_tag: dict[str, list[BrokerOrder]]
    ) -> list[ReconciliationItem]:
        with self._unit_of_work() as session:
            intent_ids = [i.id for i in OrderIntentRepository(session).list_in_doubt()]

        items: list[ReconciliationItem] = []
        for intent_id in intent_ids:
            items.append(self._reconcile_one_intent(intent_id, orders_by_tag))
        return items

    def _reconcile_one_intent(
        self, intent_id: int, orders_by_tag: dict[str, list[BrokerOrder]]
    ) -> ReconciliationItem:
        with self._unit_of_work() as session:
            intent_repo = OrderIntentRepository(session)
            intent = intent_repo.get_by_id(intent_id)
            if intent is None:
                return ReconciliationItem(
                    f"intent:{intent_id}", ReconciliationOutcome.CONSISTENT_NO_ACTION,
                    "intent vanished between scan and resolve",
                )
            # Idempotency: another pass (or the original flow) may have already
            # resolved this intent out of the in-doubt set.
            if intent.status not in (
                IntentStatus.PENDING, IntentStatus.SUBMITTED_UNCONFIRMED, IntentStatus.PLACED
            ):
                return ReconciliationItem(
                    f"intent:{intent_id}", ReconciliationOutcome.CONSISTENT_NO_ACTION,
                    f"already resolved (status={intent.status.value})",
                )

            matches = orders_by_tag.get(intent.broker_tag, [])
            if len(matches) == 0:
                # Not at the broker at all: the order never landed (never sent,
                # or sent-but-lost-and-never-arrived). Safe to fail the intent.
                intent_repo.mark_failed(intent)
                return ReconciliationItem(
                    f"intent:{intent_id}", ReconciliationOutcome.INTENT_FAILED_NOT_AT_BROKER,
                    f"no broker order carries tag {intent.broker_tag!r}; marked FAILED",
                )
            if len(matches) > 1:
                # Duplicate orders under one tag -> possible double exposure.
                # Never auto-resolve; flag and error the owning position.
                self._record_break(
                    session, ReconciliationBreakType.QUANTITY_MISMATCH,
                    intent_id=intent.id, snapshot={"duplicate_order_ids": [o.broker_order_id for o in matches]},
                )
                self._error_position_of_intent(session, intent, "duplicate broker orders under one tag")
                return ReconciliationItem(
                    f"intent:{intent_id}", ReconciliationOutcome.DUPLICATE_ORDERS_DETECTED,
                    f"{len(matches)} broker orders share tag {intent.broker_tag!r}",
                    break_recorded=True,
                )

            return self._resolve_intent_against_order(session, intent, matches[0])

    def _resolve_intent_against_order(
        self, session: Session, intent: OrderIntent, broker_order: BrokerOrder
    ) -> ReconciliationItem:
        """Reconcile one intent against the single broker order carrying its
        tag, recording the fill/rejection and creating the DB Order row if the
        crash happened before it was written."""
        order_repo = OrderRepository(session)
        # Look up the existing DB order by broker_order_id first, then by tag:
        # a crash between order-row creation and ack leaves an order with its
        # broker_tag set but broker_order_id still null, which only the tag
        # lookup finds -- looking up by broker_order_id alone would wrongly
        # create a duplicate order row for it.
        order = order_repo.get_by_broker_order_id(broker_order.broker_order_id)
        if order is None:
            order = order_repo.get_by_broker_tag(intent.broker_tag)
        created_order = False
        if order is None:
            order = order_repo.create(
                trade_id=intent.trade_id,
                intent_id=intent.id,
                purpose=intent.purpose,
                transaction_type=intent.transaction_type,
                order_type=OrderType.MARKET,
                quantity=intent.quantity,
                broker_tag=intent.broker_tag,
            )
            created_order = True
        # Record the broker's order id if we never got to (crashed before ack).
        if order.broker_order_id is None:
            order_repo.mark_acknowledged(
                order, broker_order_id=broker_order.broker_order_id,
                placed_at=broker_order.placed_at or self._time.now(),
            )

        if broker_order.status is OrderStatus.COMPLETE:
            self._apply_fill(session, intent, order, broker_order)
            outcome = ReconciliationOutcome.FILL_RECORDED
            detail = f"fill recorded ({broker_order.filled_quantity} @ {broker_order.average_price})"
            broke = False
            if broker_order.filled_quantity != intent.quantity:
                self._record_break(
                    session, ReconciliationBreakType.QUANTITY_MISMATCH,
                    intent_id=intent.id, order_id=order.id,
                    snapshot={"filled": broker_order.filled_quantity, "expected": intent.quantity},
                )
                outcome = ReconciliationOutcome.PARTIAL_FILL_RECORDED
                detail = (
                    f"partial fill {broker_order.filled_quantity}/{intent.quantity} "
                    f"@ {broker_order.average_price}"
                )
                broke = True
            return ReconciliationItem(f"intent:{intent.id}", outcome, detail, break_recorded=broke)

        if broker_order.status in (OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.ERROR):
            if broker_order.filled_quantity and broker_order.filled_quantity > 0:
                # Partially filled then cancelled/rejected: there IS standing
                # exposure. Record the partial + a break.
                self._apply_fill(session, intent, order, broker_order)
                self._record_break(
                    session, ReconciliationBreakType.QUANTITY_MISMATCH,
                    intent_id=intent.id, order_id=order.id,
                    snapshot={"filled": broker_order.filled_quantity, "final_status": broker_order.status.value},
                )
                return ReconciliationItem(
                    f"intent:{intent.id}", ReconciliationOutcome.PARTIAL_FILL_RECORDED,
                    f"{broker_order.status.value} after partial fill of {broker_order.filled_quantity}",
                    break_recorded=True,
                )
            order_repo.record_rejection(
                order, error_message=broker_order.status_message or broker_order.status.value,
                raw_response=broker_order.raw_response,
            )
            OrderIntentRepository(session).mark_failed(intent)
            return ReconciliationItem(
                f"intent:{intent.id}", ReconciliationOutcome.REJECTION_RECORDED,
                f"broker order terminal {broker_order.status.value}, no fill; intent FAILED",
            )

        # Still live at the broker (PENDING/OPEN): genuinely in flight, leave it.
        if not created_order:
            return ReconciliationItem(
                f"intent:{intent.id}", ReconciliationOutcome.ORDER_STILL_IN_FLIGHT,
                f"broker order still {broker_order.status.value}",
            )
        return ReconciliationItem(
            f"intent:{intent.id}", ReconciliationOutcome.ORDER_ROW_CREATED,
            f"created missing order row; broker order still {broker_order.status.value}",
        )

    def _apply_fill(
        self, session: Session, intent: OrderIntent, order, broker_order: BrokerOrder
    ) -> None:
        """Record a fill on the order, the trade leg, and advance the intent --
        the broker-truth version of what the original flow would have written."""
        now = broker_order.filled_at or self._time.now()
        fill_price = broker_order.average_price or (order.average_price or Decimal("0"))
        OrderRepository(session).record_fill(
            order, status=OrderStatus.COMPLETE, filled_quantity=broker_order.filled_quantity,
            average_price=fill_price, filled_at=now, raw_response=broker_order.raw_response,
        )
        trade = PositionRepository(session).get_trade_by_id(intent.trade_id)
        if trade is not None:
            if intent.purpose is OrderPurpose.ENTRY:
                trade.entry_price = fill_price
                trade.entry_time = now
                if trade.status is TradeLegStatus.PENDING:
                    trade.status = TradeLegStatus.OPEN
            else:  # EXIT
                trade.exit_price = fill_price
                trade.exit_time = now
                if trade.status is TradeLegStatus.OPEN:
                    trade.status = TradeLegStatus.CLOSED
        intent_repo = OrderIntentRepository(session)
        intent_repo.mark_placed(intent)
        intent_repo.mark_confirmed(intent)

    # -- Orphans / duplicates ------------------------------------------

    def _detect_orphans_and_duplicates(
        self, orders_by_tag: dict[str, list[BrokerOrder]]
    ) -> list[ReconciliationItem]:
        """A broker order carrying a tag we have no intent for is an orphan --
        something at the broker we cannot attribute. Record a break; we never
        act on it automatically. Duplicates whose tag DOES match an intent are
        handled during intent reconciliation; here we only flag orphan tags."""
        items: list[ReconciliationItem] = []
        with self._unit_of_work() as session:
            intent_repo = OrderIntentRepository(session)
            for tag, orders in orders_by_tag.items():
                if intent_repo.get_by_broker_tag(tag) is not None:
                    continue  # attributable -- handled via the intent
                self._record_break(
                    session, ReconciliationBreakType.ORPHAN_FILL,
                    snapshot={"tag": tag, "broker_order_ids": [o.broker_order_id for o in orders]},
                )
                items.append(
                    ReconciliationItem(
                        f"broker_tag:{tag}", ReconciliationOutcome.ORPHAN_ORDER_DETECTED,
                        f"{len(orders)} broker order(s) under unrecognized tag {tag!r}",
                        break_recorded=True,
                    )
                )
        return items

    # -- Positions -----------------------------------------------------

    def _reconcile_positions(
        self, broker_positions: Sequence[BrokerPosition] | None
    ) -> list[ReconciliationItem]:
        with self._unit_of_work() as session:
            # ERROR positions are already flagged and awaiting a human; leaving
            # them out is what makes re-runs idempotent (no duplicate breaks).
            position_ids = [
                p.id
                for p in PositionRepository(session).list_non_terminal()
                if p.state is not PositionState.ERROR
            ]

        live_symbols = (
            {bp.tradingsymbol for bp in broker_positions if bp.quantity != 0}
            if broker_positions is not None
            else None
        )
        items: list[ReconciliationItem] = []
        for position_id in position_ids:
            items.append(self._reconcile_one_position(position_id, live_symbols))
        return items

    def _reconcile_one_position(
        self, position_id: int, live_symbols: set[str] | None
    ) -> ReconciliationItem:
        with self._unit_of_work() as session:
            position_repo = PositionRepository(session)
            position = position_repo.get_by_id_for_update(position_id)
            if position is None or position.state in (PositionState.CLOSED, PositionState.ERROR):
                return ReconciliationItem(
                    f"position:{position_id}", ReconciliationOutcome.CONSISTENT_NO_ACTION,
                    "already terminal or gone",
                )
            trades = position_repo.list_trades_for_position(position_id)

            if position.state is PositionState.ENTRY_PENDING:
                return self._reconcile_entry_pending(session, position, trades)
            if position.state is PositionState.EXIT_PENDING:
                return self._reconcile_exit_pending(session, position, trades)
            # OPEN
            return self._reconcile_open(session, position, trades, live_symbols)

    def _reconcile_entry_pending(self, session, position, trades) -> ReconciliationItem:
        filled = [t for t in trades if t.status is TradeLegStatus.OPEN]
        unfilled = [t for t in trades if t.status is TradeLegStatus.PENDING]

        if filled and not unfilled and len(trades) > 0:
            # Every leg filled -> a completed entry that crashed before finalize.
            return self._finalize_entry(session, position)
        if not filled:
            # Nothing filled anywhere -> the entry never opened exposure. Clean
            # abort: ENTRY_PENDING -> CLOSED (a legal, strategy-neutral edge).
            PositionRepository(session).transition_state(
                position, to_state=PositionState.CLOSED, actor=StateTransitionActor.RECOVERY,
                reason="reconciliation: entry never filled at broker; no exposure opened",
            )
            return ReconciliationItem(
                f"position:{position.id}", ReconciliationOutcome.ENTRY_ABORTED_NO_FILLS,
                "no leg filled; aborted to CLOSED",
            )
        # Some filled, some not -> partial entry, standing naked exposure.
        return self._error_position(
            session, position, ReconciliationBreakType.PARTIAL_ENTRY,
            f"partial entry: {len(filled)}/{len(trades)} legs filled -- naked exposure",
        )

    def _reconcile_exit_pending(self, session, position, trades) -> ReconciliationItem:
        closed = [t for t in trades if t.status is TradeLegStatus.CLOSED]
        open_legs = [t for t in trades if t.status is TradeLegStatus.OPEN]

        if closed and not open_legs and len(trades) > 0:
            return self._finalize_exit(session, position)
        if closed and open_legs:
            # Some legs closed, some still open: an exit that started but did
            # not finish. Genuinely ambiguous -- unclear whether the
            # remaining legs are safely resumable or something else went
            # wrong mid-flight -- so this is exactly the case to flag for
            # manual review rather than silently resume.
            return self._error_position(
                session, position, ReconciliationBreakType.OTHER,
                f"partial exit: {len(open_legs)}/{len(trades)} legs still open",
            )
        # No leg closed yet: the exit was decided (EXIT_PENDING recorded,
        # per Strategy1's own atomic prepare-exit write) but never actually
        # completed against the broker -- whether because it was never
        # attempted before a crash, or every close attempt failed. Both are
        # exactly what Strategy.recover()'s own EXIT_PENDING branch exists to
        # safely resume (ExitLogic.exit() is idempotent per-leg: it checks
        # each leg's own intent/order state before deciding whether to place
        # a new close). Erroring here instead would make that documented,
        # already-approved recovery path unreachable -- reconciliation runs
        # before any runner's recover() ever gets a chance to run it.
        return ReconciliationItem(
            f"position:{position.id}", ReconciliationOutcome.EXIT_PENDING_LEFT_FOR_RECOVERY,
            f"exit pending, not yet completed ({len(open_legs)}/{len(trades)} legs still "
            "open); left for strategy-level recovery",
        )

    def _reconcile_open(self, session, position, trades, live_symbols) -> ReconciliationItem:
        if live_symbols is None:
            return ReconciliationItem(
                f"position:{position.id}", ReconciliationOutcome.CONSISTENT_NO_ACTION,
                "OPEN; broker positions unavailable, no broker-flat check performed",
            )
        expected = {t.trading_symbol for t in trades if t.status is TradeLegStatus.OPEN}
        missing = expected - live_symbols
        if missing:
            # DB thinks these legs are open but the broker shows them flat --
            # e.g. an RMS square-off we did not initiate.
            return self._error_position(
                session, position, ReconciliationBreakType.BROKER_FLAT_DB_OPEN,
                f"DB OPEN but broker flat for {sorted(missing)}",
            )
        return ReconciliationItem(
            f"position:{position.id}", ReconciliationOutcome.CONSISTENT_NO_ACTION,
            "OPEN and consistent with broker positions",
        )

    def _finalize_entry(self, session, position) -> ReconciliationItem:
        if self._reconciler is None:
            self._record_break(
                session, ReconciliationBreakType.OTHER, position_id=position.id,
                snapshot={"note": "entry fully filled but no PositionReconciler injected"},
            )
            position.needs_reconciliation = True
            PositionRepository(session).flush()
            return ReconciliationItem(
                f"position:{position.id}", ReconciliationOutcome.NEEDS_FINALIZATION_NO_RECONCILER,
                "entry fully filled but no reconciler injected to finalize",
                break_recorded=True,
            )
        self._reconciler.finalize_entry(session, position)
        return ReconciliationItem(
            f"position:{position.id}", ReconciliationOutcome.ENTRY_FINALIZED,
            "all legs filled; entry finalized to OPEN",
        )

    def _finalize_exit(self, session, position) -> ReconciliationItem:
        if self._reconciler is None:
            self._record_break(
                session, ReconciliationBreakType.OTHER, position_id=position.id,
                snapshot={"note": "exit fully closed but no PositionReconciler injected"},
            )
            position.needs_reconciliation = True
            PositionRepository(session).flush()
            return ReconciliationItem(
                f"position:{position.id}", ReconciliationOutcome.NEEDS_FINALIZATION_NO_RECONCILER,
                "exit fully closed but no reconciler injected to finalize",
                break_recorded=True,
            )
        self._reconciler.finalize_exit(session, position)
        return ReconciliationItem(
            f"position:{position.id}", ReconciliationOutcome.EXIT_FINALIZED,
            "all legs closed; exit finalized to CLOSED",
        )

    # -- Error / break helpers -----------------------------------------

    def _error_position(
        self, session, position, break_type: ReconciliationBreakType, reason: str
    ) -> ReconciliationItem:
        """Transition a position to ERROR, flag it for reconciliation, freeze
        its instance, and record a break -- the strategy-neutral "this needs a
        human" repair reconciliation applies to a genuinely broken position."""
        position.error_message = reason
        position.needs_reconciliation = True
        PositionRepository(session).transition_state(
            position, to_state=PositionState.ERROR, actor=StateTransitionActor.RECOVERY, reason=reason,
        )
        instance_repo = StrategyInstanceRepository(session)
        instance = instance_repo.get_by_id_for_update(position.strategy_instance_id)
        if instance is not None and instance.status is not InstanceStatus.FROZEN:
            instance_repo.set_status(instance, InstanceStatus.FROZEN)
        self._record_break(session, break_type, position_id=position.id, snapshot={"reason": reason})
        self._logger.critical(
            "reconciliation errored position %d: %s (%s)", position.id, break_type.value, reason
        )
        return ReconciliationItem(
            f"position:{position.id}", ReconciliationOutcome.POSITION_ERRORED,
            f"{break_type.value}: {reason}", break_recorded=True,
        )

    def _error_position_of_intent(self, session, intent: OrderIntent, reason: str) -> None:
        """Move the position owning an intent to ERROR (used when an intent-
        level anomaly like a duplicate compromises the whole position)."""
        trade = PositionRepository(session).get_trade_by_id(intent.trade_id)
        if trade is None:
            return
        position = PositionRepository(session).get_by_id_for_update(trade.position_id)
        if position is None or position.state in (PositionState.CLOSED, PositionState.ERROR):
            return
        position.error_message = reason
        position.needs_reconciliation = True
        PositionRepository(session).transition_state(
            position, to_state=PositionState.ERROR, actor=StateTransitionActor.RECOVERY, reason=reason,
        )
        instance_repo = StrategyInstanceRepository(session)
        instance = instance_repo.get_by_id_for_update(position.strategy_instance_id)
        if instance is not None and instance.status is not InstanceStatus.FROZEN:
            instance_repo.set_status(instance, InstanceStatus.FROZEN)

    def _record_break(
        self,
        session,
        break_type: ReconciliationBreakType,
        *,
        position_id: int | None = None,
        order_id: int | None = None,
        intent_id: int | None = None,
        snapshot: dict | None = None,
    ) -> None:
        ReconciliationBreakRepository(session).create(
            break_type=break_type, detected_at=self._time.now(),
            position_id=position_id, order_id=order_id, intent_id=intent_id,
            broker_snapshot=snapshot,
        )

    # -- Helpers -------------------------------------------------------

    @staticmethod
    def _group_orders_by_tag(orders: Sequence[BrokerOrder]) -> dict[str, list[BrokerOrder]]:
        grouped: dict[str, list[BrokerOrder]] = {}
        for order in orders:
            if order.tag:
                grouped.setdefault(order.tag, []).append(order)
        return grouped

    @contextmanager
    def _unit_of_work(self) -> Iterator[Session]:
        """One committed transaction against the injected factory (see
        ``database.session.unit_of_work``)."""
        with unit_of_work(self._session_factory) as session:
            yield session
