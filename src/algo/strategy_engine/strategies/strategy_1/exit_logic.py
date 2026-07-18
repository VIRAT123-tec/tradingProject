"""Exit orchestration for Strategy-1: evaluate the exit triggers and, when one
fires, close both straddle legs safely with full crash-recovery support.

Two clearly separated halves live here:

1. The *decision* -- ``evaluate_exit`` (a pure function) applies the exit
   priority order to the current premium/time/halt state and returns which
   exit reason (if any) has fired. It is pure and exhaustively unit-testable;
   it reads no database and places no orders.

2. The *execution* -- ``ExitLogic.exit(reason)`` closes both legs with BUY
   market orders, following the same intent-first crash-recovery ceremony as
   entry, computes realized P&L, and drives the position OPEN -> EXIT_PENDING
   -> CLOSED.

This module owns neither *when* to look (the monitor feeds it) nor the tick
loop itself -- ``evaluate_exit`` is called by monitor.py each tick, and
``exit`` is called once a trigger fires (or directly by the risk layer for a
kill-switch / emergency exit).

Two deliberate asymmetries with entry_logic, both safety-critical:

* Exits are *never* gated by risk. A risk-reducing close must never be blocked
  by a limit meant for new exposure, so this module never calls
  ``RiskGateway.approve_entry``; it only *reads* the halt state as a trigger
  to exit.

* A partial failure is handled by ERROR, not by unwinding. On entry a partial
  fill leaves unwanted exposure we opened, so we buy it back. On exit a leg we
  could not close is exposure we *tried and failed* to flatten -- re-opening
  anything would be nonsensical -- so the position goes to ERROR and the
  still-open leg is surfaced for a human, never silently left.

Note (tech debt, flagged): the per-leg placement ceremony here overlaps
substantially with entry_logic's. Both are candidates for extraction into the
spec's planned execution/order_manager.py in a later refactor; they are kept
self-contained for now to avoid expanding this task's scope into entry_logic.
"""

from __future__ import annotations

import threading
import time as _time_module
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

from algo.brokers.broker_base import PlaceOrderRequest
from algo.brokers.exceptions import (
    BrokerConnectionError,
    BrokerError,
    BrokerRateLimitExceededError,
    BrokerTimeoutError,
    InvalidOrderRequestError,
    OrderRejectedError,
)
from algo.common.enums import (
    Exchange,
    ExitReason,
    InstanceStatus,
    OptionType,
    OrderPurpose,
    OrderStatus,
    OrderType,
    PositionState,
    ReconciliationBreakType,
    StateTransitionActor,
    TradeLegStatus,
    TransactionType,
)
from algo.database.repositories.order_intent_repository import OrderIntentRepository
from algo.database.repositories.order_repository import OrderRepository
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.reconciliation_break_repository import (
    ReconciliationBreakRepository,
)
from algo.database.repositories.strategy_instance_repository import (
    StrategyInstanceRepository,
)
from algo.database.session import unit_of_work
from algo.services.order_update_processor import reconcile_order_terminal
from algo.strategy_engine.strategies.strategy_1.config import Strategy1Config
from algo.strategy_engine.strategies.strategy_1.state_machine import PositionStateMachine

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from algo.strategy_engine.strategy_context import StrategyContext


# ---------------------------------------------------------------------------
# Pure decision logic
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExitDecision:
    """Outcome of evaluating the exit triggers. ``reason`` is set iff
    ``should_exit`` is True."""

    should_exit: bool
    reason: ExitReason | None
    detail: str


def evaluate_exit(
    *,
    now_ist_time: time,
    hard_cutoff_time: time,
    halted: bool,
    combined_premium: Decimal | None,
    target_premium: Decimal | None,
    stoploss_premium: Decimal | None,
) -> ExitDecision:
    """Apply the exit priority order and return whether (and why) to exit.

    Priority, highest first: **time cutoff -> kill-switch/emergency -> stoploss
    -> target**. The two highest triggers (cutoff and halt) fire *regardless of
    price* -- they do not need, and are not blocked by the absence of, a current
    combined premium, because "get flat now" must not depend on market data
    being healthy. Stoploss and target require a current premium; if none is yet
    known (no tick since entry), they simply do not fire.

    Pure: no I/O, no side effects -- the single most safety-critical decision in
    the strategy, isolated for exhaustive unit testing. ``target_premium`` and
    ``stoploss_premium`` are the levels persisted on the Position at entry.
    """
    if now_ist_time >= hard_cutoff_time:
        return ExitDecision(
            True, ExitReason.TIMEOUT, f"hard cutoff {hard_cutoff_time} reached at {now_ist_time}"
        )
    if halted:
        return ExitDecision(
            True, ExitReason.KILL_SWITCH, "risk halt active (kill-switch / emergency exit)"
        )
    if combined_premium is None:
        return ExitDecision(False, None, "no combined premium yet; cannot check stoploss/target")
    if stoploss_premium is not None and combined_premium >= stoploss_premium:
        return ExitDecision(
            True,
            ExitReason.STOPLOSS,
            f"stoploss hit: combined premium {combined_premium} >= {stoploss_premium}",
        )
    if target_premium is not None and combined_premium <= target_premium:
        return ExitDecision(
            True,
            ExitReason.TARGET,
            f"target hit: combined premium {combined_premium} <= {target_premium}",
        )
    return ExitDecision(False, None, f"no trigger: combined premium {combined_premium}")


def compute_leg_realized_pnl(
    *, entry_price: Decimal, exit_price: Decimal, quantity: int
) -> Decimal:
    """Realized P&L for one *short* leg = (entry_price - exit_price) * quantity.

    A short leg is sold at entry and bought back at exit, so it profits when the
    exit price is below the entry price. Pure and unit-tested; the combined
    position P&L is the sum over both legs, which is algebraically
    ``(combined_entry_premium - combined_exit_premium) * quantity``.
    """
    return (entry_price - exit_price) * quantity


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


class ExitOutcome(str, Enum):
    """Result category of an ``exit()`` call."""

    EXITED = "EXITED"
    SKIPPED_ALREADY_CLOSED = "SKIPPED_ALREADY_CLOSED"
    SKIPPED_EXIT_IN_PROGRESS = "SKIPPED_EXIT_IN_PROGRESS"
    NOT_OPEN = "NOT_OPEN"
    PARTIAL_EXIT_ERROR = "PARTIAL_EXIT_ERROR"
    AMBIGUOUS_ERROR = "AMBIGUOUS_ERROR"
    PERSISTENCE_DIVERGENCE = "PERSISTENCE_DIVERGENCE"


@dataclass(frozen=True, slots=True)
class ExitResult:
    outcome: ExitOutcome
    message: str
    position_id: int | None = None
    realized_pnl: Decimal | None = None


class _LegCloseOutcome(str, Enum):
    CLOSED = "CLOSED"
    FAILED = "FAILED"  # could not be closed (rejected / retries exhausted)
    AMBIGUOUS = "AMBIGUOUS"  # a close order may or may not have executed


@dataclass(frozen=True, slots=True)
class _LegCloseResult:
    outcome: _LegCloseOutcome
    reason: str
    exit_price: Decimal | None = None


_ACTOR_BY_REASON: dict[ExitReason, StateTransitionActor] = {
    ExitReason.TARGET: StateTransitionActor.STRATEGY,
    ExitReason.STOPLOSS: StateTransitionActor.STRATEGY,
    ExitReason.TIMEOUT: StateTransitionActor.STRATEGY,
    ExitReason.KILL_SWITCH: StateTransitionActor.KILL_SWITCH,
    ExitReason.MANUAL: StateTransitionActor.MANUAL,
    ExitReason.ERROR: StateTransitionActor.STRATEGY,
}


class ExitLogic:
    """Orchestrates closing one Strategy-1 straddle position.

    Collaborators are injected via the ``StrategyContext`` (identity, config,
    broker, risk, time, logger, session factory). ``evaluate`` is a thin,
    side-effect-free wrapper over ``evaluate_exit`` for the monitor to call each
    tick; ``exit`` performs the actual close.

    One ``ExitLogic`` instance exists per running strategy instance (built once
    in ``Strategy1.__init__`` and shared by both ``PositionMonitor`` -- via
    ``on_tick``/``poll_and_check`` -> ``_trigger_exit`` -- and ``Strategy1.recover``'s
    interrupted-exit resume), and every caller reaches it through
    ``StrategyRunner``'s per-instance dispatch lock. That lock serialises
    *dispatch* (one tick/trigger handled at a time), but a single dispatch can
    itself take seconds (broker calls, fill-confirmation polling) -- long enough
    for two conceptually independent triggers (e.g. the monitor's own price-based
    check and the scheduler's hard-cutoff trigger, both of which route through
    ``poll_and_check``) to both decide "exit now" and both call ``exit()`` before
    either has finished, whether because they land on the same tick, because a
    future change parallelises dispatch, or because ``exit()`` is reached by
    another path entirely. Without a guard, both would race unlocked reads and
    writes of the same ``Position``/``Order`` rows -- the optimistic-locking
    version check would correctly reject the loser's write, but nothing caught
    that rejection, so it surfaced as an unhandled ``StaleDataError`` that froze
    the instance.

    ``exit()`` therefore claims a private, non-reentrant ``threading.Lock``
    (``_exit_lock``) as its very first action, before any database read. A
    caller that cannot claim it immediately is not queued or retried -- it is
    told, synchronously and without error, that an exit is already in progress
    (``ExitOutcome.SKIPPED_EXIT_IN_PROGRESS``) and returns having touched
    neither the broker nor the database. This makes "exactly one exit executes
    per position" true by construction, at the one point every caller already
    passes through, rather than relying on every current and future caller to
    independently get their own synchronization right.
    """

    def __init__(
        self,
        *,
        context: StrategyContext,
        order_timeout: float | None = None,
        fill_confirmation_attempts: int = 20,
        fill_confirmation_delay: float = 0.25,
        close_retry_attempts: int = 3,
        close_retry_delay: float = 0.5,
        sleep: Callable[[float], None] = _time_module.sleep,
    ) -> None:
        if not isinstance(context.config, Strategy1Config):
            raise TypeError(
                "ExitLogic requires context.config to be a Strategy1Config, got "
                f"{type(context.config).__name__}"
            )
        if fill_confirmation_attempts < 1:
            raise ValueError("fill_confirmation_attempts must be at least 1")
        if close_retry_attempts < 1:
            raise ValueError("close_retry_attempts must be at least 1")

        self._context = context
        self._config: Strategy1Config = context.config
        self._identity = context.identity
        self._logger = context.logger
        self._order_timeout = order_timeout
        self._fill_attempts = fill_confirmation_attempts
        self._fill_delay = fill_confirmation_delay
        self._close_attempts = close_retry_attempts
        self._close_delay = close_retry_delay
        self._sleep = sleep
        # Non-reentrant by design: a second exit() call for this instance --
        # from any caller, on any thread -- must be told to stand down, never
        # silently re-enter. See the class docstring for why this exists.
        self._exit_lock = threading.Lock()

    # -- Decision (called by monitor.py per tick) ----------------------

    def evaluate(
        self,
        *,
        now_ist_time: time,
        halted: bool,
        combined_premium: Decimal | None,
        target_premium: Decimal | None,
        stoploss_premium: Decimal | None,
    ) -> ExitDecision:
        """Thin, pure wrapper over ``evaluate_exit`` using this instance's
        configured hard cutoff time. Side-effect free -- deciding is separate
        from executing so the monitor can log/act on the decision itself."""
        return evaluate_exit(
            now_ist_time=now_ist_time,
            hard_cutoff_time=self._config.hard_cutoff_time,
            halted=halted,
            combined_premium=combined_premium,
            target_premium=target_premium,
            stoploss_premium=stoploss_premium,
        )

    # -- Execution -----------------------------------------------------

    def exit(self, reason: ExitReason) -> ExitResult:
        """Close today's open straddle for ``reason``.

        Validates state (only an OPEN or in-progress EXIT_PENDING position can
        be exited), records a durable exit intent before any broker call,
        places BUY market closes for both legs (idempotently), computes realized
        P&L, and drives the position to CLOSED. Expected non-exit conditions
        (already closed, nothing open) are returned as results, not raised.
        A leg that cannot be closed leaves the position in ERROR with a
        reconciliation break and a frozen instance.

        The very first thing this does -- before any database read -- is claim
        ``_exit_lock`` without blocking. Losing that race is not an error: it
        means another caller is already executing this exact position's exit,
        so this call returns ``SKIPPED_EXIT_IN_PROGRESS`` immediately, having
        made no broker call and no database write. See the class docstring.
        """
        if not self._exit_lock.acquire(blocking=False):
            self._logger.info(
                "exit already in progress for %s; %s trigger returning without action",
                self._identity,
                reason.value,
            )
            return ExitResult(
                ExitOutcome.SKIPPED_EXIT_IN_PROGRESS,
                f"exit already in progress; ignoring concurrent {reason.value} trigger",
            )
        try:
            trade_date = self._context.time.today()
            actor = _ACTOR_BY_REASON[reason]

            # Phase 1: validate state and record the durable pre-broker exit
            # intent (transition OPEN -> EXIT_PENDING + create both exit
            # intents), or resume an already-EXIT_PENDING position.
            prepared = self._prepare_exit(trade_date, reason, actor)
            if isinstance(prepared, ExitResult):
                return prepared

            # Phase 2: close each still-open leg.
            results: dict[OptionType, _LegCloseResult] = {}
            for option_type, leg in prepared.legs.items():
                results[option_type] = self._close_leg(leg)

            if all(r.outcome is _LegCloseOutcome.CLOSED for r in results.values()):
                return self._finalize_closed(prepared, reason, actor)

            # A leg could not be closed: this is still-live exposure we failed
            # to flatten. ERROR + reconciliation break + freeze; never unwind.
            ambiguous = any(r.outcome is _LegCloseOutcome.AMBIGUOUS for r in results.values())
            self._logger.critical(
                "exit for %s did not fully close: %s",
                self._identity,
                {ot.value: r.outcome.value for ot, r in results.items()},
            )
            self._record_break(
                prepared.position_id,
                ReconciliationBreakType.AMBIGUOUS_ACK if ambiguous else ReconciliationBreakType.OTHER,
            )
            detail = "; ".join(f"{ot.value}={r.outcome.value}" for ot, r in results.items())
            return self._fail_to_error(
                prepared.position_id,
                outcome=ExitOutcome.AMBIGUOUS_ERROR if ambiguous else ExitOutcome.PARTIAL_EXIT_ERROR,
                reason=f"exit incomplete ({reason.value}): {detail}",
            )
        finally:
            self._exit_lock.release()

    # -- Phase 1: prepare ----------------------------------------------

    def _prepare_exit(
        self, trade_date: date, reason: ExitReason, actor: StateTransitionActor
    ) -> _PreparedExit | ExitResult:
        """Validate the position state and create/resume the durable exit
        record. Returns a _PreparedExit to proceed, or an ExitResult to stop."""
        with self._unit_of_work() as session:
            position_repo = PositionRepository(session)
            position = position_repo.get_by_instance_and_date(
                self._identity.instance_id, trade_date
            )
            if position is None:
                return ExitResult(ExitOutcome.NOT_OPEN, "no position for today")
            if position.state is PositionState.CLOSED:
                return ExitResult(
                    ExitOutcome.SKIPPED_ALREADY_CLOSED,
                    "position already CLOSED",
                    position_id=position.id,
                )
            if position.state not in (PositionState.OPEN, PositionState.EXIT_PENDING):
                return ExitResult(
                    ExitOutcome.NOT_OPEN,
                    f"position state {position.state.value} is not exitable",
                    position_id=position.id,
                )

            # OPEN -> EXIT_PENDING (idempotent no-op if already EXIT_PENDING).
            if position.state is PositionState.OPEN:
                PositionStateMachine(position_repo).transition(
                    position,
                    to_state=PositionState.EXIT_PENDING,
                    actor=actor,
                    reason=f"exit triggered: {reason.value}",
                )

            legs: dict[OptionType, _ExitLegIds] = {}
            trades = position_repo.list_trades_for_position(position.id)
            for trade in trades:
                intent, _ = OrderIntentRepository(session).get_or_create(
                    trade_id=trade.id,
                    purpose=OrderPurpose.EXIT,
                    transaction_type=TransactionType.BUY,
                    quantity=trade.quantity,
                    idempotency_key=self._idempotency_key(trade_date, trade.option_type),
                    broker_tag=self._broker_tag(trade_date, trade.option_type),
                )
                legs[trade.option_type] = _ExitLegIds(
                    trade_id=trade.id,
                    intent_id=intent.id,
                    tradingsymbol=trade.trading_symbol,
                    exchange=trade.exchange,
                    option_type=trade.option_type,
                    quantity=trade.quantity,
                )
            position_id = position.id

        self._logger.info(
            "exit prepared for %s: position=%d reason=%s", self._identity, position_id, reason.value
        )
        return _PreparedExit(position_id=position_id, legs=legs)

    # -- Phase 2: close a leg ------------------------------------------

    def _close_leg(self, leg: _ExitLegIds) -> _LegCloseResult:
        """Close one leg with a BUY market order, idempotently and with bounded
        retries on never-sent failures."""
        # Idempotency: a leg already CLOSED (this call is a resume/re-run) needs
        # no action.
        with self._unit_of_work() as session:
            trade = PositionRepository(session).get_trade_by_id(leg.trade_id)
            assert trade is not None
            if trade.status is TradeLegStatus.CLOSED:
                return _LegCloseResult(
                    _LegCloseOutcome.CLOSED, "already closed", exit_price=trade.exit_price
                )
            intent = OrderIntentRepository(session).get_by_id_or_raise(leg.intent_id)
            already_attempted = intent.broker_call_started_at is not None
            broker_tag = intent.broker_tag

        # If a prior attempt already reached the broker, adopt its order rather
        # than risk a duplicate BUY (which would over-close into a long).
        if already_attempted:
            adopted = self._adopt_existing_order(leg, broker_tag)
            if adopted is not None:
                return adopted

        return self._place_close_with_retries(leg, broker_tag)

    def _adopt_existing_order(
        self, leg: _ExitLegIds, broker_tag: str
    ) -> _LegCloseResult | None:
        """If an order carrying this leg's exit tag already exists at the
        broker (from a pre-crash attempt), resolve it instead of re-placing.
        Returns a result if the order is found, else None (safe to place)."""
        try:
            existing = self._context.broker.find_order_by_tag(
                broker_tag, timeout=self._order_timeout
            )
        except BrokerError as exc:
            # Cannot confirm -> treat as ambiguous rather than risk a duplicate.
            return _LegCloseResult(
                _LegCloseOutcome.AMBIGUOUS, f"could not query existing order: {exc}"
            )
        if existing is None:
            return None
        terminal = self._poll_until_terminal(existing.broker_order_id)
        if terminal is not None and terminal.status is OrderStatus.COMPLETE:
            price = terminal.average_price or Decimal("0")
            self._record_close_fill(leg, terminal.broker_order_id, price, terminal)
            return _LegCloseResult(_LegCloseOutcome.CLOSED, "adopted prior fill", exit_price=price)
        return _LegCloseResult(
            _LegCloseOutcome.AMBIGUOUS, "prior exit order exists but is not confirmed filled"
        )

    def _place_close_with_retries(
        self, leg: _ExitLegIds, broker_tag: str
    ) -> _LegCloseResult:
        """Place the BUY close, retrying only on never-sent failures (a close
        that never reached the broker is always safe to resend); a rejection is
        definitive and a timeout is ambiguous -- neither is retried."""
        with self._unit_of_work() as session:
            intent_repo = OrderIntentRepository(session)
            intent_repo.mark_broker_call_started(
                intent_repo.get_by_id_or_raise(leg.intent_id),
                started_at=self._context.time.now(),
            )

        request = PlaceOrderRequest(
            exchange=leg.exchange,
            tradingsymbol=leg.tradingsymbol,
            transaction_type=TransactionType.BUY,
            quantity=leg.quantity,
            product=self._config.product_type,
            order_type=OrderType.MARKET,
            tag=broker_tag,
        )

        last_reason = "not attempted"
        for attempt in range(self._close_attempts):
            try:
                placement = self._context.broker.place_order(
                    request, timeout=self._order_timeout
                )
            except (OrderRejectedError, InvalidOrderRequestError) as exc:
                self._record_close_rejection(leg, str(exc))
                return _LegCloseResult(_LegCloseOutcome.FAILED, f"rejected: {exc}")
            except (BrokerConnectionError, BrokerRateLimitExceededError) as exc:
                last_reason = f"not sent: {exc}"
                self._logger.warning(
                    "close of %s leg %s not sent (attempt %d): %s",
                    self._identity,
                    leg.option_type.value,
                    attempt,
                    exc,
                )
                if attempt < self._close_attempts - 1:
                    self._sleep(self._close_delay)
                continue
            except BrokerTimeoutError as exc:
                # Ambiguous: may have executed. Never re-send a mutating call.
                return _LegCloseResult(_LegCloseOutcome.AMBIGUOUS, f"ack timed out: {exc}")

            return self._confirm_close(leg, placement.broker_order_id)

        # Exhausted retries without ever sending successfully.
        self._mark_intent_failed(leg.intent_id)
        return _LegCloseResult(_LegCloseOutcome.FAILED, last_reason)

    def _confirm_close(self, leg: _ExitLegIds, broker_order_id: str) -> _LegCloseResult:
        """Record the acknowledged close order and confirm its fill."""
        placed_at = self._context.time.now()
        with self._unit_of_work() as session:
            order_repo = OrderRepository(session)
            order = order_repo.create(
                trade_id=leg.trade_id,
                intent_id=leg.intent_id,
                purpose=OrderPurpose.EXIT,
                transaction_type=TransactionType.BUY,
                order_type=OrderType.MARKET,
                quantity=leg.quantity,
                broker_tag=self._broker_tag(self._context.time.today(), leg.option_type),
            )
            order_repo.mark_acknowledged(
                order, broker_order_id=broker_order_id, placed_at=placed_at
            )
            intent_repo = OrderIntentRepository(session)
            intent_repo.mark_placed(intent_repo.get_by_id_or_raise(leg.intent_id))

        terminal = self._poll_until_terminal(broker_order_id)
        if terminal is None:
            return _LegCloseResult(
                _LegCloseOutcome.AMBIGUOUS,
                f"close order {broker_order_id} acknowledged but not terminal in poll window",
            )
        if terminal.status is OrderStatus.COMPLETE:
            price = terminal.average_price or Decimal("0")
            self._record_close_fill(leg, broker_order_id, price, terminal)
            return _LegCloseResult(_LegCloseOutcome.CLOSED, "closed", exit_price=price)
        # A close order that came back REJECTED/CANCELLED did not close the leg.
        # An Order row for it already exists (created + acknowledged above),
        # so this must update that row, not create a second one -- hence
        # passing broker_order_id/terminal through.
        self._record_close_rejection(
            leg, terminal.status_message or terminal.status.value,
            broker_order_id=broker_order_id, broker_order=terminal,
        )
        return _LegCloseResult(_LegCloseOutcome.FAILED, f"terminal {terminal.status.value}")

    def _record_close_fill(
        self, leg: _ExitLegIds, broker_order_id: str, exit_price: Decimal, broker_order
    ) -> None:
        """Persist a leg's close: the order fill, the trade's exit price/time/
        realized P&L, the leg marked CLOSED, and the intent CONFIRMED.

        The two writes are split into two transactions on purpose. The Order-row
        fill is reconciled first, in its own unit of work
        (``reconcile_order_terminal``), because that write races the push path's
        ``OrderUpdateProcessor`` and can lose the optimistic-lock version check.
        Isolating it means a lost race rolls back *that* session cleanly and is
        swallowed (the push path already committed the identical fill), so it can
        never poison this method's own trade/leg/intent writes -- which is what
        used to surface as a ``PendingRollbackError`` freezing a position that
        had actually exited. The trade/leg/intent rows are ExitLogic's alone (the
        push path never touches them), so they are then written exactly once in a
        fresh, healthy session regardless of who won the order-row race."""
        now = broker_order.filled_at or self._context.time.now()

        reconcile_order_terminal(
            session_factory=self._context.session_factory,
            broker_order_id=broker_order_id,
            broker_order=broker_order,
            now=now,
            logger=self._logger,
        )

        with self._unit_of_work() as session:
            position_repo = PositionRepository(session)
            trade = position_repo.get_trade_by_id(leg.trade_id)
            assert trade is not None
            trade.exit_price = exit_price
            trade.exit_time = now
            if trade.entry_price is not None:
                trade.realized_pnl = compute_leg_realized_pnl(
                    entry_price=trade.entry_price, exit_price=exit_price, quantity=trade.quantity
                )
            trade.status = TradeLegStatus.CLOSED

            intent_repo = OrderIntentRepository(session)
            intent_repo.mark_confirmed(intent_repo.get_by_id_or_raise(leg.intent_id))

    # -- Finalisation --------------------------------------------------

    def _finalize_closed(
        self, prepared: _PreparedExit, reason: ExitReason, actor: StateTransitionActor
    ) -> ExitResult:
        """Both legs closed: compute the combined exit premium and realized
        P&L, record them on the Position, and transition EXIT_PENDING -> CLOSED.

        If this final write fails after both legs are already flat at the
        broker, that divergence is recorded as a reconciliation break + CRITICAL
        alert rather than lost."""
        now = self._context.time.now()
        try:
            with self._unit_of_work() as session:
                position_repo = PositionRepository(session)
                position = position_repo.get_by_id_for_update(prepared.position_id)
                if position is None:
                    raise RuntimeError(f"position {prepared.position_id} vanished before finalize")

                trades = position_repo.list_trades_for_position(position.id)
                combined_exit = sum((t.exit_price or Decimal("0") for t in trades), Decimal("0"))
                realized = sum((t.realized_pnl or Decimal("0") for t in trades), Decimal("0"))

                position.combined_exit_premium = combined_exit
                position.realized_pnl = realized
                position.exit_reason = reason
                position.exit_completed_at = now

                PositionStateMachine(position_repo).transition(
                    position,
                    to_state=PositionState.CLOSED,
                    actor=actor,
                    reason=(
                        f"exit complete ({reason.value}): exit premium {combined_exit}, "
                        f"realized P&L {realized}"
                    ),
                )
        except Exception:
            self._logger.critical(
                "BROKER/DB DIVERGENCE for %s: both legs closed at broker but the "
                "CLOSED write failed; position %d left EXIT_PENDING for recovery",
                self._identity,
                prepared.position_id,
                exc_info=True,
            )
            self._record_break(prepared.position_id, ReconciliationBreakType.OTHER)
            return ExitResult(
                ExitOutcome.PERSISTENCE_DIVERGENCE,
                "both legs closed but CLOSED persistence failed; reconciliation break recorded",
                position_id=prepared.position_id,
            )

        self._logger.info(
            "exit CLOSED for %s: position=%d reason=%s realized_pnl=%s",
            self._identity,
            prepared.position_id,
            reason.value,
            realized,
        )
        # Observability side channels: the position is now durably CLOSED and
        # its transaction committed, so both reads settled data only. Each is
        # fully isolated -- neither the Excel report nor the trade-history write
        # may turn a completed exit into an error.
        self._export_closed_trades(prepared.position_id)
        self._record_trade_history(prepared.position_id)
        return ExitResult(
            ExitOutcome.EXITED,
            f"exited ({reason.value}) with realized P&L {realized}",
            position_id=prepared.position_id,
            realized_pnl=realized,
        )

    def _export_closed_trades(self, position_id: int) -> None:
        """Ask the injected trade exporter (if any) to refresh this trading
        day's report after a position closes. No-op when no exporter is wired
        (tests, or export disabled in config); every failure is contained so
        this can never affect the exit."""
        exporter = self._context.trade_exporter
        if exporter is None:
            return
        try:
            exporter.export_closed_position(position_id)
        except Exception:  # noqa: BLE001 -- reporting must never break the exit path
            self._logger.error(
                "trade report export raised for position %d; exit is unaffected",
                position_id,
                exc_info=True,
            )

    def _record_trade_history(self, position_id: int) -> None:
        """Ask the injected recorder (if any) to persist the permanent
        trade-history row after a position closes. No-op when unwired (tests);
        every failure is contained -- the trade stays safe in the execution
        tables and can be backfilled, so this can never affect the exit."""
        recorder = self._context.trade_history_recorder
        if recorder is None:
            return
        try:
            recorder.record_closed_position(position_id)
        except Exception:  # noqa: BLE001 -- history recording must never break the exit path
            self._logger.error(
                "trade_history recording raised for position %d; exit is unaffected",
                position_id,
                exc_info=True,
            )

    # -- Error handling ------------------------------------------------

    def _fail_to_error(
        self, position_id: int, *, outcome: ExitOutcome, reason: str
    ) -> ExitResult:
        """Transition the Position to ERROR and freeze this instance, then
        return the error result."""
        self._logger.error("exit error for %s: %s", self._identity, reason)
        with self._unit_of_work() as session:
            position_repo = PositionRepository(session)
            position = position_repo.get_by_id_for_update(position_id)
            if position is not None and position.state not in (
                PositionState.CLOSED,
                PositionState.ERROR,
            ):
                position.exit_reason = ExitReason.ERROR
                position.error_message = reason
                PositionStateMachine(position_repo).mark_error(
                    position, actor=StateTransitionActor.STRATEGY, reason=reason
                )
            instance_repo = StrategyInstanceRepository(session)
            instance = instance_repo.get_by_id_for_update(self._identity.instance_id)
            if instance is not None and instance.status is not InstanceStatus.FROZEN:
                instance_repo.set_status(instance, InstanceStatus.FROZEN)
        return ExitResult(outcome, reason, position_id=position_id)

    def _record_break(self, position_id: int, break_type: ReconciliationBreakType) -> None:
        try:
            with self._unit_of_work() as session:
                ReconciliationBreakRepository(session).create(
                    break_type=break_type,
                    detected_at=self._context.time.now(),
                    position_id=position_id,
                )
            self._logger.critical(
                "reconciliation break recorded for %s: %s (position=%d)",
                self._identity,
                break_type.value,
                position_id,
            )
        except Exception:
            self._logger.critical(
                "failed to record reconciliation break %s for %s",
                break_type.value,
                self._identity,
                exc_info=True,
            )

    def _record_close_rejection(
        self,
        leg: _ExitLegIds,
        message: str,
        *,
        broker_order_id: str | None = None,
        broker_order=None,
    ) -> None:
        """Record a close attempt's rejection.

        Two distinct callers, two distinct shapes -- both handled here so
        neither duplicates this logic:

        * Pre-ack (``broker_order_id`` is None): ``place_order`` itself raised
          before any Order row existed for this attempt -- create one fresh,
          exactly as before.
        * Post-ack (``broker_order_id`` given): an Order row already exists
          (created + acknowledged by ``_confirm_close`` before polling) -- this
          must update *that* row, not create a second one, and does so through
          ``reconcile_order_terminal``: its own isolated, idempotent,
          defer-on-conflict transaction, so a concurrent push-path update to the
          same row is never raced into an error that could poison the intent
          write below (the same freeze ``_record_close_fill`` avoids).

        The intent is then marked failed in a fresh session in both shapes.
        """
        if broker_order_id is not None:
            assert broker_order is not None
            reconcile_order_terminal(
                session_factory=self._context.session_factory,
                broker_order_id=broker_order_id,
                broker_order=broker_order,
                now=self._context.time.now(),
                logger=self._logger,
            )
        with self._unit_of_work() as session:
            if broker_order_id is None:
                order_repo = OrderRepository(session)
                order = order_repo.create(
                    trade_id=leg.trade_id,
                    intent_id=leg.intent_id,
                    purpose=OrderPurpose.EXIT,
                    transaction_type=TransactionType.BUY,
                    order_type=OrderType.MARKET,
                    quantity=leg.quantity,
                    broker_tag=self._broker_tag(self._context.time.today(), leg.option_type),
                )
                order_repo.record_rejection(order, error_message=message)
            intent_repo = OrderIntentRepository(session)
            intent_repo.mark_failed(intent_repo.get_by_id_or_raise(leg.intent_id))

    def _mark_intent_failed(self, intent_id: int) -> None:
        with self._unit_of_work() as session:
            intent_repo = OrderIntentRepository(session)
            intent_repo.mark_failed(intent_repo.get_by_id_or_raise(intent_id))

    # -- Shared helpers ------------------------------------------------

    def _poll_until_terminal(self, broker_order_id: str):
        terminal = {
            OrderStatus.COMPLETE,
            OrderStatus.REJECTED,
            OrderStatus.CANCELLED,
            OrderStatus.ERROR,
        }
        for attempt in range(self._fill_attempts):
            try:
                broker_order = self._context.broker.get_order(
                    broker_order_id, timeout=self._order_timeout
                )
            except BrokerError as exc:
                self._logger.warning(
                    "get_order(%s) failed on attempt %d: %s", broker_order_id, attempt, exc
                )
                broker_order = None
            if broker_order is not None and broker_order.status in terminal:
                return broker_order
            if attempt < self._fill_attempts - 1:
                self._sleep(self._fill_delay)
        return None

    def _idempotency_key(self, trade_date: date, option_type: OptionType) -> str:
        return ":".join(
            [
                str(self._identity.instance_id),
                trade_date.isoformat(),
                option_type.value,
                OrderPurpose.EXIT.value,
            ]
        )

    def _broker_tag(self, trade_date: date, option_type: OptionType) -> str:
        tag = f"X{self._identity.instance_id}-{trade_date:%y%m%d}-{option_type.value}"
        if len(tag) > 20:
            raise ValueError(
                f"broker tag {tag!r} exceeds 20 characters -- instance id too large "
                "for the readable-tag scheme; a hashed scheme is needed"
            )
        return tag

    @contextmanager
    def _unit_of_work(self) -> Iterator[Session]:
        """One committed transaction against the context's injected factory (see
        ``database.session.unit_of_work``)."""
        with unit_of_work(self._context.session_factory) as session:
            yield session


@dataclass(frozen=True, slots=True)
class _ExitLegIds:
    trade_id: int
    intent_id: int
    tradingsymbol: str
    exchange: Exchange
    option_type: OptionType
    quantity: int


@dataclass(frozen=True, slots=True)
class _PreparedExit:
    position_id: int
    legs: dict[OptionType, _ExitLegIds]
