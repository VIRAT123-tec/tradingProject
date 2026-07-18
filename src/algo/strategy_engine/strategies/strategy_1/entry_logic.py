"""Entry orchestration for Strategy-1: run exactly one straddle entry per
trading day per instrument, safely, with full crash-recovery support.

This module is the orchestrator that ties the previously-built pieces together
-- StrikeSelector (which ATM contracts), the combined_premium math (target/SL),
the PositionStateMachine (legal, audited state transitions), the repositories
(persistence), and BrokerBase (order placement). It owns the *sequencing and
safety policy* of an entry; it deliberately does not decide *when* to run (the
scheduler calls it) and does not monitor the position afterwards (monitor.py
does that).

The four hard requirements this module exists to satisfy, and how:

1. Exactly one entry per day -- the positions table's
   ``UNIQUE(strategy_instance_id, trade_date)`` constraint, reached through
   ``PositionRepository.get_or_create`` (insert-and-catch, never read-then-
   write), is the idempotency guard. A duplicate scheduler fire or a restart
   after entry finds the existing row and stops.

2. Recoverability of a crash mid-placement -- every order is preceded by a
   durable ``OrderIntent`` committed *before* the broker is called, and the
   intent is marked SUBMITTED_UNCONFIRMED immediately before the call. A crash
   between "order sent" and "result recorded" is therefore always detectable
   on restart (an in-doubt intent), never a silent orphan.

3. Safe partial-failure handling -- if one leg is confirmed filled and the
   other fails or is ambiguous, the confirmed leg is auto-unwound (bought back)
   so the platform is never left holding a single naked short, then the
   position goes to ERROR for review. An *ambiguous* leg (a timeout, where we
   do not know if it is live) is never auto-unwound -- unwinding a leg that
   never actually filled would itself create naked exposure -- it is left for
   recovery, which can query the broker by tag.

4. Broker/DB divergence is never silent -- if the broker succeeds but a
   subsequent DB write fails, a ReconciliationBreak row is written and a
   CRITICAL alert logged, so the discrepancy surfaces for reconciliation rather
   than disappearing.

Auto-unwind is the partial-failure policy the schema and enums were built
around (``TradeLegStatus.UNWOUND`` exists precisely for it); it is applied here
as the platform's established default.
"""

from __future__ import annotations

import time as _time_module
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
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
from algo.database.models.trade import Trade
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
from algo.strategy_engine.strategies.strategy_1.combined_premium import compute_thresholds
from algo.strategy_engine.strategies.strategy_1.config import Strategy1Config
from algo.strategy_engine.strategies.strategy_1.state_machine import PositionStateMachine

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from algo.brokers.broker_base import BrokerInstrument
    from algo.services.expiry_service import ExpiryService
    from algo.services.pricing_service import SpotPriceProvider
    from algo.strategy_engine.strategies.strategy_1.strike_selector import (
        StraddleStrikeSelection,
        StrikeSelector,
    )
    from algo.strategy_engine.strategy_context import StrategyContext


class EntryOutcome(str, Enum):
    """The result category of an ``enter()`` call, so the caller (strategy.py)
    can log/branch without inspecting the database."""

    ENTERED = "ENTERED"
    SKIPPED_ALREADY_EXISTS = "SKIPPED_ALREADY_EXISTS"
    SKIPPED_EXPIRY_DAY = "SKIPPED_EXPIRY_DAY"
    BLOCKED_BY_RISK = "BLOCKED_BY_RISK"
    ENTRY_REJECTED = "ENTRY_REJECTED"
    PARTIAL_ENTRY_ERROR = "PARTIAL_ENTRY_ERROR"
    AMBIGUOUS_ERROR = "AMBIGUOUS_ERROR"
    PERSISTENCE_DIVERGENCE = "PERSISTENCE_DIVERGENCE"


@dataclass(frozen=True, slots=True)
class EntryResult:
    """Outcome of one entry attempt. ``position_id`` is present once a Position
    row exists (i.e. everything except the pre-creation skips)."""

    outcome: EntryOutcome
    message: str
    position_id: int | None = None


class _LegOutcome(str, Enum):
    """Internal per-leg placement result."""

    FILLED = "FILLED"
    FAILED = "FAILED"  # definitively not live -- rejection or never-sent; no exposure
    AMBIGUOUS = "AMBIGUOUS"  # may or may not be live -- must not be auto-unwound


@dataclass(frozen=True, slots=True)
class _LegResult:
    outcome: _LegOutcome
    reason: str
    fill_price: Decimal | None = None


class EntryLogic:
    """Orchestrates one straddle entry for one Strategy-1 instance.

    All collaborators are injected (dependency injection throughout): the
    strategy's ``StrategyContext`` supplies identity/config/broker/risk/time/
    logger and the DB session factory; ``StrikeSelector``, ``ExpiryService``,
    and ``SpotPriceProvider`` are injected directly since they need services
    the context does not itself carry. This is what makes the whole flow
    unit-testable against the simulation broker plus fakes.

    ``sleep`` and the fill-confirmation knobs are injectable so tests run with
    no real delay; against the synchronous simulation broker an order is
    already terminal on the first status read, so no sleeping occurs.
    """

    def __init__(
        self,
        *,
        context: StrategyContext,
        strike_selector: StrikeSelector,
        expiry_service: ExpiryService,
        spot_price_provider: SpotPriceProvider,
        order_timeout: float | None = None,
        fill_confirmation_attempts: int = 20,
        fill_confirmation_delay: float = 0.25,
        sleep: Callable[[float], None] = _time_module.sleep,
    ) -> None:
        if not isinstance(context.config, Strategy1Config):
            raise TypeError(
                "EntryLogic requires context.config to be a Strategy1Config, got "
                f"{type(context.config).__name__}"
            )
        if fill_confirmation_attempts < 1:
            raise ValueError("fill_confirmation_attempts must be at least 1")

        self._context = context
        self._config: Strategy1Config = context.config
        self._identity = context.identity
        self._logger = context.logger
        self._strike_selector = strike_selector
        self._expiry_service = expiry_service
        self._spot_price_provider = spot_price_provider
        self._order_timeout = order_timeout
        self._fill_attempts = fill_confirmation_attempts
        self._fill_delay = fill_confirmation_delay
        self._sleep = sleep

    # -- Public API ----------------------------------------------------

    def enter(self) -> EntryResult:
        """Attempt today's straddle entry, returning a structured result.

        Expected outcomes (already-entered, expiry-day skip, risk block,
        rejection, partial failure, ambiguity) are returned as ``EntryResult``
        values, not raised -- they are normal, handled conditions. Only a
        genuinely unexpected failure propagates as an exception, which the
        StrategyRunner turns into an instance freeze. A handled ERROR outcome
        (rejection / partial / ambiguity) transitions the Position to ERROR
        *and* freezes this instance itself, per the spec, before returning.
        """
        trade_date = self._context.time.today()

        existing = self._find_existing_position(trade_date)
        if existing is not None:
            self._logger.info(
                "entry skipped: position already exists for %s on %s (state=%s)",
                self._identity,
                trade_date,
                existing,
            )
            return EntryResult(
                EntryOutcome.SKIPPED_ALREADY_EXISTS,
                f"position already exists for {trade_date} in state {existing}",
            )

        if self._should_skip_expiry_day(trade_date):
            self._logger.info("entry skipped: expiry-day (0DTE) skip for %s", self._identity)
            return EntryResult(
                EntryOutcome.SKIPPED_EXPIRY_DAY,
                f"skip_on_expiry_day is set and {trade_date} is an expiry day",
            )

        risk_block = self._check_risk()
        if risk_block is not None:
            self._logger.warning("entry blocked by risk for %s: %s", self._identity, risk_block)
            return EntryResult(EntryOutcome.BLOCKED_BY_RISK, risk_block)

        selection = self._resolve_straddle(trade_date)

        # Durable pre-broker record -- Position(ENTRY_PENDING) + both Trade legs
        # + both OrderIntents, committed before any broker call.
        durable = self._create_durable_record(trade_date, selection)
        if durable is None:
            # A concurrent writer created the position between our check and
            # this insert; the unique constraint won, so we stand down.
            self._logger.info("entry skipped: lost the create race for %s", self._identity)
            return EntryResult(
                EntryOutcome.SKIPPED_ALREADY_EXISTS,
                "another writer created today's position concurrently",
            )

        return self._place_and_finalize(durable, selection)

    # -- Pre-gates -----------------------------------------------------

    def _find_existing_position(self, trade_date: date) -> PositionState | None:
        """Cheap idempotency early-out: the state of today's position if one
        already exists, else None. The authoritative guard is still
        get_or_create below; this only avoids doing work when an entry has
        clearly already happened."""
        with self._unit_of_work() as session:
            position = PositionRepository(session).get_by_instance_and_date(
                self._identity.instance_id, trade_date
            )
            return position.state if position is not None else None

    def _should_skip_expiry_day(self, trade_date: date) -> bool:
        if not self._config.skip_on_expiry_day:
            return False
        expiry = self._expiry_service.get_current_weekly_expiry(
            self._identity.instrument, trade_date
        )
        return expiry == trade_date

    def _check_risk(self) -> str | None:
        """Run pre-trade risk gates. Returns a reason string if entry is
        blocked, or None if approved."""
        if self._context.risk.is_halted(self._identity):
            return "trading is halted (kill-switch / emergency-exit active)"
        quantity = self._config.lots  # lot count; lot_size is applied at order time
        decision = self._context.risk.approve_entry(self._identity, quantity=quantity)
        if not decision.approved:
            return decision.reason or "entry rejected by risk (no reason given)"
        return None

    def _resolve_straddle(self, trade_date: date) -> StraddleStrikeSelection:
        spot_ltp = self._spot_price_provider.get_spot_ltp(self._identity.instrument)
        return self._strike_selector.select(
            instrument=self._identity.instrument, spot_ltp=spot_ltp, as_of=trade_date
        )

    # -- Durable pre-broker record -------------------------------------

    def _create_durable_record(
        self, trade_date: date, selection: StraddleStrikeSelection
    ) -> _DurableRecord | None:
        """Create and commit Position(ENTRY_PENDING) + both Trade legs + both
        OrderIntents. Returns their ids, or None if the position already
        existed (lost create race)."""
        lot_size = selection.call.lot_size
        quantity = self._config.lots * lot_size

        with self._unit_of_work() as session:
            position_repo = PositionRepository(session)
            position, created = position_repo.get_or_create(
                strategy_instance_id=self._identity.instance_id,
                trade_date=trade_date,
                initial_state=PositionState.ENTRY_PENDING,
                expiry_date=selection.expiry,
                strike=selection.atm_strike,
                strike_interval=selection.strike_interval,
                lots=self._config.lots,
                lot_size=lot_size,
                quantity=quantity,
                entry_spot_ltp=selection.spot_ltp,
                target_pct=self._config.target_pct,
                sl_pct=self._config.sl_pct,
                entry_signal_time=self._context.time.now(),
            )
            if not created:
                return None

            legs: dict[OptionType, _LegIds] = {}
            for option_type, contract in (
                (OptionType.CE, selection.call),
                (OptionType.PE, selection.put),
            ):
                trade = position_repo.add_trade(
                    Trade(
                        position_id=position.id,
                        option_type=option_type,
                        trading_symbol=contract.tradingsymbol,
                        exchange=contract.exchange,
                        strike=selection.atm_strike,
                        quantity=quantity,
                        status=TradeLegStatus.PENDING,
                    )
                )
                intent, _ = OrderIntentRepository(session).get_or_create(
                    trade_id=trade.id,
                    purpose=OrderPurpose.ENTRY,
                    transaction_type=TransactionType.SELL,
                    quantity=quantity,
                    idempotency_key=self._idempotency_key(
                        trade_date, option_type, OrderPurpose.ENTRY
                    ),
                    broker_tag=self._broker_tag(trade_date, option_type, OrderPurpose.ENTRY),
                )
                legs[option_type] = _LegIds(
                    trade_id=trade.id,
                    intent_id=intent.id,
                    tradingsymbol=contract.tradingsymbol,
                    contract=contract,
                )

            self._logger.info(
                "durable entry record created for %s: position=%d strike=%s expiry=%s qty=%d",
                self._identity,
                position.id,
                selection.atm_strike,
                selection.expiry,
                quantity,
            )
            return _DurableRecord(position_id=position.id, quantity=quantity, legs=legs)

    # -- Placement + finalisation --------------------------------------

    def _place_and_finalize(
        self, durable: _DurableRecord, selection: StraddleStrikeSelection
    ) -> EntryResult:
        ce = durable.legs[OptionType.CE]
        pe = durable.legs[OptionType.PE]

        ce_result = self._place_leg(ce, durable.quantity)

        if ce_result.outcome is _LegOutcome.FAILED:
            # No exposure from CE; do not place PE (a single leg is not a
            # straddle and would be naked). Clean-ish failure -> ERROR.
            return self._fail_to_error(
                durable.position_id,
                outcome=EntryOutcome.ENTRY_REJECTED,
                reason=f"CE leg failed before any exposure: {ce_result.reason}",
            )
        if ce_result.outcome is _LegOutcome.AMBIGUOUS:
            # CE may or may not be live; placing PE could create a naked pair,
            # and we cannot safely unwind an unknown leg. Freeze + reconcile.
            self._record_break(durable.position_id, ce.intent_id, ReconciliationBreakType.AMBIGUOUS_ACK)
            return self._fail_to_error(
                durable.position_id,
                outcome=EntryOutcome.AMBIGUOUS_ERROR,
                reason=f"CE leg outcome ambiguous: {ce_result.reason}",
            )

        # CE is FILLED. Place PE.
        pe_result = self._place_leg(pe, durable.quantity)

        if pe_result.outcome is _LegOutcome.FILLED:
            return self._finalize_open(durable, selection, ce_result, pe_result)

        # Partial entry: CE is live, PE failed or is ambiguous. Auto-unwind CE.
        self._logger.critical(
            "partial entry for %s: CE filled, PE %s -- auto-unwinding CE leg",
            self._identity,
            pe_result.outcome.value,
        )
        unwound = self._auto_unwind(ce, durable.quantity)
        break_type = (
            ReconciliationBreakType.AMBIGUOUS_ACK
            if pe_result.outcome is _LegOutcome.AMBIGUOUS
            else ReconciliationBreakType.PARTIAL_ENTRY
        )
        self._record_break(durable.position_id, pe.intent_id, break_type)
        detail = "CE unwound" if unwound else "CE UNWIND FAILED -- NAKED EXPOSURE"
        return self._fail_to_error(
            durable.position_id,
            outcome=EntryOutcome.PARTIAL_ENTRY_ERROR,
            reason=f"partial entry (PE {pe_result.outcome.value}); {detail}",
        )

    def _place_leg(self, leg: _LegIds, quantity: int) -> _LegResult:
        """Place one SELL market order for a leg, following the intent-first
        crash-recovery ceremony, and confirm the fill. Returns the leg's
        outcome; never raises for an expected broker condition."""
        # Mark the intent SUBMITTED_UNCONFIRMED and commit *before* the call,
        # so a crash during the call is recoverable as an in-doubt intent.
        with self._unit_of_work() as session:
            intent = OrderIntentRepository(session).get_by_id_or_raise(leg.intent_id)
            OrderIntentRepository(session).mark_broker_call_started(
                intent, started_at=self._context.time.now()
            )
            broker_tag = intent.broker_tag

        request = PlaceOrderRequest(
            exchange=leg.contract.exchange,
            tradingsymbol=leg.tradingsymbol,
            transaction_type=TransactionType.SELL,
            quantity=quantity,
            product=self._config.product_type,
            order_type=OrderType.MARKET,
            tag=broker_tag,
        )

        try:
            placement = self._context.broker.place_order(request, timeout=self._order_timeout)
        except (OrderRejectedError, InvalidOrderRequestError) as exc:
            self._record_leg_rejection(leg, str(exc))
            return _LegResult(_LegOutcome.FAILED, f"rejected before ack: {exc}")
        except (BrokerConnectionError, BrokerRateLimitExceededError) as exc:
            # Never reached the broker -> no exposure -> definitively failed.
            self._mark_intent_failed(leg.intent_id)
            return _LegResult(_LegOutcome.FAILED, f"not sent: {exc}")
        except BrokerTimeoutError as exc:
            # Ambiguous: the order may have reached the broker. Leave the
            # intent SUBMITTED_UNCONFIRMED for recovery; never auto-retry.
            return _LegResult(_LegOutcome.AMBIGUOUS, f"ack timed out: {exc}")

        # Broker acknowledged: record the Order row and confirm the fill.
        return self._confirm_and_record(leg, quantity, placement.broker_order_id)

    def _confirm_and_record(
        self, leg: _LegIds, quantity: int, broker_order_id: str
    ) -> _LegResult:
        """Create the Order row for an acknowledged placement, poll for its
        terminal status, and persist the outcome."""
        placed_at = self._context.time.now()
        with self._unit_of_work() as session:
            order = OrderRepository(session).create(
                trade_id=leg.trade_id,
                intent_id=leg.intent_id,
                purpose=OrderPurpose.ENTRY,
                transaction_type=TransactionType.SELL,
                order_type=OrderType.MARKET,
                quantity=quantity,
                broker_tag=self._entry_tag_for(leg),
            )
            OrderRepository(session).mark_acknowledged(
                order, broker_order_id=broker_order_id, placed_at=placed_at
            )
            OrderIntentRepository(session).mark_placed(
                OrderIntentRepository(session).get_by_id_or_raise(leg.intent_id)
            )
            order_id = order.id

        broker_order = self._poll_until_terminal(broker_order_id)

        if broker_order is None:
            # Acknowledged but never reached a terminal state in the poll
            # window: there is a live order we cannot confirm as filled.
            return _LegResult(
                _LegOutcome.AMBIGUOUS,
                f"order {broker_order_id} acknowledged but not terminal within poll window",
            )

        # Reconcile the Order row with the broker's terminal snapshot in its own
        # isolated transaction. This write races OrderUpdateProcessor's push-path
        # write to the same row; keeping it self-contained (see
        # reconcile_order_terminal) means a lost optimistic-lock race rolls back
        # *that* session cleanly and is deferred to the push path's committed
        # result, instead of poisoning a shared session and freezing entry with a
        # PendingRollbackError.
        reconcile_order_terminal(
            session_factory=self._context.session_factory,
            broker_order_id=broker_order_id,
            broker_order=broker_order,
            now=self._context.time.now(),
            logger=self._logger,
        )

        # Read the now-committed terminal state from a fresh session and act on
        # it -- whichever writer won the race, the committed row is the truth.
        with self._unit_of_work() as session:
            order_repo = OrderRepository(session)
            order = order_repo.get_by_id_or_raise(order_id)
            if order.status is OrderStatus.COMPLETE:
                fill_price = order.average_price or Decimal("0")
                return _LegResult(_LegOutcome.FILLED, "filled", fill_price=fill_price)
            # REJECTED / CANCELLED after ack: no standing exposure from a
            # zero-filled entry order. The order row was already reconciled
            # above; only the intent -- which only entry_logic.py ever touches --
            # remains to be marked failed, here in this same fresh session.
            intent_repo = OrderIntentRepository(session)
            intent_repo.mark_failed(intent_repo.get_by_id_or_raise(leg.intent_id))
            return _LegResult(_LegOutcome.FAILED, f"terminal {order.status.value} after ack")

    def _poll_until_terminal(self, broker_order_id: str):
        """Poll ``get_order`` until the order is terminal or attempts run out.
        Returns the terminal BrokerOrder, or None if it never settled."""
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

    def _finalize_open(
        self,
        durable: _DurableRecord,
        selection: StraddleStrikeSelection,
        ce_result: _LegResult,
        pe_result: _LegResult,
    ) -> EntryResult:
        """Both legs filled: record fills onto the trades, compute entry
        premium + target/SL, transition the Position to OPEN, and confirm the
        intents -- all in one transaction.

        If this final DB write fails after the broker has already filled both
        legs, that is the broker/DB divergence the spec calls out: record a
        reconciliation break + CRITICAL alert and leave the position
        ENTRY_PENDING for recovery, rather than losing the discrepancy.
        """
        assert ce_result.fill_price is not None
        assert pe_result.fill_price is not None
        entry_premium = ce_result.fill_price + pe_result.fill_price
        thresholds = compute_thresholds(
            entry_premium, self._config.target_pct, self._config.sl_pct
        )
        now = self._context.time.now()

        try:
            with self._unit_of_work() as session:
                position_repo = PositionRepository(session)
                position = position_repo.get_by_id_for_update(durable.position_id)
                if position is None:
                    raise RuntimeError(f"position {durable.position_id} vanished before finalize")

                for option_type, leg_result in (
                    (OptionType.CE, ce_result),
                    (OptionType.PE, pe_result),
                ):
                    trade = position_repo.get_trade_by_id(durable.legs[option_type].trade_id)
                    assert trade is not None
                    trade.entry_price = leg_result.fill_price
                    trade.entry_time = now
                    trade.status = TradeLegStatus.OPEN

                position.combined_entry_premium = entry_premium
                position.target_premium = thresholds.target_premium
                position.stoploss_premium = thresholds.stoploss_premium
                position.entry_completed_at = now

                PositionStateMachine(position_repo).transition(
                    position,
                    to_state=PositionState.OPEN,
                    actor=StateTransitionActor.STRATEGY,
                    reason=(
                        f"entry filled: combined premium {entry_premium} "
                        f"(CE {ce_result.fill_price} + PE {pe_result.fill_price})"
                    ),
                )
                for leg in durable.legs.values():
                    intent_repo = OrderIntentRepository(session)
                    intent_repo.mark_confirmed(intent_repo.get_by_id_or_raise(leg.intent_id))
        except Exception:
            self._logger.critical(
                "BROKER/DB DIVERGENCE for %s: both legs filled at broker but "
                "the OPEN write failed; position %d left ENTRY_PENDING for recovery",
                self._identity,
                durable.position_id,
                exc_info=True,
            )
            self._record_break(
                durable.position_id, None, ReconciliationBreakType.ORPHAN_FILL
            )
            return EntryResult(
                EntryOutcome.PERSISTENCE_DIVERGENCE,
                "both legs filled but OPEN persistence failed; reconciliation break recorded",
                position_id=durable.position_id,
            )

        self._logger.info(
            "entry OPEN for %s: position=%d entry_premium=%s target=%s stoploss=%s",
            self._identity,
            durable.position_id,
            entry_premium,
            thresholds.target_premium,
            thresholds.stoploss_premium,
        )
        return EntryResult(
            EntryOutcome.ENTERED,
            f"entered at combined premium {entry_premium}",
            position_id=durable.position_id,
        )

    # -- Auto-unwind + error handling ----------------------------------

    def _auto_unwind(self, leg: _LegIds, quantity: int) -> bool:
        """Buy back a confirmed-live leg to flatten it after a partial entry.
        Returns True if the unwind order filled, False otherwise (in which
        case naked exposure remains -- the caller records a break and alerts).
        """
        trade_date = self._context.time.today()
        try:
            with self._unit_of_work() as session:
                intent, _ = OrderIntentRepository(session).get_or_create(
                    trade_id=leg.trade_id,
                    purpose=OrderPurpose.EXIT,
                    transaction_type=TransactionType.BUY,
                    quantity=quantity,
                    idempotency_key=self._idempotency_key(
                        trade_date, leg.option_type, OrderPurpose.EXIT, marker="UNWIND"
                    ),
                    broker_tag=self._unwind_tag(trade_date, leg.option_type),
                )
                OrderIntentRepository(session).mark_broker_call_started(
                    intent, started_at=self._context.time.now()
                )
                unwind_tag = intent.broker_tag
                unwind_intent_id = intent.id

            request = PlaceOrderRequest(
                exchange=leg.contract.exchange,
                tradingsymbol=leg.tradingsymbol,
                transaction_type=TransactionType.BUY,
                quantity=quantity,
                product=self._config.product_type,
                order_type=OrderType.MARKET,
                tag=unwind_tag,
            )
            placement = self._context.broker.place_order(request, timeout=self._order_timeout)
            broker_order = self._poll_until_terminal(placement.broker_order_id)
        except BrokerError:
            self._logger.critical(
                "auto-unwind order FAILED for %s leg %s -- naked exposure remains",
                self._identity,
                leg.option_type.value,
                exc_info=True,
            )
            return False

        filled = broker_order is not None and broker_order.status is OrderStatus.COMPLETE
        with self._unit_of_work() as session:
            trade = PositionRepository(session).get_trade_by_id(leg.trade_id)
            if trade is not None:
                trade.status = TradeLegStatus.UNWOUND if filled else TradeLegStatus.ERROR
                if filled and broker_order is not None:
                    trade.exit_price = broker_order.average_price
                    trade.exit_time = broker_order.filled_at or self._context.time.now()
            intent_repo = OrderIntentRepository(session)
            intent = intent_repo.get_by_id_or_raise(unwind_intent_id)
            if filled:
                intent_repo.mark_confirmed(intent)
            else:
                intent_repo.mark_failed(intent)
        return filled

    def _fail_to_error(
        self, position_id: int, *, outcome: EntryOutcome, reason: str
    ) -> EntryResult:
        """Transition the Position to ERROR and freeze this instance, then
        return the error result. Best-effort: a failure to even persist the
        ERROR state is logged critically and re-raised so the runner's own
        freeze path nets it."""
        self._logger.error("entry error for %s: %s", self._identity, reason)
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
        return EntryResult(outcome, reason, position_id=position_id)

    def _record_break(
        self, position_id: int, intent_id: int | None, break_type: ReconciliationBreakType
    ) -> None:
        """Persist a reconciliation break (the durable form of the CRITICAL
        alert) in its own transaction, so it survives even if surrounding
        writes are rolled back."""
        try:
            with self._unit_of_work() as session:
                ReconciliationBreakRepository(session).create(
                    break_type=break_type,
                    detected_at=self._context.time.now(),
                    position_id=position_id,
                    intent_id=intent_id,
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

    def _record_leg_rejection(self, leg: _LegIds, message: str) -> None:
        """Record a pre-ack rejection: an Order row (REJECTED) for the audit
        trail plus the intent marked FAILED."""
        with self._unit_of_work() as session:
            order_repo = OrderRepository(session)
            order = order_repo.create(
                trade_id=leg.trade_id,
                intent_id=leg.intent_id,
                purpose=OrderPurpose.ENTRY,
                transaction_type=TransactionType.SELL,
                order_type=OrderType.MARKET,
                quantity=self._config.lots * leg.contract.lot_size,
                broker_tag=self._entry_tag_for(leg),
            )
            order_repo.record_rejection(order, error_message=message)
            intent_repo = OrderIntentRepository(session)
            intent_repo.mark_failed(intent_repo.get_by_id_or_raise(leg.intent_id))

    def _mark_intent_failed(self, intent_id: int) -> None:
        with self._unit_of_work() as session:
            intent_repo = OrderIntentRepository(session)
            intent_repo.mark_failed(intent_repo.get_by_id_or_raise(intent_id))

    # -- Keys / tags ---------------------------------------------------

    def _idempotency_key(
        self,
        trade_date: date,
        option_type: OptionType,
        purpose: OrderPurpose,
        *,
        marker: str | None = None,
    ) -> str:
        parts = [
            str(self._identity.instance_id),
            trade_date.isoformat(),
            option_type.value,
            purpose.value,
        ]
        if marker:
            parts.append(marker)
        return ":".join(parts)

    def _broker_tag(
        self, trade_date: date, option_type: OptionType, purpose: OrderPurpose
    ) -> str:
        prefix = "E" if purpose is OrderPurpose.ENTRY else "X"
        tag = f"{prefix}{self._identity.instance_id}-{trade_date:%y%m%d}-{option_type.value}"
        return self._validate_tag(tag)

    def _unwind_tag(self, trade_date: date, option_type: OptionType) -> str:
        tag = f"U{self._identity.instance_id}-{trade_date:%y%m%d}-{option_type.value}"
        return self._validate_tag(tag)

    def _entry_tag_for(self, leg: _LegIds) -> str:
        return self._broker_tag(self._context.time.today(), leg.option_type, OrderPurpose.ENTRY)

    @staticmethod
    def _validate_tag(tag: str) -> str:
        if len(tag) > 20:
            raise ValueError(
                f"broker tag {tag!r} exceeds 20 characters -- instance id too large "
                "for the readable-tag scheme; a hashed scheme is needed"
            )
        return tag

    # -- Unit of work --------------------------------------------------

    @contextmanager
    def _unit_of_work(self) -> Iterator[Session]:
        """One committed transaction against the context's injected factory --
        the shared commit/rollback/close pattern (see
        ``database.session.unit_of_work``)."""
        with unit_of_work(self._context.session_factory) as session:
            yield session


@dataclass(frozen=True, slots=True)
class _LegIds:
    trade_id: int
    intent_id: int
    tradingsymbol: str
    contract: BrokerInstrument

    @property
    def option_type(self) -> OptionType:
        # The contract always carries its own option type (validated at
        # resolution time by the strike selector).
        assert self.contract.option_type is not None
        return self.contract.option_type


@dataclass(frozen=True, slots=True)
class _DurableRecord:
    position_id: int
    quantity: int
    legs: dict[OptionType, _LegIds]
