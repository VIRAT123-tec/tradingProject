"""Strategy-1 implementation of strategy_base.py's contract: the orchestrator
that wires the already-built, independently-tested Strategy-1 components
together and drives them through the generic Strategy Engine's lifecycle.

This module coordinates; it does not decide. Every actual decision -- what the
ATM strike is, what the combined premium is, when to enter, when to exit, which
state transitions are legal -- already lives in strike_selector.py,
combined_premium.py, entry_logic.py, exit_logic.py, monitor.py, and
state_machine.py, each independently unit-tested. ``Strategy1`` only:

* constructs and connects those components from the injected ``StrategyContext``
  (dependency injection: every collaborator is either read off the context or
  accepted as an optional override for tests -- nothing is ever hardcoded or
  self-constructed from globals),
* routes the generic engine's lifecycle calls (``recover``, ``on_time_trigger``,
  ``on_market_tick``, ``on_shutdown``, ``health``) to the right component,
* registers itself with the strategy registry so ``instance_factory`` can build
  it from configuration alone.

``combined_premium.py`` and ``state_machine.py`` are not constructed directly
here: the former is wired transitively (``PositionMonitor`` builds its own
``CombinedPremiumTracker`` per position at ``attach()`` time), and the latter is
used both transitively (entry_logic/exit_logic each construct their own
``PositionStateMachine`` per unit of work) and directly, in this module's own
recovery path when an ENTRY_PENDING position cannot be safely resumed (see
``_mark_position_error_and_freeze`` below).

Must remain instrument-agnostic -- instrument-specific behavior comes only from
the injected config (``Strategy1Config``, one instance per instrument YAML) and
the injected instrument-aware seams (``StrikeSelector``, market data), never
from a conditional on ``self.context.identity.instrument`` inside this file.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from algo.common.enums import ExitReason, InstanceStatus, PositionState, StateTransitionActor
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.strategy_instance_repository import (
    StrategyInstanceRepository,
)
from algo.database.session import unit_of_work
from algo.strategy_engine.strategies.strategy_1.config import RetrySettings, Strategy1Config
from algo.strategy_engine.strategies.strategy_1.entry_logic import EntryLogic, EntryOutcome
from algo.strategy_engine.strategies.strategy_1.exit_logic import ExitLogic
from algo.strategy_engine.strategies.strategy_1.monitor import PositionMonitor
from algo.strategy_engine.strategies.strategy_1.state_machine import PositionStateMachine
from algo.strategy_engine.strategies.strategy_1.strike_selector import StrikeSelector
from algo.strategy_engine.strategy_base import Strategy, StrategyHealth, TimeTrigger, TriggerCatchUpPolicy
from algo.strategy_engine.strategy_registry import register_strategy

if TYPE_CHECKING:
    from pydantic import BaseModel
    from sqlalchemy.orm import Session

    from algo.strategy_engine.strategy_context import StrategyContext, Tick

_ENTRY_TRIGGER = "entry"
_CUTOFF_TRIGGER = "cutoff"

# Must match exit_logic._prepare_exit's transition-reason format exactly --
# see _infer_pending_exit_reason below.
_EXIT_TRIGGER_REASON_PREFIX = "exit triggered: "


@register_strategy("strategy_1")
class Strategy1(Strategy):
    """Non-directional ATM short straddle: one weekly-expiry cycle per trading
    day, run identically on any instrument via injected config.

    Sub-components (``StrikeSelector``, ``EntryLogic``, ``ExitLogic``,
    ``PositionMonitor``) are built from ``context`` if not explicitly passed in,
    so the generic ``instance_factory`` (which constructs a strategy with only
    ``strategy_cls(context)``) works unchanged, while tests can inject fakes for
    any of them directly without needing a fully wired real stack.
    """

    def __init__(
        self,
        context: StrategyContext,
        *,
        strike_selector: StrikeSelector | None = None,
        entry_logic: EntryLogic | None = None,
        exit_logic: ExitLogic | None = None,
        monitor: PositionMonitor | None = None,
    ) -> None:
        super().__init__(context)

        if not isinstance(context.config, Strategy1Config):
            raise TypeError(
                "Strategy1 requires context.config to be a Strategy1Config, got "
                f"{type(context.config).__name__}"
            )

        # StrikeSelector is only actually needed to build a real EntryLogic; if
        # a caller supplies entry_logic directly (e.g. a test injecting a fake),
        # constructing a StrikeSelector -- and therefore requiring
        # instrument_service/expiry_service to be present on the context -- is
        # unnecessary work this strategy should not force on that caller.
        if strike_selector is not None:
            self._strike_selector: StrikeSelector | None = strike_selector
        elif entry_logic is None:
            self._strike_selector = self._build_strike_selector(context)
        else:
            self._strike_selector = None

        config: Strategy1Config = context.config  # type: ignore[assignment]
        retry = config.retry

        self._exit_logic = exit_logic or ExitLogic(
            context=context,
            order_timeout=retry.order_timeout_seconds,
            fill_confirmation_attempts=retry.fill_confirmation_attempts,
            fill_confirmation_delay=retry.fill_confirmation_delay_seconds,
            close_retry_attempts=retry.close_retry_attempts,
            close_retry_delay=retry.close_retry_delay_seconds,
        )

        if entry_logic is not None:
            self._entry_logic = entry_logic
        else:
            assert self._strike_selector is not None
            self._entry_logic = self._build_entry_logic(context, self._strike_selector, retry)

        self._monitor = monitor or PositionMonitor(
            context=context,
            exit_logic=self._exit_logic,
            # The degraded-feed polling cadence doubles as the staleness
            # threshold: if no tick has arrived within that same window, the
            # feed is by definition not keeping up with the cadence we'd poll
            # at anyway, so it's time to fall back to polling.
            max_tick_staleness_seconds=config.polling_interval_seconds,
        )

    @staticmethod
    def _build_strike_selector(context: StrategyContext) -> StrikeSelector:
        instrument_service = context.instrument_service
        expiry_service = context.expiry_service
        if instrument_service is None or expiry_service is None:
            raise TypeError(
                "Strategy1 requires StrategyContext.instrument_service and "
                "expiry_service to be set, unless a strike_selector is supplied directly"
            )
        return StrikeSelector(
            instrument_service=instrument_service,
            expiry_service=expiry_service,
            broker=context.broker,
        )

    @staticmethod
    def _build_entry_logic(
        context: StrategyContext, strike_selector: StrikeSelector, retry: RetrySettings
    ) -> EntryLogic:
        expiry_service = context.expiry_service
        spot_price_provider = context.spot_price_provider
        if expiry_service is None or spot_price_provider is None:
            raise TypeError(
                "Strategy1 requires StrategyContext.expiry_service and "
                "spot_price_provider to be set, unless an entry_logic is supplied directly"
            )
        return EntryLogic(
            context=context,
            strike_selector=strike_selector,
            expiry_service=expiry_service,
            spot_price_provider=spot_price_provider,
            order_timeout=retry.order_timeout_seconds,
            fill_confirmation_attempts=retry.fill_confirmation_attempts,
            fill_confirmation_delay=retry.fill_confirmation_delay_seconds,
        )

    # -- Configuration ---------------------------------------------------

    @classmethod
    def config_schema(cls) -> type[BaseModel]:
        return Strategy1Config

    # -- Scheduling --------------------------------------------------------

    def scheduled_triggers(self) -> Sequence[TimeTrigger]:
        """Two triggers, derived from this instance's own config: ``entry`` at
        the configured entry time (never fired late if missed -- a missed entry
        window is a deliberate no-trade, not something to force after the
        market has moved), and ``cutoff`` at the hard exit time, which *does*
        fire on catch-up -- a restart past the cutoff must still force the
        exit, exactly the scenario ``TriggerCatchUpPolicy.FIRE_ON_STARTUP``
        exists for.
        """
        config: Strategy1Config = self.context.config  # type: ignore[assignment]
        return [
            TimeTrigger(_ENTRY_TRIGGER, config.entry_time, TriggerCatchUpPolicy.SKIP),
            TimeTrigger(_CUTOFF_TRIGGER, config.hard_cutoff_time, TriggerCatchUpPolicy.FIRE_ON_STARTUP),
        ]

    # -- Lifecycle -----------------------------------------------------

    def initialize(self) -> None:
        """One-time setup for a fresh instance with no prior state today.

        Nothing to subscribe to yet -- the straddle's CE/PE contracts are not
        known until entry resolves them -- so this is intentionally light: it
        exists as an explicit, logged marker that today started clean, for
        parity with every other lifecycle transition being observable.
        """
        self.context.logger.info(
            "Strategy1 initialize: fresh start for %s on %s",
            self.context.identity,
            self.context.time.today(),
        )

    def recover(self) -> None:
        """Reconstruct from the database rather than assuming IDLE.

        Reads today's Position (if any) and its state, then takes exactly the
        action that state calls for:

        * no position yet -> equivalent to a fresh start -> ``initialize()``.
        * OPEN -> resume monitoring via ``PositionMonitor.attach()``.
        * EXIT_PENDING -> an exit was interrupted mid-flight; complete it by
          re-invoking ``ExitLogic.exit()`` with the original reason recovered
          from the position's own state-transition audit trail (exit_logic's
          own idempotency -- per-leg tag lookup, terminal-state checks -- makes
          resuming safe even if some legs already closed before the crash).
        * ENTRY_PENDING -> entry_logic has no supported path to resume an
          interrupted *entry* (see the assumptions in this task's summary);
          rather than silently leaving it stuck, this is treated as
          unrecoverable and the position is moved to ERROR with the instance
          frozen for manual review, via the same state machine +
          StrategyInstanceRepository calls entry_logic/exit_logic already use.
        * CLOSED / ERROR -> nothing to do; already terminal or already
          awaiting a human.
        """
        trade_date = self.context.time.today()
        with self._unit_of_work() as session:
            position = PositionRepository(session).get_by_instance_and_date(
                self.context.identity.instance_id, trade_date
            )
            state = position.state if position is not None else None
            position_id = position.id if position is not None else None

        if state is None or state is PositionState.IDLE:
            self.context.logger.info(
                "recover: nothing persisted yet for %s; treating as fresh start",
                self.context.identity,
            )
            self.initialize()
            return

        if state is PositionState.CLOSED:
            self.context.logger.info(
                "recover: position %d already CLOSED for %s; nothing to do",
                position_id,
                self.context.identity,
            )
            return

        if state is PositionState.ERROR:
            self.context.logger.warning(
                "recover: position %d is ERROR for %s; awaiting manual review",
                position_id,
                self.context.identity,
            )
            return

        if state is PositionState.OPEN:
            outcome = self._monitor.attach()
            self.context.logger.info(
                "recover: resumed monitoring position %d for %s (%s)",
                position_id,
                self.context.identity,
                outcome.value,
            )
            return

        if state is PositionState.EXIT_PENDING:
            assert position_id is not None
            reason = self._infer_pending_exit_reason(position_id)
            self.context.logger.warning(
                "recover: position %d is EXIT_PENDING for %s; completing "
                "interrupted exit as %s",
                position_id,
                self.context.identity,
                reason.value,
            )
            result = self._exit_logic.exit(reason)
            self.context.logger.info(
                "recover: interrupted-exit completion for position %d: %s",
                position_id,
                result.outcome.value,
            )
            return

        # state is ENTRY_PENDING: no supported resume path.
        assert state is PositionState.ENTRY_PENDING
        assert position_id is not None
        self.context.logger.critical(
            "recover: position %d is ENTRY_PENDING for %s; entry_logic cannot "
            "currently resume an interrupted entry -- marking ERROR for manual review",
            position_id,
            self.context.identity,
        )
        self._mark_position_error_and_freeze(
            position_id,
            "recovery found ENTRY_PENDING position; automatic entry resume is not "
            "supported -- verify broker state manually before clearing",
        )

    def on_time_trigger(self, trigger_name: str) -> None:
        """Route a due scheduled trigger to the component that owns it.

        ``entry`` -> attempt today's entry, then start monitoring if it opened.
        ``cutoff`` -> ask the monitor to check now, via the same evaluation path
        ticks use (``poll_and_check``), so the hard cutoff is enforced by the
        one priority-ordered decision function even when no fresh tick has
        arrived.
        """
        if trigger_name == _ENTRY_TRIGGER:
            self._handle_entry_trigger()
        elif trigger_name == _CUTOFF_TRIGGER:
            self._monitor.poll_and_check()
        else:
            self.context.logger.warning(
                "Strategy1 received unknown time trigger %r for %s",
                trigger_name,
                self.context.identity,
            )

    def on_market_tick(self, tick: Tick) -> None:
        """Pure delegation to the monitor -- it already no-ops safely when not
        actively watching a position."""
        self._monitor.on_tick(tick)

    def on_monitor_cycle(self) -> None:
        """Periodic safety-net cadence: run the monitor's poll-and-check, the
        same price-independent-plus-price-based exit evaluation the hard-cutoff
        trigger uses. The monitor no-ops safely when no position is being
        watched, so this is cheap when flat and is the guaranteed intraday
        stop-loss/target driver when a straddle is open (it polls the broker
        for fresh leg prices when the live tick feed is stale or absent)."""
        self._monitor.poll_and_check()

    def on_shutdown(self) -> None:
        """Graceful shutdown: stop monitoring (unsubscribe from market data).

        Does not touch the open position -- an open straddle survives the
        process stopping and is resumed by ``recover()`` on the next start.
        """
        self._monitor.stop()

    # -- Introspection -------------------------------------------------

    def health(self) -> StrategyHealth:
        """Cheap, non-raising health snapshot: today's position state plus,
        while actively monitoring, the last known combined premium."""
        try:
            trade_date = self.context.time.today()
            with self._unit_of_work() as session:
                position = PositionRepository(session).get_by_instance_and_date(
                    self.context.identity.instance_id, trade_date
                )
            state_label = position.state.value if position is not None else "NO_POSITION"
            healthy = position is None or position.state is not PositionState.ERROR
            detail = (
                f"combined_premium={self._monitor.last_combined_premium}"
                if self._monitor.is_active
                else None
            )
            return StrategyHealth(
                healthy=healthy,
                state=state_label,
                checked_at=self.context.time.now(),
                detail=detail,
            )
        except Exception as exc:  # noqa: BLE001 -- health() must never raise
            return StrategyHealth(
                healthy=False,
                state="UNKNOWN",
                checked_at=datetime.now(timezone.utc),
                detail=f"health check failed: {exc}",
            )

    # -- Internal --------------------------------------------------------

    def _handle_entry_trigger(self) -> None:
        result = self._entry_logic.enter()
        self.context.logger.info(
            "entry trigger for %s: %s (%s)",
            self.context.identity,
            result.outcome.value,
            result.message,
        )
        if result.outcome is EntryOutcome.ENTERED:
            self._monitor.attach()

    def _infer_pending_exit_reason(self, position_id: int) -> ExitReason:
        """Best-effort recovery of the ExitReason that originally triggered an
        interrupted exit, read from the position's own audit trail
        (``PositionStateTransition.reason``, written by exit_logic in the exact
        format ``"exit triggered: {reason.value}"``).

        Falls back to ``ExitReason.MANUAL`` if no matching transition is found
        (an unexpected condition -- e.g. audit format drift) rather than
        guessing a specific automated reason; either way exit_logic's own
        idempotent per-leg handling makes completing the exit safe regardless
        of which reason label is used to do it.
        """
        with self._unit_of_work() as session:
            transitions = PositionRepository(session).list_state_transitions(position_id)
        for transition in reversed(transitions):
            if transition.to_state is PositionState.EXIT_PENDING and transition.reason:
                for candidate in ExitReason:
                    if transition.reason == f"{_EXIT_TRIGGER_REASON_PREFIX}{candidate.value}":
                        return candidate
        self.context.logger.warning(
            "recover: could not infer original exit reason for position %d "
            "from its audit trail; defaulting to MANUAL",
            position_id,
        )
        return ExitReason.MANUAL

    def _mark_position_error_and_freeze(self, position_id: int, reason: str) -> None:
        """Transition the position to ERROR and freeze this instance -- the
        same repository/state-machine calls entry_logic and exit_logic already
        use for an unrecoverable failure, applied here for one recovery finds
        can't handle safely."""
        with self._unit_of_work() as session:
            position_repo = PositionRepository(session)
            position = position_repo.get_by_id_for_update(position_id)
            if position is not None and position.state not in (
                PositionState.CLOSED,
                PositionState.ERROR,
            ):
                position.error_message = reason
                PositionStateMachine(position_repo).mark_error(
                    position, actor=StateTransitionActor.RECOVERY, reason=reason
                )
            instance_repo = StrategyInstanceRepository(session)
            instance = instance_repo.get_by_id_for_update(self.context.identity.instance_id)
            if instance is not None and instance.status is not InstanceStatus.FROZEN:
                instance_repo.set_status(instance, InstanceStatus.FROZEN)

    @contextmanager
    def _unit_of_work(self) -> Iterator[Session]:
        """One committed transaction against the context's injected factory --
        the shared commit/rollback/close pattern (see
        ``database.session.unit_of_work``). A plain ``with session_factory() as
        session:`` only closes on exit and never commits, so every write in
        this module must go through this helper rather than that raw form.
        """
        with unit_of_work(self.context.session_factory) as session:
            yield session
