"""Continuous monitoring of an open Strategy-1 position: recompute the combined
premium as market data arrives, evaluate the exit priority, and delegate to
exit_logic when a trigger fires.

Responsibility boundary -- this module *watches and decides when to ask for an
exit*; it never executes one. It places no orders and contains no broker-
specific logic: live prices arrive as pushed ticks or are pulled through the
``MarketDataGateway`` seam, exit decisions go through ``ExitLogic``, and state
is read from the database. What it owns is the loop-free monitoring surface the
strategy/scheduler drives:

* ``attach()``  -- reconstruct monitoring state from the database (this *is* the
  module's restart recovery: it never assumes a fresh position, it reads what is
  actually OPEN).
* ``on_tick()`` -- the preferred path: a pushed websocket tick updates the
  combined premium and evaluates the exit conditions immediately.
* ``poll_and_check()`` -- the safety-net path the monitoring scheduler calls on
  a cadence: it always enforces the price-independent triggers (time cutoff /
  kill-switch), and if websocket ticks have gone stale it *falls back to
  polling* the gateway for fresh prices before evaluating.
* ``stop()`` -- graceful shutdown: unsubscribe and go inactive without touching
  the open position (shutting down is not exiting).

The exit priority itself (Time -> Kill-switch/Emergency -> Stop-loss -> Target)
lives in ``exit_logic.evaluate_exit``; this module only supplies the current
premium/time/halt inputs and acts on the decision. Keeping the decision in one
pure, exhaustively-tested place -- rather than re-deriving it here -- is
deliberate.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

from algo.brokers.broker_base import InstrumentIdentifier
from algo.common.enums import ExitReason, OptionType, PositionState
from algo.database.repositories.position_repository import PositionRepository
from algo.logging.strategy_logger import PositionMonitorLogger
from algo.strategy_engine.strategies.strategy_1.combined_premium import CombinedPremiumTracker
from algo.strategy_engine.strategies.strategy_1.config import Strategy1Config
from algo.strategy_engine.strategies.strategy_1.exit_logic import ExitOutcome
from algo.strategy_engine.strategy_context import Tick

if TYPE_CHECKING:
    from algo.strategy_engine.strategies.strategy_1.exit_logic import ExitLogic, ExitResult
    from algo.strategy_engine.strategy_context import StrategyContext


class AttachOutcome(str, Enum):
    """Result of ``attach()`` -- what the database said about today's position
    when monitoring was (re)started."""

    MONITORING = "MONITORING"  # an OPEN position is now being monitored
    PENDING_EXIT = "PENDING_EXIT"  # EXIT_PENDING -- an interrupted exit; caller must complete it
    NOTHING_TO_MONITOR = "NOTHING_TO_MONITOR"  # none / CLOSED / ERROR / not yet entered


@dataclass(frozen=True, slots=True)
class _MonitoredPosition:
    """Immutable snapshot of what the monitor needs about the position it is
    watching, loaded once at attach time from the database.

    ``strike``/``expiry``/``quantity``/``entry_premium``/``entry_time`` are
    not used by any decision in this module -- they are captured purely so
    ``PositionMonitorLogger`` can display them, off the same ``position`` row
    already fetched in ``_load_monitored``, with no extra query.
    """

    position_id: int
    call_instrument: InstrumentIdentifier
    put_instrument: InstrumentIdentifier
    target_premium: Decimal | None
    stoploss_premium: Decimal | None
    strike: Decimal | None
    expiry: date | None
    quantity: int | None
    entry_premium: Decimal | None
    entry_time: datetime | None


class PositionMonitor:
    """Watches one OPEN Strategy-1 position and asks ``ExitLogic`` to close it
    when an exit condition fires.

    Injected with its ``StrategyContext`` (identity, config, market-data gateway,
    risk gateway, time, logger, session factory) and the ``ExitLogic`` it
    delegates to. Owns no thread and no loop -- ``on_tick`` and
    ``poll_and_check`` are called by the strategy/scheduler, which keeps this
    class trivially unit-testable.
    """

    def __init__(
        self,
        *,
        context: StrategyContext,
        exit_logic: ExitLogic,
        max_tick_staleness_seconds: float = 5.0,
        observability: PositionMonitorLogger | None = None,
    ) -> None:
        if not isinstance(context.config, Strategy1Config):
            raise TypeError(
                "PositionMonitor requires context.config to be a Strategy1Config, got "
                f"{type(context.config).__name__}"
            )
        if max_tick_staleness_seconds <= 0:
            raise ValueError("max_tick_staleness_seconds must be positive")

        self._context = context
        self._config: Strategy1Config = context.config
        self._identity = context.identity
        self._logger = context.logger
        self._exit_logic = exit_logic
        self._max_staleness = max_tick_staleness_seconds
        # Observability only -- see logging/strategy_logger.py's module
        # docstring. Never consulted for any decision in this class.
        self._observability = observability or PositionMonitorLogger(
            identity=context.identity,
            time_provider=context.time,
            market_data=context.market_data,
            spot_price_provider=context.spot_price_provider,
            logger=context.logger,
        )

        self._active = False
        self._monitored: _MonitoredPosition | None = None
        self._tracker: CombinedPremiumTracker | None = None
        self._last_tick_at: datetime | None = None
        self._last_combined_premium: Decimal | None = None

    # -- Properties ----------------------------------------------------

    @property
    def is_active(self) -> bool:
        """Whether the monitor is currently watching an open position."""
        return self._active

    @property
    def last_combined_premium(self) -> Decimal | None:
        """The most recently computed combined premium, for health/heartbeat
        reporting. ``None`` before both legs have priced at least once."""
        return self._last_combined_premium

    # -- Lifecycle -----------------------------------------------------

    def attach(self) -> AttachOutcome:
        """Reconstruct monitoring state from the database and, for an OPEN
        position, begin watching it.

        This is both the post-entry start and the restart-recovery entry point:
        the monitor never assumes it is fresh: it reads today's position and
        resumes from whatever state the database (the source of truth) holds. An
        OPEN position is subscribed to and monitored; an EXIT_PENDING position
        (an exit interrupted mid-flight) is reported back as ``PENDING_EXIT`` for
        the caller to complete via ``ExitLogic``, since finishing an interrupted
        exit is an execution concern, not a monitoring one.
        """
        trade_date = self._context.time.today()
        with self._context.session_factory() as session:
            repo = PositionRepository(session)
            position = repo.get_by_instance_and_date(self._identity.instance_id, trade_date)
            if position is None or position.state in (
                PositionState.IDLE,
                PositionState.ENTRY_PENDING,
                PositionState.CLOSED,
                PositionState.ERROR,
            ):
                self._logger.info(
                    "monitor attach: nothing to monitor for %s (state=%s)",
                    self._identity,
                    None if position is None else position.state,
                )
                return AttachOutcome.NOTHING_TO_MONITOR
            if position.state is PositionState.EXIT_PENDING:
                self._logger.warning(
                    "monitor attach: position %d is EXIT_PENDING (interrupted exit) for %s",
                    position.id,
                    self._identity,
                )
                return AttachOutcome.PENDING_EXIT

            monitored = self._load_monitored(repo, position.id)

        self._monitored = monitored
        self._tracker = CombinedPremiumTracker(
            call_instrument=monitored.call_instrument,
            put_instrument=monitored.put_instrument,
            target_pct=self._config.target_pct,
            sl_pct=self._config.sl_pct,
        )
        self._last_tick_at = None
        self._last_combined_premium = None
        self._subscribe()
        self._active = True
        self._logger.info(
            "monitor attached to position %d for %s (target=%s stoploss=%s)",
            monitored.position_id,
            self._identity,
            monitored.target_premium,
            monitored.stoploss_premium,
        )
        self._observability.on_attach(
            position_id=monitored.position_id,
            strike=monitored.strike,
            expiry=monitored.expiry,
            quantity=monitored.quantity,
            call_symbol=monitored.call_instrument.tradingsymbol,
            put_symbol=monitored.put_instrument.tradingsymbol,
            entry_premium=monitored.entry_premium,
            entry_time=monitored.entry_time,
        )
        return AttachOutcome.MONITORING

    def stop(self) -> None:
        """Graceful shutdown: unsubscribe and go inactive. Does not close the
        position -- the open trade survives the process stopping and is resumed
        by ``attach()`` on the next start."""
        if not self._active:
            return
        self._logger.info("monitor stopping for %s", self._identity)
        self._unsubscribe()
        self._active = False

    # -- Market-data driven evaluation ---------------------------------

    def on_tick(self, tick: Tick) -> None:
        """Preferred path: a pushed websocket tick. Updates the combined premium
        for the relevant leg and evaluates the exit conditions immediately.

        Ticks for unrelated instruments (or before the sibling leg has priced)
        are folded in harmlessly by the tracker; only ticks for one of the two
        monitored legs refresh the websocket-freshness clock used to decide
        whether polling is needed.
        """
        if not self._active or self._tracker is None or self._monitored is None:
            return
        if tick.instrument in (self._monitored.call_instrument, self._monitored.put_instrument):
            self._last_tick_at = self._context.time.now()
        self._tracker.on_tick(tick)
        self._evaluate(self._current_premium())

    def poll_and_check(self) -> None:
        """Safety-net path, called on a cadence by the monitoring scheduler.

        Always evaluates the price-independent triggers (time cutoff and
        kill-switch fire regardless of market data). If websocket ticks have
        gone stale -- no recent tick, or the feed reports disconnected -- it
        falls back to polling the gateway for fresh leg prices before evaluating
        the price-based triggers (stop-loss / target).
        """
        if not self._active:
            return
        if self._is_market_data_stale():
            self._poll_prices()
        self._evaluate(self._current_premium())

    # -- Internal: evaluation ------------------------------------------

    def _evaluate(self, combined_premium: Decimal | None) -> None:
        """Ask exit_logic for a decision under the approved priority and act on
        it. Never places orders itself -- it calls ``ExitLogic.exit``."""
        if not self._active or self._monitored is None:
            return
        self._last_combined_premium = combined_premium
        decision = self._exit_logic.evaluate(
            now_ist_time=self._context.time.now_ist().time(),
            halted=self._context.risk.is_halted(self._identity),
            combined_premium=combined_premium,
            target_premium=self._monitored.target_premium,
            stoploss_premium=self._monitored.stoploss_premium,
        )
        self._log_cycle()
        if decision.should_exit:
            assert decision.reason is not None
            self._trigger_exit(decision.reason, decision.detail)

    def _log_cycle(self) -> None:
        """Observability only -- emits the periodic monitoring snapshot (see
        logging/strategy_logger.py). Reads only already-known tracker state
        plus a read-only market-data-connection check; never touches the
        exit decision above, which has already run by the time this is
        called."""
        assert self._tracker is not None  # guarded by the _monitored check above
        self._observability.on_cycle(
            combined_premium=self._last_combined_premium,
            call_price=self._tracker.latest_call_price,
            put_price=self._tracker.latest_put_price,
            target_premium=self._monitored.target_premium if self._monitored else None,
            stoploss_premium=self._monitored.stoploss_premium if self._monitored else None,
            last_tick_at=self._last_tick_at,
            price_source="LIVE KITE WEBSOCKET" if not self._is_market_data_stale() else "POLLING FALLBACK",
        )

    def _trigger_exit(self, reason: ExitReason, detail: str) -> None:
        """Delegate the actual close to exit_logic, then deactivate so a single
        trigger produces exactly one exit attempt (no repeated firing)."""
        self._logger.info(
            "monitor triggering exit for %s: %s (%s)", self._identity, reason.value, detail
        )
        try:
            result = self._exit_logic.exit(reason)
            self._logger.info(
                "monitor exit result for %s: %s (%s)",
                self._identity,
                result.outcome.value,
                result.message,
            )
            if result.outcome is ExitOutcome.EXITED:
                self._log_close(reason, result)
        finally:
            # Whatever the outcome (closed, already-closed, or errored/frozen),
            # this monitor's work on this position is done. Stop watching so we
            # do not re-fire against a position that is exiting or errored.
            self._unsubscribe()
            self._active = False

    def _log_close(self, reason: ExitReason, result: ExitResult) -> None:
        """Observability only -- reads the now-closed position's own row
        (already committed by exit_logic.exit() above) purely to display the
        exit premium/time; never writes anything and runs strictly after the
        real close has already completed."""
        exit_premium: Decimal | None = None
        exit_time: datetime | None = None
        try:
            if result.position_id is not None:
                with self._context.session_factory() as session:
                    position = PositionRepository(session).get_by_id(result.position_id)
                    if position is not None:
                        exit_premium = position.combined_exit_premium
                        exit_time = position.exit_completed_at
        except Exception:  # noqa: BLE001 -- observability must never break exit handling
            self._logger.debug("failed to read closed position for observability log", exc_info=True)
        self._observability.on_close(
            reason=reason, exit_premium=exit_premium, exit_time=exit_time,
            realized_pnl=result.realized_pnl,
        )

    # -- Internal: market data -----------------------------------------

    def _current_premium(self) -> Decimal | None:
        if self._tracker is None:
            return None
        snapshot = self._tracker.current_snapshot()
        return snapshot.combined_premium if snapshot is not None else None

    def _is_market_data_stale(self) -> bool:
        """True if we should poll rather than trust pushed ticks: never received
        a tick, the last tick is older than the staleness budget, or the feed
        reports itself disconnected."""
        try:
            connected = self._context.market_data.is_connected()
        except Exception:  # noqa: BLE001 -- a gateway hiccup should not crash monitoring
            connected = False
        if not connected or self._last_tick_at is None:
            return True
        age = (self._context.time.now() - self._last_tick_at).total_seconds()
        return age > self._max_staleness

    def _poll_prices(self) -> None:
        """Pull fresh leg prices through the gateway and feed them to the tracker
        as synthetic ticks. A polling failure is logged and swallowed -- the
        price-independent triggers still evaluate on stale/absent data, which is
        exactly what must keep working when the feed is down."""
        if self._monitored is None:
            return
        instruments = [self._monitored.call_instrument, self._monitored.put_instrument]
        try:
            ltps = self._context.market_data.get_ltps(instruments)
        except Exception:  # noqa: BLE001
            self._logger.warning(
                "monitor poll failed for %s; evaluating on last-known data", self._identity,
                exc_info=True,
            )
            return
        now = self._context.time.now()
        assert self._tracker is not None
        for instrument in instruments:
            price = ltps.get(instrument)
            if price is not None:
                self._tracker.on_tick(
                    Tick(instrument=instrument, last_price=price, timestamp=now)
                )

    def _subscribe(self) -> None:
        if self._monitored is None:
            return
        try:
            self._context.market_data.subscribe(
                [self._monitored.call_instrument, self._monitored.put_instrument]
            )
        except Exception:  # noqa: BLE001 -- subscription failure falls back to polling
            self._logger.warning(
                "monitor subscribe failed for %s; will rely on polling", self._identity,
                exc_info=True,
            )

    def _unsubscribe(self) -> None:
        if self._monitored is None:
            return
        try:
            self._context.market_data.unsubscribe(
                [self._monitored.call_instrument, self._monitored.put_instrument]
            )
        except Exception:  # noqa: BLE001
            self._logger.warning("monitor unsubscribe failed for %s", self._identity, exc_info=True)

    # -- Internal: loading ---------------------------------------------

    def _load_monitored(self, repo: PositionRepository, position_id: int) -> _MonitoredPosition:
        """Build the immutable monitoring snapshot from the OPEN position and its
        two trade legs."""
        position = repo.get_by_id_or_raise(position_id)
        trades = repo.list_trades_for_position(position_id)
        by_type = {t.option_type: t for t in trades}
        if OptionType.CE not in by_type or OptionType.PE not in by_type:
            raise ValueError(
                f"position {position_id} does not have both CE and PE legs to monitor"
            )
        call = by_type[OptionType.CE]
        put = by_type[OptionType.PE]
        return _MonitoredPosition(
            position_id=position_id,
            call_instrument=InstrumentIdentifier(
                exchange=call.exchange, tradingsymbol=call.trading_symbol
            ),
            put_instrument=InstrumentIdentifier(
                exchange=put.exchange, tradingsymbol=put.trading_symbol
            ),
            target_premium=position.target_premium,
            stoploss_premium=position.stoploss_premium,
            strike=position.strike,
            expiry=position.expiry_date,
            quantity=position.quantity,
            entry_premium=position.combined_entry_premium,
            entry_time=position.entry_completed_at,
        )
