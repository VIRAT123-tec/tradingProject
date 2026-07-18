"""StrategyRunner: drives one strategy instance's lifecycle and isolates its
failures from the rest of the platform.

The runner is a *passive dispatch target*, not a thread owner. External threads
call into it: the platform ``scheduler.py`` thread calls ``dispatch_time_trigger``
when a declared trigger is due, and the market-data tick thread calls
``dispatch_tick``. The runner serializes those calls behind a per-instance lock,
so a strategy's own hooks never run concurrently for the same instance and need
no locking of their own.

Two guarantees the runner exists to provide:

1. Recovery on start -- ``start()`` calls ``Strategy.recover()`` (reconstruct
   from the database) before accepting any trigger or tick, so a restart never
   assumes a clean slate.
2. Fault isolation -- any unhandled exception from a strategy hook is caught,
   logged, and turned into a FROZEN transition of *that instance's*
   ``StrategyInstance`` row. The exception is not propagated to the shared
   scheduler/tick thread, so one instrument's failure never takes down the
   others (the spec's "freeze that instrument's instance, not the whole
   platform").
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from enum import Enum
from typing import TYPE_CHECKING

from algo.common.enums import InstanceStatus
from algo.database.repositories.strategy_instance_repository import (
    StrategyInstanceRepository,
)
from algo.database.session import unit_of_work

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.orm import Session

    from algo.strategy_engine.strategy_base import Strategy, StrategyHealth, TimeTrigger
    from algo.strategy_engine.strategy_context import Tick


class RunnerStatus(str, Enum):
    """Lifecycle status of a runner (distinct from the strategy's own
    domain state machine, which the engine never interprets)."""

    CREATED = "CREATED"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    FROZEN = "FROZEN"


class StrategyRunner:
    """Owns the lifecycle of exactly one ``Strategy`` instance."""

    def __init__(self, strategy: Strategy) -> None:
        """The runner reads everything it needs (context, identity,
        session factory, logger) from the strategy's injected context, so
        wiring stays in one place -- ``instance_factory``."""
        self._strategy = strategy
        self._context = strategy.context
        self._logger = strategy.context.logger
        # RLock, not Lock: serializes external dispatches while tolerating an
        # (unusual but legal) re-entrant call from within a hook on the same
        # thread rather than self-deadlocking.
        self._lock = threading.RLock()
        self._status = RunnerStatus.CREATED

    @property
    def status(self) -> RunnerStatus:
        """Current lifecycle status (thread-safe read of a simple enum)."""
        return self._status

    @property
    def identity_str(self) -> str:
        """Human-readable instance identity, for logs and error messages."""
        return str(self._context.identity)

    def scheduled_triggers(self) -> Sequence[TimeTrigger]:
        """The strategy's declared time triggers, for the scheduler to arm.

        Read-through to the strategy; safe to call before ``start()`` so the
        scheduler can register triggers up front.
        """
        return self._strategy.scheduled_triggers()

    def start(self) -> None:
        """Bring the instance up: run recovery, then begin accepting dispatch.

        Idempotency/safety: only valid from CREATED. If ``recover()`` raises,
        the instance is frozen (not left half-started) and the runner ends in
        FROZEN rather than RUNNING. Raises ``RuntimeError`` if called from any
        status other than CREATED (a wiring bug, not a runtime condition).
        """
        with self._lock:
            if self._status is not RunnerStatus.CREATED:
                raise RuntimeError(
                    f"start() called on {self.identity_str} in status "
                    f"{self._status.value}; only CREATED is valid"
                )
            self._logger.info("starting strategy instance %s", self.identity_str)
            if self._run_guarded("recover", self._strategy.recover):
                self._status = RunnerStatus.RUNNING
                self._logger.info("strategy instance %s is RUNNING", self.identity_str)

    def dispatch_time_trigger(self, trigger_name: str) -> None:
        """Deliver a due scheduled trigger to the strategy.

        Ignored (with a warning) unless the runner is RUNNING -- a trigger
        firing against a stopped or frozen instance is dropped rather than
        resurrecting it. Any exception from the hook freezes the instance and
        is not re-raised to the caller's (shared) scheduler thread.
        """
        with self._lock:
            if self._status is not RunnerStatus.RUNNING:
                self._logger.warning(
                    "ignoring time trigger %r for %s in status %s",
                    trigger_name,
                    self.identity_str,
                    self._status.value,
                )
                return
            self._run_guarded(
                f"on_time_trigger[{trigger_name}]",
                lambda: self._strategy.on_time_trigger(trigger_name),
            )

    def dispatch_tick(self, tick: Tick) -> None:
        """Deliver one market tick to the strategy.

        Dropped silently at debug level (not warning -- ticks are high
        frequency) unless RUNNING. Serialized with time triggers by the same
        lock; a long-running trigger will briefly back-pressure ticks, which
        is the intended "one thing at a time per instance" behavior.
        """
        with self._lock:
            if self._status is not RunnerStatus.RUNNING:
                self._logger.debug(
                    "dropping tick for %s in status %s",
                    self.identity_str,
                    self._status.value,
                )
                return
            self._run_guarded(
                "on_market_tick", lambda: self._strategy.on_market_tick(tick)
            )

    def dispatch_monitor_cycle(self) -> None:
        """Deliver one periodic monitoring cycle to the strategy.

        This is the driver behind the intraday stop-loss/target safety net: the
        monitoring scheduler calls it on a cadence, and the strategy's
        ``on_monitor_cycle`` re-evaluates its open position. Dropped silently at
        debug level (like ticks, it is high-frequency and mostly a no-op)
        unless RUNNING, and serialized with time triggers and ticks by the same
        per-instance lock. Any exception freezes the instance and is not
        re-raised to the caller's shared monitoring thread.
        """
        with self._lock:
            if self._status is not RunnerStatus.RUNNING:
                self._logger.debug(
                    "dropping monitor cycle for %s in status %s",
                    self.identity_str,
                    self._status.value,
                )
                return
            self._run_guarded("on_monitor_cycle", self._strategy.on_monitor_cycle)

    def stop(self) -> None:
        """Gracefully stop the instance: call ``on_shutdown`` exactly once and
        move to STOPPED.

        Idempotent: a second call (or a call on an already-frozen instance) is
        a no-op. A failure inside ``on_shutdown`` is logged critically but
        does not prevent the transition to STOPPED -- the process is coming
        down regardless, and freezing a stopping instance would be pointless.
        Stopping does *not* flatten open positions; it only stops event
        handling. The DB remains the source of truth for what is still open.
        """
        with self._lock:
            if self._status in (RunnerStatus.STOPPED, RunnerStatus.FROZEN):
                self._logger.info(
                    "stop() on %s is a no-op in status %s",
                    self.identity_str,
                    self._status.value,
                )
                return
            self._logger.info("stopping strategy instance %s", self.identity_str)
            try:
                self._strategy.on_shutdown()
            except Exception:
                self._logger.critical(
                    "on_shutdown() raised for %s during graceful stop",
                    self.identity_str,
                    exc_info=True,
                )
            self._status = RunnerStatus.STOPPED

    def health(self) -> StrategyHealth:
        """Return the strategy's health snapshot, degraded to unhealthy if the
        runner is frozen or the strategy's own ``health()`` raises."""
        from algo.strategy_engine.strategy_base import StrategyHealth

        if self._status is RunnerStatus.FROZEN:
            return StrategyHealth(
                healthy=False,
                state=RunnerStatus.FROZEN.value,
                checked_at=self._context.time.now(),
                detail="instance frozen after an unrecoverable hook failure",
            )
        try:
            return self._strategy.health()
        except Exception:
            self._logger.error(
                "health() raised for %s; reporting unhealthy",
                self.identity_str,
                exc_info=True,
            )
            return StrategyHealth(
                healthy=False,
                state=self._status.value,
                checked_at=self._context.time.now(),
                detail="health() raised",
            )

    # -- Internal ------------------------------------------------------

    def _run_guarded(self, label: str, action: Callable[[], None]) -> bool:
        """Run ``action``; on any exception, freeze the instance and swallow.

        Returns True on success, False if the action failed and the instance
        was frozen. Caller holds ``self._lock``. Swallowing (not re-raising)
        is deliberate: the caller is a shared scheduler/tick thread iterating
        over many instances, and one instance's failure must not break that
        loop.
        """
        try:
            action()
            return True
        except Exception:
            self._logger.critical(
                "hook %s failed for %s; freezing this instance",
                label,
                self.identity_str,
                exc_info=True,
            )
            self._status = RunnerStatus.FROZEN
            self._freeze_instance(reason=f"hook {label} raised")
            return False

    def _freeze_instance(self, *, reason: str) -> None:
        """Persist the FROZEN status onto this instance's ``StrategyInstance``
        row, isolating it pending manual review.

        Best-effort and self-contained: a failure to even record the freeze is
        logged critically (it cannot be escalated further from here), but the
        in-memory status is already FROZEN, so no further dispatch will reach
        the strategy regardless of whether the DB write succeeds.
        """
        try:
            with self._unit_of_work() as session:
                repo = StrategyInstanceRepository(session)
                instance = repo.get_by_id_for_update(self._context.identity.instance_id)
                if instance is None:
                    self._logger.critical(
                        "cannot freeze %s: StrategyInstance row %d missing",
                        self.identity_str,
                        self._context.identity.instance_id,
                    )
                    return
                if instance.status is not InstanceStatus.FROZEN:
                    repo.set_status(instance, InstanceStatus.FROZEN)
                    self._logger.critical(
                        "froze %s (reason: %s)", self.identity_str, reason
                    )
        except Exception:
            self._logger.critical(
                "failed to persist FROZEN status for %s (reason: %s)",
                self.identity_str,
                reason,
                exc_info=True,
            )

    @contextmanager
    def _unit_of_work(self) -> Iterator[Session]:
        """One committed transaction against the context's injected factory.

        Not ``database.session.session_scope()``, which binds to the global
        session factory -- the runner uses the factory in its (possibly
        test-injected) context. Delegates to ``database.session.unit_of_work``
        for the shared commit/rollback/close pattern.
        """
        with unit_of_work(self._context.session_factory) as session:
            yield session
